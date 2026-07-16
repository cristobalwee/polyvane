"""Post-resolution P&L review system.

Two responsibilities:

  1. Check pending trades whose markets have likely resolved (on a configurable
     interval) and update the journal with `outcome` + `pnl`. Resolution data
     comes from the same Polymarket Gamma API the strategy uses, plus (where
     possible) the recorded historical reading from the resolution source.

  2. Periodically (default: weekly on Sunday) compute review metrics —
     predicted vs realized edge, by strategy / city / volume tier — and
     persist them in a separate SQLite table `review_metrics` so trends
     can be plotted offline.

  3. Flag systematic biases ("NYC forecasts overestimate by 1.2°F") and
     resolution mismatches (forecast vs actual deviates more than the
     model's std dev) for manual review.

The reviewer is deliberately conservative about *closing* trades: if it
can't determine the resolution unambiguously, it leaves the trade pending
rather than guess.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import statistics
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import aiohttp


log = logging.getLogger("monitoring.reviewer")


GAMMA_BASE = "https://gamma-api.polymarket.com"


_REVIEW_SCHEMA = """
CREATE TABLE IF NOT EXISTS review_metrics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       TEXT    NOT NULL,
    period          TEXT    NOT NULL,         -- 'weekly', 'manual', etc.
    bucket_kind     TEXT    NOT NULL,         -- 'strategy' | 'city' | 'volume_tier' | 'overall'
    bucket_key      TEXT    NOT NULL,         -- e.g. 'NYC', 'weather', 'high'
    n_trades        INTEGER NOT NULL,
    win_rate        REAL,
    avg_edge_in     REAL,
    avg_realized    REAL,                     -- mean (pnl / size_usd) for closed trades
    bias_pct        REAL,                     -- avg(predicted - realized) — positive = overestimating
    total_pnl       REAL,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_review_kind_key ON review_metrics(bucket_kind, bucket_key);
CREATE INDEX IF NOT EXISTS idx_review_timestamp ON review_metrics(timestamp);
"""


@dataclass
class ReviewerConfig:
    auto_resolve_check_interval_sec: int = 600
    weekly_review_day: str = "sunday"        # weekday name, lowercase
    bias_flag_threshold_pct: float = 0.02    # 2% absolute realized vs predicted gap
    mismatch_std_dev_multiple: float = 2.0
    kalshi_base_url: str = "https://demo-api.kalshi.co/trade-api/v2"

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> "ReviewerConfig":
        d = d or {}
        return cls(
            auto_resolve_check_interval_sec=int(d.get("auto_resolve_check_interval_sec", 600)),
            weekly_review_day=str(d.get("weekly_review_day") or "sunday").lower(),
            bias_flag_threshold_pct=float(d.get("bias_flag_threshold_pct", 0.02)),
            mismatch_std_dev_multiple=float(d.get("mismatch_std_dev_multiple", 2.0)),
            kalshi_base_url=str(
                d.get("kalshi_base_url") or "https://demo-api.kalshi.co/trade-api/v2"
            ),
        )


@contextmanager
def _connect(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


class Reviewer:
    def __init__(
        self,
        config: ReviewerConfig,
        journal_db_path: Path,
        *,
        alert_hook: Any = None,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.config = config
        self.db_path = Path(journal_db_path)
        self._alert_hook = alert_hook
        self._session = session
        self._owns_session = session is None
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._last_weekly_review_at: datetime | None = None
        with _connect(self.db_path) as conn:
            conn.executescript(_REVIEW_SCHEMA)
            conn.commit()

    async def start(self) -> None:
        if self._owns_session and self._session is None:
            self._session = aiohttp.ClientSession()
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="reviewer")
            log.info("Reviewer started (interval=%ss, weekly=%s)",
                     self.config.auto_resolve_check_interval_sec,
                     self.config.weekly_review_day)

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.check_resolutions()
                if self._is_weekly_review_due():
                    await self.run_weekly_review()
            except Exception:
                log.exception("reviewer cycle failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self.config.auto_resolve_check_interval_sec,
                )
            except asyncio.TimeoutError:
                continue

    # ---------------- resolution ----------------

    async def check_resolutions(self) -> int:
        """Settle any pending trades whose markets have closed.

        Routes Polymarket trades to Gamma API and Kalshi trades to
        Kalshi REST API. Returns the number of trades closed this pass.
        """
        with _connect(self.db_path) as conn:
            pending = [dict(r) for r in conn.execute(
                "SELECT * FROM trades WHERE outcome = 'pending'"
            ).fetchall()]
        if not pending:
            return 0

        polymarket_pending = [
            p for p in pending
            if (p.get("exchange") or "polymarket") == "polymarket"
        ]
        kalshi_pending = [
            p for p in pending
            if (p.get("exchange") or "polymarket") == "kalshi"
        ]

        closed = 0
        closed += await self._check_polymarket_resolutions(polymarket_pending)
        closed += await self._check_kalshi_resolutions(kalshi_pending)

        try:
            await self._annotate_stop_counterfactuals()
        except Exception:
            log.exception("stop-loss counterfactual annotation failed")

        if closed:
            log.info("Reviewer: closed %d trade(s) this pass", closed)
        return closed

    async def _annotate_stop_counterfactuals(self) -> int:
        """Record what each stopped-out trade WOULD have earned if held.

        Once a stopped market finalizes, annotate the trade with the market's
        actual result and the hold-to-resolution PnL. This is what makes the
        stop threshold tunable from data instead of vibes: the 2026-07-15
        manual audit showed the 0.50 stop saved money (-$81 realized vs -$110
        held) while the tightened 0.35 stop cost money (5/10 stopped markets
        went on to resolve YES) — this pass keeps that ledger current
        automatically. Kalshi only; runs once per trade (skips rows already
        annotated).
        """
        if self._session is None:
            return 0
        with _connect(self.db_path) as conn:
            lost = [dict(r) for r in conn.execute(
                "SELECT * FROM trades WHERE outcome = 'lost' AND exchange = 'kalshi'"
            ).fetchall()]

        todo: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for t in lost:
            try:
                meta = json.loads(t.get("metadata_json") or "{}")
            except json.JSONDecodeError:
                continue
            if meta.get("exit_reason") != "stop_loss":
                continue
            if "stop_counterfactual" in meta:
                continue
            todo.append((t, meta))
        if not todo:
            return 0

        states: dict[str, dict[str, Any]] = {}
        for ticker in sorted({t["market_id"] for t, _ in todo}):
            state = await self._fetch_kalshi_market_state(ticker)
            if state is not None:
                states[ticker] = state

        from core.logger import _sanitize_for_json
        annotated = 0
        for t, meta in todo:
            state = states.get(t["market_id"])
            if state is None:
                continue
            if str(state.get("status") or "").lower() != "finalized":
                continue
            result = str(state.get("result") or "").lower()
            if result not in ("yes", "no"):
                continue
            shares = float(t.get("shares") or 0.0)
            size = float(t.get("size_usd") or 0.0)
            settle = 1.0 if result == "yes" else 0.0
            hold_pnl = shares * settle - size
            stop_pnl = float(t.get("pnl") or 0.0)
            meta["stop_counterfactual"] = {
                "market_result": result,
                "hold_pnl": round(hold_pnl, 4),
                # Positive = the stop saved money vs holding; negative = the
                # stop cut a position that would have done better held.
                "stop_saved_usd": round(stop_pnl - hold_pnl, 4),
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }
            with _connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE trades SET metadata_json = ? WHERE id = ?",
                    (json.dumps(_sanitize_for_json(meta), default=str), t["id"]),
                )
                conn.commit()
            annotated += 1
            log.info(
                "Stop counterfactual trade #%d %s: market resolved %s — "
                "stop pnl $%+.2f vs hold $%+.2f (%s $%.2f)",
                t["id"], t["market_id"], result.upper(), stop_pnl, hold_pnl,
                "saved" if stop_pnl >= hold_pnl else "cost",
                abs(stop_pnl - hold_pnl),
            )
        return annotated

    async def _check_polymarket_resolutions(self, pending: list[dict[str, Any]]) -> int:
        if not pending:
            return 0
        # De-dup by market_id so we hit the API once per unique market.
        unique_market_ids = sorted({p["market_id"] for p in pending})
        market_states: dict[str, dict[str, Any]] = {}
        for mid in unique_market_ids:
            state = await self._fetch_market_state(mid)
            if state is not None:
                market_states[mid] = state

        closed = 0
        for trade in pending:
            state = market_states.get(trade["market_id"])
            if state is None:
                continue
            outcome = self._infer_outcome(trade, state)
            if outcome is None:
                continue  # not yet resolved or ambiguous
            won, settled_price = outcome
            pnl = self._compute_pnl(trade, won, settled_price)
            self._record_resolution(trade, won, pnl, settled_price, state)
            self._maybe_flag_mismatch(trade, state)
            closed += 1
        return closed

    async def _check_kalshi_resolutions(self, pending: list[dict[str, Any]]) -> int:
        """Check Kalshi pending trades for resolution via Kalshi REST API.

        Market status is public (no auth required). Falls back gracefully if
        the session is unavailable or the API is unreachable.
        """
        if not pending:
            return 0
        if self._session is None:
            log.warning(
                "Reviewer: %d Kalshi trade(s) pending but no HTTP session — skipping",
                len(pending),
            )
            return 0

        unique_tickers = sorted({p["market_id"] for p in pending})
        market_states: dict[str, dict[str, Any]] = {}
        for ticker in unique_tickers:
            state = await self._fetch_kalshi_market_state(ticker)
            if state is not None:
                market_states[ticker] = state

        closed = 0
        for trade in pending:
            state = market_states.get(trade["market_id"])
            if state is None:
                continue
            outcome = self._infer_kalshi_outcome(trade, state)
            if outcome is None:
                continue
            won, settled_price = outcome
            pnl = self._compute_pnl(trade, won, settled_price)
            self._record_resolution(trade, won, pnl, settled_price, state)
            closed += 1
        return closed

    async def _fetch_kalshi_market_state(self, ticker: str) -> dict[str, Any] | None:
        """Fetch Kalshi market state (no auth required for market data)."""
        assert self._session is not None
        url = f"{self.config.kalshi_base_url}/markets/{ticker}"
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 400:
                    log.debug("Kalshi market fetch HTTP %d for %s", resp.status, ticker)
                    return None
                data = await resp.json(content_type=None)
                # Kalshi wraps the market in a "market" key
                if isinstance(data, dict):
                    return data.get("market") or data
                return None
        except aiohttp.ClientError as exc:
            log.debug("Kalshi market fetch failed for %s: %s", ticker, exc)
            return None

    @staticmethod
    def _infer_kalshi_outcome(
        trade: dict[str, Any], state: dict[str, Any]
    ) -> tuple[bool, float] | None:
        """Return (won, settled_yes_price) or None if not yet finalized.

        Kalshi markets resolve with status="finalized" and result="yes"|"no".
        """
        if str(state.get("status") or "").lower() != "finalized":
            return None
        result = str(state.get("result") or "").lower()
        if result not in ("yes", "no"):
            return None
        yes_won = result == "yes"
        settled_price = 1.0 if yes_won else 0.0
        direction = trade.get("direction", "YES")
        if direction == "YES":
            won = yes_won
        else:
            won = not yes_won
        return won, settled_price

    async def _fetch_market_state(self, market_id: str) -> dict[str, Any] | None:
        assert self._session is not None
        url = f"{GAMMA_BASE}/markets/{market_id}"
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 400:
                    return None
                data = await resp.json()
                if isinstance(data, dict):
                    return data
                if isinstance(data, list) and data:
                    return data[0]
                return None
        except aiohttp.ClientError as e:
            log.debug("Gamma fetch failed for %s: %s", market_id, e)
            return None

    @staticmethod
    def _infer_outcome(trade: dict[str, Any], state: dict[str, Any]) -> tuple[bool, float] | None:
        """Return (won, settled_price_for_yes) or None if still pending.

        Polymarket markets close, then resolve. We only act on resolved
        markets (closed=true AND outcome prices collapsed to 0/1).
        """
        if not bool(state.get("closed")):
            return None
        prices_raw = state.get("outcomePrices")
        outcomes_raw = state.get("outcomes")
        prices = _maybe_json(prices_raw)
        outcomes = _maybe_json(outcomes_raw)
        if not prices or not outcomes:
            return None
        try:
            yes_idx = next(i for i, o in enumerate(outcomes) if str(o).strip().lower() == "yes")
            yes_price = float(prices[yes_idx])
        except (StopIteration, ValueError, IndexError):
            return None
        # We only treat 0/1 as resolved; anything in between means the
        # market closed but resolution data isn't published yet.
        if yes_price not in (0.0, 1.0):
            return None
        side = trade.get("direction") or "YES"
        if side == "YES":
            won = yes_price >= 0.5
        else:
            won = yes_price < 0.5
        return won, yes_price

    @staticmethod
    def _compute_pnl(trade: dict[str, Any], won: bool, settled_price_for_yes: float) -> float:
        """Settled binary payoff. Each share pays $1 if outcome matches direction, else $0."""
        size = float(trade.get("size_usd") or 0.0)
        entry = float(trade.get("entry_price") or 0.0)
        if entry <= 0:
            return 0.0
        shares = size / entry
        side = trade.get("direction") or "YES"
        if side == "YES":
            settle = 1.0 if settled_price_for_yes >= 0.5 else 0.0
        else:
            settle = 0.0 if settled_price_for_yes >= 0.5 else 1.0
        pnl = shares * settle - size
        # A partial stop-loss exit shrinks the row to the remainder and banks
        # the realized chunk in metadata (TradeJournal.apply_partial_exit);
        # fold it back in so the trade's total PnL is complete.
        try:
            meta = json.loads(trade.get("metadata_json") or "{}")
            pnl += float(meta.get("partial_exit_pnl") or 0.0)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
        return pnl

    def _record_resolution(
        self,
        trade: dict[str, Any],
        won: bool,
        pnl: float,
        settled_price: float,
        state: dict[str, Any],
    ) -> None:
        outcome_str = "won" if won else "lost"
        try:
            meta = json.loads(trade.get("metadata_json") or "{}")
        except json.JSONDecodeError:
            # Malformed metadata in the journal (legacy rows where ±inf
            # leaked in). Don't fail the resolution write — start fresh
            # with just the resolution fields so future reads succeed.
            log.warning(
                "trade #%d has malformed metadata_json; resetting on resolution",
                trade["id"],
            )
            meta = {}
        meta["resolved_yes_price"] = settled_price
        meta["resolved_at"] = datetime.now(timezone.utc).isoformat()
        meta["resolution_source"] = "gamma"
        from core.logger import _sanitize_for_json
        with _connect(self.db_path) as conn:
            conn.execute(
                "UPDATE trades SET outcome=?, pnl=?, metadata_json=? WHERE id=?",
                (outcome_str, pnl, json.dumps(_sanitize_for_json(meta), default=str), trade["id"]),
            )
            conn.commit()
        log.info(
            "Resolved trade #%d: %s @ %.3f -> %s pnl=%+0.2f",
            trade["id"], trade["market_id"], settled_price, outcome_str, pnl,
        )

    def _maybe_flag_mismatch(self, trade: dict[str, Any], state: dict[str, Any]) -> None:
        meta = json.loads(trade.get("metadata_json") or "{}")
        forecast_temp = meta.get("forecast_temp")
        if forecast_temp is None:
            return
        # We don't have an actual reading from the resolution source here
        # (would require Wunderground API access). The flag is conservative:
        # if the trade lost AND the predicted prob was high, that's likely a
        # source mismatch worth investigating.
        prob = meta.get("model_prob")
        if prob is not None and trade.get("outcome") == "lost" and float(prob) > 0.6:
            self._emit("error", {
                "source": "reviewer",
                "message": (
                    f"Possible resolution mismatch: trade #{trade['id']} on "
                    f"{meta.get('city', '?')} predicted p={float(prob):.2f} but lost. "
                    f"Forecast={forecast_temp}{'°F' if meta.get('unit')=='fahrenheit' else '°C'}. "
                    "Verify the resolution station."
                ),
                "count": 1,
            })

    # ---------------- weekly review ----------------

    def _is_weekly_review_due(self) -> bool:
        weekday_names = ("monday", "tuesday", "wednesday", "thursday",
                         "friday", "saturday", "sunday")
        try:
            target_idx = weekday_names.index(self.config.weekly_review_day)
        except ValueError:
            return False
        now = datetime.now(timezone.utc)
        if now.weekday() != target_idx:
            return False
        if self._last_weekly_review_at is None:
            return True
        return self._last_weekly_review_at.date() < now.date()

    async def run_weekly_review(self) -> dict[str, Any]:
        summary = self.compute_review_metrics(period="weekly", since_days=7, persist=True)
        self._last_weekly_review_at = datetime.now(timezone.utc)
        self._emit("daily_summary", {  # reuse the channel; payload says weekly_review
            "realized_pnl_usd": summary.get("total_pnl", 0.0),
            "trades_today": summary.get("n_trades", 0),
            "win_rate": summary.get("win_rate", 0.0),
            "open_positions": 0,
            "weekly_review": True,
        })
        for note in summary.get("flags", []):
            log.warning("Weekly-review flag: %s", note)
        return summary

    def compute_review_metrics(
        self,
        *,
        period: str = "manual",
        since_days: int | None = None,
        persist: bool = False,
    ) -> dict[str, Any]:
        cutoff = (
            (datetime.now(timezone.utc) - timedelta(days=since_days)).isoformat()
            if since_days else None
        )
        # Filter by resolved_at for closed trades, by entry timestamp for
        # pending ones — so a trade resolved in-window but entered earlier
        # still contributes to the window's win_rate and PnL. The
        # json_valid() guard is for malformed metadata_json (legacy rows
        # with ±Infinity); without it json_extract raises and the entire
        # SELECT fails.
        with _connect(self.db_path) as conn:
            if cutoff:
                rows = [dict(r) for r in conn.execute(
                    """
                    SELECT * FROM trades
                    WHERE
                      (outcome IN ('won','lost')
                        AND COALESCE(
                              CASE WHEN json_valid(metadata_json)
                                   THEN json_extract(metadata_json, '$.resolved_at')
                                   ELSE NULL END,
                              timestamp
                            ) >= ?)
                      OR (outcome = 'pending' AND timestamp >= ?)
                    """,
                    (cutoff, cutoff),
                ).fetchall()]
            else:
                rows = [dict(r) for r in conn.execute("SELECT * FROM trades").fetchall()]

        closed = [r for r in rows if r["outcome"] in ("won", "lost")]
        flags: list[str] = []

        # By-bucket aggregates.
        bucket_metrics: list[tuple[str, str, dict[str, Any]]] = []
        for kind, key_fn in (
            ("strategy",     lambda r: r["strategy"] or "unknown"),
            ("city",         lambda r: _meta(r).get("city", "unknown")),
            ("volume_tier",  lambda r: _meta(r).get("volume_tier", "default")),
        ):
            groups: dict[str, list[dict[str, Any]]] = {}
            for r in rows:
                groups.setdefault(key_fn(r), []).append(r)
            for key, items in groups.items():
                m = _bucket_summary(items)
                bucket_metrics.append((kind, str(key), m))
                bias = m["bias_pct"]
                if (m["n_closed"] >= 5 and abs(bias) >= self.config.bias_flag_threshold_pct):
                    direction = "overestimate" if bias > 0 else "underestimate"
                    flags.append(
                        f"{kind}={key}: predicted-vs-realized gap {bias:+.1%} "
                        f"({direction}) over {m['n_closed']} closed trade(s)"
                    )

        # Stop-loss counterfactual bucket: across annotated stop-outs in the
        # window, how much did stopping save (or cost) vs holding, and how
        # often did the stopped market go on to resolve YES? A persistently
        # negative save with a high cut-winner rate says the threshold is
        # inside normal churn and should loosen.
        cf_rows = [
            (r, _meta(r)["stop_counterfactual"]) for r in closed
            if isinstance(_meta(r).get("stop_counterfactual"), dict)
        ]
        if cf_rows:
            n_cf = len(cf_rows)
            saved = sum(float(cf.get("stop_saved_usd") or 0.0) for _, cf in cf_rows)
            cut_winners = sum(1 for _, cf in cf_rows if cf.get("market_result") == "yes")
            bucket_metrics.append(("stop_loss", "counterfactual", {
                "n_closed": n_cf,
                "win_rate": cut_winners / n_cf,   # rate of stops that cut an eventual winner
                "avg_edge_in": None,
                "avg_realized": None,
                "bias_pct": None,
                "total_pnl": saved,               # net $ saved by stopping vs holding
            }))
            if saved < 0:
                flags.append(
                    f"stop_loss counterfactual: stopping COST ${-saved:.2f} vs holding "
                    f"over {n_cf} stop(s); {cut_winners} cut eventual winners — "
                    "threshold may be too tight"
                )

        if persist:
            self._persist_metrics(period=period, bucket_metrics=bucket_metrics)

        overall = _bucket_summary(rows)
        return {
            "n_trades": overall["n"],
            "n_closed": overall["n_closed"],
            "win_rate": overall["win_rate"] or 0.0,
            "total_pnl": overall["total_pnl"],
            "avg_edge_in": overall["avg_edge_in"],
            "avg_realized": overall["avg_realized"],
            "by_bucket": bucket_metrics,
            "flags": flags,
        }

    def _persist_metrics(
        self,
        *,
        period: str,
        bucket_metrics: list[tuple[str, str, dict[str, Any]]],
    ) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        with _connect(self.db_path) as conn:
            for kind, key, m in bucket_metrics:
                conn.execute(
                    """INSERT INTO review_metrics
                       (timestamp, period, bucket_kind, bucket_key, n_trades, win_rate,
                        avg_edge_in, avg_realized, bias_pct, total_pnl, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ts, period, kind, key, m["n_closed"],
                        m["win_rate"], m["avg_edge_in"], m["avg_realized"],
                        m["bias_pct"], m["total_pnl"], "",
                    ),
                )
            conn.commit()

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        if self._alert_hook is None:
            return
        try:
            self._alert_hook(event_type, payload)
        except Exception:
            log.debug("alert hook raised", exc_info=True)


# ----- helpers -----

def _bucket_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [r for r in items if r["outcome"] in ("won", "lost")]
    wins = sum(1 for r in closed if r["outcome"] == "won")
    realized: list[float] = []
    edges_in: list[float] = []
    for r in items:
        edges_in.append(float(r["edge_at_entry"] or 0.0))
    for r in closed:
        size = float(r["size_usd"] or 0.0)
        if size > 0:
            realized.append(float(r["pnl"] or 0.0) / size)
    avg_edge_in = statistics.fmean(edges_in) if edges_in else 0.0
    avg_realized = statistics.fmean(realized) if realized else 0.0
    return {
        "n": len(items),
        "n_closed": len(closed),
        "win_rate": (wins / len(closed)) if closed else None,
        "total_pnl": sum(float(r["pnl"] or 0.0) for r in closed),
        "avg_edge_in": avg_edge_in,
        "avg_realized": avg_realized,
        # 'bias' = predicted edge minus realized return, where positive means
        # the strategy systematically overestimates.
        "bias_pct": avg_edge_in - avg_realized,
    }


def _meta(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("metadata_json") or "{}"
    try:
        return json.loads(raw) if isinstance(raw, str) else dict(raw)
    except json.JSONDecodeError:
        return {}


def _maybe_json(v: Any) -> list[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            return list(json.loads(v))
        except (json.JSONDecodeError, TypeError):
            return []
    return []
