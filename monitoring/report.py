"""Comprehensive on-demand trade report.

Single command for ground-truth state of the journal. Designed to be the
answer to "is the daily summary lying to me?" — it reads the same SQLite
journal but presents lifetime + windowed views side-by-side and surfaces
calibration metrics that the daily summary doesn't.

Run:
    python -m monitoring.report                       # lifetime + today
    python -m monitoring.report --since 7d            # lifetime + 7d
    python -m monitoring.report --since 24h           # lifetime + last 24h
    python -m monitoring.report --recent 30           # last N settled trades
    python -m monitoring.report --json                # machine-readable
    python -m monitoring.report --db data/foo.db      # alternate DB

What's in here that perf_report.py is not:
  * Lifetime + windowed views in one shot — small-N noise vs accumulated
    signal at a glance.
  * `Implied%` (avg entry-price-implied win prob) and `Calib` (actual −
    implied) — the headline diagnostic for "do I have edge?".
  * `Brier` — mean squared error of the entry price as a probability
    estimate. 0.25 is the no-information baseline for binary markets.
  * `ROI%` — realized_pnl / deployed_in_resolved_trades. Win rate alone
    can mislead (e.g., lazy_70 wins 70% by construction; the question is
    whether $-weighted return is positive).
  * Recent settled trades log + open positions log inline.

All resolution-time filters use `metadata.resolved_at` (with timestamp
fallback), matching the fix applied across perf_report / journal / reviewer.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"


@dataclass
class StrategyView:
    """Per-strategy stats for one window. Open-position fields are
    lifetime regardless of the window — they always represent live
    exposure."""
    strategy: str
    # Entries opened in window.
    trades: int = 0
    deployed_usd: float = 0.0
    avg_edge_at_entry: float = 0.0
    # Resolutions in window.
    wins: int = 0
    losses: int = 0
    realized_pnl_usd: float = 0.0
    deployed_in_resolved_usd: float = 0.0   # sum(size_usd) for trades resolved in window
    sum_implied_prob: float = 0.0           # sum of implied-prob at entry, for resolved-in-window
    sum_brier: float = 0.0                  # sum of (implied_prob - actual)^2
    # Open positions (lifetime).
    open_positions: int = 0
    open_exposure_usd: float = 0.0

    @property
    def resolved(self) -> int:
        return self.wins + self.losses

    @property
    def win_rate(self) -> float | None:
        return (self.wins / self.resolved) if self.resolved else None

    @property
    def implied_win_rate(self) -> float | None:
        """Mean entry-price-implied probability across resolved trades."""
        return (self.sum_implied_prob / self.resolved) if self.resolved else None

    @property
    def calibration_delta(self) -> float | None:
        """Actual win rate − implied win rate. >0 = beating the market."""
        wr = self.win_rate
        imp = self.implied_win_rate
        return (wr - imp) if (wr is not None and imp is not None) else None

    @property
    def brier(self) -> float | None:
        """Mean Brier score over resolved trades. Lower is better;
        0.25 is the no-information baseline."""
        return (self.sum_brier / self.resolved) if self.resolved else None

    @property
    def roi_pct(self) -> float | None:
        """realized_pnl / deployed-in-resolved-trades. Sign and magnitude
        of edge in $ terms — preferred over win rate alone."""
        if self.deployed_in_resolved_usd <= 0:
            return None
        return self.realized_pnl_usd / self.deployed_in_resolved_usd


@dataclass
class ReportSnapshot:
    """Top-level report payload. `lifetime` and `window` are parallel
    StrategyView lists keyed by strategy name."""
    generated_at: datetime
    db_path: Path
    window_label: str
    lifetime: list[StrategyView] = field(default_factory=list)
    window: list[StrategyView] = field(default_factory=list)
    recent_settled: list[dict[str, Any]] = field(default_factory=list)
    open_positions: list[dict[str, Any]] = field(default_factory=list)


# ---------- DB helpers ----------

@contextmanager
def _connect(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _parse_since(s: str) -> datetime:
    """Accept '7d', '12h', '1h', '30m', or 'today' (UTC day-start)."""
    s = s.strip().lower()
    now = datetime.now(timezone.utc)
    if s in ("today", ""):
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    m = re.fullmatch(r"(\d+)([dhm])", s)
    if not m:
        raise SystemExit(f"--since: expected '7d', '12h', '30m', or 'today', got {s!r}")
    n = int(m.group(1))
    unit = m.group(2)
    delta = {"d": timedelta(days=n), "h": timedelta(hours=n), "m": timedelta(minutes=n)}[unit]
    return now - delta


def _resolve_db_path(cli_db: str | None) -> Path:
    if cli_db:
        return Path(cli_db).resolve()
    if DEFAULT_CONFIG.exists():
        try:
            cfg = yaml.safe_load(DEFAULT_CONFIG.read_text()) or {}
            db_rel = (cfg.get("logger") or {}).get("db_path") or "data/trade_journal.db"
        except Exception:
            db_rel = "data/trade_journal.db"
    else:
        db_rel = "data/trade_journal.db"
    return (PROJECT_ROOT / db_rel).resolve()


def _implied_prob(direction: str, entry_price: float) -> float:
    """Probability the trade pays $1 implied by the entry price.
    For YES it's the price; for NO it's 1−price."""
    p = entry_price if (direction or "YES") == "YES" else 1.0 - entry_price
    return max(0.0, min(1.0, p))


def _format_bucket_with_unit(bucket: str, unit: str) -> str:
    """Render `bucket` with a unit suffix, but don't double up if the
    bucket label already carries one (different strategies stash the
    label in different shapes — lazy_weather embeds 'X°F' / 'X°C',
    weather stores just 'X-Y'). Returns empty string for empty bucket."""
    if not bucket:
        return ""
    if "°" in bucket:
        return bucket
    unit_short = "°F" if unit.startswith("f") else ("°C" if unit.startswith("c") else "")
    return f"{bucket}{unit_short}"


# ---------- Collection ----------

def _collect_view(
    conn: sqlite3.Connection,
    *,
    since: datetime | None,
) -> dict[str, StrategyView]:
    """Build per-strategy views for one window (or lifetime when since=None).

    Same filter discipline as perf_report.collect: entries by `timestamp`,
    resolutions by `metadata.resolved_at` with a fallback to `timestamp`.
    """
    by_strategy: dict[str, StrategyView] = {}

    for row in conn.execute("SELECT DISTINCT strategy FROM trades"):
        by_strategy.setdefault(row["strategy"], StrategyView(strategy=row["strategy"]))

    # Entries.
    if since is None:
        entry_filter, entry_params = "", ()
    else:
        entry_filter = "WHERE timestamp >= ?"
        entry_params = (since.isoformat(),)

    for row in conn.execute(
        f"""
        SELECT strategy,
               COUNT(*) AS trades,
               COALESCE(SUM(size_usd), 0.0) AS deployed_usd,
               COALESCE(AVG(edge_at_entry), 0.0) AS avg_edge
        FROM trades {entry_filter}
        GROUP BY strategy
        """,
        entry_params,
    ).fetchall():
        s = by_strategy.setdefault(row["strategy"], StrategyView(strategy=row["strategy"]))
        s.trades = int(row["trades"] or 0)
        s.deployed_usd = float(row["deployed_usd"] or 0.0)
        s.avg_edge_at_entry = float(row["avg_edge"] or 0.0)

    # Resolutions — pull row-level so we can compute Brier and implied prob
    # from entry_price + direction. Aggregating in SQL would lose that.
    if since is None:
        res_sql = "SELECT * FROM trades WHERE outcome IN ('won','lost')"
        res_params: tuple = ()
    else:
        # json_valid() guard — malformed metadata_json (legacy rows with
        # ±Infinity from temperature edge buckets) would otherwise raise
        # "malformed JSON" and abort the query.
        res_sql = (
            "SELECT * FROM trades "
            "WHERE outcome IN ('won','lost') "
            "AND COALESCE("
            "      CASE WHEN json_valid(metadata_json) "
            "           THEN json_extract(metadata_json, '$.resolved_at') "
            "           ELSE NULL END,"
            "      timestamp"
            "    ) >= ?"
        )
        res_params = (since.isoformat(),)

    for row in conn.execute(res_sql, res_params).fetchall():
        s = by_strategy.setdefault(row["strategy"], StrategyView(strategy=row["strategy"]))
        outcome = row["outcome"]
        if outcome == "won":
            s.wins += 1
        else:
            s.losses += 1
        s.realized_pnl_usd += float(row["pnl"] or 0.0)
        s.deployed_in_resolved_usd += float(row["size_usd"] or 0.0)
        p = _implied_prob(row["direction"] or "YES", float(row["entry_price"] or 0.0))
        s.sum_implied_prob += p
        actual = 1.0 if outcome == "won" else 0.0
        s.sum_brier += (p - actual) ** 2

    # Open positions (lifetime).
    for row in conn.execute(
        """
        SELECT strategy,
               COUNT(*) AS open_count,
               COALESCE(SUM(size_usd), 0.0) AS exposure_usd
        FROM trades WHERE outcome = 'pending'
        GROUP BY strategy
        """,
    ).fetchall():
        s = by_strategy.setdefault(row["strategy"], StrategyView(strategy=row["strategy"]))
        s.open_positions = int(row["open_count"] or 0)
        s.open_exposure_usd = float(row["exposure_usd"] or 0.0)

    return by_strategy


# SQL fragment: extract a JSON path safely, returning NULL when
# metadata_json is not valid JSON. Inline this in SELECT / ORDER BY /
# WHERE so a single bad row doesn't abort the whole query.
def _safe_extract(path: str) -> str:
    return (
        f"CASE WHEN json_valid(metadata_json) "
        f"     THEN json_extract(metadata_json, '{path}') "
        f"     ELSE NULL END"
    )


def _collect_recent_settled(conn: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT id, strategy, market_id, market_question, direction,
               entry_price, size_usd, outcome, pnl, timestamp,
               {_safe_extract('$.resolved_at')} AS resolved_at,
               {_safe_extract('$.city')}        AS city,
               {_safe_extract('$.bucket')}      AS bucket,
               {_safe_extract('$.unit')}        AS unit
        FROM trades
        WHERE outcome IN ('won','lost')
        ORDER BY COALESCE({_safe_extract('$.resolved_at')}, timestamp) DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def _collect_open_positions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT id, strategy, market_id, direction, entry_price, size_usd,
               edge_at_entry, timestamp,
               {_safe_extract('$.city')}    AS city,
               {_safe_extract('$.bucket')}  AS bucket,
               {_safe_extract('$.unit')}    AS unit,
               {_safe_extract('$.end_utc')} AS end_utc
        FROM trades
        WHERE outcome = 'pending'
        ORDER BY timestamp ASC
        """,
    ).fetchall()
    return [dict(r) for r in rows]


def collect(
    db_path: Path,
    *,
    window_since: datetime,
    recent_limit: int = 30,
) -> ReportSnapshot:
    if not db_path.exists():
        raise SystemExit(f"trade journal not found at {db_path}")

    with _connect(db_path) as conn:
        lifetime_by = _collect_view(conn, since=None)
        window_by = _collect_view(conn, since=window_since)
        recent = _collect_recent_settled(conn, limit=recent_limit)
        opens = _collect_open_positions(conn)

    lifetime = sorted(lifetime_by.values(), key=lambda v: v.strategy)
    window = sorted(window_by.values(), key=lambda v: v.strategy)
    return ReportSnapshot(
        generated_at=datetime.now(timezone.utc),
        db_path=db_path,
        window_label=window_since.isoformat(),
        lifetime=lifetime,
        window=window,
        recent_settled=recent,
        open_positions=opens,
    )


# ---------- Rendering ----------

_HEADERS = [
    "Strategy", "Trades", "Open", "Exposure", "Deployed",
    "Realized", "ROI%", "WinRate", "Implied%", "Calib", "Brier", "AvgEdge",
]


def _fmt_view_row(v: StrategyView) -> list[str]:
    wr = f"{v.win_rate:.0%}" if v.win_rate is not None else "  -"
    imp = f"{v.implied_win_rate:.0%}" if v.implied_win_rate is not None else "  -"
    calib = f"{v.calibration_delta:+.0%}" if v.calibration_delta is not None else "  -"
    brier = f"{v.brier:.3f}" if v.brier is not None else "    -"
    roi = f"{v.roi_pct:+.1%}" if v.roi_pct is not None else "    -"
    avg_edge = f"{v.avg_edge_at_entry:.3f}" if v.trades else "    -"
    return [
        v.strategy,
        f"{v.trades:d}",
        f"{v.open_positions:d}",
        f"${v.open_exposure_usd:,.2f}",
        f"${v.deployed_usd:,.2f}",
        f"{'+' if v.realized_pnl_usd >= 0 else ''}${v.realized_pnl_usd:,.2f}",
        roi, wr, imp, calib, brier, avg_edge,
    ]


def _totals(views: list[StrategyView]) -> StrategyView:
    t = StrategyView(strategy="TOTAL")
    for v in views:
        t.trades += v.trades
        t.deployed_usd += v.deployed_usd
        t.open_positions += v.open_positions
        t.open_exposure_usd += v.open_exposure_usd
        t.wins += v.wins
        t.losses += v.losses
        t.realized_pnl_usd += v.realized_pnl_usd
        t.deployed_in_resolved_usd += v.deployed_in_resolved_usd
        t.sum_implied_prob += v.sum_implied_prob
        t.sum_brier += v.sum_brier
    # avg_edge_at_entry on totals isn't meaningful — leave at 0.
    return t


def _render_table(title: str, views: list[StrategyView]) -> str:
    rows: list[list[str]] = [_fmt_view_row(v) for v in views]
    rows.append(_fmt_view_row(_totals(views)))
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(_HEADERS)]

    def line(parts: list[str]) -> str:
        return "  ".join(p.ljust(w) for p, w in zip(parts, widths))

    out = [title, "", line(_HEADERS), line(["-" * w for w in widths])]
    for r in rows[:-1]:
        out.append(line(r))
    out.append(line(["-" * w for w in widths]))
    out.append(line(rows[-1]))
    return "\n".join(out)


def _render_recent(rows: list[dict[str, Any]], *, limit_for_header: int) -> str:
    if not rows:
        return f"Recent settled trades (last {limit_for_header}): (none)"
    out = [f"Recent settled trades (last {min(limit_for_header, len(rows))}):", ""]
    for r in rows:
        outcome = r.get("outcome") or "?"
        icon = "✅" if outcome == "won" else "❌"
        city = r.get("city") or ""
        bucket = r.get("bucket") or ""
        bucket_str = _format_bucket_with_unit(bucket, r.get("unit") or "")
        label = " ".join(p for p in [city, bucket_str] if p) \
                or (str(r.get("market_id") or "")[:16])
        entry = float(r.get("entry_price") or 0.0)
        size = float(r.get("size_usd") or 0.0)
        pnl = float(r.get("pnl") or 0.0)
        resolved = (r.get("resolved_at") or r.get("timestamp") or "")[:19]
        out.append(
            f"  {icon} #{r['id']:<4} {r.get('strategy') or '?':<22} "
            f"{(r.get('direction') or 'YES'):<3} {label:<24} "
            f"@ {entry:.3f}  size ${size:>6,.2f}  "
            f"pnl {'+' if pnl >= 0 else ''}${pnl:>7,.2f}  "
            f"resolved {resolved}"
        )
    return "\n".join(out)


def _render_open(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Open positions: (none)"
    out = [f"Open positions ({len(rows)}):", ""]
    for r in rows:
        city = r.get("city") or ""
        bucket = r.get("bucket") or ""
        bucket_str = _format_bucket_with_unit(bucket, r.get("unit") or "")
        label = " ".join(p for p in [city, bucket_str] if p) \
                or (str(r.get("market_id") or "")[:16])
        entry = float(r.get("entry_price") or 0.0)
        size = float(r.get("size_usd") or 0.0)
        edge = float(r.get("edge_at_entry") or 0.0)
        end_utc = (r.get("end_utc") or "")[:19] or "?"
        opened = (r.get("timestamp") or "")[:19]
        out.append(
            f"  · #{r['id']:<4} {r.get('strategy') or '?':<22} "
            f"{(r.get('direction') or 'YES'):<3} {label:<24} "
            f"@ {entry:.3f}  size ${size:>6,.2f}  edge {edge:+.3f}  "
            f"opened {opened}  resolves {end_utc}"
        )
    return "\n".join(out)


def render(snapshot: ReportSnapshot, *, window_label: str) -> str:
    parts: list[str] = []
    parts.append(
        f"PolyVane report — generated {snapshot.generated_at.isoformat(timespec='seconds')}\n"
        f"  db: {snapshot.db_path}"
    )
    parts.append("")
    parts.append(_render_table("LIFETIME", snapshot.lifetime))
    parts.append("")
    parts.append(_render_table(f"WINDOW: {window_label}", snapshot.window))
    parts.append("")
    parts.append("Legend:  Implied% = mean entry-price-implied win prob across resolved trades")
    parts.append("         Calib   = WinRate − Implied%   (>0 = beating the market)")
    parts.append("         Brier   = mean (implied_prob − actual)²; 0.25 = no-information baseline")
    parts.append("         ROI%    = realized_pnl ÷ deployed_in_resolved_trades")
    parts.append("")
    parts.append(_render_recent(snapshot.recent_settled, limit_for_header=len(snapshot.recent_settled)))
    parts.append("")
    parts.append(_render_open(snapshot.open_positions))
    parts.append("")
    return "\n".join(parts)


# ---------- JSON ----------

def _view_to_json(v: StrategyView) -> dict[str, Any]:
    return {
        "strategy": v.strategy,
        "trades": v.trades,
        "deployed_usd": round(v.deployed_usd, 2),
        "avg_edge_at_entry": round(v.avg_edge_at_entry, 4),
        "wins": v.wins,
        "losses": v.losses,
        "resolved": v.resolved,
        "win_rate": round(v.win_rate, 4) if v.win_rate is not None else None,
        "implied_win_rate": round(v.implied_win_rate, 4) if v.implied_win_rate is not None else None,
        "calibration_delta": round(v.calibration_delta, 4) if v.calibration_delta is not None else None,
        "brier": round(v.brier, 4) if v.brier is not None else None,
        "realized_pnl_usd": round(v.realized_pnl_usd, 2),
        "deployed_in_resolved_usd": round(v.deployed_in_resolved_usd, 2),
        "roi_pct": round(v.roi_pct, 4) if v.roi_pct is not None else None,
        "open_positions": v.open_positions,
        "open_exposure_usd": round(v.open_exposure_usd, 2),
    }


def to_json(snapshot: ReportSnapshot, *, window_label: str) -> dict[str, Any]:
    return {
        "generated_at": snapshot.generated_at.isoformat(),
        "db_path": str(snapshot.db_path),
        "window": window_label,
        "lifetime": [_view_to_json(v) for v in snapshot.lifetime],
        "window_view": [_view_to_json(v) for v in snapshot.window],
        "recent_settled": snapshot.recent_settled,
        "open_positions": snapshot.open_positions,
    }


# ---------- CLI ----------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Comprehensive PolyVane trade report.")
    p.add_argument(
        "--since", default="today",
        help="Window: 'today' (UTC day-start), '7d', '12h', '30m'. Default 'today'.",
    )
    p.add_argument("--recent", type=int, default=30,
                   help="Show this many most-recent settled trades. Default 30.")
    p.add_argument("--db", default=None,
                   help="Path to trade journal SQLite (default: from config).")
    p.add_argument("--json", action="store_true",
                   help="Emit JSON instead of a human-readable report.")
    args = p.parse_args(argv)

    db = _resolve_db_path(args.db)
    since = _parse_since(args.since)
    label = args.since if args.since != "today" else "today (UTC)"

    snapshot = collect(db, window_since=since, recent_limit=args.recent)

    if args.json:
        sys.stdout.write(json.dumps(to_json(snapshot, window_label=label), indent=2, default=str) + "\n")
    else:
        sys.stdout.write(render(snapshot, window_label=label) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
