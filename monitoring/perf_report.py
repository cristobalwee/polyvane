"""Per-strategy performance report.

End-of-day breakdown of which strategies actually traded, what they're
holding, and what they made or lost. Reads directly from the trade journal
SQLite — no live state required, so safe to run against a copied DB.

Run:
    python -m monitoring.perf_report                    # today (UTC)
    python -m monitoring.perf_report --since 7d         # last 7 days
    python -m monitoring.perf_report --since 1h         # last hour
    python -m monitoring.perf_report --all              # lifetime
    python -m monitoring.perf_report --db <path>        # alternate DB

Output columns:
    Strategy        — strategy name
    Trades          — entries opened in window (filter: entry timestamp)
    Open            — positions still pending resolution (lifetime — open
                      positions always represent live exposure regardless of window)
    Exposure ($)    — sum(size_usd) for open positions
    Deployed ($)    — sum(size_usd) for trades opened in window
    Realized ($)    — sum(pnl) for trades RESOLVED in window (filter:
                      metadata.resolved_at, falls back to entry timestamp)
    Win rate        — wins / (wins + losses) for resolved-in-window trades
    Avg edge        — avg(edge_at_entry) over trades opened in window

Why two filters: a trade entered yesterday and resolved today should count
toward today's realized PnL but NOT today's "trades opened" — bucketing
both on entry timestamp made resolved-but-stale trades invisible in the
daily summary table.

`--json` emits the same data as a JSON object (one key per strategy) for
piping into other tools.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"


@dataclass
class StrategyStats:
    strategy: str
    trades: int = 0
    deployed_usd: float = 0.0
    open_positions: int = 0
    open_exposure_usd: float = 0.0
    wins: int = 0
    losses: int = 0
    realized_pnl_usd: float = 0.0
    avg_edge_at_entry: float = 0.0

    @property
    def resolved(self) -> int:
        return self.wins + self.losses

    @property
    def win_rate(self) -> float | None:
        return (self.wins / self.resolved) if self.resolved else None


@contextmanager
def _connect(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _parse_since(s: str) -> datetime:
    """Accept '7d', '12h', '1h', '30m', or 'today'."""
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


def collect(db_path: Path, *, since: datetime | None) -> list[StrategyStats]:
    """Return per-strategy stats. `since=None` → lifetime.

    Two filters are applied at different scopes:
      * Entries (`trades`, `deployed_usd`, `avg_edge`) — by `timestamp`.
      * Resolutions (`wins`, `losses`, `realized`) — by `resolved_at`
        (from metadata_json), falling back to `timestamp` for legacy
        rows that pre-date the field. This is what makes a trade
        opened yesterday but resolved today count toward today's PnL.
    """
    if not db_path.exists():
        raise SystemExit(f"trade journal not found at {db_path}")

    by_strategy: dict[str, StrategyStats] = {}

    with _connect(db_path) as conn:
        # Strategy roster: every strategy that's ever traded, plus open
        # positions (so a strategy with no trades-in-window but still holding
        # a stale position shows up).
        for row in conn.execute("SELECT DISTINCT strategy FROM trades"):
            by_strategy.setdefault(row["strategy"], StrategyStats(strategy=row["strategy"]))

        # Entries opened in window (timestamp filter).
        if since is None:
            entry_filter = ""
            entry_params: tuple = ()
        else:
            entry_filter = "WHERE timestamp >= ?"
            entry_params = (since.isoformat(),)

        entry_rows = conn.execute(
            f"""
            SELECT strategy,
                   COUNT(*) AS trades,
                   COALESCE(SUM(size_usd), 0.0) AS deployed_usd,
                   COALESCE(AVG(edge_at_entry), 0.0) AS avg_edge
            FROM trades
            {entry_filter}
            GROUP BY strategy
            """,
            entry_params,
        ).fetchall()
        for row in entry_rows:
            s = by_strategy.setdefault(row["strategy"], StrategyStats(strategy=row["strategy"]))
            s.trades = int(row["trades"] or 0)
            s.deployed_usd = float(row["deployed_usd"] or 0.0)
            s.avg_edge_at_entry = float(row["avg_edge"] or 0.0)

        # Resolutions in window (resolved_at filter, with timestamp fallback).
        # Lifetime mode skips the window predicate entirely.
        if since is None:
            res_filter = "WHERE outcome IN ('won','lost')"
            res_params: tuple = ()
        else:
            # Guard json_extract with json_valid() — malformed metadata_json
            # (e.g. legacy rows with ±Infinity from temperature edge buckets)
            # would otherwise tear down the whole query.
            res_filter = (
                "WHERE outcome IN ('won','lost') "
                "AND COALESCE("
                "      CASE WHEN json_valid(metadata_json) "
                "           THEN json_extract(metadata_json, '$.resolved_at') "
                "           ELSE NULL END,"
                "      timestamp"
                "    ) >= ?"
            )
            res_params = (since.isoformat(),)

        res_rows = conn.execute(
            f"""
            SELECT strategy,
                   SUM(CASE outcome WHEN 'won'  THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE outcome WHEN 'lost' THEN 1 ELSE 0 END) AS losses,
                   COALESCE(SUM(pnl), 0.0) AS realized
            FROM trades
            {res_filter}
            GROUP BY strategy
            """,
            res_params,
        ).fetchall()
        for row in res_rows:
            s = by_strategy.setdefault(row["strategy"], StrategyStats(strategy=row["strategy"]))
            s.wins = int(row["wins"] or 0)
            s.losses = int(row["losses"] or 0)
            s.realized_pnl_usd = float(row["realized"] or 0.0)

        # Open positions (not window-scoped — currently pending positions
        # that came from any prior trade still represent live exposure).
        open_rows = conn.execute(
            """
            SELECT strategy,
                   COUNT(*) AS open_count,
                   COALESCE(SUM(size_usd), 0.0) AS exposure_usd
            FROM trades
            WHERE outcome = 'pending'
            GROUP BY strategy
            """,
        ).fetchall()
        for row in open_rows:
            s = by_strategy.setdefault(row["strategy"], StrategyStats(strategy=row["strategy"]))
            s.open_positions = int(row["open_count"] or 0)
            s.open_exposure_usd = float(row["exposure_usd"] or 0.0)

    return sorted(by_strategy.values(), key=lambda s: s.strategy)


def render_table(stats: list[StrategyStats], *, window_label: str) -> str:
    """Render an ASCII table without external deps. (`rich` is fine in
    interactive CLI use; we keep this importable from a webhook context.)"""
    if not stats:
        return f"(no trading activity in window: {window_label})\n"

    headers = ["Strategy", "Trades", "Open", "Exposure", "Deployed", "Realized", "WinRate", "AvgEdge"]
    rows: list[list[str]] = []
    totals = StrategyStats(strategy="TOTAL")

    for s in stats:
        wr = f"{s.win_rate:.0%}" if s.win_rate is not None else "  -"
        rows.append([
            s.strategy,
            f"{s.trades:d}",
            f"{s.open_positions:d}",
            f"${s.open_exposure_usd:,.2f}",
            f"${s.deployed_usd:,.2f}",
            f"{'+' if s.realized_pnl_usd >= 0 else ''}${s.realized_pnl_usd:,.2f}",
            wr,
            f"{s.avg_edge_at_entry:.3f}" if s.trades else "    -",
        ])
        totals.trades += s.trades
        totals.deployed_usd += s.deployed_usd
        totals.open_positions += s.open_positions
        totals.open_exposure_usd += s.open_exposure_usd
        totals.wins += s.wins
        totals.losses += s.losses
        totals.realized_pnl_usd += s.realized_pnl_usd

    twr = f"{totals.win_rate:.0%}" if totals.win_rate is not None else "  -"
    rows.append([
        totals.strategy,
        f"{totals.trades:d}",
        f"{totals.open_positions:d}",
        f"${totals.open_exposure_usd:,.2f}",
        f"${totals.deployed_usd:,.2f}",
        f"{'+' if totals.realized_pnl_usd >= 0 else ''}${totals.realized_pnl_usd:,.2f}",
        twr,
        "    -",
    ])

    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]

    def _line(parts: list[str]) -> str:
        return "  ".join(p.ljust(w) for p, w in zip(parts, widths))

    out: list[str] = []
    out.append(f"PolyVane performance — window: {window_label}")
    out.append("")
    out.append(_line(headers))
    out.append(_line(["-" * w for w in widths]))
    for r in rows[:-1]:
        out.append(_line(r))
    out.append(_line(["-" * w for w in widths]))
    out.append(_line(rows[-1]))
    return "\n".join(out) + "\n"


def to_json(stats: list[StrategyStats]) -> dict:
    return {
        s.strategy: {
            "trades": s.trades,
            "open_positions": s.open_positions,
            "open_exposure_usd": round(s.open_exposure_usd, 2),
            "deployed_usd": round(s.deployed_usd, 2),
            "realized_pnl_usd": round(s.realized_pnl_usd, 2),
            "wins": s.wins,
            "losses": s.losses,
            "win_rate": round(s.win_rate, 4) if s.win_rate is not None else None,
            "avg_edge_at_entry": round(s.avg_edge_at_entry, 4),
        }
        for s in stats
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Per-strategy performance report.")
    p.add_argument("--since", default="today",
                   help="Window: 'today' (UTC day-start), '7d', '12h', '30m'. Default 'today'.")
    p.add_argument("--all", action="store_true", help="Lifetime stats (overrides --since).")
    p.add_argument("--db", default=None, help="Path to trade journal SQLite (default: from config).")
    p.add_argument("--json", action="store_true", help="Emit JSON instead of a table.")
    args = p.parse_args(argv)

    db = _resolve_db_path(args.db)

    if args.all:
        since: datetime | None = None
        window_label = "lifetime"
    else:
        since = _parse_since(args.since)
        window_label = args.since if args.since != "today" else "today (UTC)"

    stats = collect(db, since=since)

    if args.json:
        sys.stdout.write(json.dumps(to_json(stats), indent=2) + "\n")
    else:
        sys.stdout.write(render_table(stats, window_label=window_label))
    return 0


if __name__ == "__main__":
    sys.exit(main())
