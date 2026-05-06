"""CLI dashboard: open positions, P&L, win rate, per-strategy + per-city + per-tier breakdowns.

Run standalone:
    python -m monitoring.dashboard
    python -m monitoring.dashboard --refresh 30
    python -m monitoring.dashboard --once

Reads `config/config.yaml` for the journal path and `monitoring.dashboard_refresh_sec`.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@contextmanager
def _connect(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@dataclass
class _Aggregates:
    open_positions: list[dict[str, Any]]
    daily_pnl: float
    weekly_pnl: float
    monthly_pnl: float
    capital_deployed: float
    capital_returned: float
    by_strategy: list[dict[str, Any]]
    by_city: list[dict[str, Any]]
    by_tier: list[dict[str, Any]]
    strategy_health: list[dict[str, Any]]


def collect(db_path: Path, *, daily_loss_limit_usd: float = 50.0) -> _Aggregates:
    now = datetime.now(timezone.utc)
    day = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    week = (now - timedelta(days=7)).isoformat()
    month = (now - timedelta(days=30)).isoformat()

    with _connect(db_path) as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM trades ORDER BY timestamp DESC LIMIT 5000"
        ).fetchall()]

    open_positions = [r for r in rows if r["outcome"] == "pending"]
    closed = [r for r in rows if r["outcome"] in ("won", "lost")]
    daily_pnl = sum(float(r["pnl"] or 0.0) for r in closed if r["timestamp"] >= day)
    weekly_pnl = sum(float(r["pnl"] or 0.0) for r in closed if r["timestamp"] >= week)
    monthly_pnl = sum(float(r["pnl"] or 0.0) for r in closed if r["timestamp"] >= month)
    capital_deployed = sum(float(r["size_usd"] or 0.0) for r in open_positions)
    capital_returned = sum(float(r["size_usd"] or 0.0) + float(r["pnl"] or 0.0)
                           for r in closed)

    # Per-strategy aggregates (only closed trades fold into win rate / edge).
    by_strat = _group_by(rows, lambda r: r["strategy"] or "unknown")
    by_city = _group_by(rows, lambda r: _meta(r).get("city", "—"))
    by_tier = _group_by(rows, lambda r: _meta(r).get("volume_tier", "default"))

    # Strategy-health rows.
    today_count_by_strat: dict[str, int] = defaultdict(int)
    for r in rows:
        if r["timestamp"] >= day:
            today_count_by_strat[r["strategy"] or "unknown"] += 1

    strat_health = []
    for s in sorted(today_count_by_strat.keys() | {row["strategy"] for row in rows}):
        if not s:
            continue
        strat_health.append({
            "strategy": s,
            "trades_today": today_count_by_strat.get(s, 0),
            "drawdown_pct": (-daily_pnl / daily_loss_limit_usd) if daily_loss_limit_usd > 0 else 0.0,
        })

    return _Aggregates(
        open_positions=open_positions,
        daily_pnl=daily_pnl,
        weekly_pnl=weekly_pnl,
        monthly_pnl=monthly_pnl,
        capital_deployed=capital_deployed,
        capital_returned=capital_returned,
        by_strategy=by_strat,
        by_city=by_city,
        by_tier=by_tier,
        strategy_health=strat_health,
    )


def _meta(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("metadata_json") or "{}"
    try:
        return json.loads(raw) if isinstance(raw, str) else dict(raw)
    except json.JSONDecodeError:
        return {}


def _group_by(
    rows: list[dict[str, Any]],
    key_fn,
) -> list[dict[str, Any]]:
    """Compute count, win rate, total PnL, avg edge-at-entry, avg actual-edge per group.

    'actual_edge' = pnl / size_usd (per-trade simple return). Only computed
    over closed trades.
    """
    bucket_trades: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        bucket_trades[key_fn(r)].append(r)

    out: list[dict[str, Any]] = []
    for key, items in bucket_trades.items():
        closed = [r for r in items if r["outcome"] in ("won", "lost")]
        wins = sum(1 for r in closed if r["outcome"] == "won")
        total_pnl = sum(float(r["pnl"] or 0.0) for r in closed)
        avg_edge_in = (
            sum(float(r["edge_at_entry"] or 0.0) for r in items) / len(items)
            if items else 0.0
        )
        avg_realized = 0.0
        if closed:
            realized = []
            for r in closed:
                size = float(r["size_usd"] or 0.0)
                if size > 0:
                    realized.append(float(r["pnl"] or 0.0) / size)
            if realized:
                avg_realized = sum(realized) / len(realized)
        out.append({
            "key": key,
            "n": len(items),
            "n_closed": len(closed),
            "win_rate": (wins / len(closed)) if closed else None,
            "total_pnl": total_pnl,
            "avg_edge_in": avg_edge_in,
            "avg_realized": avg_realized,
        })
    out.sort(key=lambda d: (-(d["n_closed"] or 0), -d["n"]))
    return out


def render(agg: _Aggregates, *, db_path: Path) -> None:
    try:
        from rich.console import Console
        from rich.table import Table
    except ImportError:
        _render_plain(agg, db_path=db_path)
        return

    console = Console()
    console.clear()
    console.rule(f"polyvane — {datetime.now(timezone.utc).isoformat(timespec='seconds')}Z")

    summary = Table(title="Summary", show_header=False, expand=False)
    summary.add_column("metric", style="cyan")
    summary.add_column("value")
    summary.add_row("DB", str(db_path))
    summary.add_row("Open positions", str(len(agg.open_positions)))
    summary.add_row("Daily P&L",   _money(agg.daily_pnl))
    summary.add_row("Weekly P&L",  _money(agg.weekly_pnl))
    summary.add_row("Monthly P&L", _money(agg.monthly_pnl))
    summary.add_row("Capital deployed",  _money(agg.capital_deployed))
    summary.add_row("Capital returned",  _money(agg.capital_returned))
    console.print(summary)

    if agg.strategy_health:
        h = Table(title="Strategy health (today)")
        h.add_column("strategy"); h.add_column("trades today", justify="right")
        h.add_column("daily drawdown vs limit", justify="right")
        for r in agg.strategy_health:
            dd = r["drawdown_pct"]
            dd_str = f"{dd:.0%}" if dd > 0 else "—"
            h.add_row(r["strategy"], str(r["trades_today"]), dd_str)
        console.print(h)

    console.print(_breakdown_table("By strategy", agg.by_strategy, key_label="strategy"))
    console.print(_breakdown_table("By city",     agg.by_city,     key_label="city"))
    console.print(_breakdown_table("By volume tier", agg.by_tier,  key_label="tier"))


def _breakdown_table(title: str, rows: list[dict[str, Any]], *, key_label: str):
    from rich.table import Table
    t = Table(title=title)
    t.add_column(key_label)
    t.add_column("trades", justify="right")
    t.add_column("closed", justify="right")
    t.add_column("win rate", justify="right")
    t.add_column("PnL", justify="right")
    t.add_column("avg edge in", justify="right")
    t.add_column("avg realized", justify="right")
    for r in rows[:30]:
        wr = r["win_rate"]
        t.add_row(
            str(r["key"]),
            str(r["n"]),
            str(r["n_closed"]),
            f"{wr:.0%}" if wr is not None else "—",
            _money(r["total_pnl"]),
            f"{r['avg_edge_in']:+.1%}",
            f"{r['avg_realized']:+.1%}" if r["n_closed"] else "—",
        )
    return t


def _money(v: float) -> str:
    return f"${v:+.2f}"


def _render_plain(agg: _Aggregates, *, db_path: Path) -> None:
    print(f"polyvane dashboard ({datetime.now(timezone.utc).isoformat(timespec='seconds')}Z)")
    print(f"  db: {db_path}")
    print(f"  open: {len(agg.open_positions)}  daily PnL: {_money(agg.daily_pnl)}  "
          f"weekly: {_money(agg.weekly_pnl)}  monthly: {_money(agg.monthly_pnl)}")
    print(f"  capital deployed: {_money(agg.capital_deployed)}  "
          f"returned: {_money(agg.capital_returned)}")
    for label, rows in (("strategy", agg.by_strategy),
                       ("city", agg.by_city),
                       ("tier", agg.by_tier)):
        print(f"\n  by {label}:")
        for r in rows[:30]:
            wr = r["win_rate"]
            print(f"    {r['key']:<24} n={r['n']:<4} closed={r['n_closed']:<4} "
                  f"wr={wr:.0%} " if wr is not None else
                  f"    {r['key']:<24} n={r['n']:<4} closed={r['n_closed']:<4} wr=—  "
                  f"pnl={_money(r['total_pnl'])}")


def _load_config() -> dict[str, Any]:
    cfg_path = PROJECT_ROOT / "config" / "config.yaml"
    if not cfg_path.exists():
        return {}
    with cfg_path.open() as f:
        return yaml.safe_load(f) or {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="polyvane CLI dashboard")
    parser.add_argument("--refresh", type=int, default=None,
                        help="Refresh interval in seconds (overrides config)")
    parser.add_argument("--once", action="store_true",
                        help="Render once and exit (useful for cron / piping)")
    args = parser.parse_args(argv)

    cfg = _load_config()
    db_path = PROJECT_ROOT / cfg.get("logger", {}).get("db_path", "data/trade_journal.db")
    refresh = args.refresh or int(cfg.get("monitoring", {}).get("dashboard_refresh_sec", 30))
    daily_loss_limit = float(cfg.get("risk", {}).get("max_daily_loss_usd", 50.0))

    if not db_path.exists():
        print(f"trade journal not found at {db_path} — start the bot first")
        return 1

    if args.once:
        agg = collect(db_path, daily_loss_limit_usd=daily_loss_limit)
        render(agg, db_path=db_path)
        return 0

    try:
        while True:
            agg = collect(db_path, daily_loss_limit_usd=daily_loss_limit)
            render(agg, db_path=db_path)
            time.sleep(refresh)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
