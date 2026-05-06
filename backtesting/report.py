"""Backtest performance reporting.

Reads a `BacktestResult` and prints (and optionally writes JSON for):

  - headline: total trades, win rate, average edge, total P&L, max drawdown,
    Sharpe-like ratio (mean daily return / std daily return)
  - breakdowns: P&L by city, by volume tier, by primary vs adjacent bucket
  - sanity counts: discovered markets, skipped (no resolution / no price)

Sharpe-like is computed on a daily-return series: each UTC day's net P&L
divided by the day's deployed capital. Annualization is intentionally
skipped — temperature markets resolve daily, so the ratio is already on a
unit that's directly comparable across runs.
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

from .runner import BacktestResult, TradeRecord


log = logging.getLogger("backtesting.report")


VOLUME_TIERS = (
    ("low", 0.0, 50_000.0),
    ("mid", 50_000.0, 200_000.0),
    ("high", 200_000.0, math.inf),
)


def render_report(result: BacktestResult, *, output_path: Path | None = None) -> dict[str, Any]:
    summary = _summarize(result)
    _print_summary(result, summary)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps({
            "config": _config_dict(result),
            "summary": summary,
            "trades": [_trade_dict(t) for t in result.trades],
        }, default=str, indent=2))
        log.info("Wrote backtest report -> %s", output_path)
    return summary


def _summarize(result: BacktestResult) -> dict[str, Any]:
    trades = result.trades
    n = len(trades)
    if n == 0:
        return {
            "total_trades": 0, "win_rate": 0.0, "avg_edge": 0.0,
            "total_pnl_usd": 0.0, "max_drawdown_usd": 0.0, "sharpe_like": 0.0,
            "by_city": {}, "by_volume_tier": {}, "by_bucket_role": {},
            "discovered_markets": result.discovered_markets,
            "skipped_no_resolution": result.skipped_no_resolution,
            "skipped_no_price": result.skipped_no_price,
        }

    wins = sum(1 for t in trades if t.pnl_usd > 0)
    avg_edge = sum(t.edge for t in trades) / n
    total_pnl = sum(t.pnl_usd for t in trades)
    max_dd = _max_drawdown(trades)
    sharpe = _sharpe_like(trades)

    return {
        "total_trades": n,
        "win_rate": wins / n,
        "avg_edge": avg_edge,
        "total_pnl_usd": total_pnl,
        "max_drawdown_usd": max_dd,
        "sharpe_like": sharpe,
        "by_city": _group_pnl(trades, key=lambda t: t.city or "unknown"),
        "by_volume_tier": _group_pnl(trades, key=lambda t: _volume_tier(t.volume_usd)),
        "by_bucket_role": _group_pnl(trades, key=lambda t: t.bucket_role or "primary"),
        "discovered_markets": result.discovered_markets,
        "skipped_no_resolution": result.skipped_no_resolution,
        "skipped_no_price": result.skipped_no_price,
    }


def _group_pnl(trades: list[TradeRecord], *, key) -> dict[str, dict[str, float]]:
    buckets: dict[str, list[TradeRecord]] = defaultdict(list)
    for t in trades:
        buckets[key(t)].append(t)
    out: dict[str, dict[str, float]] = {}
    for k, ts in buckets.items():
        wins = sum(1 for x in ts if x.pnl_usd > 0)
        out[k] = {
            "trades": len(ts),
            "wins": wins,
            "win_rate": wins / len(ts) if ts else 0.0,
            "total_pnl_usd": sum(x.pnl_usd for x in ts),
            "avg_edge": sum(x.edge for x in ts) / len(ts) if ts else 0.0,
            "avg_pnl_per_trade_usd": (sum(x.pnl_usd for x in ts) / len(ts)) if ts else 0.0,
        }
    return out


def _volume_tier(volume_usd: float) -> str:
    for name, lo, hi in VOLUME_TIERS:
        if lo <= volume_usd < hi:
            return name
    return "unknown"


def _max_drawdown(trades: list[TradeRecord]) -> float:
    chrono = sorted(trades, key=lambda t: t.as_of)
    peak = 0.0
    running = 0.0
    max_dd = 0.0
    for t in chrono:
        running += t.pnl_usd
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _sharpe_like(trades: list[TradeRecord]) -> float:
    """Mean / std of daily returns (P&L / capital deployed that day)."""
    by_day: dict[date, tuple[float, float]] = {}  # day -> (pnl, capital)
    for t in trades:
        d = t.as_of.date()
        pnl, cap = by_day.get(d, (0.0, 0.0))
        by_day[d] = (pnl + t.pnl_usd, cap + t.size_usd)
    rets: list[float] = []
    for pnl, cap in by_day.values():
        if cap > 0:
            rets.append(pnl / cap)
    if len(rets) < 2:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    std = math.sqrt(var)
    return mean / std if std > 0 else 0.0


def _print_summary(result: BacktestResult, summary: dict[str, Any]) -> None:
    cfg = result.config
    print("=" * 70)
    print(f"Backtest: {cfg.strategy_name}  [{cfg.start} .. {cfg.end}]")
    if cfg.param_overrides:
        print(f"Param overrides: {cfg.param_overrides}")
    print(f"Position size (paper): ${cfg.paper_position_usd:.2f}")
    print(f"Discovered markets:    {summary['discovered_markets']}")
    print(f"Skipped (no resolution): {summary['skipped_no_resolution']}")
    print(f"Skipped (no price):    {summary['skipped_no_price']}")
    print("-" * 70)
    print(f"Total trades:          {summary['total_trades']}")
    print(f"Win rate:              {summary['win_rate']:.1%}")
    print(f"Avg edge:              {summary['avg_edge']:.3f}")
    print(f"Total P&L:             ${summary['total_pnl_usd']:.2f}")
    print(f"Max drawdown:          ${summary['max_drawdown_usd']:.2f}")
    print(f"Sharpe-like:           {summary['sharpe_like']:.3f}")
    _print_breakdown("By city", summary["by_city"])
    _print_breakdown("By volume tier", summary["by_volume_tier"])
    _print_breakdown("By bucket role", summary["by_bucket_role"])
    print("=" * 70)


def _print_breakdown(title: str, rows: dict[str, dict[str, float]]) -> None:
    if not rows:
        return
    print(f"\n{title}:")
    print(f"  {'group':<20} {'trades':>7} {'win_rate':>10} {'pnl':>10} {'avg_edge':>10}")
    for k, v in sorted(rows.items(), key=lambda kv: -kv[1]["total_pnl_usd"]):
        print(
            f"  {k:<20} {int(v['trades']):>7d} {v['win_rate']:>10.1%} "
            f"{v['total_pnl_usd']:>10.2f} {v['avg_edge']:>10.3f}"
        )


def _trade_dict(t: TradeRecord) -> dict[str, Any]:
    d = asdict(t)
    d["as_of"] = t.as_of.isoformat()
    d["target_date"] = t.target_date.isoformat()
    return d


def _config_dict(result: BacktestResult) -> dict[str, Any]:
    cfg = result.config
    return {
        "start": cfg.start.isoformat(),
        "end": cfg.end.isoformat(),
        "strategy_name": cfg.strategy_name,
        "as_of_offsets_hours": list(cfg.as_of_offsets_hours),
        "paper_position_usd": cfg.paper_position_usd,
        "param_overrides": cfg.param_overrides,
    }
