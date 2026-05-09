"""GET /performance and /performance/timeseries — aggregated P&L views.

Both endpoints derive everything from the trades table directly rather
than calling into monitoring/perf_report.py — that module is sync and
prints to stdout, neither of which fits an async API.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_db, load_config
from api.models import (
    PerformanceBreakdownRow,
    PerformancePoint,
    PerformanceResponse,
    PerformanceSummary,
    PerformanceTimeseriesResponse,
)


router = APIRouter()


# ---- helpers ------------------------------------------------------------

def _utc_day_start(now: datetime) -> datetime:
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def _period_to_since(period: str) -> datetime | None:
    """Map period label -> start datetime (UTC). None means 'all time'."""
    now = datetime.now(timezone.utc)
    if period == "today":
        return _utc_day_start(now)
    if period == "week":
        return _utc_day_start(now) - timedelta(days=7)
    if period == "month":
        return _utc_day_start(now) - timedelta(days=30)
    if period == "all":
        return None
    raise HTTPException(status_code=400, detail=f"unknown period: {period!r}")


def _ts_to_since(period: str) -> datetime:
    """For the timeseries endpoint — 'all' is bounded back to the first
    trade we find. Caller handles 'all' separately."""
    now = datetime.now(timezone.utc)
    if period == "24h":
        return now - timedelta(hours=24)
    if period == "7d":
        return now - timedelta(days=7)
    if period == "30d":
        return now - timedelta(days=30)
    if period == "all":
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    raise HTTPException(status_code=400, detail=f"unknown period: {period!r}")


def _parse_metadata(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        m = json.loads(raw)
        return m if isinstance(m, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _volume_tier(volume_usd: float | None, tiers: list[dict[str, Any]]) -> str:
    """Match a market's volume against config.risk.volume_position_tiers."""
    if volume_usd is None:
        return "unknown"
    for tier in tiers:
        lo = float(tier.get("min_volume_usd", 0.0))
        hi_raw = tier.get("max_volume_usd")
        hi = float("inf") if hi_raw in (None, "inf", "infinity") else float(hi_raw)
        if lo <= volume_usd < hi:
            return str(tier.get("name") or "tier")
    return "unknown"


# ---- /performance -------------------------------------------------------

async def _summary_for_period(
    db: aiosqlite.Connection, since: datetime | None
) -> tuple[PerformanceSummary, list[aiosqlite.Row], float]:
    """Compute summary block + return resolved rows for max-drawdown and ROI."""
    if since is None:
        where = ""
        params: tuple = ()
    else:
        where = "WHERE timestamp >= ?"
        params = (since.isoformat(),)

    async with db.execute(
        f"""
        SELECT
            COUNT(*)                                            AS trades,
            COALESCE(SUM(CASE outcome WHEN 'pending' THEN 0 ELSE pnl END), 0.0) AS total_pnl,
            SUM(CASE outcome WHEN 'won'  THEN 1 ELSE 0 END)     AS wins,
            SUM(CASE outcome WHEN 'lost' THEN 1 ELSE 0 END)     AS losses,
            COALESCE(AVG(edge_at_entry), 0.0)                   AS avg_edge,
            COALESCE(SUM(size_usd), 0.0)                        AS total_size
        FROM trades
        {where}
        """,
        params,
    ) as cur:
        agg = await cur.fetchone()

    trades = int(agg["trades"] or 0) if agg else 0
    wins = int(agg["wins"] or 0) if agg else 0
    losses = int(agg["losses"] or 0) if agg else 0
    resolved = wins + losses
    total_pnl = float(agg["total_pnl"] or 0.0) if agg else 0.0
    avg_edge = float(agg["avg_edge"] or 0.0) if agg else 0.0
    total_size = float(agg["total_size"] or 0.0) if agg else 0.0

    # Max-drawdown: walk resolved trades in time order tracking running
    # equity peak. Peak-to-trough drop is the answer (negative number).
    async with db.execute(
        f"""
        SELECT timestamp, pnl, size_usd, entry_price, shares
        FROM trades
        {where + (' AND ' if where else 'WHERE ')}outcome IN ('won','lost')
        ORDER BY timestamp ASC
        """,
        params,
    ) as cur:
        resolved_rows = await cur.fetchall()

    peak = 0.0
    cumulative = 0.0
    max_dd = 0.0
    avg_realized_edge_num = 0.0
    avg_realized_edge_den = 0
    for r in resolved_rows:
        pnl = float(r["pnl"] or 0.0)
        cumulative += pnl
        peak = max(peak, cumulative)
        max_dd = min(max_dd, cumulative - peak)
        # Realized edge per resolved trade ≈ pnl / size_usd. Skip rows with
        # zero size to avoid divide-by-zero noise in the average.
        size = float(r["size_usd"] or 0.0)
        if size > 0:
            avg_realized_edge_num += pnl / size
            avg_realized_edge_den += 1

    avg_realized_edge = (avg_realized_edge_num / avg_realized_edge_den) if avg_realized_edge_den else 0.0
    win_rate = (wins / resolved) if resolved else 0.0
    roi_pct = (total_pnl / total_size * 100.0) if total_size else 0.0

    summary = PerformanceSummary(
        total_pnl=round(total_pnl, 2),
        total_trades=trades,
        wins=wins,
        losses=losses,
        win_rate=round(win_rate, 4),
        avg_edge_at_entry=round(avg_edge, 4),
        avg_realized_edge=round(avg_realized_edge, 4),
        max_drawdown=round(max_dd, 2),
        roi_pct=round(roi_pct, 2),
    )
    return summary, list(resolved_rows), total_size


async def _breakdown(
    db: aiosqlite.Connection,
    since: datetime | None,
    group_by: str,
) -> list[PerformanceBreakdownRow]:
    """Aggregate by city, strategy, or volume_tier.

    `city` and `volume_tier` need a Python pass because they live inside
    `metadata_json`, not as SQL columns. `strategy` we can do in SQL.
    """
    if since is None:
        where = ""
        params: tuple = ()
    else:
        where = "WHERE timestamp >= ?"
        params = (since.isoformat(),)

    if group_by == "strategy":
        async with db.execute(
            f"""
            SELECT
                strategy AS grp,
                COALESCE(SUM(CASE outcome WHEN 'pending' THEN 0 ELSE pnl END), 0.0) AS pnl,
                COUNT(*) AS trades,
                SUM(CASE outcome WHEN 'won'  THEN 1 ELSE 0 END) AS wins,
                SUM(CASE outcome WHEN 'lost' THEN 1 ELSE 0 END) AS losses
            FROM trades
            {where}
            GROUP BY strategy
            ORDER BY pnl DESC
            """,
            params,
        ) as cur:
            rows = await cur.fetchall()
        out: list[PerformanceBreakdownRow] = []
        for r in rows:
            resolved = int(r["wins"] or 0) + int(r["losses"] or 0)
            wr = (int(r["wins"] or 0) / resolved) if resolved else 0.0
            out.append(PerformanceBreakdownRow(
                group=r["grp"],
                pnl=round(float(r["pnl"] or 0.0), 2),
                trades=int(r["trades"] or 0),
                win_rate=round(wr, 4),
            ))
        return out

    if group_by not in ("city", "volume_tier"):
        raise HTTPException(status_code=400, detail=f"unknown group_by: {group_by!r}")

    cfg = load_config()
    tiers = list((cfg.get("risk") or {}).get("volume_position_tiers") or [])

    async with db.execute(
        f"SELECT outcome, pnl, metadata_json FROM trades {where}",
        params,
    ) as cur:
        rows = await cur.fetchall()

    buckets: dict[str, dict[str, float]] = {}
    for r in rows:
        meta = _parse_metadata(r["metadata_json"])
        if group_by == "city":
            key = str(meta.get("city") or "unknown")
        else:
            key = _volume_tier(meta.get("volume_usd"), tiers)
        b = buckets.setdefault(key, {"pnl": 0.0, "trades": 0, "wins": 0, "losses": 0})
        b["trades"] += 1
        if r["outcome"] == "won":
            b["wins"] += 1
            b["pnl"] += float(r["pnl"] or 0.0)
        elif r["outcome"] == "lost":
            b["losses"] += 1
            b["pnl"] += float(r["pnl"] or 0.0)

    out2: list[PerformanceBreakdownRow] = []
    for key, b in buckets.items():
        resolved = int(b["wins"]) + int(b["losses"])
        wr = (b["wins"] / resolved) if resolved else 0.0
        out2.append(PerformanceBreakdownRow(
            group=key,
            pnl=round(b["pnl"], 2),
            trades=int(b["trades"]),
            win_rate=round(wr, 4),
        ))
    out2.sort(key=lambda r: r.pnl, reverse=True)
    return out2


@router.get("", response_model=PerformanceResponse)
async def get_performance(
    db: aiosqlite.Connection = Depends(get_db),
    period: Literal["today", "week", "month", "all"] = Query("week"),
    group_by: Literal["city", "strategy", "volume_tier"] = Query("strategy"),
) -> PerformanceResponse:
    since = _period_to_since(period)
    summary, _resolved, _size = await _summary_for_period(db, since)
    breakdown = await _breakdown(db, since, group_by)
    return PerformanceResponse(
        period=period,
        summary=summary,
        breakdown=breakdown,
    )


# ---- /performance/timeseries -------------------------------------------

@router.get("/timeseries", response_model=PerformanceTimeseriesResponse)
async def get_performance_timeseries(
    db: aiosqlite.Connection = Depends(get_db),
    period: Literal["24h", "7d", "30d", "all"] = Query("7d"),
    interval: Literal["1h", "1d"] = Query("1d"),
) -> PerformanceTimeseriesResponse:
    since = _ts_to_since(period)
    bucket_seconds = 3600 if interval == "1h" else 86400

    async with db.execute(
        """
        SELECT timestamp, pnl
        FROM trades
        WHERE outcome IN ('won','lost') AND timestamp >= ?
        ORDER BY timestamp ASC
        """,
        (since.isoformat(),),
    ) as cur:
        rows = await cur.fetchall()

    # Bucket trades into time-floored points and walk cumulative.
    # The dashboard wants a continuous series, so we emit one point per
    # bucket from `since` to now even if no trade landed in it.
    end = datetime.now(timezone.utc)
    bucketed: dict[int, float] = {}
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["timestamp"])
        except (TypeError, ValueError):
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        floor = int(ts.timestamp() // bucket_seconds) * bucket_seconds
        bucketed[floor] = bucketed.get(floor, 0.0) + float(r["pnl"] or 0.0)

    points: list[PerformancePoint] = []
    cumulative = 0.0
    if rows or period != "all":
        # Anchor the series at the period start so the chart starts at 0.
        start_floor = int(since.timestamp() // bucket_seconds) * bucket_seconds
        end_floor = int(end.timestamp() // bucket_seconds) * bucket_seconds
        for t in range(start_floor, end_floor + bucket_seconds, bucket_seconds):
            cumulative += bucketed.get(t, 0.0)
            points.append(PerformancePoint(
                time=datetime.fromtimestamp(t, tz=timezone.utc),
                cumulative_pnl=round(cumulative, 2),
            ))

    return PerformanceTimeseriesResponse(
        period=period,
        interval=interval,
        points=points,
    )
