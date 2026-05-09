"""GET /markets — markets the bot is currently exposed to.

The bot's full scan-time view of "active markets being scanned" lives in
its in-memory MarketCache, which this separate process can't read. We
fall back to the next-best thing: distinct markets the bot has open
positions on, derived from the trade journal. Each row carries the
metadata the bot persisted at entry time — `volume_usd`,
`liquidity_usd`, `end_utc`, `bucket`, `forecast_source`, etc.

The dashboard should treat this list as "markets we're holding" rather
than "all markets being scanned right now". A future enhancement could
have the bot persist its scan results to a JSON snapshot the API reads.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import aiosqlite
from fastapi import APIRouter, Depends

from api.deps import get_db
from api.models import Market, MarketBucket, MarketsResponse


router = APIRouter()


def _parse_metadata(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        m = json.loads(raw)
        return m if isinstance(m, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _parse_dt(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


@router.get("", response_model=MarketsResponse)
async def get_markets(
    db: aiosqlite.Connection = Depends(get_db),
) -> MarketsResponse:
    # Pull every open position; group by market_id and assemble a Market
    # entry per group. Same market_id may have multiple open positions
    # across strategies — we collapse them and surface one row.
    async with db.execute(
        "SELECT market_id, market_question, entry_price, metadata_json "
        "FROM trades WHERE outcome = 'pending' "
        "ORDER BY timestamp DESC"
    ) as cur:
        rows = await cur.fetchall()

    by_market: dict[str, dict[str, Any]] = {}
    for r in rows:
        mid = r["market_id"]
        meta = _parse_metadata(r["metadata_json"])
        slot = by_market.setdefault(mid, {
            "market_id": mid,
            "market_question": r["market_question"],
            "city": meta.get("city"),
            "resolution_date": _parse_dt(meta.get("end_utc")),
            "total_volume": meta.get("volume_usd"),
            "resolution_source": meta.get("forecast_source") or meta.get("station"),
            "buckets": [],
        })
        bucket_label = meta.get("bucket")
        if bucket_label:
            yes_price = float(r["entry_price"])
            slot["buckets"].append(MarketBucket(
                label=bucket_label,
                yes_price=yes_price,
                no_price=max(0.0, min(1.0, 1.0 - yes_price)),
            ))

    markets = [
        Market(
            market_id=m["market_id"],
            market_question=m["market_question"],
            city=m["city"],
            resolution_date=m["resolution_date"],
            bucket_count=len(m["buckets"]),
            total_volume=m["total_volume"],
            resolution_source=m["resolution_source"],
            # The bot only stores positions on markets where it confirmed
            # resolution at entry time, so anything we surface here has
            # already passed that gate.
            resolution_confirmed=True,
            buckets=m["buckets"],
        )
        for m in by_market.values()
    ]

    return MarketsResponse(
        markets=markets,
        note=(
            "Lists markets with open positions. The bot's full live scan list "
            "lives in-process and is not exposed yet."
        ),
    )
