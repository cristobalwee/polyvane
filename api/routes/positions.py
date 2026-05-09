"""GET /positions — open and resolved positions, paginated.

Resolved-position fields like `exit_price` and `resolved_at` aren't stored
in the journal schema — the bot only writes `outcome` and `pnl` at exit
time. We surface what the journal has and leave the rest as null so the
dashboard can show "—" without breaking.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal

import aiosqlite
from fastapi import APIRouter, Depends, Query

from api.deps import get_db
from api.models import PaginationMeta, Position, PositionsResponse


router = APIRouter()


def _parse_metadata(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        m = json.loads(raw)
        return m if isinstance(m, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _row_to_position(row: aiosqlite.Row) -> Position:
    meta = _parse_metadata(row["metadata_json"])
    try:
        opened_at = datetime.fromisoformat(row["timestamp"])
    except (TypeError, ValueError):
        opened_at = datetime.fromtimestamp(0)
    return Position(
        id=int(row["id"]),
        market_id=row["market_id"],
        market_question=row["market_question"],
        city=meta.get("city"),
        bucket=meta.get("bucket"),
        direction=row["direction"],
        entry_price=float(row["entry_price"]),
        current_price=None,
        exit_price=None,
        size_usd=float(row["size_usd"]),
        shares=float(row["shares"]),
        edge_at_entry=float(row["edge_at_entry"]),
        opened_at=opened_at,
        resolved_at=None,
        outcome=row["outcome"],
        pnl=(float(row["pnl"]) if row["pnl"] is not None else None),
        strategy=row["strategy"],
        metadata=meta,
    )


@router.get("", response_model=PositionsResponse)
async def get_positions(
    db: aiosqlite.Connection = Depends(get_db),
    status: Literal["open", "resolved", "all"] = Query("open"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> PositionsResponse:
    if status == "open":
        where = "WHERE outcome = 'pending'"
    elif status == "resolved":
        where = "WHERE outcome IN ('won','lost')"
    else:
        where = ""

    async with db.execute(f"SELECT COUNT(*) AS n FROM trades {where}") as cur:
        row = await cur.fetchone()
    total = int(row["n"]) if row else 0

    async with db.execute(
        f"SELECT * FROM trades {where} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
        (limit, offset),
    ) as cur:
        rows = await cur.fetchall()

    positions = [_row_to_position(r) for r in rows]
    return PositionsResponse(
        positions=positions,
        pagination=PaginationMeta(
            total=total,
            limit=limit,
            offset=offset,
            has_more=(offset + len(positions)) < total,
        ),
    )
