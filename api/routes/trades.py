"""GET /trades — paginated, filterable trade journal."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_db
from api.models import PaginationMeta, Trade, TradesResponse


router = APIRouter()


_SORT_TO_SQL = {
    "newest": "ORDER BY timestamp DESC",
    "oldest": "ORDER BY timestamp ASC",
    "pnl_desc": "ORDER BY pnl DESC NULLS LAST",
    "pnl_asc": "ORDER BY pnl ASC NULLS LAST",
    "edge_desc": "ORDER BY edge_at_entry DESC",
}


def _parse_metadata(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        m = json.loads(raw)
        return m if isinstance(m, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _row_to_trade(row: aiosqlite.Row) -> Trade:
    meta = _parse_metadata(row["metadata_json"])
    try:
        ts = datetime.fromisoformat(row["timestamp"])
    except (TypeError, ValueError):
        ts = datetime.fromtimestamp(0)
    return Trade(
        id=int(row["id"]),
        timestamp=ts,
        strategy=row["strategy"],
        market_id=row["market_id"],
        market_question=row["market_question"],
        direction=row["direction"],
        entry_price=float(row["entry_price"]),
        size_usd=float(row["size_usd"]),
        shares=float(row["shares"]),
        edge_at_entry=float(row["edge_at_entry"]),
        outcome=row["outcome"],
        pnl=(float(row["pnl"]) if row["pnl"] is not None else None),
        city=meta.get("city"),
        bucket=meta.get("bucket"),
        metadata=meta,
    )


@router.get("", response_model=TradesResponse)
async def get_trades(
    db: aiosqlite.Connection = Depends(get_db),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    city: str | None = Query(None, description="Filter by metadata.city (exact match)"),
    strategy: str | None = Query(None),
    outcome: Literal["won", "lost", "pending"] | None = Query(None),
    date_from: datetime | None = Query(None, description="ISO 8601 lower bound (inclusive)"),
    date_to: datetime | None = Query(None, description="ISO 8601 upper bound (exclusive)"),
    sort: Literal["newest", "oldest", "pnl_desc", "pnl_asc", "edge_desc"] = Query("newest"),
) -> TradesResponse:
    # Build WHERE clause from SQL-safe filters; city goes through Python
    # because it lives in metadata_json. SQLite has no JSON1 guarantee in
    # older builds, and the values are short — Python filtering is fine
    # for the datasets we'll see (thousands of rows, not millions).
    clauses: list[str] = []
    params: list[Any] = []
    if strategy is not None:
        clauses.append("strategy = ?")
        params.append(strategy)
    if outcome is not None:
        clauses.append("outcome = ?")
        params.append(outcome)
    if date_from is not None:
        clauses.append("timestamp >= ?")
        params.append(date_from.isoformat())
    if date_to is not None:
        clauses.append("timestamp < ?")
        params.append(date_to.isoformat())

    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    order_sql = _SORT_TO_SQL.get(sort)
    if order_sql is None:
        # Defensive — Literal type should already prevent this.
        raise HTTPException(status_code=400, detail=f"unknown sort: {sort!r}")

    # When filtering by city we have to pull the page after Python-filtering,
    # so total/has_more reflect the post-filter view. We over-fetch and
    # stop counting once we hit `limit + offset`.
    if city is None:
        async with db.execute(
            f"SELECT COUNT(*) AS n FROM trades {where_sql}", params
        ) as cur:
            row = await cur.fetchone()
        total = int(row["n"]) if row else 0

        async with db.execute(
            f"SELECT * FROM trades {where_sql} {order_sql} LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ) as cur:
            rows = await cur.fetchall()
        trades = [_row_to_trade(r) for r in rows]
    else:
        async with db.execute(
            f"SELECT * FROM trades {where_sql} {order_sql}", params,
        ) as cur:
            all_rows = await cur.fetchall()
        filtered = [r for r in all_rows if _parse_metadata(r["metadata_json"]).get("city") == city]
        total = len(filtered)
        page = filtered[offset:offset + limit]
        trades = [_row_to_trade(r) for r in page]

    return TradesResponse(
        trades=trades,
        pagination=PaginationMeta(
            total=total,
            limit=limit,
            offset=offset,
            has_more=(offset + len(trades)) < total,
        ),
    )
