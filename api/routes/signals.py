"""GET /signals — recent signals (acted on / rejected).

The trade journal only records signals that were *acted on* — rejected
signals don't get persisted by the bot today. So /signals?status=rejected
returns an empty list with a note explaining why. /signals?status=acted
(or 'all') returns recent journal entries reframed as signal records:
edge, model probability (from metadata if present), market price, and
the resulting trade direction.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal

import aiosqlite
from fastapi import APIRouter, Depends, Query

from api.deps import get_db
from api.models import Signal, SignalsResponse


router = APIRouter()


def _parse_metadata(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        m = json.loads(raw)
        return m if isinstance(m, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _row_to_signal(row: aiosqlite.Row) -> Signal:
    meta = _parse_metadata(row["metadata_json"])
    try:
        ts = datetime.fromisoformat(row["timestamp"])
    except (TypeError, ValueError):
        ts = datetime.fromtimestamp(0)
    return Signal(
        market_id=row["market_id"],
        market_question=row["market_question"],
        bucket=meta.get("bucket"),
        edge=float(row["edge_at_entry"]),
        model_probability=meta.get("model_prob"),
        market_price=float(row["entry_price"]),
        action="traded",
        rejection_reason=None,
        timestamp=ts,
        strategy=row["strategy"],
    )


@router.get("", response_model=SignalsResponse)
async def get_signals(
    db: aiosqlite.Connection = Depends(get_db),
    limit: int = Query(20, ge=1, le=200),
    status: Literal["acted", "rejected", "all"] = Query("all"),
) -> SignalsResponse:
    if status == "rejected":
        return SignalsResponse(
            signals=[],
            note=(
                "Rejected signals are not persisted by the bot — only acted-on "
                "signals reach the trade journal. Rejection logging is a planned "
                "enhancement (see core.risk.check_trade)."
            ),
        )

    async with db.execute(
        "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?", (limit,)
    ) as cur:
        rows = await cur.fetchall()
    signals = [_row_to_signal(r) for r in rows]

    note = None
    if status == "all":
        note = "Only acted-on signals are stored. Rejected signals are not yet logged."
    return SignalsResponse(signals=signals, note=note)
