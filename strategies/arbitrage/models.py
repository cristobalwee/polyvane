"""Dataclasses for the arbitrage strategy."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ArbitrageOpportunity:
    """A YES/NO mispricing on a single Polymarket binary outcome.

    `spread` is the deficit below 1.0 — the gross profit per $1 of paired
    fill before fees. `estimated_fill_cost` is what the legs would actually
    cost given the available depth at quoted prices, including the
    configured fee buffer.
    """
    market_id: str
    market_question: str
    yes_price: float            # current YES ask
    no_price: float             # current NO ask
    spread: float               # 1.0 - (yes_price + no_price); positive = potential edge
    depth_yes: float            # USD depth at the YES ask
    depth_no: float             # USD depth at the NO ask
    estimated_fill_cost: float  # USD to fully realize the arb leg pair
    volume_24h: float
    category: str               # 'weather' | 'crypto' | 'politics' | ...
    yes_token_id: str | None = None
    no_token_id: str | None = None
    detected_at: str | None = None  # ISO UTC
    metadata: dict[str, Any] = field(default_factory=dict)
