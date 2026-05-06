"""Dataclasses for the whale-tracking strategy."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class WhaleAlert:
    """Detected large position from a tracked wallet.

    Emitted by the tracker whenever a tracked wallet's position on a market
    crosses `min_trade_size_usd`. Multiple alerts in close succession on
    the same (market_id, direction) feed the consensus check in the
    strategy.
    """
    wallet_address: str
    market_id: str
    direction: str            # 'YES' | 'NO'
    size_usd: float
    wallet_score: float       # [0, 1] — see WalletScorer
    timestamp: datetime
    token_id: str | None = None
    market_question: str | None = None
    category: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.direction not in ("YES", "NO"):
            raise ValueError(f"direction must be 'YES' or 'NO', got {self.direction!r}")
        if not (0.0 <= self.wallet_score <= 1.0):
            raise ValueError(f"wallet_score must be in [0, 1], got {self.wallet_score}")


@dataclass
class WalletProfile:
    """Aggregate stats for a tracked wallet, used by `WalletScorer`.

    `is_likely_arb_bot` is a derived flag: bots that hedge YES+NO across
    the same market (or pair markets across venues) generate trade flow
    that looks like alpha but is actually risk-neutral. We exclude them
    from consensus when `exclude_arb_bots: true`.
    """
    address: str
    total_trades: int
    win_rate: float            # [0, 1]
    avg_roi: float             # fractional, e.g. 0.07 for +7%
    history_length_days: int   # how far back the wallet's trade history goes
    top_categories: list[str] = field(default_factory=list)
    avg_position_size: float = 0.0
    is_likely_arb_bot: bool = False
    last_seen: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
