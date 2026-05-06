"""Wallet quality scoring (stub).

Builds a `WalletProfile` from a wallet's trade history and produces a
single quality score in [0, 1] that gates whether the wallet's signals
contribute to a consensus trigger.

Scoring sketch (to be tuned with real data):

  score = w1 * win_rate
        + w2 * sigmoid(avg_roi * k)
        + w3 * sigmoid(history_length_days / 90)
        + w4 * diversification_score(top_categories, total_trades)
        - arb_bot_penalty(is_likely_arb_bot)

Each weight is bounded so any single feature can't dominate. The arb-bot
penalty is large enough that flagged wallets fall well below
`min_wallet_score` even if their other stats look great.

`is_likely_arb_bot` heuristics (combine with OR; tune false positives):
  - >X% of trades have a matching opposite-side trade in the same market
    within Y minutes
  - high trade frequency relative to position holding time
  - positions concentrated in liquid markets only
"""
from __future__ import annotations

import logging
from typing import Any

from .models import WalletProfile


log = logging.getLogger("strategy.whale.scorer")


class WalletScorer:
    def __init__(self, *, client: Any) -> None:
        self._client = client
        self._profile_cache: dict[str, WalletProfile] = {}

    async def get_profile(self, address: str) -> WalletProfile | None:
        """Fetch (or return cached) profile for a wallet.

        TODO(whale): fetch trade history from Polymarket Data API
        (`/trades?user=<addr>`), aggregate into the WalletProfile fields,
        and cache the result with a 12-24h TTL since these stats don't
        move per-scan.
        """
        cached = self._profile_cache.get(address)
        if cached is not None:
            return cached
        log.debug("WalletScorer.get_profile() not implemented for %s", address)
        return None

    def score(self, profile: WalletProfile) -> float:
        """Map a profile to a [0, 1] quality score.

        TODO(whale): tune weights against real per-wallet performance.
        Until then, returns the raw `win_rate` as a placeholder so that
        `min_wallet_score` filtering at least has a directionally-correct
        signal during integration testing.
        """
        return max(0.0, min(1.0, profile.win_rate))
