"""Arbitrage opportunity scanner (stub).

The scanner is responsible for:

  1. Collecting candidate markets — preferentially from `MarketCache`
     (markets already populated by the weather strategy), falling back
     to a direct Gamma fetch for categories the cache hasn't covered.

  2. For each candidate, reading the YES and NO best-ask + book depth
     (CLOB book API) and computing `spread = 1.0 - (yes_price + no_price)`.

  3. Filtering for `spread >= min_spread_pct + fee_buffer_pct` and both
     legs having `>= min_book_depth_usd` of depth at the quoted price.

  4. Returning a list of `ArbitrageOpportunity` rows the strategy can
     turn into Signals.

The scanner does NOT submit orders or split into legs — that's the
strategy's job. The scanner is pure read-side.
"""
from __future__ import annotations

import logging
from typing import Any

from core.market_cache import MarketCache, MarketSnapshot

from .models import ArbitrageOpportunity


log = logging.getLogger("strategy.arbitrage.scanner")


class ArbitrageScanner:
    def __init__(
        self,
        *,
        client: Any,                    # core.client.ClobClient (for live book fetch)
        market_cache: MarketCache | None,
        min_spread_pct: float,
        min_book_depth_usd: float,
        fee_buffer_pct: float,
        share_market_data: bool,
        categories: list[str],
    ) -> None:
        self._client = client
        self._cache = market_cache
        self.min_spread_pct = float(min_spread_pct)
        self.min_book_depth_usd = float(min_book_depth_usd)
        self.fee_buffer_pct = float(fee_buffer_pct)
        self.share_market_data = bool(share_market_data)
        self.categories = list(categories)

    async def scan(self) -> list[ArbitrageOpportunity]:
        """Return all current arb opportunities. Empty list when nothing fires.

        TODO(arb): full implementation. Steps, in order:
          - read all fresh snapshots from the cache (filtered by `categories`
            if set), via `MarketCache.get_fresh()`
          - for any market category not represented in the cache, fetch
            an event list directly from Gamma (cf. `weather.markets`)
          - for each candidate market, look up the CLOB book for both
            outcome tokens and pull best YES ask + best NO ask
          - compute `spread = 1.0 - (yes_ask + no_ask)`; skip when
            `spread < min_spread_pct + fee_buffer_pct`
          - confirm `min(depth_yes_usd, depth_no_usd) >= min_book_depth_usd`
            at the quoted prices
          - emit `ArbitrageOpportunity` rows
        """
        log.debug("ArbitrageScanner.scan() not implemented (categories=%s)", self.categories or "*")
        return []

    def _candidates_from_cache(self) -> list[MarketSnapshot]:
        if self._cache is None or not self.share_market_data:
            return []
        # NOTE: returns the raw snapshots; the caller pulls live book data
        # for prices/depth before computing spread. The cache only saves
        # the discovery round-trip, not the book-quote round-trip.
        # When `categories` is empty, return all fresh snapshots.
        return []  # TODO(arb): wire to MarketCache.get_fresh per category
