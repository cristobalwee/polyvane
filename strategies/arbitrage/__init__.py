"""Cross-market arbitrage strategy (scaffold).

Picks up opportunities where YES + NO < 1 - fees on the same outcome,
across all Polymarket categories. Designed to layer on top of the shared
`core.market_cache.MarketCache` — when the weather strategy populates the
cache with temperature-market prices, the arb scanner reads from there
instead of re-fetching Gamma.

Not yet implemented; see `strategy.py` for TODOs.
"""
from .strategy import ArbitrageStrategy

__all__ = ["ArbitrageStrategy"]
