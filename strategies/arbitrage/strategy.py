"""ArbitrageStrategy: cross-market YES+NO < 1 detector (scaffold).

When fully built, this strategy:

  - Scans Polymarket markets across configured categories (or all categories
    if none are set), preferring data already in the shared `MarketCache`
    populated by other strategies (e.g. weather) so we don't burn API calls.
  - Detects markets where YES + NO < 1 - fees by enough margin to cover
    taker fees + slippage + the configured `min_spread_pct` floor.
  - Emits paired Signals — one YES leg, one NO leg — with metadata flagging
    them as a unit so the executor can size them together.

Currently a stub: `scan()` returns an empty list and logs once that the
strategy is not yet implemented. Enabling it in config does NOT submit
orders.
"""
from __future__ import annotations

import logging
from typing import Any

from strategies.base import BaseStrategy, Signal, StrategyContext, TradeIntent

from .scanner import ArbitrageScanner


log = logging.getLogger("strategy.arbitrage")


class ArbitrageStrategy(BaseStrategy):
    name = "arbitrage"

    def __init__(self, params: dict[str, Any], context: StrategyContext) -> None:
        super().__init__(params, context)
        self._min_spread_pct: float = float(params.get("min_spread_pct", 0.04))
        self._min_book_depth_usd: float = float(params.get("min_book_depth_usd", 50.0))
        self._max_position_usd: float = float(params.get("max_position_usd", 10.0))
        self._scan_interval_sec: float = float(params.get("scan_interval_sec", 30.0))
        self._fee_buffer_pct: float = float(params.get("fee_buffer_pct", 0.01))
        self._share_market_data: bool = bool(params.get("share_market_data", True))
        self._categories: list[str] = list(params.get("categories") or [])

        self._scanner: ArbitrageScanner | None = None
        self._warned_not_implemented = False

    async def setup(self) -> None:
        self._scanner = ArbitrageScanner(
            client=self.context.client,
            market_cache=self.context.market_cache,
            min_spread_pct=self._min_spread_pct,
            min_book_depth_usd=self._min_book_depth_usd,
            fee_buffer_pct=self._fee_buffer_pct,
            share_market_data=self._share_market_data,
            categories=self._categories,
        )
        self.log.info(
            "ArbitrageStrategy ready (SCAFFOLD): min_spread=%.3f min_depth=$%.0f "
            "max_pos=$%.0f fee_buffer=%.3f categories=%s share_market_data=%s",
            self._min_spread_pct, self._min_book_depth_usd,
            self._max_position_usd, self._fee_buffer_pct,
            self._categories or "*", self._share_market_data,
        )

    async def scan(self) -> list[Signal]:
        if not self._warned_not_implemented:
            self.log.warning("strategy not yet implemented — returning no signals")
            self._warned_not_implemented = True
        # TODO(arb): when implemented, call self._scanner.scan() and convert
        # each ArbitrageOpportunity into a paired (YES, NO) Signal set with
        # metadata={'arb_pair_id': ..., 'leg': 'yes'/'no'} so the executor
        # can size them as a unit. The category should be propagated from
        # the opportunity for the per-category exposure cap to apply.
        return []

    async def evaluate(self, signal: Signal) -> TradeIntent | None:
        # TODO(arb): enforce min_book_depth_usd and the leg-pair invariant
        # (refuse the YES leg if the matching NO leg is no longer fillable),
        # then return a TradeIntent with size_usd_hint <= max_position_usd.
        return None
