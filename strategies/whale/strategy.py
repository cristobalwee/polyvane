"""WhaleStrategy: copy-trade a curated list of high-quality wallets (scaffold).

Pipeline (when fully built):

  1. `WalletTracker.poll()` returns recent large positions across all
     tracked wallets.
  2. Group alerts by (market_id, direction); drop wallets whose score
     is below `min_wallet_score`, and (if `exclude_arb_bots`) drop any
     wallet flagged as a likely arb bot.
  3. Trigger a Signal whenever the surviving wallet count on one side
     of a market reaches `consensus_threshold * len(tracked_wallets)`.
     Edge is heuristic: max(min_wallet_score, mean wallet_score in the
     consensus group) - market_price.

Currently a stub: `scan()` returns an empty list and logs once. Enabling
in config does NOT submit orders.
"""
from __future__ import annotations

import logging
from typing import Any

from strategies.base import BaseStrategy, Signal, StrategyContext, TradeIntent

from .scorer import WalletScorer
from .tracker import WalletTracker


log = logging.getLogger("strategy.whale")


class WhaleStrategy(BaseStrategy):
    name = "whale"

    def __init__(self, params: dict[str, Any], context: StrategyContext) -> None:
        super().__init__(params, context)
        self._tracked_wallets: list[str] = list(params.get("tracked_wallets") or [])
        self._min_wallet_score: float = float(params.get("min_wallet_score", 0.7))
        self._min_trade_size_usd: float = float(params.get("min_trade_size_usd", 5000.0))
        self._consensus_threshold: float = float(params.get("consensus_threshold", 0.8))
        self._scan_interval_sec: float = float(params.get("scan_interval_sec", 60.0))
        self._rpc_url: str = str(params.get("rpc_provider_url", ""))
        self._exclude_arb_bots: bool = bool(params.get("exclude_arb_bots", True))
        self._categories: list[str] = list(params.get("categories") or [])

        self._tracker: WalletTracker | None = None
        self._scorer: WalletScorer | None = None
        self._warned_not_implemented = False

    async def setup(self) -> None:
        self._tracker = WalletTracker(
            client=self.context.client,
            rpc_provider_url=self._rpc_url,
            tracked_wallets=self._tracked_wallets,
            min_trade_size_usd=self._min_trade_size_usd,
        )
        self._scorer = WalletScorer(client=self.context.client)
        self.log.info(
            "WhaleStrategy ready (SCAFFOLD): tracked=%d min_score=%.2f "
            "min_trade=$%.0f consensus=%.2f exclude_arb_bots=%s categories=%s",
            len(self._tracked_wallets), self._min_wallet_score,
            self._min_trade_size_usd, self._consensus_threshold,
            self._exclude_arb_bots, self._categories or "*",
        )

    async def scan(self) -> list[Signal]:
        if not self._warned_not_implemented:
            self.log.warning("strategy not yet implemented — returning no signals")
            self._warned_not_implemented = True
        # TODO(whale): full implementation. Steps:
        #   - alerts = await self._tracker.poll()
        #   - resolve each alert.wallet_address through self._scorer to get
        #     a current WalletProfile and quality score
        #   - drop alerts whose wallet falls below `min_wallet_score`, and
        #     (if exclude_arb_bots) those flagged as is_likely_arb_bot
        #   - group surviving alerts by (market_id, direction)
        #   - emit one Signal per group whose participant count clears
        #     consensus_threshold * len(self._tracked_wallets)
        #   - confidence = mean wallet_score in the consensus group
        #   - apply `categories` filter against alert.category
        return []

    async def evaluate(self, signal: Signal) -> TradeIntent | None:
        # TODO(whale): apply per-strategy size cap, refuse signals where
        # market price has already moved past the consensus mean entry,
        # then return a TradeIntent with size_usd_hint set conservatively
        # (whale signals carry execution risk if the whale's already filled).
        return None
