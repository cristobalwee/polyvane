"""Wallet activity tracker (stub).

Watches a fixed list of wallet addresses on Polygon and produces a stream
of `WhaleAlert`s whenever a tracked wallet enters/exits a position above
the configured floor.

Two architectural options for the production build (decide at impl time):

  - **Polling Polymarket Data API.** Periodic GET against the public
    `/positions?user=<addr>` endpoint, diff the position set against the
    last snapshot, emit an alert per position-delta crossing the floor.
    Simpler, no chain access needed, ~minutes of latency.

  - **WebSocket / RPC subscription.** Subscribe to USDC + CTF transfer
    logs from the tracked addresses via Polygon RPC, decode the order
    fills inline. Lower latency, more brittle, costs an RPC plan.

The scaffold leaves the decision open; both paths fit behind the
`WalletTracker.poll()` interface below.
"""
from __future__ import annotations

import logging
from typing import Any

from .models import WhaleAlert


log = logging.getLogger("strategy.whale.tracker")


class WalletTracker:
    def __init__(
        self,
        *,
        client: Any,                    # core.client.ClobClient (read-only)
        rpc_provider_url: str,
        tracked_wallets: list[str],
        min_trade_size_usd: float,
    ) -> None:
        self._client = client
        self._rpc_url = rpc_provider_url
        self._wallets = list(tracked_wallets)
        self.min_trade_size_usd = float(min_trade_size_usd)

        # TODO(whale): persist last-seen position snapshots per wallet so
        # restart doesn't replay the entire backfill window.
        self._last_seen_positions: dict[str, dict[str, float]] = {}

    async def poll(self) -> list[WhaleAlert]:
        """Return new alerts since the last call. Empty list when idle.

        TODO(whale): full implementation. Steps:
          - for each wallet in `self._wallets`, fetch current open positions
          - diff against `_last_seen_positions[wallet]`; produce a
            position delta {market_id: usd_delta} per wallet
          - skip deltas under `min_trade_size_usd`
          - look up market metadata (question, category, token_id_yes/no)
            from the shared MarketCache or a direct Gamma fetch
          - resolve the wallet's score (see scorer.WalletScorer)
          - emit one WhaleAlert per surviving delta
          - persist current positions back to `_last_seen_positions`
        """
        log.debug("WalletTracker.poll() not implemented (wallets=%d)", len(self._wallets))
        return []
