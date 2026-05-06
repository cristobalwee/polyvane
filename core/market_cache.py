"""Process-local TTL cache for market data shared across strategies.

The weather strategy already pulls Polymarket events on every scan. The
arbitrage scanner wants the same prices/depth without re-hitting Gamma —
so each strategy that fetches market data writes into this cache, and any
strategy that wants to read can do so without re-fetching.

Design choices:

  - **In-memory only.** No persistence. Cache exists for the bot's lifetime
    and is rebuilt from scratch on restart. We're not trying to survive
    crashes — we're trying to avoid burning API quota during one scan
    cycle.

  - **Per-entry TTL.** Each writer specifies how long its data stays
    fresh; reads return None for expired entries. Defaults to 30s, which
    matches the arb scanner cadence and is just under typical Gamma
    refresh rates.

  - **Thread/coroutine-safe via a single asyncio.Lock.** Strategies run
    concurrently; the lock guards the dict but is held for microseconds
    per op, so contention is negligible.

  - **Free-form value.** The cache stores `MarketSnapshot` objects whose
    `data` field is a dict. The schema is by convention, not validated:
    weather writers populate `yes_price`, `no_price`, `book_depth_usd`,
    `volume_24h`, `category`, etc. Future strategies can add fields
    without touching the cache contract.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any


DEFAULT_TTL_SEC = 30.0


@dataclass
class MarketSnapshot:
    """One cached observation of a market.

    `data` is the per-strategy convention. Conventional keys:
      - 'yes_price', 'no_price'              float | None
      - 'book_depth_usd_yes', 'book_depth_usd_no'  float | None
      - 'volume_24h_usd'                     float
      - 'liquidity_usd'                      float
      - 'category'                           str    (e.g. 'weather', 'crypto')
      - 'token_id_yes', 'token_id_no'        str | None
      - 'question'                           str
      - 'end_date_utc'                       str (ISO)
    """
    market_id: str
    written_at: float            # monotonic seconds, from time.monotonic()
    ttl_sec: float
    source_strategy: str          # which strategy wrote this entry
    data: dict[str, Any] = field(default_factory=dict)

    def is_fresh(self, now: float | None = None) -> bool:
        t = now if now is not None else time.monotonic()
        return (t - self.written_at) < self.ttl_sec


class MarketCache:
    """Process-local TTL cache. Construct once, share across strategies."""

    def __init__(self, *, default_ttl_sec: float = DEFAULT_TTL_SEC) -> None:
        self._default_ttl = float(default_ttl_sec)
        self._entries: dict[str, MarketSnapshot] = {}
        self._lock = asyncio.Lock()

    @property
    def default_ttl_sec(self) -> float:
        return self._default_ttl

    async def put(
        self,
        market_id: str,
        data: dict[str, Any],
        *,
        source_strategy: str,
        ttl_sec: float | None = None,
    ) -> None:
        snap = MarketSnapshot(
            market_id=market_id,
            written_at=time.monotonic(),
            ttl_sec=ttl_sec if ttl_sec is not None else self._default_ttl,
            source_strategy=source_strategy,
            data=dict(data),
        )
        async with self._lock:
            self._entries[market_id] = snap

    async def put_many(
        self,
        rows: list[tuple[str, dict[str, Any]]],
        *,
        source_strategy: str,
        ttl_sec: float | None = None,
    ) -> None:
        ttl = ttl_sec if ttl_sec is not None else self._default_ttl
        now = time.monotonic()
        async with self._lock:
            for market_id, data in rows:
                self._entries[market_id] = MarketSnapshot(
                    market_id=market_id,
                    written_at=now,
                    ttl_sec=ttl,
                    source_strategy=source_strategy,
                    data=dict(data),
                )

    async def get(self, market_id: str) -> MarketSnapshot | None:
        async with self._lock:
            snap = self._entries.get(market_id)
        if snap is None or not snap.is_fresh():
            return None
        return snap

    async def get_fresh(self, *, category: str | None = None) -> list[MarketSnapshot]:
        """All non-expired snapshots, optionally filtered by data['category']."""
        now = time.monotonic()
        async with self._lock:
            snaps = [s for s in self._entries.values() if s.is_fresh(now)]
        if category is not None:
            snaps = [s for s in snaps if s.data.get("category") == category]
        return snaps

    async def evict_stale(self) -> int:
        now = time.monotonic()
        async with self._lock:
            stale = [k for k, s in self._entries.items() if not s.is_fresh(now)]
            for k in stale:
                del self._entries[k]
        return len(stale)

    async def clear(self) -> None:
        async with self._lock:
            self._entries.clear()

    async def stats(self) -> dict[str, int]:
        now = time.monotonic()
        async with self._lock:
            total = len(self._entries)
            fresh = sum(1 for s in self._entries.values() if s.is_fresh(now))
        return {"total": total, "fresh": fresh, "stale": total - fresh}
