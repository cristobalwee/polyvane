"""Strategy base class + Signal dataclass.

A strategy module subclasses `BaseStrategy`, implements `scan()` (returns a
list of `Signal`s) and `evaluate(signal)` (returns a `TradeIntent | None`).
The main loop polls each enabled strategy on its configured interval,
runs every emitted signal through the risk gate, and hands approved intents
to the executor.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


log = logging.getLogger(__name__)


@dataclass
class Signal:
    """A candidate trade idea emitted by a strategy.

    `edge` is the strategy's estimate of (true_prob - market_price), in [-1, 1].
    `confidence` is a strategy-defined score in [0, 1] used for tie-breaking
    and per-strategy reporting; it is NOT directly consumed by the risk module.
    `metadata` is freeform — anything the strategy wants logged with the trade.
    """
    market_id: str
    direction: str  # 'YES' or 'NO'
    edge: float
    confidence: float
    market_question: str | None = None
    price: float | None = None       # market price at signal time
    category: str | None = None      # used by per-category exposure cap
    token_id: str | None = None      # CLOB token id, if known
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.direction not in ("YES", "NO"):
            raise ValueError(f"direction must be 'YES' or 'NO', got {self.direction!r}")
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError(f"confidence must be in [0, 1], got {self.confidence}")


@dataclass
class TradeIntent:
    """Strategy's decision after evaluating its own signal.

    The executor consumes this. `size_usd` is a hint; the risk module has
    final authority on size and may shrink (or reject) it.
    """
    signal: Signal
    size_usd_hint: float | None = None  # None = let the risk module decide via Kelly
    reason: str = ""


class BaseStrategy(ABC):
    """Abstract base. Concrete strategies live under `strategies/<name>.py`."""

    # Each subclass MUST set this. Used to look up per-strategy config and
    # tag trades in the journal.
    name: str = ""

    def __init__(self, params: dict[str, Any], context: "StrategyContext") -> None:
        if not self.name:
            raise RuntimeError(f"{type(self).__name__}.name must be set on the subclass")
        self.params = params or {}
        self.context = context
        self.log = logging.getLogger(f"strategy.{self.name}")

    async def setup(self) -> None:
        """Optional one-time initialization (open data connections, etc.)."""
        return None

    async def teardown(self) -> None:
        """Optional cleanup on shutdown."""
        return None

    @abstractmethod
    async def scan(self) -> list[Signal]:
        """Poll data sources and return zero or more candidate signals."""
        raise NotImplementedError

    @abstractmethod
    async def evaluate(self, signal: Signal) -> TradeIntent | None:
        """Decide whether (and how) to act on a signal. Return None to skip."""
        raise NotImplementedError


@dataclass
class StrategyContext:
    """Read-only handle passed to strategies at construction.

    Strategies should NOT call the executor directly — the main loop owns
    routing. They MAY use the client for read-only market data lookups,
    use `market_cache` to share/discover snapshots populated by other
    strategies, and read the `journal` to rebuild in-memory state at
    setup() (e.g. lazy hydrates its ladder-fired set from open positions
    so a bot restart doesn't re-fire rungs that already have open trades).
    Strategies must NEVER write to the journal directly; the executor owns
    that path.
    """
    client: Any              # core.client.ClobClient
    config: dict[str, Any]   # full parsed config (read-only)
    market_cache: Any = None  # core.market_cache.MarketCache | None
    journal: Any = None       # core.logger.TradeJournal | None — read-only use
