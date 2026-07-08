"""Order execution engine.

Two modes:
  - paper: every intent is logged to the journal; no orders submitted.
  - live: orders are posted via the CLOB V2 client, with optional staged tranches.

Every trade is gated by `RiskManager.check_trade()` first. The executor never
sizes a trade itself — it forwards the strategy's hint (or None) to the risk
module, which has final authority.

V2 notes:
  * Order construction uses keyword args + `PartialCreateOrderOptions`.
  * `neg_risk=True` is required for negative-risk markets (multi-outcome,
    e.g. temperature buckets). We detect this from the signal metadata
    (set by the strategy when it builds the Signal).
  * No more `nonce` / `feeRateBps` / `taker` — uniqueness is timestamp-based
    in V2 and fees are charged at match time, not embedded in the order.
"""
from __future__ import annotations

import asyncio
import logging
import math
import uuid
from dataclasses import dataclass
from typing import Any

from core.client import ClobClient
from core.kalshi_client import (
    KalshiDuplicateOrder,
    KalshiInsufficientFundsError,
    KalshiOrderNotFilled,
    KalshiTransientError,
    parse_kalshi_number,
)
# Side-effect import: registers the TRADE log level + .trade() helper.
from core.logging_config import TRADE_LEVEL  # noqa: F401
from core.logger import TradeJournal, TradeRecord
from core.risk import RiskManager
from strategies.base import TradeIntent


log = logging.getLogger(__name__)


class StaleQuoteError(Exception):
    """The market re-quoted just before order submission no longer matches the
    signal price.

    The lazy strategy's thesis is price-IS-signal, so a book that moved
    materially between scan and submit invalidates the trade outright — most
    dramatically when a market crashes at resolution-relevant moments (trade 16:
    scanned at $0.69, the book collapsed to $0.02 eleven seconds before the
    crossing bid landed, and the IOC happily filled 6 contracts of a dead
    market). No order has been placed when this is raised.
    """
    pass


@dataclass
class ExecutionConfig:
    mode: str                  # 'paper' | 'live'  (Polymarket)
    kalshi_mode: str           # 'paper' | 'live'  (Kalshi; defaults to mode)
    order_type: str            # 'limit' | 'market'
    staged_entry: bool
    staged_tranches: int
    limit_offset_pct: float
    max_order_retries: int
    retry_backoff_sec: float
    # Maximum tolerated move between the scanned signal price and a fresh
    # quote taken immediately before submitting a Kalshi order. A larger move
    # means the signal is stale (e.g. the book crashed at dawn on a low-temp
    # market) — abort instead of crossing into a repriced market.
    max_price_drift: float = 0.10
    # Per-strategy live allowlist. When non-empty, ONLY these strategy
    # instance names may submit live orders — every other strategy stays in
    # paper even on a live exchange. Empty = legacy behavior (exchange mode
    # governs all strategies). Strategy names are the instance names recorded
    # in trade metadata (e.g. "lazy_kalshi"), matching dedup/bankroll keys.
    live_strategies: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ExecutionConfig":
        mode = str(d["mode"]).lower()
        if mode not in ("paper", "live"):
            raise ValueError(f"execution.mode must be 'paper' or 'live', got {mode!r}")
        kalshi_raw = str(d.get("kalshi_mode") or mode).lower()
        kalshi_mode = kalshi_raw if kalshi_raw in ("paper", "live") else mode
        return cls(
            mode=mode,
            kalshi_mode=kalshi_mode,
            order_type=str(d["order_type"]).lower(),
            staged_entry=bool(d["staged_entry"]),
            staged_tranches=max(1, int(d["staged_tranches"])),
            limit_offset_pct=float(d["limit_offset_pct"]),
            max_order_retries=int(d["max_order_retries"]),
            retry_backoff_sec=float(d["retry_backoff_sec"]),
            max_price_drift=float(d.get("max_price_drift", 0.10)),
            live_strategies=tuple(
                str(s) for s in (d.get("live_strategies") or [])
            ),
        )

    @property
    def is_paper(self) -> bool:
        return self.mode == "paper"

    def is_paper_for(self, exchange: str) -> bool:
        """Return True if the given exchange is in paper mode."""
        if exchange == "kalshi":
            return self.kalshi_mode == "paper"
        return self.mode == "paper"

    def is_paper_for_strategy(self, exchange: str, strategy: str) -> bool:
        """True if this (exchange, strategy) trade must be paper.

        A trade only goes live when (1) the exchange is in live mode AND
        (2) either no live allowlist is configured, or the strategy is on it.
        This lets a single live exchange host both live and paper strategies
        (e.g. lazy_kalshi live while weather_kalshi stays paper).
        """
        if self.is_paper_for(exchange):
            return True
        if self.live_strategies and strategy not in self.live_strategies:
            return True
        return False


@dataclass
class ExecutionResult:
    accepted: bool
    trade_id: int | None = None
    filled_usd: float = 0.0
    reason: str = ""
    order_responses: list[Any] | None = None


class Executor:
    """Routes approved trade intents to either the journal (paper) or the CLOB (live)."""

    def __init__(
        self,
        config: ExecutionConfig,
        risk: RiskManager,
        journal: TradeJournal,
        client: ClobClient,
        clients: dict[str, Any] | None = None,
    ) -> None:
        self.config = config
        self.risk = risk
        self.journal = journal
        self.client = client
        self.clients: dict[str, Any] = clients or {"polymarket": client}
        self._alert_hook: Any = None
        self._pause_hook: Any = None

    def set_alert_hook(self, hook: Any) -> None:
        self._alert_hook = hook

    def set_pause_hook(self, hook: Any) -> None:
        """Register a callback invoked when trading should halt (e.g. the
        account ran out of funds mid-scan). Wired by main.py to flip the
        HealthMonitor's balance-pause flag, which every strategy loop honors."""
        self._pause_hook = hook

    def _client_for(self, exchange: str) -> Any:
        return self.clients.get(exchange, self.client)

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        hook = self._alert_hook
        if hook is None:
            return
        try:
            hook(event_type, payload)
        except Exception:
            log.debug("alert hook raised", exc_info=True)

    @staticmethod
    def _is_neg_risk(intent: TradeIntent) -> bool:
        """Detect whether the target market is a negative-risk (multi-outcome)
        market. Temperature/weather buckets, election multi-candidates, etc.

        Strategies should set `neg_risk` in `signal.metadata` when known. We
        also fall back to category-based heuristics for known multi-outcome
        categories. Kalshi markets are always binary — never neg-risk.
        """
        # Kalshi binary markets are never neg-risk regardless of category.
        if getattr(intent.signal, "exchange", "polymarket") == "kalshi":
            return False
        meta = intent.signal.metadata or {}
        if "neg_risk" in meta:
            return bool(meta["neg_risk"])
        cat = (intent.signal.category or "").lower()
        return cat in ("temperature", "weather")

    async def _observe_book_depth(
        self, token_id: str | None, exchange: str = "polymarket"
    ) -> dict[str, Any] | None:
        client = self._client_for(exchange)
        if not token_id or not client.is_initialized:
            return None
        try:
            book = await client.get_orderbook(token_id)
        except Exception:
            log.debug("orderbook fetch failed for %s", token_id, exc_info=True)
            return None
        if not book:
            return None

        def _depth(side: list[Any] | None) -> tuple[float, float]:
            if not side:
                return 0.0, 0.0
            total_shares = 0.0
            total_usd = 0.0
            for level in side:
                try:
                    px = float(getattr(level, "price", level.get("price")))
                    sz = float(getattr(level, "size", level.get("size")))
                except Exception:
                    continue
                total_shares += sz
                total_usd += px * sz
            return total_shares, total_usd

        bids = getattr(book, "bids", None) or (book.get("bids") if isinstance(book, dict) else None)
        asks = getattr(book, "asks", None) or (book.get("asks") if isinstance(book, dict) else None)
        bid_sh, bid_usd = _depth(bids)
        ask_sh, ask_usd = _depth(asks)
        return {
            "bid_shares": bid_sh, "bid_usd": bid_usd,
            "ask_shares": ask_sh, "ask_usd": ask_usd,
            "total_usd": bid_usd + ask_usd,
        }

    async def submit(self, intent: TradeIntent, *, bankroll_usd: float) -> ExecutionResult:
        sig = intent.signal
        exchange = getattr(sig, "exchange", "polymarket")
        strategy_name = self.context_strategy(intent)
        is_paper = self.config.is_paper_for_strategy(exchange, strategy_name)
        bucket = sig.metadata.get("bucket") or sig.market_question or "?"

        if sig.price is None:
            log.warning(
                "TRADE_REJECTED | market=%s | exchange=%s | reason=missing_price",
                sig.market_id, exchange,
            )
            return ExecutionResult(False, reason="signal missing price")

        log.trade(
            "TRADE_SIGNAL | market=%s | exchange=%s | bucket=%s | edge=%.4f | confidence=%.2f | price=$%.4f",
            sig.market_id, exchange, bucket, sig.edge, sig.confidence, sig.price,
        )

        book_depth = await self._observe_book_depth(sig.token_id, exchange)

        market_volume_usd = sig.metadata.get("volume_usd")
        try:
            market_volume_usd = float(market_volume_usd) if market_volume_usd is not None else None
        except (TypeError, ValueError):
            market_volume_usd = None

        decision = self.risk.check_trade(
            strategy=self.context_strategy(intent),
            market_id=sig.market_id,
            category=sig.category,
            edge=sig.edge,
            price=sig.price,
            bankroll_usd=bankroll_usd,
            proposed_size_usd=intent.size_usd_hint,
            market_volume_usd=market_volume_usd,
        )
        if not decision.approved:
            log.trade(
                "TRADE_REJECTED | market=%s | exchange=%s | reason=%s",
                sig.market_id, exchange, decision.reason,
            )
            return ExecutionResult(False, reason=decision.reason)

        size_usd = decision.size_usd
        shares = size_usd / sig.price if sig.price > 0 else 0.0
        neg_risk = self._is_neg_risk(intent)

        log.info(
            "Sizing %s on %s: size=$%.2f tier=%s volume=$%s book_depth=%s neg_risk=%s",
            self.context_strategy(intent), sig.market_id, size_usd, decision.volume_tier,
            f"{market_volume_usd:.0f}" if market_volume_usd is not None else "n/a",
            f"${book_depth['total_usd']:.0f}" if book_depth else "n/a",
            neg_risk,
        )

        exchange_version = "v2" if exchange == "polymarket" else "v2"
        metadata = {
            "category": sig.category or "",
            "confidence": sig.confidence,
            "token_id": sig.token_id,
            "intent_reason": intent.reason,
            "mode": "paper" if is_paper else "live",
            "order_type": self.config.order_type,
            "staged_entry": self.config.staged_entry,
            "volume_tier": decision.volume_tier,
            "market_volume_usd": market_volume_usd,
            "book_depth_usd": book_depth["total_usd"] if book_depth else None,
            "neg_risk": neg_risk,
            "exchange": exchange,
            "exchange_version": exchange_version,
            **sig.metadata,
        }
        record = TradeRecord(
            strategy=self.context_strategy(intent),
            market_id=sig.market_id,
            market_question=sig.market_question,
            direction=sig.direction,
            entry_price=sig.price,
            size_usd=size_usd,
            shares=shares,
            edge_at_entry=sig.edge,
            metadata=metadata,
            exchange=exchange,
        )
        trade_id = self.journal.record_entry(record)
        self._emit("trade_executed", {
            "trade_id": trade_id,
            "strategy": record.strategy,
            "market_id": sig.market_id,
            "market_question": sig.market_question,
            "direction": sig.direction,
            "entry_price": sig.price,
            "size_usd": size_usd,
            "edge": sig.edge,
            "tier": decision.volume_tier,
            "mode": "paper" if is_paper else "live",
            "metadata": dict(metadata),
        })

        side_str = "BUY" if sig.direction == "YES" else "SELL"

        if is_paper:
            log.trade(
                "TRADE_PAPER | market=%s | exchange=%s | bucket=%s | side=%s | price=$%.4f | size=$%.2f | shares=%.2f | edge=%.4f | trade_id=%d",
                sig.market_id, exchange, bucket, side_str, sig.price, size_usd, shares, sig.edge, trade_id,
            )
            return ExecutionResult(
                accepted=True,
                trade_id=trade_id,
                filled_usd=size_usd,
                reason="paper",
            )

        # Live mode — route to the correct exchange.
        try:
            if exchange == "kalshi":
                responses = await self._place_kalshi_order(intent, size_usd)
            else:
                responses = await self._place_live_orders(intent, size_usd, neg_risk=neg_risk)
        except KalshiInsufficientFundsError as e:
            # The order was rejected — Kalshi placed nothing — so the journal row
            # written above is bogus. Void it (otherwise it counts as an open
            # position) and halt trading rather than retrying an order the
            # account can't fund.
            self.journal.void_entry(trade_id, reason="insufficient_funds")
            log.error(
                "TRADE_REJECTED | market=%s | exchange=%s | reason=insufficient_funds | error=%r",
                sig.market_id, exchange, e,
            )
            self._emit("insufficient_funds", {
                "strategy": record.strategy,
                "market_id": sig.market_id,
                "market_question": sig.market_question,
                "size_usd": size_usd,
            })
            if self._pause_hook is not None:
                try:
                    self._pause_hook("insufficient_funds")
                except Exception:
                    log.debug("pause hook raised", exc_info=True)
            return ExecutionResult(
                accepted=False,
                trade_id=trade_id,
                reason="insufficient_funds",
            )
        except StaleQuoteError as e:
            # The market repriced between scan and submit — the price-is-signal
            # thesis no longer holds. No order was placed; void the entry so it
            # doesn't consume a position slot or settle as a fake result.
            self.journal.void_entry(trade_id, reason="stale_quote")
            log.info(
                "TRADE_STALE_QUOTE | market=%s | exchange=kalshi | bucket=%s | %s | voided trade_id=%d",
                sig.market_id, bucket, e, trade_id,
            )
            return ExecutionResult(accepted=False, trade_id=trade_id, reason="stale_quote")
        except KalshiOrderNotFilled:
            # FOK couldn't fill against resting volume — an EXPECTED outcome on a
            # thin book, not an error. Nothing was placed, so void the entry (no
            # alert, no phantom). Info-level: this will be common on illiquid
            # city markets.
            self.journal.void_entry(trade_id, reason="unfilled")
            log.info(
                "TRADE_UNFILLED | market=%s | exchange=kalshi | bucket=%s | price=$%.4f | fok_no_volume trade_id=%d",
                sig.market_id, bucket, sig.price, trade_id,
            )
            return ExecutionResult(accepted=False, trade_id=trade_id, reason="unfilled")
        except KalshiDuplicateOrder:
            # An idempotent retry hit an order that already landed — the original
            # stands. Keep the journal entry (voiding would drop a real position;
            # the reviewer settles it) and warn rather than alert.
            log.warning(
                "TRADE_DUPLICATE | market=%s | exchange=kalshi | bucket=%s | order already exists, keeping entry trade_id=%d",
                sig.market_id, bucket, trade_id,
            )
            return ExecutionResult(
                accepted=True, trade_id=trade_id, filled_usd=size_usd, reason="duplicate",
            )
        except Exception as e:
            # The order attempt failed — nothing was placed on the exchange, so
            # the journal row written above is bogus. Void it; otherwise it
            # lingers as a phantom `pending` position that the resolution
            # reviewer later settles into a fake win/loss (this is what made a
            # dead v1 endpoint look like a stream of losing live trades).
            self.journal.void_entry(trade_id, reason="order_error")
            log.exception(
                "TRADE_REJECTED | market=%s | exchange=%s | reason=order_error | error=%r",
                sig.market_id, exchange, e,
            )
            return ExecutionResult(
                accepted=False,
                trade_id=trade_id,
                reason=f"order_error: {e}",
            )

        # Kalshi fill reconciliation. A fill-or-kill either fills fully or is
        # canceled (HTTP 201 with fill_count=0) — no exception in the latter
        # case. Void unfilled entries and true up filled ones to the actual
        # count / average price so the journal mirrors the real account.
        if exchange == "kalshi":
            resp0 = responses[0] if responses else {}
            fill_count = parse_kalshi_number(
                resp0.get("fill_count") if isinstance(resp0, dict) else None
            )
            if not fill_count or fill_count <= 0:
                self.journal.void_entry(trade_id, reason="unfilled")
                log.trade(
                    "TRADE_UNFILLED | market=%s | exchange=kalshi | bucket=%s | price=$%.4f | voided trade_id=%d",
                    sig.market_id, bucket, sig.price, trade_id,
                )
                return ExecutionResult(accepted=False, trade_id=trade_id, reason="unfilled")
            avg_price = parse_kalshi_number(
                resp0.get("average_fill_price") if isinstance(resp0, dict) else None
            ) or sig.price
            filled_usd = fill_count * avg_price
            self.journal.update_entry_fill(
                trade_id,
                entry_price=avg_price,
                shares=fill_count,
                size_usd=filled_usd,
            )
            size_usd = filled_usd

        for resp in responses:
            order_id = (
                getattr(resp, "order_id", None)
                or (resp.get("order_id") if isinstance(resp, dict) else None)
                or (resp.get("orderID") if isinstance(resp, dict) else None)
                or (resp.get("order", {}).get("order_id") if isinstance(resp, dict) else None)
                or "?"
            )
            log.trade(
                "TRADE_LIVE | market=%s | exchange=%s | bucket=%s | side=%s | price=$%.4f | size=$%.2f | order_id=%s",
                sig.market_id, exchange, bucket, side_str, sig.price, size_usd, order_id,
            )

        return ExecutionResult(
            accepted=True,
            trade_id=trade_id,
            filled_usd=size_usd,
            reason="live",
            order_responses=responses,
        )

    @staticmethod
    def context_strategy(intent: TradeIntent) -> str:
        meta = intent.signal.metadata or {}
        return str(meta.get("strategy") or meta.get("strategy_name") or "unknown")

    async def _place_kalshi_order(
        self,
        intent: TradeIntent,
        size_usd: float,
    ) -> list[Any]:
        """Place an immediate-or-cancel YES buy on Kalshi (V2 order endpoint).

        Kalshi's V2 book is expressed on the YES leg: a buy is a ``bid``. We
        price it slightly ABOVE the signal (cross the spread) so it's marketable
        — a resting buy below the ask never fills. IOC takes whatever volume is
        resting now and cancels the rest, so thin city books fill partially
        rather than being skipped wholesale (as fill-or-kill did). submit()
        reconciles the journal to the actual ``fill_count``; a zero-fill (empty
        book) comes back as fill_count=0 and is voided as unfilled.
        """
        sig = intent.signal
        ticker = sig.token_id
        if not ticker:
            raise RuntimeError("Kalshi live mode requires signal.token_id (ticker)")
        if sig.direction != "YES":
            # Strategies only ever buy YES. NO-side entries have no unambiguous
            # V2 mapping (buying NO ≠ selling YES when opening), so fail loud
            # rather than risk placing an inverted bet with real money.
            raise RuntimeError(f"Kalshi V2 orders only support YES entries, got {sig.direction!r}")
        client = self._client_for("kalshi")
        if not client.is_initialized:
            raise RuntimeError("Kalshi client not initialized")
        if not client.is_authenticated:
            raise RuntimeError("Kalshi client not authenticated; cannot place live orders")

        # Re-quote immediately before ordering. The scan that produced this
        # signal can be up to scan_interval + queue latency old; a crossing
        # IOC/FOK bid fills at whatever is resting NOW, not at the scanned
        # price. get_midpoint() falls back to 0.5 on an empty book, which the
        # drift check also (correctly) rejects.
        fresh = await client.get_midpoint(ticker)
        fresh_mid = float(fresh.get("mid", 0.5))
        drift = abs(fresh_mid - sig.price)
        if drift > self.config.max_price_drift:
            raise StaleQuoteError(
                f"{ticker}: signal price ${sig.price:.2f} but current mid "
                f"${fresh_mid:.2f} (drift {drift:.2f} > {self.config.max_price_drift:.2f})"
            )

        # Cross the spread so the FOK fills: pay up to slightly ABOVE the signal,
        # then round UP to the next whole cent (Kalshi binary markets tick in
        # cents; the client rejects sub-cent prices). Ceiling — not nearest —
        # keeps the order marketable after the snap.
        offset = self.config.limit_offset_pct
        raw_price = sig.price * (1.0 + offset)
        limit_price = min(0.99, max(0.01, math.ceil(raw_price * 100) / 100.0))

        # Contracts affordable at the limit price (each costs `limit_price` USD).
        count = max(1, int(size_usd / limit_price))

        # Stable per-order id so a transient retry is idempotent (Kalshi dedupes
        # on client_order_id) rather than double-filling.
        client_order_id = uuid.uuid4().hex

        attempt = 0
        while True:
            try:
                resp = await client.create_order(
                    ticker=ticker,
                    side="bid",
                    count=count,
                    price=limit_price,
                    time_in_force="immediate_or_cancel",
                    client_order_id=client_order_id,
                )
                log.info(
                    "Kalshi order posted: bid %s @ $%.4f count=%d coid=%s",
                    ticker, limit_price, count, client_order_id,
                )
                return [resp]
            except KalshiTransientError:
                # Network / 5xx blip — safe to retry with the same coid.
                attempt += 1
                if attempt >= self.config.max_order_retries:
                    raise
                await asyncio.sleep(self.config.retry_backoff_sec * attempt)
            # Everything else — insufficient funds, FOK no-fill, duplicate,
            # malformed/rejected order — is NOT retryable. Retrying a permanent
            # 4xx just re-submits the same client_order_id and trips
            # `order_already_exists`. Propagate to submit() to handle per-type.

    async def _place_live_orders(
        self,
        intent: TradeIntent,
        size_usd: float,
        *,
        neg_risk: bool,
    ) -> list[Any]:
        """Split into tranches if configured and post each via the CLOB V2 client."""
        sig = intent.signal
        if sig.token_id is None:
            raise RuntimeError("Live mode requires signal.token_id")
        if not self.client.is_initialized:
            raise RuntimeError("CLOB client not initialized")
        if not self.client.is_authenticated:
            raise RuntimeError("CLOB client not authenticated; cannot place live orders")

        try:
            from py_clob_client_v2 import Side  # type: ignore
        except ImportError as e:
            raise RuntimeError("py-clob-client-v2 required for live trading") from e

        tranches = self.config.staged_tranches if self.config.staged_entry else 1
        per_tranche_usd = size_usd / tranches
        responses: list[Any] = []

        side = Side.BUY if sig.direction == "YES" else Side.SELL
        offset = self.config.limit_offset_pct
        limit_price = sig.price * (1.0 - offset) if sig.direction == "YES" else sig.price * (1.0 + offset)
        limit_price = max(0.01, min(0.99, limit_price))

        tick_size = str(sig.metadata.get("tick_size") or "0.01")

        for i in range(tranches):
            shares = per_tranche_usd / sig.price if sig.price > 0 else 0.0
            attempt = 0
            while True:
                try:
                    resp = await self.client.create_and_post_order(
                        token_id=sig.token_id,
                        price=limit_price,
                        size=shares,
                        side=side,
                        tick_size=tick_size,
                        neg_risk=neg_risk,
                    )
                    responses.append(resp)
                    log.info(
                        "Live tranche %d/%d posted: %s %s @ %.4f size=%.4f neg_risk=%s",
                        i + 1, tranches, side, sig.market_id, limit_price, shares, neg_risk,
                    )
                    break
                except Exception:
                    attempt += 1
                    if attempt >= self.config.max_order_retries:
                        raise
                    await asyncio.sleep(self.config.retry_backoff_sec * attempt)

        return responses
