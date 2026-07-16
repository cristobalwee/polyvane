"""Stop-loss position monitor.

Runs on a configurable interval, fetches current midpoint prices for all
open positions that carry a token_id, and exits any that have lost at least
`stop_loss_pct` of their entry value.

Stop condition: current_price <= entry_price * (1 - stop_loss_pct)
Default 50%: triggers when price halves from entry.

Paper mode: records the exit directly in the journal (no order submitted).
Live mode: places a SELL limit order on the CLOB, then records the exit.

Multi-exchange: each position's exchange is read from the `exchange` column
(added in the schema migration) and the appropriate client is used for
midpoint queries and exit orders.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import uuid
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

from core.kalshi_client import parse_kalshi_number
from core.logger import TradeJournal

if TYPE_CHECKING:
    from core.executor import ExecutionConfig


log = logging.getLogger(__name__)


class StopLossManager:
    def __init__(
        self,
        journal: TradeJournal,
        client: Any,
        *,
        clients: dict[str, Any] | None = None,
        is_paper: bool,
        stop_loss_pct: float = 0.50,
        exec_config: "ExecutionConfig | None" = None,
    ) -> None:
        self._journal = journal
        self._client = client
        self._clients: dict[str, Any] = clients or {"polymarket": client}
        self._is_paper = is_paper
        self._exec_config = exec_config
        self._stop_loss_pct = stop_loss_pct
        self._alert_hook: Any = None

    def set_alert_hook(self, hook: Any) -> None:
        self._alert_hook = hook

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        hook = self._alert_hook
        if hook is None:
            return
        try:
            hook(event_type, payload)
        except Exception:
            log.debug("alert hook raised on stop_loss", exc_info=True)

    def _is_paper_for(self, exchange: str) -> bool:
        """Per-exchange paper check; falls back to global is_paper."""
        if self._exec_config is not None:
            return self._exec_config.is_paper_for(exchange)
        return self._is_paper

    def _client_for(self, exchange: str) -> Any:
        return self._clients.get(exchange, self._client)

    async def check_and_execute(self) -> int:
        """Check all open positions; stop out any that have lost >= stop_loss_pct.

        Returns the count of positions stopped.
        """
        open_positions = self._journal.open_positions()
        if not open_positions:
            return 0

        # Build candidate list with (pos, token_id, exchange).
        candidates: list[tuple[dict[str, Any], str, str]] = []
        for pos in open_positions:
            try:
                meta = json.loads(pos.get("metadata_json") or "{}")
            except (json.JSONDecodeError, TypeError):
                meta = {}
            token_id = meta.get("token_id") or ""
            if not token_id:
                continue
            # Skip positions whose market has already closed: there is no book
            # to exit into, and a sell order 404s (market_not_found). Trade 16's
            # zombie position fired one every 60s all night (2026-07-08),
            # paging the operator every alert-cooldown window until Kalshi
            # finalized the market. Post-close settlement is the resolution
            # reviewer's job, not the stop-loss's.
            end_iso = meta.get("end_utc")
            if end_iso:
                try:
                    end_dt = datetime.fromisoformat(end_iso)
                    if end_dt <= datetime.now(timezone.utc):
                        continue
                except (ValueError, TypeError):
                    pass
            # Exchange: prefer the dedicated column, fall back to metadata.
            exchange = pos.get("exchange") or meta.get("exchange") or "polymarket"
            candidates.append((pos, token_id, exchange))

        if not candidates:
            return 0

        marks = await self._fetch_midpoints(candidates)

        stopped = 0
        for pos, token_id, exchange in candidates:
            current_price = marks.get(token_id)
            if current_price is None:
                continue

            entry_price = float(pos.get("entry_price") or 0.0)
            if entry_price <= 0:
                continue

            if current_price > entry_price * (1.0 - self._stop_loss_pct):
                continue

            try:
                await self._execute_stop(pos, token_id, entry_price, current_price, exchange)
                stopped += 1
            except Exception:
                log.exception(
                    "stop_loss: failed to exit trade_id=%s market=%s exchange=%s",
                    pos.get("id"), pos.get("market_id"), exchange,
                )

        return stopped

    async def _fetch_midpoints(
        self,
        candidates: list[tuple[dict[str, Any], str, str]],
    ) -> dict[str, float]:
        """Fetch midpoints for all candidates, routing to the correct client per exchange."""
        if not candidates:
            return {}

        async def _fetch(tid: str, exchange: str) -> tuple[str, float | None]:
            client = self._client_for(exchange)
            if not client.is_initialized:
                return tid, None
            try:
                if exchange == "kalshi":
                    # Mark at the best YES bid — the price a stop-out can
                    # actually sell into. The midpoint of a one-sided book is
                    # fiction: when the ask side vanishes the mid craters
                    # through the trigger and the journal booked exits at
                    # $0.01–0.03 that no order could have achieved (trades
                    # 59/60/305). No bid at all means there is nothing to
                    # exit into — skip; the IOC would just cancel unfilled.
                    book = await asyncio.wait_for(
                        client.get_orderbook(tid, depth=1), timeout=5.0
                    )
                    ob = book.get("orderbook_fp") or book.get("orderbook") or book
                    yes_levels = ob.get("yes_dollars") or ob.get("yes") or []
                    return tid, client._best_book_price(yes_levels)
                resp = await asyncio.wait_for(client.get_midpoint(tid), timeout=5.0)
                mid = resp.get("mid") if isinstance(resp, dict) else resp
                return tid, float(mid) if mid is not None else None
            except Exception:
                return tid, None

        # Deduplicate token_ids but preserve their exchange.
        seen: dict[str, str] = {}
        for _, tid, exchange in candidates:
            if tid not in seen:
                seen[tid] = exchange

        results = await asyncio.gather(*(_fetch(tid, exch) for tid, exch in seen.items()))
        return {tid: mid for tid, mid in results if mid is not None}

    async def _execute_stop(
        self,
        pos: dict[str, Any],
        token_id: str,
        entry_price: float,
        current_price: float,
        exchange: str,
    ) -> None:
        trade_id = int(pos["id"])
        shares = float(pos.get("shares") or 0.0)
        size_usd = float(pos.get("size_usd") or 0.0)
        loss_pct = (1.0 - current_price / entry_price) * 100

        log.warning(
            "STOP_LOSS | trade_id=%d strategy=%s market=%s exchange=%s "
            "entry=%.4f current=%.4f loss=%.1f%%",
            trade_id, pos.get("strategy"), pos.get("market_id"), exchange,
            entry_price, current_price, loss_pct,
        )

        # Paper mode books the exit at the observed mark. Live mode books it
        # at what the exchange actually reports back — never at the mark.
        exit_price = current_price
        exited_shares = shares

        if not self._is_paper_for(exchange):
            if exchange == "kalshi":
                fill_count, avg_fill = await self._place_kalshi_sell(
                    pos, token_id, current_price, shares
                )
                if fill_count is None:
                    # Dust position (< 0.01 sellable) — left for the reviewer.
                    return
                if fill_count <= 0:
                    # IOC came back canceled with zero fills: the bid we
                    # marked against was gone by the time the order landed.
                    # The position is still LIVE on Kalshi, so the journal
                    # row must stay open — recording an exit here is how
                    # closed-in-journal/open-on-exchange zombies were born.
                    # The next check retries; alert so the operator knows.
                    log.warning(
                        "STOP_LOSS_UNFILLED | trade_id=%d market=%s — IOC canceled "
                        "with no fills, position remains open; will retry",
                        trade_id, pos.get("market_id"),
                    )
                    self._emit("stop_loss_unfilled", {
                        "trade_id": trade_id,
                        "strategy": pos.get("strategy"),
                        "market_id": pos.get("market_id"),
                        "entry_price": entry_price,
                        "mark_price": current_price,
                        "shares": shares,
                        "exchange": exchange,
                    })
                    return
                exit_price = avg_fill if avg_fill is not None else current_price
                if fill_count < shares - 0.005:
                    # Partial fill: shrink the journal row to the remainder
                    # and bank the realized chunk in metadata. The row stays
                    # open so the next check (or the reviewer at settlement)
                    # handles what's still on the exchange.
                    realized = (exit_price - entry_price) * fill_count
                    remaining = round(shares - fill_count, 2)
                    self._journal.apply_partial_exit(
                        trade_id,
                        shares=remaining,
                        size_usd=round(entry_price * remaining, 6),
                        realized_pnl=realized,
                        metadata={
                            "last_partial_exit_price": exit_price,
                            "last_partial_exit_shares": fill_count,
                        },
                    )
                    log.warning(
                        "STOP_LOSS_PARTIAL | trade_id=%d market=%s filled %.2f/%.2f "
                        "@ $%.2f (realized $%.2f), %.2f contracts remain open",
                        trade_id, pos.get("market_id"), fill_count, shares,
                        exit_price, realized, remaining,
                    )
                    self._emit("stop_loss_partial", {
                        "trade_id": trade_id,
                        "strategy": pos.get("strategy"),
                        "market_id": pos.get("market_id"),
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "filled_shares": fill_count,
                        "remaining_shares": remaining,
                        "pnl": realized,
                        "exchange": exchange,
                    })
                    return
                exited_shares = fill_count
            else:
                await self._place_sell_order(pos, token_id, current_price, shares)

        pnl = (exit_price - entry_price) * exited_shares
        self._journal.record_exit(
            trade_id=trade_id,
            outcome="lost",
            pnl=pnl,
            metadata={
                "exit_reason": "stop_loss",
                "exit_price": exit_price,
                "exit_mark": current_price,
                "stop_loss_pct": self._stop_loss_pct,
                "exchange": exchange,
            },
        )

        self._emit("stop_loss_triggered", {
            "trade_id": trade_id,
            "strategy": pos.get("strategy"),
            "market_id": pos.get("market_id"),
            "market_question": pos.get("market_question"),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "loss_pct": loss_pct,
            "pnl": pnl,
            "size_usd": size_usd,
            "exchange": exchange,
        })

    async def _place_sell_order(
        self,
        pos: dict[str, Any],
        token_id: str,
        current_price: float,
        shares: float,
    ) -> None:
        try:
            meta = json.loads(pos.get("metadata_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            meta = {}

        try:
            from py_clob_client_v2 import Side  # type: ignore
        except ImportError as e:
            raise RuntimeError("py-clob-client-v2 required for live stop-loss orders") from e

        if not self._client.is_authenticated:
            raise RuntimeError("CLOB client not authenticated; cannot place stop-loss sell order")

        neg_risk = bool(meta.get("neg_risk", False))
        tick_size = str(meta.get("tick_size") or "0.01")
        limit_price = max(0.01, current_price * 0.99)

        await self._client.create_and_post_order(
            token_id=token_id,
            price=limit_price,
            size=shares,
            side=Side.SELL,
            tick_size=tick_size,
            neg_risk=neg_risk,
        )

    async def _place_kalshi_sell(
        self,
        pos: dict[str, Any],
        ticker: str,
        current_price: float,
        shares: float,
    ) -> tuple[float | None, float | None]:
        """Place a Kalshi exit order to close a YES position (V2 endpoint).

        Closing a YES long = selling YES = a ``side="ask"`` order. We price it
        slightly BELOW the current mark and use immediate-or-cancel so we take
        whatever exit liquidity exists right now rather than resting (a stop-out
        should get out, even partially, not sit unfilled).

        Returns ``(fill_count, average_fill_price)`` parsed from the V2
        response. ``(0.0, None)`` means the IOC canceled with no fills — the
        position is still open on the exchange. ``(None, None)`` means no
        order was placed (dust position left for the reviewer)."""
        client = self._client_for("kalshi")
        if not client.is_authenticated:
            raise RuntimeError("Kalshi client not authenticated; cannot place stop-loss order")

        direction = pos.get("direction", "YES")
        if direction != "YES":
            # Entries are YES-only (see executor); a NO position shouldn't exist.
            raise RuntimeError(f"Kalshi V2 stop-loss only closes YES positions, got {direction!r}")

        # Cross down to fill, then round DOWN to a whole cent (binary markets
        # tick in cents; sub-cent prices are rejected). Floor keeps the exit
        # marketable after the snap.
        limit_price = min(0.99, max(0.01, math.floor(current_price * 0.99 * 100) / 100.0))
        # Sell EXACTLY what we hold — Kalshi contracts are fractional under the
        # fp schema. Rounding up oversells (a 0.3-contract position is not 1
        # contract); truncating strands the remainder untracked.
        count = round(shares, 2)
        if count < 0.01:
            log.warning(
                "stop_loss: trade_id=%s holds %.4f contracts (< 0.01 sellable) — leaving for the reviewer",
                pos.get("id"), shares,
            )
            return None, None

        resp = await client.create_order(
            ticker=ticker,
            side="ask",
            count=count,
            price=limit_price,
            time_in_force="immediate_or_cancel",
            client_order_id=uuid.uuid4().hex,
        )
        # V2 reports fill_count / average_fill_price on the 201 — an IOC that
        # found no bid comes back canceled with fill_count=0, NOT an error.
        resp0 = resp if isinstance(resp, dict) else {}
        fill_count = parse_kalshi_number(resp0.get("fill_count")) or 0.0
        avg_fill = parse_kalshi_number(resp0.get("average_fill_price"))
        return fill_count, avg_fill
