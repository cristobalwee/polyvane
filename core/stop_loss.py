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
from typing import Any, TYPE_CHECKING

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
        pnl = (current_price - entry_price) * shares
        loss_pct = (1.0 - current_price / entry_price) * 100

        log.warning(
            "STOP_LOSS | trade_id=%d strategy=%s market=%s exchange=%s "
            "entry=%.4f current=%.4f loss=%.1f%% pnl=$%.2f",
            trade_id, pos.get("strategy"), pos.get("market_id"), exchange,
            entry_price, current_price, loss_pct, pnl,
        )

        if not self._is_paper_for(exchange):
            if exchange == "kalshi":
                await self._place_kalshi_sell(pos, token_id, current_price, shares)
            else:
                await self._place_sell_order(pos, token_id, current_price, shares)

        self._journal.record_exit(
            trade_id=trade_id,
            outcome="lost",
            pnl=pnl,
            metadata={
                "exit_reason": "stop_loss",
                "exit_price": current_price,
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
            "exit_price": current_price,
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
    ) -> None:
        """Place a Kalshi exit order (sell YES or buy NO to close the position)."""
        client = self._client_for("kalshi")
        if not client.is_authenticated:
            raise RuntimeError("Kalshi client not authenticated; cannot place stop-loss order")

        direction = pos.get("direction", "YES")
        # To exit a YES position: sell YES contracts back.
        # To exit a NO position: sell NO contracts back.
        side = "yes" if direction == "YES" else "no"
        yes_price_cents = client.float_to_cents(current_price * 0.99)  # slight offset
        count = max(1, int(shares))

        await client.create_order(
            ticker=ticker,
            side=side,
            count=count,
            yes_price=yes_price_cents if side == "yes" else None,
            no_price=(100 - yes_price_cents) if side == "no" else None,
        )
