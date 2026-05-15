"""Stop-loss position monitor.

Runs on a configurable interval, fetches current midpoint prices for all
open positions that carry a token_id, and exits any that have lost at least
`stop_loss_pct` of their entry value.

Stop condition: current_price <= entry_price * (1 - stop_loss_pct)
Default 50%: triggers when price halves from entry.

Paper mode: records the exit directly in the journal (no order submitted).
Live mode: places a SELL limit order on the CLOB, then records the exit.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from core.logger import TradeJournal


log = logging.getLogger(__name__)


class StopLossManager:
    def __init__(
        self,
        journal: TradeJournal,
        client: Any,
        *,
        is_paper: bool,
        stop_loss_pct: float = 0.50,
    ) -> None:
        self._journal = journal
        self._client = client
        self._is_paper = is_paper
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

    async def check_and_execute(self) -> int:
        """Check all open positions; stop out any that have lost >= stop_loss_pct.

        Returns the count of positions stopped.
        """
        open_positions = self._journal.open_positions()
        if not open_positions:
            return 0

        candidates: list[tuple[dict[str, Any], str]] = []
        for pos in open_positions:
            try:
                meta = json.loads(pos.get("metadata_json") or "{}")
            except (json.JSONDecodeError, TypeError):
                meta = {}
            token_id = meta.get("token_id") or ""
            if token_id:
                candidates.append((pos, token_id))

        if not candidates:
            return 0

        marks = await self._fetch_midpoints({tid for _, tid in candidates})

        stopped = 0
        for pos, token_id in candidates:
            current_price = marks.get(token_id)
            if current_price is None:
                continue

            entry_price = float(pos.get("entry_price") or 0.0)
            if entry_price <= 0:
                continue

            if current_price > entry_price * (1.0 - self._stop_loss_pct):
                continue

            try:
                await self._execute_stop(pos, token_id, entry_price, current_price)
                stopped += 1
            except Exception:
                log.exception(
                    "stop_loss: failed to exit trade_id=%s market=%s",
                    pos.get("id"), pos.get("market_id"),
                )

        return stopped

    async def _fetch_midpoints(self, token_ids: set[str]) -> dict[str, float]:
        if not token_ids or not self._client.is_initialized:
            return {}

        async def _fetch(tid: str) -> tuple[str, float | None]:
            try:
                resp = await asyncio.wait_for(self._client.get_midpoint(tid), timeout=5.0)
                mid = resp.get("mid") if isinstance(resp, dict) else resp
                return tid, float(mid) if mid is not None else None
            except Exception:
                return tid, None

        results = await asyncio.gather(*(_fetch(tid) for tid in token_ids))
        return {tid: mid for tid, mid in results if mid is not None}

    async def _execute_stop(
        self,
        pos: dict[str, Any],
        token_id: str,
        entry_price: float,
        current_price: float,
    ) -> None:
        trade_id = int(pos["id"])
        shares = float(pos.get("shares") or 0.0)
        size_usd = float(pos.get("size_usd") or 0.0)
        pnl = (current_price - entry_price) * shares
        loss_pct = (1.0 - current_price / entry_price) * 100

        log.warning(
            "STOP_LOSS | trade_id=%d strategy=%s market=%s "
            "entry=%.4f current=%.4f loss=%.1f%% pnl=$%.2f",
            trade_id, pos.get("strategy"), pos.get("market_id"),
            entry_price, current_price, loss_pct, pnl,
        )

        if not self._is_paper:
            await self._place_sell_order(pos, token_id, current_price, shares)

        self._journal.record_exit(
            trade_id=trade_id,
            outcome="lost",
            pnl=pnl,
            metadata={
                "exit_reason": "stop_loss",
                "exit_price": current_price,
                "stop_loss_pct": self._stop_loss_pct,
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
