"""Polymarket CLOB V2 API client wrapper.

Wraps `py-clob-client-v2` with:
  - Two initialization modes:
      paper: unauthenticated client, read-only market data only — no PK required
      live:  authenticated client (L1 wallet key + L2 API creds)
  - async-friendly call surface (sync calls are dispatched to a thread)
  - exponential-backoff retry on transient errors

Polymarket migrated to CLOB V2 on 2026-04-28. The legacy `py-clob-client`
package no longer works against production. Even paper-mode market data
reads now go through the V2 backend, so this module is required for any
mode of operation.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


log = logging.getLogger(__name__)


class TransientAPIError(Exception):
    """Raised when an upstream call should be retried."""


@dataclass
class ClientConfig:
    clob_host: str
    chain_id: int
    request_timeout_sec: float
    retry_max_attempts: int
    retry_initial_delay_sec: float
    retry_max_delay_sec: float

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ClientConfig":
        return cls(
            clob_host=str(d["clob_host"]),
            chain_id=int(d["chain_id"]),
            request_timeout_sec=float(d["request_timeout_sec"]),
            retry_max_attempts=int(d["retry_max_attempts"]),
            retry_initial_delay_sec=float(d["retry_initial_delay_sec"]),
            retry_max_delay_sec=float(d["retry_max_delay_sec"]),
        )


class ClobClient:
    """Async wrapper around py-clob-client-v2's ClobClient.

    Initialization is split from construction so the bot can boot without
    credentials in paper mode. The two entry points:
      * `initialize_unauthenticated()` — read-only market data, no PK required
      * `initialize_authenticated(...)` — full trading, L1 + L2 auth
    """

    def __init__(self, config: ClientConfig) -> None:
        self.config = config
        self._inner: Any = None
        self._initialized = False
        self._authenticated = False

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated

    def initialize_unauthenticated(self) -> None:
        """Bring up a read-only V2 client. Sufficient for paper-mode market data."""
        try:
            from py_clob_client_v2 import ClobClient as _Inner  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "py-clob-client-v2 is not installed. Run `pip install -r requirements.txt`."
            ) from e

        self._inner = _Inner(
            host=self.config.clob_host,
            chain_id=self.config.chain_id,
        )
        self._initialized = True
        self._authenticated = False
        log.info(
            "CLOB V2 client initialized (unauthenticated) host=%s chain_id=%s",
            self.config.clob_host, self.config.chain_id,
        )

    def initialize_authenticated(
        self,
        private_key: str,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
    ) -> None:
        """Bring up a fully-authenticated V2 client for live trading.

        V2 detects signature type from the wallet key, so no `signature_type`
        or `funder` arguments are passed.
        """
        try:
            from py_clob_client_v2 import ApiCreds, ClobClient as _Inner  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "py-clob-client-v2 is not installed. Run `pip install -r requirements.txt`."
            ) from e

        if not (api_key and api_secret and api_passphrase):
            raise ValueError(
                "Live mode requires CLOB_API_KEY, CLOB_SECRET, CLOB_PASS_PHRASE. "
                "Derive them with: PK=0x... python -m core.derive_creds"
            )

        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
        self._inner = _Inner(
            host=self.config.clob_host,
            chain_id=self.config.chain_id,
            key=private_key,
            creds=creds,
        )
        self._initialized = True
        self._authenticated = True
        log.info(
            "CLOB V2 client initialized (authenticated) host=%s chain_id=%s",
            self.config.clob_host, self.config.chain_id,
        )

    def _require_init(self) -> Any:
        if not self._initialized or self._inner is None:
            raise RuntimeError("ClobClient not initialized. Call initialize_*() first.")
        return self._inner

    def _require_auth(self) -> Any:
        inner = self._require_init()
        if not self._authenticated:
            raise RuntimeError(
                "Operation requires an authenticated client. "
                "Currently running in unauthenticated (paper) mode."
            )
        return inner

    async def _retrying_call(self, fn: Callable[[], Any], *, op: str) -> Any:
        """Run a sync callable in a worker thread, with exponential backoff."""
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.config.retry_max_attempts),
            wait=wait_exponential(
                multiplier=self.config.retry_initial_delay_sec,
                max=self.config.retry_max_delay_sec,
            ),
            retry=retry_if_exception_type((TransientAPIError, asyncio.TimeoutError, ConnectionError)),
            reraise=True,
        ):
            with attempt:
                try:
                    return await asyncio.wait_for(
                        asyncio.to_thread(fn),
                        timeout=self.config.request_timeout_sec,
                    )
                except (asyncio.TimeoutError, ConnectionError):
                    log.warning("HEALTH_API_ERROR | endpoint=%s | reason=transient | retrying", op)
                    raise
                except Exception as e:
                    msg = str(e).lower()
                    if any(s in msg for s in ("timeout", "temporarily", "503", "502", "504", "rate limit", "429")):
                        log.warning("HEALTH_API_ERROR | endpoint=%s | reason=%r | retrying", op, e)
                        raise TransientAPIError(str(e)) from e
                    raise

    # ---- Read endpoints ----------------------------------------------------

    async def get_markets(self, next_cursor: str = "") -> Any:
        inner = self._require_init()
        return await self._retrying_call(lambda: inner.get_markets(next_cursor=next_cursor), op="get_markets")

    async def get_market(self, condition_id: str) -> Any:
        inner = self._require_init()
        return await self._retrying_call(lambda: inner.get_market(condition_id), op="get_market")

    async def get_orderbook(self, token_id: str) -> Any:
        inner = self._require_init()
        return await self._retrying_call(lambda: inner.get_order_book(token_id), op="get_order_book")

    async def get_midpoint(self, token_id: str) -> Any:
        inner = self._require_init()
        return await self._retrying_call(lambda: inner.get_midpoint(token_id), op="get_midpoint")

    async def get_price(self, token_id: str, side: str) -> Any:
        inner = self._require_init()
        return await self._retrying_call(lambda: inner.get_price(token_id, side), op="get_price")

    # ---- Trading endpoints (authenticated only) ---------------------------

    async def create_and_post_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: Any,
        *,
        tick_size: str = "0.01",
        neg_risk: bool = False,
        order_type: Any = None,
    ) -> Any:
        """V2 create-and-post in a single call.

        `side` should be a `py_clob_client_v2.Side` enum value (BUY/SELL).
        `order_type` defaults to GTC if not provided.
        """
        inner = self._require_auth()
        from py_clob_client_v2 import (  # type: ignore
            OrderArgs, OrderType, PartialCreateOrderOptions,
        )

        if order_type is None:
            order_type = OrderType.GTC

        def _go() -> Any:
            return inner.create_and_post_order(
                order_args=OrderArgs(
                    token_id=token_id,
                    price=price,
                    side=side,
                    size=size,
                ),
                options=PartialCreateOrderOptions(
                    tick_size=tick_size,
                    neg_risk=neg_risk,
                ),
                order_type=order_type,
            )

        return await self._retrying_call(_go, op="create_and_post_order")

    async def cancel_order(self, order_id: str) -> Any:
        inner = self._require_auth()
        return await self._retrying_call(lambda: inner.cancel(order_id), op="cancel")

    async def get_open_orders(self) -> Any:
        inner = self._require_auth()
        return await self._retrying_call(lambda: inner.get_orders(), op="get_orders")
