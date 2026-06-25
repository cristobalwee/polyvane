"""Kalshi REST API v2 client.

Mirrors core/client.py's interface for Polymarket but targets Kalshi's REST
API instead of the py-clob-client-v2 SDK. Authentication uses RSA-SHA256
request signing (pycryptodome, already a transitive dependency).

Paper mode  → demo-api.kalshi.co   (no credentials required for reads)
Live mode   → trading-api.kalshi.com (KALSHI_KEY_ID + KALSHI_PRIVATE_KEY_PATH)

Kalshi prices are in cents (1–99); all public methods return floats in [0, 1].
"""
from __future__ import annotations

import asyncio
import base64
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import aiohttp
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)

KALSHI_DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
KALSHI_LIVE_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


def _parse_price_dollars_kalshi(*values: Any) -> float | None:
    """First value parseable as a decimal-dollar price strictly in (0, 1).

    Kalshi's 2026-06 schema reports prices as decimal-dollar strings under
    ``*_dollars`` keys (e.g. ``"0.6700"``); 0.00/1.00 are no-offer sentinels.
    """
    for v in values:
        if v is None or v == "":
            continue
        try:
            p = float(v)
        except (TypeError, ValueError):
            continue
        if 0.0 < p < 1.0:
            return p
    return None


class KalshiTransientError(Exception):
    pass


@dataclass
class KalshiClientConfig:
    base_url: str
    key_id: str = ""
    private_key_path: str = ""
    request_timeout_sec: float = 10.0
    retry_max_attempts: int = 3
    retry_initial_delay_sec: float = 1.0
    retry_max_delay_sec: float = 30.0

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "KalshiClientConfig":
        return cls(
            base_url=str(d.get("base_url") or KALSHI_DEMO_BASE_URL),
            key_id=str(d.get("key_id") or ""),
            private_key_path=str(d.get("private_key_path") or ""),
            request_timeout_sec=float(d.get("request_timeout_sec", 10.0)),
            retry_max_attempts=int(d.get("retry_max_attempts", 3)),
            retry_initial_delay_sec=float(d.get("retry_initial_delay_sec", 1.0)),
            retry_max_delay_sec=float(d.get("retry_max_delay_sec", 30.0)),
        )


class KalshiClient:
    """Async Kalshi REST v2 client with paper/live modes."""

    def __init__(self, config: KalshiClientConfig) -> None:
        self.config = config
        self._session: aiohttp.ClientSession | None = None
        self._rsa_key: Any = None  # Crypto.PublicKey.RSA key object
        self._initialized = False
        self._authenticated = False

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def initialize_unauthenticated(self) -> None:
        """Paper mode: open an aiohttp session, skip key loading."""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout_sec),
        )
        self._initialized = True
        self._authenticated = False
        log.info("KalshiClient initialized (unauthenticated / paper mode) → %s", self.config.base_url)

    def initialize_authenticated(self) -> None:
        """Live mode: load RSA key and open session."""
        if not self.config.key_id:
            raise ValueError("KALSHI_KEY_ID is required for live mode")
        if not self.config.private_key_path:
            raise ValueError("KALSHI_PRIVATE_KEY_PATH is required for live mode")
        try:
            from Crypto.PublicKey import RSA  # type: ignore
        except ImportError as exc:
            raise RuntimeError("pycryptodome is required for Kalshi live mode") from exc
        with open(self.config.private_key_path, "r") as f:
            pem = f.read()
        self._rsa_key = RSA.import_key(pem)
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout_sec),
        )
        self._initialized = True
        self._authenticated = True
        log.info("KalshiClient initialized (authenticated) → %s", self.config.base_url)

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Price conversion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def cents_to_float(cents: int | float) -> float:
        return float(cents) / 100.0

    @staticmethod
    def float_to_cents(p: float) -> int:
        return max(1, min(99, round(p * 100)))

    # ------------------------------------------------------------------
    # Auth signing
    # ------------------------------------------------------------------

    def _auth_headers(self, method: str, path: str) -> dict[str, str]:
        """Build Kalshi RSA auth headers for a signed request."""
        if not self._authenticated or self._rsa_key is None:
            return {}
        try:
            from Crypto.Hash import SHA256  # type: ignore
            from Crypto.Signature import pkcs1_15  # type: ignore
        except ImportError as exc:
            raise RuntimeError("pycryptodome required for request signing") from exc
        ts_ms = str(int(time.time() * 1000))
        # Kalshi signs the FULL request path, including the API prefix that
        # lives in base_url (e.g. "/trade-api/v2"). Signing only the bare
        # endpoint path ("/markets") yields a 401 "missing or invalid
        # signature". Derive the prefix from base_url so this stays correct
        # regardless of host/version.
        base_path = urlparse(self.config.base_url).path.rstrip("/")
        full_path = f"{base_path}{path}"
        message = f"{ts_ms}{method.upper()}{full_path}"
        h = SHA256.new(message.encode("utf-8"))
        sig = pkcs1_15.new(self._rsa_key).sign(h)
        return {
            "KALSHI-ACCESS-KEY": self.config.key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
        }

    # ------------------------------------------------------------------
    # Low-level HTTP helpers
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        if self._session is None:
            raise RuntimeError("KalshiClient not initialized — call initialize_*()")
        url = self.config.base_url.rstrip("/") + path
        headers = {"Content-Type": "application/json", **self._auth_headers(method, path)}
        try:
            async with self._session.request(
                method,
                url,
                params=params,
                json=body,
                headers=headers,
            ) as resp:
                text = await resp.text()
                if resp.status in (429, 502, 503, 504):
                    raise KalshiTransientError(f"HTTP {resp.status}: {text[:200]}")
                if resp.status >= 400:
                    raise RuntimeError(f"Kalshi API error {resp.status}: {text[:500]}")
                if not text:
                    return {}
                return await resp.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise KalshiTransientError(str(exc)) from exc

    async def _retrying_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.config.retry_max_attempts),
            wait=wait_exponential(
                multiplier=self.config.retry_initial_delay_sec,
                max=self.config.retry_max_delay_sec,
            ),
            retry=retry_if_exception_type(KalshiTransientError),
            reraise=True,
        ):
            with attempt:
                return await self._request(method, path, params=params, body=body)

    async def _get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        return await self._retrying_request("GET", path, params=params)

    async def _post(self, path: str, *, body: dict[str, Any]) -> Any:
        return await self._retrying_request("POST", path, body=body)

    async def _delete(self, path: str) -> Any:
        return await self._retrying_request("DELETE", path)

    # ------------------------------------------------------------------
    # Read endpoints (paper + live)
    # ------------------------------------------------------------------

    async def get_markets(
        self,
        *,
        status: str = "open",
        ticker_prefix: str | None = None,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        cursor: str = "",
        limit: int = 200,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"status": status, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        if ticker_prefix:
            params["tickers"] = ticker_prefix
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        return await self._get("/markets", params=params)

    async def get_market(self, ticker: str) -> dict[str, Any]:
        return await self._get(f"/markets/{ticker}")

    async def get_orderbook(self, ticker: str, *, depth: int = 10) -> dict[str, Any]:
        return await self._get(f"/markets/{ticker}/orderbook", params={"depth": depth})

    @staticmethod
    def _best_book_price(levels: Any) -> float | None:
        """Best (highest) price from one side of a Kalshi order book.

        Handles both the 2026-06 ``*_dollars`` schema — a list of
        ``[price_str, size_str]`` pairs in decimal dollars — and the legacy
        ``[{"price": cents}]`` schema. Returns a price in (0, 1) or None.
        """
        if not levels:
            return None
        prices: list[float] = []
        for lvl in levels:
            try:
                raw = lvl.get("price") if isinstance(lvl, dict) else lvl[0]
                p = float(raw)
            except (TypeError, ValueError, IndexError, KeyError):
                continue
            if p > 1.0:        # legacy integer cents
                p /= 100.0
            if 0.0 < p < 1.0:
                prices.append(p)
        return max(prices) if prices else None

    def _market_last_price(self, market: dict[str, Any]) -> float | None:
        """Best-effort last/ask price from a market object, new or old schema."""
        return _parse_price_dollars_kalshi(
            market.get("last_price_dollars"),
            market.get("yes_ask_dollars"),
        ) or (
            self.cents_to_float(market.get("last_price") or market.get("yes_ask"))
            if (market.get("last_price") or market.get("yes_ask")) else None
        )

    async def get_midpoint(self, ticker: str) -> dict[str, float]:
        """Return {"mid": float} for compatibility with ClobClient.get_midpoint()."""
        mid: float | None = None
        try:
            book = await self.get_orderbook(ticker, depth=1)
            ob = book.get("orderbook_fp") or book.get("orderbook") or book
            yes_levels = ob.get("yes_dollars") or ob.get("yes") or []
            no_levels = ob.get("no_dollars") or ob.get("no") or []
            best_bid = self._best_book_price(yes_levels)         # best YES bid
            best_no_bid = self._best_book_price(no_levels)       # best NO bid
            best_ask = (1.0 - best_no_bid) if best_no_bid is not None else None  # → YES ask
            if best_bid is not None and best_ask is not None:
                mid = (best_bid + best_ask) / 2.0
            elif best_bid is not None:
                mid = best_bid
            elif best_ask is not None:
                mid = best_ask
            if mid is None:
                # Empty book — fall back to last traded price.
                mkt = await self.get_market(ticker)
                mid = self._market_last_price(mkt.get("market") or mkt)
        except Exception:
            log.debug("KalshiClient.get_midpoint(%s) fallback to market fetch", ticker, exc_info=True)
            try:
                mkt = await self.get_market(ticker)
                mid = self._market_last_price(mkt.get("market") or mkt)
            except Exception:
                mid = None
        if mid is None:
            mid = 0.5
        return {"mid": max(0.01, min(0.99, mid))}

    # ------------------------------------------------------------------
    # Trading endpoints (live only)
    # ------------------------------------------------------------------

    async def create_order(
        self,
        ticker: str,
        side: str,
        count: int,
        *,
        action: str = "buy",
        yes_price: int | None = None,
        no_price: int | None = None,
        order_type: str = "limit",
        expiration_ts: int | None = None,
    ) -> dict[str, Any]:
        if not self._authenticated:
            raise RuntimeError("KalshiClient not authenticated — cannot place orders")
        action = action.lower()
        if action not in ("buy", "sell"):
            raise ValueError(f"Kalshi order action must be 'buy' or 'sell', got {action!r}")
        body: dict[str, Any] = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "count": count,
            "type": order_type,
        }
        if yes_price is not None:
            body["yes_price"] = yes_price
        if no_price is not None:
            body["no_price"] = no_price
        if expiration_ts is not None:
            body["expiration_ts"] = expiration_ts
        return await self._post("/portfolio/orders", body=body)

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        if not self._authenticated:
            raise RuntimeError("KalshiClient not authenticated")
        return await self._delete(f"/portfolio/orders/{order_id}")

    async def get_positions(self) -> dict[str, Any]:
        if not self._authenticated:
            raise RuntimeError("KalshiClient not authenticated")
        return await self._get("/portfolio/positions")
