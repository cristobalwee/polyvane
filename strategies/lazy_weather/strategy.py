"""LazyWeatherStrategy — buy any temperature bucket trading at >= threshold,
hold to resolution.

Pipeline (per scan):
  1. Pull all active temperature events from Gamma (no city filter).
  2. For each bucket: if YES price >= price_threshold AND resolution window
     in [min_hours, max_hours], emit a Signal.
  3. Edge reported is `claimed_edge_pct` — the *hypothesis* that consensus
     gives at least that much advantage over implied price. Trades are
     paper-mode only by default; resolution data updates the journal so
     we can later test whether the claim holds.

There is no forecasting, no calibration, no agreement gate. The "model" is
simply: the crowd is right.

Variants subclass this with a different `name` and different params (mainly
`price_threshold`).
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

import aiohttp

from strategies.base import BaseStrategy, Signal, StrategyContext, TradeIntent

from .scanner import LazyGammaScanner, LazyMarket


class LazyWeatherStrategy(BaseStrategy):
    name = "lazy_weather"

    def __init__(self, params: dict[str, Any], context: StrategyContext) -> None:
        super().__init__(params, context)
        self._price_threshold: float = float(params.get("price_threshold", 0.50))
        self._claimed_edge: float = float(params.get("claimed_edge_pct", 0.10))
        self._min_hours: float = float(params.get("min_hours_to_resolution", 12.0))
        self._max_hours: float = float(params.get("max_forecast_horizon_hours", 48.0))
        self._scan_interval_sec: float = float(params.get("scan_interval_sec", 300.0))
        self._min_liquidity: float = float(params.get("min_liquidity_usd", 100.0))
        self._max_price: float = float(params.get("max_price", 0.95))
        self._request_timeout_sec: float = float(params.get("request_timeout_sec", 15.0))

        self._session: aiohttp.ClientSession | None = None
        self._scanner: LazyGammaScanner | None = None
        self._last_scan_at: datetime | None = None
        # Track which (market_id, bucket_label) pairs we've already signalled
        # in the current resolution window so we don't re-emit on every poll.
        self._seen: set[tuple[str, str]] = set()

    async def setup(self) -> None:
        self._session = aiohttp.ClientSession()
        self._scanner = LazyGammaScanner(
            self._session,
            request_timeout_sec=self._request_timeout_sec,
        )
        self.log.info(
            "LazyWeatherStrategy ready: threshold=$%.2f claimed_edge=%.2f window=%.0f-%.0fh min_liq=$%.0f",
            self._price_threshold, self._claimed_edge,
            self._min_hours, self._max_hours, self._min_liquidity,
        )

    async def teardown(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def scan(self) -> list[Signal]:
        if not self._should_scan_now():
            return []
        assert self._scanner is not None
        markets = await self._scanner.fetch_active()
        self._last_scan_at = datetime.now(timezone.utc)
        if not markets:
            return []

        now = datetime.now(timezone.utc)
        signals: list[Signal] = []
        candidates = 0
        skipped_window = 0
        skipped_price = 0
        skipped_liq = 0
        skipped_seen = 0

        for m in markets:
            hours_to_end = (m.end_date_utc - now).total_seconds() / 3600.0
            if hours_to_end < self._min_hours or hours_to_end > self._max_hours:
                skipped_window += 1
                continue
            if m.yes_price < self._price_threshold or m.yes_price > self._max_price:
                skipped_price += 1
                continue
            if m.liquidity_usd < self._min_liquidity:
                skipped_liq += 1
                continue
            key = (m.market_id, m.bucket_label)
            if key in self._seen:
                skipped_seen += 1
                continue
            self._seen.add(key)
            candidates += 1
            signals.append(self._make_signal(m))

        # Garbage-collect _seen of entries whose markets are no longer in the
        # active result set (they've resolved or been dropped from Gamma).
        # Cheap O(n) check on every scan.
        live_keys = {(m.market_id, m.bucket_label) for m in markets}
        self._seen &= live_keys

        self.log.info(
            "lazy scan: %d markets total | %d candidate signals | "
            "skipped: window=%d price=%d liq=%d seen=%d",
            len(markets), candidates, skipped_window, skipped_price,
            skipped_liq, skipped_seen,
        )
        return signals

    def _make_signal(self, m: LazyMarket) -> Signal:
        # Confidence reported = the price itself (i.e. crowd-implied prob).
        # Edge reported = the hypothesis we're testing (claimed_edge_pct).
        # The journal records both, so post-resolution we can measure
        # whether `model_prob = price + claimed_edge` was right.
        confidence = max(0.0, min(1.0, m.yes_price))
        return Signal(
            market_id=m.market_id,
            direction="YES",
            edge=self._claimed_edge,
            confidence=confidence,
            market_question=m.question,
            price=m.yes_price,
            category="weather",
            token_id=m.yes_token_id,
            metadata={
                "bucket": m.bucket_label,
                "bucket_low": m.bucket_low,
                "bucket_high": m.bucket_high,
                "unit": m.unit,
                "city": m.city,
                "metric": m.metric,
                "end_utc": m.end_date_utc.isoformat(),
                "liquidity_usd": m.liquidity_usd,
                "volume_usd": m.volume_usd,
                "price_threshold": self._price_threshold,
                "claimed_edge_pct": self._claimed_edge,
                "lazy_thesis": "crowd_consensus",
                "neg_risk": True,  # temperature buckets are negative-risk markets
            },
        )

    async def evaluate(self, signal: Signal) -> TradeIntent | None:
        # The scan() already filtered window/price/liquidity. Re-check
        # window here in case the loop's queueing introduced a delay
        # near the boundary.
        end_iso = signal.metadata.get("end_utc")
        if end_iso:
            try:
                end_dt = datetime.fromisoformat(end_iso)
                hours_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0
            except ValueError:
                hours_left = math.inf
            if hours_left < self._min_hours or hours_left > self._max_hours:
                return None

        return TradeIntent(
            signal=signal,
            size_usd_hint=None,  # let the risk module size via Kelly
            reason=(
                f"lazy[t=${self._price_threshold:.2f}]: "
                f"{signal.metadata.get('city')} {signal.metadata.get('bucket')} "
                f"@ ${signal.price:.2f} "
                f"(claimed_edge={self._claimed_edge:.2f}, "
                f"resolves_in={(end_iso or '?')[:16]})"
            ),
        )

    def _should_scan_now(self) -> bool:
        if self._last_scan_at is None:
            return True
        elapsed = (datetime.now(timezone.utc) - self._last_scan_at).total_seconds()
        return elapsed >= self._scan_interval_sec
