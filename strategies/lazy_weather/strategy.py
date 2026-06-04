"""LazyWeatherStrategy — buy any temperature bucket trading at >= threshold,
hold to resolution.

Pipeline (per scan):
  1. Pull all active temperature events from Gamma (no city filter).
  2. For each bucket: if YES price clears one of the configured `thresholds`
     AND resolution window in [min_hours, max_hours], emit a Signal — once
     per (market_id, bucket_label, threshold). The first threshold is the
     entry floor; higher thresholds are ladder adds (price-conviction pyramid).
  3. Edge reported is `claimed_edge_pct` — the *hypothesis* that consensus
     gives at least that much advantage over implied price. Trades are
     paper-mode only by default; resolution data updates the journal so
     we can later test whether the claim holds.

There is no forecasting, no calibration, no agreement gate. The "model" is
simply: the crowd is right.

The ladder requires `risk.dedup_open_positions = false` (or a per-strategy
override) so that multiple buys on the same bucket can pyramid as price rises.
The strategy enforces "each threshold fires at most once per bucket" itself.
"""
from __future__ import annotations

import json
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
        # Exchange this instance trades on. Defaults to Polymarket.
        self._exchange: str = str(params.get("exchange") or "polymarket").lower()
        # Ladder thresholds: entry floor first, then pyramid levels. Each fires
        # at most once per (market_id, bucket_label). `price_threshold` is kept
        # as a back-compat alias for a single-rung ladder.
        thresholds = params.get("thresholds")
        if thresholds is None:
            single = params.get("price_threshold", 0.60)
            thresholds = [single]
        self._thresholds: list[float] = sorted(float(t) for t in thresholds)
        self._claimed_edge: float = float(params.get("claimed_edge_pct", 0.10))
        self._min_hours: float = float(params.get("min_hours_to_resolution", 12.0))
        self._max_hours: float = float(params.get("max_forecast_horizon_hours", 48.0))
        self._scan_interval_sec: float = float(params.get("scan_interval_sec", 300.0))
        self._min_liquidity: float = float(params.get("min_liquidity_usd", 100.0))
        self._max_price: float = float(params.get("max_price", 0.95))
        self._request_timeout_sec: float = float(params.get("request_timeout_sec", 15.0))
        cities = params.get("cities") or []
        self._kalshi_cities: set[str] | None = {str(c) for c in cities} if cities else None

        self._session: aiohttp.ClientSession | None = None
        self._scanner: LazyGammaScanner | None = None
        self._kalshi_scanner: Any | None = None
        self._last_scan_at: datetime | None = None
        # Per-bucket ladder state: which thresholds have already fired.
        # Key: (market_id, bucket_label) where bucket_label="" for Kalshi binary markets.
        # Survives across scans so a bucket bouncing between $0.59 and $0.61
        # only triggers the $0.60 buy once. Does NOT survive bot restarts —
        # the journal-side dedup catches that.
        self._fired: dict[tuple[str, str], set[float]] = {}

    async def setup(self) -> None:
        self._session = aiohttp.ClientSession()
        if self._exchange == "kalshi":
            from strategies.weather.kalshi_markets import KalshiMarketScanner
            kalshi_client = self.context.get_client("kalshi")
            if kalshi_client is None:
                self.log.warning(
                    "LazyWeatherStrategy (Kalshi): no Kalshi client in context — scans will return empty"
                )
            self._kalshi_scanner = KalshiMarketScanner(
                kalshi_client,
                request_timeout_sec=self._request_timeout_sec,
            ) if kalshi_client else None
            self._scanner = None
        else:
            self._scanner = LazyGammaScanner(
                self._session,
                request_timeout_sec=self._request_timeout_sec,
            )
            self._kalshi_scanner = None
        self._hydrate_fired_from_journal()
        ladder = ", ".join(f"${t:.2f}" for t in self._thresholds)
        self.log.info(
            "LazyWeatherStrategy ready: exchange=%s ladder=[%s] claimed_edge=%.2f window=%.0f-%.0fh min_liq=$%.0f",
            self._exchange, ladder, self._claimed_edge,
            self._min_hours, self._max_hours, self._min_liquidity,
        )

    def _hydrate_fired_from_journal(self) -> None:
        """Restore `_fired` from the journal so a bot restart doesn't
        re-fire ladder rungs that already have open positions.

        The risk module's `(strategy, market_id)` dedup is OFF for this
        strategy (required for ladder pyramiding), so the strategy itself
        is the only thing preventing duplicate rung buys after restart.
        Rows missing `metadata.ladder_threshold` are skipped — they
        pre-date the ladder rollout and shouldn't seed the map.

        Filters by exchange so Polymarket and Kalshi instances don't
        seed each other's _fired sets.
        """
        journal = getattr(self.context, "journal", None)
        if journal is None:
            self.log.debug("no journal in context — skipping _fired hydration")
            return
        try:
            rows = journal.open_positions()
        except Exception:
            self.log.warning("failed to read open positions for hydration", exc_info=True)
            return
        seeded_rungs = 0
        instance_name = getattr(self, "_instance_name", self.name)
        for row in rows:
            if row.get("strategy") not in (self.name, instance_name):
                continue
            try:
                meta = json.loads(row.get("metadata_json") or "{}")
            except (TypeError, ValueError):
                continue
            # Filter by exchange — don't cross-contaminate.
            row_exchange = row.get("exchange") or meta.get("exchange") or "polymarket"
            if row_exchange != self._exchange:
                continue
            # For Kalshi binary markets, bucket may be absent; use "" as placeholder.
            bucket = meta.get("bucket") or ""
            threshold = meta.get("ladder_threshold")
            if threshold is None:
                continue
            key = (str(row.get("market_id")), str(bucket))
            self._fired.setdefault(key, set()).add(float(threshold))
            seeded_rungs += 1
        if seeded_rungs:
            self.log.info(
                "lazy[%s]: hydrated _fired from journal — %d open rungs across %d buckets",
                self._exchange, seeded_rungs, len(self._fired),
            )

    async def teardown(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def scan(self) -> list[Signal]:
        if not self._should_scan_now():
            return []
        if self._exchange == "kalshi":
            return await self._scan_kalshi()
        return await self._scan_polymarket()

    async def _scan_polymarket(self) -> list[Signal]:
        assert self._scanner is not None
        markets = await self._scanner.fetch_active()
        self._last_scan_at = datetime.now(timezone.utc)
        if not markets:
            return []

        now = datetime.now(timezone.utc)
        signals: list[Signal] = []
        candidates = 0
        skipped_window = 0
        skipped_below_floor = 0
        skipped_max_price = 0
        skipped_liq = 0
        skipped_already_fired = 0

        floor = self._thresholds[0]
        for m in markets:
            hours_to_end = (m.end_date_utc - now).total_seconds() / 3600.0
            if hours_to_end < self._min_hours or hours_to_end > self._max_hours:
                skipped_window += 1
                continue
            if m.yes_price > self._max_price:
                skipped_max_price += 1
                continue
            if m.yes_price < floor:
                skipped_below_floor += 1
                continue
            if m.liquidity_usd < self._min_liquidity:
                skipped_liq += 1
                continue
            key = (m.market_id, m.bucket_label)
            fired = self._fired.setdefault(key, set())
            crossed = [t for t in self._thresholds if m.yes_price >= t and t not in fired]
            if not crossed:
                skipped_already_fired += 1
                continue
            for t in crossed:
                fired.add(t)
                candidates += 1
                signals.append(self._make_signal(m, t))

        # Garbage-collect _fired entries whose markets are no longer in the
        # active result set (resolved or dropped from Gamma).
        live_keys = {(m.market_id, m.bucket_label) for m in markets}
        self._fired = {k: v for k, v in self._fired.items() if k in live_keys}

        self.log.info(
            "lazy[polymarket] scan: %d markets total | %d candidate signals | "
            "skipped: window=%d below_floor=%d max_price=%d liq=%d already_fired=%d",
            len(markets), candidates, skipped_window, skipped_below_floor,
            skipped_max_price, skipped_liq, skipped_already_fired,
        )
        return signals

    async def _scan_kalshi(self) -> list[Signal]:
        if self._kalshi_scanner is None:
            return []
        markets = await self._kalshi_scanner.fetch_active_weather(
            tradeable_cities=self._kalshi_cities,
        )
        self._last_scan_at = datetime.now(timezone.utc)
        if not markets:
            return []

        now = datetime.now(timezone.utc)
        signals: list[Signal] = []
        candidates = 0
        skipped_window = 0
        skipped_below_floor = 0
        skipped_max_price = 0
        skipped_already_fired = 0

        floor = self._thresholds[0]
        for m in markets:
            hours_to_end = (m.end_date_utc - now).total_seconds() / 3600.0
            if hours_to_end < self._min_hours or hours_to_end > self._max_hours:
                skipped_window += 1
                continue
            if m.yes_price > self._max_price:
                skipped_max_price += 1
                continue
            if m.yes_price < floor:
                skipped_below_floor += 1
                continue
            # Kalshi binary markets: key uses ticker + empty bucket_label
            key = (m.ticker, "")
            fired = self._fired.setdefault(key, set())
            crossed = [t for t in self._thresholds if m.yes_price >= t and t not in fired]
            if not crossed:
                skipped_already_fired += 1
                continue
            for t in crossed:
                fired.add(t)
                candidates += 1
                signals.append(self._make_kalshi_signal(m, t))

        # Garbage-collect expired Kalshi markets from _fired.
        live_keys = {(m.ticker, "") for m in markets}
        # Only remove Kalshi keys (those with empty bucket_label might overlap if
        # Polymarket ever has empty labels, so restrict to actually-fired Kalshi entries).
        self._fired = {
            k: v for k, v in self._fired.items()
            if not (k[1] == "" and k not in live_keys and self._exchange == "kalshi")
        }

        self.log.info(
            "lazy[kalshi] scan: %d markets total | %d candidate signals | "
            "skipped: window=%d below_floor=%d max_price=%d already_fired=%d",
            len(markets), candidates, skipped_window, skipped_below_floor,
            skipped_max_price, skipped_already_fired,
        )
        return signals

    def _make_kalshi_signal(self, m: Any, threshold: float) -> Signal:
        """Build a Signal from a KalshiWeatherMarket entry."""
        confidence = max(0.0, min(1.0, m.yes_price))
        return Signal(
            market_id=m.ticker,
            direction="YES",
            edge=self._claimed_edge,
            confidence=confidence,
            market_question=m.title,
            price=m.yes_price,
            category="weather",
            token_id=m.ticker,   # Kalshi uses ticker as the trade target identifier
            exchange="kalshi",
            metadata={
                "city": m.city,
                "metric": m.metric,
                "threshold_f": m.threshold_f,
                "threshold_unit": "fahrenheit",
                "end_utc": m.end_date_utc.isoformat(),
                "volume_usd": m.volume_usd,
                "ladder_threshold": threshold,
                "ladder_thresholds": list(self._thresholds),
                "claimed_edge_pct": self._claimed_edge,
                "lazy_thesis": "crowd_consensus",
                "neg_risk": False,   # Kalshi binary markets are never neg-risk
            },
        )

    def _make_signal(self, m: LazyMarket, threshold: float) -> Signal:
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
                "ladder_threshold": threshold,
                "ladder_thresholds": list(self._thresholds),
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

        threshold = signal.metadata.get("ladder_threshold", self._thresholds[0])
        return TradeIntent(
            signal=signal,
            size_usd_hint=None,  # let the risk module size via Kelly
            reason=(
                f"lazy[rung=${threshold:.2f}]: "
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
