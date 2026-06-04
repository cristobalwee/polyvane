"""WeatherStrategy: trade Polymarket temperature buckets vs forecasts.

Pipeline (per scan):
  1. Discover active weather markets on Polymarket (Gamma API).
  2. For each market's city, fetch forecasts from every configured model
     (NOAA + Open-Meteo GFS/ECMWF/ICON for US cities; Open-Meteo only
     for non-US).
  3. Blend the per-model forecasts into bucket probabilities using a
     weighted Gaussian mixture. Suppress the market entirely if the
     ensemble's agreement score is below `min_agreement`.
  4. For each bucket, compare model probability to YES price. Emit a
     Signal whenever the edge clears the bucket-class threshold:
       - "primary" bucket (the one containing the ensemble point estimate)
         must clear `min_edge_pct`.
       - any other bucket ("adjacent") must clear `adjacent_min_edge_pct`,
         and only if `trade_adjacent_buckets` is true.

When only one model is configured, the ensemble degenerates to a single
Gaussian — exactly the behaviour of the previous single-model implementation.
"""
from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aiohttp

from strategies.base import BaseStrategy, Signal, StrategyContext, TradeIntent

from . import kalshi_resolution, resolution
from .ensemble import EnsembleForecaster, EnsembleResult
from .markets import GammaClient
from .models import Forecast, TemperatureBucket, WeatherMarket, WeatherSignal
from .noaa import ForecastClient, all_known_sources


# GFS model runs (UTC hours).
GFS_RUN_HOURS = (0, 6, 12, 18)

# Default forecast-model set when `forecast_models` isn't set in config.
# Default forecast set drops ECMWF: the historical-archive endpoint
# wasn't returning samples during calibration (verify before re-enabling).
_DEFAULT_FORECAST_MODELS = [
    "noaa",
    "open_meteo_gfs",
    "open_meteo_icon",
]


@dataclass
class _ScanReport:
    markets_scanned: int
    forecasts_fetched: int
    suppressed_low_agreement: int
    signals: list[WeatherSignal]


class WeatherStrategy(BaseStrategy):
    name = "weather"

    def __init__(self, params: dict[str, Any], context: StrategyContext) -> None:
        super().__init__(params, context)

        # Exchange this instance trades on. Defaults to Polymarket.
        self._exchange: str = str(params.get("exchange") or "polymarket").lower()

        requested = params.get("cities")
        if requested:
            requested = list(requested)
        else:
            requested = resolution.all_cities()

        self._require_confirmed: bool = bool(params.get("require_confirmed_resolution", True))
        tradeable, skipped = resolution.filter_tradeable(
            requested, require_confirmed=self._require_confirmed,
        )
        self._tradeable_cities: set[str] = set(tradeable)
        self._skipped_cities: list[tuple[str, str]] = skipped

        self._min_edge: float = float(params.get("min_edge_pct", 0.15))
        self._adjacent_min_edge: float = float(params.get("adjacent_min_edge_pct", 0.10))
        self._trade_adjacent: bool = bool(params.get("trade_adjacent_buckets", True))
        # Absolute floor on model_prob. Edge alone can be inflated by tiny
        # YES prices on long-shot adjacent buckets — a 0.05 model_prob vs
        # $0.005 price clears a 0.04 edge gate but has historically been a
        # net loser (-$526 across 128 trades in the 2026-05-10..25 window).
        # 0.20 matches the empirical cliff between losing and winning bins.
        self._min_model_prob: float = float(params.get("min_model_prob", 0.20))
        # Sanity cap on claimed edge. A 30%+ edge in a liquid binary market is
        # almost always a model bug (probability inflation, sign error, bias
        # uncorrected) — not real alpha. We log loudly and skip.
        self._max_edge_sanity: float = float(params.get("max_edge_sanity", 0.30))
        self._min_hours: float = float(params.get("min_hours_to_resolution", 12))
        self._max_horizon_hours: float = float(params.get("max_forecast_horizon_hours", 48))
        self._scan_interval_sec: float = float(params.get("scan_interval_sec", 300))
        self._scan_on_model_update: bool = bool(params.get("scan_on_model_update", True))
        self._min_liquidity: float = float(params.get("min_liquidity_usd", 100.0))
        self._request_timeout_sec: float = float(params.get("request_timeout_sec", 15.0))
        self._noaa_max_rps: float = float(params.get("noaa_max_rps", 5.0))

        # Ensemble configuration.
        models = params.get("forecast_models") or _DEFAULT_FORECAST_MODELS
        weights_cfg = params.get("forecast_model_weights") or {}
        self._model_weights = self._build_weights(models, weights_cfg)
        # Default min_agreement = 0.0 (gate disabled). The historical 0.7+
        # default was filtering 90%+ of signals and selection-biasing the
        # surviving set toward correlated-bias forecasts — exactly the trades
        # that lost most on conservative variants. Re-enable only after
        # bias correction is in place and we can prove the gate adds value.
        self._min_agreement: float = float(params.get("min_agreement", 0.0))
        self._agreement_scale_f: float = float(params.get("agreement_scale_f", 3.0))
        per_source_std = params.get("per_source_std_dev_f") or {}
        self._per_source_std: dict[str, float] = {k: float(v) for k, v in per_source_std.items()}
        # Per-city bias correction. Subtracted from each member's mean before
        # ensembling. Values come from calibrate.py output. Only applied when
        # |bias| >= min_bias_apply_f to avoid over-correcting on noise.
        bias_cfg = params.get("per_city_bias_f") or {}
        self._per_city_bias_f: dict[str, float] = {k: float(v) for k, v in bias_cfg.items()}
        self._min_bias_apply_f: float = float(params.get("min_bias_apply_f", 0.5))

        self._session: aiohttp.ClientSession | None = None
        self._gamma: GammaClient | None = None
        self._kalshi_scanner: Any | None = None
        self._forecaster: ForecastClient | None = None
        self._ensemble: EnsembleForecaster | None = None

        # Throttle bookkeeping.
        self._last_scan_at: datetime | None = None
        self._last_run_boundary_seen: datetime | None = None

        # Optional alert hook for new-city detections; injected by main.py.
        self._alert_hook: Any = None

    @staticmethod
    def _build_weights(models: list[str], weights_cfg: dict[str, float]) -> dict[str, float]:
        known = set(all_known_sources())
        out: dict[str, float] = {}
        for m in models:
            if m not in known:
                logging.getLogger("strategy.weather").warning(
                    "Ignoring unknown forecast model %r — expected one of %s", m, sorted(known),
                )
                continue
            out[m] = float(weights_cfg.get(m, 1.0))
        return out

    def set_alert_hook(self, hook: Any) -> None:
        self._alert_hook = hook

    @property
    def last_scan_at(self) -> datetime | None:
        return self._last_scan_at

    @property
    def forecast_client(self) -> ForecastClient | None:
        return self._forecaster

    @property
    def ensemble(self) -> EnsembleForecaster | None:
        return self._ensemble

    async def setup(self) -> None:
        if not self._tradeable_cities and self._exchange == "polymarket":
            self.log.warning(
                "WeatherStrategy: no tradeable cities after filtering — scans will "
                "discover zero markets. Check `cities` and the resolution registry."
            )
        if self._skipped_cities:
            for city, reason in self._skipped_cities:
                self.log.warning("Skipping city %r — %s", city, reason)
        self._session = aiohttp.ClientSession()

        if self._exchange == "kalshi":
            from .kalshi_markets import KalshiMarketScanner
            kalshi_client = self.context.get_client("kalshi")
            if kalshi_client is None:
                self.log.warning(
                    "WeatherStrategy (Kalshi): no Kalshi client in context — scans will return empty"
                )
            self._kalshi_scanner = KalshiMarketScanner(
                kalshi_client,
                request_timeout_sec=self._request_timeout_sec,
            ) if kalshi_client else None
            self._gamma = None
        else:
            self._gamma = GammaClient(
                self._session,
                request_timeout_sec=self._request_timeout_sec,
                on_unknown_city=self._handle_unknown_city,
            )
            self._kalshi_scanner = None

        self._forecaster = ForecastClient(
            self._session,
            max_rps=self._noaa_max_rps,
            request_timeout_sec=self._request_timeout_sec,
        )
        self._ensemble = EnsembleForecaster(
            self._forecaster,
            model_weights=self._model_weights,
            per_source_std_dev_f=self._per_source_std or None,
            min_agreement=self._min_agreement,
            agreement_scale_f=self._agreement_scale_f,
            per_city_bias_f=self._per_city_bias_f or None,
            min_bias_apply_f=self._min_bias_apply_f,
        )
        bias_cities = sorted(
            c for c, b in self._per_city_bias_f.items() if abs(b) >= self._min_bias_apply_f
        )
        self.log.info(
            "WeatherStrategy ready: exchange=%s tradeable=%s skipped=%d models=%s min_edge=%.2f "
            "adj_edge=%.2f min_p=%.2f min_agreement=%.2f bias_cities=%s",
            self._exchange,
            sorted(self._tradeable_cities),
            len(self._skipped_cities),
            list(self._model_weights.keys()),
            self._min_edge,
            self._adjacent_min_edge,
            self._min_model_prob,
            self._min_agreement,
            bias_cities,
        )

    async def teardown(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _handle_unknown_city(self, raw_city: str) -> None:
        self.log.warning("New city detected on Polymarket: %r — not in resolution registry", raw_city)
        hook = self._alert_hook
        if hook is None:
            return
        try:
            hook("new_city_detected", {"city": raw_city})
        except Exception:
            self.log.debug("alert hook raised on new_city_detected", exc_info=True)

    async def scan(self) -> list[Signal]:
        if not self._should_scan_now():
            return []
        if self._exchange == "kalshi":
            return await self._scan_kalshi()
        return await self._scan_polymarket()

    async def _scan_polymarket(self) -> list[Signal]:
        report = await self._run_scan()
        self._last_scan_at = datetime.now(timezone.utc)

        signals: list[Signal] = []
        for ws in report.signals:
            confidence = min(1.0, max(0.0, ws.model_prob))
            metadata = {
                "city": ws.market.city,
                "station": ws.market.station_id,
                "metric": ws.market.raw.get("metric"),
                "bucket": ws.bucket.label,
                "unit": ws.market.unit,
                "forecast_temp": ws.forecast.temperature,
                "forecast_unit": ws.forecast.unit,
                "forecast_source": ws.forecast.source,
                "model_prob": ws.model_prob,
                "target_date": ws.market.target_date.isoformat(),
                "end_utc": ws.market.end_date_utc.isoformat(),
                "liquidity_usd": ws.market.liquidity_usd,
                "volume_usd": ws.market.volume_usd,
            }
            extras = ws.market.raw.get("ensemble_meta")
            if extras:
                metadata.update(extras)
            metadata["bucket_role"] = ws.market.raw.get("_bucket_role", {}).get(ws.bucket.label, "primary")
            signals.append(Signal(
                market_id=ws.market.market_id,
                direction="YES",
                edge=ws.edge,
                confidence=confidence,
                market_question=ws.market.question,
                price=ws.bucket.price,
                category="weather",
                token_id=ws.bucket.token_id,
                exchange="polymarket",
                metadata=metadata,
            ))

        if signals:
            self.log.info(
                "scan[polymarket]: %d markets, %d forecasts, %d suppressed (low agreement), %d signal(s)",
                report.markets_scanned, report.forecasts_fetched,
                report.suppressed_low_agreement, len(signals),
            )
        else:
            self.log.info(
                "scan[polymarket]: %d markets, %d forecasts, %d suppressed — no actionable signals",
                report.markets_scanned, report.forecasts_fetched,
                report.suppressed_low_agreement,
            )
        return signals

    async def _scan_kalshi(self) -> list[Signal]:
        """Scan Kalshi temperature binary markets using CDF exceedance probabilities."""
        if self._kalshi_scanner is None or self._ensemble is None:
            return []
        self._last_scan_at = datetime.now(timezone.utc)

        markets = await self._kalshi_scanner.fetch_active_weather(
            tradeable_cities=self._tradeable_cities or None,
        )
        if not markets:
            self.log.info("scan[kalshi]: no active weather markets found")
            return []

        now = datetime.now(timezone.utc)
        signals: list[Signal] = []
        forecasts_fetched = 0
        skipped_window = 0
        skipped_edge = 0

        for m in markets:
            hours_to_end = (m.end_date_utc - now).total_seconds() / 3600.0
            if hours_to_end < self._min_hours or hours_to_end > self._max_horizon_hours:
                skipped_window += 1
                continue

            src = kalshi_resolution.get(m.city) or resolution.get(m.city)
            if src is None:
                continue

            try:
                forecasts = await self._ensemble.fetch_member_forecasts(
                    src=src,
                    target_date=m.target_date,
                    metric=m.metric,
                    unit="fahrenheit",
                )
            except Exception:
                self.log.debug(
                    "Kalshi forecast fetch failed for %s %s", m.city, m.ticker, exc_info=True
                )
                continue

            if not forecasts:
                continue
            forecasts_fetched += len(forecasts)

            # P(temp >= threshold_f) under the Gaussian mixture.
            prob = self._ensemble.cdf_exceedance(
                forecasts, m.threshold_f, city=m.city
            )
            edge = prob - m.yes_price

            if abs(edge) < self._min_edge:
                skipped_edge += 1
                continue
            if abs(edge) > self._max_edge_sanity:
                self.log.warning(
                    "Kalshi: skipping implausibly large edge (city=%s ticker=%s edge=%.2f cap=%.2f)",
                    m.city, m.ticker, edge, self._max_edge_sanity,
                )
                continue

            confidence = max(0.0, min(1.0, prob))
            signals.append(Signal(
                market_id=m.ticker,
                direction="YES",
                edge=edge,
                confidence=confidence,
                market_question=m.title,
                price=m.yes_price,
                category="weather",
                token_id=m.ticker,
                exchange="kalshi",
                metadata={
                    "city": m.city,
                    "metric": m.metric,
                    "threshold_f": m.threshold_f,
                    "threshold_unit": "fahrenheit",
                    "model_prob": prob,
                    "target_date": m.target_date.isoformat(),
                    "end_utc": m.end_date_utc.isoformat(),
                    "volume_usd": m.volume_usd,
                    "neg_risk": False,
                    "bucket_role": "primary",
                },
            ))

        self.log.info(
            "scan[kalshi]: %d markets, %d forecasts, %d skipped(window), %d skipped(edge), %d signal(s)",
            len(markets), forecasts_fetched, skipped_window, skipped_edge, len(signals),
        )
        return signals

    async def evaluate(self, signal: Signal) -> TradeIntent | None:
        bucket_role = signal.metadata.get("bucket_role", "primary")
        edge_floor = self._min_edge if bucket_role == "primary" else self._adjacent_min_edge
        if signal.edge < edge_floor:
            return None
        if signal.confidence < self._min_model_prob:
            return None
        if signal.edge > self._max_edge_sanity:
            self.log.warning(
                "skipping signal with implausibly large edge — likely model bug "
                "(city=%s bucket=%s p=%.2f price=%.2f edge=%.2f cap=%.2f)",
                signal.metadata.get("city"), signal.metadata.get("bucket"),
                signal.confidence, signal.price, signal.edge, self._max_edge_sanity,
            )
            return None
        liquidity = float(signal.metadata.get("liquidity_usd", 0.0))
        if liquidity < self._min_liquidity:
            return None

        end_iso = signal.metadata.get("end_utc")
        if end_iso:
            try:
                end_dt = datetime.fromisoformat(end_iso)
                hours_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0
            except ValueError:
                hours_left = math.inf
            if hours_left < self._min_hours:
                return None
            if hours_left > self._max_horizon_hours:
                return None

        unit = signal.metadata.get("unit", "fahrenheit")
        unit_short = "°F" if unit == "fahrenheit" else "°C"
        agreement = signal.metadata.get("agreement_score")
        agreement_str = f", agreement={agreement:.2f}" if isinstance(agreement, (int, float)) else ""
        return TradeIntent(
            signal=signal,
            size_usd_hint=None,
            reason=(
                f"weather[{bucket_role}]: {signal.metadata.get('city')} forecast="
                f"{signal.metadata.get('forecast_temp')}{unit_short} "
                f"in {signal.metadata.get('bucket')} "
                f"(p={signal.confidence:.2f}, price={signal.price:.2f}, "
                f"edge={signal.edge:.2f}{agreement_str})"
            ),
        )

    # --- scan internals ---

    def _should_scan_now(self) -> bool:
        now = datetime.now(timezone.utc)
        if self._last_scan_at is None:
            return True
        elapsed = (now - self._last_scan_at).total_seconds()
        if elapsed >= self._scan_interval_sec:
            return True
        if self._scan_on_model_update:
            boundary = self._latest_run_boundary(now)
            if boundary > self._last_scan_at and boundary != self._last_run_boundary_seen:
                self._last_run_boundary_seen = boundary
                return True
        return False

    @staticmethod
    def _latest_run_boundary(now: datetime) -> datetime:
        hour = max(h for h in GFS_RUN_HOURS if h <= now.hour)
        return now.replace(hour=hour, minute=0, second=0, microsecond=0)

    async def _publish_to_cache(self, markets: list[WeatherMarket]) -> None:
        """Write per-bucket snapshots into the shared MarketCache.

        Other strategies (e.g. arbitrage) read from the same cache to avoid
        re-fetching the Gamma event list. We only have YES prices here —
        NO is implicit (1 - yes_price) for binary outcomes — so the arb
        scanner still needs to pull the CLOB book for actual fillable
        depth, but the discovery round-trip is saved.
        """
        cache = getattr(self.context, "market_cache", None)
        if cache is None or not markets:
            return
        rows: list[tuple[str, dict[str, Any]]] = []
        for m in markets:
            for b in m.buckets:
                rows.append((m.market_id, {
                    "category": "weather",
                    "subcategory": "temperature",
                    "city": m.city,
                    "question": m.question,
                    "yes_price": b.price,
                    "no_price": max(0.0, min(1.0, 1.0 - b.price)),
                    "token_id_yes": b.token_id,
                    "bucket_label": b.label,
                    "bucket_low": b.low,
                    "bucket_high": b.high,
                    "unit": b.unit,
                    "end_date_utc": m.end_date_utc.isoformat(),
                    "liquidity_usd": m.liquidity_usd,
                    "volume_24h_usd": m.volume_usd,
                }))
        try:
            await cache.put_many(rows, source_strategy=self.name)
        except Exception:
            self.log.debug("market_cache publish failed", exc_info=True)

    async def _run_scan(self) -> _ScanReport:
        assert self._gamma is not None and self._forecaster is not None and self._ensemble is not None
        markets = await self._gamma.fetch_active_weather(tradeable_cities=self._tradeable_cities)
        if not markets:
            return _ScanReport(0, 0, 0, [])

        await self._publish_to_cache(markets)

        # Polymarket emits one WeatherMarket *per bucket*. Group them so the
        # ensemble runs once per (event, target_date, city) — otherwise we'd
        # re-fetch four model forecasts for every bucket.
        by_event: dict[tuple[str, str, str], list[WeatherMarket]] = {}
        now = datetime.now(timezone.utc)
        for m in markets:
            hours_to_end = (m.end_date_utc - now).total_seconds() / 3600.0
            if hours_to_end <= 0 or hours_to_end > self._max_horizon_hours:
                continue
            key = (m.city, m.target_date.isoformat(), str(m.raw.get("metric")))
            by_event.setdefault(key, []).append(m)

        forecasts_fetched = 0
        suppressed = 0
        signals: list[WeatherSignal] = []

        async def _scan_event(group: list[WeatherMarket]) -> tuple[int, int, list[WeatherSignal]]:
            sample = group[0]
            src = resolution.get(sample.city)
            if src is None:
                return (0, 0, [])
            forecasts = await self._ensemble.fetch_member_forecasts(
                src=src,
                target_date=sample.target_date,
                metric=str(sample.raw.get("metric", "highest")),
                unit=sample.unit,
            )
            if not forecasts:
                return (0, 0, [])

            # Collect every bucket across the event's markets so the ensemble
            # gets a complete view; then emit one signal per (market, bucket)
            # the strategy can act on. Each WeatherMarket here carries exactly
            # one bucket (Gamma flattens that way).
            all_buckets = [b for m in group for b in m.buckets]
            ensemble_result = self._ensemble.build(
                forecasts, all_buckets, sample.unit, city=sample.city,
            )
            if ensemble_result is None:
                return (len(forecasts), 0, [])
            if ensemble_result.agreement_score < self._min_agreement:
                return (len(forecasts), 1, [])

            point_label = _bucket_label_for_value(all_buckets, ensemble_result.point_forecast)
            primary_forecast = _synthesize_forecast(forecasts, ensemble_result, sample.unit)
            local_signals: list[WeatherSignal] = []
            for m in group:
                m.raw["_bucket_role"] = {b.label: ("primary" if b.label == point_label else "adjacent") for b in m.buckets}
                m.raw["ensemble_meta"] = {
                    "agreement_score": ensemble_result.agreement_score,
                    "ensemble_point": ensemble_result.point_forecast,
                    "bias_applied_f": ensemble_result.bias_applied_f,
                    "ensemble_members": [
                        {"source": mb.source, "weight": mb.weight, "mean_f": mb.mean_f, "std_f": mb.std_f}
                        for mb in ensemble_result.members
                    ],
                }
                for bucket in m.buckets:
                    role = "primary" if bucket.label == point_label else "adjacent"
                    if role == "adjacent" and not self._trade_adjacent:
                        continue
                    p = ensemble_result.bucket_probs.get(bucket.label, 0.0)
                    edge = p - bucket.price
                    floor = self._min_edge if role == "primary" else self._adjacent_min_edge
                    if edge >= floor:
                        local_signals.append(WeatherSignal(
                            market=m,
                            bucket=bucket,
                            forecast=primary_forecast,
                            model_prob=p,
                            edge=edge,
                        ))
            return (len(forecasts), 0, local_signals)

        results = await asyncio.gather(*(_scan_event(g) for g in by_event.values()))
        for fc, sup, sigs in results:
            forecasts_fetched += fc
            suppressed += sup
            signals.extend(sigs)

        return _ScanReport(
            markets_scanned=len(markets),
            forecasts_fetched=forecasts_fetched,
            suppressed_low_agreement=suppressed,
            signals=signals,
        )


def _bucket_label_for_value(buckets: list[TemperatureBucket], value: float) -> str | None:
    """Return the label of the bucket that contains `value`, or None."""
    for b in buckets:
        if b.low <= value < b.high:
            return b.label
    return None


def _synthesize_forecast(
    forecasts: list[Forecast],
    ensemble_result: EnsembleResult,
    unit: str,
) -> Forecast:
    """Build a Forecast object representing the ensemble point estimate.

    Used as the `forecast` field on WeatherSignal so downstream metadata
    (forecast_temp, forecast_source) reflects the blend, not any one model.
    """
    sample = forecasts[0]
    sources = sorted({f.source for f in forecasts})
    return Forecast(
        station_id=sample.station_id,
        valid_from=sample.valid_from,
        valid_to=sample.valid_to,
        temperature=ensemble_result.point_forecast,
        std_dev=sample.std_dev,
        unit=unit,
        source="ensemble:" + "+".join(sources),
        fetched_at=datetime.now(timezone.utc),
    )
