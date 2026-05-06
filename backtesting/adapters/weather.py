"""Weather strategy adapter for the backtest runner.

Replays weather markets through the same ensemble + bucket scoring logic
the live `WeatherStrategy` uses, but with historical inputs:
  - market list comes from Gamma's closed-events endpoint
  - per-model forecasts come from Open-Meteo's historical-forecast API
    (the model run preceding the as-of timestamp)
  - actuals come from Open-Meteo's archive API

Settlement is by bucket containment: if `actual_temp` falls inside
[bucket.low, bucket.high), YES wins (settled = 1.0); otherwise 0.0.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

import yaml

from strategies.base import Signal, TradeIntent
from strategies.weather import resolution
from strategies.weather.ensemble import EnsembleForecaster
from strategies.weather.markets import _parse_bucket  # type: ignore[attr-defined]
from strategies.weather.models import Forecast, TemperatureBucket
from strategies.weather.noaa import US_ONLY_SOURCES, all_known_sources
from strategies.weather.strategy import (
    WeatherStrategy,
    _DEFAULT_FORECAST_MODELS,
    _bucket_label_for_value,
)

from ..data_loader import HistoricalDataLoader, HistoricalMarketRecord
from ..runner import HistoricalMarket, HistoricalResolution


log = logging.getLogger("backtesting.adapter.weather")


CONFIG_PATH = "config/config.yaml"


def _load_weather_params() -> dict[str, Any]:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f) or {}
    for s in cfg.get("strategies", []):
        if s.get("name") == "weather":
            return dict(s.get("params", {}))
    return {}


class WeatherAdapter:
    name = "weather"

    def __init__(self, *, param_overrides: dict[str, Any] | None = None) -> None:
        params = _load_weather_params()
        if param_overrides:
            params.update(param_overrides)
        self._params = params

        # Ensemble configuration mirrors WeatherStrategy._build_weights but
        # without instantiating aiohttp / forecast clients (the adapter does
        # its own historical fetching). The ensemble math is reused.
        models = params.get("forecast_models") or _DEFAULT_FORECAST_MODELS
        weights_cfg = params.get("forecast_model_weights") or {}
        self._weights = WeatherStrategy._build_weights(models, weights_cfg)
        self._per_source_std = {
            k: float(v) for k, v in (params.get("per_source_std_dev_f") or {}).items()
        }
        self._min_agreement = float(params.get("min_agreement", 0.7))
        self._agreement_scale_f = float(params.get("agreement_scale_f", 3.0))
        self._min_edge = float(params.get("min_edge_pct", 0.15))
        self._adjacent_min_edge = float(params.get("adjacent_min_edge_pct", 0.10))
        self._trade_adjacent = bool(params.get("trade_adjacent_buckets", True))
        self._min_liquidity = float(params.get("min_liquidity_usd", 100.0))
        self._min_hours = float(params.get("min_hours_to_resolution", 12))
        self._max_horizon = float(params.get("max_forecast_horizon_hours", 48))
        self._tradeable, _ = resolution.filter_tradeable(
            params.get("cities") or resolution.all_cities(),
            require_confirmed=bool(params.get("require_confirmed_resolution", True)),
        )

    # ----- discovery -----

    async def discover_markets(
        self,
        loader: HistoricalDataLoader,
        *,
        start: date,
        end: date,
    ) -> list[HistoricalMarket]:
        events = await loader.fetch_resolved_weather_events(start=start, end=end)
        out: list[HistoricalMarket] = []
        for ev in events:
            for record in _flatten_event(ev, set(self._tradeable)):
                out.append(HistoricalMarket(
                    market_id=record.market_id,
                    category="weather",
                    end_date_utc=record.end_date_utc,
                    target_date=record.target_date,
                    metadata=record.__dict__,
                ))
        return out

    # ----- signal generation -----

    async def signals_for(
        self,
        market: HistoricalMarket,
        *,
        as_of: datetime,
        loader: HistoricalDataLoader,
    ) -> list[Signal]:
        rec = HistoricalMarketRecord(**{
            k: v for k, v in market.metadata.items()
            if k in HistoricalMarketRecord.__dataclass_fields__
        })
        src = resolution.get(rec.city)
        if src is None:
            return []

        eligible = [
            s for s in self._weights.keys()
            if not (s in US_ONLY_SOURCES and not src.is_us)
        ]
        if not eligible:
            return []

        forecasts: list[Forecast] = []
        for source in eligible:
            hf = await loader.fetch_historical_forecast(
                lat=src.lat, lon=src.lon,
                target_date=rec.target_date, metric=rec.metric,
                unit=rec.unit, as_of=as_of, source=source, city=rec.city,
            )
            if hf is None:
                continue
            forecasts.append(Forecast(
                station_id=src.station_id,
                valid_from=datetime.combine(rec.target_date, datetime.min.time(), tzinfo=timezone.utc),
                valid_to=datetime.combine(rec.target_date, datetime.max.time(), tzinfo=timezone.utc),
                temperature=hf.forecast_temp,
                std_dev=2.0,
                unit=rec.unit,
                source=source,
                fetched_at=as_of,
            ))
        if not forecasts:
            return []

        # Reuse the live ensemble math directly; pass `client=None` since
        # the adapter never calls fetch_member_forecasts.
        ensemble = EnsembleForecaster(
            client=None,  # type: ignore[arg-type]
            model_weights=self._weights,
            per_source_std_dev_f=self._per_source_std or None,
            min_agreement=self._min_agreement,
            agreement_scale_f=self._agreement_scale_f,
        )
        buckets = [_bucket_from_dict(b) for b in rec.buckets]
        result = ensemble.build(forecasts, buckets, rec.unit)
        if result is None or result.agreement_score < self._min_agreement:
            return []

        point_label = _bucket_label_for_value(buckets, result.point_forecast)
        signals: list[Signal] = []
        for b, raw in zip(buckets, rec.buckets):
            role = "primary" if b.label == point_label else "adjacent"
            if role == "adjacent" and not self._trade_adjacent:
                continue
            p = result.bucket_probs.get(b.label, 0.0)
            edge = p - b.price
            floor = self._min_edge if role == "primary" else self._adjacent_min_edge
            if edge < floor:
                continue
            signals.append(Signal(
                market_id=str(raw.get("market_id") or rec.market_id),
                direction="YES",
                edge=edge,
                confidence=min(1.0, max(0.0, p)),
                market_question=rec.question,
                price=b.price,
                category="weather",
                token_id=raw.get("token_id"),
                metadata={
                    "city": rec.city,
                    "bucket": b.label,
                    "bucket_role": role,
                    "bucket_low": b.low,
                    "bucket_high": b.high,
                    "unit": rec.unit,
                    "metric": rec.metric,
                    "model_prob": p,
                    "agreement_score": result.agreement_score,
                    "ensemble_point": result.point_forecast,
                    "forecast_temp": result.point_forecast,
                    "forecast_unit": rec.unit,
                    "forecast_source": "ensemble:" + "+".join(sorted({f.source for f in forecasts})),
                    "target_date": rec.target_date.isoformat(),
                    "end_utc": rec.end_date_utc.isoformat(),
                    "liquidity_usd": rec.liquidity_usd,
                    "volume_usd": rec.volume_usd,
                    "ensemble_members": [
                        {"source": m.source, "weight": m.weight, "mean_f": m.mean_f, "std_f": m.std_f}
                        for m in result.members
                    ],
                },
            ))
        return signals

    # ----- evaluation gate -----

    async def evaluate(self, signal: Signal) -> TradeIntent | None:
        role = signal.metadata.get("bucket_role", "primary")
        floor = self._min_edge if role == "primary" else self._adjacent_min_edge
        if signal.edge < floor:
            return None
        if float(signal.metadata.get("liquidity_usd", 0.0)) < self._min_liquidity:
            return None
        # Horizon gate is checked at signal-generation time (the runner picks
        # as_of offsets explicitly), so don't re-gate on hours_to_end here.
        return TradeIntent(signal=signal, size_usd_hint=None, reason=f"weather[{role}]")

    # ----- resolution -----

    async def resolve(
        self,
        market: HistoricalMarket,
        *,
        loader: HistoricalDataLoader,
    ) -> HistoricalResolution | None:
        meta = market.metadata
        src = resolution.get(str(meta.get("city")))
        if src is None:
            return None
        actual = await loader.fetch_actual(
            lat=src.lat, lon=src.lon,
            target_date=market.target_date,
            metric=str(meta.get("metric")),
            unit=str(meta.get("unit")),
            city=str(meta.get("city")),
        )
        if actual is None:
            return None
        return HistoricalResolution(
            market_id=market.market_id,
            metadata={
                "actual_temp": actual.actual_temp,
                "unit": actual.unit,
            },
        )

    def settle(self, signal: Signal, resolution: HistoricalResolution) -> float:
        actual = float(resolution.metadata["actual_temp"])
        lo = float(signal.metadata["bucket_low"])
        hi = float(signal.metadata["bucket_high"])
        won = lo <= actual < hi
        return 1.0 if won else 0.0


# ----- helpers -----


def _bucket_from_dict(d: dict[str, Any]) -> TemperatureBucket:
    return TemperatureBucket(
        low=float(d["low"]),
        high=float(d["high"]),
        unit=str(d["unit"]),
        label=str(d["label"]),
        token_id=d.get("token_id"),
        price=float(d.get("price", 0.0)),
    )


def _flatten_event(ev: dict[str, Any], tradeable: set[str]) -> list[HistoricalMarketRecord]:
    """Mirror of `markets.GammaClient._parse_event` but for closed events.

    Reuses the same regex parsers; produces one HistoricalMarketRecord per
    event (with all buckets aggregated).
    """
    from datetime import datetime as _dt
    from strategies.weather.markets import (
        _EVENT_TITLE_RX, _MONTHS, _normalize_city, _parse_dt, _yes_side, _maybe_json,
    )
    title = str(ev.get("title") or "")
    m = _EVENT_TITLE_RX.search(title)
    if not m:
        return []
    metric = m["metric"].lower()
    raw_city = m["city"].strip()
    city = _normalize_city(raw_city)
    if city is None or city not in tradeable:
        return []
    src = resolution.get(city)
    if src is None:
        return []
    try:
        month = _MONTHS[m["month"].lower()[:3]]
        day = int(m["day"])
        year = int(m["year"]) if m["year"] else _dt.now(timezone.utc).year
        target_date = date(year, month, day)
    except (KeyError, ValueError):
        return []
    end_iso = ev.get("endDate") or ev.get("endDateIso")
    end_dt = _parse_dt(end_iso) if end_iso else None
    if end_dt is None:
        return []
    market_unit = "fahrenheit" if src.unit == "fahrenheit" else "celsius"

    # Group all buckets under one record per event so the ensemble runs once
    # over the full bucket set for that market.
    buckets: list[dict[str, Any]] = []
    sample_market_id = None
    sample_question = None
    total_liq = 0.0
    total_vol = 0.0
    for raw in ev.get("markets") or []:
        bucket = _parse_bucket(str(raw.get("question") or ""))
        if bucket is None:
            continue
        lo, hi, label, parsed_unit = bucket
        if parsed_unit != market_unit:
            continue
        outcomes = _maybe_json(raw.get("outcomes"))
        prices = _maybe_json(raw.get("outcomePrices"))
        token_ids = _maybe_json(raw.get("clobTokenIds"))
        yes_price, yes_token = _yes_side(outcomes, prices, token_ids)
        if yes_price is None:
            continue
        market_id = str(raw.get("id") or raw.get("conditionId") or raw.get("slug"))
        if sample_market_id is None:
            sample_market_id = market_id
            sample_question = str(raw.get("question") or "")
        liq = float(raw.get("liquidityNum") or raw.get("liquidity") or 0.0)
        vol = float(raw.get("volumeNum") or raw.get("volume") or raw.get("volumeClob") or 0.0)
        total_liq += liq
        total_vol += vol
        buckets.append({
            "low": lo, "high": hi, "unit": market_unit, "label": label,
            "token_id": yes_token, "price": yes_price, "market_id": market_id,
            "liquidity_usd": liq, "volume_usd": vol,
        })

    if not buckets or sample_market_id is None:
        return []
    return [HistoricalMarketRecord(
        market_id=sample_market_id,
        event_id=str(ev.get("id") or ev.get("slug") or ""),
        question=str(sample_question),
        raw_event_title=title,
        city=city,
        target_date=target_date,
        metric=metric,
        end_date_utc=end_dt,
        unit=market_unit,
        buckets=buckets,
        liquidity_usd=total_liq,
        volume_usd=total_vol,
    )]
