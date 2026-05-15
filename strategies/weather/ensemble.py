"""Multi-model ensemble forecaster.

Each configured model returns its own `Forecast` (point estimate + std dev).
The ensemble treats each as a Gaussian and blends them into a weighted
mixture; bucket probabilities come from integrating the mixture across each
bucket's [low, high) range.

Optionally applies a per-city bias correction before ensembling. The bias
comes from `calibrate.py`'s 30-day forecast-vs-actual study and represents
the city's historical warm-side error: positive bias means forecasts run
warm and we subtract before computing bucket probs. Cities without a
configured bias (or with |bias| below the apply-floor) pass through raw.

Three outputs per market:
  - `bucket_probs`: model probability for every bucket in the market.
  - `agreement_score` in [0, 1]: how tightly the per-model point forecasts
    cluster, normalized by an expected-spread scale (in the canonical unit).
    1.0 = identical forecasts; ~0 = forecasts spread well beyond the typical
    forecast error. Below `min_agreement` the ensemble is suppressed and no
    signals are emitted for that market.
  - `bias_applied_f`: amount subtracted from each member's mean for this
    city. Logged into trade metadata so we can A/B corrected vs uncorrected
    after the fact.

Canonical internal unit is fahrenheit. Per-model forecasts are converted to
fahrenheit on intake; output is converted back to the market's reporting unit
when the ensemble result is read.
"""
from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import date
from typing import Sequence

from .models import Forecast, TemperatureBucket
from .noaa import (
    ForecastClient,
    US_ONLY_SOURCES,
    c_to_f,
    f_to_c,
)
from .resolution import ResolutionSource


log = logging.getLogger("strategy.weather.ensemble")


# Default canonical-unit (°F) std devs per source. Open-Meteo's three NWP
# variants don't all agree on accuracy — ECMWF is the most skillful in
# multi-day temperature, ICON close behind, GFS noisier. These are starting
# points; the calibration script tunes them per-city.
DEFAULT_MODEL_STD_DEV_F: dict[str, float] = {
    "noaa": 1.8,
    "open_meteo_ecmwf": 1.9,
    "open_meteo_icon": 2.1,
    "open_meteo_gfs": 2.4,
    "open_meteo": 2.0,
}


@dataclass
class ModelForecast:
    """One model's contribution to the ensemble, in canonical fahrenheit."""
    source: str
    weight: float
    mean_f: float
    std_f: float


@dataclass
class EnsembleResult:
    """Output for one (market, target_date) pair."""
    members: list[ModelForecast]
    point_forecast: float          # weighted-mean point estimate, in market unit
    point_forecast_f: float        # canonical-unit point estimate
    unit: str                      # market's reporting unit
    agreement_score: float         # [0, 1]
    bucket_probs: dict[str, float]  # bucket label -> model probability
    # Bias correction (°F) applied to each member's mean before ensembling.
    # Positive = raw forecasts run warm; we subtracted this much from each
    # member to land at point_forecast_f. 0.0 when no per-city correction
    # is configured for the city, or when |bias| < min_bias_apply_f.
    bias_applied_f: float = 0.0


class EnsembleForecaster:
    """Blend multiple per-model `Forecast`s into bucket probabilities.

    `model_weights` is a {source: weight} map. Sources missing from
    `available_sources` (e.g. NOAA for non-US) are dropped and the remaining
    weights renormalized.
    """

    def __init__(
        self,
        client: ForecastClient,
        *,
        model_weights: dict[str, float],
        per_source_std_dev_f: dict[str, float] | None = None,
        min_agreement: float = 0.7,
        # The agreement-score scale: what spread (°F, std of per-model means)
        # corresponds to "moderate disagreement." 0 std → 1.0 score; this
        # scale → ~0.37; 2x → ~0.14. Tuned to ~3°F by default — beyond a
        # 3°F spread across models, we should be skeptical.
        agreement_scale_f: float = 3.0,
        # Per-city bias correction (°F). Each member's mean has the city's
        # bias subtracted before ensembling. Positive bias = raw forecasts
        # run warm; subtracting cools the corrected point. Sourced from
        # calibrate.py per-city RMSE/bias output.
        per_city_bias_f: dict[str, float] | None = None,
        # Floor on |bias| to actually apply the correction. Below this,
        # treat as noise and skip — protects cities with thin sample counts
        # or near-zero bias from over-correction.
        min_bias_apply_f: float = 0.5,
    ) -> None:
        self._client = client
        self._weights = dict(model_weights)
        self._stds = dict(per_source_std_dev_f or DEFAULT_MODEL_STD_DEV_F)
        self.min_agreement = float(min_agreement)
        self._agreement_scale_f = float(agreement_scale_f)
        self._per_city_bias_f = {k: float(v) for k, v in (per_city_bias_f or {}).items()}
        self._min_bias_apply_f = float(min_bias_apply_f)

    def bias_for_city(self, city: str | None) -> float:
        if not city:
            return 0.0
        b = self._per_city_bias_f.get(city, 0.0)
        return b if abs(b) >= self._min_bias_apply_f else 0.0

    @property
    def configured_sources(self) -> list[str]:
        return list(self._weights.keys())

    def sources_for(self, src: ResolutionSource) -> list[str]:
        """Drop sources that don't cover this station (NOAA for non-US)."""
        return [
            s for s in self._weights.keys()
            if not (s in US_ONLY_SOURCES and not src.is_us)
        ]

    def per_source_std_f(self, source: str) -> float:
        return float(self._stds.get(source, DEFAULT_MODEL_STD_DEV_F.get(source, 2.0)))

    async def fetch_member_forecasts(
        self,
        *,
        src: ResolutionSource,
        target_date: date,
        metric: str,
        unit: str,
    ) -> list[Forecast]:
        """Fan out concurrent fetches across all eligible sources."""
        sources = self.sources_for(src)
        if not sources:
            return []

        async def _fetch(source: str) -> Forecast | None:
            std_f = self.per_source_std_f(source)
            std_in_market_unit = std_f if unit == "fahrenheit" else std_f * 5.0 / 9.0
            return await self._client.get_daily_extreme(
                source=source,
                station_id=src.station_id,
                lat=src.lat,
                lon=src.lon,
                target_date=target_date,
                metric=metric,
                unit=unit,
                default_std_dev=std_in_market_unit,
            )

        return [f for f in await asyncio.gather(*(_fetch(s) for s in sources)) if f is not None]

    def build(
        self,
        forecasts: Sequence[Forecast],
        buckets: Sequence[TemperatureBucket],
        market_unit: str,
        *,
        city: str | None = None,
    ) -> EnsembleResult | None:
        """Combine per-model forecasts into bucket probabilities + agreement.

        Internally everything works in fahrenheit; we convert per-model means
        and the bucket bounds when the market reports in celsius. When `city`
        has a configured bias, each member's mean is corrected before any
        downstream computation (point forecast, agreement, bucket probs).
        """
        bias_f = self.bias_for_city(city)
        members = self._members_in_canonical_unit(forecasts, bias_f=bias_f)
        if not members:
            return None

        point_f = sum(m.weight * m.mean_f for m in members)
        # Spread of per-model point forecasts (weighted std), in fahrenheit.
        var = sum(m.weight * (m.mean_f - point_f) ** 2 for m in members)
        spread_f = math.sqrt(max(var, 0.0))
        agreement = math.exp(-spread_f / max(self._agreement_scale_f, 1e-6))

        bucket_probs: dict[str, float] = {}
        for b in buckets:
            lo_f, hi_f = _bucket_bounds_to_f(b)
            p = 0.0
            for m in members:
                p += m.weight * _gaussian_interval_prob(lo_f, hi_f, m.mean_f, m.std_f)
            bucket_probs[b.label] = max(0.0, min(1.0, p))

        point_in_market_unit = point_f if market_unit == "fahrenheit" else f_to_c(point_f)
        return EnsembleResult(
            members=members,
            point_forecast=point_in_market_unit,
            point_forecast_f=point_f,
            unit=market_unit,
            agreement_score=agreement,
            bucket_probs=bucket_probs,
            bias_applied_f=bias_f,
        )

    def cdf_exceedance(
        self,
        forecasts: Sequence[Forecast],
        threshold_f: float,
        *,
        city: str | None = None,
    ) -> float:
        """P(temp >= threshold_f) under the weighted Gaussian mixture.

        Used for Kalshi binary threshold markets ("Will HIGH temp be >= X°F?")
        instead of bucket probabilities. Applies the same per-city bias
        correction as `build()`.
        """
        bias_f = self.bias_for_city(city)
        members = self._members_in_canonical_unit(forecasts, bias_f=bias_f)
        if not members:
            return 0.0
        p = sum(
            m.weight * (1.0 - _norm_cdf((threshold_f - m.mean_f) / max(m.std_f, 1e-9)))
            for m in members
        )
        return max(0.0, min(1.0, p))

    def _members_in_canonical_unit(
        self,
        forecasts: Sequence[Forecast],
        *,
        bias_f: float = 0.0,
    ) -> list[ModelForecast]:
        """Convert each forecast to fahrenheit, drop unweighted sources, renormalize.

        When `bias_f != 0`, subtract it from each member's mean — the bias is
        the historically-observed warm-direction error of the city's
        forecasts (positive bias = forecasts run warm). Std and weights are
        unchanged.
        """
        members: list[ModelForecast] = []
        for f in forecasts:
            w = self._weights.get(f.source)
            if w is None or w <= 0:
                continue
            raw_mean_f = f.temperature if f.unit == "fahrenheit" else c_to_f(f.temperature)
            mean_f = raw_mean_f - bias_f
            std_f = self.per_source_std_f(f.source)
            members.append(ModelForecast(source=f.source, weight=w, mean_f=mean_f, std_f=std_f))
        if not members:
            return []
        total = sum(m.weight for m in members)
        if total <= 0:
            return []
        return [
            ModelForecast(source=m.source, weight=m.weight / total, mean_f=m.mean_f, std_f=m.std_f)
            for m in members
        ]


def _bucket_bounds_to_f(b: TemperatureBucket) -> tuple[float, float]:
    if b.unit == "fahrenheit":
        return (b.low, b.high)
    # celsius -> fahrenheit; preserve infinities
    lo = -math.inf if b.low == -math.inf else c_to_f(b.low)
    hi = math.inf if b.high == math.inf else c_to_f(b.high)
    return (lo, hi)


def _gaussian_interval_prob(low: float, high: float, mean: float, std: float) -> float:
    """P(temp in [low, high)) under N(mean, std**2)."""
    if std <= 0:
        return 1.0 if low <= mean < high else 0.0
    return _norm_cdf((high - mean) / std) - _norm_cdf((low - mean) / std)


def _norm_cdf(x: float) -> float:
    if x == math.inf:
        return 1.0
    if x == -math.inf:
        return 0.0
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
