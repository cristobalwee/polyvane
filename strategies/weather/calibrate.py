"""Per-model + per-city calibration against the last 30 days of resolutions.

Walks resolved weather markets in the recent past, runs each ensemble
member's historical forecast against the actual outcome, and reports:

  - **Model weight recommendations**: inverse-MSE weighting across the
    sources that have data for at least N markets. The output is a
    drop-in `forecast_model_weights:` block for config.yaml.

  - **Per-city std-dev recommendations**: empirical RMSE of the ensemble
    point forecast vs the actual, in fahrenheit. If a city's empirical
    error is meaningfully different from the configured default, the
    script prints a tuned value to drop into `per_source_std_dev_f` (or,
    for one-off per-city overrides, into a dedicated section the strategy
    can grow into).

  - **Resolution sanity check**: flags cities where ensemble RMSE exceeds
    `--anomaly-rmse` (default 5°F). High RMSE in one city while neighbors
    look fine is a tell that the resolution station in the registry
    might not match what Polymarket is using.

Standalone:

    python -m strategies.weather.calibrate
    python -m strategies.weather.calibrate --days 60 --min-samples 8

The script does NOT mutate config.yaml — it prints recommendations.
The operator decides what to apply.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp

from backtesting.adapters.weather import _flatten_event, _load_weather_params
from backtesting.data_loader import HistoricalDataLoader
from strategies.weather import resolution
from strategies.weather.noaa import US_ONLY_SOURCES, all_known_sources, c_to_f


log = logging.getLogger("strategy.weather.calibrate")


async def _amain(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    params = _load_weather_params()
    cities_cfg = params.get("cities") or resolution.all_cities()
    require_confirmed = bool(params.get("require_confirmed_resolution", True))
    tradeable, _ = resolution.filter_tradeable(cities_cfg, require_confirmed=require_confirmed)
    tradeable_set = set(tradeable)
    sources = list(params.get("forecast_models") or all_known_sources())

    end = date.today()
    start = end - timedelta(days=args.days)

    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    async with aiohttp.ClientSession() as session:
        loader = HistoricalDataLoader(session, cache_dir=cache_dir)
        events = await loader.fetch_resolved_weather_events(start=start, end=end)
        log.info("Fetched %d closed events in [%s, %s]", len(events), start, end)

        # Per-source error lists, in canonical fahrenheit.
        # Per-city ensemble-point error lists, in fahrenheit.
        per_source_errors_f: dict[str, list[float]] = defaultdict(list)
        per_city_errors_f: dict[str, list[float]] = defaultdict(list)
        per_city_per_source_errors_f: dict[tuple[str, str], list[float]] = defaultdict(list)
        sample_counts: dict[str, int] = defaultdict(int)

        for ev in events:
            for rec in _flatten_event(ev, tradeable_set):
                src = resolution.get(rec.city)
                if src is None:
                    continue
                actual = await loader.fetch_actual(
                    lat=src.lat, lon=src.lon,
                    target_date=rec.target_date,
                    metric=rec.metric, unit=rec.unit, city=rec.city,
                )
                if actual is None:
                    continue
                actual_f = actual.actual_temp if actual.unit == "fahrenheit" else c_to_f(actual.actual_temp)
                # Use as_of = end - 24h as the calibration as-of point. This
                # matches the most common live-trade window (24-36h horizon).
                as_of = rec.end_date_utc - timedelta(hours=24)
                model_means_f: list[float] = []
                for source in sources:
                    if source in US_ONLY_SOURCES and not src.is_us:
                        continue
                    hf = await loader.fetch_historical_forecast(
                        lat=src.lat, lon=src.lon,
                        target_date=rec.target_date, metric=rec.metric,
                        unit=rec.unit, as_of=as_of, source=source, city=rec.city,
                    )
                    if hf is None:
                        continue
                    forecast_f = hf.forecast_temp if hf.unit == "fahrenheit" else c_to_f(hf.forecast_temp)
                    err = forecast_f - actual_f
                    per_source_errors_f[source].append(err)
                    per_city_per_source_errors_f[(rec.city, source)].append(err)
                    sample_counts[source] += 1
                    model_means_f.append(forecast_f)
                if model_means_f:
                    ensemble_point = sum(model_means_f) / len(model_means_f)
                    per_city_errors_f[rec.city].append(ensemble_point - actual_f)

    _print_report(
        per_source_errors_f, per_city_errors_f, per_city_per_source_errors_f,
        sample_counts, min_samples=args.min_samples, anomaly_rmse=args.anomaly_rmse,
    )
    return 0


def _rmse(errors: list[float]) -> float:
    if not errors:
        return 0.0
    return math.sqrt(sum(e * e for e in errors) / len(errors))


def _bias(errors: list[float]) -> float:
    if not errors:
        return 0.0
    return sum(errors) / len(errors)


def _print_report(
    per_source_errors_f: dict[str, list[float]],
    per_city_errors_f: dict[str, list[float]],
    per_city_per_source_errors_f: dict[tuple[str, str], list[float]],
    sample_counts: dict[str, int],
    *,
    min_samples: int,
    anomaly_rmse: float,
) -> None:
    print("=" * 72)
    print("Per-source error (in °F)")
    print(f"  {'source':<20} {'samples':>8} {'rmse':>8} {'bias':>8}")
    for source, errs in sorted(per_source_errors_f.items()):
        rmse = _rmse(errs)
        bias = _bias(errs)
        print(f"  {source:<20} {len(errs):>8d} {rmse:>8.3f} {bias:>+8.3f}")
    print()

    # Inverse-MSE weight recommendation.
    eligible = {
        s: errs for s, errs in per_source_errors_f.items()
        if len(errs) >= min_samples
    }
    if eligible:
        weights = {s: 1.0 / max(_rmse(errs) ** 2, 1e-6) for s, errs in eligible.items()}
        total = sum(weights.values())
        normalized = {s: w / total for s, w in weights.items()}
        print("Recommended weights (drop into config.yaml under forecast_model_weights):")
        print("  forecast_model_weights:")
        for s, w in sorted(normalized.items(), key=lambda kv: -kv[1]):
            print(f"    {s}: {w:.3f}")
        print()

    # Per-source std-dev recommendation = ensemble RMSE per source.
    print("Per-source std-dev recommendation (RMSE in °F):")
    print("  per_source_std_dev_f:")
    for source, errs in sorted(per_source_errors_f.items()):
        if len(errs) < min_samples:
            continue
        print(f"    {source}: {_rmse(errs):.2f}")
    print()

    # Per-city ensemble RMSE.
    print("Per-city ensemble RMSE (°F) — flags resolution-source anomalies")
    print(f"  {'city':<20} {'samples':>8} {'rmse':>8} {'bias':>8} {'flag':>10}")
    for city, errs in sorted(per_city_errors_f.items()):
        rmse = _rmse(errs)
        bias = _bias(errs)
        flag = "ANOMALY" if rmse >= anomaly_rmse else ""
        print(f"  {city:<20} {len(errs):>8d} {rmse:>8.3f} {bias:>+8.3f} {flag:>10}")
    print()

    # Per-(city, source) breakdown when a city is anomalous.
    anomalous_cities = [c for c, errs in per_city_errors_f.items() if _rmse(errs) >= anomaly_rmse]
    if anomalous_cities:
        print("Per-source breakdown for anomalous cities:")
        for city in anomalous_cities:
            print(f"  {city}:")
            for (c, source), errs in per_city_per_source_errors_f.items():
                if c != city:
                    continue
                if len(errs) < 1:
                    continue
                print(f"    {source:<20} samples={len(errs)} rmse={_rmse(errs):.3f} bias={_bias(errs):+.3f}")
    print("=" * 72)


def main() -> None:
    parser = argparse.ArgumentParser(prog="strategies.weather.calibrate")
    parser.add_argument("--days", type=int, default=30,
                        help="Calibration window length in days (default 30)")
    parser.add_argument("--min-samples", type=int, default=5,
                        help="Per-source samples required before recommending weights / std devs")
    parser.add_argument("--anomaly-rmse", type=float, default=5.0,
                        help="Per-city RMSE (°F) above which to flag resolution anomaly")
    parser.add_argument("--cache-dir", default=".calibrate-cache",
                        help="Reuse the backtest data cache to avoid re-fetching")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_amain(args)))


if __name__ == "__main__":
    main()
