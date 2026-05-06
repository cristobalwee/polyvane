"""Forecast fetchers — global coverage, multi-model, unit-aware.

Sources are addressed by short name:
  - 'noaa'              — National Weather Service (api.weather.gov). US only.
  - 'open_meteo_gfs'    — Open-Meteo, GFS run.
  - 'open_meteo_ecmwf'  — Open-Meteo, ECMWF IFS.
  - 'open_meteo_icon'   — Open-Meteo, DWD ICON.
  - 'open_meteo'        — Open-Meteo default blend (used as a single-model
                          fallback when ensemble isn't configured).

Each named source is fetched independently and produces its own `Forecast`.
The ensemble layer (see `ensemble.py`) then mixes them. The fetcher itself
holds no opinion about weighting or agreement — it just returns the daily
extreme (max or min) for the target UTC day in the market's reporting unit.

Internally the hourly series is always cached in fahrenheit (NOAA's native);
conversion to celsius happens at read time when the market reports in °C.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import aiohttp

from .models import Forecast


log = logging.getLogger("strategy.weather.noaa")


USER_AGENT = "polyvane-weather/0.3 (research bot; contact via repo)"
NWS_BASE = "https://api.weather.gov"
OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"


# Map our internal source names to Open-Meteo `models=` parameter values.
# `None` means use Open-Meteo's default blend (no `models=` parameter).
_OPEN_METEO_MODEL_PARAM: dict[str, str | None] = {
    "open_meteo": None,
    "open_meteo_gfs": "gfs_seamless",
    "open_meteo_ecmwf": "ecmwf_ifs04",
    "open_meteo_icon": "icon_seamless",
}

# Sources that don't cover non-US territory.
US_ONLY_SOURCES = frozenset({"noaa"})


def is_open_meteo(source: str) -> bool:
    return source in _OPEN_METEO_MODEL_PARAM


def all_known_sources() -> list[str]:
    return ["noaa", *_OPEN_METEO_MODEL_PARAM.keys()]


def c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def f_to_c(f: float) -> float:
    return (f - 32.0) * 5.0 / 9.0


def convert(value: float, from_unit: str, to_unit: str) -> float:
    if from_unit == to_unit:
        return value
    if from_unit == "celsius" and to_unit == "fahrenheit":
        return c_to_f(value)
    if from_unit == "fahrenheit" and to_unit == "celsius":
        return f_to_c(value)
    raise ValueError(f"unsupported unit conversion {from_unit!r} -> {to_unit!r}")


class _RateLimiter:
    """Async token bucket: `rate` calls per second, serial."""

    def __init__(self, rate: float) -> None:
        self._min_interval = 1.0 / rate
        self._lock = asyncio.Lock()
        self._next_at = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            wait = self._next_at - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = loop.time()
            self._next_at = max(now, self._next_at) + self._min_interval


class ForecastClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        max_rps: float = 5.0,
        request_timeout_sec: float = 15.0,
    ) -> None:
        self._session = session
        self._limiter = _RateLimiter(max_rps)
        self._timeout = aiohttp.ClientTimeout(total=request_timeout_sec)
        # Cache key: (source, lat, lon). Series stored in fahrenheit.
        self._series_cache: dict[tuple[str, float, float], tuple[datetime, list[tuple[datetime, float]]]] = {}
        self._series_ttl = timedelta(hours=1)
        # Per-source error counter (rolling, reset on each successful fetch).
        self._consecutive_errors: dict[str, int] = {s: 0 for s in all_known_sources()}

    @property
    def consecutive_errors(self) -> dict[str, int]:
        return dict(self._consecutive_errors)

    async def get_daily_extreme(
        self,
        *,
        source: str,
        station_id: str,
        lat: float,
        lon: float,
        target_date: date | datetime,
        metric: str,                     # 'highest' or 'lowest'
        unit: str,                       # 'fahrenheit' | 'celsius' — output unit
        default_std_dev: float = 2.0,
    ) -> Forecast | None:
        """Return the forecast HIGH or LOW for a given UTC calendar day from a single named model."""
        series_f = await self._get_hourly_series(source, lat, lon)
        if not series_f:
            return None

        target = target_date
        if isinstance(target, datetime):
            target = target.date()
        day_start = datetime(target.year, target.month, target.day, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        in_day = [(t, v) for (t, v) in series_f if day_start <= t < day_end]
        if not in_day:
            return None

        if metric == "highest":
            value_f = max(v for _, v in in_day)
        elif metric == "lowest":
            value_f = min(v for _, v in in_day)
        else:
            log.warning("Unknown metric %r — expected 'highest' or 'lowest'", metric)
            return None

        value = value_f if unit == "fahrenheit" else f_to_c(value_f)
        return Forecast(
            station_id=station_id,
            valid_from=day_start,
            valid_to=day_end,
            temperature=value,
            std_dev=default_std_dev,
            unit=unit,
            source=source,
            fetched_at=datetime.now(timezone.utc),
        )

    async def _get_hourly_series(
        self,
        source: str,
        lat: float,
        lon: float,
    ) -> list[tuple[datetime, float]]:
        """Return [(utc_time, temp_f)]. Cache key is per (source, lat, lon)."""
        key = (source, round(lat, 4), round(lon, 4))
        cached = self._series_cache.get(key)
        now = datetime.now(timezone.utc)
        if cached and now - cached[0] < self._series_ttl:
            return cached[1]

        try:
            if source == "noaa":
                series = await self._fetch_noaa_series(lat, lon)
            elif is_open_meteo(source):
                series = await self._fetch_open_meteo_series(lat, lon, _OPEN_METEO_MODEL_PARAM[source])
            else:
                log.warning("Unknown forecast source %r for %.4f,%.4f", source, lat, lon)
                return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self._consecutive_errors[source] = self._consecutive_errors.get(source, 0) + 1
            log.warning("Forecast fetch failed (%s, %.4f,%.4f): %s", source, lat, lon, e)
            return []

        if series:
            self._consecutive_errors[source] = 0
            self._series_cache[key] = (now, series)
        return series

    async def _fetch_noaa_series(self, lat: float, lon: float) -> list[tuple[datetime, float]]:
        points_url = f"{NWS_BASE}/points/{lat:.4f},{lon:.4f}"
        points = await self._get_json(points_url)
        if not points:
            return []
        forecast_url = points.get("properties", {}).get("forecastHourly")
        if not forecast_url:
            log.warning("NWS points response missing forecastHourly for %.4f,%.4f", lat, lon)
            return []
        hourly = await self._get_json(forecast_url)
        if not hourly:
            return []
        out: list[tuple[datetime, float]] = []
        for p in hourly.get("properties", {}).get("periods", []):
            t = _parse_iso(p["startTime"])
            temp = float(p["temperature"])
            if str(p.get("temperatureUnit", "F")).upper() == "C":
                temp = c_to_f(temp)
            out.append((t, temp))
        return out

    async def _fetch_open_meteo_series(
        self,
        lat: float,
        lon: float,
        model_param: str | None,
    ) -> list[tuple[datetime, float]]:
        params: dict[str, str] = {
            "latitude": f"{lat:.4f}",
            "longitude": f"{lon:.4f}",
            "hourly": "temperature_2m",
            "temperature_unit": "fahrenheit",
            "forecast_days": "5",
            "timezone": "UTC",
        }
        if model_param is not None:
            params["models"] = model_param
        data = await self._get_json(OPEN_METEO_BASE, params=params)
        if not data:
            return []
        hourly = data.get("hourly") or {}
        times = hourly.get("time") or []
        temps = hourly.get("temperature_2m") or []
        out: list[tuple[datetime, float]] = []
        for ts, t in zip(times, temps):
            if t is None:
                continue
            dt = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
            out.append((dt, float(t)))
        return out

    async def _get_json(self, url: str, *, params: dict[str, str] | None = None) -> dict[str, Any] | None:
        await self._limiter.acquire()
        headers = {"User-Agent": USER_AGENT, "Accept": "application/geo+json,application/json"}
        async with self._session.get(url, headers=headers, params=params, timeout=self._timeout) as resp:
            if resp.status == 429:
                log.warning("Rate limited by %s; backing off 2s", url)
                await asyncio.sleep(2.0)
                return None
            if resp.status >= 400:
                body = (await resp.text())[:200]
                log.warning("Fetch %s -> HTTP %d: %s", url, resp.status, body)
                return None
            return await resp.json()


def _parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
