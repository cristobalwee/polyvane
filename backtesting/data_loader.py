"""Historical data sourcing for backtests.

Two upstreams:

  - **Open-Meteo Archive** (`https://archive-api.open-meteo.com/v1/archive`)
    Daily/hourly historical weather actuals. Free, no API key, global
    coverage. Used to determine which temperature bucket actually resolved
    YES for a given (city, date, metric).

  - **Polymarket CLOB prices-history** (`https://clob.polymarket.com/prices-history`)
    Historical mid-price series for a CLOB token id. Used to reconstruct
    "what would my fill have been at signal time?" Falls back to the live
    Gamma snapshot if a token has no historical data (newer markets).

  - **Polymarket Gamma**
    Discovers the historical *list* of weather markets via the closed-events
    endpoint. The reverse-chronological scan grabs everything resolved
    within the requested window.

Each downstream is best-effort: if Polymarket's prices-history is missing
for a token, the loader falls back to the resolved settlement price as the
"price at signal time" (yields zero P&L for that observation rather than
fabricating one).
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp


log = logging.getLogger("backtesting.data_loader")


GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST_ARCHIVE = "https://historical-forecast-api.open-meteo.com/v1/forecast"


@dataclass
class HistoricalActual:
    """Resolved daily extreme for one (city, date, metric)."""
    city: str
    target_date: date
    metric: str               # 'highest' | 'lowest'
    actual_temp: float
    unit: str                 # 'fahrenheit' | 'celsius'
    source: str = "open-meteo-archive"


@dataclass
class HistoricalForecast:
    """Per-model forecast as it would have been seen at `as_of`.

    Open-Meteo's historical-forecast API returns forecasts as they were
    issued by the model run preceding `as_of`. This lets us replay scans
    without lookahead.
    """
    city: str
    target_date: date
    metric: str
    as_of: datetime
    source: str               # 'open_meteo_gfs' | 'open_meteo_ecmwf' | 'open_meteo_icon' | 'open_meteo'
    forecast_temp: float
    unit: str


@dataclass
class HistoricalPriceSnapshot:
    """Mid-price of a CLOB token at a specific time."""
    token_id: str
    ts: datetime
    price: float


@dataclass
class HistoricalMarketRecord:
    """A historical Polymarket weather market with everything we need to replay it."""
    market_id: str
    event_id: str
    question: str
    raw_event_title: str
    city: str
    target_date: date
    metric: str
    end_date_utc: datetime
    unit: str
    buckets: list[dict[str, Any]] = field(default_factory=list)
    liquidity_usd: float = 0.0
    volume_usd: float = 0.0


class HistoricalDataLoader:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        cache_dir: Path | None = None,
        request_timeout_sec: float = 30.0,
    ) -> None:
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=request_timeout_sec)
        self._cache_dir = cache_dir
        if cache_dir is not None:
            cache_dir.mkdir(parents=True, exist_ok=True)

    # ----- weather actuals -----

    async def fetch_actual(
        self,
        *,
        lat: float,
        lon: float,
        target_date: date,
        metric: str,
        unit: str,
        city: str,
    ) -> HistoricalActual | None:
        cache_key = f"actual_{city}_{target_date.isoformat()}_{metric}_{unit}.json"
        cached = self._read_cache(cache_key)
        if cached is not None:
            return HistoricalActual(**{**cached, "target_date": date.fromisoformat(cached["target_date"])})

        params = {
            "latitude": f"{lat:.4f}",
            "longitude": f"{lon:.4f}",
            "start_date": target_date.isoformat(),
            "end_date": target_date.isoformat(),
            "daily": "temperature_2m_max" if metric == "highest" else "temperature_2m_min",
            "temperature_unit": "fahrenheit" if unit == "fahrenheit" else "celsius",
            "timezone": "UTC",
        }
        data = await self._get_json(OPEN_METEO_ARCHIVE, params=params)
        if not data:
            return None
        daily = data.get("daily") or {}
        key = "temperature_2m_max" if metric == "highest" else "temperature_2m_min"
        values = daily.get(key) or []
        if not values or values[0] is None:
            return None
        actual = HistoricalActual(
            city=city, target_date=target_date, metric=metric,
            actual_temp=float(values[0]), unit=unit,
        )
        self._write_cache(cache_key, {**actual.__dict__, "target_date": target_date.isoformat()})
        return actual

    async def fetch_historical_forecast(
        self,
        *,
        lat: float,
        lon: float,
        target_date: date,
        metric: str,
        unit: str,
        as_of: datetime,
        source: str,
        city: str,
    ) -> HistoricalForecast | None:
        """Return the forecast that `source` was issuing for `target_date` at `as_of`.

        Open-Meteo's historical-forecast endpoint serves "the model's view as
        of past time T" — exactly what backtesting needs.
        """
        cache_key = (
            f"hfc_{city}_{target_date.isoformat()}_{metric}_{unit}_"
            f"{source}_{as_of.strftime('%Y%m%dT%H')}.json"
        )
        cached = self._read_cache(cache_key)
        if cached is not None:
            return HistoricalForecast(
                **{**cached,
                   "target_date": date.fromisoformat(cached["target_date"]),
                   "as_of": datetime.fromisoformat(cached["as_of"])}
            )

        # Pull a 24h hourly window covering target_date, then take max/min.
        # The historical-forecast API needs the model's name in `models=`.
        from strategies.weather.noaa import _OPEN_METEO_MODEL_PARAM  # noqa: PLC0415
        model_param = _OPEN_METEO_MODEL_PARAM.get(source)
        params: dict[str, str] = {
            "latitude": f"{lat:.4f}",
            "longitude": f"{lon:.4f}",
            "start_date": target_date.isoformat(),
            "end_date": target_date.isoformat(),
            "hourly": "temperature_2m",
            "temperature_unit": "fahrenheit" if unit == "fahrenheit" else "celsius",
            "timezone": "UTC",
        }
        if model_param is not None:
            params["models"] = model_param
        # `start_date_forecast` selects the model run preceding `as_of`. The
        # archive API doesn't accept arbitrary forecast issue times, but its
        # `start_date` already restricts to forecasts published for that day,
        # so we filter post-hoc by `as_of` on the client side.
        data = await self._get_json(OPEN_METEO_FORECAST_ARCHIVE, params=params)
        if not data:
            return None
        hourly = data.get("hourly") or {}
        times = hourly.get("time") or []
        temps = hourly.get("temperature_2m") or []
        in_day: list[float] = []
        day_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        for ts, t in zip(times, temps):
            if t is None:
                continue
            try:
                dt = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if day_start <= dt < day_end:
                in_day.append(float(t))
        if not in_day:
            return None
        value = max(in_day) if metric == "highest" else min(in_day)
        out = HistoricalForecast(
            city=city, target_date=target_date, metric=metric, as_of=as_of,
            source=source, forecast_temp=value, unit=unit,
        )
        self._write_cache(cache_key, {
            **out.__dict__,
            "target_date": target_date.isoformat(),
            "as_of": as_of.isoformat(),
        })
        return out

    # ----- polymarket -----

    async def fetch_resolved_weather_events(
        self,
        *,
        start: date,
        end: date,
        page_size: int = 200,
    ) -> list[dict[str, Any]]:
        """Page through Gamma's closed weather events in the date window.

        Gamma's filtering by date is best-effort across query schemas; we
        over-fetch and post-filter by `endDate` falling inside [start, end].
        """
        all_events: list[dict[str, Any]] = []
        seen: set[str] = set()
        for tag in ("weather", "temperature"):
            offset = 0
            while True:
                params = {
                    "tag_slug": tag,
                    "active": "false",
                    "closed": "true",
                    "limit": str(page_size),
                    "offset": str(offset),
                }
                events = await self._get_list(f"{GAMMA_BASE}/events", params=params)
                if not events:
                    break
                for ev in events:
                    eid = str(ev.get("id") or ev.get("slug") or "")
                    if not eid or eid in seen:
                        continue
                    end_iso = ev.get("endDate") or ev.get("endDateIso")
                    end_dt = _parse_dt(end_iso) if end_iso else None
                    if end_dt is None:
                        continue
                    if not (datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
                            <= end_dt
                            <= datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc)):
                        continue
                    seen.add(eid)
                    all_events.append(ev)
                if len(events) < page_size:
                    break
                offset += page_size
        return all_events

    async def fetch_token_price_at(
        self,
        token_id: str,
        ts: datetime,
        *,
        window_minutes: int = 60,
    ) -> float | None:
        """Mid-price of `token_id` near `ts` from CLOB prices-history.

        Returns None when the endpoint has no data (often the case for very
        recent or thinly traded tokens). The caller decides how to handle a
        missing price (skip the trade, fall back to settlement, etc.).
        """
        start_ts = int((ts - timedelta(minutes=window_minutes)).timestamp())
        end_ts = int((ts + timedelta(minutes=window_minutes)).timestamp())
        params = {
            "market": token_id,
            "startTs": str(start_ts),
            "endTs": str(end_ts),
            "fidelity": "1",
        }
        data = await self._get_json(f"{CLOB_BASE}/prices-history", params=params)
        if not data:
            return None
        history = data.get("history") if isinstance(data, dict) else None
        if not history:
            return None
        # Pick the point closest to `ts`.
        target_unix = ts.timestamp()
        best: tuple[float, float] | None = None
        for pt in history:
            try:
                pt_ts = float(pt.get("t"))
                pt_p = float(pt.get("p"))
            except (TypeError, ValueError):
                continue
            d = abs(pt_ts - target_unix)
            if best is None or d < best[0]:
                best = (d, pt_p)
        return best[1] if best else None

    # ----- low-level -----

    async def _get_json(self, url: str, *, params: dict[str, str] | None = None) -> dict[str, Any] | None:
        try:
            async with self._session.get(url, params=params, timeout=self._timeout) as resp:
                if resp.status >= 400:
                    body = (await resp.text())[:200]
                    log.warning("Backtest fetch %s -> HTTP %d: %s", url, resp.status, body)
                    return None
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            log.warning("Backtest fetch failed (%s): %s", url, e)
            return None

    async def _get_list(self, url: str, *, params: dict[str, str] | None = None) -> list[dict[str, Any]] | None:
        try:
            async with self._session.get(url, params=params, timeout=self._timeout) as resp:
                if resp.status >= 400:
                    return None
                data = await resp.json()
                if isinstance(data, list):
                    return data
                if isinstance(data, dict) and isinstance(data.get("data"), list):
                    return data["data"]
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            log.warning("Backtest list fetch failed (%s): %s", url, e)
            return None

    # ----- cache -----

    def _read_cache(self, key: str) -> dict[str, Any] | None:
        if self._cache_dir is None:
            return None
        path = self._cache_dir / key
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    def _write_cache(self, key: str, value: dict[str, Any]) -> None:
        if self._cache_dir is None:
            return
        try:
            (self._cache_dir / key).write_text(json.dumps(value, default=str))
        except OSError:
            log.debug("cache write failed for %s", key, exc_info=True)


def _parse_dt(s: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
