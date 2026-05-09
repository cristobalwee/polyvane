"""Polymarket weather-market discovery via the public Gamma API.

We query `/events?tag_slug=weather` (no auth needed). Each event groups
several binary YES/NO sub-markets — one per temperature bucket — under a
common question like "Highest temperature in NYC on April 26?". We
parse the event title for (city, metric, target_date) and each sub-market's
question for its bucket range and YES price.

Currently supported question shapes (case-insensitive), in °F or °C:
  - "... be between LO-HI° ..."
  - "... be between LO and HI° ..."   (also "&")
  - "... be HI° or below ..."
  - "... be LO° or higher ..."
"""
from __future__ import annotations

import json
import logging
import math
import re
from datetime import date, datetime, timezone
from typing import Any

import aiohttp

from . import resolution
from .models import TemperatureBucket, WeatherMarket


log = logging.getLogger("strategy.weather.markets")


GAMMA_BASE = "https://gamma-api.polymarket.com"


# Sub-market question patterns. The unit is captured so the bucket can be
# tagged (markets resolve in either °F or °C depending on the city).
_BETWEEN_DASH_RX = re.compile(
    r"between\s+(?P<lo>-?\d{1,3})\s*[-–to]+\s*(?P<hi>-?\d{1,3})\s*°?\s*(?P<unit>[FC])",
    re.IGNORECASE,
)
_BETWEEN_AND_RX = re.compile(
    r"between\s+(?P<lo>-?\d{1,3})\s*(?:&|and)\s*(?P<hi>-?\d{1,3})\s*°?\s*(?P<unit>[FC])",
    re.IGNORECASE,
)
_OR_BELOW_RX = re.compile(
    r"(?P<hi>-?\d{1,3})\s*°?\s*(?P<unit>[FC])\s+or\s+below",
    re.IGNORECASE,
)
_OR_HIGHER_RX = re.compile(
    r"(?P<lo>-?\d{1,3})\s*°?\s*(?P<unit>[FC])\s+or\s+(?:higher|above)",
    re.IGNORECASE,
)


# Event-title parsing.
_EVENT_TITLE_RX = re.compile(
    r"(?P<metric>highest|lowest)\s+temperature\s+in\s+(?P<city>[A-Za-z .]+?)\s+on\s+"
    r"(?P<month>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+"
    r"(?P<day>\d{1,2})(?:,\s*(?P<year>\d{4}))?",
    re.IGNORECASE,
)
_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}


# Aliases for city names as they appear in Polymarket event titles. Maps
# raw spellings to a canonical resolution-registry city key. Keep order
# stable: longer/more-specific aliases come first within a list because
# `_normalize_city` does a substring check (e.g. so "los angeles" matches
# before "san francisco" can mis-match a generic substring).
_CITY_ALIASES: dict[str, list[str]] = {
    "NYC":            ["nyc", "new york city", "new york"],
    "London":         ["london"],
    "Hong Kong":      ["hong kong", "hk"],
    "Seoul":          ["seoul"],
    "Shanghai":       ["shanghai"],
    "Dallas":         ["dallas"],
    "Atlanta":        ["atlanta"],
    "Toronto":        ["toronto"],
    "Ankara":         ["ankara"],
    "Wellington":     ["wellington"],
    # US additions
    "Austin":         ["austin"],
    "Chicago":        ["chicago"],
    "Denver":         ["denver"],
    "Houston":        ["houston"],
    "Los Angeles":    ["los angeles", "la"],
    "Miami":          ["miami"],
    "San Francisco":  ["san francisco", "sf"],
    "Seattle":        ["seattle"],
    # International additions (unconfirmed sources — see resolution.py)
    "Beijing":        ["beijing"],
    "Tokyo":          ["tokyo"],
    "Singapore":      ["singapore"],
    "Mexico City":    ["mexico city"],
    "Sao Paulo":      ["sao paulo", "são paulo"],
    "Buenos Aires":   ["buenos aires"],
    "Madrid":         ["madrid"],
    "Paris":          ["paris"],
    "Munich":         ["munich"],
    "Amsterdam":      ["amsterdam"],
    "Helsinki":       ["helsinki"],
    "Tel Aviv":       ["tel aviv"],
    "Istanbul":       ["istanbul"],
    "Moscow":         ["moscow"],
    "Warsaw":         ["warsaw"],
    "Milan":          ["milan"],
    "Cape Town":      ["cape town"],
    "Manila":         ["manila"],
    "Jakarta":        ["jakarta"],
    "Taipei":         ["taipei"],
    "Busan":          ["busan"],
}


class GammaClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        request_timeout_sec: float = 15.0,
        on_unknown_city: Any = None,  # Optional[Callable[[str], None]]
    ) -> None:
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=request_timeout_sec)
        self._on_unknown_city = on_unknown_city
        self._reported_unknown: set[str] = set()

    async def fetch_active_weather(
        self,
        *,
        tradeable_cities: set[str],
        limit: int = 200,
    ) -> list[WeatherMarket]:
        """Pull active weather events and flatten them into per-bucket WeatherMarkets.

        `tradeable_cities` gates which cities produce signals — markets for
        cities outside this set are dropped (after the unknown-city callback
        has had a chance to fire for novel cities not in the registry at all).
        """
        seen_events: dict[str, dict[str, Any]] = {}
        for tag in ("weather", "temperature"):
            params = {
                "tag_slug": tag,
                "active": "true",
                "closed": "false",
                "limit": str(limit),
            }
            for ev in (await self._get(f"{GAMMA_BASE}/events", params=params)) or []:
                eid = str(ev.get("id") or ev.get("slug") or "")
                if eid and eid not in seen_events:
                    seen_events[eid] = ev

        out: list[WeatherMarket] = []
        for ev in seen_events.values():
            out.extend(self._parse_event(ev, tradeable_cities))
        return out

    async def _get(self, url: str, *, params: dict[str, str] | None = None) -> list[dict[str, Any]] | None:
        try:
            async with self._session.get(url, params=params, timeout=self._timeout) as resp:
                if resp.status >= 400:
                    body = (await resp.text())[:200]
                    log.warning("Gamma %s -> HTTP %d: %s", url, resp.status, body)
                    return None
                data = await resp.json()
                if isinstance(data, dict) and "data" in data:
                    return data["data"]
                if isinstance(data, list):
                    return data
                return None
        except (aiohttp.ClientError, json.JSONDecodeError) as e:
            log.warning("Gamma fetch failed: %s", e)
            return None

    def _parse_event(
        self,
        ev: dict[str, Any],
        tradeable_cities: set[str],
    ) -> list[WeatherMarket]:
        title = str(ev.get("title") or "")
        m = _EVENT_TITLE_RX.search(title)
        if not m:
            return []
        metric = m["metric"].lower()
        raw_city = m["city"].strip()
        city = _normalize_city(raw_city)
        if city is None:
            # Possibly a new city we haven't catalogued yet — fire the
            # callback so the alert system can flag it for manual review.
            if raw_city.lower() not in self._reported_unknown:
                self._reported_unknown.add(raw_city.lower())
                if callable(self._on_unknown_city):
                    try:
                        self._on_unknown_city(raw_city)
                    except Exception:
                        log.debug("on_unknown_city callback raised", exc_info=True)
            return []

        if city not in tradeable_cities:
            # Known city but skipped (e.g. unconfirmed source, or disabled
            # in config). Caller logs the global skip list once at startup;
            # logging per-market here would be noise.
            return []

        src = resolution.get(city)
        if src is None:
            return []

        try:
            month = _MONTHS[m["month"].lower()[:3]]
            day = int(m["day"])
            year = int(m["year"]) if m["year"] else datetime.now(timezone.utc).year
            target_date = date(year, month, day)
        except (KeyError, ValueError):
            return []

        end_iso = ev.get("endDate") or ev.get("endDateIso")
        end_dt = _parse_dt(end_iso) if end_iso else None
        if end_dt is None:
            return []

        market_unit = "fahrenheit" if src.unit == "fahrenheit" else "celsius"

        out: list[WeatherMarket] = []
        for raw in ev.get("markets") or []:
            bucket = _parse_bucket(str(raw.get("question") or ""))
            if bucket is None:
                continue
            lo, hi, label, parsed_unit = bucket
            # If the parsed unit doesn't match the city's resolution unit,
            # skip — most likely a market mis-tagged or the parser hit a
            # cross-listed °F variant of a °C city. Safer to drop.
            if parsed_unit != market_unit:
                continue
            outcomes = _maybe_json(raw.get("outcomes"))
            prices = _maybe_json(raw.get("outcomePrices"))
            token_ids = _maybe_json(raw.get("clobTokenIds"))
            yes_price, yes_token = _yes_side(outcomes, prices, token_ids)
            if yes_price is None:
                continue
            liquidity = float(raw.get("liquidityNum") or raw.get("liquidity") or 0.0)
            volume = float(
                raw.get("volumeNum")
                or raw.get("volume")
                or raw.get("volumeClob")
                or 0.0
            )
            b = TemperatureBucket(
                low=lo, high=hi, unit=market_unit, label=label,
                token_id=yes_token, price=yes_price,
            )
            out.append(WeatherMarket(
                market_id=str(raw.get("id") or raw.get("conditionId") or raw.get("slug")),
                question=str(raw.get("question") or ""),
                city=city,
                station_id=src.station_id,
                target_date=target_date,
                end_date_utc=end_dt,
                unit=market_unit,
                buckets=[b],
                liquidity_usd=liquidity,
                volume_usd=volume,
                raw={"event_title": title, "metric": metric, "market": raw},
            ))
        return out


def _parse_bucket(text: str) -> tuple[float, float, str, str] | None:
    """Return (low, high, label, unit). Open-ended buckets use +/-inf.

    For an integer-wide bucket like '50° or below' or '50-51°', we treat
    the high bound as exclusive and add 1 to the high (so '50-51°F' covers
    [50, 52) — the resolved 'integer reading' of 51 will fall inside).
    """
    m = _BETWEEN_DASH_RX.search(text) or _BETWEEN_AND_RX.search(text)
    if m:
        lo, hi = float(m["lo"]), float(m["hi"])
        if hi < lo:
            lo, hi = hi, lo
        unit = "fahrenheit" if m["unit"].upper() == "F" else "celsius"
        suffix = "°F" if unit == "fahrenheit" else "°C"
        return (lo, hi + 1.0, f"{int(lo)}-{int(hi)}{suffix}", unit)
    m = _OR_BELOW_RX.search(text)
    if m:
        hi = float(m["hi"])
        unit = "fahrenheit" if m["unit"].upper() == "F" else "celsius"
        suffix = "°F" if unit == "fahrenheit" else "°C"
        return (-math.inf, hi + 1.0, f"≤{int(hi)}{suffix}", unit)
    m = _OR_HIGHER_RX.search(text)
    if m:
        lo = float(m["lo"])
        unit = "fahrenheit" if m["unit"].upper() == "F" else "celsius"
        suffix = "°F" if unit == "fahrenheit" else "°C"
        return (lo, math.inf, f"≥{int(lo)}{suffix}", unit)
    return None


def _normalize_city(raw_city: str) -> str | None:
    lo = raw_city.strip().lower()
    for city, aliases in _CITY_ALIASES.items():
        if lo == city.lower() or lo in aliases:
            return city
        for alias in aliases:
            if alias in lo:
                return city
    return None


def _parse_dt(s: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _maybe_json(v: Any) -> list[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _yes_side(
    outcomes: list[Any],
    prices: list[Any],
    token_ids: list[Any],
) -> tuple[float | None, str | None]:
    if not outcomes or not prices:
        return (None, None)
    for i, name in enumerate(outcomes):
        if str(name).strip().lower() == "yes":
            try:
                price = float(prices[i])
            except (ValueError, TypeError, IndexError):
                return (None, None)
            tid = str(token_ids[i]) if i < len(token_ids) else None
            return (price, tid)
    return (None, None)
