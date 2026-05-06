"""Resolution source registry for the weather strategy.

Each Polymarket temperature market resolves against a specific weather
station reported by a specific data provider. This registry is the single
source of truth for that mapping. The strategy refuses to trade any market
whose city is missing here, or whose entry is marked unconfirmed when
`require_confirmed_resolution` is true.

The registry is intentionally Python (not YAML) so it lives next to the
parsing code that depends on it and is easy to extend with new providers.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable


log = logging.getLogger("strategy.weather.resolution")


# Data providers we know how to fetch forecasts from. 'wunderground' is the
# resolution source for most Polymarket temperature markets but we don't have
# a stable forecast endpoint for it — we use Open-Meteo as the *forecast*
# proxy and rely on Wunderground only for the post-resolution actual.
DataProvider = str  # 'wunderground' | 'noaa' | 'jma' | 'kma' | 'hko' | 'metservice'
Unit = str          # 'celsius' | 'fahrenheit'


@dataclass(frozen=True)
class ResolutionSource:
    city: str
    station_name: str
    station_id: str
    data_provider: DataProvider
    lat: float
    lon: float
    unit: Unit
    # `confirmed=False` means we have not personally verified the station
    # against a recent resolution. The strategy will skip such markets when
    # `require_confirmed_resolution` is enabled.
    confirmed: bool = True
    notes: str = ""

    @property
    def is_us(self) -> bool:
        # NOAA only covers US stations; everything else uses Open-Meteo.
        return self.data_provider == "noaa" or self.station_id.startswith(("K",))


# Cities we have NOT yet personally confirmed a resolution station for. Listed
# here (vs omitted) so the alert + skip-with-warning machinery can flag them.
_UNCONFIRMED = {
    "Seoul":   "Source TBD — likely KMA but station unverified",
    "Shanghai": "Source TBD — likely CMA Pudong/Hongqiao but station unverified",
}


_REGISTRY: dict[str, ResolutionSource] = {
    "NYC": ResolutionSource(
        city="NYC",
        station_name="LaGuardia Airport",
        station_id="KLGA",
        data_provider="wunderground",
        lat=40.7729, lon=-73.8740,
        unit="fahrenheit",
        notes="2°F buckets",
    ),
    "London": ResolutionSource(
        city="London",
        station_name="London City Airport",
        station_id="EGLC",
        data_provider="wunderground",
        lat=51.5053, lon=0.0553,
        unit="celsius",
        notes="1°C buckets",
    ),
    "Hong Kong": ResolutionSource(
        city="Hong Kong",
        station_name="Hong Kong Observatory",
        station_id="HKO",
        data_provider="wunderground",
        lat=22.3022, lon=114.1742,
        unit="celsius",
    ),
    "Dallas": ResolutionSource(
        city="Dallas",
        station_name="Dallas Love Field",
        station_id="KDAL",
        data_provider="wunderground",
        lat=32.8471, lon=-96.8518,
        unit="fahrenheit",
    ),
    "Atlanta": ResolutionSource(
        city="Atlanta",
        station_name="Hartsfield-Jackson Atlanta International",
        station_id="KATL",
        data_provider="wunderground",
        lat=33.6407, lon=-84.4277,
        unit="fahrenheit",
    ),
    "Toronto": ResolutionSource(
        city="Toronto",
        station_name="Toronto Pearson International",
        station_id="CYYZ",
        data_provider="wunderground",
        lat=43.6777, lon=-79.6248,
        unit="celsius",
    ),
    "Ankara": ResolutionSource(
        city="Ankara",
        station_name="Esenboga Airport",
        station_id="LTAC",
        data_provider="wunderground",
        lat=40.1281, lon=32.9951,
        unit="celsius",
    ),
    "Wellington": ResolutionSource(
        city="Wellington",
        station_name="Wellington Airport",
        station_id="NZWN",
        data_provider="wunderground",
        lat=-41.3272, lon=174.8053,
        unit="celsius",
    ),
    "Seoul": ResolutionSource(
        city="Seoul",
        station_name="(unconfirmed)",
        station_id="",
        data_provider="wunderground",
        lat=37.5665, lon=126.9780,
        unit="celsius",
        confirmed=False,
        notes=_UNCONFIRMED["Seoul"],
    ),
    "Shanghai": ResolutionSource(
        city="Shanghai",
        station_name="(unconfirmed)",
        station_id="",
        data_provider="wunderground",
        lat=31.2304, lon=121.4737,
        unit="celsius",
        confirmed=False,
        notes=_UNCONFIRMED["Shanghai"],
    ),
}


def get(city: str) -> ResolutionSource | None:
    return _REGISTRY.get(city)


def all_cities() -> list[str]:
    return list(_REGISTRY.keys())


def confirmed_cities() -> list[str]:
    return [c for c, r in _REGISTRY.items() if r.confirmed]


def unconfirmed_cities() -> list[str]:
    return [c for c, r in _REGISTRY.items() if not r.confirmed]


def is_known(city: str) -> bool:
    return city in _REGISTRY


def filter_tradeable(
    cities: Iterable[str],
    *,
    require_confirmed: bool,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Split a list of requested cities into (tradeable, skipped_with_reason).

    The strategy uses this at startup and on each scan to decide which
    markets to score. `skipped_with_reason` is what gets logged.
    """
    tradeable: list[str] = []
    skipped: list[tuple[str, str]] = []
    for c in cities:
        src = _REGISTRY.get(c)
        if src is None:
            skipped.append((c, "not in resolution registry"))
            continue
        if require_confirmed and not src.confirmed:
            skipped.append((c, f"unconfirmed resolution source: {src.notes or 'see registry'}"))
            continue
        tradeable.append(c)
    return tradeable, skipped
