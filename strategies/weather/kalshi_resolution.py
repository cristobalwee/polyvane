"""Kalshi-specific weather resolution sources.

Kalshi temperature markets settle against the official NWS climate report
station for each series. Keep this separate from the Polymarket registry:
Polymarket and Kalshi can use different stations for the same displayed city.
"""
from __future__ import annotations

from .resolution import ResolutionSource


_REGISTRY: dict[str, ResolutionSource] = {
    "Atlanta": ResolutionSource(
        city="Atlanta",
        station_name="Hartsfield-Jackson Atlanta International",
        station_id="KATL",
        data_provider="noaa",
        lat=33.6407,
        lon=-84.4277,
        unit="fahrenheit",
        notes="Kalshi KXHIGH/KXLOW Atlanta NWS climate report source",
    ),
    "Chicago": ResolutionSource(
        city="Chicago",
        station_name="Chicago O'Hare International",
        station_id="KORD",
        data_provider="noaa",
        lat=41.9742,
        lon=-87.9073,
        unit="fahrenheit",
        notes="Kalshi KXHIGH/KXLOW Chicago NWS climate report source",
    ),
    "Dallas": ResolutionSource(
        city="Dallas",
        station_name="Dallas/Fort Worth International",
        station_id="KDFW",
        data_provider="noaa",
        lat=32.8998,
        lon=-97.0403,
        unit="fahrenheit",
        notes="Kalshi KXHIGH/KXLOW Dallas NWS climate report source",
    ),
    "Houston": ResolutionSource(
        city="Houston",
        station_name="George Bush Intercontinental",
        station_id="KIAH",
        data_provider="noaa",
        lat=29.9902,
        lon=-95.3368,
        unit="fahrenheit",
        notes="Kalshi KXHIGH/KXLOW Houston NWS climate report source",
    ),
    "Miami": ResolutionSource(
        city="Miami",
        station_name="Miami International",
        station_id="KMIA",
        data_provider="noaa",
        lat=25.7959,
        lon=-80.2870,
        unit="fahrenheit",
        notes="Kalshi KXHIGH/KXLOW Miami NWS climate report source",
    ),
    "NYC": ResolutionSource(
        city="NYC",
        station_name="Central Park",
        station_id="KNYC",
        data_provider="noaa",
        lat=40.7794,
        lon=-73.9692,
        unit="fahrenheit",
        notes="Kalshi KXHIGHNY/KXLOWNY NWS climate report source",
    ),
}


def get(city: str) -> ResolutionSource | None:
    return _REGISTRY.get(city)


def all_cities() -> list[str]:
    return list(_REGISTRY.keys())
