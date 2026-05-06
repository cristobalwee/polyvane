"""Dataclasses shared across the weather strategy modules."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass
class Forecast:
    """A point-forecast snapshot for one weather station, valid for a window.

    `temperature` and `std_dev` are in `unit` ('fahrenheit' | 'celsius') —
    matched to the resolution source's reporting unit so they line up with
    the market's bucket bounds without conversion.
    """
    station_id: str
    valid_from: datetime  # UTC
    valid_to: datetime    # UTC
    temperature: float
    std_dev: float
    unit: str             # 'fahrenheit' | 'celsius'
    source: str           # 'noaa' | 'open-meteo'
    fetched_at: datetime  # UTC


@dataclass
class TemperatureBucket:
    """One outcome bucket inside a temperature market.

    `low` and `high` define a half-open range [low, high) in the bucket's
    native unit. Edge buckets ('< 30' / '> 100') use +/-inf for one bound.
    """
    low: float
    high: float
    unit: str              # 'fahrenheit' | 'celsius'
    label: str
    token_id: str | None
    price: float           # YES price in [0, 1]


@dataclass
class WeatherMarket:
    """A Polymarket weather market, parsed into a normalized form."""
    market_id: str
    question: str
    city: str
    station_id: str
    target_date: date
    end_date_utc: datetime
    unit: str = "fahrenheit"
    buckets: list[TemperatureBucket] = field(default_factory=list)
    liquidity_usd: float = 0.0
    volume_usd: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class WeatherSignal:
    """Derived signal: one market + one bucket + the model's view on it."""
    market: WeatherMarket
    bucket: TemperatureBucket
    forecast: Forecast
    model_prob: float
    edge: float           # model_prob - bucket.price
