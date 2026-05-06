"""lazy_60 — buy temperature buckets at YES >= $0.60, hold to resolution."""
from strategies.lazy_weather.strategy import LazyWeatherStrategy


class Lazy60Strategy(LazyWeatherStrategy):
    name = "lazy_60"
