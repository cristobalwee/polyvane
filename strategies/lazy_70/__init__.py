"""lazy_70 — buy temperature buckets at YES >= $0.70, hold to resolution."""
from strategies.lazy_weather.strategy import LazyWeatherStrategy


class Lazy70Strategy(LazyWeatherStrategy):
    name = "lazy_70"
