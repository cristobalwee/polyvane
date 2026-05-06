"""lazy_50 — buy temperature buckets at YES >= $0.50, hold to resolution."""
from strategies.lazy_weather.strategy import LazyWeatherStrategy


class Lazy50Strategy(LazyWeatherStrategy):
    name = "lazy_50"
