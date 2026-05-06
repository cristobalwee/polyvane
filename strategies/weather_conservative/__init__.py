"""weather_conservative — tighter edge floors, higher agreement bar,
primary buckets only (no adjacent-bucket trades).

Trades a smaller, higher-conviction set of signals. The hypothesis: trade
quality matters more than trade count once you're past the obvious mispricings.
"""
from strategies.weather.strategy import WeatherStrategy


class WeatherConservativeStrategy(WeatherStrategy):
    name = "weather_conservative"
