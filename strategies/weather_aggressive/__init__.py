"""weather_aggressive — looser edge floors and agreement gate.

Trades signals the baseline weather strategy would skip. Useful for testing
whether the "more signals, lower quality" tradeoff pays off in practice.
"""
from strategies.weather.strategy import WeatherStrategy


class WeatherAggressiveStrategy(WeatherStrategy):
    name = "weather_aggressive"
