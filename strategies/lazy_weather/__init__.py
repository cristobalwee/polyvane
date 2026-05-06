"""Lazy weather strategy — piggyback on market consensus, no forecasting.

The thesis: if a temperature bucket's YES price has crossed a threshold (the
crowd is convinced) and resolution is within the next 12-48h, that crowd
opinion is more reliable than a 50-day-old forecast. Buy. Hold. Wait.

This module is the "base" implementation. Each `lazy_<threshold>` directory
exists only to give a different `name` (so trades segregate in the journal
and the config can independently size each variant's paper bankroll).
"""

from .strategy import LazyWeatherStrategy

__all__ = ["LazyWeatherStrategy"]
