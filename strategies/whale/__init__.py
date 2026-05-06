"""Whale-tracking copy-trade strategy (scaffold).

Watches a curated list of high-quality Polymarket wallets and emits Signals
when a quorum of them takes the same side of the same market with size
above a configured floor. Wallets are scored by historical win rate, ROI,
diversification, and arb-bot likelihood; low-quality / arb-bot wallets are
excluded from the consensus check.

Not yet implemented; see `strategy.py` for TODOs.
"""
from .strategy import WhaleStrategy

__all__ = ["WhaleStrategy"]
