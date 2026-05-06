"""Strategy-agnostic backtesting framework.

Replays historical market + signal data through a strategy's scoring logic,
simulates paper execution, and resolves positions against ground truth.

Strategies plug in via `StrategyAdapter` (see `runner.py`). Each adapter
knows how to:
  - enumerate the historical "markets" relevant to a given UTC date
  - score each market into Signals using historical-as-of-date inputs
  - identify the resolved outcome for a closed market

The runner itself is strategy-blind — it walks dates, asks the adapter for
signals, simulates fills at the historical mid, and records P&L. Future
strategies (whale_tracker, arb) implement their own adapter and reuse the
rest.

Public symbols are imported from submodules at use site rather than
re-exported here, so that `python -m backtesting.runner` doesn't trigger
a double-import of the runner module.
"""
