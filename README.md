# polyvane

Polymarket trading bot. Multi-strategy, paper-mode by default, with a
multi-model ensemble forecaster for temperature markets and a strategy-
agnostic backtesting + calibration framework.

> **Not financial advice.** Capital at risk. Edges compress as competition
> grows; no model in this repo is guaranteed to be profitable in live
> conditions. Run paper mode for at least a full week before you consider
> live trading, and only deploy capital you can afford to lose.

---

## Architecture

```
                   ┌──────────────────────────┐
                   │       main.py            │
                   │  (config + event loop)   │
                   └───┬───────────────┬──────┘
                       │               │
         ┌─────────────▼───┐    ┌──────▼──────────┐
         │ StrategyContext │    │ RiskManager /   │
         │  client         │    │ Executor /      │
         │  config         │    │ Wallet / Journal│
         │  market_cache ──┼───►│                 │
         └─────────────┬───┘    └─────────────────┘
                       │
        ┌──────────────┼──────────────────────────┐
        │              │                          │
  ┌─────▼────┐  ┌──────▼──────┐         ┌─────────▼────────┐
  │ weather/ │  │ arbitrage/  │         │     whale/       │
  │ (live)   │  │ (scaffold)  │         │   (scaffold)     │
  └─────┬────┘  └─────────────┘         └──────────────────┘
        │
        ├── ensemble forecaster (NOAA + Open-Meteo GFS/ECMWF/ICON)
        ├── adjacent-bucket signals (Gaussian mixture probabilities)
        ├── resolution registry (per-city station + data provider)
        └── publishes prices into market_cache for other strategies

  ┌──────────────────────────────────────────────────────────┐
  │  backtesting/  (strategy-agnostic replay + reporting)    │
  │     data_loader · runner · report · adapters/            │
  │                                                           │
  │  strategies/weather/calibrate.py                          │
  │     (per-model weight + per-city std-dev recommendations) │
  └──────────────────────────────────────────────────────────┘
```

Top-level layout:

| Path | What's there |
|---|---|
| `main.py` | Loads config, builds the strategy context, runs the event loop. |
| `core/` | Shared infra: `client`, `executor`, `risk`, `logger`, `wallet`, `market_cache`. |
| `strategies/base.py` | `BaseStrategy` + `Signal` + `TradeIntent` + `StrategyContext`. |
| `strategies/weather/` | Temperature-bucket strategy (live). |
| `strategies/arbitrage/` | YES+NO mispricing scanner (scaffold; not yet implemented). |
| `strategies/whale/` | Tracked-wallet copy-trade (scaffold; not yet implemented). |
| `backtesting/` | Replay engine, reports, per-strategy adapters. |
| `monitoring/` | Alert bus, dashboard, health checks, reviewer. |
| `config/config.yaml` | Single source of truth for every tunable parameter. |
| `data/trade_journal.db` | SQLite trade journal. |

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` (or create one) and fill the relevant
secrets — only required for **live** mode:

```env
POLYGON_RPC_URL=https://polygon-rpc.com
POLYGON_PRIVATE_KEY=0x...        # signer for on-chain settlement
CLOB_API_KEY=...
CLOB_API_SECRET=...
CLOB_API_PASSPHRASE=...
```

Fund the wallet with USDC.e on Polygon (the bot reads on-chain balance
as the live bankroll). Paper mode uses `paper_bankroll_usd` from
`config.yaml` and never touches the chain.

---

## Running

| Mode | Command | What happens |
|---|---|---|
| Paper (default) | `make run` | Strategies run, signals are logged, no orders are submitted. |
| Live | `LIVE=true make run-live` | Real orders. The `LIVE=true` env var is a deliberate safety check. |
| Smoke test | `python main.py --smoke-test` | Loads config, instantiates strategies, exits. |

Switching between modes also requires `execution.mode` in `config.yaml`
to be set to `paper` or `live` — the env var alone won't do it.

---

## Cities & resolution sources

Each Polymarket temperature market resolves against a specific weather
station via a specific data provider (NOAA / Wunderground / KMA / etc.).
Those mappings live in [strategies/weather/resolution.py](strategies/weather/resolution.py)
as a Python registry — single source of truth.

To add or update a city:

1. Identify Polymarket's reported resolution station (read the rules tab
   on a recent event for that city).
2. Add a `ResolutionSource(...)` entry to `_REGISTRY` with `lat`, `lon`,
   `unit`, and `confirmed=False` until you've personally verified one
   resolution.
3. Add the city's normalized name to the `cities:` allowlist in
   `config/config.yaml`.
4. Run `make verify-sources` (when implemented) to confirm Polymarket's
   current event titles match what the registry expects.

The existing tradeable set is: NYC, Dallas, Atlanta, London, Hong Kong,
Toronto, Ankara, Wellington, Seoul (unconfirmed), Shanghai (unconfirmed).
With `require_confirmed_resolution: true`, unconfirmed cities are skipped
with a startup warning.

---

## Adding a strategy

1. Create `strategies/<name>/__init__.py` exporting a single
   `BaseStrategy` subclass (the loader auto-discovers it).
2. Implement `scan()` (returns `list[Signal]`) and `evaluate(signal)`
   (returns `TradeIntent | None`). `setup()` and `teardown()` are
   optional.
3. Add a config block to `config/config.yaml` under `strategies:`:
   ```yaml
   - name: <name>
     enabled: false
     params:
       # whatever your strategy reads from `params`
   ```
4. (Optional, for backtestable strategies) Implement an adapter at
   `backtesting/adapters/<name>.py` that satisfies the `StrategyAdapter`
   protocol in `backtesting/runner.py`. The adapter is the only piece
   the runner imports for that strategy.

`main.py` requires no edits — the loader discovers any `strategies.<name>`
package whose name appears in config.

---

## Backtesting

```bash
make backtest
# or, with full control:
python -m backtesting.runner --start 2026-01-01 --end 2026-03-31 --strategy weather
```

What-if mode: replay with adjusted thresholds without touching config:

```bash
python -m backtesting.runner --start 2026-01-01 --end 2026-03-31 --strategy weather \
    --override min_edge_pct=0.20 --override adjacent_min_edge_pct=0.05
```

The runner walks each market at multiple as-of timestamps (default 48,
36, 24, and 12 hours before resolution), pulls the per-model historical
forecast from Open-Meteo, simulates a paper fill at the historical mid
from CLOB `prices-history`, and settles against the actual from
Open-Meteo Archive.

The report breaks down P&L by city, by volume tier, and by primary vs
adjacent bucket so you can see which slice is carrying the strategy.
A `--cache-dir` flag (default `.backtest-cache`) makes reruns cheap.

---

## Calibration

```bash
make calibrate
# or:
python -m strategies.weather.calibrate --days 30 --min-samples 5
```

Outputs three things:

1. A drop-in `forecast_model_weights:` block based on inverse-MSE
   weighting per source over the calibration window.
2. A `per_source_std_dev_f:` block based on the empirical RMSE of each
   source (the default 2°F std may be too wide or too narrow per source).
3. A per-city ensemble-RMSE table that flags `ANOMALY` for cities where
   the registered station may be wrong (forecast vs actual diverges
   well past the global mean).

The script never modifies `config.yaml`. Apply the recommendations by
hand after reviewing.

---

## Risk & disclaimers

- This bot trades real money in live mode. Test in paper mode first.
- Polymarket temperature markets are increasingly bot-saturated; the
  ensemble + adjacent-bucket logic is a durability play, not a guarantee.
- The arbitrage and whale strategies are scaffolds only — enabling them
  in config currently produces no orders. They print "strategy not yet
  implemented" once at startup.
- Polymarket and the underlying chain (Polygon) can have outages, oracle
  disputes, and unilateral resolution changes. The bot's risk module
  enforces a daily loss limit and circuit-breaker, but that's not a
  substitute for monitoring.
- This repository is not financial advice. The author is not your
  fiduciary. You are responsible for understanding the code you run.
