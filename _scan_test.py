"""One-shot paper-mode scan of WeatherStrategy. Prints a formatted summary."""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from strategies.base import StrategyContext
from strategies.weather import WeatherStrategy


async def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("scan_test")

    cfg = yaml.safe_load((ROOT / "config" / "config.yaml").read_text())
    weather_entry = next(s for s in cfg["strategies"] if s["name"] == "weather")
    params = weather_entry.get("params") or {}

    ctx = StrategyContext(client=None, config=cfg)
    strat = WeatherStrategy(params=params, context=ctx)

    log.info("Starting one-shot weather scan (paper mode, no orders submitted)")
    await strat.setup()
    try:
        signals = await strat.scan()
    finally:
        await strat.teardown()

    print()
    print("=" * 72)
    print(" WEATHER STRATEGY — paper-mode scan summary")
    print("=" * 72)
    print(f" Cities configured : {params.get('cities')}")
    print(f" min_edge_pct      : {params.get('min_edge_pct')}")
    print(f" min_hours_to_res  : {params.get('min_hours_to_resolution')}")
    print(f" std_dev_f         : {params.get('bucket_probability_std_dev_f')}")
    print(f" std_dev_c         : {params.get('bucket_probability_std_dev_c')}")
    print(f" Signals emitted   : {len(signals)}")
    if not signals:
        print(" (no actionable signals — either no markets discovered, no forecasts,")
        print("  or no bucket cleared the edge gate)")
    else:
        print()
        for s in signals:
            md = s.metadata
            unit_short = "°F" if md.get("forecast_unit") == "fahrenheit" else "°C"
            print(f"  • {md.get('city')} {md.get('metric')} {md.get('bucket')} on {md.get('target_date')}")
            print(f"      forecast={md.get('forecast_temp'):.1f}{unit_short}  p={md.get('model_prob'):.2f}"
                  f"  market={s.price:.2f}  edge={s.edge:+.2f}  liq=${md.get('liquidity_usd'):.0f}"
                  f"  vol=${md.get('volume_usd', 0):.0f}")
            print(f"      market_id={s.market_id}  token={s.token_id}")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
