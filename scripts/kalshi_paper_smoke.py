"""One-shot Kalshi paper scan + execution smoke test.

This verifies the full local path:
  Kalshi market scan -> strategy evaluation -> risk gate -> paper journal entry.

It always forces paper execution, regardless of config or environment, and
defaults to a throwaway DB under data/ so live orders cannot be submitted.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.client import ClientConfig, ClobClient
from core.executor import ExecutionConfig, Executor
from core.kalshi_client import KalshiClient, KalshiClientConfig
from core.logger import TradeJournal
from core.market_cache import MarketCache
from core.risk import RiskConfig, RiskManager
from main import load_strategies
from strategies.base import Signal, StrategyContext, TradeIntent
from strategies.weather.kalshi_markets import KalshiMarketScanner


def _instance_name(entry: dict[str, Any]) -> str:
    name = str(entry.get("name") or "")
    exchange = str((entry.get("params") or {}).get("exchange") or "polymarket").lower()
    return f"{name}_{exchange}" if exchange != "polymarket" else name


async def main() -> int:
    parser = argparse.ArgumentParser(description="Run a one-shot Kalshi paper smoke test.")
    parser.add_argument("--config", default=str(ROOT / "config" / "config.yaml"))
    parser.add_argument("--db", default=str(ROOT / "data" / "kalshi_paper_smoke.db"))
    parser.add_argument("--max-trades", type=int, default=10)
    parser.add_argument("--scan-timeout-sec", type=float, default=30.0)
    parser.add_argument(
        "--no-executor-probe",
        action="store_true",
        help="Do not submit a paper-only executor probe when live strategy signals are absent.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("kalshi_paper_smoke")

    load_dotenv(ROOT / ".env")
    cfg_path = Path(args.config)
    cfg = yaml.safe_load(cfg_path.read_text())
    if not isinstance(cfg, dict):
        raise ValueError(f"config must be a YAML mapping: {cfg_path}")

    # Force a Kalshi paper run even if the checked-in or local config is live.
    cfg["execution"] = dict(cfg["execution"])
    cfg["execution"]["mode"] = "paper"
    cfg["execution"]["kalshi_mode"] = "paper"
    cfg["execution"]["live_strategies"] = []
    cfg["risk"] = dict(cfg["risk"])
    dedup_overrides = dict(cfg["risk"].get("dedup_open_positions_per_strategy") or {})
    dedup_overrides["kalshi_paper_probe"] = False
    cfg["risk"]["dedup_open_positions_per_strategy"] = dedup_overrides

    cfg["strategies"] = [
        s for s in cfg.get("strategies", [])
        if s.get("enabled") and str((s.get("params") or {}).get("exchange") or "").lower() == "kalshi"
    ]
    if not cfg["strategies"]:
        log.error("No enabled Kalshi strategy entries found in %s", cfg_path)
        return 2

    clob = ClobClient(ClientConfig.from_dict(cfg["api"]))
    clob.initialize_unauthenticated()

    kalshi_raw = cfg["api"].get("kalshi") or {}
    kalshi_cfg = KalshiClientConfig.from_dict({
        **kalshi_raw,
        "base_url": kalshi_raw.get("demo_base_url", "https://api.elections.kalshi.com/trade-api/v2"),
        "key_id": os.getenv("KALSHI_KEY_ID", ""),
        "private_key_path": os.getenv("KALSHI_PRIVATE_KEY_PATH", ""),
    })
    kalshi = KalshiClient(kalshi_cfg)
    key_path_exists = bool(kalshi_cfg.private_key_path and Path(kalshi_cfg.private_key_path).exists())
    has_kalshi_creds = bool(kalshi_cfg.key_id and key_path_exists)
    if has_kalshi_creds:
        kalshi.initialize_authenticated()
        log.info("Kalshi client authenticated for market-data reads.")
    else:
        kalshi.initialize_unauthenticated()
        if kalshi_cfg.key_id and kalshi_cfg.private_key_path and not key_path_exists:
            log.warning(
                "Kalshi key path does not exist locally (%s); market scan may return no data.",
                kalshi_cfg.private_key_path,
            )
        else:
            log.warning("No Kalshi credentials found; market scan may return no data.")

    clients = {"polymarket": clob, "kalshi": kalshi}
    journal = TradeJournal(Path(args.db))
    exec_cfg = ExecutionConfig.from_dict(cfg["execution"])
    risk = RiskManager(RiskConfig.from_dict(cfg["risk"]), journal, is_paper=True)
    executor = Executor(exec_cfg, risk, journal, clob, clients=clients)
    context = StrategyContext(
        client=clob,
        config=cfg,
        market_cache=MarketCache(),
        journal=journal,
        clients=clients,
    )

    strategies = load_strategies(cfg, context, log)
    submitted = 0
    scanned = 0
    try:
        for strategy in strategies:
            instance_name = getattr(strategy, "_instance_name", strategy.name)
            await strategy.setup()
            try:
                try:
                    signals = await asyncio.wait_for(
                        strategy.scan(),
                        timeout=max(1.0, args.scan_timeout_sec),
                    )
                except asyncio.TimeoutError:
                    log.error(
                        "%s scan timed out after %.1fs",
                        instance_name,
                        args.scan_timeout_sec,
                    )
                    continue
                scanned += len(signals)
                log.info("%s emitted %d signal(s)", instance_name, len(signals))
                for sig in signals:
                    if submitted >= args.max_trades:
                        break
                    sig.metadata.setdefault("strategy", instance_name)
                    intent = await strategy.evaluate(sig)
                    if intent is None:
                        continue
                    bankroll = float((cfg.get("paper_bankroll_per_strategy") or {}).get(instance_name, cfg.get("paper_bankroll_usd", 1000.0)))
                    result = await executor.submit(intent, bankroll_usd=bankroll)
                    if result.accepted:
                        submitted += 1
                if submitted >= args.max_trades:
                    break
            finally:
                await strategy.teardown()
        if submitted == 0 and not args.no_executor_probe:
            probe = await _submit_executor_probe(cfg, executor, kalshi, log)
            submitted += int(probe)
    finally:
        await kalshi.close()

    log.info(
        "Kalshi paper smoke complete: strategies=%s signals=%d paper_entries=%d db=%s",
        ", ".join(_instance_name(s) for s in cfg["strategies"]),
        scanned,
        submitted,
        args.db,
    )
    return 0 if submitted > 0 else 1


async def _submit_executor_probe(
    cfg: dict[str, Any],
    executor: Executor,
    kalshi: KalshiClient,
    log: logging.Logger,
) -> bool:
    """Submit one paper-only trade on a real Kalshi market to verify execution plumbing."""
    weather_entry = next(
        (
            s for s in cfg.get("strategies", [])
            if s.get("name") == "weather"
            and str((s.get("params") or {}).get("exchange") or "").lower() == "kalshi"
        ),
        None,
    )
    cities = set((weather_entry or {}).get("params", {}).get("cities", [])) or None
    scanner = KalshiMarketScanner(kalshi)
    markets = await scanner.fetch_active_weather(tradeable_cities=cities)
    market = next((m for m in markets if 0.01 <= m.yes_price <= 0.99), None)
    if market is None:
        log.warning("executor probe skipped: no suitable Kalshi market found")
        return False

    signal = Signal(
        market_id=market.ticker,
        direction="YES",
        edge=max(0.10, float(cfg["risk"].get("min_edge_pct", 0.10))),
        confidence=max(0.0, min(1.0, market.yes_price)),
        market_question=market.title,
        price=market.yes_price,
        category="weather",
        token_id=market.ticker,
        exchange="kalshi",
        metadata={
            "strategy": "kalshi_paper_probe",
            "city": market.city,
            "metric": market.metric,
            "threshold_f": market.threshold_f,
            "end_utc": market.end_date_utc.isoformat(),
            "volume_usd": market.volume_usd,
            "paper_probe": True,
            "neg_risk": False,
        },
    )
    result = await executor.submit(
        TradeIntent(signal=signal, reason="kalshi paper smoke executor probe"),
        bankroll_usd=float(cfg.get("paper_bankroll_usd", 1000.0)),
    )
    if result.accepted:
        log.info(
            "executor probe paper entry accepted: trade_id=%s market=%s",
            result.trade_id,
            market.ticker,
        )
        return True
    log.warning("executor probe rejected: %s", result.reason)
    return False


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
