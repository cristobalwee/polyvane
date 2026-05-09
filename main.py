"""Entry point.

Loads config, initializes core modules, discovers enabled strategies, and runs
an asyncio event loop where each strategy polls on its own cadence and a
background task re-evaluates the risk circuit breaker.

Run:
    python main.py                    # uses config/config.yaml
    python main.py path/to/cfg.yaml   # custom config path

Environment overrides:
    TRADING_MODE       paper | live   (overrides config execution.mode)
    PK                 0x... wallet key (live only)
    CLOB_API_KEY/CLOB_SECRET/CLOB_PASS_PHRASE   V2 API creds (live only)
    POLYGON_RPC_URL    Polygon RPC (live only)
    LOGS_DIR           override logs/ destination
    HEARTBEAT_FILE     override data/heartbeat path
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from core.client import ClientConfig, ClobClient
from core.executor import ExecutionConfig, Executor
from core.logger import TradeJournal
from core.logging_config import setup_logging
from core.market_cache import MarketCache
from core.risk import RiskConfig, RiskManager
from core.wallet import Wallet, WalletConfig
from monitoring.alerts import AlertBus, AlertConfig, is_summary_due, utc_now
from monitoring.health import HealthConfig, HealthMonitor
from monitoring.reviewer import Reviewer, ReviewerConfig
from strategies.base import BaseStrategy, StrategyContext, TradeIntent


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"
DEFAULT_HEARTBEAT = PROJECT_ROOT / "data" / "heartbeat"
LIVE_TRADING_SAFETY_FILE = PROJECT_ROOT / ".live-trading-enabled"

VERSION = "1.0"


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    with path.open("r") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError(f"config must be a YAML mapping; got {type(cfg).__name__}")
    for required in ("risk", "execution", "polling", "logger", "api", "strategies"):
        if required not in cfg:
            raise ValueError(f"config missing required section: {required!r}")
    return cfg


def load_strategies(
    cfg: dict[str, Any],
    context: StrategyContext,
    log: logging.Logger,
) -> list[BaseStrategy]:
    strategies: list[BaseStrategy] = []
    for entry in cfg.get("strategies", []):
        if not entry.get("enabled", False):
            continue
        name = entry["name"]
        module_path = f"strategies.{name}"
        try:
            mod = importlib.import_module(module_path)
        except ModuleNotFoundError:
            log.warning("Strategy %r enabled in config but module %s not found — skipping", name, module_path)
            continue
        # Find the BaseStrategy subclass defined IN this module (not just
        # imported into it — e.g. `lazy` re-exports a thin subclass of
        # `LazyWeatherStrategy`). Prefer a class whose `.name` matches the
        # config entry's name; fall back to any class defined here.
        candidates: list[type] = []
        for attr in vars(mod).values():
            if (
                isinstance(attr, type)
                and issubclass(attr, BaseStrategy)
                and attr is not BaseStrategy
                and getattr(attr, "__module__", "").startswith(module_path)
            ):
                candidates.append(attr)
        cls = next((c for c in candidates if getattr(c, "name", "") == name), None) \
            or (candidates[0] if candidates else None)
        if cls is None:
            log.warning("Strategy module %s has no BaseStrategy subclass — skipping", module_path)
            continue
        instance = cls(params=entry.get("params") or {}, context=context)
        strategies.append(instance)
        log.info("Loaded strategy: %s", name)
    return strategies


def _print_startup_banner(
    log: logging.Logger,
    *,
    mode: str,
    cities: list[str],
    strategies: list[str],
    config_path: Path,
) -> None:
    cities_str = ", ".join(cities) if cities else "(none)"
    strategies_str = ", ".join(strategies) if strategies else "(none enabled)"
    banner = [
        "========================================",
        f"PolyVane v{VERSION}",
        f"Mode: {mode.upper()}",
        "Exchange: Polymarket CLOB V2",
        f"Strategies: {strategies_str}",
        f"Cities: {cities_str}",
        f"Config: {config_path}",
        "========================================",
    ]
    for line in banner:
        log.info(line)


def _resolve_trading_mode(cfg: dict[str, Any]) -> str:
    """Env TRADING_MODE wins over config.execution.mode. Default 'paper'."""
    env = (os.environ.get("TRADING_MODE") or "").strip().lower()
    if env in ("paper", "live"):
        return env
    cfg_mode = str(cfg["execution"].get("mode", "paper")).lower()
    return cfg_mode if cfg_mode in ("paper", "live") else "paper"


async def heartbeat_loop(
    heartbeat_file: Path,
    interval_sec: float,
    stop_event: asyncio.Event,
) -> None:
    """Periodically `touch` the heartbeat file. The healthcheck cron checks
    this file's mtime to detect a stuck process."""
    heartbeat_file.parent.mkdir(parents=True, exist_ok=True)
    while not stop_event.is_set():
        try:
            heartbeat_file.write_text(datetime.now(timezone.utc).isoformat() + "\n")
        except OSError:
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass


async def health_heartbeat_loop(
    journal: TradeJournal,
    mode: str,
    interval_sec: float,
    stop_event: asyncio.Event,
    log: logging.Logger,
) -> None:
    """Emit a structured HEALTH_HEARTBEAT log line periodically."""
    started_at = time.monotonic()
    try:
        import psutil  # type: ignore
        proc = psutil.Process()
    except ImportError:
        proc = None

    while not stop_event.is_set():
        uptime_sec = int(time.monotonic() - started_at)
        h, rem = divmod(uptime_sec, 3600)
        m, _ = divmod(rem, 60)
        if proc is not None:
            try:
                mem_mb = int(proc.memory_info().rss / (1024 * 1024))
            except Exception:
                mem_mb = -1
        else:
            mem_mb = -1
        try:
            open_positions = len(journal.open_positions())
        except Exception:
            open_positions = -1

        log.info(
            "HEALTH_HEARTBEAT | uptime=%dh%02dm | memory_mb=%d | open_positions=%d | mode=%s",
            h, m, mem_mb, open_positions, mode,
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass


async def strategy_loop(
    strategy: BaseStrategy,
    executor: Executor,
    risk: RiskManager,
    bankroll_provider,
    interval_sec: float,
    log: logging.Logger,
    stop_event: asyncio.Event,
    health: HealthMonitor | None = None,
) -> None:
    """Per-strategy poll loop: scan -> evaluate -> submit, repeat."""
    await strategy.setup()
    try:
        while not stop_event.is_set():
            halted, reason = risk.is_halted()
            paused_for_balance = bool(health and health.trading_paused_for_balance)
            if halted:
                log.warning("Strategy %s paused: %s", strategy.name, reason)
            elif paused_for_balance:
                log.warning("Strategy %s paused: low wallet balance", strategy.name)
            else:
                scan_started = time.monotonic()
                log.info("SCAN_START | strategy=%s", strategy.name)
                try:
                    signals = await strategy.scan()
                    if health is not None:
                        health.record_scan(strategy.name)
                except Exception:
                    log.exception("Strategy %s scan() raised", strategy.name)
                    if health is not None:
                        health.record_error(f"{strategy.name}.scan")
                    signals = []

                for sig in signals:
                    sig.metadata.setdefault("strategy", strategy.name)
                    try:
                        intent: TradeIntent | None = await strategy.evaluate(sig)
                    except Exception:
                        log.exception("Strategy %s evaluate() raised", strategy.name)
                        continue
                    if intent is None:
                        continue
                    try:
                        bankroll = await bankroll_provider(strategy.name)
                        await executor.submit(intent, bankroll_usd=bankroll)
                    except Exception:
                        log.exception("Executor submit failed for strategy %s", strategy.name)
                        if health is not None:
                            health.record_error("executor")

                duration = time.monotonic() - scan_started
                log.info(
                    "SCAN_COMPLETE | strategy=%s | signals=%d | duration=%.1fs",
                    strategy.name, len(signals), duration,
                )

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
            except asyncio.TimeoutError:
                pass
    finally:
        await strategy.teardown()


async def risk_monitor_loop(
    risk: RiskManager,
    interval_sec: float,
    stop_event: asyncio.Event,
    log: logging.Logger,
) -> None:
    while not stop_event.is_set():
        try:
            tripped = risk.evaluate_circuit_breaker()
            if tripped:
                log.error("RISK_CIRCUIT_BREAKER | action=HALT_TRADING")
        except Exception:
            log.exception("risk monitor raised")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass


async def daily_summary_loop(
    journal: TradeJournal,
    alerts: AlertBus,
    alert_cfg: AlertConfig,
    reviewer: Reviewer,
    client: ClobClient,
    interval_sec: float,
    stop_event: asyncio.Event,
    log: logging.Logger,
) -> None:
    last_sent: datetime | None = None
    while not stop_event.is_set():
        try:
            now = utc_now()
            if is_summary_due(now, alert_cfg, last_sent):
                # Force-resolve any pending trades whose markets have settled
                # on Polymarket. Without this, settled-but-not-yet-flipped
                # positions show up in the "open" list with mark="—" and
                # their P/L is invisible in the summary.
                try:
                    closed_now = await reviewer.check_resolutions()
                    if closed_now:
                        log.info(
                            "daily_summary: reviewer settled %d trade(s) before composing summary",
                            closed_now,
                        )
                except Exception:
                    log.exception("daily_summary: reviewer.check_resolutions() raised")

                summary = reviewer.compute_review_metrics(period="daily", since_days=1, persist=False)
                day_start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
                day_start = day_start_dt.isoformat()
                realized = journal.realized_pnl_since(day_start)
                trades_today = journal.count_entries_since(day_start)
                open_rows = journal.open_positions()

                # Trades that resolved between the last summary and now (or
                # since day start on first run). These are the ones whose
                # outcome you'd otherwise have no way to see in Discord.
                resolutions_since = (last_sent or day_start_dt).isoformat()
                resolved_rows = journal.closed_since(resolutions_since)
                per_strategy_resolutions = _group_resolutions_by_strategy(resolved_rows)

                # Per-strategy breakdown — use monitoring.report so the row
                # carries calibration_delta / implied_win_rate too. Same
                # entry/resolution filter discipline as perf_report.
                per_strategy: list[dict] = []
                try:
                    from monitoring.report import _collect_view, _connect, _view_to_json
                    with _connect(journal.db_path) as conn:
                        views = _collect_view(
                            conn,
                            since=now.replace(hour=0, minute=0, second=0, microsecond=0),
                        )
                    per_strategy = [
                        _view_to_json(v)
                        for v in sorted(views.values(), key=lambda x: x.strategy)
                        if v.trades > 0 or v.open_positions > 0
                    ]
                except Exception:
                    log.exception("monitoring.report view collection failed in daily summary")

                per_strategy_positions = await _build_open_positions_by_strategy(
                    open_rows, client, log,
                )

                alerts.emit("daily_summary", {
                    "realized_pnl_usd": realized,
                    "trades_today": trades_today,
                    "win_rate": summary.get("win_rate", 0.0),
                    "open_positions": len(open_rows),
                    "per_strategy": per_strategy,
                    "per_strategy_positions": per_strategy_positions,
                    "per_strategy_resolutions": per_strategy_resolutions,
                    "resolutions_since": resolutions_since,
                })
                last_sent = now
        except Exception:
            log.exception("daily summary loop raised")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            pass


def _group_resolutions_by_strategy(
    resolved_rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Parse metadata_json once and group settled trades by strategy.

    Each row carries the fields the alert formatter needs (city, bucket,
    direction, entry, outcome, pnl) so it doesn't have to touch the DB.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in resolved_rows:
        try:
            meta = json.loads(r.get("metadata_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            meta = {}
        grouped.setdefault(r.get("strategy") or "unknown", []).append({
            "trade_id": r.get("id"),
            "strategy": r.get("strategy"),
            "market_id": r.get("market_id"),
            "direction": r.get("direction"),
            "entry_price": r.get("entry_price"),
            "size_usd": r.get("size_usd"),
            "outcome": r.get("outcome"),
            "pnl": r.get("pnl"),
            "metadata": meta,
        })
    return grouped


async def _build_open_positions_by_strategy(
    open_rows: list[dict[str, Any]],
    client: ClobClient,
    log: logging.Logger,
) -> dict[str, list[dict[str, Any]]]:
    """Group open positions by strategy, parsing metadata_json and attaching
    a current mark price (best-effort midpoint per token_id)."""
    parsed: list[dict[str, Any]] = []
    token_ids: list[str] = []
    for r in open_rows:
        try:
            meta = json.loads(r.get("metadata_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            meta = {}
        token_id = meta.get("token_id") or ""
        parsed.append({
            "trade_id": r.get("id"),
            "strategy": r.get("strategy"),
            "market_id": r.get("market_id"),
            "direction": r.get("direction"),
            "entry_price": r.get("entry_price"),
            "size_usd": r.get("size_usd"),
            "shares": r.get("shares"),
            "metadata": meta,
            "token_id": token_id,
            "mark_price": None,
        })
        if token_id:
            token_ids.append(token_id)

    # Fetch midpoints for unique token_ids in parallel. Best-effort —
    # any failure leaves mark_price=None and the formatter renders "—".
    unique_tokens = sorted(set(token_ids))
    marks: dict[str, float] = {}
    if unique_tokens and client.is_initialized:
        async def _fetch(tid: str) -> tuple[str, float | None]:
            try:
                resp = await asyncio.wait_for(client.get_midpoint(tid), timeout=5.0)
                mid = resp.get("mid") if isinstance(resp, dict) else resp
                return tid, float(mid) if mid is not None else None
            except Exception:
                return tid, None
        results = await asyncio.gather(
            *(_fetch(tid) for tid in unique_tokens), return_exceptions=False,
        )
        for tid, mid in results:
            if mid is not None:
                marks[tid] = mid
        if len(marks) < len(unique_tokens):
            log.info(
                "daily_summary: fetched marks for %d/%d open token_ids",
                len(marks), len(unique_tokens),
            )

    grouped: dict[str, list[dict[str, Any]]] = {}
    for p in parsed:
        if p["token_id"] in marks:
            p["mark_price"] = marks[p["token_id"]]
        grouped.setdefault(p["strategy"] or "unknown", []).append(p)
    return grouped


def _validate_live_safety(log: logging.Logger) -> bool:
    """Return True iff live trading is allowed to start. False = exit."""
    if not LIVE_TRADING_SAFETY_FILE.exists():
        log.error(
            "FATAL | Cannot start in LIVE mode — safety file missing. "
            "Run: touch %s",
            LIVE_TRADING_SAFETY_FILE,
        )
        return False
    return True


async def run(config_path: Path) -> int:
    cfg = load_config(config_path)

    logs_dir = os.environ.get("LOGS_DIR") or str(PROJECT_ROOT / "logs")
    log = setup_logging(
        logs_dir=logs_dir,
        console_level=cfg["logger"].get("level", "INFO"),
    )
    load_dotenv(PROJECT_ROOT / ".env")

    mode = _resolve_trading_mode(cfg)
    # Override exec config so downstream code stays consistent.
    cfg["execution"]["mode"] = mode

    if mode == "live" and not _validate_live_safety(log):
        return 3

    journal = TradeJournal(PROJECT_ROOT / cfg["logger"]["db_path"])

    exec_cfg = ExecutionConfig.from_dict(cfg["execution"])
    risk = RiskManager(RiskConfig.from_dict(cfg["risk"]), journal, is_paper=exec_cfg.is_paper)
    client = ClobClient(ClientConfig.from_dict(cfg["api"]))
    wallet = Wallet(WalletConfig(rpc_url=os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")))

    if exec_cfg.is_paper:
        client.initialize_unauthenticated()
        log.warning("EXECUTION MODE: paper — no live orders will be submitted.")
    else:
        log.warning("EXECUTION MODE: LIVE — orders will be submitted to Polymarket.")
        pk = os.getenv("PK")
        if not pk:
            log.error("FATAL | Live mode requires PK in environment.")
            return 2
        api_key = os.getenv("CLOB_API_KEY", "")
        api_secret = os.getenv("CLOB_SECRET", "")
        api_pass = os.getenv("CLOB_PASS_PHRASE", "")
        if not (api_key and api_secret and api_pass):
            log.error(
                "FATAL | Live mode requires CLOB_API_KEY, CLOB_SECRET, CLOB_PASS_PHRASE. "
                "Derive with: PK=0x... python -m core.derive_creds"
            )
            return 2
        wallet.initialize(pk)
        client.initialize_authenticated(
            private_key=pk,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_pass,
        )

    # Banner — printed AFTER initialization so the mode line reflects reality.
    enabled_strategy_names = [s["name"] for s in cfg.get("strategies", []) if s.get("enabled")]
    weather_cfg = next(
        (s for s in cfg.get("strategies", []) if s.get("name") == "weather" and s.get("enabled")),
        None,
    )
    cities = list((weather_cfg or {}).get("params", {}).get("cities", [])) if weather_cfg else []
    _print_startup_banner(
        log,
        mode=mode,
        cities=cities,
        strategies=enabled_strategy_names,
        config_path=config_path,
    )

    executor = Executor(exec_cfg, risk, journal, client)

    monitoring_cfg = cfg.get("monitoring") or {}
    # Bridge env vars into the alert config. The webhook URL is a secret —
    # we don't want it in the YAML that gets rsynced. Env var wins when set;
    # config field is the fallback for local dev.
    alerts_dict = dict(monitoring_cfg.get("alerts") or {})
    for env_key, cfg_key in (
        ("DISCORD_WEBHOOK_URL",  "discord_webhook_url"),
        ("DISCORD_USER_ID",      "discord_user_id"),
        ("TELEGRAM_BOT_TOKEN",   "telegram_bot_token"),
        ("TELEGRAM_CHAT_ID",     "telegram_chat_id"),
    ):
        env_val = os.environ.get(env_key, "").strip()
        if env_val:
            alerts_dict[cfg_key] = env_val
    alert_cfg = AlertConfig.from_dict(alerts_dict)
    alerts = AlertBus(alert_cfg)

    health_cfg = HealthConfig.from_dict(monitoring_cfg.get("health"))
    health = HealthMonitor(
        health_cfg,
        alert_hook=alerts.emit,
        wallet_balance_provider=(wallet.get_pusd_balance if wallet.is_initialized else None),
        is_paper_mode=exec_cfg.is_paper,
    )

    reviewer_cfg = ReviewerConfig.from_dict(monitoring_cfg.get("review"))
    reviewer = Reviewer(reviewer_cfg, journal.db_path, alert_hook=alerts.emit)

    risk.set_alert_hook(alerts.emit)
    executor.set_alert_hook(alerts.emit)

    market_cache_ttl = float(cfg.get("market_cache", {}).get("default_ttl_sec", 30.0))
    market_cache = MarketCache(default_ttl_sec=market_cache_ttl)
    context = StrategyContext(client=client, config=cfg, market_cache=market_cache, journal=journal)
    strategies = load_strategies(cfg, context, log)
    for s in strategies:
        if hasattr(s, "set_alert_hook"):
            s.set_alert_hook(alerts.emit)

    if not strategies:
        log.warning("No strategies enabled. Bot will idle (risk monitor only).")

    # Per-strategy paper bankroll. The default applies when a strategy isn't
    # listed under `paper_bankroll_per_strategy`. In live mode, all strategies
    # share the on-chain pUSD balance — the paper allocation is informational.
    paper_bankroll_default = float(cfg.get("paper_bankroll_usd", 1000.0))
    paper_bankroll_per_strategy: dict[str, float] = {
        k: float(v) for k, v in (cfg.get("paper_bankroll_per_strategy") or {}).items()
    }
    if exec_cfg.is_paper:
        log.info(
            "Paper bankroll: default=$%.0f, per-strategy=%s",
            paper_bankroll_default,
            ", ".join(f"{k}=${v:.0f}" for k, v in paper_bankroll_per_strategy.items()) or "(none)",
        )

    async def bankroll_provider(strategy_name: str) -> float:
        if exec_cfg.is_paper or not wallet.is_initialized:
            return paper_bankroll_per_strategy.get(strategy_name, paper_bankroll_default)
        try:
            balance = await wallet.get_pusd_balance()
            log.info("HEALTH_WALLET | pUSD_balance=$%.2f", balance)
            return balance
        except Exception:
            log.exception("Failed to read on-chain pUSD balance; falling back to paper bankroll")
            return paper_bankroll_default

    stop_event = asyncio.Event()

    def _handle_signal(sig: int) -> None:
        log.info("Received signal %d, shutting down...", sig)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _handle_signal, s)
        except NotImplementedError:
            pass

    await alerts.start()
    await health.start()
    await reviewer.start()

    heartbeat_file_str = os.environ.get("HEARTBEAT_FILE") or str(DEFAULT_HEARTBEAT)
    heartbeat_file = Path(heartbeat_file_str)
    heartbeat_interval = float(monitoring_cfg.get("health", {}).get("heartbeat_interval_sec", 60.0))

    tasks: list[asyncio.Task] = [
        asyncio.create_task(
            heartbeat_loop(heartbeat_file, heartbeat_interval, stop_event),
            name="heartbeat",
        ),
        asyncio.create_task(
            health_heartbeat_loop(journal, mode, 15 * 60.0, stop_event, log),
            name="health_heartbeat",
        ),
        asyncio.create_task(
            risk_monitor_loop(risk, float(cfg["polling"]["risk_check_interval_sec"]), stop_event, log),
            name="risk_monitor",
        ),
        asyncio.create_task(
            daily_summary_loop(
                journal=journal,
                alerts=alerts,
                alert_cfg=alert_cfg,
                reviewer=reviewer,
                client=client,
                interval_sec=60.0,
                stop_event=stop_event,
                log=log,
            ),
            name="daily_summary",
        ),
    ]
    for strategy in strategies:
        tasks.append(
            asyncio.create_task(
                strategy_loop(
                    strategy=strategy,
                    executor=executor,
                    risk=risk,
                    bankroll_provider=bankroll_provider,
                    interval_sec=float(cfg["polling"]["market_scan_interval_sec"]),
                    log=log,
                    stop_event=stop_event,
                    health=health,
                ),
                name=f"strategy:{strategy.name}",
            )
        )

    log.info("Event loop running with %d strategy task(s).", len(strategies))
    if exec_cfg.is_paper:
        log.info("Paper mode: circuit breaker disabled — daily-loss halt will not engage.")
    await stop_event.wait()
    log.info("Stop requested. Cancelling %d task(s)...", len(tasks))
    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await t
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Task %s raised during shutdown", t.get_name())

    await reviewer.stop()
    await health.stop()
    await alerts.stop()

    log.info("Shutdown complete.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Polymarket trading bot.")
    parser.add_argument(
        "config",
        nargs="?",
        default=str(DEFAULT_CONFIG),
        help="Path to config YAML (default: config/config.yaml)",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Initialize everything, log status, then exit. Used to verify config + imports.",
    )
    args = parser.parse_args(argv)

    config_path = Path(args.config)

    if args.smoke_test:
        return asyncio.run(_smoke_test(config_path))

    return asyncio.run(run(config_path))


async def _smoke_test(config_path: Path) -> int:
    cfg = load_config(config_path)
    log = setup_logging(console_level=cfg["logger"].get("level", "INFO"))
    log.info("Smoke test: config loaded OK from %s", config_path)
    log.info("  risk:      %s", cfg["risk"])
    log.info("  execution: %s", cfg["execution"])
    log.info("  polling:   %s", cfg["polling"])
    log.info("  api:       %s", cfg["api"])
    log.info("  strategies: %s", [s["name"] for s in cfg["strategies"]])

    journal = TradeJournal(PROJECT_ROOT / cfg["logger"]["db_path"])
    log.info("Trade journal initialized at %s", journal.db_path)

    exec_cfg = ExecutionConfig.from_dict(cfg["execution"])
    risk = RiskManager(RiskConfig.from_dict(cfg["risk"]), journal, is_paper=exec_cfg.is_paper)
    log.info("Risk manager initialized (kelly_fraction=%s, max_position_usd=%s)",
             risk.config.kelly_fraction, risk.config.max_position_usd)
    log.info("Execution config: mode=%s order_type=%s staged=%s",
             exec_cfg.mode, exec_cfg.order_type, exec_cfg.staged_entry)

    ClobClient(ClientConfig.from_dict(cfg["api"]))
    log.info("CLOB client constructed (not initialized — paper-mode smoke test).")

    context = StrategyContext(client=None, config=cfg, market_cache=MarketCache())
    strategies = load_strategies(cfg, context, log)
    log.info("Strategies discovered: %d enabled", len(strategies))

    log.info("Smoke test passed. Exiting.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
