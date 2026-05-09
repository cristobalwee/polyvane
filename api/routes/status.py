"""GET /status — bot health, mode, strategy roster, risk counters.

This endpoint is composed from on-disk state only (config + heartbeat +
journal) so the API can run as a separate process. Fields that require
the live bot's in-memory state (last_scan_duration_sec, exact uptime
since process start) are best-effort or null.
"""
from __future__ import annotations

from datetime import datetime, timezone

import aiosqlite
from fastapi import APIRouter, Depends

from api import API_VERSION
from api.deps import (
    get_db,
    heartbeat_uptime_seconds,
    last_heartbeat_at,
    load_config,
    trading_mode,
)
from api.models import RiskStatus, StatusResponse, StrategyStatus, WalletStatus


router = APIRouter()


def _utc_day_start_iso() -> str:
    now = datetime.now(timezone.utc)
    return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


async def _open_position_count(db: aiosqlite.Connection) -> int:
    async with db.execute(
        "SELECT COUNT(*) AS n FROM trades WHERE outcome = 'pending'"
    ) as cur:
        row = await cur.fetchone()
    return int(row["n"]) if row else 0


async def _daily_realized_pnl(db: aiosqlite.Connection) -> float:
    async with db.execute(
        "SELECT COALESCE(SUM(pnl), 0.0) AS total FROM trades "
        "WHERE outcome IN ('won','lost') AND timestamp >= ?",
        (_utc_day_start_iso(),),
    ) as cur:
        row = await cur.fetchone()
    return float(row["total"]) if row else 0.0


async def _last_signal_at_per_strategy(db: aiosqlite.Connection) -> dict[str, datetime]:
    """Map strategy -> timestamp of the most recent recorded entry.

    Used as a proxy for "last_signal_at" — the journal only stores acted
    signals (rejected ones aren't persisted), so this is the best we can
    do without the bot's in-memory signal log.
    """
    out: dict[str, datetime] = {}
    async with db.execute(
        "SELECT strategy, MAX(timestamp) AS ts FROM trades GROUP BY strategy"
    ) as cur:
        rows = await cur.fetchall()
    for r in rows:
        ts = r["ts"]
        if not ts:
            continue
        try:
            out[r["strategy"]] = datetime.fromisoformat(ts)
        except ValueError:
            continue
    return out


async def _open_position_strategies(db: aiosqlite.Connection) -> set[str]:
    async with db.execute(
        "SELECT DISTINCT strategy FROM trades WHERE outcome = 'pending'"
    ) as cur:
        rows = await cur.fetchall()
    return {r["strategy"] for r in rows}


def _weather_city_counts(params: dict) -> tuple[int | None, int | None]:
    """Return (cities_active, cities_skipped) for a weather strategy entry.

    `cities_active` counts cities the operator listed in config; we don't
    re-run the resolution-registry filter from here (that would couple the
    API to the strategy package). `cities_skipped` is left as None.
    """
    cities = params.get("cities")
    if not isinstance(cities, list):
        return None, None
    return len(cities), None


@router.get("", response_model=StatusResponse)
async def get_status(db: aiosqlite.Connection = Depends(get_db)) -> StatusResponse:
    cfg = load_config()
    mode = trading_mode()

    last_signals = await _last_signal_at_per_strategy(db)
    open_positions = await _open_position_count(db)
    daily_pnl = await _daily_realized_pnl(db)

    strategies: dict[str, StrategyStatus] = {}
    for entry in cfg.get("strategies") or []:
        name = entry.get("name")
        if not name:
            continue
        params = entry.get("params") or {}
        cities_active, cities_skipped = (None, None)
        if name.startswith("weather"):
            cities_active, cities_skipped = _weather_city_counts(params)
        strategies[name] = StrategyStatus(
            enabled=bool(entry.get("enabled", False)),
            cities_active=cities_active,
            cities_skipped=cities_skipped,
            last_signal_at=last_signals.get(name),
        )

    risk_cfg = cfg.get("risk") or {}
    risk = RiskStatus(
        open_positions=open_positions,
        max_concurrent=int(risk_cfg.get("max_concurrent_positions", 0)),
        daily_pnl=round(daily_pnl, 2),
        daily_loss_limit=-abs(float(risk_cfg.get("max_daily_loss_usd", 0.0))),
        # The bot's in-memory circuit-breaker flag isn't accessible from
        # this process. Approximate: tripped iff today's realized PnL has
        # already crossed the loss limit.
        circuit_breaker_active=daily_pnl <= -abs(float(risk_cfg.get("max_daily_loss_usd", 0.0))),
    )

    wallet = WalletStatus(
        pUSD_balance=None,
        address=None,
    )

    api_cfg = cfg.get("api") or {}
    exchange_version = str(api_cfg.get("exchange_version", "v2"))
    exchange = f"polymarket_clob_{exchange_version}"

    return StatusResponse(
        mode=mode,  # type: ignore[arg-type]
        uptime_seconds=heartbeat_uptime_seconds(),
        last_scan_at=last_heartbeat_at(),
        last_scan_duration_sec=None,
        strategies=strategies,
        wallet=wallet,
        risk=risk,
        version=API_VERSION,
        exchange=exchange,
    )
