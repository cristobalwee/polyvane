"""Shared dependencies: read-only DB access, config + env loading.

The API NEVER writes to the bot's SQLite database. We open the file with
SQLite's `mode=ro` URI and `aiosqlite` for non-blocking reads. A single
connection is shared via the FastAPI app state — SQLite handles concurrent
readers fine on its own.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import aiosqlite
import yaml
from fastapi import HTTPException, Request


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"
DEFAULT_HEARTBEAT = PROJECT_ROOT / "data" / "heartbeat"


# Module-level cache of the parsed YAML config. Reloads if the file's
# mtime changes — lets the operator edit config without restarting the API.
_config_cache: dict[str, Any] | None = None
_config_mtime: float = 0.0


def load_config() -> dict[str, Any]:
    """Load config/config.yaml. Re-reads on mtime change."""
    global _config_cache, _config_mtime
    path_env = os.environ.get("POLYVANE_CONFIG")
    path = Path(path_env) if path_env else DEFAULT_CONFIG
    if not path.exists():
        raise RuntimeError(f"config not found at {path}")
    mtime = path.stat().st_mtime
    if _config_cache is not None and mtime == _config_mtime:
        return _config_cache
    with path.open("r") as f:
        _config_cache = yaml.safe_load(f) or {}
    _config_mtime = mtime
    return _config_cache


def trading_mode() -> str:
    """Resolve mode the same way main.py does: env wins over config."""
    env = (os.environ.get("TRADING_MODE") or "").strip().lower()
    if env in ("paper", "live"):
        return env
    cfg = load_config()
    cfg_mode = str((cfg.get("execution") or {}).get("mode", "paper")).lower()
    return cfg_mode if cfg_mode in ("paper", "live") else "paper"


def db_path() -> Path:
    """Absolute path to the bot's trade journal SQLite file."""
    cfg = load_config()
    rel = (cfg.get("logger") or {}).get("db_path") or "data/trade_journal.db"
    return (PROJECT_ROOT / rel).resolve()


def heartbeat_path() -> Path:
    env = os.environ.get("HEARTBEAT_FILE")
    return Path(env) if env else DEFAULT_HEARTBEAT


# ---- DB connection lifecycle --------------------------------------------

async def open_db(app_state: Any) -> None:
    """Open the read-only SQLite connection and stash on app.state."""
    path = db_path()
    if not path.exists():
        raise RuntimeError(
            f"trade journal not found at {path}. Start the bot once to create it, "
            "or set the path via config.logger.db_path."
        )
    # Read-only URI — even a bug in our code can't write. `nolock=1` is fine
    # since SQLite's WAL handles multi-reader concurrency.
    uri = f"file:{path}?mode=ro"
    conn = await aiosqlite.connect(uri, uri=True, timeout=10.0)
    conn.row_factory = aiosqlite.Row
    app_state.db = conn


async def close_db(app_state: Any) -> None:
    conn = getattr(app_state, "db", None)
    if conn is not None:
        await conn.close()
        app_state.db = None


async def get_db(request: Request) -> AsyncIterator[aiosqlite.Connection]:
    """FastAPI dependency yielding the shared aiosqlite connection."""
    conn = getattr(request.app.state, "db", None)
    if conn is None:
        raise HTTPException(status_code=503, detail="database not initialized")
    yield conn


# ---- Process startup time -----------------------------------------------
# The API tracks its own start time. We can't easily ask the bot when it
# started without parsing systemd or adding a status writer; for the
# dashboard's purposes "API uptime" is a reasonable proxy when both
# services restart together (they share a deploy).

_started_monotonic = time.monotonic()


def api_uptime_seconds() -> int:
    return int(time.monotonic() - _started_monotonic)


def heartbeat_uptime_seconds() -> int | None:
    """Best-effort uptime from heartbeat-file mtime. Returns None if absent."""
    p = heartbeat_path()
    if not p.exists():
        return None
    try:
        return max(0, int(time.time() - p.stat().st_mtime))
    except OSError:
        return None


def last_heartbeat_at() -> datetime | None:
    """When the bot last touched its heartbeat file (UTC). None if missing."""
    p = heartbeat_path()
    if not p.exists():
        return None
    try:
        return datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return None
