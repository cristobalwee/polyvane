"""Centralized logging configuration.

Three outputs:
  1. Console (stdout) at INFO — captured by the systemd journal in production.
  2. File (logs/polyvane.log) at DEBUG — full detail, rotated by size as a
     belt-and-braces backup to logrotate.
  3. File (logs/trades.log) — TRADE-level events only, the audit trail.

A custom TRADE level (numeric 25, between INFO and WARNING) is registered
on the `logging` module. Use `log.trade("...")` after calling
`get_logger(name)` in your module.
"""
from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import cast


# Custom level: between INFO (20) and WARNING (30).
TRADE_LEVEL = 25
logging.addLevelName(TRADE_LEVEL, "TRADE")


def _trade(self: logging.Logger, message: str, *args, **kwargs) -> None:
    if self.isEnabledFor(TRADE_LEVEL):
        self._log(TRADE_LEVEL, message, args, **kwargs)


# Patch onto Logger so any `logging.getLogger(...)` call gets `.trade(...)`.
logging.Logger.trade = _trade  # type: ignore[attr-defined]


_LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


class _TradeOnlyFilter(logging.Filter):
    """Pass only records at exactly the TRADE level."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno == TRADE_LEVEL


def setup_logging(
    *,
    logs_dir: str | Path | None = None,
    console_level: str = "INFO",
    file_level: str = "DEBUG",
) -> logging.Logger:
    """Configure root logger for the bot. Idempotent.

    `logs_dir` resolution order:
      1. argument
      2. LOGS_DIR env var
      3. ./logs (relative to cwd)

    Returns a named logger ('polyvane') for the caller to use as the root
    of its own log namespace.
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    # Idempotent: if we've configured before, don't stack handlers.
    if any(getattr(h, "_polyvane_managed", False) for h in root.handlers):
        return logging.getLogger("polyvane")

    # Wipe any pre-existing default handlers so we own the output surface.
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # 1. Console -> stdout. INFO+ for systemd journal.
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(getattr(logging, console_level.upper(), logging.INFO))
    console.setFormatter(fmt)
    console._polyvane_managed = True  # type: ignore[attr-defined]
    root.addHandler(console)

    # Resolve logs dir.
    resolved_dir = Path(
        logs_dir
        or os.environ.get("LOGS_DIR")
        or "logs"
    )
    try:
        resolved_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        # If we can't write logs (e.g. systemd ProtectSystem misconfig),
        # console handler still works — keep going.
        sys.stderr.write(f"WARN: cannot create logs dir {resolved_dir}: {e}\n")
        return logging.getLogger("polyvane")

    # 2. File -> polyvane.log at DEBUG. Rotated at 50MB, keep 5.
    full = logging.handlers.RotatingFileHandler(
        resolved_dir / "polyvane.log",
        maxBytes=50 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
        delay=True,
    )
    full.setLevel(getattr(logging, file_level.upper(), logging.DEBUG))
    full.setFormatter(fmt)
    full._polyvane_managed = True  # type: ignore[attr-defined]
    root.addHandler(full)

    # 3. File -> trades.log, TRADE-level only.
    trades = logging.handlers.RotatingFileHandler(
        resolved_dir / "trades.log",
        maxBytes=50 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
        delay=True,
    )
    trades.setLevel(TRADE_LEVEL)
    trades.addFilter(_TradeOnlyFilter())
    trades.setFormatter(fmt)
    trades._polyvane_managed = True  # type: ignore[attr-defined]
    root.addHandler(trades)

    # Quiet noisy third-party loggers.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    logging.getLogger("web3").setLevel(logging.WARNING)

    return logging.getLogger("polyvane")


def get_logger(name: str) -> logging.Logger:
    """Same as `logging.getLogger(name)`, typed to expose `.trade(...)`."""
    return cast(logging.Logger, logging.getLogger(name))
