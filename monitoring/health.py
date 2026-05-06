"""Bot health monitor: scan staleness, API errors, wallet balance.

Polled by main.py on a slow cadence. Failure signals are emitted on the
same alert bus as everything else, with cooldowns to prevent spam.

Design:
  * `record_scan(strategy)` is called by the strategy loop after every
    successful scan. The monitor compares the latest timestamp against
    `max_scan_gap_sec` to detect stalls.
  * `record_error(source)` and `consecutive_errors(source)` track repeated
    upstream failures (NOAA / Open-Meteo / Gamma).
  * Wallet checks query the wallet directly; in paper mode they're skipped.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Deque


log = logging.getLogger("monitoring.health")


@dataclass
class HealthConfig:
    min_wallet_balance_usd: float = 20.0
    max_scan_gap_sec: int = 900            # 15 minutes
    error_alert_threshold: int = 5          # consecutive errors before alerting
    error_window_sec: int = 3600            # rolling window for error counting
    poll_interval_sec: int = 60

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> "HealthConfig":
        d = d or {}
        return cls(
            min_wallet_balance_usd=float(d.get("min_wallet_balance_usd", 20.0)),
            max_scan_gap_sec=int(d.get("max_scan_gap_sec", 900)),
            error_alert_threshold=int(d.get("error_alert_threshold", 5)),
            error_window_sec=int(d.get("error_window_sec", 3600)),
            poll_interval_sec=int(d.get("poll_interval_sec", 60)),
        )


class HealthMonitor:
    def __init__(
        self,
        config: HealthConfig,
        *,
        alert_hook: Any = None,
        wallet_balance_provider: Callable[[], Any] | None = None,
        is_paper_mode: bool = True,
    ) -> None:
        self.config = config
        self._alert_hook = alert_hook
        self._wallet_balance_provider = wallet_balance_provider
        self._is_paper = is_paper_mode

        self._last_scan_at: dict[str, float] = {}
        self._error_log: dict[str, Deque[float]] = {}
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

        # State so we don't re-alert about the same stall every cycle.
        self._stall_alerted: dict[str, bool] = {}
        self._wallet_alerted: bool = False
        # Set externally when low-wallet alert has paused trading.
        self.trading_paused_for_balance: bool = False

    def record_scan(self, strategy: str) -> None:
        self._last_scan_at[strategy] = time.monotonic()
        if self._stall_alerted.pop(strategy, False):
            log.info("Strategy %s recovered from scan stall", strategy)

    def record_error(self, source: str) -> int:
        """Append an error timestamp for `source` and return the rolling count."""
        now = time.monotonic()
        log_q = self._error_log.setdefault(source, deque())
        log_q.append(now)
        cutoff = now - self.config.error_window_sec
        while log_q and log_q[0] < cutoff:
            log_q.popleft()
        return len(log_q)

    def consecutive_errors(self, source: str) -> int:
        return len(self._error_log.get(source, ()))

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="health_monitor")
            log.info("HealthMonitor started (poll=%ss, max_scan_gap=%ss)",
                     self.config.poll_interval_sec, self.config.max_scan_gap_sec)

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._check_scan_freshness()
                await self._check_wallet_balance()
                self._check_error_thresholds()
            except Exception:
                log.exception("health monitor cycle failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.config.poll_interval_sec,
                )
            except asyncio.TimeoutError:
                continue

    def _check_scan_freshness(self) -> None:
        now = time.monotonic()
        for strategy, ts in list(self._last_scan_at.items()):
            gap = now - ts
            if gap > self.config.max_scan_gap_sec and not self._stall_alerted.get(strategy):
                self._stall_alerted[strategy] = True
                self._emit("health_warning", {
                    "message": (
                        f"Strategy {strategy!r} hasn't scanned in {gap:.0f}s "
                        f"(threshold {self.config.max_scan_gap_sec}s)"
                    ),
                })

    async def _check_wallet_balance(self) -> None:
        if self._is_paper or self._wallet_balance_provider is None:
            return
        try:
            balance = self._wallet_balance_provider()
            if asyncio.iscoroutine(balance):
                balance = await balance
            balance = float(balance)
        except Exception:
            log.debug("wallet balance check failed", exc_info=True)
            return

        if balance < self.config.min_wallet_balance_usd:
            self.trading_paused_for_balance = True
            if not self._wallet_alerted:
                self._wallet_alerted = True
                self._emit("health_warning", {
                    "message": (
                        f"Wallet balance ${balance:.2f} below minimum "
                        f"${self.config.min_wallet_balance_usd:.2f} — trading paused"
                    ),
                })
        else:
            if self._wallet_alerted:
                log.info("Wallet balance recovered: $%.2f", balance)
            self._wallet_alerted = False
            self.trading_paused_for_balance = False

    def _check_error_thresholds(self) -> None:
        for source, q in self._error_log.items():
            n = len(q)
            if n >= self.config.error_alert_threshold:
                self._emit("error", {
                    "source": source,
                    "message": f"{n} errors in last {self.config.error_window_sec}s",
                    "count": n,
                })

    def snapshot(self) -> dict[str, Any]:
        """Synchronous read for the dashboard / health endpoints."""
        now = time.monotonic()
        return {
            "now_utc": datetime.now(timezone.utc).isoformat(),
            "scans": {s: max(0.0, now - ts) for s, ts in self._last_scan_at.items()},
            "errors": {src: len(q) for src, q in self._error_log.items()},
            "trading_paused_for_balance": self.trading_paused_for_balance,
        }

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        if self._alert_hook is None:
            return
        try:
            self._alert_hook(event_type, payload)
        except Exception:
            log.debug("alert hook raised", exc_info=True)
