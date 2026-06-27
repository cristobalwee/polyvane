"""Alert bus — fans events out to configured channels (Discord/Telegram).

Design goals:
  * Lightweight: emit() never blocks the trading hot path. Channels run on
    a background asyncio queue.
  * Typed event names so callers don't fan out raw strings: see EVENT_TYPES.
  * Per-event-type cooldowns so a flapping condition doesn't spam the channel.
  * Configurable channels are no-ops when their credentials are missing —
    so you can run with Discord-only, Telegram-only, both, or neither.

Wire-up: main.py constructs an AlertBus, then passes its `emit` method (a
callable taking event_type and a payload dict) to the executor / risk module
/ strategies. The bus runs on the event loop alongside the trading tasks.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dtime, timezone
from typing import Any

import aiohttp


# Discord webhook content cap. Detail messages are split at trade boundaries
# to stay under this.
_DISCORD_CONTENT_LIMIT = 1900


log = logging.getLogger("monitoring.alerts")


EVENT_TYPES = (
    "trade_executed",
    "daily_summary",
    "drawdown_warning",
    "circuit_breaker",
    "error",
    "new_city_detected",
    "health_warning",
    "stop_loss_triggered",
)


@dataclass
class AlertConfig:
    discord_webhook_url: str = ""
    discord_user_id: str = ""              # numeric Discord ID for daily-summary @ping
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    daily_summary_time: str = "13:00"      # local civil time, HH:MM
    trade_notifications: bool = False
    new_city_notifications: bool = True
    alert_cooldown_sec: int = 300          # per-event-type cooldown floor

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> "AlertConfig":
        d = d or {}
        return cls(
            discord_webhook_url=str(d.get("discord_webhook_url") or ""),
            discord_user_id=str(d.get("discord_user_id") or "").strip(),
            telegram_bot_token=str(d.get("telegram_bot_token") or ""),
            telegram_chat_id=str(d.get("telegram_chat_id") or ""),
            daily_summary_time=str(d.get("daily_summary_time") or "13:00"),
            trade_notifications=bool(d.get("trade_notifications", False)),
            new_city_notifications=bool(d.get("new_city_notifications", True)),
            alert_cooldown_sec=int(d.get("alert_cooldown_sec", 300)),
        )

    @property
    def has_discord(self) -> bool:
        return bool(self.discord_webhook_url)

    @property
    def has_telegram(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)


@dataclass
class _AlertEvent:
    event_type: str
    payload: dict[str, Any]
    enqueued_at: float = field(default_factory=time.monotonic)


class AlertBus:
    """Async fan-out queue. Construct once; call `start()` from the event loop.

    `emit()` is sync and non-blocking — it just drops the event onto an
    asyncio queue. The background task drains the queue and POSTs to each
    configured channel.
    """

    def __init__(self, config: AlertConfig, *, session: aiohttp.ClientSession | None = None) -> None:
        self.config = config
        self._session = session
        self._owns_session = session is None
        self._queue: asyncio.Queue[_AlertEvent] = asyncio.Queue(maxsize=512)
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._last_emit_at: dict[str, float] = {}

    async def start(self) -> None:
        if self._owns_session and self._session is None:
            self._session = aiohttp.ClientSession()
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="alert_bus")
            log.info(
                "AlertBus started — discord=%s telegram=%s cooldown=%ds",
                "on" if self.config.has_discord else "off",
                "on" if self.config.has_telegram else "off",
                self.config.alert_cooldown_sec,
            )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    def emit(self, event_type: str, payload: dict[str, Any]) -> None:
        """Non-blocking fire. Safe to call from any thread on the loop."""
        if event_type == "trade_executed" and not self.config.trade_notifications:
            return
        if event_type == "new_city_detected" and not self.config.new_city_notifications:
            return
        # Per-event-type cooldown; circuit_breaker bypasses (always alert).
        # For trade_executed, bucket by exchange so Polymarket and Kalshi
        # notifications don't suppress each other.
        if event_type != "circuit_breaker":
            now = time.monotonic()
            exchange = payload.get("metadata", {}).get("exchange") if event_type == "trade_executed" else None
            cooldown_key = f"{event_type}:{exchange}" if exchange else event_type
            last = self._last_emit_at.get(cooldown_key, 0.0)
            if now - last < self.config.alert_cooldown_sec:
                return
            self._last_emit_at[cooldown_key] = now
        try:
            self._queue.put_nowait(_AlertEvent(event_type, payload))
        except asyncio.QueueFull:
            log.warning("alert queue full — dropping %s", event_type)

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self._dispatch(event)
            except Exception:
                log.exception("alert dispatch failed for %s", event.event_type)

    async def _dispatch(self, event: _AlertEvent) -> None:
        # Daily summary fans out into a head message + one detail message per
        # strategy with open positions. Everything else is a single message.
        if event.event_type == "daily_summary":
            messages = format_daily_summary_messages(event.payload, self.config)
        else:
            messages = [format_message(event.event_type, event.payload, cfg=self.config)]
        for msg in messages:
            if self.config.has_discord:
                await self._post_discord(msg, event)
            if self.config.has_telegram:
                await self._post_telegram(msg)

    async def _post_discord(self, msg: str, event: _AlertEvent) -> None:
        assert self._session is not None
        body = {"content": msg, "username": "polyvane"}
        try:
            async with self._session.post(
                self.config.discord_webhook_url,
                json=body,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 400:
                    log.warning("Discord alert -> HTTP %d", resp.status)
        except aiohttp.ClientError as e:
            log.warning("Discord alert failed: %s", e)

    async def _post_telegram(self, msg: str) -> None:
        assert self._session is not None
        url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
        body = {"chat_id": self.config.telegram_chat_id, "text": msg, "parse_mode": "Markdown"}
        try:
            async with self._session.post(
                url, json=body,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 400:
                    log.warning("Telegram alert -> HTTP %d", resp.status)
        except aiohttp.ClientError as e:
            log.warning("Telegram alert failed: %s", e)


def _format_market_meta_line(meta: dict[str, Any]) -> str:
    """Compact second-line of context for a single trade alert.

    Pulls fields the weather/lazy_weather strategies stash on Signal.metadata:
    city, end_utc (ISO), bucket, metric, unit. Missing fields are skipped.
    """
    parts: list[str] = []
    city = meta.get("city")
    if city:
        parts.append(f"🏙️ {city}")
    end_iso = meta.get("end_utc") or meta.get("end_date_utc")
    if end_iso:
        parts.append(f"📅 {_format_resolution_date(end_iso)}")
    bucket = meta.get("bucket")
    if bucket:
        unit = meta.get("unit") or ""
        unit_short = "°F" if unit.startswith("f") else ("°C" if unit.startswith("c") else "")
        metric = meta.get("metric") or "temp"
        parts.append(f"🎯 {metric} {bucket}{unit_short}".rstrip())
    return "  ".join(parts)


def _format_resolution_date(end_iso: str) -> str:
    """'2026-05-08T18:00:00+00:00' → '2026-05-08 (in 3d)' / '(in 4h)' / '(past)'."""
    try:
        end_dt = datetime.fromisoformat(end_iso)
    except (ValueError, TypeError):
        return str(end_iso)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    delta = end_dt - datetime.now(timezone.utc)
    secs = int(delta.total_seconds())
    if secs < 0:
        rel = "past"
    elif secs < 3600:
        rel = f"in {max(1, secs // 60)}m"
    elif secs < 86_400:
        rel = f"in {secs // 3600}h"
    else:
        rel = f"in {secs // 86_400}d"
    return f"{end_dt.strftime('%Y-%m-%d')} ({rel})"


def format_daily_summary_messages(payload: dict[str, Any], cfg: AlertConfig) -> list[str]:
    """Head message + one detail message per strategy.

    Each detail message lists trades that settled since the last summary
    (with final P/L) and trades that are still open, one line each. Long
    sections are split at trade boundaries to stay under Discord's
    2000-char content cap.
    """
    messages: list[str] = [_format_summary_head(payload, cfg)]
    per_positions: dict[str, list[dict[str, Any]]] = payload.get("per_strategy_positions") or {}
    per_resolutions: dict[str, list[dict[str, Any]]] = payload.get("per_strategy_resolutions") or {}
    strategies = sorted(set(per_positions) | set(per_resolutions))
    for strategy in strategies:
        positions = per_positions.get(strategy) or []
        resolutions = per_resolutions.get(strategy) or []
        if not positions and not resolutions:
            continue
        messages.extend(_format_strategy_detail(strategy, positions, resolutions))
    return messages


def _format_summary_head(payload: dict[str, Any], cfg: AlertConfig) -> str:
    ping = f"<@{cfg.discord_user_id}> " if cfg.discord_user_id else "@cristo_grana "
    head = (
        f"{ping}📊 **Daily summary** — "
        f"PnL: ${payload.get('realized_pnl_usd', 0.0):+.2f}  "
        f"trades: {payload.get('trades_today', 0)}  "
        f"win rate: {payload.get('win_rate', 0.0):.0%}  "
        f"open: {payload.get('open_positions', 0)}"
    )
    per_strategy = payload.get("per_strategy") or []
    if not per_strategy:
        return head
    # `calib` = actual_win_rate − implied_win_rate (entry-price-implied prob).
    # Positive means the strategy is winning more than the price predicted —
    # the headline diagnostic for "is there real edge?". Source: each row
    # may carry `calibration_delta` (from monitoring.report._view_to_json);
    # legacy rows from perf_report won't, in which case we render '-'.
    rows = ["```"]
    rows.append(
        f"{'strategy':<22} {'tr':>3} {'op':>3} "
        f"{'expo':>9} {'pnl':>9} {'wr':>5} {'calib':>5}"
    )
    for s in per_strategy:
        wr = f"{s['win_rate']:.0%}" if s.get("win_rate") is not None else "  -"
        cd = s.get("calibration_delta")
        calib = f"{cd:+.0%}" if isinstance(cd, (int, float)) else "  -"
        pnl = float(s["realized_pnl_usd"])
        pnl_str = f"{'-' if pnl < 0 else '+'}${abs(pnl):,.2f}"
        rows.append(
            f"{s['strategy']:<22} "
            f"{s['trades']:>3d} "
            f"{s['open_positions']:>3d} "
            f"${s['open_exposure_usd']:>7,.0f} "
            f"{pnl_str:>8} "
            f"{wr:>5} "
            f"{calib:>5}"
        )
    rows.append("```")
    return head + "\n" + "\n".join(rows)


def _format_strategy_detail(
    strategy: str,
    positions: list[dict[str, Any]],
    resolutions: list[dict[str, Any]],
) -> list[str]:
    """One or more messages summarizing a strategy's settled and open trades.

    Layout:
        ━━━ <strategy> — N open · cost · expo · P/L (unrealized)
        ✅/❌ <one-line settled trade>
        …
        · YES/NO  <one-line open position>
        …

    Long sections split at line boundaries to fit Discord's content cap.
    """
    cost_total = sum(float(p.get("size_usd") or 0.0) for p in positions)
    expo_total = sum(_position_exposure_usd(p) for p in positions)
    pnl_total = sum(_position_unrealized_pnl(p) or 0.0 for p in positions)
    pnl_known = any(_position_unrealized_pnl(p) is not None for p in positions)
    pnl_str = _format_signed_usd(pnl_total) if pnl_known else "—"

    header_parts = [
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📂 **{strategy}** — {len(positions)} open · cost ${cost_total:,.2f} · "
        f"expo ${expo_total:,.2f} · P/L {pnl_str}",
    ]
    if resolutions:
        wins = sum(1 for r in resolutions if r.get("outcome") == "won")
        realized = sum(float(r.get("pnl") or 0.0) for r in resolutions)
        realized_str = _format_signed_usd(realized)
        header_parts.append(
            f"  settled since last summary: {len(resolutions)} "
            f"({wins}W/{len(resolutions) - wins}L) · realized {realized_str}"
        )
    header = "\n".join(header_parts)

    lines: list[str] = []
    for r in resolutions:
        lines.append(_format_resolution_line(r))
    if resolutions and positions:
        lines.append("  · open ·")
    for p in positions:
        lines.append(_format_position_line(p))

    chunks: list[str] = []
    current = header
    for line in lines:
        candidate = f"{current}\n{line}"
        if len(candidate) > _DISCORD_CONTENT_LIMIT:
            chunks.append(current)
            current = f"📂 **{strategy}** _(cont.)_\n{line}"
        else:
            current = candidate
    chunks.append(current)
    return chunks


def _format_position_line(p: dict[str, Any]) -> str:
    """Single-line summary of an open position.

    Format: `  YES NYC 75-80°F (in 3d) · $3.00 @ 0.250 → 0.300 · +$0.60`
    Falls back gracefully when metadata fields are missing.
    """
    meta = p.get("metadata") or {}
    direction = p.get("direction") or ""
    label = _format_market_label(meta) or str(p.get("market_id") or "?")[:8]
    entry = float(p.get("entry_price") or 0.0)
    size = float(p.get("size_usd") or 0.0)
    pnl = _position_unrealized_pnl(p)
    mark = p.get("mark_price")
    end_iso = meta.get("end_utc") or meta.get("end_date_utc")
    end_short = _format_relative(end_iso) if end_iso else ""
    if isinstance(mark, (int, float)):
        mark_str = f"{mark:.3f}"
        pnl_str = _format_signed_usd(pnl) if pnl is not None else "—"
    elif end_short == "past":
        mark_str = "settled?"
        pnl_str = "awaiting"
    else:
        mark_str = "—"
        pnl_str = "—"
    when = f" ({end_short})" if end_short else ""
    return (
        f"  {direction} {label}{when} · "
        f"${size:,.2f} @ {entry:.3f} → {mark_str} · {pnl_str}"
    )


def _format_resolution_line(r: dict[str, Any]) -> str:
    """Single-line summary of a settled trade.

    Format: `  ✅ NYC 75-80°F won @ 0.250 → +$3.00`
    """
    meta = r.get("metadata") or {}
    label = _format_market_label(meta) or str(r.get("market_id") or "?")[:8]
    direction = r.get("direction") or ""
    outcome = r.get("outcome") or "?"
    icon = "✅" if outcome == "won" else "❌"
    entry = float(r.get("entry_price") or 0.0)
    pnl = r.get("pnl")
    pnl_str = _format_signed_usd(pnl) if pnl is not None else "—"
    return f"  {icon} {direction} {label} {outcome} @ {entry:.3f} → {pnl_str}"


def _format_market_label(meta: dict[str, Any]) -> str:
    """Compact `<city> <bucket><unit>` label, dropping pieces we don't have."""
    parts: list[str] = []
    city = meta.get("city")
    if city:
        parts.append(str(city))
    bucket = meta.get("bucket")
    if bucket:
        unit = meta.get("unit") or ""
        unit_short = "°F" if unit.startswith("f") else ("°C" if unit.startswith("c") else "")
        parts.append(f"{bucket}{unit_short}")
    return " ".join(parts)


def _format_signed_usd(value: float) -> str:
    """Render `$1.10` / `-$3.00` (sign always before the dollar sign)."""
    return f"{'-' if value < 0 else '+'}${abs(value):,.2f}"


def _format_relative(end_iso: str) -> str:
    """`2026-05-08T18:00:00+00:00` → `in 3d` / `in 4h` / `past`."""
    try:
        end_dt = datetime.fromisoformat(end_iso)
    except (ValueError, TypeError):
        return ""
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    secs = int((end_dt - datetime.now(timezone.utc)).total_seconds())
    if secs < 0:
        return "past"
    if secs < 3600:
        return f"in {max(1, secs // 60)}m"
    if secs < 86_400:
        return f"in {secs // 3600}h"
    return f"in {secs // 86_400}d"


def _position_exposure_usd(p: dict[str, Any]) -> float:
    """Current $ value of a position. Falls back to size_usd if no mark."""
    mark = p.get("mark_price")
    shares = p.get("shares")
    if isinstance(mark, (int, float)) and isinstance(shares, (int, float)):
        return float(mark) * float(shares)
    return float(p.get("size_usd") or 0.0)


def _position_unrealized_pnl(p: dict[str, Any]) -> float | None:
    """None when no mark is available."""
    mark = p.get("mark_price")
    shares = p.get("shares")
    if not (isinstance(mark, (int, float)) and isinstance(shares, (int, float))):
        return None
    return float(mark) * float(shares) - float(p.get("size_usd") or 0.0)


def format_message(
    event_type: str,
    payload: dict[str, Any],
    *,
    cfg: AlertConfig | None = None,
) -> str:
    """Render a clean human-readable message. Channel-agnostic.

    Each event type gets a small, predictable layout — easier to skim than a
    raw payload dump. `cfg` is consulted only for events that need a Discord
    @mention (currently: circuit_breaker), and is optional for back-compat.
    """
    cfg = cfg or AlertConfig()
    if event_type == "trade_executed":
        head = (
            f"📈 **Trade**: {payload.get('strategy')} {payload.get('direction')} "
            f"`{payload.get('market_id')}` @ {payload.get('entry_price'):.3f} — "
            f"size ${payload.get('size_usd'):.2f} (tier={payload.get('tier')}, "
            f"edge={payload.get('edge'):.1%}) [{payload.get('mode')}]"
        )
        meta_line = _format_market_meta_line(payload.get("metadata") or {})
        return f"{head}\n{meta_line}" if meta_line else head
    if event_type == "daily_summary":
        # Kept for back-compat callers that want a single-string render.
        return format_daily_summary_messages(payload, cfg)[0]
    if event_type == "drawdown_warning":
        frac = payload.get("fraction_of_limit", 0.0)
        return (
            f"⚠️ **Drawdown warning** — daily PnL "
            f"${payload.get('realized_pnl_usd', 0.0):+.2f} "
            f"({frac:.0%} of -${payload.get('daily_loss_limit_usd', 0.0):.0f} limit)"
        )
    if event_type == "circuit_breaker":
        # Live-mode only (paper-mode RiskManager never emits this), so always
        # ping the operator — this is the "real money is bleeding" alarm.
        ping = f"<@{cfg.discord_user_id}> " if cfg.discord_user_id else ""
        return (
            f"{ping}🛑 **CIRCUIT BREAKER** — trading halted. "
            f"PnL: ${payload.get('realized_pnl_usd', 0.0):+.2f} / "
            f"-${payload.get('daily_loss_limit_usd', 0.0):.0f}"
        )
    if event_type == "insufficient_funds":
        # Account ran out of fundable balance — ping the operator; trading is
        # paused until the balance recovers (top up, or positions resolve).
        ping = f"<@{cfg.discord_user_id}> " if cfg.discord_user_id else ""
        question = payload.get("market_question") or payload.get("market_id") or "?"
        return (
            f"{ping}💸 **Insufficient funds** [{payload.get('strategy')}] — "
            f"order on `{question}` (${payload.get('size_usd', 0.0):.2f}) rejected. "
            "Trading paused until balance recovers."
        )
    if event_type == "error":
        return (
            f"❗ **Error** — {payload.get('source', 'bot')}: "
            f"{payload.get('message', 'see logs')} "
            f"(count={payload.get('count', 1)})"
        )
    if event_type == "new_city_detected":
        return (
            f"🌍 **New city detected** — `{payload.get('city')}` not in resolution "
            "registry. Add it to `strategies/weather/resolution.py` to start trading."
        )
    if event_type == "health_warning":
        return f"❤️‍🩹 **Health** — {payload.get('message', 'see logs')}"
    if event_type == "stop_loss_triggered":
        question = payload.get("market_question") or payload.get("market_id") or "?"
        return (
            f"🛑 **Stop-loss** [{payload.get('strategy')}] "
            f"`{question}` — "
            f"entry {payload.get('entry_price', 0.0):.3f} → "
            f"exit {payload.get('exit_price', 0.0):.3f} "
            f"(-{payload.get('loss_pct', 0.0):.0f}%) "
            f"PnL ${payload.get('pnl', 0.0):+.2f}"
        )
    return f"[{event_type}] {payload}"


def is_summary_due(now: datetime, cfg: AlertConfig, last_sent_at: datetime | None) -> bool:
    """True iff `now` has crossed today's daily_summary_time and it hasn't
    been sent yet today."""
    try:
        hh, mm = (int(x) for x in cfg.daily_summary_time.split(":"))
        target = dtime(hh, mm)
    except ValueError:
        return False
    if now.time() < target:
        return False
    if last_sent_at is None:
        return True
    return last_sent_at.date() < now.date()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
