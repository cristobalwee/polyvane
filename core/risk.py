"""Risk management — Kelly sizing, hard limits, daily-loss circuit breaker.

The risk manager exposes a single `check_trade()` method the executor MUST call
before every order. A negative decision aborts the trade. The manager also
tracks realized PnL on the current UTC day and trips a circuit breaker that
halts ALL further trading once `max_daily_loss_usd` is exceeded.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from core.logger import TradeJournal


log = logging.getLogger(__name__)


@dataclass
class VolumeTier:
    """Per-volume-bucket position cap.

    A market's `volume_usd` is matched against the first tier whose range
    contains it; the matching tier's `max_position_usd` is the hard cap for
    that trade. Tiers are evaluated in order, so put narrower ranges first.
    """
    name: str
    min_volume_usd: float
    max_volume_usd: float       # math.inf for the top tier
    max_position_usd: float


# Defaults that mirror the operational picture as of April 2026 (markets
# scaled past $200K cumulative volume; competition denser at the top end).
# Override via `risk.volume_position_tiers` in config.yaml.
_DEFAULT_VOLUME_TIERS: list[VolumeTier] = [
    VolumeTier("low",  0.0,        50_000.0,  3.0),
    VolumeTier("mid",  50_000.0,   200_000.0, 8.0),
    VolumeTier("high", 200_000.0,  float("inf"), 15.0),
]


@dataclass
class RiskConfig:
    kelly_fraction: float
    max_position_usd: float
    max_daily_positions: int
    max_concurrent_positions: int
    max_daily_loss_usd: float
    max_category_exposure_pct: float
    min_edge_pct: float
    volume_position_tiers: list[VolumeTier]
    # Per-strategy concurrent-position cap. 0 = disabled (only the global
    # cap applies). When >0, each strategy is limited to this many open
    # positions independently — strategies don't starve each other when
    # they fire near-simultaneously, so multi-strategy comparisons stay
    # fair. Global cap above remains the hard ceiling for bug containment.
    max_concurrent_positions_per_strategy: int = 0
    # When true, reject any new trade that matches an already-open position
    # on (strategy, market_id, direction). Prevents accidental re-entry
    # when a strategy's scan re-fires on the same market before resolution,
    # and survives bot restarts (in-memory dedup sets don't). Set false if
    # you want a strategy to legitimately pyramid into a position.
    dedup_open_positions: bool = True
    # Per-strategy override for `dedup_open_positions`. Strategies named here
    # use the override; everything else uses the global default. Used by
    # the lazy ladder: each price rung issues a fresh BUY on the same bucket,
    # which the global dedup would otherwise reject as duplicate.
    dedup_open_positions_per_strategy: dict[str, bool] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RiskConfig":
        tiers_raw = d.get("volume_position_tiers")
        tiers = _parse_tiers(tiers_raw) if tiers_raw else list(_DEFAULT_VOLUME_TIERS)
        return cls(
            kelly_fraction=float(d["kelly_fraction"]),
            max_position_usd=float(d["max_position_usd"]),
            max_daily_positions=int(d["max_daily_positions"]),
            max_concurrent_positions=int(d["max_concurrent_positions"]),
            max_daily_loss_usd=float(d["max_daily_loss_usd"]),
            max_category_exposure_pct=float(d["max_category_exposure_pct"]),
            min_edge_pct=float(d["min_edge_pct"]),
            volume_position_tiers=tiers,
            max_concurrent_positions_per_strategy=int(
                d.get("max_concurrent_positions_per_strategy", 0)
            ),
            dedup_open_positions=bool(d.get("dedup_open_positions", True)),
            dedup_open_positions_per_strategy={
                str(k): bool(v) for k, v in (d.get("dedup_open_positions_per_strategy") or {}).items()
            },
        )

    def dedup_for(self, strategy: str) -> bool:
        if strategy in self.dedup_open_positions_per_strategy:
            return self.dedup_open_positions_per_strategy[strategy]
        return self.dedup_open_positions


def _parse_tiers(raw: Any) -> list[VolumeTier]:
    out: list[VolumeTier] = []
    for entry in raw or []:
        max_vol = entry.get("max_volume_usd")
        out.append(VolumeTier(
            name=str(entry.get("name") or "tier"),
            min_volume_usd=float(entry.get("min_volume_usd", 0.0)),
            max_volume_usd=float("inf") if max_vol in (None, "inf", "infinity") else float(max_vol),
            max_position_usd=float(entry["max_position_usd"]),
        ))
    return out


@dataclass
class TradeDecision:
    """Returned by `RiskManager.check_trade()`."""
    approved: bool
    size_usd: float
    reason: str = ""
    volume_tier: str = "default"

    def __bool__(self) -> bool:
        return self.approved


def kelly_size(edge: float, price: float, bankroll_usd: float, fraction: float) -> float:
    """Fractional Kelly for a binary outcome priced at `price` with true probability
    `price + edge`. Returns the recommended USD stake.

    For a binary bet at price p (0 < p < 1) where you believe true prob is q = p + edge:
      Kelly fraction = (q - p) / (1 - p) = edge / (1 - p)  on the YES side.
    Multiplies by `fraction` (e.g., 0.25 for quarter-Kelly) and clamps to [0, bankroll].
    """
    if price <= 0.0 or price >= 1.0 or edge <= 0.0 or bankroll_usd <= 0.0:
        return 0.0
    raw = edge / (1.0 - price)
    stake = max(0.0, min(1.0, raw)) * fraction * bankroll_usd
    return stake


class RiskManager:
    """Pre-trade risk gate. Stateless w.r.t. fills — it queries the journal."""

    def __init__(
        self,
        config: RiskConfig,
        journal: TradeJournal,
        *,
        is_paper: bool = False,
    ) -> None:
        self.config = config
        self.journal = journal
        # In paper mode the circuit breaker is fully disabled — there's no real
        # money at stake, so halting trading just hides whatever went wrong
        # from the post-hoc review (and prevents the strategies under evaluation
        # from getting more samples). The breaker re-engages in live mode.
        self._is_paper = bool(is_paper)
        self._lock = Lock()
        self._tripped: bool = False
        self._tripped_reason: str = ""
        self._day_anchor: str = self._utc_day_start_iso()
        # Optional alert hook: callable(event_type, payload). Set by main.py
        # once the alert bus is constructed; left None in tests / smoke runs.
        self._alert_hook: Any = None
        self._drawdown_warned: bool = False

    def set_alert_hook(self, hook: Any) -> None:
        self._alert_hook = hook

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        hook = self._alert_hook
        if hook is None:
            return
        try:
            hook(event_type, payload)
        except Exception:
            log.debug("alert hook raised", exc_info=True)

    @staticmethod
    def _utc_day_start_iso() -> str:
        now = datetime.now(timezone.utc)
        return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

    def _maybe_roll_day(self) -> None:
        anchor = self._utc_day_start_iso()
        if anchor != self._day_anchor:
            log.info("Risk: rolling to new UTC day; clearing circuit breaker if tripped")
            self._day_anchor = anchor
            self._tripped = False
            self._tripped_reason = ""
            self._drawdown_warned = False

    def is_halted(self) -> tuple[bool, str]:
        if self._is_paper:
            return False, ""
        with self._lock:
            self._maybe_roll_day()
            return self._tripped, self._tripped_reason

    def evaluate_circuit_breaker(self) -> bool:
        """Re-check daily PnL against the circuit-breaker threshold.

        Returns True if the breaker is currently tripped.
        """
        if self._is_paper:
            return False
        with self._lock:
            self._maybe_roll_day()
            realized = self.journal.realized_pnl_since(self._day_anchor)
            limit = abs(self.config.max_daily_loss_usd)

            # 50%-of-limit drawdown warning, fired at most once per UTC day.
            if not self._drawdown_warned and realized <= -0.5 * limit and realized > -limit:
                self._drawdown_warned = True
                self._emit("drawdown_warning", {
                    "realized_pnl_usd": realized,
                    "daily_loss_limit_usd": limit,
                    "fraction_of_limit": abs(realized) / limit if limit else 0.0,
                })

            if realized <= -limit and not self._tripped:
                self._tripped = True
                self._tripped_reason = (
                    f"Daily loss circuit breaker tripped: realized PnL "
                    f"${realized:.2f} <= -${limit:.2f}"
                )
                log.error(self._tripped_reason)
                self._emit("circuit_breaker", {
                    "realized_pnl_usd": realized,
                    "daily_loss_limit_usd": limit,
                    "reason": self._tripped_reason,
                })
            return self._tripped

    def cap_for_volume(self, market_volume_usd: float | None) -> tuple[float, str]:
        """Return (max_position_usd, tier_name) for a given market volume.

        If volume is None or no tier matches, fall back to the global
        `max_position_usd` and tier name 'default'.
        """
        if market_volume_usd is None:
            return self.config.max_position_usd, "default"
        for tier in self.config.volume_position_tiers:
            if tier.min_volume_usd <= market_volume_usd < tier.max_volume_usd:
                cap = min(tier.max_position_usd, self.config.max_position_usd) \
                    if self.config.max_position_usd > 0 else tier.max_position_usd
                # Honor whichever cap is tighter so the global cap remains a
                # safety ceiling when set conservatively in config.
                return min(tier.max_position_usd, self.config.max_position_usd), tier.name
        return self.config.max_position_usd, "default"

    def size_for_signal(
        self,
        edge: float,
        price: float,
        bankroll_usd: float,
        *,
        market_volume_usd: float | None = None,
    ) -> tuple[float, str]:
        """Compute fractional-Kelly size, capped by the volume tier.

        Returns (size_usd, tier_name).
        """
        raw = kelly_size(edge, price, bankroll_usd, self.config.kelly_fraction)
        cap, tier_name = self.cap_for_volume(market_volume_usd)
        return min(raw, cap), tier_name

    def check_trade(
        self,
        *,
        strategy: str,
        market_id: str,
        category: str | None,
        edge: float,
        price: float,
        bankroll_usd: float,
        proposed_size_usd: float | None = None,
        market_volume_usd: float | None = None,
    ) -> TradeDecision:
        """Pre-trade gate. Returns a `TradeDecision` with the approved size.

        If `proposed_size_usd` is None, the manager computes it via fractional Kelly.
        """
        with self._lock:
            self._maybe_roll_day()

            # Circuit-breaker gates: skipped entirely in paper mode so we keep
            # collecting trade samples even when a strategy is bleeding.
            if not self._is_paper:
                if self._tripped:
                    return TradeDecision(False, 0.0, f"circuit_breaker: {self._tripped_reason}")

            if edge < self.config.min_edge_pct:
                return TradeDecision(
                    False, 0.0,
                    f"edge {edge:.4f} below min_edge_pct {self.config.min_edge_pct}",
                )

            # Realized loss check (re-runs the breaker if just-tripped). Live only.
            if not self._is_paper:
                realized = self.journal.realized_pnl_since(self._day_anchor)
                if realized <= -abs(self.config.max_daily_loss_usd):
                    self._tripped = True
                    self._tripped_reason = (
                        f"Daily loss ${realized:.2f} <= -${self.config.max_daily_loss_usd:.2f}"
                    )
                    return TradeDecision(False, 0.0, f"circuit_breaker: {self._tripped_reason}")

            # Daily entry-count cap.
            entries_today = self.journal.count_entries_since(self._day_anchor)
            if entries_today >= self.config.max_daily_positions:
                return TradeDecision(
                    False, 0.0,
                    f"max_daily_positions reached ({entries_today}/{self.config.max_daily_positions})",
                )

            # Concurrent open-position cap + per-category exposure.
            open_positions = self.journal.open_positions()

            # One-and-done dedup: reject if this strategy already holds an
            # open position on the same market_id. Catches the common
            # "scan re-fires on the same market" case and survives bot
            # restarts (unlike per-strategy in-memory _seen sets).
            #
            # Dedup key is (strategy, market_id) — direction-agnostic.
            # Polymarket markets are bucket-specific, so each bucket has its
            # own market_id; different buckets within an event are NOT
            # blocked by this. Both YES and NO on the same bucket would
            # collide — that's intentional. A strategy that legitimately
            # wants both legs (e.g. arbitrage) can opt out by setting
            # `risk.dedup_open_positions: false` in config.
            if self.config.dedup_for(strategy):
                duplicate = next(
                    (p for p in open_positions
                     if p.get("strategy") == strategy and p.get("market_id") == market_id),
                    None,
                )
                if duplicate is not None:
                    return TradeDecision(
                        False, 0.0,
                        f"duplicate_open_position: {strategy} already holds "
                        f"trade_id={duplicate.get('id')} on {market_id}",
                    )

            if len(open_positions) >= self.config.max_concurrent_positions:
                return TradeDecision(
                    False, 0.0,
                    f"max_concurrent_positions reached ({len(open_positions)})",
                )

            # Per-strategy concurrent cap. Only applied when configured (>0).
            if self.config.max_concurrent_positions_per_strategy > 0:
                strategy_open = sum(
                    1 for p in open_positions if p.get("strategy") == strategy
                )
                if strategy_open >= self.config.max_concurrent_positions_per_strategy:
                    return TradeDecision(
                        False, 0.0,
                        f"max_concurrent_positions_per_strategy reached for "
                        f"{strategy} ({strategy_open}/"
                        f"{self.config.max_concurrent_positions_per_strategy})",
                    )

            # Sizing — clamped by both the global cap and the volume tier.
            cap, tier_name = self.cap_for_volume(market_volume_usd)
            if proposed_size_usd is None:
                raw = kelly_size(edge, price, bankroll_usd, self.config.kelly_fraction)
                size = min(raw, cap)
            else:
                size = min(float(proposed_size_usd), cap)

            if size <= 0.0:
                return TradeDecision(False, 0.0, "computed size <= 0", volume_tier=tier_name)

            # Per-category exposure check (only meaningful if a category is provided).
            if category:
                exposure_by_cat: dict[str, float] = defaultdict(float)
                total_exposure = 0.0
                for p in open_positions:
                    cat = (p.get("metadata_json") or "{}")
                    # metadata_json is a string; we only care about category if present.
                    # Lightweight parse to avoid importing json here.
                    cat_value = ""
                    if '"category"' in cat:
                        try:
                            import json as _json
                            cat_value = _json.loads(cat).get("category", "") or ""
                        except Exception:
                            cat_value = ""
                    exposure_by_cat[cat_value] += float(p.get("size_usd") or 0.0)
                    total_exposure += float(p.get("size_usd") or 0.0)

                projected_total = total_exposure + size
                if projected_total > 0:
                    projected_cat = exposure_by_cat[category] + size
                    pct = projected_cat / projected_total
                    if pct > self.config.max_category_exposure_pct:
                        return TradeDecision(
                            False, 0.0,
                            f"category '{category}' would be {pct:.1%} of book "
                            f"(> {self.config.max_category_exposure_pct:.0%})",
                        )

            log.debug(
                "Risk approved %s on %s: size=$%.2f tier=%s edge=%.4f price=%.4f",
                strategy, market_id, size, tier_name, edge, price,
            )
            return TradeDecision(True, size, "ok", volume_tier=tier_name)

    def trip(self, reason: str) -> None:
        """Manually trip the circuit breaker (e.g., for fatal errors)."""
        if self._is_paper:
            log.warning("Risk: trip() ignored in paper mode (reason=%s)", reason)
            return
        with self._lock:
            self._tripped = True
            self._tripped_reason = reason
            log.error("Risk: circuit breaker manually tripped: %s", reason)
