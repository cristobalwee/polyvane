"""Pydantic response models for the API.

Every endpoint returns a typed model from this file. Datetimes serialize as
ISO 8601 with UTC `Z` suffix to match what the dashboard expects. Money
values are floats in USD; we don't bother with Decimal — the journal stores
floats and we'd just be lying about precision if we converted.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


def _iso_z(dt: datetime) -> str:
    """ISO 8601 with `Z` suffix, never `+00:00`. Matches JS Date parsing."""
    s = dt.isoformat()
    return s.replace("+00:00", "Z") if s.endswith("+00:00") else s


class _Base(BaseModel):
    model_config = ConfigDict(json_encoders={datetime: _iso_z})


# ---- /ping --------------------------------------------------------------

class PingResponse(_Base):
    ok: bool = True
    service: str = "polyvane-api"
    version: str
    time: datetime


# ---- /status ------------------------------------------------------------

class StrategyStatus(_Base):
    enabled: bool
    cities_active: int | None = None
    cities_skipped: int | None = None
    last_signal_at: datetime | None = None


class WalletStatus(_Base):
    pUSD_balance: float | None = None
    address: str | None = None


class RiskStatus(_Base):
    open_positions: int
    max_concurrent: int
    daily_pnl: float
    daily_loss_limit: float
    circuit_breaker_active: bool


class StatusResponse(_Base):
    mode: Literal["paper", "live"]
    uptime_seconds: int | None = None
    last_scan_at: datetime | None = None
    last_scan_duration_sec: float | None = None
    strategies: dict[str, StrategyStatus]
    wallet: WalletStatus
    risk: RiskStatus
    version: str
    exchange: str


# ---- /positions ---------------------------------------------------------

class Position(_Base):
    id: int
    market_id: str
    market_question: str | None = None
    city: str | None = None
    bucket: str | None = None
    direction: str
    entry_price: float
    current_price: float | None = None
    exit_price: float | None = None
    size_usd: float
    shares: float
    edge_at_entry: float
    opened_at: datetime
    resolved_at: datetime | None = None
    outcome: Literal["won", "lost", "pending"]
    pnl: float | None = None
    strategy: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class PaginationMeta(_Base):
    total: int
    limit: int
    offset: int
    has_more: bool


class PositionsResponse(_Base):
    positions: list[Position]
    pagination: PaginationMeta


# ---- /performance -------------------------------------------------------

class PerformanceSummary(_Base):
    total_pnl: float
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    avg_edge_at_entry: float
    avg_realized_edge: float
    max_drawdown: float
    roi_pct: float


class PerformanceBreakdownRow(_Base):
    group: str
    pnl: float
    trades: int
    win_rate: float


class PerformanceResponse(_Base):
    period: str
    summary: PerformanceSummary
    breakdown: list[PerformanceBreakdownRow]


class PerformancePoint(_Base):
    time: datetime
    cumulative_pnl: float


class PerformanceTimeseriesResponse(_Base):
    period: str
    interval: str
    points: list[PerformancePoint]


# ---- /trades ------------------------------------------------------------

class Trade(_Base):
    id: int
    timestamp: datetime
    strategy: str
    market_id: str
    market_question: str | None = None
    direction: str
    entry_price: float
    size_usd: float
    shares: float
    edge_at_entry: float
    outcome: Literal["won", "lost", "pending"]
    pnl: float | None = None
    city: str | None = None
    bucket: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TradesResponse(_Base):
    trades: list[Trade]
    pagination: PaginationMeta


# ---- /markets -----------------------------------------------------------

class MarketBucket(_Base):
    label: str
    yes_price: float | None = None
    no_price: float | None = None


class Market(_Base):
    market_id: str
    market_question: str | None = None
    city: str | None = None
    resolution_date: datetime | None = None
    bucket_count: int
    total_volume: float | None = None
    resolution_source: str | None = None
    resolution_confirmed: bool
    buckets: list[MarketBucket] = Field(default_factory=list)


class MarketsResponse(_Base):
    markets: list[Market]
    note: str | None = None


# ---- /signals -----------------------------------------------------------

class Signal(_Base):
    market_id: str
    market_question: str | None = None
    bucket: str | None = None
    edge: float
    model_probability: float | None = None
    market_price: float
    action: Literal["traded", "rejected"]
    rejection_reason: str | None = None
    timestamp: datetime
    strategy: str


class SignalsResponse(_Base):
    signals: list[Signal]
    note: str | None = None


# ---- error envelope -----------------------------------------------------

class ErrorResponse(_Base):
    error: str
    detail: str | None = None
