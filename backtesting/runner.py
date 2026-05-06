"""Replay historical data through a strategy and simulate paper execution.

The runner is strategy-blind. Strategies plug in via `StrategyAdapter`,
which converts historical state (markets + as-of forecasts + actuals) into
the same `Signal` / `TradeIntent` objects the live pipeline produces.

What the runner does in one pass:

  1. For each (city, target_date, metric) discovered by the adapter,
     for each as-of timestamp T preceding the market's resolution:
       - ask the adapter to produce candidate Signals for that (market, T)
       - apply the strategy's normal `evaluate()` to gate by edge / horizon
       - look up the historical mid-price at T (paper fill assumption)
       - record an open position
  2. At the resolution boundary, walk all open positions for that market
     and settle each at its bucket's actual outcome (1.0 if YES wins,
     else 0.0).
  3. Hand the resulting `TradeRecord`s to `report.py` for metrics.

CLI:

    python -m backtesting.runner --start 2026-01-01 --end 2026-03-31 \\
        --strategy weather

    # what-if: replay with adjusted thresholds
    python -m backtesting.runner --start 2026-01-01 --end 2026-03-31 \\
        --strategy weather --override min_edge_pct=0.20 \\
        --override adjacent_min_edge_pct=0.05
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol, Sequence

import aiohttp

from strategies.base import Signal, TradeIntent

from .data_loader import HistoricalDataLoader


log = logging.getLogger("backtesting.runner")


# ----- public interfaces -----


@dataclass
class HistoricalMarket:
    """Strategy-neutral handle for a single resolved market.

    Adapters return a list of these so the runner can iterate without
    knowing about temperature buckets, whale trades, or arb spreads.
    """
    market_id: str
    category: str                    # e.g. 'weather'
    end_date_utc: datetime
    target_date: date
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class HistoricalResolution:
    """How a market resolved, in adapter-defined terms."""
    market_id: str
    metadata: dict[str, Any]


class StrategyAdapter(Protocol):
    """Backtesting interface a strategy must provide.

    Lives separately from `BaseStrategy` because live scanning hits APIs
    while backtesting reads pre-fetched historical data — coupling them
    would force every strategy to also expose a "score from historical
    data" path. Strategies opt in to backtesting by implementing this.
    """

    name: str

    async def discover_markets(
        self,
        loader: HistoricalDataLoader,
        *,
        start: date,
        end: date,
    ) -> list[HistoricalMarket]:
        ...

    async def signals_for(
        self,
        market: HistoricalMarket,
        *,
        as_of: datetime,
        loader: HistoricalDataLoader,
    ) -> list[Signal]:
        ...

    async def evaluate(self, signal: Signal) -> TradeIntent | None:
        ...

    async def resolve(
        self,
        market: HistoricalMarket,
        *,
        loader: HistoricalDataLoader,
    ) -> HistoricalResolution | None:
        ...

    def settle(self, signal: Signal, resolution: HistoricalResolution) -> float:
        """Return the realized outcome (1.0 for YES win, 0.0 for loss)."""
        ...


@dataclass
class BacktestConfig:
    start: date
    end: date
    strategy_name: str
    # As-of offsets from market end, in hours. Each emits one scan window
    # per market — e.g. [48, 36, 24, 12] backtests four sequential as-ofs.
    as_of_offsets_hours: tuple[float, ...] = (48.0, 36.0, 24.0, 12.0)
    paper_position_usd: float = 5.0
    cache_dir: Path | None = None
    # Strategy parameter overrides for what-if mode.
    param_overrides: dict[str, Any] = field(default_factory=dict)


@dataclass
class TradeRecord:
    market_id: str
    category: str
    city: str
    bucket: str
    bucket_role: str                  # 'primary' | 'adjacent'
    direction: str                    # 'YES' | 'NO'
    as_of: datetime
    target_date: date
    entry_price: float
    fill_price: float
    model_prob: float
    edge: float
    size_usd: float
    settled_price: float              # 1.0 win, 0.0 loss
    pnl_usd: float
    volume_usd: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BacktestResult:
    config: BacktestConfig
    trades: list[TradeRecord]
    discovered_markets: int
    skipped_no_resolution: int
    skipped_no_price: int


# ----- runner -----


class BacktestRunner:
    def __init__(
        self,
        adapter: StrategyAdapter,
        config: BacktestConfig,
    ) -> None:
        self.adapter = adapter
        self.config = config

    async def run(self) -> BacktestResult:
        async with aiohttp.ClientSession() as session:
            loader = HistoricalDataLoader(session, cache_dir=self.config.cache_dir)
            markets = await self.adapter.discover_markets(
                loader, start=self.config.start, end=self.config.end,
            )
            log.info("Discovered %d historical markets in [%s, %s]",
                     len(markets), self.config.start, self.config.end)

            trades: list[TradeRecord] = []
            no_resolution = 0
            no_price = 0

            # Process markets serially per as-of offset to keep API pressure low.
            for market in markets:
                resolution = await self.adapter.resolve(market, loader=loader)
                if resolution is None:
                    no_resolution += 1
                    continue
                for offset_h in self.config.as_of_offsets_hours:
                    as_of = market.end_date_utc - timedelta(hours=offset_h)
                    if as_of < datetime.combine(
                        self.config.start, datetime.min.time(), tzinfo=timezone.utc,
                    ):
                        continue
                    signals = await self.adapter.signals_for(
                        market, as_of=as_of, loader=loader,
                    )
                    for signal in signals:
                        intent = await self.adapter.evaluate(signal)
                        if intent is None:
                            continue
                        token_id = signal.token_id
                        fill_price = signal.price or 0.0
                        if token_id:
                            historical_price = await loader.fetch_token_price_at(token_id, as_of)
                            if historical_price is not None:
                                fill_price = historical_price
                            else:
                                no_price += 1
                        if fill_price <= 0 or fill_price >= 1:
                            continue
                        settled = self.adapter.settle(signal, resolution)
                        size_usd = self.config.paper_position_usd
                        # YES position: shares = size / price; payout = shares * settled.
                        shares = size_usd / fill_price
                        pnl = shares * settled - size_usd
                        meta = dict(signal.metadata)
                        trades.append(TradeRecord(
                            market_id=signal.market_id,
                            category=signal.category or market.category,
                            city=str(meta.get("city", "")),
                            bucket=str(meta.get("bucket", "")),
                            bucket_role=str(meta.get("bucket_role", "primary")),
                            direction=signal.direction,
                            as_of=as_of,
                            target_date=market.target_date,
                            entry_price=signal.price or 0.0,
                            fill_price=fill_price,
                            model_prob=float(meta.get("model_prob", signal.confidence)),
                            edge=signal.edge,
                            size_usd=size_usd,
                            settled_price=settled,
                            pnl_usd=pnl,
                            volume_usd=float(meta.get("volume_usd", 0.0)),
                            metadata=meta,
                        ))
            return BacktestResult(
                config=self.config,
                trades=trades,
                discovered_markets=len(markets),
                skipped_no_resolution=no_resolution,
                skipped_no_price=no_price,
            )


# ----- CLI -----


def _parse_overrides(items: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for it in items:
        if "=" not in it:
            raise SystemExit(f"--override expects key=value, got {it!r}")
        k, v = it.split("=", 1)
        out[k.strip()] = _coerce(v.strip())
    return out


def _coerce(v: str) -> Any:
    low = v.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        if "." in v:
            return float(v)
        return int(v)
    except ValueError:
        return v


def _build_adapter(name: str, overrides: dict[str, Any]) -> StrategyAdapter:
    if name == "weather":
        from .adapters.weather import WeatherAdapter  # noqa: PLC0415
        return WeatherAdapter(param_overrides=overrides)
    raise SystemExit(f"unknown strategy: {name!r}")


async def _amain(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    overrides = _parse_overrides(args.override or [])
    adapter = _build_adapter(args.strategy, overrides)
    cfg = BacktestConfig(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        strategy_name=args.strategy,
        cache_dir=Path(args.cache_dir) if args.cache_dir else None,
        param_overrides=overrides,
        paper_position_usd=args.position_usd,
    )
    runner = BacktestRunner(adapter, cfg)
    result = await runner.run()

    from .report import render_report  # noqa: PLC0415
    render_report(result, output_path=Path(args.output) if args.output else None)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="backtesting.runner")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--cache-dir", default=".backtest-cache")
    parser.add_argument("--position-usd", type=float, default=5.0)
    parser.add_argument(
        "--override", action="append",
        help="strategy param override, e.g. --override min_edge_pct=0.20",
    )
    parser.add_argument("--output", help="write report JSON to this path")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_amain(args)))


if __name__ == "__main__":
    main()
