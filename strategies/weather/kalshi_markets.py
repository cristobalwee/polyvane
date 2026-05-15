"""Kalshi weather market discovery.

Fetches active Kalshi temperature binary markets and normalizes them into
`KalshiWeatherMarket` objects. These are the Kalshi counterpart of the
Polymarket `WeatherMarket` objects produced by `markets.py` / `GammaClient`.

Kalshi temperature tickers follow the pattern:
    KXHIGH<CITY_CODE>-<YYMONDD>-T<THRESHOLD>
    KXLOW<CITY_CODE>-<YYMONDD>-T<THRESHOLD>

Examples:
    KXHIGHATL-25MAY14-T68   (Atlanta HIGH ≥ 68°F on 2025-05-14)
    KXLOWNYC-25MAY14-T55    (NYC LOW ≥ 55°F on 2025-05-14)

Each market is binary: resolves YES if the observed extreme meets/exceeds
the threshold; NO otherwise. All Kalshi US temperature markets report in °F.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from core.kalshi_client import KalshiClient


log = logging.getLogger("strategy.weather.kalshi_markets")


# Regex for Kalshi weather tickers.
_TICKER_RE = re.compile(
    r"^KX(HIGH|LOW)([A-Z]{2,5})-(\d{2})([A-Z]{3})(\d{2})-T(\d+)$",
    re.IGNORECASE,
)

_MONTH_NAMES = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Map Kalshi 3-5 letter city codes → canonical city names used by the rest
# of the bot. Add entries as Kalshi lists new cities.
_KALSHI_CITY_CODES: dict[str, str] = {
    "ATL": "Atlanta",
    "NYC": "NYC",
    "CHI": "Chicago",
    "DAL": "Dallas",
    "DEN": "Denver",
    "HOU": "Houston",
    "LAX": "Los Angeles",
    "MIA": "Miami",
    "SFO": "San Francisco",
    "SEA": "Seattle",
    "AUS": "Austin",
    "TOR": "Toronto",
    "BOS": "Boston",
    "PHX": "Phoenix",
    "LAS": "Las Vegas",
    "MSP": "Minneapolis",
    "STL": "St. Louis",
    "DFW": "Dallas",    # alternate Dallas code
    "ORD": "Chicago",   # alternate Chicago code
    "JFK": "NYC",       # alternate NYC code
}


@dataclass
class KalshiWeatherMarket:
    """One active Kalshi temperature binary market."""
    ticker: str              # e.g. "KXHIGHATL-25MAY14-T68"
    title: str               # human-readable title from API
    city: str                # canonical city name
    city_code: str           # raw code from ticker (e.g. "ATL")
    metric: str              # "highest" | "lowest"
    target_date: date
    threshold_f: float       # threshold temperature in °F
    yes_price: float         # [0, 1] converted from Kalshi cents
    no_price: float          # [0, 1]
    end_date_utc: datetime
    volume_usd: float        # total traded volume in USD
    status: str              # "open" | "closed" | "finalized"


def _parse_ticker(ticker: str) -> tuple[str, str, date, float] | None:
    """Parse a Kalshi weather ticker into (metric, city_code, target_date, threshold_f).

    Returns None if the ticker doesn't match the expected pattern.
    """
    m = _TICKER_RE.match(ticker)
    if not m:
        return None
    metric_raw, city_code, day_str, month_str, year_str, thresh_str = m.groups()
    metric = "highest" if metric_raw.upper() == "HIGH" else "lowest"
    month = _MONTH_NAMES.get(month_str.upper())
    if month is None:
        return None
    try:
        year = 2000 + int(year_str)
        day = int(day_str)
        target_date = date(year, month, day)
    except ValueError:
        return None
    threshold_f = float(thresh_str)
    return metric, city_code.upper(), target_date, threshold_f


class KalshiMarketScanner:
    """Discover and parse active Kalshi weather temperature markets."""

    def __init__(
        self,
        client: "KalshiClient",
        *,
        request_timeout_sec: float = 15.0,
    ) -> None:
        self._client = client
        self._timeout = request_timeout_sec

    async def fetch_active_weather(
        self,
        *,
        tradeable_cities: set[str] | None = None,
    ) -> list[KalshiWeatherMarket]:
        """Fetch all open Kalshi weather markets and parse ticker metadata.

        `tradeable_cities` filters to only cities the strategy is configured
        to trade. Pass None to return all recognized cities.
        """
        if not self._client.is_initialized:
            log.warning("KalshiMarketScanner: client not initialized — returning empty list")
            return []

        raw_markets = await self._paginate_markets()
        results: list[KalshiWeatherMarket] = []
        unknown_codes: set[str] = set()

        for raw in raw_markets:
            ticker = raw.get("ticker") or ""
            parsed = _parse_ticker(ticker)
            if parsed is None:
                continue

            metric, city_code, target_date, threshold_f = parsed
            city = _KALSHI_CITY_CODES.get(city_code)
            if city is None:
                if city_code not in unknown_codes:
                    log.debug(
                        "KalshiMarketScanner: unknown city code %r in ticker %r — skipping",
                        city_code, ticker,
                    )
                    unknown_codes.add(city_code)
                continue

            if tradeable_cities is not None and city not in tradeable_cities:
                continue

            yes_price_cents = raw.get("yes_ask") or raw.get("last_price") or 50
            no_price_cents = raw.get("no_ask") or (100 - yes_price_cents)
            yes_price = self._client.cents_to_float(yes_price_cents)
            no_price = self._client.cents_to_float(no_price_cents)

            # Parse close/end time.
            end_ts = raw.get("close_time") or raw.get("expiration_time") or ""
            try:
                end_dt = datetime.fromisoformat(end_ts.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                # Approximate: midnight UTC on target_date + 1 day.
                end_dt = datetime(
                    target_date.year, target_date.month, target_date.day,
                    23, 59, 0, tzinfo=timezone.utc,
                )

            volume = float(raw.get("volume") or raw.get("volume_24h") or 0.0)
            title = raw.get("title") or ticker
            status = str(raw.get("status") or "open").lower()

            results.append(KalshiWeatherMarket(
                ticker=ticker,
                title=title,
                city=city,
                city_code=city_code,
                metric=metric,
                target_date=target_date,
                threshold_f=threshold_f,
                yes_price=yes_price,
                no_price=no_price,
                end_date_utc=end_dt,
                volume_usd=volume,
                status=status,
            ))

        if unknown_codes:
            log.info(
                "KalshiMarketScanner: skipped %d market(s) with unknown city codes: %s",
                len(unknown_codes), sorted(unknown_codes),
            )

        log.debug(
            "KalshiMarketScanner: fetched %d weather markets (%d after city filter)",
            len(raw_markets), len(results),
        )
        return results

    async def _paginate_markets(self) -> list[dict[str, Any]]:
        """Paginate through all open Kalshi markets, collecting weather tickers."""
        all_markets: list[dict[str, Any]] = []
        cursor = ""
        while True:
            try:
                resp = await self._client.get_markets(status="open", cursor=cursor, limit=200)
            except Exception:
                log.warning("KalshiMarketScanner: failed to fetch markets page", exc_info=True)
                break

            page = resp.get("markets") or []
            for raw in page:
                ticker = raw.get("ticker") or ""
                if _TICKER_RE.match(ticker):
                    all_markets.append(raw)

            next_cursor = resp.get("cursor") or ""
            if not next_cursor or not page:
                break
            cursor = next_cursor

        return all_markets
