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
    "NY": "NYC",
    # Newer Kalshi temperature tickers often prefix the venue/city code with T.
    "TATL": "Atlanta",
    "TAUS": "Austin",
    "TBOS": "Boston",
    "TCHI": "Chicago",
    "TDAL": "Dallas",
    "TDEN": "Denver",
    "THOU": "Houston",
    "TLAX": "Los Angeles",
    "TLV": "Las Vegas",
    "TMIA": "Miami",
    "TMIN": "Minneapolis",
    "TNOLA": "New Orleans",
    "TNYC": "NYC",
    "TOKC": "Oklahoma City",
    "TPHIL": "Philadelphia",
    "TPHX": "Phoenix",
    "TSATX": "San Antonio",
    "TSEA": "Seattle",
    "TSFO": "San Francisco",
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
    threshold_f: float       # threshold temperature in °F (lower bound for ranges)
    yes_price: float         # [0, 1] — book MIDPOINT when two-sided, else last trade
    no_price: float          # [0, 1]
    end_date_utc: datetime
    volume_usd: float        # total traded volume in USD
    status: str              # "open" | "closed" | "finalized"
    # Book quality (Fix A): a genuine two-sided book is required to trust the
    # price as consensus. `spread is None` means we only had a one-sided book
    # or a stale last trade — no real price discovery, so liquidity filters
    # should reject it.
    yes_bid: float | None = None
    yes_ask: float | None = None
    spread: float | None = None
    # Direction of the YES outcome relative to threshold (Fix B). Kalshi
    # encodes this in the QUESTION (">76°", "<69°", "70° to 72°"), NOT the
    # ticker, so it must be parsed from the title.
    side: str = "above"      # "above" | "below" | "range"
    threshold_high: float | None = None   # upper bound for range markets


def _parse_ticker(ticker: str) -> tuple[str, str, date, float] | None:
    """Parse a Kalshi weather ticker into (metric, city_code, target_date, threshold_f).

    Returns None if the ticker doesn't match the expected pattern.
    """
    m = _TICKER_RE.match(ticker)
    if not m:
        return None
    # Kalshi dates the event YYMONDD (e.g. "26JUN16" = 2026-06-16), so the
    # first numeric group is the 2-digit year and the last is the day.
    metric_raw, city_code, year_str, month_str, day_str, thresh_str = m.groups()
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


# Range questions: "70° to 72°", "70-72°", "between 70 and 72".
_RANGE_RE = re.compile(
    r"(\d+)\s*°?\s*(?:to|and|-|–|—)\s*(\d+)\s*°",
    re.IGNORECASE,
)


def _parse_question_side(
    question: str, threshold_f: float
) -> tuple[str, float, float | None]:
    """Determine the YES outcome's direction from a Kalshi question.

    Kalshi temperature tickers carry only the threshold number (``T76``); the
    comparison direction lives in the human question — ``">76°"``, ``"<69°"``,
    or a range like ``"70° to 72°"``. Treating every market as ``P(temp >= X)``
    (the old behavior) inverts the edge for every "below" and range market.

    Returns ``(side, low, high)`` where ``side`` is ``"above" | "below" |
    "range"``. For above/below, ``low`` is the threshold and ``high`` is None.
    For ranges, ``low``/``high`` are the inclusive bounds.
    """
    q = question or ""
    rng = _RANGE_RE.search(q)
    if rng:
        a, b = float(rng.group(1)), float(rng.group(2))
        return "range", min(a, b), max(a, b)
    if "<" in q or "≤" in q or re.search(r"\b(?:or below|or lower|or less|under|below)\b", q, re.IGNORECASE):
        return "below", threshold_f, None
    # Default to the canonical ">=" framing when only ">"/"above" or nothing
    # explicit is present — Kalshi's binary temperature markets are ">=" unless
    # the question says otherwise.
    return "above", threshold_f, None


def _parse_price_dollars(*values: Any) -> float | None:
    """Return the first value parseable as a price strictly inside (0, 1).

    Kalshi's market schema (as of the 2026-06 API change) reports prices as
    decimal-dollar STRINGS under ``*_dollars`` keys — e.g. ``"0.0100"`` is
    $0.01, not 1. The 0.00 / 1.00 endpoints are the sentinels Kalshi returns
    when a side has no resting offer, so they're treated as "no price" and
    skipped. Returns None when nothing usable is found.
    """
    for v in values:
        if v is None or v == "":
            continue
        try:
            p = float(v)
        except (TypeError, ValueError):
            continue
        if 0.0 < p < 1.0:
            return p
    return None


def _first_float(*values: Any, default: float = 0.0) -> float:
    """First parseable float among values (handles Kalshi's ``_fp`` strings)."""
    for v in values:
        if v is None or v == "":
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return default


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

        raw_markets = await self._fetch_weather_markets(tradeable_cities=tradeable_cities)
        results: list[KalshiWeatherMarket] = []
        unknown_codes: set[str] = set()
        skipped_no_price = 0

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

            # Consensus price = the book MIDPOINT, not the ask.
            #
            # Buying the ask of a wide/illiquid book guarantees an instant
            # markdown: the journal showed dozens of $0.75 entries on
            # zero-volume tail markets (both ">76°" and "<69°" of the same
            # day quoted at $0.75) that the stop-loss then exited at a ~50%
            # loss. $0.75 was a one-sided market-maker ask, not a crowd
            # belief. We now require a genuine two-sided book and price at the
            # midpoint; one-sided books fall back to last trade with
            # `spread=None` so the strategy's liquidity filter rejects them.
            #
            # Kalshi's 2026-06 schema reports decimal-dollar strings under
            # *_dollars keys; the legacy integer-cent fields are a fallback.
            yes_bid = _parse_price_dollars(raw.get("yes_bid_dollars"))
            if yes_bid is None:
                legacy_bid = raw.get("yes_bid")
                yes_bid = self._client.cents_to_float(legacy_bid) if legacy_bid else None
            yes_ask = _parse_price_dollars(raw.get("yes_ask_dollars"))
            if yes_ask is None:
                legacy_ask = raw.get("yes_ask")
                yes_ask = self._client.cents_to_float(legacy_ask) if legacy_ask else None

            if yes_bid is not None and yes_ask is not None and yes_ask >= yes_bid:
                yes_price = round((yes_bid + yes_ask) / 2.0, 4)
                spread = round(yes_ask - yes_bid, 4)
            else:
                # One-sided or absent book — fall back to last trade and flag
                # the missing spread so downstream filters can drop it.
                yes_price = _parse_price_dollars(raw.get("last_price_dollars"))
                if yes_price is None:
                    legacy = raw.get("last_price")
                    yes_price = self._client.cents_to_float(legacy) if legacy else None
                spread = None

            if yes_price is None:
                # No usable quote (empty book / unpriced market). Skip rather
                # than inventing a 0.50 midpoint — that silently fed every
                # market into the strategy as a coinflip.
                skipped_no_price += 1
                continue
            no_price = round(1.0 - yes_price, 4)

            # Direction of the YES outcome (Fix B) — parsed from the question.
            title = raw.get("title") or ticker
            side, thresh_low, thresh_high = _parse_question_side(title, threshold_f)

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

            volume = _first_float(
                raw.get("volume_fp"), raw.get("volume_24h_fp"),
                raw.get("volume"), raw.get("volume_24h"),
            )
            status = str(raw.get("status") or "open").lower()

            results.append(KalshiWeatherMarket(
                ticker=ticker,
                title=title,
                city=city,
                city_code=city_code,
                metric=metric,
                target_date=target_date,
                threshold_f=thresh_low,
                yes_price=yes_price,
                no_price=no_price,
                end_date_utc=end_dt,
                volume_usd=volume,
                status=status,
                yes_bid=yes_bid,
                yes_ask=yes_ask,
                spread=spread,
                side=side,
                threshold_high=thresh_high,
            ))

        if unknown_codes:
            log.info(
                "KalshiMarketScanner: skipped %d market(s) with unknown city codes: %s",
                len(unknown_codes), sorted(unknown_codes),
            )
        if skipped_no_price:
            log.warning(
                "KalshiMarketScanner: skipped %d market(s) with no usable price "
                "(empty book or unexpected schema) out of %d parsed",
                skipped_no_price, len(raw_markets),
            )

        log.debug(
            "KalshiMarketScanner: fetched %d weather markets (%d after city filter)",
            len(raw_markets), len(results),
        )
        return results

    async def _fetch_weather_markets(
        self,
        *,
        tradeable_cities: set[str] | None,
    ) -> list[dict[str, Any]]:
        """Fetch open weather markets by series ticker instead of broad pagination."""
        all_markets: list[dict[str, Any]] = []
        seen: set[str] = set()
        for series_ticker in self._series_tickers(tradeable_cities):
            cursor = ""
            while True:
                try:
                    resp = await self._client.get_markets(
                        status="open",
                        cursor=cursor,
                        limit=200,
                        series_ticker=series_ticker,
                    )
                except Exception:
                    log.debug(
                        "KalshiMarketScanner: failed to fetch series %s",
                        series_ticker,
                        exc_info=True,
                    )
                    break

                page = resp.get("markets") or []
                for raw in page:
                    ticker = raw.get("ticker") or ""
                    if ticker in seen or not _TICKER_RE.match(ticker):
                        continue
                    seen.add(ticker)
                    all_markets.append(raw)

                next_cursor = resp.get("cursor") or ""
                if not next_cursor or not page:
                    break
                cursor = next_cursor

        return all_markets

    @staticmethod
    def _series_tickers(tradeable_cities: set[str] | None) -> list[str]:
        """Return likely Kalshi high/low temperature series tickers."""
        city_codes = sorted({
            code for code, city in _KALSHI_CITY_CODES.items()
            if tradeable_cities is None or city in tradeable_cities
        })
        return [
            f"KX{metric}{code}"
            for code in city_codes
            for metric in ("HIGH", "LOW")
        ]
