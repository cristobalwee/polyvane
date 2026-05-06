"""Lightweight Gamma scanner for lazy weather strategy.

Unlike `strategies.weather.markets.GammaClient`, this scanner:
  * Trades ALL cities (no resolution-registry filter), since lazy doesn't
    need to know the resolution station — it trusts Polymarket to mark
    won/lost from the official resolution.
  * Returns a flat list of buckets with just the fields the lazy strategy
    needs: bucket label, YES price, token_id, end_utc, liquidity, volume.

Reuses the bucket-parsing regex helpers from `strategies.weather.markets`
so we don't fork the question-shape vocabulary.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aiohttp

from strategies.weather.markets import (
    _maybe_json,
    _parse_bucket,
    _parse_dt,
    _yes_side,
)


log = logging.getLogger("strategy.lazy_weather.scanner")


GAMMA_BASE = "https://gamma-api.polymarket.com"

# Same event-title shape the weather scanner uses, but we don't require the
# city to be in any registry — we keep whatever the title says.
_EVENT_TITLE_RX = re.compile(
    r"(?P<metric>highest|lowest)\s+temperature\s+in\s+(?P<city>[A-Za-z .]+?)\s+on\s+",
    re.IGNORECASE,
)


@dataclass
class LazyMarket:
    """One Polymarket temperature bucket, stripped to what lazy needs."""
    market_id: str
    question: str
    city: str            # raw city from event title (no normalization)
    bucket_label: str
    bucket_low: float
    bucket_high: float
    unit: str
    yes_price: float
    yes_token_id: str | None
    end_date_utc: datetime
    liquidity_usd: float
    volume_usd: float
    metric: str          # "highest" | "lowest"


class LazyGammaScanner:
    """Public Gamma `/events?tag_slug=...` reader. No auth, no city filter."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        request_timeout_sec: float = 15.0,
    ) -> None:
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=request_timeout_sec)

    async def fetch_active(self, *, limit: int = 200) -> list[LazyMarket]:
        seen: dict[str, dict[str, Any]] = {}
        for tag in ("weather", "temperature"):
            params = {
                "tag_slug": tag,
                "active": "true",
                "closed": "false",
                "limit": str(limit),
            }
            for ev in (await self._get(f"{GAMMA_BASE}/events", params=params)) or []:
                eid = str(ev.get("id") or ev.get("slug") or "")
                if eid and eid not in seen:
                    seen[eid] = ev

        out: list[LazyMarket] = []
        for ev in seen.values():
            out.extend(self._parse_event(ev))
        return out

    async def _get(self, url: str, *, params: dict[str, str] | None = None) -> list[dict[str, Any]] | None:
        try:
            async with self._session.get(url, params=params, timeout=self._timeout) as resp:
                if resp.status >= 400:
                    body = (await resp.text())[:200]
                    log.warning("Gamma %s -> HTTP %d: %s", url, resp.status, body)
                    return None
                data = await resp.json()
                if isinstance(data, dict) and "data" in data:
                    return data["data"]
                if isinstance(data, list):
                    return data
                return None
        except (aiohttp.ClientError, json.JSONDecodeError) as e:
            log.warning("Gamma fetch failed: %s", e)
            return None

    def _parse_event(self, ev: dict[str, Any]) -> list[LazyMarket]:
        title = str(ev.get("title") or "")
        m = _EVENT_TITLE_RX.search(title)
        if not m:
            return []
        metric = m["metric"].lower()
        city = m["city"].strip()

        end_iso = ev.get("endDate") or ev.get("endDateIso")
        end_dt = _parse_dt(end_iso) if end_iso else None
        if end_dt is None:
            return []

        out: list[LazyMarket] = []
        for raw in ev.get("markets") or []:
            bucket = _parse_bucket(str(raw.get("question") or ""))
            if bucket is None:
                continue
            lo, hi, label, unit = bucket
            outcomes = _maybe_json(raw.get("outcomes"))
            prices = _maybe_json(raw.get("outcomePrices"))
            token_ids = _maybe_json(raw.get("clobTokenIds"))
            yes_price, yes_token = _yes_side(outcomes, prices, token_ids)
            if yes_price is None:
                continue
            out.append(LazyMarket(
                market_id=str(raw.get("id") or raw.get("conditionId") or raw.get("slug")),
                question=str(raw.get("question") or ""),
                city=city,
                bucket_label=label,
                bucket_low=lo,
                bucket_high=hi,
                unit=unit,
                yes_price=yes_price,
                yes_token_id=yes_token,
                end_date_utc=end_dt,
                liquidity_usd=float(raw.get("liquidityNum") or raw.get("liquidity") or 0.0),
                volume_usd=float(
                    raw.get("volumeNum")
                    or raw.get("volume")
                    or raw.get("volumeClob")
                    or 0.0
                ),
                metric=metric,
            ))
        return out
