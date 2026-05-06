"""Cross-check the resolution registry against Polymarket's active markets.

Pulls every active weather/temperature event from Gamma, normalizes the
city names with the same parser the live strategy uses, and reports:

  - cities in the registry that are NOT currently appearing on Polymarket
    (could mean the city's markets are seasonal, or the alias rules
    have drifted out of sync with the event titles)

  - cities Polymarket is currently posting that the registry doesn't
    cover (new market we haven't catalogued — needs `add-city`)

  - cities marked confirmed=False but appearing on Polymarket (operator
    should verify a recent resolution and flip the flag)

Usage:
    python -m scripts.verify_sources
"""
from __future__ import annotations

import asyncio
import sys

import aiohttp

from strategies.weather import resolution
from strategies.weather.markets import GammaClient


async def _amain() -> int:
    seen_unknown: list[str] = []

    def _on_unknown(raw_city: str) -> None:
        seen_unknown.append(raw_city)

    async with aiohttp.ClientSession() as session:
        gamma = GammaClient(session, on_unknown_city=_on_unknown)
        # Pass an empty allow-set so every recognized city falls into
        # the "known city but skipped" branch — the discovery side-effect
        # we want is just `_on_unknown_city` firing for novel cities.
        # Then we re-collect the raw event titles to extract recognized
        # cities for the appearance set.
        events = await gamma._get(  # type: ignore[attr-defined]
            "https://gamma-api.polymarket.com/events",
            params={"tag_slug": "weather", "active": "true", "closed": "false", "limit": "200"},
        ) or []

    from strategies.weather.markets import _EVENT_TITLE_RX, _normalize_city  # noqa: PLC0415

    appearing_known: set[str] = set()
    appearing_raw_unknown: set[str] = set()
    for ev in events:
        title = str(ev.get("title") or "")
        m = _EVENT_TITLE_RX.search(title)
        if not m:
            continue
        raw = m["city"].strip()
        norm = _normalize_city(raw)
        if norm is None:
            appearing_raw_unknown.add(raw)
        else:
            appearing_known.add(norm)

    registry = set(resolution.all_cities())
    confirmed = set(resolution.confirmed_cities())
    unconfirmed = set(resolution.unconfirmed_cities())

    in_registry_not_seen = sorted(registry - appearing_known)
    seen_not_in_registry = sorted(appearing_raw_unknown)
    seen_but_unconfirmed = sorted(appearing_known & unconfirmed)

    print("=" * 72)
    print(f"Active weather events fetched: {len(events)}")
    print(f"Registry cities:               {len(registry)}  (confirmed={len(confirmed)}, unconfirmed={len(unconfirmed)})")
    print(f"Cities currently on Polymarket: {len(appearing_known)}  (+{len(appearing_raw_unknown)} unknown)")
    print()

    if seen_not_in_registry:
        print("[!] Cities on Polymarket that are NOT in the registry:")
        for raw in seen_not_in_registry:
            print(f"      {raw!r}  →  run `make add-city`")
        print()

    if seen_but_unconfirmed:
        print("[!] Confirmed=False cities currently posting on Polymarket:")
        for c in seen_but_unconfirmed:
            src = resolution.get(c)
            print(f"      {c}  →  station={src.station_id or '(none)'}  notes={src.notes or '-'}")
        print("      Verify one resolution and flip confirmed=True.")
        print()

    if in_registry_not_seen:
        print("[ ] Registry cities NOT currently appearing on Polymarket:")
        for c in in_registry_not_seen:
            print(f"      {c}")
        print("      (Could be seasonal — only flag if you expected to see it.)")
        print()

    print("=" * 72)

    if seen_not_in_registry or seen_but_unconfirmed:
        return 1
    return 0


def main() -> int:
    return asyncio.run(_amain())


if __name__ == "__main__":
    sys.exit(main())
