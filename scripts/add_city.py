"""Interactive helper for adding a city to the resolution registry.

Prompts for the fields the registry needs, then prints a Python snippet
the operator can paste into `strategies/weather/resolution._REGISTRY`.

The script does NOT mutate the registry file directly — too easy to
clobber adjacent entries. Print + paste keeps the operator in the loop.

Usage:
    python -m scripts.add_city
"""
from __future__ import annotations

import sys
from textwrap import dedent


def _ask(prompt: str, *, default: str | None = None, allow_empty: bool = False) -> str:
    suffix = f" [{default}]" if default is not None else ""
    while True:
        v = input(f"{prompt}{suffix}: ").strip()
        if not v and default is not None:
            return default
        if v or allow_empty:
            return v
        print("  (required)")


def _ask_float(prompt: str) -> float:
    while True:
        try:
            return float(_ask(prompt))
        except ValueError:
            print("  (must be a number)")


def _ask_choice(prompt: str, choices: list[str], *, default: str) -> str:
    rendered = "/".join(c if c != default else c.upper() for c in choices)
    while True:
        v = _ask(f"{prompt} ({rendered})", default=default).lower()
        if v in choices:
            return v
        print(f"  (must be one of: {', '.join(choices)})")


def main() -> int:
    print("Adding a new resolution source for the weather strategy.")
    print("(Ctrl-C to abort.)\n")
    try:
        city = _ask("Canonical city name (e.g. 'NYC', 'Hong Kong')")
        station_name = _ask("Station name (human readable, e.g. 'LaGuardia Airport')")
        station_id = _ask("Station ID (ICAO/airport/HKO/etc.)", allow_empty=True)
        provider = _ask_choice(
            "Data provider",
            ["wunderground", "noaa", "jma", "kma", "hko", "metservice", "cma"],
            default="wunderground",
        )
        lat = _ask_float("Latitude (decimal)")
        lon = _ask_float("Longitude (decimal)")
        unit = _ask_choice("Reporting unit", ["fahrenheit", "celsius"], default="celsius")
        confirmed = _ask_choice(
            "Have you personally verified this against a recent resolution?",
            ["yes", "no"], default="no",
        ) == "yes"
        notes = _ask("Notes (optional)", allow_empty=True)
    except (EOFError, KeyboardInterrupt):
        print("\naborted")
        return 1

    snippet = dedent(f'''
        "{city}": ResolutionSource(
            city="{city}",
            station_name="{station_name}",
            station_id="{station_id}",
            data_provider="{provider}",
            lat={lat}, lon={lon},
            unit="{unit}",
            confirmed={confirmed},
            notes={"" if not notes else f'"{notes}"'},
        ),
    ''').strip()

    print()
    print("=" * 72)
    print("Paste the following into strategies/weather/resolution.py inside _REGISTRY:")
    print("=" * 72)
    print(snippet)
    print("=" * 72)
    print()
    print(f"Then add {city!r} to `cities:` in config/config.yaml.")
    if not confirmed:
        print("Heads up: confirmed=False — this city will be skipped at startup")
        print("until you flip it to True after verifying one resolution.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
