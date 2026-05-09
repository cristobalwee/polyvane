"""Resolution source registry for the weather strategy.

Each Polymarket temperature market resolves against a specific weather
station reported by a specific data provider. This registry is the single
source of truth for that mapping. The strategy refuses to trade any market
whose city is missing here, or whose entry is marked unconfirmed when
`require_confirmed_resolution` is true.

The registry is intentionally Python (not YAML) so it lives next to the
parsing code that depends on it and is easy to extend with new providers.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable


log = logging.getLogger("strategy.weather.resolution")


# Data providers we know how to fetch forecasts from. 'wunderground' is the
# resolution source for most Polymarket temperature markets but we don't have
# a stable forecast endpoint for it — we use Open-Meteo as the *forecast*
# proxy and rely on Wunderground only for the post-resolution actual.
DataProvider = str  # 'wunderground' | 'noaa' | 'jma' | 'kma' | 'hko' | 'metservice'
Unit = str          # 'celsius' | 'fahrenheit'


@dataclass(frozen=True)
class ResolutionSource:
    city: str
    station_name: str
    station_id: str
    data_provider: DataProvider
    lat: float
    lon: float
    unit: Unit
    # `confirmed=False` means we have not personally verified the station
    # against a recent resolution. The strategy will skip such markets when
    # `require_confirmed_resolution` is enabled.
    confirmed: bool = True
    notes: str = ""

    @property
    def is_us(self) -> bool:
        # NOAA only covers US stations; everything else uses Open-Meteo.
        return self.data_provider == "noaa" or self.station_id.startswith(("K",))


# Cities we have NOT yet personally confirmed a resolution station for. Listed
# here (vs omitted) so the alert + skip-with-warning machinery can flag them,
# AND so the bot doesn't keep firing `new_city_detected` alerts on every scan.
# Best-guess station per row; flip `confirmed=True` after verifying one resolution.
_UNCONFIRMED = {
    "Seoul":      "Source TBD — likely KMA but station unverified",
    "Shanghai":   "Source TBD — likely CMA Pudong/Hongqiao but station unverified",
    "Beijing":    "Source TBD — likely CMA Beijing Capital but station unverified",
    "Tokyo":      "Source TBD — likely JMA Tokyo but station/airport unverified",
    "Singapore":  "Source TBD — likely Changi (WSSS) but station unverified",
    "Mexico City": "Source TBD — likely Benito Juarez (MMMX) but station unverified",
    "Sao Paulo":  "Source TBD — likely Guarulhos (SBGR) or Congonhas (SBSP) — unverified",
    "Buenos Aires": "Source TBD — likely Ezeiza (SAEZ) or Aeroparque (SABE) — unverified",
    "Madrid":     "Source TBD — likely Barajas (LEMD) but unverified",
    "Paris":      "Source TBD — likely Charles de Gaulle (LFPG) or Orly (LFPO) — unverified",
    "Munich":     "Source TBD — likely Franz Josef Strauss (EDDM) but unverified",
    "Amsterdam":  "Source TBD — likely Schiphol (EHAM) but unverified",
    "Helsinki":   "Source TBD — likely Helsinki-Vantaa (EFHK) but unverified",
    "Tel Aviv":   "Source TBD — likely Ben Gurion (LLBG) but unverified",
    "Istanbul":   "Source TBD — IST (LTFM) vs SAW (LTFJ) ambiguous — unverified",
    "Moscow":     "Source TBD — likely Sheremetyevo (UUEE) but unverified",
    "Warsaw":     "Source TBD — likely Chopin (EPWA) but unverified",
    "Milan":      "Source TBD — Malpensa (LIMC) vs Linate (LIML) ambiguous — unverified",
    "Cape Town":  "Source TBD — likely Cape Town International (FACT) but unverified",
    "Manila":     "Source TBD — likely NAIA (RPLL) but unverified",
    "Jakarta":    "Source TBD — likely Soekarno-Hatta (WIII) but unverified",
    "Taipei":     "Source TBD — Songshan (RCSS) vs Taoyuan (RCTP) ambiguous — unverified",
    "Busan":      "Source TBD — likely Gimhae (RKPK) but unverified",
}


_REGISTRY: dict[str, ResolutionSource] = {
    "NYC": ResolutionSource(
        city="NYC",
        station_name="LaGuardia Airport",
        station_id="KLGA",
        data_provider="wunderground",
        lat=40.7729, lon=-73.8740,
        unit="fahrenheit",
        notes="2°F buckets",
    ),
    "London": ResolutionSource(
        city="London",
        station_name="London City Airport",
        station_id="EGLC",
        data_provider="wunderground",
        lat=51.5053, lon=0.0553,
        unit="celsius",
        notes="1°C buckets",
    ),
    "Hong Kong": ResolutionSource(
        city="Hong Kong",
        station_name="Hong Kong Observatory",
        station_id="HKO",
        data_provider="wunderground",
        lat=22.3022, lon=114.1742,
        unit="celsius",
    ),
    "Dallas": ResolutionSource(
        city="Dallas",
        station_name="Dallas Love Field",
        station_id="KDAL",
        data_provider="wunderground",
        lat=32.8471, lon=-96.8518,
        unit="fahrenheit",
    ),
    "Atlanta": ResolutionSource(
        city="Atlanta",
        station_name="Hartsfield-Jackson Atlanta International",
        station_id="KATL",
        data_provider="wunderground",
        lat=33.6407, lon=-84.4277,
        unit="fahrenheit",
    ),
    "Toronto": ResolutionSource(
        city="Toronto",
        station_name="Toronto Pearson International",
        station_id="CYYZ",
        data_provider="wunderground",
        lat=43.6777, lon=-79.6248,
        unit="celsius",
    ),
    "Ankara": ResolutionSource(
        city="Ankara",
        station_name="Esenboga Airport",
        station_id="LTAC",
        data_provider="wunderground",
        lat=40.1281, lon=32.9951,
        unit="celsius",
    ),
    "Wellington": ResolutionSource(
        city="Wellington",
        station_name="Wellington Airport",
        station_id="NZWN",
        data_provider="wunderground",
        lat=-41.3272, lon=174.8053,
        unit="celsius",
    ),
    "Seoul": ResolutionSource(
        city="Seoul",
        station_name="(unconfirmed)",
        station_id="",
        data_provider="wunderground",
        lat=37.5665, lon=126.9780,
        unit="celsius",
        confirmed=False,
        notes=_UNCONFIRMED["Seoul"],
    ),
    "Shanghai": ResolutionSource(
        city="Shanghai",
        station_name="(unconfirmed)",
        station_id="",
        data_provider="wunderground",
        lat=31.2304, lon=121.4737,
        unit="celsius",
        confirmed=False,
        notes=_UNCONFIRMED["Shanghai"],
    ),

    # ---- Major US airports (NWS-covered; Wunderground typically resolves to these) ----
    "Austin": ResolutionSource(
        city="Austin",
        station_name="Austin-Bergstrom International",
        station_id="KAUS",
        data_provider="wunderground",
        lat=30.1945, lon=-97.6699,
        unit="fahrenheit",
    ),
    "Chicago": ResolutionSource(
        city="Chicago",
        station_name="O'Hare International",
        station_id="KORD",
        data_provider="wunderground",
        lat=41.9742, lon=-87.9073,
        unit="fahrenheit",
    ),
    "Denver": ResolutionSource(
        city="Denver",
        station_name="Denver International",
        station_id="KDEN",
        data_provider="wunderground",
        lat=39.8561, lon=-104.6737,
        unit="fahrenheit",
    ),
    "Houston": ResolutionSource(
        city="Houston",
        station_name="George Bush Intercontinental",
        station_id="KIAH",
        data_provider="wunderground",
        lat=29.9902, lon=-95.3368,
        unit="fahrenheit",
    ),
    "Los Angeles": ResolutionSource(
        city="Los Angeles",
        station_name="Los Angeles International",
        station_id="KLAX",
        data_provider="wunderground",
        lat=33.9416, lon=-118.4085,
        unit="fahrenheit",
    ),
    "Miami": ResolutionSource(
        city="Miami",
        station_name="Miami International",
        station_id="KMIA",
        data_provider="wunderground",
        lat=25.7959, lon=-80.2870,
        unit="fahrenheit",
    ),
    "San Francisco": ResolutionSource(
        city="San Francisco",
        station_name="San Francisco International",
        station_id="KSFO",
        data_provider="wunderground",
        lat=37.6213, lon=-122.3790,
        unit="fahrenheit",
    ),
    "Seattle": ResolutionSource(
        city="Seattle",
        station_name="Seattle-Tacoma International",
        station_id="KSEA",
        data_provider="wunderground",
        lat=47.4502, lon=-122.3088,
        unit="fahrenheit",
    ),

    # ---- International — best-guess station, marked unconfirmed until verified ----
    "Beijing": ResolutionSource(
        city="Beijing", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=39.9042, lon=116.4074,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Beijing"],
    ),
    "Tokyo": ResolutionSource(
        city="Tokyo", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=35.6762, lon=139.6503,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Tokyo"],
    ),
    "Singapore": ResolutionSource(
        city="Singapore", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=1.3521, lon=103.8198,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Singapore"],
    ),
    "Mexico City": ResolutionSource(
        city="Mexico City", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=19.4326, lon=-99.1332,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Mexico City"],
    ),
    "Sao Paulo": ResolutionSource(
        city="Sao Paulo", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=-23.5505, lon=-46.6333,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Sao Paulo"],
    ),
    "Buenos Aires": ResolutionSource(
        city="Buenos Aires", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=-34.6037, lon=-58.3816,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Buenos Aires"],
    ),
    "Madrid": ResolutionSource(
        city="Madrid", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=40.4168, lon=-3.7038,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Madrid"],
    ),
    "Paris": ResolutionSource(
        city="Paris", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=48.8566, lon=2.3522,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Paris"],
    ),
    "Munich": ResolutionSource(
        city="Munich", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=48.1351, lon=11.5820,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Munich"],
    ),
    "Amsterdam": ResolutionSource(
        city="Amsterdam", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=52.3676, lon=4.9041,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Amsterdam"],
    ),
    "Helsinki": ResolutionSource(
        city="Helsinki", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=60.1699, lon=24.9384,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Helsinki"],
    ),
    "Tel Aviv": ResolutionSource(
        city="Tel Aviv", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=32.0853, lon=34.7818,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Tel Aviv"],
    ),
    "Istanbul": ResolutionSource(
        city="Istanbul", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=41.0082, lon=28.9784,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Istanbul"],
    ),
    "Moscow": ResolutionSource(
        city="Moscow", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=55.7558, lon=37.6173,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Moscow"],
    ),
    "Warsaw": ResolutionSource(
        city="Warsaw", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=52.2297, lon=21.0122,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Warsaw"],
    ),
    "Milan": ResolutionSource(
        city="Milan", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=45.4642, lon=9.1900,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Milan"],
    ),
    "Cape Town": ResolutionSource(
        city="Cape Town", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=-33.9249, lon=18.4241,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Cape Town"],
    ),
    "Manila": ResolutionSource(
        city="Manila", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=14.5995, lon=120.9842,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Manila"],
    ),
    "Jakarta": ResolutionSource(
        city="Jakarta", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=-6.2088, lon=106.8456,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Jakarta"],
    ),
    "Taipei": ResolutionSource(
        city="Taipei", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=25.0330, lon=121.5654,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Taipei"],
    ),
    "Busan": ResolutionSource(
        city="Busan", station_name="(unconfirmed)", station_id="",
        data_provider="wunderground", lat=35.1796, lon=129.0756,
        unit="celsius", confirmed=False, notes=_UNCONFIRMED["Busan"],
    ),
}


def get(city: str) -> ResolutionSource | None:
    return _REGISTRY.get(city)


def all_cities() -> list[str]:
    return list(_REGISTRY.keys())


def confirmed_cities() -> list[str]:
    return [c for c, r in _REGISTRY.items() if r.confirmed]


def unconfirmed_cities() -> list[str]:
    return [c for c, r in _REGISTRY.items() if not r.confirmed]


def is_known(city: str) -> bool:
    return city in _REGISTRY


def filter_tradeable(
    cities: Iterable[str],
    *,
    require_confirmed: bool,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Split a list of requested cities into (tradeable, skipped_with_reason).

    The strategy uses this at startup and on each scan to decide which
    markets to score. `skipped_with_reason` is what gets logged.
    """
    tradeable: list[str] = []
    skipped: list[tuple[str, str]] = []
    for c in cities:
        src = _REGISTRY.get(c)
        if src is None:
            skipped.append((c, "not in resolution registry"))
            continue
        if require_confirmed and not src.confirmed:
            skipped.append((c, f"unconfirmed resolution source: {src.notes or 'see registry'}"))
            continue
        tradeable.append(c)
    return tradeable, skipped
