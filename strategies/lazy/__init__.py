"""lazy — buy temperature buckets at YES >= entry threshold; pyramid as
price climbs through configured ladder rungs; hold to resolution.

Replaces the lazy_50 / lazy_60 / lazy_70 split. The proven entry floor
from lifetime data is $0.60: below that, win rate drops below the
breakeven needed for the price (1W/5L at $0.50–0.55 in the journal),
and at $0.55–0.60 the strategy is barely break-even. Above $0.60 the
journal shows ~94%+ win rate.

Ladder semantics: each (market_id, bucket_label) fires each threshold
at most once. If a bucket prints above the highest rung on first sight,
all rungs fire on the same scan — the risk module then sizes each
add via Kelly, with the higher-priced rungs naturally smaller in
absolute payoff but larger in Kelly fraction (since edge / (1 - p)
grows as price rises). The aggregate position is capped by the
per-strategy bankroll and the `max_position_usd` global.

Requires `risk.dedup_open_positions = false` for this strategy (or a
per-strategy override) so adds beyond the first don't get rejected as
duplicate-on-market_id.
"""
from strategies.lazy_weather.strategy import LazyWeatherStrategy


class LazyStrategy(LazyWeatherStrategy):
    name = "lazy"
