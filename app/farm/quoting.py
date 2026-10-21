from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal

from app.farm.schemas import Market
from app.types import QuoteDepth, TimeRemainingFilter


def size_per_market(market: Market) -> Decimal:
    """Order size in shares: the larger of exchange and reward min."""
    return max(market.min_order_size, market.rewards_min_size)


def resolve_tier(
    size_tiers: list,
    size: Decimal,
    fallback_depth: QuoteDepth,
    fallback_reward_min: Decimal,
    fallback_liq_min: Decimal,
    fallback_zone_liq_max: Decimal | None,
    fallback_time_remaining: TimeRemainingFilter,
) -> tuple[QuoteDepth, Decimal, Decimal, Decimal | None, TimeRemainingFilter]:
    """(quote_depth, reward_min, liq_min, zone_liq_max, time_remaining) for a market of `size`
    shares. The market falls in the first tier whose max_shares >= its size (max_shares=None is the
    catch-all, sorted last). Empty tiers -> the global fallbacks (legacy behavior). zone_liq_max may
    be None (off)."""
    if not size_tiers:
        return (
            fallback_depth,
            fallback_reward_min,
            fallback_liq_min,
            fallback_zone_liq_max,
            fallback_time_remaining,
        )
    ordered = sorted(size_tiers, key=lambda t: (t.max_shares is None, t.max_shares or Decimal(0)))
    for tier in ordered:
        if tier.max_shares is None or size <= tier.max_shares:
            return (
                tier.quote_depth,
                tier.reward_min,
                tier.liq_min,
                tier.zone_liq_max,
                tier.time_remaining,
            )
    return (
        fallback_depth,
        fallback_reward_min,
        fallback_liq_min,
        fallback_zone_liq_max,
        fallback_time_remaining,
    )


def tiers_log_repr(size_tiers: list) -> str:
    """Compact one-line repr of the size tiers for the farm_config log — one tier per '|' group as
    `max_shares:quote_depth:reward_min:liq_min:zone:time_remaining` ('inf' for the catch-all, 'off'
    when a tier's zone cap is None); 'none' when there are no tiers."""
    return (
        "|".join(
            f"{'inf' if t.max_shares is None else t.max_shares}:{t.quote_depth}:{t.reward_min}"
            f":{t.liq_min}:{'off' if t.zone_liq_max is None else t.zone_liq_max}:{t.time_remaining}"
            for t in size_tiers
        )
        or "none"
    )


def two_leg_cost(
    market: Market, yes_mid: Decimal, no_mid: Decimal, quote_depth: QuoteDepth = "safe"
) -> Decimal | None:
    """USDC committed to quote BOTH legs at min size: size × (yes_bid + no_bid), using actual
    quoted bids (negRisk/multi-outcome bids can sum well above $1). Returns None if either leg is
    unquoteable, so the caller skips the market instead of placing one leg and getting bounced."""
    yes_quote = compute_quote(market, yes_mid, quote_depth)
    no_quote = compute_quote(market, no_mid, quote_depth)
    if yes_quote is None or no_quote is None:
        return None
    return size_per_market(market) * (yes_quote[0] + no_quote[0])


def quote_distance(quote_depth: QuoteDepth, max_spread_price: Decimal, tick: Decimal) -> Decimal:
    """Distance from the midpoint to place each leg, per the chosen depth: safe = far edge
    (max_spread - 1 tick), aggressive = 1 tick from mid, normal = ~half the max spread. All clamped
    to (0, edge] so the order still rests strictly inside the zone."""
    edge = max_spread_price - tick
    if edge <= 0:
        return edge
    if quote_depth == "aggressive":
        return min(tick, edge)
    if quote_depth == "normal":
        half = round_to_tick(max_spread_price / 2, tick)
        return min(max(half, tick), edge)
    return edge


def compute_quote(
    market: Market, midpoint: Decimal, quote_depth: QuoteDepth = "safe"
) -> tuple[Decimal, Decimal] | None:
    """Compute (bid_price, ask_price) at the quote_depth distance from the midpoint, strictly inside
    rewards.max_spread so the order still scores (>0) for rewards. Returns None if there's no room
    for a valid quote (degenerate market or midpoint at the boundary)."""
    if midpoint <= 0 or midpoint >= 1:
        return None

    max_spread_price = market.rewards_max_spread_cents / Decimal(100)

    distance = quote_distance(quote_depth, max_spread_price, market.tick_size)
    if distance <= 0:
        return None

    bid = ceil_to_tick(midpoint - distance, market.tick_size)
    ask = floor_to_tick(midpoint + distance, market.tick_size)

    if bid <= 0 or ask >= 1:
        return None
    if bid >= midpoint or ask <= midpoint:
        return None
    if bid >= ask:
        return None

    return bid, ask


def ceil_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    """Round price UP to the nearest valid tick multiple."""
    return (price / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def compute_inventory_quote(
    best_bid: Decimal,
    best_ask: Decimal,
    tick: Decimal,
) -> tuple[Decimal, Decimal] | None:
    """Exact BBA offsets; reject crossed/self-crossing quotes rather than changing the rule."""
    if not all(value.is_finite() for value in (best_bid, best_ask, tick)):
        return None
    if not Decimal(0) < best_bid < best_ask < Decimal(1) or tick <= 0:
        return None
    if best_bid % tick or best_ask % tick:
        return None
    bid = best_ask - 2 * tick
    ask = best_bid + tick
    if not Decimal(0) < bid < ask < Decimal(1) or bid > Decimal("0.05"):
        return None
    return bid, ask


def floor_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    """Round price DOWN to the nearest valid tick multiple."""
    return (price / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def round_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    """Round price to the NEAREST valid tick multiple (half rounds up)."""
    return (price / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick
