"""Extreme Price Reward Farming: a standalone, side-effect-free order planner.

Call build_entry_plan with fresh YES/NO midpoints and the cheap token's best
ask. Submit entry_order through the execution client, then call build_quotes
only with confirmed, available inventory after entry is filled/cancelled.
Submit the returned SELL then BUY as post-only orders. The caller owns order
IDs, fill reconciliation, balance/fee reserves, cancellation and retries; do
not repeatedly submit a plan while its orders are already live.

Prices are dollars (0.045 = 4.5 cents). Reward rate is the MARKET's daily pool,
not expected personal earnings. Both quotes must remain within the live
size-adjusted reward spread to score at extreme prices:
https://docs.polymarket.com/programs/liquidity-rewards
The worker uses this planner through extreme_worker; this module itself sends no orders.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_FLOOR, Decimal

from pydantic import BaseModel, ConfigDict

from app.bot.schemas import LimitOrder
from app.farm.quoting import compute_inventory_quote
from app.farm.schemas import Market
from app.types import PositiveDecimal

STRATEGY_NAME = "Extreme Price Reward Farming"
D = Decimal


class ExtremePriceRewardConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    min_reward_per_day: PositiveDecimal  # Caller defines what counts as high reward.
    capital_budget: PositiveDecimal = D("85")  # Entry + pending rebuy, excluding fees.
    target_shares: PositiveDecimal = D("1000")  # Legacy input; cash allocation controls size.
    # Legacy compatibility only; live quotes use fixed tick offsets.
    sell_markup: PositiveDecimal = D("0.003")
    rebuy_discount: PositiveDecimal = D("0.005")


@dataclass(frozen=True)
class EntryPlan:
    token_id: str
    outcome: str
    entry_price: Decimal
    sell_price: Decimal
    rebuy_price: Decimal
    shares: Decimal
    minimum_shares: Decimal
    rebuy_budget: Decimal = D(0)

    @property
    def capital_required(self) -> Decimal:
        return self.shares * self.entry_price + self.rebuy_budget

    @property
    def entry_order(self) -> LimitOrder:
        return _order(self.token_id, "BUY", self.entry_price, self.shares)


def _order(token_id: str, side: str, price: Decimal, size: Decimal) -> LimitOrder:
    return LimitOrder(token_id=token_id, side=side, price=float(price), size=float(size))


def _price(value: Decimal) -> bool:
    return value.is_finite() and D("0") < value < D("1")


def build_entry_plan(
    market: Market,
    config: ExtremePriceRewardConfig,
    *,
    yes_midpoint: Decimal,
    no_midpoint: Decimal,
    cheap_best_ask: Decimal,
    cheap_best_bid: Decimal,
    reward_midpoint: Decimal,
    now: datetime | None = None,
) -> EntryPlan | None:
    """Screen one market and budget a cheap-side entry plus one lower rebuy.

    Returns None for ineligible markets. Entry is a price-capped limit order;
    a submitted order is not proof of a fill. Midpoints must be fresh observed
    values for each token, rather than an assumed complement.
    """
    if market.end_date <= (now or datetime.now(timezone.utc)):
        return None
    if market.rewards_rate_per_day < config.min_reward_per_day:
        return None
    if not all(_price(p) for p in (yes_midpoint, no_midpoint, cheap_best_ask)):
        return None
    if yes_midpoint <= D("0.05") and no_midpoint >= D("0.95"):
        token, outcome = market.yes_token_id, "YES"
    elif no_midpoint <= D("0.05") and yes_midpoint >= D("0.95"):
        token, outcome = market.no_token_id, "NO"
    else:
        return None

    quotes = compute_inventory_quote(cheap_best_bid, cheap_best_ask, market.tick_size)
    if quotes is None or cheap_best_ask > D("0.05") or not _price(reward_midpoint):
        return None
    rebuy, sell = quotes
    spread = market.rewards_max_spread_cents / D(100)
    # Validate the future reward quotes BEFORE spending any money on entry.
    if max(abs(rebuy - reward_midpoint), abs(sell - reward_midpoint)) >= spread:
        return None
    entry = cheap_best_ask
    minimum = max(market.min_order_size, market.rewards_min_size)
    # Split principal equally in dollars, not in shares. The cheaper bid buys more shares.
    half = config.capital_budget / 2
    shares = (half / entry).to_integral_value(rounding=ROUND_FLOOR)
    rebuy_shares = (half / rebuy).to_integral_value(rounding=ROUND_FLOOR)
    if min(shares, rebuy_shares) < minimum:
        return None
    return EntryPlan(token, outcome, entry, sell, rebuy, shares, minimum, half)


def build_quotes(
    plan: EntryPlan,
    *,
    available_shares: Decimal,
    available_cash: Decimal,
    reward_midpoint: Decimal,
    max_spread_cents: Decimal,
) -> tuple[LimitOrder, LimitOrder] | None:
    """Return (SELL, BUY) for confirmed unreserved inventory and free cash.

    Cash must exclude open-order commitments and fee reserves. Supply the live
    size-adjusted midpoint for the selected token and current reward spread.
    Partial fills produce smaller quotes; insufficient reward size waits.
    This is a quote plan, not an exchange eligibility or earnings guarantee.
    """
    values = (available_shares, available_cash, max_spread_cents)
    if any(not v.is_finite() or v < 0 for v in values) or not _price(reward_midpoint):
        return None
    spread = max_spread_cents / D("100")
    if not plan.rebuy_price < plan.sell_price:
        return None
    if (
        max(abs(plan.sell_price - reward_midpoint), abs(reward_midpoint - plan.rebuy_price))
        >= spread
    ):
        return None
    sell_size = min(plan.shares, available_shares).to_integral_value(rounding=ROUND_FLOOR)
    buy_size = (min(plan.rebuy_budget, available_cash) / plan.rebuy_price).to_integral_value(
        rounding=ROUND_FLOOR
    )
    if min(sell_size, buy_size) < plan.minimum_shares:
        return None
    return (
        _order(plan.token_id, "SELL", plan.sell_price, sell_size),
        _order(plan.token_id, "BUY", plan.rebuy_price, buy_size),
    )
