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
This module is not wired into the existing farm worker and sends no orders.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from pydantic import BaseModel, ConfigDict

from app.bot.schemas import LimitOrder
from app.farm.schemas import Market
from app.types import PositiveDecimal

STRATEGY_NAME = "Extreme Price Reward Farming"
D = Decimal


class ExtremePriceRewardConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    min_reward_per_day: PositiveDecimal  # Caller defines what counts as high reward.
    capital_budget: PositiveDecimal = D("85")  # Entry + pending rebuy, excluding fees.
    target_shares: PositiveDecimal = D("1000")
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

    @property
    def capital_required(self) -> Decimal:
        return self.shares * (self.entry_price + self.rebuy_price)

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

    tick = market.tick_size
    entry = (cheap_best_ask / tick).to_integral_value(rounding=ROUND_CEILING) * tick
    sell = ((entry + config.sell_markup) / tick).to_integral_value(
        rounding=ROUND_CEILING
    ) * tick
    rebuy = ((entry - config.rebuy_discount) / tick).to_integral_value(
        rounding=ROUND_FLOOR
    ) * tick
    if not D("0") < rebuy < entry <= D("0.05") or not entry < sell < D("1"):
        return None
    minimum = max(market.min_order_size, market.rewards_min_size)
    # Whole shares deliberately round down; both stages fit within the budget.
    shares = min(config.target_shares, config.capital_budget / (entry + rebuy))
    shares = shares.to_integral_value(rounding=ROUND_FLOOR)
    if shares < minimum:
        return None
    return EntryPlan(token, outcome, entry, sell, rebuy, shares, minimum)


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
    if not plan.rebuy_price < reward_midpoint < plan.sell_price:
        return None
    if max(plan.sell_price - reward_midpoint, reward_midpoint - plan.rebuy_price) >= spread:
        return None
    size = min(plan.shares, available_shares, available_cash / plan.rebuy_price)
    size = size.to_integral_value(rounding=ROUND_FLOOR)
    if size < plan.minimum_shares:
        return None
    return (
        _order(plan.token_id, "SELL", plan.sell_price, size),
        _order(plan.token_id, "BUY", plan.rebuy_price, size),
    )
