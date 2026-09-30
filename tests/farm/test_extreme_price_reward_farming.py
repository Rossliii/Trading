from datetime import datetime, timezone
from decimal import Decimal as D

import pytest

from app.farm.extreme_price_reward_farming import (
    ExtremePriceRewardConfig,
    build_entry_plan,
    build_quotes,
)
from app.farm.schemas import Market


def market(**changes):
    data = dict(
        condition_id="extreme", slug="extreme", question="Example?",
        yes_token_id="yes", no_token_id="no", rewards_max_spread_cents="3",
        rewards_min_size="100", rewards_rate_per_day="100", tick_size="0.001",
        min_order_size="5", end_date=datetime(2099, 1, 1, tzinfo=timezone.utc),
        created_at=datetime(2020, 1, 1, tzinfo=timezone.utc), volume_24h="1000",
        liquidity="1000", spread_cents="1", price_change_24h="0",
    )
    return Market(**(data | changes))


def entry(m=None, **config):
    return build_entry_plan(
        m or market(), ExtremePriceRewardConfig(min_reward_per_day="50", **config),
        yes_midpoint=D("0.045"), no_midpoint=D("0.955"), cheap_best_ask=D("0.045"),
    )


def quotes(plan, **changes):
    args = dict(available_shares=D("1000"), available_cash=D("40"),
                reward_midpoint=D("0.045"), max_spread_cents=D("3"))
    return build_quotes(plan, **(args | changes))


def test_example_and_capital():
    plan = entry()
    assert plan.entry_price * plan.shares == D("45")
    assert plan.capital_required == D("85")
    sell, buy = quotes(plan)
    assert (sell.side, sell.price, sell.size) == ("SELL", 0.048, 1000)
    assert (buy.side, buy.price, buy.size) == ("BUY", 0.04, 1000)
    assert entry(capital_budget="45").capital_required <= D("45")


@pytest.mark.parametrize("yes,no,expected", [
    ("0.05", "0.95", "YES"), ("0.95", "0.05", "NO"),
    ("0.051", "0.949", None), ("0.04", "0.94", None),
])
def test_extreme_boundaries(yes, no, expected):
    plan = build_entry_plan(
        market(), ExtremePriceRewardConfig(min_reward_per_day="50"),
        yes_midpoint=D(yes), no_midpoint=D(no), cheap_best_ask=D("0.045"),
    )
    assert (plan.outcome if plan else None) == expected
    if plan:
        assert plan.token_id == expected.lower()


def test_ineligible_market_and_tick_rounding():
    assert entry(market(rewards_rate_per_day="49")) is None
    assert entry(market(end_date=datetime(2020, 1, 1, tzinfo=timezone.utc))) is None
    assert entry(capital_budget="1") is None
    plan = entry(market(tick_size="0.01"))
    assert (plan.entry_price, plan.sell_price, plan.rebuy_price) == (
        D("0.05"), D("0.06"), D("0.04"))


def test_confirmed_inventory_cash_and_reward_constraints():
    plan = entry()
    assert quotes(plan, available_shares=D("0")) is None
    assert quotes(plan, available_shares=D("99")) is None
    assert quotes(plan, available_cash=D("0")) is None
    assert quotes(plan, max_spread_cents=D("0.5")) is None
    assert quotes(plan, reward_midpoint=D("0.06")) is None
    sell, buy = quotes(plan, available_shares=D("250"))
    assert sell.size == buy.size == 250
