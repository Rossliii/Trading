from datetime import datetime, timezone
from decimal import Decimal as D

import pytest

from app.farm.extreme_price_reward_farming import (
    ExtremePriceRewardConfig,
    build_entry_plan,
    build_quotes,
)
from app.farm.quoting import compute_inventory_quote
from app.farm.schemas import Market


@pytest.mark.parametrize(
    "bid,ask,tick,expected",
    [
        ("0.044", "0.045", "0.001", (D("0.043"), D("0.045"))),
        ("0.042", "0.044", "0.001", (D("0.042"), D("0.043"))),
        ("0.04", "0.05", "0.01", (D("0.03"), D("0.05"))),
        ("0.04", "0.043", "0.001", None),
        ("0.001", "0.002", "0.001", None),
        ("0.044", "0.045", "0.01", None),
    ],
)
def test_exact_tick_offsets_or_rejection(bid, ask, tick, expected):
    assert compute_inventory_quote(D(bid), D(ask), D(tick)) == expected


def market(**changes):
    data = dict(
        condition_id="extreme",
        slug="extreme",
        question="Example?",
        yes_token_id="yes",
        no_token_id="no",
        rewards_max_spread_cents="3",
        rewards_min_size="100",
        rewards_rate_per_day="100",
        tick_size="0.001",
        min_order_size="5",
        end_date=datetime(2099, 1, 1, tzinfo=timezone.utc),
        created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        volume_24h="1000",
        liquidity="1000",
        spread_cents="1",
        price_change_24h="0",
    )
    return Market(**(data | changes))


def entry(m=None, **config):
    return build_entry_plan(
        m or market(),
        ExtremePriceRewardConfig(min_reward_per_day="50", **config),
        yes_midpoint=D("0.045"),
        no_midpoint=D("0.955"),
        cheap_best_ask=D("0.045"),
        cheap_best_bid=D("0.044"),
        reward_midpoint=D("0.0445"),
    )


def quotes(plan, **changes):
    args = dict(
        available_shares=D("1000"),
        available_cash=D("40"),
        reward_midpoint=D("0.045"),
        max_spread_cents=D("3"),
    )
    return build_quotes(plan, **(args | changes))


def test_example_and_capital():
    plan = entry()
    assert plan.entry_price * plan.shares == D("42.48")
    assert plan.rebuy_budget == D("42.5")
    assert plan.capital_required == D("84.98")
    sell, buy = quotes(plan)
    assert (sell.side, sell.price, sell.size) == ("SELL", 0.045, 944)
    assert (buy.side, buy.price, buy.size) == ("BUY", 0.043, 930)
    assert entry(capital_budget="45").capital_required <= D("45")


@pytest.mark.parametrize(
    "yes,no,expected",
    [
        ("0.05", "0.95", "YES"),
        ("0.95", "0.05", "NO"),
        ("0.051", "0.949", None),
        ("0.04", "0.94", None),
    ],
)
def test_extreme_boundaries(yes, no, expected):
    plan = build_entry_plan(
        market(),
        ExtremePriceRewardConfig(min_reward_per_day="50"),
        yes_midpoint=D(yes),
        no_midpoint=D(no),
        cheap_best_ask=D("0.045"),
        cheap_best_bid=D("0.044"),
        reward_midpoint=D("0.0445"),
    )
    assert (plan.outcome if plan else None) == expected
    if plan:
        assert plan.token_id == expected.lower()


def test_ineligible_market_and_tick_rounding():
    assert entry(market(rewards_rate_per_day="49")) is None
    assert entry(market(end_date=datetime(2020, 1, 1, tzinfo=timezone.utc))) is None
    assert entry(capital_budget="1") is None
    assert entry(market(tick_size="0.01")) is None


def test_confirmed_inventory_cash_and_reward_constraints():
    plan = entry()
    assert quotes(plan, available_shares=D("0")) is None
    assert quotes(plan, available_shares=D("99")) is None
    assert quotes(plan, available_cash=D("0")) is None
    assert quotes(plan, max_spread_cents=D("0.15")) is None
    assert quotes(plan, reward_midpoint=D("0.08")) is None
    sell, buy = quotes(plan, available_shares=D("250"))
    assert sell.size == 250
    assert buy.size == 930


def test_half_cash_split_is_independent_of_share_target_and_partial_inventory():
    plan = entry(target_shares="10")
    assert plan.shares == 944
    sell, buy = quotes(plan, available_shares=D(250), available_cash=D("42.5"))
    assert sell.size == 250
    assert buy.size == 988
    assert D(str(buy.size)) * plan.rebuy_price <= D("42.5")
    assert plan.shares * plan.entry_price <= D("42.5")
