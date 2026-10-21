from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.farm.reward_eligibility import active_reward_rate, verify_market_rewards


def config(start="2026-10-01", end="2026-10-04", rate="10"):
    return {"asset_address": "asset", "start_date": start, "end_date": end, "rate_per_day": rate}


def test_only_active_programs_are_summed():
    now = datetime(2026, 10, 3, tzinfo=timezone.utc)
    assert active_reward_rate(
        [
            config(),
            config(rate="5"),
            config(end="2026-10-03"),
            config(start="2026-10-04"),
        ],
        now,
    ) == Decimal("15")


def test_epoch_milliseconds_and_open_ended_program():
    now = datetime(2026, 10, 3, tzinfo=timezone.utc)
    assert active_reward_rate([config(start=0, end=None)], now) == Decimal("10")


def test_invalid_dates_fail_verification():
    with pytest.raises(ValueError):
        active_reward_rate([config(start="bad-date")])


@pytest.mark.parametrize(
    "programs",
    [[], [config(end="2000-01-01", start="1999-01-01")], [config(start="2099-01-01", end=None)]],
)
async def test_stale_positive_metadata_does_not_enable_rewards(market, programs):
    response = MagicMock()
    response.json.return_value = {
        "data": [
            {
                "condition_id": market.condition_id,
                "rewards_config": programs,
                "total_daily_rate": "100",
                "rewards_min_size": "100",
                "rewards_max_spread": "3",
            }
        ]
    }
    http = AsyncMock()
    http.get.return_value = response
    assert await verify_market_rewards(http, market, Decimal(0)) is None


async def test_active_rewards_replace_cached_parameters(market):
    response = MagicMock()
    response.json.return_value = {
        "data": [
            {
                "condition_id": market.condition_id,
                "rewards_config": [config(start="2000-01-01", end="2099-01-01")],
                "rewards_min_size": "123",
                "rewards_max_spread": "2",
            }
        ]
    }
    http = AsyncMock()
    http.get.return_value = response
    fresh = await verify_market_rewards(http, market, Decimal("5"))
    assert fresh.rewards_rate_per_day == Decimal("10")
    assert fresh.rewards_min_size == Decimal("123")
    assert fresh.rewards_max_spread_cents == Decimal("2")
