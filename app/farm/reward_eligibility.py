"""Verify active liquidity rewards independently of sampling metadata."""

from datetime import datetime, timezone
from decimal import Decimal

from polymarket.models.clob.rewards import CurrentRewardConfig

from app.constants import CLOB_HOST, PAGINATION_END_CURSOR


def active_reward_rate(configs, now=None):
    now = now or datetime.now(timezone.utc)
    total = Decimal(0)
    for raw in configs:
        config = CurrentRewardConfig.model_validate(raw)
        start = config.start_date
        end = config.end_date
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end is not None and end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        if start <= now and (end is None or now < end):
            if not config.rate_per_day.is_finite() or config.rate_per_day < 0:
                raise ValueError("Invalid reward rate")
            total += config.rate_per_day
    return total


async def verify_market_rewards(http, market, minimum):
    """Return fresh reward metadata, or None for an inactive/ineligible market.

    Malformed responses and HTTP failures raise: callers must not use cached rewards.
    """
    configs = []
    metadata = None
    cursor = ""
    seen = set()
    while True:
        response = await http.get(
            f"{CLOB_HOST}/rewards/markets/{market.condition_id}",
            params={"next_cursor": cursor} if cursor else {},
        )
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict) or not isinstance(body.get("data"), list):
            raise ValueError("Malformed market rewards response")
        for item in body["data"]:
            if item["condition_id"] != market.condition_id:
                raise ValueError("Market rewards condition ID mismatch")
            metadata = item
            configs.extend(item["rewards_config"])
        cursor = body.get("next_cursor")
        if not cursor or cursor == PAGINATION_END_CURSOR:
            break
        if cursor in seen:
            raise ValueError("Repeated market rewards cursor")
        seen.add(cursor)
    rate = active_reward_rate(configs)
    if metadata is None or rate <= 0 or rate < minimum:
        return None
    spread = Decimal(str(metadata["rewards_max_spread"]))
    size = Decimal(str(metadata["rewards_min_size"]))
    if not spread.is_finite() or not size.is_finite() or spread <= 0 or size <= 0:
        raise ValueError("Invalid reward eligibility parameters")
    return market.model_copy(
        update={
            "rewards_rate_per_day": rate,
            "rewards_max_spread_cents": spread,
            "rewards_min_size": size,
        }
    )
