from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.constants import MAX_CONCURRENT_POSITIONS
from app.types import (
    Change24hFilter,
    CreatedDateFilter,
    NonNegativeDecimal,
    PositiveDecimal,
    ProbabilityDecimal,
    QuoteDepth,
    Range24hFilter,
    TimeRemainingFilter,
)

RANGE_FIELDS: tuple[tuple[str, str], ...] = (
    ("vol_min", "vol_max"),
    ("liq_min", "liq_max"),
    ("spread_min", "spread_max"),
)


class FarmFilters(BaseModel):
    vol_min: NonNegativeDecimal
    vol_max: NonNegativeDecimal
    liq_min: NonNegativeDecimal
    liq_max: NonNegativeDecimal
    spread_min: NonNegativeDecimal
    spread_max: NonNegativeDecimal
    reward_min: NonNegativeDecimal
    time_remaining: TimeRemainingFilter
    created_date: CreatedDateFilter
    change_24h: Change24hFilter
    range_24h: Range24hFilter = "all"
    zone_liq_max: NonNegativeDecimal | None = None
    price_min: ProbabilityDecimal | None = None
    price_max: ProbabilityDecimal | None = None
    max_fill_loss: NonNegativeDecimal | None = None
    min_bid_depth_mult: NonNegativeDecimal | None = None

    @model_validator(mode="after")
    def validate_ranges(self) -> "FarmFilters":
        for lo_name, hi_name in RANGE_FIELDS:
            lo = getattr(self, lo_name)
            hi = getattr(self, hi_name)
            if lo > hi:
                raise ValueError(f"{lo_name} ({lo}) must be <= {hi_name} ({hi})")
        if (
            self.price_min is not None
            and self.price_max is not None
            and self.price_min > self.price_max
        ):
            raise ValueError(
                f"price_min ({self.price_min}) must be <= price_max ({self.price_max})"
            )
        return self


class SizeTier(BaseModel):
    max_shares: PositiveDecimal | None = None
    quote_depth: QuoteDepth = "normal"
    reward_min: NonNegativeDecimal = Decimal(0)
    liq_min: NonNegativeDecimal = Decimal(0)
    zone_liq_max: NonNegativeDecimal | None = None
    time_remaining: TimeRemainingFilter = "all"


class FarmConfig(BaseModel):
    filters: FarmFilters
    bankroll: PositiveDecimal
    max_session_loss: PositiveDecimal
    quote_depth: QuoteDepth = "safe"
    max_concurrent_positions: int = Field(default=MAX_CONCURRENT_POSITIONS, gt=0)
    size_tiers: list[SizeTier] = Field(default_factory=list)
    extreme_capital_budget: PositiveDecimal = Decimal("85")
    extreme_target_shares: PositiveDecimal = Field(
        default=Decimal("1000"), description="Legacy field; sizing now uses a 50/50 cash split."
    )
    # Legacy compatibility only; live quotes use fixed tick offsets.
    extreme_sell_markup: PositiveDecimal = Decimal("0.003")
    extreme_rebuy_discount: PositiveDecimal = Decimal("0.005")
    extreme_fee_reserve: NonNegativeDecimal = Decimal("2")


class Market(BaseModel):
    condition_id: str
    slug: str
    question: str

    yes_token_id: str
    no_token_id: str

    rewards_max_spread_cents: PositiveDecimal
    rewards_min_size: PositiveDecimal
    rewards_rate_per_day: NonNegativeDecimal

    tick_size: PositiveDecimal
    min_order_size: PositiveDecimal

    end_date: datetime
    created_at: datetime
    volume_24h: NonNegativeDecimal
    liquidity: NonNegativeDecimal
    spread_cents: NonNegativeDecimal
    price_change_24h: Decimal
    taker_fee_rate: ProbabilityDecimal = Decimal(0)

    game_start_time: datetime | None = None
    event_slug: str = ""
    midpoint: NonNegativeDecimal | None = None
    zone_liquidity: NonNegativeDecimal | None = None
    exit_loss: NonNegativeDecimal | None = None
    yes_bid_depth: NonNegativeDecimal | None = None
    no_bid_depth: NonNegativeDecimal | None = None
    sports_event_active: bool = False
    effective_depth: QuoteDepth | None = None
    effective_reward_min: NonNegativeDecimal | None = None
    effective_liq_min: NonNegativeDecimal | None = None
    effective_zone_liq_max: NonNegativeDecimal | None = None
    effective_time_remaining: TimeRemainingFilter | None = None
    adaptive_depth: QuoteDepth | None = None


class ExitOrder(BaseModel):
    outcome: str
    placed_at: datetime
    is_floor: bool = False


class RestingOrder(BaseModel):
    outcome: str
    side: str
    shares: NonNegativeDecimal
    value: NonNegativeDecimal


class MarketPosition(BaseModel):
    market: Market
    yes_order_id: str
    no_order_id: str
    yes_price: Decimal
    no_price: Decimal
    strategy_capital: NonNegativeDecimal | None = None
    resting_orders: list[RestingOrder] | None = None
    position_pnl: Decimal | None = None
    yes_shares: Decimal = Decimal(0)
    yes_cost_basis: Decimal = Decimal(0)
    no_shares: Decimal = Decimal(0)
    no_cost_basis: Decimal = Decimal(0)
    last_best_bid: Decimal | None = None
    last_best_ask: Decimal | None = None
    yes_best_bid: Decimal | None = None
    no_best_bid: Decimal | None = None
    exit_orders: dict[str, ExitOrder] = Field(default_factory=dict)
    orders_missing_ticks: int = 0
    candidate_miss_ticks: int = 0
    quotes_pulled: bool = False
    zero_balance_since: dict[str, datetime] = Field(default_factory=dict)
    yes_moved_up: bool = False
    no_moved_up: bool = False
    yes_requote_cancel_fails: int = 0
    no_requote_cancel_fails: int = 0
    quotes_pulled_at: datetime | None = None
    yes_prev_price: Decimal | None = None
    no_prev_price: Decimal | None = None
    yes_prev_price_at: datetime | None = None
    no_prev_price_at: datetime | None = None
    yes_hysteresis_episode_at: datetime | None = None
    no_hysteresis_episode_at: datetime | None = None


class LiveBook(BaseModel):
    bids: dict[Decimal, Decimal] = Field(default_factory=dict)
    asks: dict[Decimal, Decimal] = Field(default_factory=dict)


class MarketHealth(BaseModel):
    recent_failures: list[float] = Field(default_factory=list)
    paused_until: datetime | None = None
    open_failures: int = 0
    price_samples: list[tuple[float, Decimal]] = Field(default_factory=list)
    blacklist_until: datetime | None = None
    blacklist_permanent: bool = False
    fill_strikes: int = 0
    net_fill_pnl: Decimal = Decimal(0)
    open_rt_net: dict[str, Decimal] = Field(default_factory=dict)
    loss_roundtrips: int = 0
    cum_reward_credit: Decimal = Decimal(0)
    guard_pull_until: datetime | None = None
    severe_guard_trips: int = 0


class FokExitInfo(BaseModel):
    token_id: str
    size: Decimal
    outcome: str
    slug: str
    entry_cost: Decimal = Decimal(0)
    staged_at: datetime | None = None


class ExitCostInfo(BaseModel):
    entry_cost: Decimal
    entry_size: Decimal
    slug: str


class OrderInfo(BaseModel):
    condition_id: str
    outcome: str
    token_id: str
    placed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    retired_at: datetime | None = None


class BookSample(BaseModel):
    """Pure instrumentation: tracks a filled token so post_fill_sampler_loop can watch its book
    recover (or not) for POST_FILL_SAMPLE_SECONDS after the fill."""

    token_id: str
    slug: str
    outcome: str
    entry_px: Decimal
    started_at: datetime


class FarmState(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    config: FarmConfig
    wallet_address: str = ""
    starting_balance: Decimal | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    rewards_earned: Decimal = Decimal(0)
    expected_rewards_per_day: Decimal = Decimal(0)
    market_rewards: dict[str, Decimal] = Field(default_factory=dict)
    market_rewards_baseline: dict[str, Decimal] = Field(default_factory=dict)
    total_volume: Decimal = Decimal(0)
    session_loss: Decimal = Decimal(0)
    killed: bool = False
    positions: dict[str, MarketPosition] = Field(default_factory=dict)
    pending_exit_order_ids: set[str] = Field(default_factory=set)
    recent_exits: dict[str, datetime] = Field(default_factory=dict)
    reconcile_attempts: dict[str, int] = Field(default_factory=dict)
    exit_cost_basis: dict[str, ExitCostInfo] = Field(default_factory=dict)
    pending_fok_exits: dict[str, FokExitInfo] = Field(default_factory=dict)
    processed_events: set[tuple[str, str]] = Field(default_factory=set)
    order_registry: dict[str, OrderInfo] = Field(default_factory=dict)
    health: dict[str, MarketHealth] = Field(default_factory=dict)
    live_books: dict[str, LiveBook] = Field(default_factory=dict)
    excluded_markets: set[str] = Field(default_factory=set)
    excluded_events: dict[str, datetime] = Field(default_factory=dict)
    booked_exit_fills: set[tuple[str, str]] = Field(default_factory=set)  # (trade.id, exit_oid)
    pending_orphan_cancels: set[str] = Field(default_factory=set)
    reaper_unknown_ids: set[str] = Field(default_factory=set)
    book_samples: dict[str, BookSample] = Field(default_factory=dict)

    def elapsed_seconds(self, now: datetime | None = None) -> int:
        now = now or datetime.now(timezone.utc)
        return max(0, int((now - self.started_at).total_seconds()))

    def rewards_per_hour(self) -> Decimal:
        return self.expected_rewards_per_day / Decimal(24)
