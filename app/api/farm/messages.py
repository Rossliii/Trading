from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from app.farm.schemas import FarmConfig, RestingOrder
from app.types import NonNegativeDecimal, OrderSide, PositiveDecimal

# ── Client → server ─────────────────────────────────────────────────────────


class FarmCreateMessage(FarmConfig):
    type: Literal["farm_create"]


class FarmCancelMessage(BaseModel):
    type: Literal["farm_cancel"]


FarmClientMessage = Annotated[
    FarmCreateMessage | FarmCancelMessage,
    Field(discriminator="type"),
]


# ── Server → client: lifecycle ──────────────────────────────────────────────


class FarmStartedEvent(BaseModel):
    type: Literal["farm_started"] = "farm_started"


class FarmCancelledEvent(BaseModel):
    type: Literal["farm_cancelled"] = "farm_cancelled"


class FarmErrorEvent(BaseModel):
    type: Literal["farm_error"] = "farm_error"
    reason: str


class FarmKilledEvent(BaseModel):
    type: Literal["farm_killed"] = "farm_killed"
    reason: Literal["max_session_loss"]
    session_loss: Decimal
    unrealized_loss: Decimal = Decimal(0)
    total_loss: Decimal = Decimal(0)
    session_reward: Decimal = Decimal(0)
    net_loss: Decimal = Decimal(0)


# ── Server → client: orders ─────────────────────────────────────────────────


CancelReason = Literal[
    "too_close_to_spread",
    "tick_size_change",
    "market_dropped",
    "shutdown",
    "requote",
    "filled_exit",
    "server_cancelled",
    "guard_pulled",
    "entry_partial_fill",
    "entry_timeout",
    "market_missing",
    "reward_below_minimum",
    "reward_inactive_or_unverified",
    "reward_parameters_changed",
    "market_expired",
    "market_excluded",
    "strategy_halted",
    "book_missing",
    "book_one_sided",
    "reward_depth_insufficient",
    "midpoint_outside_quotes",
    "outside_reward_spread",
    "quote_would_cross",
    "dynamic_quote_unavailable",
    "ladder_fill",
    "lower_buy_stop",
]


class OrderPlacedEvent(BaseModel):
    type: Literal["order_placed"] = "order_placed"
    market_id: str
    slug: str
    question: str
    outcome: str
    side: OrderSide
    price: PositiveDecimal
    size: PositiveDecimal
    capital_locked: PositiveDecimal
    order_id: str


class OrderCancelledEvent(BaseModel):
    type: Literal["order_cancelled"] = "order_cancelled"
    market_id: str
    slug: str
    outcome: str
    order_id: str
    reason: CancelReason
    details: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class OrderFilledEvent(BaseModel):
    type: Literal["order_filled"] = "order_filled"
    market_id: str
    slug: str
    outcome: str
    side: OrderSide
    price: PositiveDecimal
    size: PositiveDecimal
    shares_after: Decimal
    cost_basis_after: Decimal


# ── Server → client: aggregate ──────────────────────────────────────────────


class FarmSummaryEvent(BaseModel):
    type: Literal["farm_summary"] = "farm_summary"
    total_volume: NonNegativeDecimal
    total_rewards: NonNegativeDecimal
    rewards_per_hour: NonNegativeDecimal = Decimal(0)
    rewards_per_day: NonNegativeDecimal = Decimal(0)
    elapsed_seconds: int = 0
    active_markets: int
    session_loss: Decimal
    max_session_loss: PositiveDecimal
    wallet_balance: NonNegativeDecimal | None = None


# ── Server → client: positions snapshot ─────────────────────────────────────


class FarmPositionEntry(BaseModel):
    market_id: str
    slug: str
    question: str
    event_slug: str
    yes_price: NonNegativeDecimal
    no_price: NonNegativeDecimal
    midpoint: NonNegativeDecimal | None = None
    capital_deployed: NonNegativeDecimal
    yes_shares: Decimal = Decimal(0)
    no_shares: Decimal = Decimal(0)
    unrealized_pnl: Decimal = Decimal(0)
    position_pnl: Decimal | None = None
    limit_orders: list[RestingOrder] | None = None


class FarmPositionsEvent(BaseModel):
    type: Literal["farm_positions"] = "farm_positions"
    positions: list[FarmPositionEntry]
