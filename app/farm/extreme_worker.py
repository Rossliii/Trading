"""Settlement-aware runtime for Extreme Price Reward Farming.

Settlement-aware ladder cycles with fixed BBA tick offsets. A lower-buy fill
liquidates session inventory and stops, except when both quote sides fully fill.
Ordinary stop/kill cancels orders and keeps inventory. Existing wallet holdings
are not adopted or liquidated. Missing open orders alone never imply a fill.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import ROUND_FLOOR, Decimal

import httpx

from app.api.farm.messages import (
    FarmErrorEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    OrderPlacedEvent,
)
from app.api.messages import send_event
from app.bot.balance import get_balance
from app.bot.heartbeat import heartbeat_loop
from app.bot.order_snapshot import OrderSnapshot
from app.bot.schemas import LimitOrder, OrderBook
from app.constants import REQUOTE_HYSTERESIS_SECONDS
from app.farm.discovery import fetch_books, fetch_eligible_markets, fetch_midpoints
from app.farm.extreme_price_reward_farming import (
    EntryPlan,
    ExtremePriceRewardConfig,
    build_entry_plan,
    build_quotes,
)
from app.farm.fees import taker_fee
from app.farm.kill_switch import should_kill
from app.farm.quoting import compute_inventory_quote
from app.farm.requote import is_flip_flop_requote, watch_quote_changes
from app.farm.reward_eligibility import verify_market_rewards
from app.farm.rewards import rewards_poll_loop
from app.farm.schemas import FarmState, LiveBook, Market, MarketPosition, RestingOrder

logger = logging.getLogger(__name__)
D = Decimal


@dataclass
class TrackedOrder:
    order_id: str
    order: LimitOrder
    entry: bool = False
    placed_at: float = field(default_factory=time.monotonic)
    snapshot: OrderSnapshot | None = None
    booked: set[str] = field(default_factory=set)
    cycle: int = 0
    market_amount: Decimal | None = None

    @property
    def confirmed_size(self):
        return (
            sum((f.size for f in self.snapshot.fills if f.status == "CONFIRMED"), D(0))
            if self.snapshot
            else D(0)
        )

    @property
    def fully_filled(self):
        return self.done and self.confirmed_size == D(str(self.order.size))

    @property
    def done(self):
        return self.snapshot is not None and self.snapshot.terminal and self.snapshot.settled

    @property
    def remaining(self):
        if self.snapshot is not None and self.snapshot.terminal:
            return D(0)
        matched = self.snapshot.matched if self.snapshot else D(0)
        return max(D(0), D(str(self.order.size)) - matched)


@dataclass
class Inventory:
    market: Market
    plan: EntryPlan
    budget: Decimal
    orders: list[TrackedOrder] = field(default_factory=list)
    shares: Decimal = D(0)
    cost: Decimal = D(0)
    # Realized losses and fees still consume the allocation; profits do not enlarge it.
    loss: Decimal = D(0)
    fees_paid: Decimal = D(0)
    halted: bool = False
    mark_price: Decimal | None = None
    previous_quotes: dict[str, tuple[Decimal, datetime]] = field(default_factory=dict)
    cycle: int = 0
    restart_entry: bool = False

    def live(self, side):
        return [o for o in self.orders if not o.done and o.order.side == side]


def reward_midpoint(book: OrderBook, size: Decimal) -> Decimal | None:
    """Use the price reached at the reward size cutoff on each book side."""
    prices = []
    for levels, reverse in ((book.bids, True), (book.asks, False)):
        total = D(0)
        for level in sorted(levels, key=lambda x: x.price, reverse=reverse):
            total += level.size
            if total >= size:
                prices.append(level.price)
                break
    if len(prices) != 2 or prices[0] >= prices[1]:
        return None
    return sum(prices) / 2


class ExtremeRuntime:
    def __init__(self, client, state: FarmState, websocket):
        self.client, self.state, self.websocket = client, state, websocket
        self.inventory: dict[str, Inventory] = {}
        self.completed: set[str] = set()
        self.markets: dict[str, Market] = {}
        self.discovery_at = float("-inf")
        self.discovery_task = None
        self.midpoints = {}
        self.cash = D(0)
        self.stopping_reason: str | None = None
        self.http = None

    async def check_rewards(self, inv):
        try:
            current = await verify_market_rewards(
                self.http, inv.market, self.state.config.filters.reward_min
            )
        except Exception:
            logger.warning("extreme_reward_verification_failed market=%s", inv.market.slug)
            current = None
        parameters_changed = current is not None and (
            current.rewards_min_size != inv.market.rewards_min_size
            or current.rewards_max_spread_cents != inv.market.rewards_max_spread_cents
        )
        if current is None or parameters_changed:
            inv.halted = True
            for order in inv.orders:
                await self.cancel(
                    inv,
                    order,
                    "reward_parameters_changed"
                    if parameters_changed
                    else "reward_inactive_or_unverified",
                )
            return False
        inv.market = current
        self.state.positions[current.condition_id].market = current
        return True

    async def refresh_universe(self, http):
        markets = await fetch_eligible_markets(http)
        midpoints = (
            await fetch_midpoints(
                http,
                [
                    token
                    for market in markets
                    for token in (market.yes_token_id, market.no_token_id)
                ],
            )
            if markets
            else {}
        )
        self.markets = {market.condition_id: market for market in markets}
        self.midpoints = midpoints
        self.discovery_at = time.monotonic()

    async def emit(self, event):
        # A UI disconnect must not roll back a successfully tracked order/fill.
        try:
            await send_event(self.websocket, event)
        except Exception:
            logger.exception("extreme: could not deliver event")

    def mirror(self, inv):
        pos = self.state.positions[inv.market.condition_id]
        side = inv.plan.outcome.lower()
        setattr(pos, side + "_shares", inv.shares)
        setattr(pos, side + "_cost_basis", inv.cost)
        pos.position_pnl = (
            inv.shares * inv.mark_price - inv.cost if inv.mark_price is not None else None
        )
        pos.resting_orders = [
            RestingOrder(
                outcome=inv.plan.outcome,
                side=order.order.side,
                shares=order.remaining,
                value=order.remaining * D(str(order.order.price)),
            )
            for order in inv.orders
            if not order.done and order.remaining > 0 and order.market_amount is None
        ]
        pos.strategy_capital = inv.cost + sum(
            (o.remaining * D(str(o.order.price)) for o in inv.live("BUY")), D(0)
        )

    async def poll(self, inv, order):
        if order.done:
            return
        snap = await self.client.order_snapshot(order.order_id)
        if snap.order_id != order.order_id:
            raise ValueError("Order snapshot identity mismatch")
        if (
            not (order.entry and order.market_amount is not None)
            and snap.matched > D(str(order.order.size))
        ) or sum((fill.size for fill in snap.fills), D(0)) > snap.matched:
            raise ValueError("Order snapshot fill quantities are inconsistent")
        for fill in snap.fills:
            if fill.status != "CONFIRMED" or fill.trade_id in order.booked:
                continue
            value = fill.size * fill.price
            fee = (
                taker_fee(fill.size, fill.price, inv.market.taker_fee_rate) if fill.taker else D(0)
            )
            inv.fees_paid += fee
            if order.order.side == "BUY":
                inv.shares += fill.size
                inv.cost += value + fee
            else:
                if fill.size > inv.shares:
                    raise ValueError("Confirmed sell exceeds tracked inventory")
                released = inv.cost * fill.size / inv.shares
                inv.shares -= fill.size
                inv.cost -= released
                loss = released - value + fee
                inv.loss += loss
                self.state.session_loss += loss
            order.booked.add(fill.trade_id)
            self.state.total_volume += value
            await self.emit(
                OrderFilledEvent(
                    market_id=inv.market.condition_id,
                    slug=inv.market.slug,
                    outcome=inv.plan.outcome,
                    side=order.order.side,
                    price=fill.price,
                    size=fill.size,
                    shares_after=inv.shares,
                    cost_basis_after=inv.cost,
                )
            )
        order.snapshot = snap
        self.mirror(inv)

    async def cancel(self, inv, order, reason="market_dropped", details=None):
        if order.done:
            return
        if order.snapshot is not None and order.snapshot.terminal:
            await self.poll(inv, order)
            return
        context = {
            "side": order.order.side,
            "price": str(order.order.price),
            "requested_size": str(order.order.size),
            "held_shares": str(inv.shares),
            "age_seconds": round(time.monotonic() - order.placed_at, 3),
            "matched_size": str(order.snapshot.matched) if order.snapshot else None,
            **(details or {}),
        }
        payload = json.dumps(context, sort_keys=True)
        logger.info(
            "extreme_cancel_requested market=%s order=%s reason=%s details=%s",
            inv.market.slug,
            order.order_id,
            reason,
            payload,
        )
        try:
            await self.client.cancel_order(order.order_id)
            await self.poll(inv, order)
            self.mirror(inv)
            if order.snapshot is None or not order.snapshot.terminal:
                raise RuntimeError("Order cancellation not confirmed")
        except Exception:
            logger.exception(
                "extreme_cancel_unverified market=%s order=%s reason=%s details=%s",
                inv.market.slug,
                order.order_id,
                reason,
                payload,
            )
            raise
        if order.snapshot.status.upper() not in {"CANCELED", "CANCELLED"}:
            logger.info(
                "extreme_cancel_raced market=%s order=%s reason=%s status=%s",
                inv.market.slug,
                order.order_id,
                reason,
                order.snapshot.status,
            )
            return
        logger.info(
            "extreme_cancel_confirmed market=%s order=%s reason=%s details=%s",
            inv.market.slug,
            order.order_id,
            reason,
            payload,
        )
        await self.emit(
            OrderCancelledEvent(
                market_id=inv.market.condition_id,
                slug=inv.market.slug,
                outcome=inv.plan.outcome,
                order_id=order.order_id,
                reason=reason,
                details=context,
            )
        )

    async def place(self, inv, order, *, entry=False, market_amount=None):
        async def submit():
            if market_amount is None:
                if inv.halted or not await self.check_rewards(inv):
                    return
                if D(str(order.size)) < inv.market.rewards_min_size:
                    return
            if (
                self.state.killed
                or (
                    inv.market.condition_id in self.state.excluded_markets and market_amount is None
                )
                or (self.stopping_reason and order.side == "BUY")
            ):
                raise RuntimeError("Trading stopped or market excluded before submission")
            if market_amount is None:
                oid = await self.client.place_limit_order(order, post_only=not entry)
            else:
                oid = await self.client.place_market_order(
                    order.token_id, order.side, float(market_amount)
                )
                logger.info(
                    "extreme_market_order market=%s side=%s amount=%s order=%s cycle=%d",
                    inv.market.slug,
                    order.side,
                    market_amount,
                    oid,
                    inv.cycle,
                )
            if not oid:
                raise RuntimeError("Order submission returned no ID")
            inv.orders.append(
                TrackedOrder(oid, order, entry=entry, cycle=inv.cycle, market_amount=market_amount)
            )
            if order.side == "BUY":
                self.cash -= D(str(order.price)) * D(str(order.size))
            self.mirror(inv)
            await self.emit(
                OrderPlacedEvent(
                    market_id=inv.market.condition_id,
                    slug=inv.market.slug,
                    question=inv.market.question,
                    outcome=inv.plan.outcome,
                    side=order.side,
                    price=order.price,
                    size=order.size,
                    capital_locked=D(str(order.price)) * D(str(order.size)),
                    order_id=oid,
                )
            )

        # Let an in-flight SDK thread finish and register its order before shutdown cancel_all.
        task = asyncio.create_task(submit())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    def cycle_orders(self, inv):
        return [
            o
            for o in inv.orders
            if o.cycle == inv.cycle and not o.entry and o.market_amount is None
        ]

    async def reconcile_cycle(self, inv):
        """Cancel both sides on any match, then classify only settled fills, including races."""
        orders = self.cycle_orders(inv)
        if not any(o.snapshot and o.snapshot.matched > 0 for o in orders):
            return False
        for order in orders:
            await self.cancel(inv, order, "ladder_fill")
        if any(not o.done for o in orders):
            return True
        buys = [o for o in orders if o.order.side == "BUY"]
        sells = [o for o in orders if o.order.side == "SELL"]
        bought = sum((o.confirmed_size for o in buys), D(0))
        sold = sum((o.confirmed_size for o in sells), D(0))
        both_full = bool(buys and sells and buys[-1].fully_filled and sells[-1].fully_filled)
        if bought and not both_full:
            self.stopping_reason = "lower_buy_fill"
            logger.warning(
                "extreme_ladder_stop market=%s bought=%s sold=%s", inv.market.slug, bought, sold
            )
            return True
        inv.cycle += 1
        inv.previous_quotes.clear()
        inv.restart_entry = bool(sold and not bought and inv.shares == 0)
        logger.info(
            "extreme_ladder_next market=%s cycle=%d both_full=%s restart_entry=%s shares=%s",
            inv.market.slug,
            inv.cycle,
            both_full,
            inv.restart_entry,
            inv.shares,
        )
        return True  # Next tick obtains a fresh book after cancellation and settlement.

    async def liquidate_session(self):
        """Cancel session orders; wait for settlement; FAK sell residual inventory until flat."""
        for inv in self.inventory.values():
            for order in inv.orders:
                await self.poll(inv, order)
                if order.market_amount is None:
                    await self.cancel(inv, order, "lower_buy_stop")
        if any(not o.done for inv in self.inventory.values() for o in inv.orders):
            return
        for inv in self.inventory.values():
            if inv.shares <= 0:
                continue
            available = await self.client.conditional_balance(inv.plan.token_id)
            size = min(inv.shares, available)
            if size <= 0:
                logger.warning(
                    "extreme_liquidation_wait_balance market=%s remaining=%s",
                    inv.market.slug,
                    inv.shares,
                )
                continue
            await self.place(
                inv,
                LimitOrder(
                    token_id=inv.plan.token_id,
                    side="SELL",
                    size=float(size),
                    price=float(inv.mark_price or inv.plan.sell_price),
                ),
                market_amount=size,
            )
        if all(inv.shares == 0 for inv in self.inventory.values()):
            self.state.killed = True
            logger.info("extreme_liquidation_complete reason=%s", self.stopping_reason)
            await self.emit(
                FarmErrorEvent(reason="lower_buy_fill; session inventory liquidated; stopped")
            )

    async def manage(self, inv, book, eligible, ineligible_reason="market_dropped"):
        context = {
            "reward_per_day": str(inv.market.rewards_rate_per_day),
            "reward_min": str(self.state.config.filters.reward_min),
            "end_date": inv.market.end_date.isoformat(),
            "sell_price": str(inv.plan.sell_price),
            "rebuy_price": str(inv.plan.rebuy_price),
            "reward_min_size": str(inv.market.rewards_min_size),
            "max_spread_cents": str(inv.market.rewards_max_spread_cents),
            "book_timestamp": book.timestamp.isoformat() if book else None,
            "tick_size": str(book.tick_size) if book else None,
            "best_bid": str(max(level.price for level in book.bids))
            if book and book.bids
            else None,
            "best_ask": str(min(level.price for level in book.asks))
            if book and book.asks
            else None,
        }
        if not eligible or inv.halted:
            reason = ineligible_reason if not eligible else "strategy_halted"
            inv.halted = True
            for order in inv.orders:
                await self.cancel(inv, order, reason, context)
            return
        entries = [o for o in inv.orders if o.entry and o.cycle == inv.cycle]
        if entries and entries[-1].done and not entries[-1].booked:
            inv.halted = True
            return
        for order in inv.orders:
            if order.entry and not order.done:
                if order.booked or time.monotonic() - order.placed_at >= 60:
                    reason = "entry_partial_fill" if order.booked else "entry_timeout"
                    await self.cancel(inv, order, reason, context)
                return  # Wait for confirmed settlement before offering shares.
        if any(o.snapshot is None or not o.snapshot.settled for o in inv.orders if not o.done):
            return
        if book is None or not book.bids or not book.asks:
            reason = "book_missing" if book is None else "book_one_sided"
            for order in inv.orders:
                await self.cancel(inv, order, reason, context)
            return
        mid = reward_midpoint(book, inv.market.rewards_min_size)
        minimum = max(book.min_order_size, inv.market.rewards_min_size)
        best_bid = max(level.price for level in book.bids)
        best_ask = min(level.price for level in book.asks)
        spread = inv.market.rewards_max_spread_cents / 100
        cost = inv.cost / inv.shares if inv.shares > 0 else D(0)
        quotes = compute_inventory_quote(
            best_bid,
            best_ask,
            book.tick_size,
        )
        bid, ask = quotes if quotes else (inv.plan.rebuy_price, inv.plan.sell_price)
        context.update(
            {
                "midpoint": str((best_bid + best_ask) / 2),
                "cost_per_share": str(cost),
                "new_buy_price": str(bid),
                "new_sell_price": str(ask),
            }
        )
        context["reward_midpoint"] = str(mid) if mid is not None else None
        if mid is None:
            reason = "reward_depth_insufficient"
        elif quotes is None:
            reason = "dynamic_quote_unavailable"
        elif max(abs(ask - mid), abs(mid - bid)) >= spread:
            reason = "outside_reward_spread"
        elif ask <= best_bid or bid >= best_ask:
            reason = "quote_would_cross"
        else:
            reason = None
        if reason:
            for order in inv.orders:
                await self.cancel(inv, order, reason, context)
            return
        if inv.restart_entry:
            # Revalidate the next reward quotes before spending the next round's entry half.
            reserve = self.state.config.extreme_fee_reserve
            cash = await get_balance(self.state.wallet_address)
            committed = sum(
                (
                    o.remaining * D(str(o.order.price))
                    for position in self.inventory.values()
                    for o in position.live("BUY")
                ),
                D(0),
            )
            principal = min(inv.budget - max(D(0), inv.loss) - reserve, cash - committed - reserve)
            amount = min(inv.plan.rebuy_budget, principal / 2).quantize(
                D("0.01"), rounding=ROUND_FLOOR
            )
            if (
                best_ask > D("0.05")
                or amount <= 0
                or min(amount / best_ask, amount / bid) < minimum
            ):
                return
            if taker_fee(amount / best_ask, best_ask, inv.market.taker_fee_rate) > reserve:
                return
            inv.plan = replace(
                inv.plan, entry_price=best_ask, sell_price=ask, rebuy_price=bid, rebuy_budget=amount
            )
            await self.place(
                inv,
                LimitOrder(
                    token_id=inv.plan.token_id,
                    side="BUY",
                    price=float(best_ask),
                    size=float(amount / best_ask),
                ),
                entry=True,
                market_amount=amount,
            )
            inv.restart_entry = False
            return
        # Cancel only changed sides. Never replace until all matched trades settle.
        now = datetime.now(timezone.utc)
        targets = {"BUY": bid, "SELL": ask}
        for side, target in list(targets.items()):
            for order in inv.live(side):
                old = D(str(order.order.price))
                if old == target:
                    continue
                prev, at = inv.previous_quotes.get(side, (None, None))
                safe_old = (
                    old % book.tick_size == 0
                    and abs(old - mid) < spread
                    and (old < min(ask, best_ask) if side == "BUY" else old > max(bid, best_bid))
                )
                if safe_old and is_flip_flop_requote(
                    old, target, prev, at, book.tick_size, now, REQUOTE_HYSTERESIS_SECONDS
                ):
                    targets[side] = old
                    continue
                reason = "tick_size_change" if old % book.tick_size else "requote"
                await self.cancel(inv, order, reason, context)
                inv.previous_quotes[side] = (old, now)
        if any(
            not order.done and (order.snapshot is None or not order.snapshot.settled)
            for order in inv.orders
        ):
            return
        # Cancellation can confirm new fills. Recompute on the next fresh book when cost changes.
        new_cost = inv.cost / inv.shares if inv.shares > 0 else D(0)
        if should_kill(self.state):
            self.state.killed = True
            await self.emit(FarmErrorEvent(reason="extreme_max_session_loss; inventory retained"))
            return
        if new_cost != cost:
            return
        if any(o.snapshot and o.snapshot.matched > 0 for o in self.cycle_orders(inv)):
            return  # A fill raced with repricing; classify it before any replacement.
        inv.plan = replace(inv.plan, rebuy_price=targets["BUY"], sell_price=targets["SELL"])
        # Release only confirmed cancelled commitments and recheck the actual cash balance.
        self.cash = await get_balance(self.state.wallet_address)
        self.cash -= sum(
            (
                order.remaining * D(str(order.order.price))
                for position in self.inventory.values()
                for order in position.live("BUY")
            ),
            D(0),
        )
        # Confirmed trade shares can differ from spendable shares after fees/settlement lag.
        balance = await self.client.conditional_balance(inv.plan.token_id)
        sell_size = min(inv.shares, balance).to_integral_value(rounding=ROUND_FLOOR)
        if not inv.live("SELL") and sell_size >= book.min_order_size:
            await self.place(
                inv,
                LimitOrder(
                    token_id=inv.plan.token_id,
                    side="SELL",
                    price=float(inv.plan.sell_price),
                    size=float(sell_size),
                ),
            )
        reserve = max(D(0), self.state.config.extreme_fee_reserve - inv.fees_paid)
        room = max(D(0), inv.budget - inv.cost - max(D(0), inv.loss) - reserve)
        # No more than one lower bid; stop accumulating once the allocation is occupied.
        buy_size = min(
            inv.plan.rebuy_budget / inv.plan.rebuy_price,
            room / inv.plan.rebuy_price,
            max(D(0), self.cash - reserve) / inv.plan.rebuy_price,
        )
        buy_size = buy_size.to_integral_value(rounding=ROUND_FLOOR)
        if not inv.live("BUY") and buy_size >= minimum:
            # A zero-fill entry is never blindly retried as a new entry.
            if not any(o.booked for o in inv.orders):
                inv.halted = True
                return
            # Do not add exposure while purchased shares are still unavailable for sale.
            if inv.shares > 0 and not inv.live("SELL"):
                return
            await self.place(
                inv,
                LimitOrder(
                    token_id=inv.plan.token_id,
                    side="BUY",
                    price=float(inv.plan.rebuy_price),
                    size=float(buy_size),
                ),
            )

    async def tick(self, http):
        self.http = http
        if self.stopping_reason:
            await self.liquidate_session()
            return
        state = self.state
        if state.killed:
            return
        # Poll balances before fills: reserve every outstanding BUY in addition to held cost.
        self.cash = await get_balance(state.wallet_address)
        self.cash -= sum(
            (
                D(str(o.order.price)) * o.remaining
                for inv in self.inventory.values()
                for o in inv.live("BUY")
            ),
            D(0),
        )
        if self.discovery_task is not None and self.discovery_task.done():
            await self.discovery_task
            self.discovery_task = None
        if self.discovery_task is None and time.monotonic() - self.discovery_at >= 60:
            if self.inventory:
                self.discovery_task = asyncio.create_task(self.refresh_universe(http))
            else:
                await self.refresh_universe(http)
        candidates = sorted(
            (
                m
                for m in self.markets.values()
                if m.rewards_rate_per_day > 0
                and m.rewards_rate_per_day >= state.config.filters.reward_min
                and m.condition_id not in self.inventory
                and m.condition_id not in self.completed
                and m.condition_id not in state.excluded_markets
                and m.end_date > datetime.now(timezone.utc)
            ),
            key=lambda m: (-m.rewards_rate_per_day, m.condition_id),
        )
        mids = self.midpoints
        candidates = [
            m
            for m in candidates
            if (
                D(0) < mids.get(m.yes_token_id, D(1)) <= D("0.05")
                and mids.get(m.no_token_id, D(0)) >= D("0.95")
            )
            or (
                D(0) < mids.get(m.no_token_id, D(1)) <= D("0.05")
                and mids.get(m.yes_token_id, D(0)) >= D("0.95")
            )
        ]
        candidates = candidates[: state.config.max_concurrent_positions * 3]
        tokens = {t for m in candidates for t in (m.yes_token_id, m.no_token_id)}
        tokens.update(inv.plan.token_id for inv in self.inventory.values())
        books = await fetch_books(http, list(tokens)) if tokens else {}
        for inv in self.inventory.values():
            book = books.get(inv.plan.token_id)
            inv.mark_price = (
                (max(level.price for level in book.bids) + min(level.price for level in book.asks))
                / 2
                if book and book.bids and book.asks
                else None
            )
            if book:
                state.live_books[inv.plan.token_id] = LiveBook(
                    bids={level.price: level.size for level in book.bids},
                    asks={level.price: level.size for level in book.asks},
                )
            # Refresh metadata without losing the original entry prices.
            current = self.markets.get(inv.market.condition_id)
            if current:
                inv.market = current
                state.positions[current.condition_id].market = current
            # Account for every market before checking the session-wide loss limit.
            for order in inv.orders:
                await self.poll(inv, order)
            self.mirror(inv)
        transitioning = set()
        for inv in self.inventory.values():
            if inv.halted:
                await self.manage(inv, books.get(inv.plan.token_id), False, "strategy_halted")
                transitioning.add(inv.market.condition_id)
                continue
            if not await self.check_rewards(inv):
                transitioning.add(inv.market.condition_id)
                continue
            if await self.reconcile_cycle(inv):
                transitioning.add(inv.market.condition_id)
        if self.stopping_reason:
            await self.liquidate_session()
            return
        if should_kill(state):
            state.killed = True
            await self.emit(FarmErrorEvent(reason="extreme_max_session_loss; inventory retained"))
            return
        for inv in list(self.inventory.values()):
            if inv.market.condition_id in transitioning:
                continue
            current = self.markets.get(inv.market.condition_id)
            if inv.market.condition_id in state.excluded_markets:
                reason = "market_excluded"
            elif current is None:
                reason = "market_missing"
            elif current.end_date <= datetime.now(timezone.utc):
                reason = "market_expired"
            elif (
                current.rewards_rate_per_day <= 0
                or current.rewards_rate_per_day < state.config.filters.reward_min
            ):
                reason = "reward_below_minimum"
            else:
                reason = None
            book = books.get(inv.plan.token_id)
            await self.manage(inv, book, reason is None, reason)
            if state.killed:
                return
            if inv.halted and inv.shares == 0 and all(order.done for order in inv.orders):
                cid = inv.market.condition_id
                self.completed.add(cid)
                del self.inventory[cid]
                state.positions.pop(cid, None)
        allocated = sum((inv.budget for inv in self.inventory.values()), D(0))
        for market in candidates:
            if len(self.inventory) >= state.config.max_concurrent_positions:
                break
            if market.condition_id in state.excluded_markets or state.killed:
                continue
            try:
                market = await verify_market_rewards(http, market, state.config.filters.reward_min)
            except Exception:
                logger.warning("extreme_reward_verification_failed market=%s", market.slug)
                continue
            if market is None:
                continue
            reserve = state.config.extreme_fee_reserve
            budget = min(
                state.config.extreme_capital_budget + reserve,
                state.config.bankroll - allocated - max(D(0), state.session_loss),
                self.cash,
            )
            if budget <= reserve:
                continue
            cheap = (
                market.yes_token_id
                if mids[market.yes_token_id] <= D("0.05")
                else market.no_token_id
            )
            book = books.get(cheap)
            if book is None or not book.bids or not book.asks:
                continue
            market = market.model_copy(
                update={
                    "tick_size": book.tick_size,
                    "min_order_size": book.min_order_size,
                }
            )
            config = ExtremePriceRewardConfig(
                min_reward_per_day=max(D("0.00001"), state.config.filters.reward_min),
                capital_budget=budget - reserve,
                target_shares=state.config.extreme_target_shares,
            )
            live_midpoints = {}
            for token in (market.yes_token_id, market.no_token_id):
                live = books.get(token)
                if live is not None and live.bids and live.asks:
                    live_midpoints[token] = (
                        max(level.price for level in live.bids)
                        + min(level.price for level in live.asks)
                    ) / 2
            if len(live_midpoints) != 2:
                continue
            mid = reward_midpoint(book, market.rewards_min_size)
            if mid is None:
                continue
            plan = build_entry_plan(
                market,
                config,
                yes_midpoint=live_midpoints[market.yes_token_id],
                no_midpoint=live_midpoints[market.no_token_id],
                cheap_best_ask=min(level.price for level in book.asks),
                cheap_best_bid=max(level.price for level in book.bids),
                reward_midpoint=mid,
            )
            if plan is None or plan.token_id != cheap:
                continue
            if (
                mid is None
                or build_quotes(
                    plan,
                    available_shares=plan.shares,
                    available_cash=plan.rebuy_budget,
                    reward_midpoint=mid,
                    max_spread_cents=market.rewards_max_spread_cents,
                )
                is None
            ):
                continue
            if taker_fee(plan.shares, plan.entry_price, market.taker_fee_rate) > reserve:
                continue
            # Do not sell or mix a user's pre-existing YES or NO holdings into this session.
            if (
                await self.client.conditional_balance(market.yes_token_id) > 0
                or await self.client.conditional_balance(market.no_token_id) > 0
            ):
                continue
            inv = Inventory(market, plan, budget)
            self.inventory[market.condition_id] = inv
            state.positions[market.condition_id] = MarketPosition(
                market=market,
                yes_order_id="",
                no_order_id="",
                yes_price=plan.entry_price if plan.outcome == "YES" else D(0),
                no_price=plan.entry_price if plan.outcome == "NO" else D(0),
                strategy_capital=D(0),
            )
            allocated += budget
            await self.place(inv, plan.entry_order, entry=True)


async def cancel_and_verify(client, reason="startup"):
    """Cancel all orders, checking the exchange rather than assuming HTTP success."""
    for attempt in range(3):
        try:
            logger.info("extreme_cancel_all_requested reason=%s attempt=%d", reason, attempt + 1)
            await client.cancel_all()
            if not await client.get_open_order_ids():
                logger.info("extreme_cancel_all_confirmed reason=%s", reason)
                return
        except Exception:
            logger.exception("extreme: cancel-all attempt %d failed", attempt + 1)
        await asyncio.sleep(1)
    logger.error("extreme_cancel_all_unverified reason=%s", reason)
    raise RuntimeError("Could not verify cancellation; check wallet orders")


async def run_extreme(client, state, websocket, farm_session):
    runtime = ExtremeRuntime(client, state, websocket)
    tasks = []
    stop_reason = "shutdown"
    quote_wake = asyncio.Event()
    quote_task = None
    subscribed = set()
    try:
        await cancel_and_verify(client)
        state.starting_balance = await get_balance(state.wallet_address)
        from app.farm.worker import push_summary_loop

        tasks = [
            asyncio.create_task(heartbeat_loop(client)),
            asyncio.create_task(rewards_poll_loop(client, state)),
            asyncio.create_task(push_summary_loop(websocket, state)),
        ]
        async with httpx.AsyncClient(timeout=30) as http:
            while not state.killed:
                for task in tasks:
                    if task.done() and not task.cancelled() and task.exception() is not None:
                        raise RuntimeError(
                            "Extreme worker background task failed"
                        ) from task.exception()
                # Any unknown submission/poll result stops the session. No blind retries.
                await runtime.tick(http)
                if state.killed:
                    stop_reason = runtime.stopping_reason or "session_loss"
                if not state.killed:
                    tokens = {
                        inv.plan.token_id for inv in runtime.inventory.values() if not inv.halted
                    }
                    if tokens != subscribed:
                        if quote_task is not None:
                            quote_task.cancel()
                            await asyncio.gather(quote_task, return_exceptions=True)
                        quote_task = (
                            asyncio.create_task(watch_quote_changes(sorted(tokens), quote_wake))
                            if tokens
                            else None
                        )
                        subscribed = tokens
                    if quote_task is not None and quote_task.done():
                        await quote_task
                        raise RuntimeError("Quote subscription ended unexpectedly")
                    try:
                        await asyncio.wait_for(quote_wake.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        pass
                    quote_wake.clear()
    except asyncio.CancelledError:
        raise
    except Exception:
        stop_reason = "worker_error"
        logger.exception("extreme worker stopped; cancelling orders and retaining inventory")
        await runtime.emit(
            FarmErrorEvent(
                reason="extreme_worker_stopped; orders cancelled if reachable; inventory retained"
            )
        )
    finally:
        state.killed = True
        if quote_task is not None:
            tasks.append(quote_task)
        if runtime.discovery_task is not None:
            tasks.append(runtime.discovery_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await cancel_and_verify(client, reason=stop_reason)
            logger.info("extreme stopped: resting orders cancelled; wallet inventory retained")
        except Exception:
            await runtime.emit(FarmErrorEvent(reason="cancel_unverified; check wallet orders"))
            raise
        finally:
            farm_session.state = None
