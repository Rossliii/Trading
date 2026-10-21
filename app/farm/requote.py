import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal

import httpx
from fastapi import WebSocket

from app.api.farm.messages import OrderCancelledEvent, OrderPlacedEvent
from app.api.messages import send_event
from app.bot.cancel import cancel_order, cancel_orders
from app.bot.market_ws import stream_market_events
from app.bot.orders import get_open_order_ids
from app.bot.schemas import (
    BestBidAsk,
    BookLevel,
    BookSnapshot,
    LastTradePrice,
    LimitOrder,
    PriceChange,
    TickSizeChange,
)
from app.bot.trader import place_limit_order
from app.constants import (
    BEST_BID_GAP_PULL_FRACTION,
    QUEUE_SURF_MIN_HOLD_SECONDS,
    REQUOTE_CANCEL_FAIL_PULL_THRESHOLD,
    REQUOTE_GIVEUP_COOLDOWN_SECONDS,
    REQUOTE_HYSTERESIS_EPISODE_GAP_SECONDS,
    REQUOTE_HYSTERESIS_SECONDS,
    REQUOTE_TICK_BUFFER,
)
from app.farm.discovery import fetch_midpoints
from app.farm.exit_cost import immediate_sell_loss, top_of_book_fill_loss
from app.farm.gating import quote_block_reason, should_quote
from app.farm.health import mark_guard_pulled, mark_paused, record_guard_trip, record_market_failure
from app.farm.kill_switch import should_kill, trigger_kill
from app.farm.queue_surf import pick_surf_level, should_resurf
from app.farm.quoting import compute_quote, size_per_market
from app.farm.schemas import FarmState, LiveBook, MarketPosition, OrderInfo
from app.farm.volatility import blacklist_for_gap, record_price_sample
from app.infra.strategy_log import strat
from app.types import QuoteDepth

logger = logging.getLogger(__name__)


async def watch_quote_changes(token_ids: list[str], wake: asyncio.Event) -> None:
    """Wake the inventory worker on market updates, without running legacy trading hooks.

    The worker serializes order actions and verifies a fresh REST book before quoting.
    Event coalescing prevents concurrent cancel/replace operations on the same order.
    """
    async for event in stream_market_events(token_ids):
        if isinstance(event, (BestBidAsk, BookSnapshot, PriceChange, TickSizeChange)):
            wake.set()


async def old_order_is_live(client, old_oid: str) -> bool | None:
    """True if old_oid still rests server-side, False if confirmed gone, None if the poll failed."""
    if not old_oid:
        return False
    try:
        live = await get_open_order_ids(client)
    except Exception:
        logger.warning("requote liveness poll failed for old_oid=%s — treating as unknown", old_oid)
        return None
    return old_oid in live


async def respawn_market_ws(
    current_task: asyncio.Task | None,
    http: httpx.AsyncClient,
    client,
    state: FarmState,
    websocket: WebSocket,
) -> asyncio.Task | None:
    if current_task is not None:
        current_task.cancel()
        await asyncio.gather(current_task, return_exceptions=True)
    token_ids = list(
        {
            t
            for pos in state.positions.values()
            for t in (pos.market.yes_token_id, pos.market.no_token_id)
        }
    )
    if not token_ids:
        return None
    keep = set(token_ids)
    state.live_books = {t: b for t, b in state.live_books.items() if t in keep}
    return asyncio.create_task(market_ws_loop(http, client, state, websocket, token_ids))


def pos_for_token(state: FarmState, token_id: str) -> MarketPosition | None:
    return next(
        (
            p
            for p in state.positions.values()
            if token_id in (p.market.yes_token_id, p.market.no_token_id)
        ),
        None,
    )


def is_flip_flop_requote(
    our_price: Decimal,
    new_price: Decimal,
    prev_price: Decimal | None,
    prev_at: datetime | None,
    tick: Decimal,
    now: datetime,
    window_seconds: float,
) -> bool:
    """True when moving our_price->new_price is a 1-tick flip-flop back to the price we just left,
    still inside the hysteresis window. Pure: the caller must ALSO confirm the current price is
    in-band before suppressing."""
    if prev_price is None or prev_at is None:
        return False
    if abs(new_price - our_price) != tick:
        return False
    if new_price != prev_price:
        return False
    return (now - prev_at).total_seconds() <= window_seconds


def apply_book_snapshot(state: FarmState, event: BookSnapshot) -> None:
    """Replace a token's live book from a full WS `book` snapshot."""
    state.live_books[event.asset_id] = LiveBook(
        bids={lvl.price: lvl.size for lvl in event.bids if lvl.size > 0},
        asks={lvl.price: lvl.size for lvl in event.asks if lvl.size > 0},
    )


def apply_price_change(state: FarmState, change) -> None:
    """Apply one incremental `price_change` level delta to a token's live book (size 0 = remove)."""
    book = state.live_books.setdefault(change.asset_id, LiveBook())
    side = book.bids if change.side == "BUY" else book.asks
    if change.size <= 0:
        side.pop(change.price, None)
    else:
        side[change.price] = change.size


async def depth_fill_loss_guard(
    client, state: FarmState, websocket: WebSocket, pos: MarketPosition, token_id: str
) -> bool:
    """Depth-aware exit-loss guard: walks the live book and, if a fill on this leg followed by an
    immediate dump into the current bids would lose more than max_fill_loss, pulls both legs. Off
    when max_fill_loss is unset. Returns True if it pulled."""
    max_fill_loss = state.config.filters.max_fill_loss
    if max_fill_loss is None:
        return False
    if pos.quotes_pulled:
        return False
    book = state.live_books.get(token_id)
    if book is None or not book.bids:
        return False
    is_yes = token_id == pos.market.yes_token_id
    our_px = pos.yes_price if is_yes else pos.no_price
    bids = [BookLevel(price=p, size=s) for p, s in book.bids.items()]
    size = size_per_market(pos.market)
    loss = immediate_sell_loss(bids, our_px, size, pos.market.taker_fee_rate)
    if loss > max_fill_loss:
        logger.warning(
            "depth-guard cancel slug=%s leg=%s est_loss=$%.2f cap=$%s our_px=%s live_bid_levels=%d",
            pos.market.slug,
            "YES" if is_yes else "NO",
            float(loss),
            max_fill_loss,
            our_px,
            len(bids),
        )
        strat(
            "depth_guard_pull",
            slug=pos.market.slug,
            cid=pos.market.condition_id,
            leg="YES" if is_yes else "NO",
            est_loss=float(loss),
            cap=max_fill_loss,
            levels=len(bids),
        )
        mark_guard_pulled(state, pos.market.condition_id)
        record_guard_trip(state, pos.market.condition_id, loss)
        await cancel_position_orders(client, state, websocket, pos)
        return True
    return False


async def handle_book_snapshot(
    client, state: FarmState, websocket: WebSocket, pos: MarketPosition, event: BookSnapshot
) -> None:
    if state.killed:
        return
    apply_book_snapshot(state, event)
    await depth_fill_loss_guard(client, state, websocket, pos, event.asset_id)


async def handle_price_change(
    client, state: FarmState, websocket: WebSocket, event: PriceChange
) -> None:
    if state.killed:
        return
    affected: set[str] = set()
    for change in event.price_changes:
        apply_price_change(state, change)
        affected.add(change.asset_id)
    for token_id in affected:
        pos = pos_for_token(state, token_id)
        if pos is not None:
            await depth_fill_loss_guard(client, state, websocket, pos, token_id)


async def market_ws_loop(
    http: httpx.AsyncClient,
    client,
    state: FarmState,
    websocket: WebSocket,
    token_ids: list[str],
) -> None:
    async for event in stream_market_events(token_ids):
        if isinstance(event, PriceChange):
            await handle_price_change(client, state, websocket, event)
            continue
        pos = pos_for_token(state, event.asset_id)
        if pos is None:
            continue
        if isinstance(event, BestBidAsk):
            await handle_bba(client, state, websocket, pos, event)
        elif isinstance(event, BookSnapshot):
            await handle_book_snapshot(client, state, websocket, pos, event)
        elif isinstance(event, TickSizeChange):
            await handle_tick_size_change(http, client, state, websocket, pos, event)
        elif isinstance(event, LastTradePrice):
            handle_last_trade(pos, event)


def handle_last_trade(pos: MarketPosition, event: LastTradePrice) -> None:
    """A trade at our resting price means FIFO consumed orders ahead of us (we moved up toward
    the front) — flag the leg (yes/no_moved_up) for the queue-surf trigger."""
    if event.asset_id == pos.market.yes_token_id:
        if event.price == pos.yes_price and pos.yes_order_id and not pos.yes_moved_up:
            pos.yes_moved_up = True
            strat(
                "surf_moved_up",
                slug=pos.market.slug,
                cid=pos.market.condition_id,
                leg="YES",
                px=event.price,
                trade_size=event.size,
            )
    elif event.asset_id == pos.market.no_token_id:
        if event.price == pos.no_price and pos.no_order_id and not pos.no_moved_up:
            pos.no_moved_up = True
            strat(
                "surf_moved_up",
                slug=pos.market.slug,
                cid=pos.market.condition_id,
                leg="NO",
                px=event.price,
                trade_size=event.size,
            )


async def maybe_surf_requote(
    client,
    state: FarmState,
    websocket: WebSocket,
    pos: MarketPosition,
    is_yes_leg: bool,
    midpoint: Decimal,
) -> bool:
    """Queue-surf re-quote: on moved_up + rested past min_hold + a deep in-band level, cancel and
    re-place there → land at the BACK of the FIFO queue. Returns True if it re-quoted."""
    moved_up = pos.yes_moved_up if is_yes_leg else pos.no_moved_up
    if not moved_up:
        return False
    oid = pos.yes_order_id if is_yes_leg else pos.no_order_id
    info = state.order_registry.get(oid)
    if info is None:
        return False
    age = (datetime.now(timezone.utc) - info.placed_at).total_seconds()
    token_id = pos.market.yes_token_id if is_yes_leg else pos.market.no_token_id
    book = state.live_books.get(token_id)
    bids = list(book.bids.items()) if book is not None else []
    level = pick_surf_level(
        bids,
        pos.market.rewards_min_size,
        midpoint,
        pos.market.rewards_max_spread_cents,
        Decimal(1),
    )
    if not should_resurf(age, moved_up, level is not None, QUEUE_SURF_MIN_HOLD_SECONDS):
        return False
    outcome = "YES" if is_yes_leg else "NO"
    new_px, depth_ahead = level
    strat(
        "surf_requote",
        slug=pos.market.slug,
        cid=pos.market.condition_id,
        leg=outcome,
        old_px=pos.yes_price if is_yes_leg else pos.no_price,
        new_px=new_px,
        depth_ahead=depth_ahead,
        dist_cents=(midpoint - new_px) * 100,
        age_s=round(age),
    )
    await requote_leg(client, state, websocket, pos, outcome, new_px)
    return True


def leg_quote_depth(state: FarmState, pos: MarketPosition) -> QuoteDepth:
    return pos.market.adaptive_depth or pos.market.effective_depth or state.config.quote_depth


async def handle_bba(
    client,
    state: FarmState,
    websocket: WebSocket,
    pos: MarketPosition,
    event: BestBidAsk,
) -> None:
    is_yes_leg = event.asset_id == pos.market.yes_token_id
    prev_leg_bid = pos.yes_best_bid if is_yes_leg else pos.no_best_bid

    pos.last_best_bid = event.best_bid
    pos.last_best_ask = event.best_ask
    if is_yes_leg:
        pos.yes_best_bid = event.best_bid
    else:
        pos.no_best_bid = event.best_bid

    if not state.killed and should_kill(state):
        await trigger_kill(client, state, websocket)
    if state.killed:
        return

    if prev_leg_bid is not None and prev_leg_bid > 0:
        gap = prev_leg_bid - event.best_bid
        if (
            gap > BEST_BID_GAP_PULL_FRACTION * prev_leg_bid
            and gap > 2 * pos.market.tick_size
            and not pos.quotes_pulled
        ):
            logger.warning(
                "best-bid gap-pull slug=%s leg=%s prev_bid=%s new_bid=%s gap=%s — crash, "
                "pulling orders + blacklisting",
                pos.market.slug,
                "YES" if is_yes_leg else "NO",
                prev_leg_bid,
                event.best_bid,
                gap,
            )
            strat(
                "gap_pull",
                slug=pos.market.slug,
                cid=pos.market.condition_id,
                leg="YES" if is_yes_leg else "NO",
                prev_bid=prev_leg_bid,
                new_bid=event.best_bid,
                gap=gap,
            )
            blacklist_for_gap(state, pos.market.condition_id)
            await cancel_position_orders(client, state, websocket, pos)
            return

    if event.asset_id == pos.market.yes_token_id:
        midpoint = (event.best_bid + event.best_ask) / 2
        tier = record_price_sample(state, pos.market.condition_id, midpoint)
        if tier is not None and not pos.quotes_pulled:
            logger.warning(
                "volatility pull slug=%s tier=%s — cancelling resting legs", pos.market.slug, tier
            )
            strat("vol_pull", slug=pos.market.slug, cid=pos.market.condition_id, tier=tier)
            await cancel_position_orders(client, state, websocket, pos)

    max_fill_loss = state.config.filters.max_fill_loss
    if max_fill_loss is not None:
        is_yes_leg = event.asset_id == pos.market.yes_token_id
        our_px = pos.yes_price if is_yes_leg else pos.no_price
        loss_est = top_of_book_fill_loss(pos.market, our_px, event.best_bid)
        if loss_est > max_fill_loss and not pos.quotes_pulled:
            logger.warning(
                "exit-loss cancel slug=%s outcome=%s est_loss=$%.2f cap=$%s our_px=%s best_bid=%s "
                "best_ask=%s spread=%s",
                pos.market.slug,
                "YES" if is_yes_leg else "NO",
                float(loss_est),
                max_fill_loss,
                our_px,
                event.best_bid,
                event.best_ask,
                event.spread,
            )
            strat(
                "exit_loss_pull",
                slug=pos.market.slug,
                cid=pos.market.condition_id,
                leg="YES" if is_yes_leg else "NO",
                est_loss=float(loss_est),
                cap=max_fill_loss,
                best_bid=event.best_bid,
                best_ask=event.best_ask,
                spread=event.spread,
            )
            mark_guard_pulled(state, pos.market.condition_id)
            record_guard_trip(state, pos.market.condition_id, loss_est)
            await cancel_position_orders(client, state, websocket, pos)
            return

    block = quote_block_reason(state, pos.market.condition_id, pos.market.event_slug)
    if block is not None:
        logger.debug("quote-gate skip slug=%s reason=%s", pos.market.slug, block)
        return

    is_yes = event.asset_id == pos.market.yes_token_id
    outcome = "YES" if is_yes else "NO"
    our_price = pos.yes_price if is_yes else pos.no_price
    tick = pos.market.tick_size
    max_spread = pos.market.rewards_max_spread_cents / 100

    midpoint = (event.best_bid + event.best_ask) / 2

    if await maybe_surf_requote(client, state, websocket, pos, is_yes, midpoint):
        return

    threatened = event.best_bid - our_price <= REQUOTE_TICK_BUFFER * tick
    out_of_zone = abs(midpoint - our_price) > max_spread

    if not threatened and not out_of_zone:
        return

    logger.debug(
        "bba slug=%s outcome=%s best_bid=%s best_ask=%s our_px=%s threatened=%s out_of_zone=%s",
        pos.market.slug,
        outcome,
        event.best_bid,
        event.best_ask,
        our_price,
        threatened,
        out_of_zone,
    )
    depth = leg_quote_depth(state, pos)
    new_quote = compute_quote(pos.market, midpoint, depth)
    if new_quote is None:
        return
    new_price = new_quote[0]

    if new_price == our_price:
        return
    if depth == "safe" and not out_of_zone and new_price > our_price:
        return

    safe_price = min(new_price, event.best_ask - tick)
    if safe_price <= 0:
        return
    if safe_price == our_price:
        return

    if (
        max_fill_loss is not None
        and top_of_book_fill_loss(pos.market, safe_price, event.best_bid) > max_fill_loss
    ):
        logger.debug(
            "requote skip (cap) slug=%s outcome=%s safe_px=%s best_bid=%s cap=$%s",
            pos.market.slug,
            outcome,
            safe_price,
            event.best_bid,
            max_fill_loss,
        )
        return

    prev_px = pos.yes_prev_price if is_yes else pos.no_prev_price
    prev_at = pos.yes_prev_price_at if is_yes else pos.no_prev_price_at
    now = datetime.now(timezone.utc)
    current_in_band = not out_of_zone and our_price < event.best_ask and not pos.quotes_pulled
    if current_in_band and is_flip_flop_requote(
        our_price, safe_price, prev_px, prev_at, tick, now, REQUOTE_HYSTERESIS_SECONDS
    ):
        logger.debug(
            "requote hysteresis skip slug=%s leg=%s held_px=%s skip_target=%s window_s=%s",
            pos.market.slug,
            outcome,
            our_price,
            safe_price,
            REQUOTE_HYSTERESIS_SECONDS,
        )
        episode_at = pos.yes_hysteresis_episode_at if is_yes else pos.no_hysteresis_episode_at
        if episode_at is None or (
            (now - episode_at).total_seconds() > REQUOTE_HYSTERESIS_EPISODE_GAP_SECONDS
        ):
            strat(
                "requote_hysteresis",
                slug=pos.market.slug,
                cid=pos.market.condition_id,
                leg=outcome,
                held_px=our_price,
                other_px=safe_price,
            )
        if is_yes:
            pos.yes_hysteresis_episode_at = now
        else:
            pos.no_hysteresis_episode_at = now
        return

    await requote_leg(client, state, websocket, pos, outcome, safe_price)


async def handle_tick_size_change(
    http: httpx.AsyncClient,
    client,
    state: FarmState,
    websocket: WebSocket,
    pos: MarketPosition,
    event: TickSizeChange,
) -> None:
    if state.killed:
        return
    pos.market.tick_size = event.new_tick_size
    midpoints = await fetch_midpoints(http, [pos.market.yes_token_id, pos.market.no_token_id])
    yes_mid = midpoints.get(pos.market.yes_token_id)
    no_mid = midpoints.get(pos.market.no_token_id)
    depth = leg_quote_depth(state, pos)
    if yes_mid is not None:
        new_quote = compute_quote(pos.market, yes_mid, depth)
        if new_quote is not None:
            await requote_leg(client, state, websocket, pos, "YES", new_quote[0])
    if no_mid is not None:
        new_quote = compute_quote(pos.market, no_mid, depth)
        if new_quote is not None:
            await requote_leg(client, state, websocket, pos, "NO", new_quote[0])


async def requote_leg(
    client,
    state: FarmState,
    websocket: WebSocket,
    pos: MarketPosition,
    outcome: str,
    new_price: Decimal,
) -> None:
    if outcome == "YES":
        old_oid = pos.yes_order_id
        token_id = pos.market.yes_token_id
        old_px = pos.yes_price
    else:
        old_oid = pos.no_order_id
        token_id = pos.market.no_token_id
        old_px = pos.no_price

    fails = pos.yes_requote_cancel_fails if outcome == "YES" else pos.no_requote_cancel_fails

    def set_fails(n: int) -> None:
        if outcome == "YES":
            pos.yes_requote_cancel_fails = n
        else:
            pos.no_requote_cancel_fails = n

    if old_oid:  # a falsy old_oid means we pulled it ourselves — nothing to cancel or announce
        try:
            await cancel_order(client, old_oid)
            set_fails(0)
        except Exception:
            live = await old_order_is_live(client, old_oid)
            if pos.quotes_pulled:
                if live is False:
                    logger.warning(
                        "requote pre-cancel of already-pulled leg failed (%s/%s old_oid=%s) but "
                        "old order confirmed gone server-side — placing replacement to self-heal",
                        pos.market.slug,
                        outcome,
                        old_oid,
                    )
                else:
                    logger.warning(
                        "requote pre-cancel of already-pulled leg failed (%s/%s old_oid=%s) and "
                        "old order still rests or liveness unknown (live=%s) — skipping self-heal "
                        "place to avoid double-rest",
                        pos.market.slug,
                        outcome,
                        old_oid,
                        live,
                    )
                    return
            elif live is False:
                set_fails(0)
                logger.warning(
                    "requote cancel raised but old_oid=%s confirmed gone server-side for %s/%s "
                    "— self-healing (placing replacement)",
                    old_oid,
                    pos.market.slug,
                    outcome,
                )
            else:
                n = fails + 1
                set_fails(n)
                if n >= REQUOTE_CANCEL_FAIL_PULL_THRESHOLD:
                    logger.error(
                        "requote cancel failed %d× for %s/%s (old_oid=%s likely still live) "
                        "— pulling position to break the loop",
                        n,
                        pos.market.slug,
                        outcome,
                        old_oid,
                    )
                    strat(
                        "requote_cancel_giveup",
                        slug=pos.market.slug,
                        cid=pos.market.condition_id,
                        leg=outcome,
                        fails=n,
                    )
                    pos.yes_requote_cancel_fails = 0
                    pos.no_requote_cancel_fails = 0
                    mark_guard_pulled(
                        state, pos.market.condition_id, cooldown_s=REQUOTE_GIVEUP_COOLDOWN_SECONDS
                    )
                    await cancel_position_orders(client, state, websocket, pos)
                    still_live = await old_order_is_live(client, old_oid)
                    if still_live is False:
                        logger.info(
                            "give-up pull: old_oid=%s confirmed gone after batch cancel (%s/%s)",
                            old_oid,
                            pos.market.slug,
                            outcome,
                        )
                    else:
                        logger.warning(
                            "give-up pull: old_oid=%s for %s/%s may STILL REST server-side "
                            "(live=%s) — untracked orphan; batch outcome is swallowed and deposit "
                            "wallets have no heartbeat deadman, cancel manually if it persists",
                            old_oid,
                            pos.market.slug,
                            outcome,
                            still_live,
                        )
                        strat(
                            "giveup_orphan",
                            slug=pos.market.slug,
                            cid=pos.market.condition_id,
                            leg=outcome,
                            oid=old_oid,
                            live=str(still_live),
                        )
                else:
                    logger.warning(
                        "requote aborted for %s/%s — old_oid=%s retained (live=%s), no new quote "
                        "placed [fail %d/%d]",
                        pos.market.slug,
                        outcome,
                        old_oid,
                        live,
                        n,
                        REQUOTE_CANCEL_FAIL_PULL_THRESHOLD,
                    )
                return

        await send_event(
            websocket,
            OrderCancelledEvent(
                market_id=pos.market.condition_id,
                slug=pos.market.slug,
                outcome=outcome,
                order_id=old_oid,
                reason="requote",
            ),
        )

    if not should_quote(state, pos):
        logger.info(
            "requote aborted pre-place (market quarantined mid-requote) %s/%s",
            pos.market.slug,
            outcome,
        )
        return

    size = float(size_per_market(pos.market))
    new_order = LimitOrder(
        token_id=token_id,
        side="BUY",
        size=size,
        price=float(new_price),
    )
    try:
        new_oid = await place_limit_order(client, new_order, post_only=True)
    except Exception:
        logger.exception("requote place failed for %s/%s", pos.market.slug, outcome)
        if record_market_failure(state, pos.market.condition_id):
            mark_paused(state, pos.market.condition_id)
            await cancel_position_orders(client, state, websocket, pos)
        return

    state.order_registry[new_oid] = OrderInfo(
        condition_id=pos.market.condition_id, outcome=outcome, token_id=token_id
    )

    if not should_quote(state, pos):
        try:
            await cancel_order(client, new_oid)
        except Exception:
            logger.exception("requote retract cancel failed for %s/%s", pos.market.slug, outcome)
        logger.info(
            "requote retracted post-place (market quarantined during place) %s/%s",
            pos.market.slug,
            outcome,
        )
        return

    retired = state.order_registry.get(old_oid)
    if retired is not None:
        retired.retired_at = datetime.now(timezone.utc)

    now = datetime.now(timezone.utc)
    if outcome == "YES":
        pos.yes_prev_price = old_px
        pos.yes_prev_price_at = now
        pos.yes_order_id = new_oid
        pos.yes_price = new_price
        pos.yes_moved_up = False
    else:
        pos.no_prev_price = old_px
        pos.no_prev_price_at = now
        pos.no_order_id = new_oid
        pos.no_price = new_price
        pos.no_moved_up = False
    pos.quotes_pulled = False
    pos.quotes_pulled_at = None
    set_fails(0)

    logger.info(
        "requote slug=%s outcome=%s old_px=%s new_px=%s",
        pos.market.slug,
        outcome,
        old_px,
        new_price,
    )
    mid = (
        (pos.last_best_bid + pos.last_best_ask) / 2
        if pos.last_best_bid is not None and pos.last_best_ask is not None
        else None
    )
    strat(
        "quote",
        slug=pos.market.slug,
        outcome=outcome,
        px=new_price,
        size=size,
        mid=mid if mid is not None else "na",
        max_spread=pos.market.rewards_max_spread_cents,
        tick=pos.market.tick_size,
        depth=leg_quote_depth(state, pos),
        tier=pos.market.effective_depth or state.config.quote_depth,
    )

    size_decimal = Decimal(str(size))
    await send_event(
        websocket,
        OrderPlacedEvent(
            market_id=pos.market.condition_id,
            slug=pos.market.slug,
            question=pos.market.question,
            outcome=outcome,
            side="BUY",
            price=new_price,
            size=size_decimal,
            capital_locked=size_decimal * new_price,
            order_id=new_oid,
        ),
    )


async def cancel_position_orders(
    client, state: FarmState, websocket: WebSocket, pos: MarketPosition
) -> None:
    """Pull both resting legs and leave clean state: batch-cancel, retire the registry entries,
    clear the stored ids, latch quotes_pulled (+timestamp), and tell the UI."""
    yes_oid, no_oid = pos.yes_order_id, pos.no_order_id
    await cancel_orders(client, yes_oid, no_oid)
    now = datetime.now(timezone.utc)
    for oid in (yes_oid, no_oid):
        info = state.order_registry.get(oid)
        if info is not None:
            info.retired_at = now  # starts the existing retention clock; late fills still resolve
    pos.yes_order_id = ""
    pos.no_order_id = ""
    pos.quotes_pulled = True
    pos.quotes_pulled_at = now
    for oid, outcome in ((yes_oid, "YES"), (no_oid, "NO")):
        if not oid:
            continue
        await send_event(
            websocket,
            OrderCancelledEvent(
                market_id=pos.market.condition_id,
                slug=pos.market.slug,
                outcome=outcome,
                order_id=oid,
                reason="guard_pulled",
            ),
        )
