from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

import httpx
from fastapi import WebSocket

if TYPE_CHECKING:
    from app.api.farm.handlers import FarmSession

from app.api.farm.messages import (
    FarmErrorEvent,
    FarmPositionEntry,
    FarmPositionsEvent,
    FarmSummaryEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
)
from app.api.messages import send_event
from app.bot.balance import get_balance
from app.bot.cancel import (
    cancel_order,
    cancel_order_with_retry,
    cancel_orders,
)
from app.bot.execution import build_execution_client
from app.bot.orders import get_open_order_ids
from app.bot.schemas import LimitOrder
from app.bot.setup import ensure_approval
from app.bot.trader import place_limit_order
from app.constants import (
    BALANCE_FETCH_TIMEOUT_SECONDS,
    DEFAULT_MIN_BID_DEPTH_MULT,
    EXIT_RECONCILE_SECONDS,
    EXIT_RETRY_SECONDS,
    GUARD_PULL_RELEASE_SECONDS,
    KILL_DRAIN_POLL_SECONDS,
    KILL_DRAIN_TIMEOUT_SECONDS,
    OPEN_FAILURE_PAUSE_THRESHOLD,
    ORDER_REGISTRY_RETENTION_SECONDS,
    POSITION_CANDIDATE_MISS_TICKS,
    POSITION_PRUNE_MISSING_TICKS,
    POST_FILL_SAMPLE_INTERVAL_SECONDS,
    POST_FILL_SAMPLE_SECONDS,
    SUMMARY_INTERVAL_SECONDS,
)
from app.db.blacklist import list_blacklist
from app.db.wallets import Wallet
from app.exceptions import BlacklistPersistenceError, WalletNotDeployedError
from app.farm.discovery import (
    fetch_books,
    fetch_eligible_markets,
    fetch_midpoints,
    fetch_open_positions,
    fetch_price_ranges,
)
from app.farm.exit_cost import market_exit_loss
from app.farm.exits import exit_held_legs, exit_position_leg, reconcile_onchain_positions
from app.farm.filters import first_failing_filter, passes_all, passes_range_24h
from app.farm.health import in_guard_pull_cooldown, is_event_excluded, is_paused, mark_paused
from app.farm.kill_switch import should_kill, trigger_kill, unrealized_loss
from app.farm.queue_surf import is_sole_qualifier
from app.farm.quoting import (
    compute_quote,
    resolve_tier,
    size_per_market,
    two_leg_cost,
)
from app.farm.schemas import (
    FarmConfig,
    FarmState,
    Market,
    MarketHealth,
    MarketPosition,
    OrderInfo,
)
from app.farm.sports_schedule import annotate_sports_gate
from app.farm.volatility import is_blacklisted, reevaluate_blacklist
from app.farm.zone_liquidity import book_bid_depth, zone_distribution, zone_liquidity_usd
from app.infra.strategy_log import strat
from app.types import QuoteDepth

logger = logging.getLogger(__name__)


async def annotate_live_metrics(
    http: httpx.AsyncClient,
    markets: list[Market],
    *,
    need_books: bool,
    need_exit_loss: bool = False,
    quote_depth: QuoteDepth = "safe",
) -> dict[str, Decimal]:
    token_ids = [t for m in markets for t in (m.yes_token_id, m.no_token_id)]
    if need_books:
        midpoints, books = await asyncio.gather(
            fetch_midpoints(http, token_ids),
            fetch_books(http, token_ids),
        )
    else:
        midpoints = await fetch_midpoints(http, token_ids)
        books = {}
    for m in markets:
        yes_mid = midpoints.get(m.yes_token_id)
        no_mid = midpoints.get(m.no_token_id)
        if yes_mid is None or no_mid is None:
            continue
        m.midpoint = yes_mid
        if need_books:
            yes_book = books.get(m.yes_token_id)
            no_book = books.get(m.no_token_id)
            if yes_book is not None and no_book is not None:
                m.zone_liquidity = zone_liquidity_usd(m, yes_book, yes_mid, no_book, no_mid)
                band = m.rewards_max_spread_cents / Decimal(100)
                m.yes_bid_depth = book_bid_depth(yes_book, yes_mid, band)
                m.no_bid_depth = book_bid_depth(no_book, no_mid, band)
                m.adaptive_depth = (
                    "safe"
                    if is_sole_qualifier(
                        [(lvl.price, lvl.size) for lvl in yes_book.bids],
                        m.rewards_min_size,
                        yes_mid,
                        m.rewards_max_spread_cents,
                    )
                    and is_sole_qualifier(
                        [(lvl.price, lvl.size) for lvl in no_book.bids],
                        m.rewards_min_size,
                        no_mid,
                        m.rewards_max_spread_cents,
                    )
                    else None
                )
            if need_exit_loss:
                depth = m.effective_depth or quote_depth
                m.exit_loss = market_exit_loss(m, yes_book, yes_mid, no_book, no_mid, depth)
    return midpoints


def deployed_capital(state: FarmState) -> Decimal:
    total = Decimal(0)
    for pos in state.positions.values():
        if pos.strategy_capital is not None:
            total += pos.strategy_capital
            continue
        size = size_per_market(pos.market)
        total += size * pos.yes_price + size * pos.no_price
    return total


def build_position_entries(state: FarmState) -> list[FarmPositionEntry]:
    entries: list[FarmPositionEntry] = []
    for cid, pos in state.positions.items():
        size = size_per_market(pos.market)
        mid = (
            (pos.last_best_bid + pos.last_best_ask) / 2
            if pos.last_best_bid is not None and pos.last_best_ask is not None
            else pos.market.midpoint
        )
        unrealized = Decimal(0)
        if mid is not None:
            unrealized = (
                pos.yes_shares * mid
                + pos.no_shares * (Decimal(1) - mid)
                - pos.yes_cost_basis
                - pos.no_cost_basis
            )
        entries.append(
            FarmPositionEntry(
                market_id=cid,
                slug=pos.market.slug,
                question=pos.market.question,
                event_slug=pos.market.event_slug,
                yes_price=pos.yes_price,
                no_price=pos.no_price,
                midpoint=mid,
                capital_deployed=(
                    pos.strategy_capital
                    if pos.strategy_capital is not None
                    else size * pos.yes_price + size * pos.no_price
                ),
                yes_shares=pos.yes_shares,
                no_shares=pos.no_shares,
                unrealized_pnl=unrealized,
                position_pnl=(
                    pos.position_pnl
                    if pos.strategy_capital is not None
                    else unrealized
                    if mid is not None
                    else None
                ),
                limit_orders=pos.resting_orders,
            )
        )
    return entries


def log_task_exit(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("background task %s died", task.get_name(), exc_info=exc)


async def reconcile_startup_positions(client, state: FarmState, http: httpx.AsyncClient) -> None:
    positions = await fetch_open_positions(http, state.wallet_address)
    if not positions:
        logger.info("startup reconcile: wallet holds no untracked positions — clean start")
        return
    logger.warning(
        "startup reconcile: wallet holds %d untracked position(s) — residue from a prior "
        "crash/restart; liquidating (LP-dedicated wallet assumed)",
        len(positions),
    )
    for p in positions:
        logger.warning(
            "startup reconcile: liquidating %s shares of %s/%s (token %s, avg %s)",
            p.size,
            p.slug,
            p.outcome,
            p.token_id,
            p.avg_price,
        )
        try:
            await exit_position_leg(
                client,
                state,
                p.token_id,
                p.size,
                p.condition_id,
                p.slug,
                p.outcome,
                entry_cost=p.size * p.avg_price,
            )
        except Exception:
            logger.exception("startup reconcile: liquidation failed for %s/%s", p.slug, p.outcome)


def apply_session_filter_defaults(config: FarmConfig) -> None:
    if config.filters.min_bid_depth_mult is None:
        config.filters.min_bid_depth_mult = DEFAULT_MIN_BID_DEPTH_MULT
        logger.info(
            "bid-depth filter unset by UI — defaulting min_bid_depth_mult=%s for this session",
            DEFAULT_MIN_BID_DEPTH_MULT,
        )


async def run_farm(
    websocket: WebSocket,
    config: FarmConfig,
    wallet: Wallet,
    license_key: str,
    farm_session: "FarmSession",
) -> None:
    """Run Extreme Price Reward Farming; legacy quote/exit loops are not started."""
    from app.farm.extreme_worker import run_extreme

    try:
        client = build_execution_client(wallet)
        if client.wallet_type != "DEPOSIT_WALLET":
            await ensure_approval(wallet.private_key)
        state = FarmState(config=config, wallet_address=wallet.proxy_address)
        # Fail closed if exclusions cannot be loaded.
        for row in await list_blacklist(license_key):
            state.excluded_markets.add(row.condition_id)
        farm_session.state = state
        logger.info("Starting Extreme Price Reward Farming (legacy strategy disabled)")
        await run_extreme(client, state, websocket, farm_session)
    except WalletNotDeployedError as exc:
        await send_event(
            websocket,
            FarmErrorEvent(reason=f"undeployed deposit wallet {wallet.proxy_address}: {exc}"),
        )
    except BlacklistPersistenceError:
        await send_event(
            websocket, FarmErrorEvent(reason="blacklist_unavailable; farm not started")
        )


def has_held_inventory(state: FarmState) -> bool:
    return any(pos.yes_shares > 0 or pos.no_shares > 0 for pos in state.positions.values())


async def await_inventory_drained(state: FarmState) -> None:
    polls = max(1, KILL_DRAIN_TIMEOUT_SECONDS // KILL_DRAIN_POLL_SECONDS)
    for _ in range(polls):
        if not has_held_inventory(state):
            logger.info("kill drain: all held inventory flattened; tearing down")
            return
        await asyncio.sleep(KILL_DRAIN_POLL_SECONDS)
    held = sum(1 for p in state.positions.values() if p.yes_shares > 0 or p.no_shares > 0)
    logger.warning(
        "kill drain: %d position(s) still held after %ds — abandoning to teardown (likely "
        "unsellable: no bid / sub-tick); chain reconcile or manual exit needed",
        held,
        KILL_DRAIN_TIMEOUT_SECONDS,
    )


async def exit_retry_loop(client, state: FarmState) -> None:
    while not state.killed or has_held_inventory(state):
        try:
            await exit_held_legs(
                client, state, cancel_resting=True, skip_in_flight=True, force_dump=state.killed
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("exit_retry_loop iteration failed; will retry")
        await asyncio.sleep(EXIT_RETRY_SECONDS)


async def exit_reconcile_loop(client, state: FarmState, http: httpx.AsyncClient) -> None:
    while not state.killed or has_held_inventory(state):
        try:
            await reconcile_onchain_positions(client, state, http)
        except asyncio.CancelledError:
            raise
        except RuntimeError as exc:
            if "client has been closed" in str(exc):
                logger.info("exit_reconcile_loop: http client closed; session ended — stopping")
                return
            logger.exception("exit_reconcile_loop iteration failed; will retry")
        except Exception:
            logger.exception("exit_reconcile_loop iteration failed; will retry")
        await asyncio.sleep(EXIT_RECONCILE_SECONDS)


async def post_fill_sampler_loop(http: httpx.AsyncClient, state: FarmState) -> None:
    """Pure instrumentation, read-only: watches every filled market's book for
    POST_FILL_SAMPLE_SECONDS after the fill, sampling every POST_FILL_SAMPLE_INTERVAL_SECONDS.
    This is the recovery-curve data gap — today the bot goes blind the instant it dumps a fill,
    so there's no way to tell whether a bid-vacuum crash recovers in time to make a "wait"
    exit worth it. Never places/cancels an order and never affects the kill switch; any failure
    is caught and logged so a bad sample can never take down the farm."""
    while not state.killed:
        try:
            now = datetime.now(timezone.utc)
            token_ids = list(state.book_samples.keys())
            if token_ids:
                books = await fetch_books(http, token_ids)
                for token_id in token_ids:
                    sample = state.book_samples.get(token_id)
                    if sample is None:
                        continue
                    elapsed_s = int((now - sample.started_at).total_seconds())
                    if elapsed_s > POST_FILL_SAMPLE_SECONDS:
                        state.book_samples.pop(token_id, None)
                        continue
                    book = books.get(token_id)
                    if book is None or not book.bids or not book.asks:
                        continue
                    best_bid = max(lvl.price for lvl in book.bids)
                    best_ask = min(lvl.price for lvl in book.asks)
                    mid = (best_bid + best_ask) / 2
                    spread = best_ask - best_bid
                    bid_depth = book_bid_depth(book, mid, Decimal("0.05"))
                    strat(
                        "book_sample",
                        slug=sample.slug,
                        token=token_id,
                        outcome=sample.outcome,
                        elapsed_s=elapsed_s,
                        entry_px=sample.entry_px,
                        best_bid=best_bid,
                        best_ask=best_ask,
                        mid=mid,
                        spread=spread,
                        bid_depth=bid_depth,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("post_fill_sampler_loop iteration failed; will retry")
        await asyncio.sleep(POST_FILL_SAMPLE_INTERVAL_SECONDS)


async def push_summary_loop(websocket: WebSocket, state: FarmState) -> None:
    # A failed/slow RPC read must never stall the loop — timeout, then resend last value.
    wallet_balance: Decimal | None = None
    while True:
        if state.killed:
            return
        cycle_start = asyncio.get_running_loop().time()
        try:
            wallet_balance = await asyncio.wait_for(
                get_balance(state.wallet_address), timeout=BALANCE_FETCH_TIMEOUT_SECONDS
            )
        except Exception as exc:
            logger.debug("summary wallet-balance fetch failed; reusing last known value: %s", exc)
        now = datetime.now(timezone.utc)
        await send_event(
            websocket,
            FarmSummaryEvent(
                total_volume=state.total_volume,
                total_rewards=state.rewards_earned,
                rewards_per_hour=state.rewards_per_hour(),
                rewards_per_day=state.expected_rewards_per_day,
                elapsed_seconds=state.elapsed_seconds(now),
                active_markets=len(state.positions),
                session_loss=state.session_loss,
                max_session_loss=state.config.max_session_loss,
                wallet_balance=wallet_balance,
            ),
        )
        await send_event(
            websocket,
            FarmPositionsEvent(positions=build_position_entries(state)),
        )
        elapsed = asyncio.get_running_loop().time() - cycle_start
        await asyncio.sleep(max(0, SUMMARY_INTERVAL_SECONDS - elapsed))


async def prune_dead_positions(
    client, state: FarmState, websocket: WebSocket, live_ids: set[str] | None = None
) -> None:
    if not state.positions:
        return
    if live_ids is None:
        try:
            live_ids = await get_open_order_ids(client)
        except Exception:
            logger.exception("open-orders poll failed; skipping dead-position prune")
            return
    for cid, pos in list(state.positions.items()):
        if (
            pos.yes_order_id in live_ids
            or pos.no_order_id in live_ids
            or pos.yes_shares > 0
            or pos.no_shares > 0
            or pos.exit_orders
        ):
            pos.orders_missing_ticks = 0
            continue
        if pos.quotes_pulled:
            pos.orders_missing_ticks = 0
            now = datetime.now(timezone.utc)
            if pos.quotes_pulled_at is None:
                pos.quotes_pulled_at = now
                continue
            elapsed = (now - pos.quotes_pulled_at).total_seconds()
            if elapsed >= GUARD_PULL_RELEASE_SECONDS:
                logger.info(
                    "releasing %s — quotes guard-pulled %.0fs ago and never re-placed",
                    pos.market.slug,
                    elapsed,
                )
                strat("guard_pull_release", slug=pos.market.slug, cid=cid, pulled_s=round(elapsed))
                await close_position(client, state, websocket, cid, reason="guard_pulled")
            continue
        pos.orders_missing_ticks += 1
        if pos.orders_missing_ticks < POSITION_PRUNE_MISSING_TICKS:
            continue
        logger.warning(
            "resting orders for %s vanished server-side — closing position for requote",
            pos.market.slug,
        )
        await close_position(client, state, websocket, cid, reason="server_cancelled")


def prune_order_registry(state: FarmState, now: datetime) -> None:
    cutoff = now - timedelta(seconds=ORDER_REGISTRY_RETENTION_SECONDS)
    stale = [
        oid
        for oid, info in state.order_registry.items()
        if info.retired_at is not None and info.retired_at <= cutoff
    ]
    for oid in stale:
        del state.order_registry[oid]

    # booked_exit_fills dedupes exit-fill bookings by (trade.id, exit_oid) and is otherwise
    # append-only, so it would grow for the life of the process — which matters once the farm
    # stops being restarted on every disconnect. It is only ever consulted for an oid still in
    # pending_exit_order_ids (handle_trade builds exit_fills from that set), so once an oid
    # drains out its keys are unreachable and dropping them cannot resurrect a double-book.
    # That makes this a provable prune rather than a size cap, which could evict a live key.
    if state.booked_exit_fills:
        live = state.pending_exit_order_ids
        state.booked_exit_fills = {key for key in state.booked_exit_fills if key[1] in live}
    if stale:
        logger.debug("pruned %d retired order-registry entries", len(stale))


async def reap_orphan_orders(client, state: FarmState, live_ids: set[str]) -> None:
    """Two-sweep cleanup for untracked resting orders: close-fail orphans retry every sweep
    until confirmed gone; reaper-discovered orders need a second consecutive sweep first."""
    for oid in list(state.pending_orphan_cancels):
        if oid not in live_ids:
            state.pending_orphan_cancels.discard(oid)
            info = state.order_registry.get(oid)
            if info is not None and info.retired_at is None:
                info.retired_at = datetime.now(timezone.utc)
            logger.info("orphan reaper: close-fail orphan %s no longer resting — resolved", oid)
    still_live = state.pending_orphan_cancels & live_ids

    tracked_ids = set(state.order_registry) | state.pending_exit_order_ids
    for pos in state.positions.values():
        tracked_ids.add(pos.yes_order_id)
        tracked_ids.add(pos.no_order_id)
        tracked_ids.update(pos.exit_orders)
    tracked_ids.discard("")

    untracked = live_ids - tracked_ids
    confirmed = untracked & state.reaper_unknown_ids
    state.reaper_unknown_ids = untracked

    to_cancel = still_live | confirmed
    if not to_cancel:
        return
    await cancel_orders(client, *to_cancel)
    for oid in to_cancel:
        path = "close_fail" if oid in still_live else "reaper_sweep"
        logger.warning("orphan reaper: cancelling untracked resting order %s (path=%s)", oid, path)
        strat("orphan_reaped", oid=oid, path=path)


async def reconcile_tick(
    http: httpx.AsyncClient,
    client,
    state: FarmState,
    websocket: WebSocket,
) -> None:
    if state.killed:
        return
    if should_kill(state):
        logger.warning(
            "reconcile backstop tripped MTM kill (market WS may be stale): realized=%s "
            "unrealized=%s threshold=%s",
            state.session_loss,
            unrealized_loss(state),
            state.config.max_session_loss,
        )
        await trigger_kill(client, state, websocket, source="reconcile_backstop")
        return
    try:
        live_ids = await get_open_order_ids(client)
    except Exception:
        logger.exception("open-orders poll failed; skipping dead-position prune and orphan reap")
        live_ids = None
    if live_ids is not None:
        await prune_dead_positions(client, state, websocket, live_ids)
        await reap_orphan_orders(client, state, live_ids)
    prune_order_registry(state, datetime.now(timezone.utc))
    markets = await fetch_eligible_markets(http)
    for cid in list(state.health.keys()):
        reevaluate_blacklist(state, cid)
    for m in markets:
        (
            m.effective_depth,
            m.effective_reward_min,
            m.effective_liq_min,
            m.effective_zone_liq_max,
            m.effective_time_remaining,
        ) = resolve_tier(
            state.config.size_tiers,
            size_per_market(m),
            state.config.quote_depth,
            state.config.filters.reward_min,
            state.config.filters.liq_min,
            state.config.filters.zone_liq_max,
            state.config.filters.time_remaining,
        )
    filters = state.config.filters
    need_zone = filters.zone_liq_max is not None or any(
        tier.zone_liq_max is not None for tier in state.config.size_tiers
    )
    need_exit_loss = filters.max_fill_loss is not None
    need_price = filters.price_min is not None or filters.price_max is not None
    need_books = need_zone or need_exit_loss or filters.min_bid_depth_mult is not None
    universe_midpoints: dict[str, Decimal] = {}
    if need_books or need_price:
        universe_midpoints = await annotate_live_metrics(
            http,
            markets,
            need_books=need_books,
            need_exit_loss=need_exit_loss,
            quote_depth=state.config.quote_depth,
        )
    sports_blocked, sports_unmapped = annotate_sports_gate(markets, datetime.now(timezone.utc))
    if sports_blocked or sports_unmapped:
        logger.info(
            "sports_gate blocked=%d unmapped=%d%s",
            sports_blocked,
            len(sports_unmapped),
            (" unmapped_names=" + "|".join(sorted(set(sports_unmapped))[:20]))
            if sports_unmapped
            else "",
        )
    candidates: dict[str, Market] = {}
    funnel: dict[str, int] = {}
    for m in markets:
        if not passes_all(m, state.config.filters):
            decision = first_failing_filter(m, state.config.filters) or "filter"
        elif is_paused(state, m.condition_id):
            decision = "paused"
        elif is_blacklisted(state, m.condition_id):
            decision = "blacklisted"
        elif in_guard_pull_cooldown(state, m.condition_id):
            decision = "guard_cooldown"
        elif m.condition_id in state.excluded_markets:
            decision = "excluded"
        elif m.event_slug and is_event_excluded(state, m.event_slug):
            decision = "excluded_event"
        else:
            decision = "candidate"
            candidates[m.condition_id] = m
        if decision != "candidate":
            funnel[decision] = funnel.get(decision, 0) + 1
        logger.debug(
            "market_eval slug=%s decision=%s mid=%s vol=%s liq=%s spread_c=%s reward=%s "
            "zone=%s max_spread_c=%s chg24h=%s end=%s created=%s "
            "tier_depth=%s tier_reward_min=%s tier_liq_min=%s tier_zone_max=%s tier_time=%s",
            m.slug,
            decision,
            m.midpoint,
            m.volume_24h,
            m.liquidity,
            m.spread_cents,
            m.rewards_rate_per_day,
            m.zone_liquidity,
            m.rewards_max_spread_cents,
            m.price_change_24h,
            m.end_date.date(),
            m.created_at.date(),
            m.effective_depth,
            m.effective_reward_min,
            m.effective_liq_min,
            m.effective_zone_liq_max,
            m.effective_time_remaining,
        )

    rng_bucket = state.config.filters.range_24h
    if rng_bucket != "all" and candidates:
        ranges = await fetch_price_ranges(http, [m.yes_token_id for m in candidates.values()])
        for cid in list(candidates):
            if not passes_range_24h(ranges.get(candidates[cid].yes_token_id), rng_bucket):
                funnel["range_24h"] = funnel.get("range_24h", 0) + 1
                del candidates[cid]

    current_ids = set(state.positions.keys())
    candidate_ids = set(candidates.keys())

    for cid in current_ids & candidate_ids:
        state.positions[cid].candidate_miss_ticks = 0

    closed = 0
    for cid in current_ids - candidate_ids:
        pos = state.positions.get(cid)
        if pos is None:
            continue
        if pos.yes_shares > 0 or pos.no_shares > 0:
            pos.candidate_miss_ticks = 0
            logger.info("deferring close for %s: still holds shares", cid)
            continue
        pos.candidate_miss_ticks += 1
        if pos.candidate_miss_ticks < POSITION_CANDIDATE_MISS_TICKS:
            logger.info(
                "deferring close for %s: candidate miss %d/%d",
                cid,
                pos.candidate_miss_ticks,
                POSITION_CANDIDATE_MISS_TICKS,
            )
            continue
        await close_position(client, state, websocket, cid, reason="market_dropped")
        closed += 1

    opened = 0
    skipped_cost = 0
    skipped_unquoteable = 0
    skipped_capacity = 0
    try:
        try:
            current_balance = await get_balance(state.wallet_address)
        except Exception:
            logger.exception("get_balance failed; skipping tick")
            return

        effective_bankroll = min(state.config.bankroll, current_balance)

        await exit_held_legs(client, state, cancel_resting=True, skip_in_flight=True)

        new_markets = [candidates[cid] for cid in sorted(candidate_ids - current_ids)]
        if not new_markets:
            return

        if universe_midpoints:
            midpoints = universe_midpoints
        else:
            new_token_ids = [t for m in new_markets for t in (m.yes_token_id, m.no_token_id)]
            midpoints = await fetch_midpoints(http, new_token_ids)
        if not midpoints:
            return

        for market in new_markets:
            if (
                market.condition_id in state.excluded_markets
                or is_blacklisted(state, market.condition_id)
                or (market.event_slug and is_event_excluded(state, market.event_slug))
            ):
                logger.debug(
                    "skip_open slug=%s — blacklisted/excluded by a fill earlier in this batch",
                    market.slug,
                )
                continue
            if len(state.positions) >= state.config.max_concurrent_positions:
                skipped_capacity += 1
                continue
            yes_mid = midpoints.get(market.yes_token_id)
            no_mid = midpoints.get(market.no_token_id)
            if yes_mid is None or no_mid is None:
                continue
            depth = market.effective_depth or state.config.quote_depth
            cost = two_leg_cost(market, yes_mid, no_mid, depth)
            if cost is None:
                skipped_unquoteable += 1
                logger.debug("skip_unquoteable slug=%s (no valid two-leg quote)", market.slug)
                continue
            if cost > effective_bankroll:
                skipped_cost += 1
                logger.debug(
                    "skip_unaffordable slug=%s cost=%s bankroll=%s",
                    market.slug,
                    cost,
                    effective_bankroll,
                )
                continue
            await open_position(client, state, websocket, market, midpoints)
            opened += 1
    finally:
        if funnel:
            logger.info(
                "filter_funnel %s",
                " ".join(f"{k}={v}" for k, v in sorted(funnel.items())),
            )
        if need_zone:
            zone_dist = zone_distribution(markets)
            if zone_dist is not None:
                lowest, typical, highest = zone_dist
                logger.info(
                    "zone_liquidity lowest=%s typical=%s highest=%s",
                    lowest,
                    typical,
                    highest,
                )
        logger.info(
            "tick fetched=%d candidates=%d positions=%d opened=%d closed=%d "
            "skipped_cost=%d skipped_unquoteable=%d skipped_capacity=%d deployed=%s",
            len(markets),
            len(candidate_ids),
            len(state.positions),
            opened,
            closed,
            skipped_cost,
            skipped_unquoteable,
            skipped_capacity,
            deployed_capital(state),
        )


async def place_leg(client, order: LimitOrder) -> tuple[str | Exception, bool]:
    try:
        return await place_limit_order(client, order, post_only=True), True
    except Exception as exc:
        return exc, False


async def open_position(
    client,
    state: FarmState,
    websocket: WebSocket,
    market: Market,
    midpoints: dict[str, Decimal],
) -> None:
    yes_mid = midpoints.get(market.yes_token_id)
    no_mid = midpoints.get(market.no_token_id)
    if yes_mid is None or no_mid is None:
        return

    depth = market.adaptive_depth or market.effective_depth or state.config.quote_depth
    yes_quote = compute_quote(market, yes_mid, depth)
    no_quote = compute_quote(market, no_mid, depth)
    if yes_quote is None or no_quote is None:
        return

    yes_bid_price = yes_quote[0]
    no_bid_price = no_quote[0]
    size = float(size_per_market(market))

    yes_order = LimitOrder(
        token_id=market.yes_token_id, side="BUY", size=size, price=float(yes_bid_price)
    )
    no_order = LimitOrder(
        token_id=market.no_token_id, side="BUY", size=size, price=float(no_bid_price)
    )

    yes_result, yes_ok = await place_leg(client, yes_order)
    no_result, no_ok = await place_leg(client, no_order)

    if not (yes_ok and no_ok):
        logger.error(
            "place_limit_order partial failure for market %s: yes_ok=%s no_ok=%s "
            "yes_err=%r no_err=%r",
            market.condition_id,
            yes_ok,
            no_ok,
            None if yes_ok else yes_result,
            None if no_ok else no_result,
        )
        for leg, oid, ok in (("YES", yes_result, yes_ok), ("NO", no_result, no_ok)):
            if not ok:
                continue
            for attempt in range(3):
                try:
                    await cancel_order(client, oid)
                    break
                except Exception:
                    if attempt == 2:
                        logger.error(
                            "rollback cancel FAILED after retries for %s leg of %s (oid=%s) — "
                            "naked resting leg; orphan-exit will recover a fill",
                            leg,
                            market.condition_id,
                            oid,
                        )
        health = state.health.setdefault(market.condition_id, MarketHealth())
        health.open_failures += 1
        if health.open_failures >= OPEN_FAILURE_PAUSE_THRESHOLD:
            logger.warning(
                "pausing %s after %s consecutive open failures",
                market.condition_id,
                health.open_failures,
            )
            mark_paused(state, market.condition_id)
        return

    yes_oid, no_oid = yes_result, no_result
    health = state.health.get(market.condition_id)
    if health is not None:
        health.open_failures = 0
    state.order_registry[yes_oid] = OrderInfo(
        condition_id=market.condition_id, outcome="YES", token_id=market.yes_token_id
    )
    state.order_registry[no_oid] = OrderInfo(
        condition_id=market.condition_id, outcome="NO", token_id=market.no_token_id
    )
    state.positions[market.condition_id] = MarketPosition(
        market=market,
        yes_order_id=yes_oid,
        no_order_id=no_oid,
        yes_price=yes_bid_price,
        no_price=no_bid_price,
    )

    logger.info(
        "position_opened slug=%s yes_px=%s no_px=%s size=%s depth=%s",
        market.slug,
        yes_bid_price,
        no_bid_price,
        size,
        depth,
    )
    yd, nd = market.yes_bid_depth, market.no_bid_depth
    strat(
        "bid_depth_entry",
        slug=market.slug,
        cid=market.condition_id,
        size=size,
        yes_depth=float(yd) if yd is not None else "na",
        no_depth=float(nd) if nd is not None else "na",
        min_ratio=(
            round(float(min(yd, nd)) / size, 2)
            if yd is not None and nd is not None and size
            else "na"
        ),
    )
    for outcome_label, px, mid in (
        ("YES", yes_bid_price, yes_mid),
        ("NO", no_bid_price, no_mid),
    ):
        strat(
            "quote",
            slug=market.slug,
            outcome=outcome_label,
            px=px,
            size=size,
            mid=mid,
            max_spread=market.rewards_max_spread_cents,
            tick=market.tick_size,
            depth=depth,
            tier=market.effective_depth or state.config.quote_depth,
        )

    size_decimal = Decimal(str(size))
    await send_event(
        websocket,
        OrderPlacedEvent(
            market_id=market.condition_id,
            slug=market.slug,
            question=market.question,
            outcome="YES",
            side="BUY",
            price=yes_bid_price,
            size=size_decimal,
            capital_locked=size_decimal * yes_bid_price,
            order_id=yes_oid,
        ),
    )
    await send_event(
        websocket,
        OrderPlacedEvent(
            market_id=market.condition_id,
            slug=market.slug,
            question=market.question,
            outcome="NO",
            side="BUY",
            price=no_bid_price,
            size=size_decimal,
            capital_locked=size_decimal * no_bid_price,
            order_id=no_oid,
        ),
    )


def drop_order(state: FarmState, order_id: str) -> None:
    """Drop a confirmed-cancelled order from the registry; no-op for a falsy/untracked id."""
    if order_id:
        state.order_registry.pop(order_id, None)


async def close_position(
    client,
    state: FarmState,
    websocket: WebSocket,
    cid: str,
    reason: str,
) -> None:
    pos = state.positions.pop(cid, None)
    if pos is None:
        return
    logger.info(
        "position_closed slug=%s reason=%s held_yes=%s held_no=%s",
        pos.market.slug,
        reason,
        pos.yes_shares,
        pos.no_shares,
    )
    failed: list[tuple[str, str]] = []
    for oid, outcome in ((pos.yes_order_id, "YES"), (pos.no_order_id, "NO")):
        if not oid:
            continue
        if await cancel_order_with_retry(client, oid):
            drop_order(state, oid)
        else:
            failed.append((oid, outcome))

    if failed:
        try:
            live_ids = await get_open_order_ids(client)
        except Exception:
            logger.exception("open-orders verification failed during close_position for %s", cid)
            live_ids = None
        for oid, outcome in failed:
            if live_ids is not None and oid not in live_ids:
                drop_order(state, oid)
                logger.info(
                    "close_position: %s leg %s (oid=%s) cancel did not confirm but order is gone "
                    "server-side — no orphan",
                    pos.market.slug,
                    outcome,
                    oid,
                )
                continue
            live = "unknown" if live_ids is None else "True"
            logger.warning(
                "close_position: %s leg %s (oid=%s) did not confirm cancelled (reason=%s, "
                "live=%s) — queuing for the orphan reaper",
                pos.market.slug,
                outcome,
                oid,
                reason,
                live,
            )
            strat(
                "close_orphan",
                slug=pos.market.slug,
                cid=cid,
                leg=outcome,
                oid=oid,
                live=live,
                reason=reason,
            )
            state.pending_orphan_cancels.add(oid)

    for oid, outcome in ((pos.yes_order_id, "YES"), (pos.no_order_id, "NO")):
        if not oid:
            continue
        await send_event(
            websocket,
            OrderCancelledEvent(
                market_id=cid,
                slug=pos.market.slug,
                outcome=outcome,
                order_id=oid,
                reason=reason,
            ),
        )
