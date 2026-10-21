import asyncio
from datetime import datetime, timezone
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.bot.order_snapshot import OrderSnapshot, SettledFill
from app.bot.schemas import OrderBook
from app.farm import extreme_worker as ew


@pytest.fixture
def setup(farm_state, market, monkeypatch):
    market = market.model_copy(
        update={
            "tick_size": D("0.001"),
            "rewards_rate_per_day": D("100"),
            "end_date": datetime(2099, 1, 1, tzinfo=timezone.utc),
        }
    )
    farm_state.positions.clear()
    farm_state.config.bankroll = D("92")
    farm_state.config.extreme_capital_budget = D("90")
    farm_state.config.max_session_loss = D("50")
    farm_state.config.filters.reward_min = D("50")
    snapshots = {}
    client = SimpleNamespace(
        cancel_all=AsyncMock(),
        get_open_order_ids=AsyncMock(return_value=set()),
        conditional_balance=AsyncMock(return_value=D(0)),
    )

    async def place(order, post_only=False):
        oid = f"o{len(snapshots)}"
        snapshots[oid] = OrderSnapshot(order_id=oid, status="LIVE", matched=0, fills=[])
        return oid

    async def cancel(oid):
        snapshots[oid].status = "CANCELED"

    async def market_place(token, side, amount):
        oid = f"o{len(snapshots)}"
        snapshots[oid] = OrderSnapshot(order_id=oid, status="LIVE", matched=0, fills=[])
        return oid

    client.place_market_order = AsyncMock(side_effect=market_place)
    client.place_limit_order = AsyncMock(side_effect=place)
    client.cancel_order = AsyncMock(side_effect=cancel)
    client.order_snapshot = AsyncMock(side_effect=lambda oid: snapshots[oid])
    runtime = ew.ExtremeRuntime(client, farm_state, AsyncMock())
    book = OrderBook(
        market=market.condition_id,
        asset_id=market.yes_token_id,
        timestamp=datetime.now(timezone.utc),
        bids=[{"price": "0.044", "size": "2000"}],
        asks=[{"price": "0.045", "size": "2000"}],
        min_order_size="5",
        tick_size="0.001",
        neg_risk=False,
        hash="",
    )
    monkeypatch.setattr(ew, "fetch_eligible_markets", AsyncMock(return_value=[market]))
    monkeypatch.setattr(
        ew,
        "fetch_midpoints",
        AsyncMock(
            return_value={
                market.yes_token_id: D("0.0445"),
                market.no_token_id: D("0.9555"),
            }
        ),
    )
    no_book = book.model_copy(deep=True)
    no_book.asset_id = market.no_token_id
    no_book.bids[0].price = D("0.955")
    no_book.asks[0].price = D("0.956")
    monkeypatch.setattr(
        ew,
        "fetch_books",
        AsyncMock(
            return_value={
                market.yes_token_id: book,
                market.no_token_id: no_book,
            }
        ),
    )
    monkeypatch.setattr(ew, "get_balance", AsyncMock(return_value=D("92")))
    monkeypatch.setattr(ew, "verify_market_rewards", AsyncMock(return_value=market))
    return runtime, client, snapshots, market, book


def confirm(snapshots, oid, size="1000", price="0.045", status="CONFIRMED"):
    snapshots[oid] = OrderSnapshot(
        order_id=oid,
        status="MATCHED",
        matched=D(size),
        fills=[
            SettledFill(
                trade_id=f"t-{oid}",
                status=status,
                size=D(size),
                price=D(price),
                taker=False,
            )
        ],
    )


async def test_inactive_rewards_skip_entry(setup, monkeypatch):
    runtime, client, _, _, _ = setup
    monkeypatch.setattr(ew, "verify_market_rewards", AsyncMock(return_value=None))
    await runtime.tick(None)
    client.place_limit_order.assert_not_awaited()
    assert not runtime.inventory


async def test_rewards_disappear_immediately_before_submission(setup, monkeypatch):
    runtime, client, _, market, _ = setup
    monkeypatch.setattr(ew, "verify_market_rewards", AsyncMock(side_effect=[market, None]))
    await runtime.tick(None)
    client.place_limit_order.assert_not_awaited()


@pytest.mark.parametrize("failure", [None, RuntimeError("reward API unavailable")])
async def test_lost_or_unverified_rewards_cancel_orders_keep_inventory(setup, monkeypatch, failure):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    check = AsyncMock(return_value=None)
    if failure is not None:
        check.side_effect = failure
    monkeypatch.setattr(ew, "verify_market_rewards", check)
    count = client.place_limit_order.await_count
    await runtime.tick(None)
    assert inv.halted and inv.shares == D("1000")
    assert snapshots["o1"].terminal and snapshots["o2"].terminal
    assert client.place_limit_order.await_count == count
    client.place_market_order.assert_not_awaited()


async def test_changed_reward_parameters_cancel_old_quotes(setup, monkeypatch):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    fresh = inv.market.model_copy(update={"rewards_max_spread_cents": D("0.1")})
    monkeypatch.setattr(ew, "verify_market_rewards", AsyncMock(return_value=fresh))
    await runtime.tick(None)
    assert inv.halted
    assert snapshots["o1"].terminal and snapshots["o2"].terminal
    client.place_market_order.assert_not_awaited()


async def test_entry_confirmation_then_two_post_only_quotes(setup):
    runtime, client, snapshots, market, _ = setup
    await runtime.tick(None)
    order = client.place_limit_order.call_args.args[0]
    assert (order.side, order.token_id, order.price, order.size) == (
        "BUY",
        market.yes_token_id,
        0.045,
        1000,
    )
    assert client.place_limit_order.call_args.kwargs == {"post_only": False}
    confirm(snapshots, "o0", status="MATCHED")
    await runtime.tick(None)
    assert client.place_limit_order.call_count == 1  # Not settled, no sell.
    confirm(snapshots, "o0")
    client.conditional_balance.return_value = D("1000")
    await runtime.tick(None)
    calls = client.place_limit_order.call_args_list
    assert [(c.args[0].side, c.args[0].price) for c in calls] == [
        ("BUY", 0.045),
        ("SELL", 0.045),
        ("BUY", 0.043),
    ]
    assert all(c.kwargs["post_only"] for c in calls[1:])
    await runtime.tick(None)
    assert client.place_limit_order.call_count == 3
    assert runtime.inventory[market.condition_id].shares == D("1000")


async def test_partial_entry_cancel_and_no_oversell(setup):
    runtime, client, snapshots, market, _ = setup
    await runtime.tick(None)
    confirm(snapshots, "o0", size="250")
    snapshots["o0"].status = "LIVE"
    client.conditional_balance.return_value = D("250")
    await runtime.tick(None)
    client.cancel_order.assert_awaited_once_with("o0")
    assert client.place_limit_order.call_count == 1
    await runtime.tick(None)
    sell = client.place_limit_order.call_args_list[1].args[0]
    assert sell.side == "SELL" and sell.size == 250
    assert runtime.inventory[market.condition_id].cost == D("11.25")


@pytest.mark.parametrize("size,status", [("100", "LIVE"), ("1046", "MATCHED")])
async def test_lower_buy_fill_liquidates_all_inventory_and_stops(setup, size, status):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    confirm(snapshots, "o2", size=size, price="0.043")
    snapshots["o2"].status = status
    total = D(1000) + D(size)
    client.conditional_balance.return_value = total
    await runtime.tick(None)
    assert snapshots["o1"].terminal and snapshots["o2"].terminal
    client.place_market_order.assert_awaited_once_with(inv.plan.token_id, "SELL", float(total))
    assert not runtime.state.killed  # Submission is not proof of liquidation.
    await runtime.tick(None)
    assert client.place_market_order.await_count == 1
    confirm(snapshots, "o3", size=str(total), price="0.044")
    client.conditional_balance.return_value = D(0)
    await runtime.tick(None)
    assert inv.shares == 0 and runtime.state.killed
    assert client.place_limit_order.call_count == 3


async def test_cancel_rejection_never_forgets_live_order(setup):
    runtime, client, snapshots, market, _ = setup
    await runtime.tick(None)
    inv = runtime.inventory[market.condition_id]
    client.cancel_order.side_effect = None
    client.cancel_order.return_value = {"not_canceled": {"o0": "retry"}}
    with pytest.raises(RuntimeError, match="not confirmed"):
        await runtime.cancel(inv, inv.orders[0])
    assert not inv.orders[0].done


async def test_blacklist_removes_quotes_without_selling_inventory(setup):
    runtime, client, snapshots, market, _ = setup
    await runtime.tick(None)
    confirm(snapshots, "o0")
    client.conditional_balance.return_value = D("1000")
    await runtime.tick(None)
    runtime.state.excluded_markets.add(market.condition_id)
    await runtime.tick(None)
    assert runtime.inventory[market.condition_id].shares == 1000
    assert snapshots["o1"].terminal and snapshots["o2"].terminal
    assert client.place_limit_order.call_count == 3


async def test_existing_inventory_is_not_adopted(setup):
    runtime, client, _, _, _ = setup
    client.conditional_balance.return_value = D("10")
    await runtime.tick(None)
    client.place_limit_order.assert_not_awaited()


@pytest.mark.parametrize("spread", ["0.1", "0.15"])
async def test_rebuy_outside_or_on_reward_boundary_prevents_first_buy(setup, spread):
    runtime, client, _, market, _ = setup
    # Rebuy .043 is .0015 away from reward midpoint .0445; sell .045 qualifies.
    market.rewards_max_spread_cents = D(spread)
    await runtime.tick(None)
    client.place_limit_order.assert_not_awaited()
    assert not runtime.inventory


async def test_invalid_rebuy_skips_to_next_market_before_any_entry(setup, monkeypatch):
    runtime, client, _, market, book = setup
    rejected = market.model_copy(
        update={
            "condition_id": "rejected",
            "rewards_rate_per_day": D(200),
            "rewards_max_spread_cents": D("0.1"),
        }
    )
    monkeypatch.setattr(ew, "fetch_eligible_markets", AsyncMock(return_value=[rejected, market]))
    await runtime.tick(None)
    client.place_limit_order.assert_awaited_once()
    assert set(runtime.inventory) == {market.condition_id}


async def test_loss_guard_precedes_new_orders(setup, monkeypatch):
    runtime, client, _, _, _ = setup
    monkeypatch.setattr(ew, "should_kill", lambda state: True)
    await runtime.tick(None)
    assert runtime.state.killed
    client.place_limit_order.assert_not_awaited()


async def test_failed_entry_never_creates_inventory(setup):
    runtime, client, snapshots, market, _ = setup
    await runtime.tick(None)
    confirm(snapshots, "o0", status="FAILED")
    await runtime.tick(None)
    assert market.condition_id not in runtime.inventory
    assert market.condition_id in runtime.completed
    assert client.place_limit_order.call_count == 1


async def test_shutdown_cancels_after_inflight_submission(setup):
    runtime, client, snapshots, market, _ = setup
    started, release = asyncio.Event(), asyncio.Event()
    original = client.place_limit_order.side_effect

    async def slow(order, post_only=False):
        started.set()
        await release.wait()
        return await original(order, post_only)

    client.place_limit_order.side_effect = slow
    task = asyncio.create_task(runtime.tick(None))
    await started.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert runtime.inventory[market.condition_id].orders[0].order_id == "o0"


async def test_worker_uses_new_runtime_only(setup, monkeypatch):
    from app.farm import worker

    runtime, client, _, _, _ = setup
    client.wallet_type = "DEPOSIT_WALLET"
    monkeypatch.setattr(worker, "build_execution_client", lambda wallet: client)
    monkeypatch.setattr(worker, "list_blacklist", AsyncMock(return_value=[]))
    run = AsyncMock()
    monkeypatch.setattr(ew, "run_extreme", run)
    legacy = AsyncMock()
    monkeypatch.setattr(worker, "reconcile_tick", legacy)
    session = SimpleNamespace(state=None)
    await worker.run_farm(
        AsyncMock(),
        runtime.state.config,
        SimpleNamespace(proxy_address="wallet"),
        "license",
        session,
    )
    run.assert_awaited_once()
    legacy.assert_not_awaited()


@pytest.mark.parametrize(
    "reason",
    [
        "book_missing",
        "book_one_sided",
        "reward_depth_insufficient",
        "outside_reward_spread",
        "dynamic_quote_unavailable",
        "strategy_halted",
    ],
)
async def test_quote_cancel_reason_and_context(setup, caplog, reason):
    runtime, client, snapshots, market, book = setup
    await runtime.tick(None)
    confirm(snapshots, "o0")
    client.conditional_balance.return_value = D("1000")
    await runtime.tick(None)
    inv = runtime.inventory[market.condition_id]
    book = book.model_copy(deep=True)
    for order in inv.orders:
        await runtime.poll(inv, order)
    if reason == "book_missing":
        book = None
    elif reason == "book_one_sided":
        book.bids = []
    elif reason == "tick_size_change":
        book.tick_size = D("0.01")
    elif reason == "reward_depth_insufficient":
        book.bids[0].size = D("1")
    elif reason == "midpoint_outside_quotes":
        book.asks[0].size = D(1)
        book.asks.append(book.asks[0].model_copy(update={"price": D("0.09"), "size": D(2000)}))
    elif reason == "outside_reward_spread":
        inv.market.rewards_max_spread_cents = D("0.1")
    elif reason == "dynamic_quote_unavailable":
        book.bids[0].price, book.asks[0].price = D("0.0001"), D("0.0002")
    else:
        inv.halted = True
    runtime.emit = AsyncMock()
    with caplog.at_level("INFO", logger=ew.__name__):
        await runtime.manage(inv, book, True)
    events = [c.args[0] for c in runtime.emit.call_args_list]
    assert len(events) == 2
    assert {e.reason for e in events} == {reason}
    assert {e.details["side"] for e in events} == {"BUY", "SELL"}
    assert all(e.details["sell_price"] == "0.045" for e in events)
    assert "extreme_cancel_requested" in caplog.text
    assert "extreme_cancel_confirmed" in caplog.text
    assert f"reason={reason}" in caplog.text
    assert '"best_bid"' in caplog.text
    assert inv.shares == 1000
    assert client.place_limit_order.call_count == 3


@pytest.mark.parametrize(
    "reason",
    [
        "market_missing",
        "market_expired",
        "reward_below_minimum",
        "market_excluded",
    ],
)
async def test_filter_cancel_reason_from_tick(setup, reason):
    runtime, client, snapshots, market, _ = setup
    await runtime.tick(None)
    confirm(snapshots, "o0")
    client.conditional_balance.return_value = D("1000")
    await runtime.tick(None)
    if reason == "market_missing":
        runtime.markets.clear()
    elif reason == "market_expired":
        runtime.markets[market.condition_id].end_date = datetime(2000, 1, 1, tzinfo=timezone.utc)
    elif reason == "reward_below_minimum":
        runtime.markets[market.condition_id].rewards_rate_per_day = D(1)
    else:
        runtime.state.excluded_markets.add(market.condition_id)
    runtime.emit = AsyncMock()
    await runtime.tick(None)
    events = [c.args[0] for c in runtime.emit.call_args_list]
    assert len(events) == 2
    assert all(e.reason == reason for e in events)


@pytest.mark.parametrize("partial", [True, False])
async def test_entry_cancel_distinguishes_partial_fill_from_timeout(setup, partial):
    runtime, client, snapshots, market, _ = setup
    await runtime.tick(None)
    if partial:
        confirm(snapshots, "o0", size="250")
        snapshots["o0"].status = "LIVE"
    else:
        runtime.inventory[market.condition_id].orders[0].placed_at -= 61
    runtime.emit = AsyncMock()
    await runtime.tick(None)
    events = [c.args[0] for c in runtime.emit.call_args_list if c.args[0].type == "order_cancelled"]
    assert len(events) == 1
    assert events[0].reason == ("entry_partial_fill" if partial else "entry_timeout")
    assert events[0].details["matched_size"] == ("250" if partial else "0")


async def test_unverified_cancel_logs_trigger_and_does_not_emit_success(setup, caplog):
    runtime, client, _, market, _ = setup
    await runtime.tick(None)
    inv = runtime.inventory[market.condition_id]
    runtime.emit = AsyncMock()
    client.cancel_order.side_effect = None
    with caplog.at_level("INFO", logger=ew.__name__):
        with pytest.raises(RuntimeError, match="not confirmed"):
            await runtime.cancel(inv, inv.orders[0], "entry_timeout")
    assert "extreme_cancel_unverified" in caplog.text
    assert "reason=entry_timeout" in caplog.text
    assert "extreme_cancel_confirmed" not in caplog.text
    runtime.emit.assert_not_awaited()


async def test_fill_during_cancel_is_not_reported_as_cancelled(setup, caplog):
    runtime, client, snapshots, market, _ = setup
    await runtime.tick(None)
    inv = runtime.inventory[market.condition_id]

    async def filled_instead(oid):
        confirm(snapshots, oid)

    client.cancel_order.side_effect = filled_instead
    runtime.emit = AsyncMock()
    with caplog.at_level("INFO", logger=ew.__name__):
        await runtime.cancel(inv, inv.orders[0], "entry_timeout")
    assert "extreme_cancel_raced" in caplog.text
    assert all(c.args[0].type != "order_cancelled" for c in runtime.emit.call_args_list)
    assert inv.shares == 1000


@pytest.mark.parametrize("reason", ["startup", "shutdown", "session_loss", "worker_error"])
async def test_bulk_cancel_logs_lifecycle_reason(setup, caplog, reason):
    _, client, _, _, _ = setup
    with caplog.at_level("INFO", logger=ew.__name__):
        await ew.cancel_and_verify(client, reason=reason)
    assert f"extreme_cancel_all_requested reason={reason}" in caplog.text
    assert f"extreme_cancel_all_confirmed reason={reason}" in caplog.text


async def quoted_inventory(setup):
    runtime, client, snapshots, market, book = setup
    await runtime.tick(None)
    confirm(snapshots, "o0")
    client.conditional_balance.return_value = D("1000")
    await runtime.tick(None)
    return runtime, client, snapshots, runtime.inventory[market.condition_id], book


async def test_price_rise_replaces_both_quotes_within_budget(setup):
    runtime, client, _, inv, book = await quoted_inventory(setup)
    book.bids[0].price, book.asks[0].price = D("0.047"), D("0.048")
    await runtime.tick(None)
    assert client.cancel_order.await_count == 2
    assert inv.live("SELL")[0].order.price == 0.048
    assert inv.live("BUY")[0].order.price == 0.046
    assert inv.live("BUY")[0].order.size == 978
    assert inv.cost + sum(o.remaining * D(str(o.order.price)) for o in inv.live("BUY")) <= 90
    assert client.place_limit_order.call_args.kwargs["post_only"] is True
    await runtime.tick(None)
    assert client.cancel_order.await_count == 2  # Unchanged BBA does not churn orders.


async def test_price_drop_moves_both_quotes_below_inventory_cost(setup):
    runtime, client, _, inv, book = await quoted_inventory(setup)
    book.bids[0].price, book.asks[0].price = D("0.042"), D("0.043")
    await runtime.tick(None)
    assert client.cancel_order.await_count == 2
    assert inv.live("SELL")[0].order.price == 0.043
    assert inv.live("BUY")[0].order.price == 0.041


async def test_cost_does_not_prevent_repricing_inside_rewards(setup):
    runtime, client, _, inv, book = await quoted_inventory(setup)
    book.bids[0].price, book.asks[0].price = D("0.020"), D("0.021")
    inv.market.rewards_max_spread_cents = D("1")
    runtime.emit = AsyncMock()
    await runtime.tick(None)
    assert inv.live("SELL")[0].order.price == 0.021
    assert inv.live("BUY")[0].order.price == 0.019
    assert inv.shares == 1000
    assert client.place_limit_order.call_count == 5


async def test_reprice_cancel_failure_never_places_replacements(setup):
    runtime, client, _, _, book = await quoted_inventory(setup)
    book.bids[0].price, book.asks[0].price = D("0.047"), D("0.048")
    client.cancel_order.side_effect = None  # API returns without removing the order.
    with pytest.raises(RuntimeError, match="not confirmed"):
        await runtime.tick(None)
    assert client.place_limit_order.call_count == 3


async def test_pending_fill_during_reprice_waits_before_replacing(setup):
    runtime, client, snapshots, inv, book = await quoted_inventory(setup)
    book.bids[0].price, book.asks[0].price = D("0.047"), D("0.048")

    async def cancel(oid):
        if oid == "o1":
            confirm(snapshots, oid, size="10", price="0.048", status="MINED")
        snapshots[oid].status = "CANCELED"

    client.cancel_order.side_effect = cancel
    await runtime.tick(None)
    assert client.place_limit_order.call_count == 3
    snapshots["o1"] = snapshots["o1"].model_copy(deep=True)
    snapshots["o1"].fills[0].status = "CONFIRMED"
    client.conditional_balance.return_value = D(990)
    await runtime.tick(None)
    assert inv.shares == 990
    await runtime.tick(None)
    assert inv.live("SELL")[0].order.size == 990


async def test_tick_change_reprices_instead_of_permanently_halting(setup):
    runtime, _, _, inv, book = await quoted_inventory(setup)
    book.tick_size = D("0.01")
    await runtime.tick(None)
    assert not inv.halted
    for order in inv.live("BUY") + inv.live("SELL"):
        assert D(str(order.order.price)) % D("0.01") == 0


async def test_one_tick_flip_flop_is_suppressed_while_old_quote_is_safe(setup):
    runtime, client, _, inv, book = await quoted_inventory(setup)
    book.bids[0].price, book.asks[0].price = D("0.045"), D("0.046")
    await runtime.tick(None)
    calls = client.cancel_order.await_count
    book.bids[0].price, book.asks[0].price = D("0.044"), D("0.045")
    await runtime.tick(None)
    assert client.cancel_order.await_count == calls
    inv.previous_quotes = {
        side: (price, datetime(2000, 1, 1, tzinfo=timezone.utc))
        for side, (price, _) in inv.previous_quotes.items()
    }
    await runtime.tick(None)
    assert client.cancel_order.await_count > calls


async def test_background_discovery_does_not_block_repricing(setup, monkeypatch):
    runtime, _, _, inv, book = await quoted_inventory(setup)
    blocked = asyncio.Event()

    async def slow(http):
        await blocked.wait()
        return []

    monkeypatch.setattr(ew, "fetch_eligible_markets", slow)
    runtime.discovery_at = float("-inf")
    book.bids[0].price, book.asks[0].price = D("0.047"), D("0.048")
    try:
        await asyncio.wait_for(runtime.tick(None), timeout=1)
        assert inv.live("BUY")[0].order.price == 0.046
        assert not runtime.discovery_task.done()
    finally:
        runtime.discovery_task.cancel()
        await asyncio.gather(runtime.discovery_task, return_exceptions=True)


async def test_market_events_wake_inventory_worker_without_legacy_handlers(monkeypatch):
    from app.bot.schemas import BestBidAsk
    from app.farm import requote

    async def events(tokens):
        assert tokens == ["yes"]
        yield BestBidAsk(
            event_type="best_bid_ask",
            market="m",
            asset_id="yes",
            best_bid="0.04",
            best_ask="0.05",
            spread="0.01",
            timestamp="1",
        )

    monkeypatch.setattr(requote, "stream_market_events", events)
    legacy = AsyncMock()
    monkeypatch.setattr(requote, "handle_bba", legacy)
    wake = asyncio.Event()
    await requote.watch_quote_changes(["yes"], wake)
    assert wake.is_set()
    legacy.assert_not_awaited()


async def test_position_table_snapshot_marks_inventory_and_resting_orders(setup):
    from app.farm.worker import build_position_entries

    runtime, client, snapshots, inv, book = await quoted_inventory(setup)
    entry = build_position_entries(runtime.state)[0]
    assert entry.position_pnl == D("-0.5")  # 1000 * 0.0445 - $45 cost
    assert {(o.side, o.shares, o.value) for o in entry.limit_orders} == {
        ("BUY", D(1046), D("44.978")),
        ("SELL", D(1000), D(45)),
    }
    confirm(snapshots, "o1", size="100", price="0.048")
    snapshots["o1"].status = "LIVE"
    client.conditional_balance.return_value = D(900)
    await runtime.tick(None)
    assert build_position_entries(runtime.state)[0].limit_orders == []
    await runtime.tick(None)
    entry = build_position_entries(runtime.state)[0]
    sell = next(order for order in entry.limit_orders if order.side == "SELL")
    assert sell.shares == 900 and sell.value == D("40.5")
    assert entry.position_pnl == D("-0.45")
    book.asks.clear()
    await runtime.tick(None)
    entry = build_position_entries(runtime.state)[0]
    assert entry.position_pnl is None
    assert entry.limit_orders == []


async def test_partial_entry_does_not_expand_lower_buy_allocation(setup):
    runtime, client, snapshots, market, _ = setup
    await runtime.tick(None)
    confirm(snapshots, "o0", size="250")
    client.conditional_balance.return_value = D(250)
    await runtime.tick(None)
    inv = runtime.inventory[market.condition_id]
    buy = inv.live("BUY")[0].order
    assert D(str(buy.size)) * D(str(buy.price)) == D("44.978")
    assert inv.cost == D("11.25")


async def test_entry_fee_uses_fee_reserve_not_lower_buy_half(setup):
    runtime, client, snapshots, market, _ = setup
    market.taker_fee_rate = D("0.01")
    await runtime.tick(None)
    confirm(snapshots, "o0")
    snapshots["o0"].fills[0].taker = True
    client.conditional_balance.return_value = D(1000)
    await runtime.tick(None)
    inv = runtime.inventory[market.condition_id]
    assert inv.fees_paid > 0
    buy = inv.live("BUY")[0].order
    assert D(str(buy.size)) * D(str(buy.price)) == D("44.978")


async def test_partial_sell_cancels_both_and_requotes_actual_inventory(setup):
    runtime, client, snapshots, inv, book = await quoted_inventory(setup)
    confirm(snapshots, "o1", size="100", price="0.045")
    snapshots["o1"].status = "LIVE"
    client.conditional_balance.return_value = D(900)
    await runtime.tick(None)
    assert set(c.args[0] for c in client.cancel_order.call_args_list) == {"o1", "o2"}
    assert client.place_limit_order.call_count == 3
    book.bids[0].price, book.asks[0].price = D("0.042"), D("0.043")
    await runtime.tick(None)
    assert inv.live("SELL")[0].order.size == 900
    assert inv.live("SELL")[0].order.price == 0.043
    assert inv.live("BUY")[0].order.price == 0.041
    client.place_market_order.assert_not_awaited()


async def test_full_sell_restarts_with_market_buy_then_quotes(setup):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    confirm(snapshots, "o1")
    client.conditional_balance.return_value = D(0)
    await runtime.tick(None)
    assert inv.restart_entry and inv.shares == 0
    client.cancel_order.assert_awaited_once_with("o2")
    await runtime.tick(None)
    client.place_market_order.assert_awaited_once_with(inv.plan.token_id, "BUY", 45.0)
    await runtime.tick(None)
    assert client.place_market_order.await_count == 1
    # Actual market quantity differs from the BBA estimate.
    confirm(snapshots, "o3", size="1125", price="0.04")
    client.conditional_balance.return_value = D(1125)
    await runtime.tick(None)
    assert inv.live("SELL")[0].order.size == 1125
    assert inv.live("BUY")[0].order.price == 0.043


async def test_restart_checks_reward_band_before_market_buy(setup):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    confirm(snapshots, "o1")
    await runtime.tick(None)
    inv.market.rewards_max_spread_cents = D("0.1")
    await runtime.tick(None)
    client.place_market_order.assert_not_awaited()
    assert inv.restart_entry


async def test_both_full_continue_ladder_with_bought_inventory(setup):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    confirm(snapshots, "o1")
    confirm(snapshots, "o2", size="1046", price="0.043")
    client.conditional_balance.return_value = D(1046)
    await runtime.tick(None)
    assert inv.shares == 1046 and inv.cycle == 1
    assert not inv.restart_entry and runtime.stopping_reason is None
    await runtime.tick(None)
    assert inv.live("SELL")[0].order.size == 1046
    assert inv.live("BUY")[0].order.size == 1046
    client.place_market_order.assert_not_awaited()


async def test_sell_cancel_races_with_buy_fill_and_triggers_liquidation(setup):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    confirm(snapshots, "o1", size="100")
    snapshots["o1"].status = "LIVE"
    original = client.cancel_order.side_effect

    async def cancel(oid):
        if oid == "o2":
            confirm(snapshots, oid, size="10", price="0.043")
        await original(oid)

    client.cancel_order.side_effect = cancel
    client.conditional_balance.return_value = D(910)
    await runtime.tick(None)
    client.place_market_order.assert_awaited_once_with(inv.plan.token_id, "SELL", 910.0)
    assert client.place_limit_order.call_count == 3


async def test_unsettled_lower_buy_cancels_quotes_but_waits_to_liquidate(setup):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    confirm(snapshots, "o2", size="10", price="0.043", status="MINED")
    snapshots["o2"].status = "LIVE"
    await runtime.tick(None)
    assert snapshots["o1"].terminal and snapshots["o2"].terminal
    client.place_market_order.assert_not_awaited()
    snapshots["o2"] = snapshots["o2"].model_copy(deep=True)
    snapshots["o2"].fills[0].status = "CONFIRMED"
    client.conditional_balance.return_value = D(1010)
    await runtime.tick(None)
    client.place_market_order.assert_awaited_once_with(inv.plan.token_id, "SELL", 1010.0)


async def test_partial_market_liquidation_retries_only_confirmed_residual(setup):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    confirm(snapshots, "o2", size="10", price="0.043")
    client.conditional_balance.return_value = D(1010)
    await runtime.tick(None)
    confirm(snapshots, "o3", size="1000", price="0.044")
    snapshots["o3"].status = "CANCELED"
    client.conditional_balance.return_value = D(10)
    await runtime.tick(None)
    assert client.place_market_order.call_args.args == (inv.plan.token_id, "SELL", 10.0)
    assert not runtime.state.killed
    confirm(snapshots, "o4", size="10", price="0.043")
    await runtime.tick(None)
    assert runtime.state.killed and inv.shares == 0


async def test_cancel_failure_prevents_liquidation_and_reentry(setup):
    runtime, client, snapshots, _, _ = await quoted_inventory(setup)
    confirm(snapshots, "o2", size="10", price="0.043")
    client.cancel_order.side_effect = None
    with pytest.raises(RuntimeError, match="not confirmed"):
        await runtime.tick(None)
    client.place_market_order.assert_not_awaited()
    assert client.place_limit_order.call_count == 3


async def test_lower_buy_stop_cancels_and_liquidates_other_session_markets(setup):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    other = ew.Inventory(
        inv.market.model_copy(update={"condition_id": "other"}),
        inv.plan,
        D(92),
        shares=D(50),
        cost=D("2.25"),
    )
    runtime.inventory["other"] = other
    runtime.state.positions["other"] = runtime.state.positions[inv.market.condition_id].model_copy(
        deep=True
    )
    await runtime.place(other, inv.orders[2].order)
    confirm(snapshots, "o2", size="10", price="0.043")
    client.conditional_balance.return_value = D(1010)
    await runtime.tick(None)
    assert snapshots["o3"].terminal
    assert [c.args[2] for c in client.place_market_order.call_args_list] == [1010.0, 50.0]
    assert all(c.args[1] == "SELL" for c in client.place_market_order.call_args_list)
    assert not runtime.state.killed


async def test_restart_market_buy_zero_fill_never_places_standalone_lower_bid(setup):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    confirm(snapshots, "o1")
    await runtime.tick(None)
    await runtime.tick(None)
    snapshots["o3"] = snapshots["o3"].model_copy(update={"status": "CANCELED"})
    await runtime.tick(None)
    assert inv.market.condition_id not in runtime.inventory
    assert client.place_limit_order.call_count == 3
    assert client.place_market_order.call_count == 1


async def test_both_fills_discovered_during_cancel_still_continue_ladder(setup):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    confirm(snapshots, "o1")

    async def cancel(oid):
        confirm(snapshots, oid, size="1046", price="0.043")

    client.cancel_order.side_effect = cancel
    client.conditional_balance.return_value = D(1046)
    await runtime.tick(None)
    assert runtime.stopping_reason is None and inv.shares == 1046
    await runtime.tick(None)
    assert inv.live("SELL")[0].order.size == 1046
    client.place_market_order.assert_not_awaited()


async def test_liquidation_waits_for_available_shares_without_claiming_flat(setup):
    runtime, client, snapshots, inv, _ = await quoted_inventory(setup)
    confirm(snapshots, "o2", size="10", price="0.043")
    client.conditional_balance.return_value = D(0)
    await runtime.tick(None)
    assert inv.shares == 1010 and not runtime.state.killed
    client.place_market_order.assert_not_awaited()
    client.conditional_balance.return_value = D(1010)
    await runtime.tick(None)
    client.place_market_order.assert_awaited_once()
