import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.api.farm.handlers import FarmSession
from app.db.blacklist import BlacklistedMarket
from app.db.wallets import Wallet
from app.farm import worker as worker_mod

CLOSED = "Cannot send a request, as the client has been closed."


def wallet() -> Wallet:
    return Wallet(
        license_key="lic",
        wallet_id="A",
        proxy_address="0x" + "ab" * 20,
        private_key="0x" + "11" * 32,
    )


def patch_session(monkeypatch, sent):
    from app.farm import extreme_worker as extreme

    mock_client = MagicMock()
    mock_client.wallet_type = "GNOSIS_SAFE"
    mock_client.cancel_all = AsyncMock()
    mock_client.get_open_order_ids = AsyncMock(return_value=set())
    monkeypatch.setattr(worker_mod, "build_execution_client", lambda wallet: mock_client)
    monkeypatch.setattr(worker_mod, "ensure_approval", AsyncMock())
    monkeypatch.setattr(worker_mod, "list_blacklist", AsyncMock(return_value=[]))
    monkeypatch.setattr(extreme, "get_balance", AsyncMock(return_value=100))

    async def idle(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(worker_mod, "push_summary_loop", idle)
    monkeypatch.setattr(extreme, "heartbeat_loop", idle)
    monkeypatch.setattr(extreme, "rewards_poll_loop", idle)

    async def fake_send_event(websocket, event):
        sent.append(event)

    monkeypatch.setattr(worker_mod, "send_event", fake_send_event)
    monkeypatch.setattr(extreme, "send_event", fake_send_event)
    return mock_client, extreme


async def test_run_farm_lifecycle_cancels_and_loads_exclusions(farm_state, monkeypatch):
    sent = []
    client, extreme = patch_session(monkeypatch, sent)
    monkeypatch.setattr(
        worker_mod,
        "list_blacklist",
        AsyncMock(return_value=[BlacklistedMarket(license_key="lic", condition_id="excluded")]),
    )
    session = FarmSession()

    async def tick(runtime, http):
        assert session.state is runtime.state
        assert runtime.state.excluded_markets == {"excluded"}
        runtime.state.killed = True

    monkeypatch.setattr(extreme.ExtremeRuntime, "tick", tick)
    await worker_mod.run_farm(MagicMock(), farm_state.config, wallet(), "lic", session)
    worker_mod.ensure_approval.assert_awaited_once()
    assert session.state is None
    assert client.cancel_all.await_count == 2
    assert client.get_open_order_ids.await_count == 2


async def test_run_farm_manual_stop_cancels_without_liquidating(farm_state, monkeypatch):
    client, extreme = patch_session(monkeypatch, [])
    in_tick = asyncio.Event()
    observed = []

    async def block(runtime, http):
        runtime.state.positions.update(farm_state.positions)
        observed.append(runtime.state)
        in_tick.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(extreme.ExtremeRuntime, "tick", block)
    session = FarmSession()
    task = asyncio.create_task(
        worker_mod.run_farm(MagicMock(), farm_state.config, wallet(), "lic", session)
    )
    await asyncio.wait_for(in_tick.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.cancel_all.await_count == 2
    assert observed[0].positions["market-A"].yes_shares == 100
    assert session.state is None


async def test_run_farm_blacklist_failure_prevents_trading(farm_state, monkeypatch):
    from app.exceptions import BlacklistPersistenceError

    sent = []
    client, extreme = patch_session(monkeypatch, sent)
    monkeypatch.setattr(
        worker_mod, "list_blacklist", AsyncMock(side_effect=BlacklistPersistenceError())
    )
    tick = AsyncMock()
    monkeypatch.setattr(extreme.ExtremeRuntime, "tick", tick)
    await worker_mod.run_farm(MagicMock(), farm_state.config, wallet(), "lic", FarmSession())
    tick.assert_not_awaited()
    client.cancel_all.assert_not_awaited()
    assert sent[0].reason == "blacklist_unavailable; farm not started"


async def test_run_farm_unknown_order_state_stops_instead_of_retrying(farm_state, monkeypatch):
    sent = []
    client, extreme = patch_session(monkeypatch, sent)
    tick = AsyncMock(side_effect=RuntimeError("uncertain submission"))
    monkeypatch.setattr(extreme.ExtremeRuntime, "tick", tick)
    await worker_mod.run_farm(MagicMock(), farm_state.config, wallet(), "lic", FarmSession())
    tick.assert_awaited_once()
    assert client.cancel_all.await_count == 2
    assert any("extreme_worker_stopped" in event.reason for event in sent)


async def test_run_farm_undeployed_deposit_wallet_skips_cleanly(farm_state, monkeypatch):
    from app.exceptions import WalletNotDeployedError

    sent = []
    client, _ = patch_session(monkeypatch, sent)

    def undeployed(wallet):
        raise WalletNotDeployedError("proxy not deployed")

    monkeypatch.setattr(worker_mod, "build_execution_client", undeployed)
    await worker_mod.run_farm(MagicMock(), farm_state.config, wallet(), "lic", FarmSession())
    worker_mod.ensure_approval.assert_not_awaited()
    client.cancel_all.assert_not_awaited()
    assert wallet().proxy_address in sent[0].reason


async def test_run_farm_deposit_wallet_skips_ensure_approval(farm_state, monkeypatch):
    client, extreme = patch_session(monkeypatch, [])
    client.wallet_type = "DEPOSIT_WALLET"

    async def tick(runtime, http):
        runtime.state.killed = True

    monkeypatch.setattr(extreme.ExtremeRuntime, "tick", tick)
    await worker_mod.run_farm(MagicMock(), farm_state.config, wallet(), "lic", FarmSession())
    worker_mod.ensure_approval.assert_not_awaited()
    assert client.cancel_all.await_count == 2


async def test_exit_reconcile_loop_stops_on_closed_client(farm_state, monkeypatch):

    async def raise_closed(client, state, http):
        raise RuntimeError(CLOSED)

    monkeypatch.setattr(worker_mod, "reconcile_onchain_positions", raise_closed)
    await asyncio.wait_for(
        worker_mod.exit_reconcile_loop(MagicMock(), farm_state, MagicMock()), timeout=1
    )


async def test_exit_reconcile_loop_retries_other_runtime_errors(farm_state, monkeypatch):
    monkeypatch.setattr(worker_mod, "EXIT_RECONCILE_SECONDS", 0)
    farm_state.positions.clear()
    calls = {"n": 0}

    async def flaky(client, state, http):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("some other transient failure")
        state.killed = True

    monkeypatch.setattr(worker_mod, "reconcile_onchain_positions", flaky)
    await asyncio.wait_for(
        worker_mod.exit_reconcile_loop(MagicMock(), farm_state, MagicMock()), timeout=1
    )
    assert calls["n"] == 2


async def test_exit_reconcile_loop_still_cancellable(farm_state, monkeypatch):

    async def block(client, state, http):
        await asyncio.Event().wait()

    monkeypatch.setattr(worker_mod, "reconcile_onchain_positions", block)
    task = asyncio.create_task(worker_mod.exit_reconcile_loop(MagicMock(), farm_state, MagicMock()))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_exit_reconcile_loop_stops_when_state_killed(farm_state, monkeypatch):
    monkeypatch.setattr(worker_mod, "EXIT_RECONCILE_SECONDS", 0)
    farm_state.positions.clear()

    async def noop(client, state, http):
        return None

    monkeypatch.setattr(worker_mod, "reconcile_onchain_positions", noop)
    farm_state.killed = False
    task = asyncio.create_task(worker_mod.exit_reconcile_loop(MagicMock(), farm_state, MagicMock()))
    await asyncio.sleep(0.01)
    assert not task.done()
    farm_state.killed = True
    await asyncio.wait_for(task, timeout=1)
