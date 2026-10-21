from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.bot.execution import LegacyExecutionClient, SecureExecutionClient
from app.bot.order_snapshot import normalize_snapshot


def trade(**changes):
    return (
        dict(
            id="t1",
            status="CONFIRMED",
            taker_order_id="entry",
            size="250",
            price="0.045",
            maker_orders=[dict(order_id="sell", matched_amount="100", price="0.048")],
        )
        | changes
    )


def order(oid="entry", **changes):
    return dict(id=oid, status="MATCHED", size_matched="250", associate_trades=["t1"]) | changes


def test_taker_and_maker_quantities_and_duplicate_trade_rows():
    snap = normalize_snapshot(order(), [trade(), trade()])
    assert snap.terminal and snap.settled
    assert len(snap.fills) == 1
    assert snap.fills[0].size == D("250") and snap.fills[0].taker
    maker = normalize_snapshot(order("sell", size_matched="100"), [trade()])
    assert maker.fills[0].size == D("100")
    assert maker.fills[0].price == D("0.048")
    assert not maker.fills[0].taker


@pytest.mark.parametrize("status", ["MATCHED", "MINED", "RETRYING"])
def test_unconfirmed_and_missing_trades_are_not_settled(status):
    assert not normalize_snapshot(order(), [trade(status=status)]).settled
    assert not normalize_snapshot(order(), []).settled


def test_associated_trade_must_reference_order():
    with pytest.raises(ValueError, match="does not contain"):
        normalize_snapshot(order("other"), [trade()])


async def test_legacy_adapter_reads_associated_trades_and_base_unit_balance():
    raw = MagicMock()
    raw.get_order.return_value = order()
    raw.get_trades.return_value = [trade()]
    raw.get_balance_allowance.return_value = {"balance": "250000000"}
    client = LegacyExecutionClient(raw, "GNOSIS_SAFE")
    snap = await client.order_snapshot("entry")
    assert snap.settled
    assert raw.get_trades.call_args.args[0].id == "t1"
    assert await client.conditional_balance("token") == 250
    raw.update_balance_allowance.assert_called_once()


async def test_secure_adapter_reads_associated_trades_and_base_unit_balance():
    raw = MagicMock(wallet_type="DEPOSIT_WALLET")
    raw.get_order.return_value = SimpleNamespace(
        associate_trades=["t1"], model_dump=lambda: order()
    )
    raw.list_account_trades.return_value.iter_items.return_value = [
        SimpleNamespace(model_dump=lambda: trade())
    ]
    raw.get_balance_allowance.return_value = SimpleNamespace(balance=250000000)
    client = SecureExecutionClient(raw)
    snap = await client.order_snapshot("entry")
    assert snap.settled
    raw.get_order.assert_called_once_with(order_id="entry")
    raw.list_account_trades.assert_called_once_with(id="t1")
    assert await client.conditional_balance("token") == 250
