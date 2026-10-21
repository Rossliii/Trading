"""Normalize authenticated order/trade responses for settlement-aware polling."""

from decimal import Decimal

from pydantic import BaseModel, Field

from app.types import NonNegativeDecimal, PositiveDecimal


class SettledFill(BaseModel):
    trade_id: str
    status: str
    size: PositiveDecimal
    price: PositiveDecimal = Field(lt=1)
    taker: bool


class OrderSnapshot(BaseModel):
    order_id: str
    status: str
    matched: NonNegativeDecimal
    fills: list[SettledFill]

    @property
    def terminal(self) -> bool:
        return self.status.upper() in {"MATCHED", "CANCELED", "CANCELLED", "EXPIRED"}

    @property
    def settled(self) -> bool:
        resolved = sum(
            (f.size for f in self.fills if f.status in {"CONFIRMED", "FAILED"}), Decimal(0)
        )
        return resolved >= self.matched and all(
            f.status in {"CONFIRMED", "FAILED"} for f in self.fills
        )


def normalize_snapshot(order: dict, trades: list[dict]) -> OrderSnapshot:
    oid = str(order["id"])
    fills = []
    seen = set()
    for trade in trades:
        tid = str(trade["id"])
        if tid in seen:
            continue
        seen.add(tid)
        if str(trade["taker_order_id"]) == oid:
            size, price, taker = trade["size"], trade["price"], True
        else:
            maker = next((m for m in trade["maker_orders"] if m["order_id"] == oid), None)
            if maker is None:
                raise ValueError("Associated trade does not contain tracked order")
            size, price, taker = maker["matched_amount"], maker["price"], False
        fills.append(
            SettledFill(
                trade_id=tid,
                status=str(trade["status"]).upper(),
                size=size,
                price=price,
                taker=taker,
            )
        )
    return OrderSnapshot(
        order_id=oid, status=str(order["status"]), matched=order["size_matched"], fills=fills
    )
