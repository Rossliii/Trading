"""Compact terminal views for farm snapshots; no account or trading side effects."""

import unicodedata
from decimal import Decimal, InvalidOperation


def width(text):
    return sum(
        0 if unicodedata.combining(c) else 2 if unicodedata.east_asian_width(c) in {"W", "F"} else 1
        for c in text
    )


def wrap(value, limit):
    lines, line = [], ""
    for char in str(value):
        if not char.isprintable():
            char = " "
        if width(line + char) > limit:
            lines.append(line)
            line = ""
        line += char
    return lines + [line]


def table(headers, rows, widths):
    border = "+" + "+".join("-" * (n + 2) for n in widths) + "+"
    result = [border]
    for index, row in enumerate([headers, *rows]):
        cells = [wrap(value, n) for value, n in zip(row, widths)]
        for i in range(max(map(len, cells))):
            values = [cell[i] if i < len(cell) else "" for cell in cells]
            result.append(
                "| "
                + " | ".join(value + " " * (n - width(value)) for value, n in zip(values, widths))
                + " |"
            )
        if index == 0:
            result.append(border)
    result.append(border)
    return "\n".join(result)


def number(value, *, money=False, signed=False):
    try:
        amount = Decimal(str(value))
        if not amount.is_finite():
            return "--"
    except (InvalidOperation, ValueError):
        return "--"
    text = f"{abs(amount):.4f}".rstrip("0").rstrip(".")
    prefix = "-" if amount < 0 else "+" if signed and amount > 0 else ""
    return prefix + ("$" if money else "") + text


def summary_table(event):
    fields = [
        "total_rewards",
        "rewards_per_hour",
        "rewards_per_day",
        "elapsed_seconds",
        "active_markets",
        "session_loss",
        "max_session_loss",
        "wallet_balance",
    ]
    rows = [
        (key, number(event.get(key), money=key not in {"elapsed_seconds", "active_markets"}))
        for key in fields
    ]
    return "Farm summary\n" + table(["Metric", "Value"], rows, [20, 20])


def positions_table(event):
    rows = []
    for position in event.get("positions", []):
        orders = position.get("limit_orders")
        cells = []
        for outcome, side in [("YES", "BUY"), ("YES", "SELL"), ("NO", "BUY"), ("NO", "SELL")]:
            if orders is None:
                cells.append("--")
                continue
            matched = [
                order
                for order in orders
                if order.get("outcome") == outcome and order.get("side") == side
            ]
            shares = sum((Decimal(str(order["shares"])) for order in matched), Decimal(0))
            value = sum((Decimal(str(order["value"])) for order in matched), Decimal(0))
            cells.append(f"{number(shares)} / {number(value, money=True)}")
        # Legacy snapshots with a null midpoint reported PnL=0 even when no mark existed.
        pnl = position.get(
            "position_pnl",
            position.get("unrealized_pnl") if position.get("midpoint") is not None else None,
        )
        rows.append([position.get("question", "--"), number(pnl, money=True, signed=True), *cells])
    if not rows:
        return "Positions: none"
    return "Positions | PnL: unrealized at midpoint | Orders: remaining shares / value\n" + table(
        ["Question", "Position PnL", "YES BUY", "YES SELL", "NO BUY", "NO SELL"],
        rows,
        [32, 13, 17, 17, 17, 17],
    )
