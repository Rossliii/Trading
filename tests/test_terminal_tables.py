from scripts.terminal_tables import positions_table, summary_table, table, width


def test_summary_only_contains_requested_metrics():
    rendered = summary_table(
        {
            "total_rewards": "0.24488",
            "total_volume": "9.6645",
            "wallet_balance": "102.738986",
            "elapsed_seconds": 392,
        }
    )
    assert "total_rewards" in rendered and "$0.2449" in rendered
    assert "392" in rendered and "$102.739" in rendered
    assert "total_volume" not in rendered and "9.6645" not in rendered


def test_positions_use_remaining_orders_not_held_shares():
    rendered = positions_table(
        {
            "positions": [
                {
                    "question": "Example market?",
                    "yes_shares": "99999",
                    "position_pnl": "-0.1234",
                    "limit_orders": [
                        {"outcome": "YES", "side": "BUY", "shares": "25", "value": "1"},
                        {"outcome": "YES", "side": "BUY", "shares": "5", "value": "0.2"},
                        {"outcome": "YES", "side": "SELL", "shares": "74", "value": "4.44"},
                    ],
                }
            ]
        }
    )
    assert "30 / $1.2" in rendered and "74 / $4.44" in rendered
    assert "-$0.1234" in rendered and "99999" not in rendered
    assert "0 / $0" in rendered


def test_unknown_pnl_and_orders_are_not_reported_as_zero():
    rendered = positions_table(
        {"positions": [{"question": "Old snapshot", "midpoint": None, "unrealized_pnl": "0"}]}
    )
    assert "--" in rendered and "$0" not in rendered
    assert positions_table({"positions": []}) == "Positions: none"


def test_table_wraps_wide_characters_and_strips_control_codes():
    rendered = table(["Question", "PnL"], [["中文市场标题很长\n\x1b", "-$1.25"]], [10, 8])
    assert len({width(line) for line in rendered.splitlines()}) == 1
    assert "\x1b" not in rendered
