# Extreme Price Reward Farming

`app/farm/worker.py::run_farm` starts `app/farm/extreme_worker.py`. The old
worker helper functions remain for historical tests; the live entry point does
not start their quote, fill, or automatic liquidation loops.

The strategy selects rewarded markets with observed YES/NO midpoints at or below
0.05 and at or above 0.95, prioritizing the highest daily market reward pool.
It submits a price-capped limit BUY for the cheap token. Only CONFIRMED trades
count as inventory. After confirmation, it offers available shares at the calculated sell
price and places one lower BUY. Both follow-up orders are post-only.

Before the first entry BUY, the worker computes SELL = best bid + one tick and
rebuy BUY = best ask - two ticks from the cheap token's live book. Both must
fall strictly inside the size-adjusted reward spread and meet minimum size.
If either fails, no entry is submitted and screening continues to the next market.
Nonpositive, off-grid or self-crossing quotes are rejected; rebuy remains at most 5 cents.

After entry, the same formulas follow live price changes. There is no inventory
cost floor: a sell can realize a loss. The legacy `extreme_sell_markup` and
`extreme_rebuy_discount` settings are accepted but have no effect.

`requote.watch_quote_changes` subscribes to the market WebSocket and wakes the
inventory worker. The worker verifies fresh REST books and processes orders
serially. A changed quote cancels only the affected side, waits for confirmed
cancellation/settlement, then checks inventory and cash again before replacement.
The existing requote hysteresis suppresses safe one-tick reversals for 3 seconds.
If new quotes fall outside the reward band, existing quotes are cancelled.

Partial entries are cancelled before quoting; incomplete settlement waits. A
zero-fill entry is cancelled after 60 seconds and is not retried that session.
Repeated polls deduplicate trade IDs. Confirmed sells release inventory cost;
confirmed buys consume it. Additional buys cannot exceed the market allocation.

## Configuration

Add these optional top-level fields to your local `Config` JSON (prices are
dollars, not cents). Existing license, bankroll and filter fields remain:

```json
{
  "extreme_capital_budget": "85",
  "extreme_fee_reserve": "2"
}
```

Set `filters.reward_min` to your minimum **market reward pool per day**, for
example `50`. It is not a personal earnings estimate. The remaining legacy
filter fields, quote depth and size tiers are accepted for compatibility but
are not used by this strategy. `bankroll`, `max_concurrent_positions`, the
user blacklist and `max_session_loss` still apply.

Each market splits its principal allocation **50/50 in dollars**: half for the
initial acquisition and half for the lower BUY. With the default $85 principal,
$42.50 buys 944 whole shares at 0.045; the other $42.50 supports 988 shares at
0.043 (best bid 0.044, best ask 0.045, tick 0.001). The separate $2 fee reserve makes the total allocation $87. Whole-share
rounding leaves small unused balances. `extreme_target_shares` is a legacy input
and no longer caps either order. Lower bids are resized from their own fixed
half-budget when repriced, subject to actual cash and the overall inventory cap.
Partial entry fills do not enlarge the lower-buy half. Entry fees consume the
fee reserve rather than being deducted a second time from lower-buy principal.
Both planned sides must meet minimum reward size, otherwise the market is skipped.

Start the backend with `uv run server`. In a second terminal run
`uv run python -m scripts.terminal`, then use `start` and its `START` prompt.
Starting the backend alone does not place trades.

## Fill-driven ladder transitions

Any detected quote match cancels both sides of that market's current cycle.
The worker waits for cancellation and trade settlement, including fills racing
with cancellation, before deciding what to do. Next-cycle quotes use a fresh
book: SELL = best bid + one tick, BUY = best ask - two ticks; the reward midpoint
is for eligibility, not the price formula.

| Settled outcome | Action |
| --- | --- |
| SELL partially filled, no BUY fill | Requote all available remaining inventory and the lower BUY. |
| SELL fully filled, no BUY fill, inventory zero | Cancel lower BUY; revalidate rewards, budget and market eligibility; market BUY the entry half; wait for settlement, then quote actual acquired inventory. |
| Lower BUY partially or fully filled, unless both sides fully filled | Cancel all session orders, wait for settlement, market SELL all session inventory, then stop. |
| Both SELL and lower BUY fully filled in the same cycle | Keep the newly bought inventory and start the next ladder cycle without another market entry. |

The both-full case takes precedence. A partial SELL plus any BUY fill stops.
An independently observed BUY fill does not wait speculatively for a future SELL;
only fills visible after cancellation/settlement count toward the exception.
Repricing without fills stays in the same cycle. Logs identify `ladder_fill`,
`lower_buy_stop`, `extreme_ladder_next`, and `extreme_ladder_stop`.

Market orders use FAK: BUY amount is dollars, SELL amount is shares. Execution
price and quantity can differ from the book estimate. Initial acquisition still
uses the existing price-capped limit order; completed-sale reentry uses market BUY.
Market reentry with no confirmed fill is not blindly retried. Liquidation retries
only a confirmed residual after a terminal FAK; it waits if inventory is not yet
spendable. An API failure stops with an error and may leave residual inventory;
submission alone never means liquidation succeeded. Existing wallet holdings
outside this session are not sold. Detection uses the existing WebSocket wakeups
and 5-second polling fallback, not a guaranteed instantaneous execution time.

## Lifecycle

- Discovery refreshes in the background every 60 seconds after each refresh
  completes. WebSocket updates wake order reconciliation, with a 5-second polling
  fallback; API latency still applies. Both execution adapters query authenticated
  order and associated trade records, so a missed WebSocket frame cannot turn
  an unfilled order into inventory.
- Quotes must fit the current reward size/spread and price tick. If data or
  prices no longer support those quotes, they are cancelled; inventory remains.
  Reward eligibility and actual payouts still depend on exchange scoring.
- On stop, disconnect, session loss limit or an ambiguous API/submission error,
  the worker cancels all wallet orders and verifies the open-order list. It
  **does not market-sell remaining inventory**. The loss limit stops further
  orders; it does not guarantee a maximum realized loss on retained shares.
- Startup also cancels existing wallet orders. Use a dedicated wallet.
  Existing token holdings are skipped, not adopted or sold. Restart does not
  resume previous inventory cycles; inspect or manage retained holdings before
  restarting. Orders and runtime inventory are tracked in memory.
- If cancellation cannot be verified, the worker reports an error; check the
  wallet's orders before restarting.

Tests use mocked exchange responses and do not place real orders.

## Terminal tables

The periodic summary and `status` command show the eight selected summary metrics
and a positions table. Each position shows its question, unrealized PnL marked
at the latest token best-bid/ask midpoint, and YES BUY / YES SELL / NO BUY / NO SELL.
Each order cell is **remaining unfilled shares / limit-price notional value**;
it is not the held inventory or collateral required by a sell order. Values
aggregate orders with the same outcome and side. Missing marks/order data show
`--`; known absence of orders shows `0 / $0`. Realized trading PnL and rewards
are not included in the position's unrealized PnL. Diagnostic order and error
events continue to print, including detailed cancellation reasons.

## Cancellation diagnostics

Run `uv run server --log` to persist cancellation diagnostics in `logs/`.
The terminal's `order_cancelled` event now includes `reason` and `details`.
Details include order side, price, requested/matched shares, order age, reward
threshold, book timestamp, tick size, best bid/ask and (when evaluated) the
size-adjusted reward midpoint. Historical logs cannot recover missing details.

Reasons: `entry_partial_fill`, `entry_timeout`, `market_missing`,
`market_expired`, `reward_below_minimum`, `market_excluded`, `strategy_halted`,
`book_missing`, `book_one_sided`, `tick_size_change`,
`reward_depth_insufficient`, `outside_reward_spread`,
`quote_would_cross`, `dynamic_quote_unavailable`, `ladder_fill`, and `lower_buy_stop`. `requote` identifies a
cancel for replacement; its details include the old/new prices, live midpoint,
and inventory cost per share. Tick changes cause re-rounding and replacement
where a valid new quote is possible, rather than permanently halting the market.
When multiple checks fail, the first failed check is
reported in the order used by the strategy.

File logs distinguish `extreme_cancel_requested`, `extreme_cancel_confirmed`,
`extreme_cancel_unverified`, and `extreme_cancel_raced`. The last means the
exchange reported a different terminal order state during cancellation, such
as MATCHED; it does not emit a successful cancellation event. Bulk cancellation
logs carry `startup`, `shutdown`, `session_loss`, or `worker_error` as the reason.
