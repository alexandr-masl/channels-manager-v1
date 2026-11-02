# Profit target reduction implementation plan

> For agentic workers: use superpowers:executing-plans to implement each slice.

**Status:** Implemented and verified.

**Goal:** Keep valid trades executable when the configured profit allocation
creates orders below exchange minimums.

**Architecture:** A pure BingX allocation planner selects the largest feasible
target count after price resolution and entry sizing. The builder publishes the
selected prices/fractions through the existing trade contract. No new exchange
requests, configuration or Trading Station runtime changes.

**Tech stack:** Rust, existing precision metadata, bounded decimal lot arithmetic,
unit tests and the isolated RabbitMQ workflow harness.

## Traced behavior

Original TypeScript commit `342f2fc`:
`src/binance/createBingXFuturesAutoTrade.ts` calls
`min_trade_quantity()` → `check_possible_sell_targets()` → `def_sell_targets()`.
The latter retains the earliest targets and redistributes reduced allocations
as integer percentages, with the remainder on the last target: `33/33/34`.

Rust ported this in commit `64b233d`. Both implementations trigger reduction only
when the target-count estimate fails. Individual fraction validation can therefore
reject a trade after the estimate passes, without attempting reduction.

Observed AM3 configuration: four targets at `40/30/20/10%`; ETH minimum and lot
size `0.01`. A synthetic `0.06 ETH` position passes the old four-target estimate
but assigns `0.006 ETH` to its last target and rejects. The observed `0.04 ETH`
trade succeeds after reduction to three targets. Rejected-account quantities
were absent from production logs; synthetic examples are regression inputs,
not claims about their historical balances.

## Allocation contract

1. Resolve target prices and validate signal/price geometry separately from sizing.
   Mixed formats, invalid prices/settings, invalid percentage bounds and arithmetic
   errors reject normally; reducing target count must not hide them.
2. Start with `min(configured_count, signal_count)`. Try the existing configured
   fractions, or existing equal fractions when there are fewer source targets
   than configured targets. A single selected target receives `100%`.
3. Preserve a feasible allocation, including its existing fraction wire values.
4. If it fails sizing, try `count - 1` down to one, largest first. Keep the first
   N resolved target prices. Generate the original equal percentage splits.
   Do not add targets beyond the configured count or retry equal weights at the
   original count; the approved fallback is reduction.
5. For each candidate, simulate Trading Station allocation with exact decimal
   arithmetic and the exchange lot size:

   ```text
   total_quantity = exact_decimal_sum(serialized entry target quantities)
   remaining = total_quantity
   for each fraction:
       quantity = floor_to_lot(min(total_quantity × fraction, remaining))
       remaining -= quantity
   ```

   Read each already-sized entry quantity from its serialized decimal value and
   sum using checked decimal arithmetic. Do not convert the builder's accumulated
   `f64` total into the planner input: `0.01 + 0.06` can become
   `0.06999999999999999`, while the exact entry sum is `0.07`. This changes only
   the feasibility calculation, not entry sizing or the published position fields.

   Validate every resulting quantity against the existing effective quantity
   bounds, including the current `Precision.min_qty` policy derived from
   `max(exchange minQty, minNotional / current_price)`. Preserve the published
   precision values; compare their decimal wire values exactly in the planner.
   Additionally require `rounded_quantity × resolved_target_price >= minNotional`.
   This target-price notional check intentionally replaces the current profit
   check at the current quote; it is a behavior change, not an unchanged policy.
   The current-quote-derived quantity minimum remains in force, so a higher LONG
   target price cannot waive it. Never round an order up to make it fit,
   over-allocate the position, or accept a zero-size target.
6. Replace the target-count feasibility heuristic with actual candidate checks.
   Remove the count-based `1.05` feasibility heuristic. For every one-target Pro
   candidate (initial or reduced), retain both safeguards: total entry quantity
   must be at least `3.5 × effective min_qty`, and executable entry notional must
   be at least `3.5 × minNotional`. Compute that notional as the checked decimal
   sum of serialized entry quantity times serialized entry price. Apply these
   checks in addition to per-target bounds; basic candidates do not inherit the
   Pro multiplier. A failed Pro minimum makes that candidate infeasible and, if
   no candidate remains, maps to `insufficientTargetBalance`.
7. If no permitted candidate fits because of minimum quantity/notional, return
   `insufficientTargetBalance`, already eligible for user notification history.
   Maximum-limit, invalid metadata/configuration and arithmetic failures retain
   internal classifications; do not describe them as insufficient balance.
8. Preserve entry sizing, position size, leverage, stops, strategy, expiry, stable
   trade/order IDs and Trading Station remainder handling. Planned quantities
   validate feasibility; the wire still sends fractions, and Trading Station
   allocates against actual fills through its existing workflow.

Arithmetic reference: `../trading-station-rust/src/trades/helpers/quantities.rs`
(`Quantity::allocate`) and `src/trades/profit.rs` (`allocate_profit_targets`).
Match decimal wire values and checked arithmetic; do not use floating-point
epsilon to promote a below-minimum allocation. Unsupported representations or
overflow must fail internally rather than be mistaken for insufficient balance.

## 1. Pure allocation planner

Files: new `src/exchanges/bingx/profit_allocation.rs`,
`src/exchanges/bingx/mod.rs`; tests alongside the private planner.

- [x] Write and run failing tests for AM3-style `0.01–0.10 ETH` positions,
  the `40/30/20/10` split, minimum quantity `0.01` and lot size `0.01`.
  With basic strategy and other limits satisfied, assert one target for `0.01`,
  two for `0.02–0.03`, three for `0.04–0.09`, and four for `0.10`.
  At `0.03`, `33/33/34` floors the first two orders to zero, so select `50/50`
  and preserve the `0.01` remainder. At `0.06`, select `33/33/34`, allocate
  `0.01/0.01/0.02`, and preserve the `0.02` remainder.
- [x] Implement candidate selection, original equal splits and exact lot-rounded
  quantity simulation. Return selected fractions and a bounded sizing diagnostic
  when reduction is required or no candidate fits.
- [x] Test valid configured splits, one configured target/four source targets,
  fewer source targets, successive reduction to three/two/one and no feasible
  target. Preserve existing remainder and remaining-quantity caps.
- [x] Test decimal boundaries, minimum notional, maximum quantity, invalid
  fractions/metadata, overflow and existing strategy safeguards.
  For one configured target, effective minimum quantity `0.01`, minimum notional
  `10`, entry/current price `1000`, and quantity `0.02`, assert that basic can
  succeed at a valid LONG target while Pro fails its `0.035`/`35` minimums.
  Test equality at both Pro thresholds and failure of each independently.
- [x] Check a range of increasing position quantities: once a feasible trade
  exists, greater quantities within entry/exchange bounds remain executable.

## 2. Builder integration and diagnostics

Files: `src/exchanges/bingx/targets.rs`, `src/exchanges/bingx/trade_builder.rs`,
`src/trading/job_handler.rs`; tests: `tests/trade_builder.rs`,
`tests/user_failure_notifications.rs` as needed.

- [x] Add failing builder regressions for the reported ETH SHORT signal and
  per-account size differences, then replace the count-only reduction branch
  with the planner. Keep price resolution and allocation as separate steps.
- [x] Add a multi-entry decimal regression with serialized quantities `0.01`
  and `0.06`: the planner total must be exactly `0.07`, and a single target must
  allocate all seven `0.01` lots. Assert existing entry and position wire fields
  remain unchanged.
- [x] Test the explicit notional policy in both directions. With current/entry
  price `1000`, effective minimum quantity `0.01`, minimum notional `10`, and a
  single `0.01` target allocation, a SHORT target at `900` fails (`9` notional)
  even though the current-quote check passes; a LONG target at `1100` passes.
  Separately verify that a higher LONG target price does not bypass the retained
  current-quote-derived quantity minimum.
- [x] Validate prices before sizing fallback. Cover absolute and percentage
  targets, rounding collapse and invalid supplied targets that would be omitted.
- [x] Preserve existing golden fixtures where behavior is valid. Explicitly
  review fixture changes caused by replacing the heuristic with actual limits.
- [x] Log reductions with channel/message, account ID/name, original/final count
  and the sizing constraint. Log infeasible allocations with actual rounded
  quantity/notional and required bounds. Never log credentials or raw job bodies.
- [x] Include channel/message/account identity in terminal builder rejections.
  Verify exhaustion maps to existing user-history reporting, while internal
  errors do not produce misleading balance notifications.
- [x] Run planner, builder and notification tests, then all-target Clippy.

## 3. Complete workflow and documentation

Files: `tests/signal_workflow_e2e.rs`, `tests/support/signal_workflow.rs`,
`tests/fixtures/signals/`, `docs/signal-manager.md`, `README.md`.

- [x] Add an ETH workflow with accounts of different scripted balances: valid
  configured allocation, reduced allocation and valid entries with infeasible exits.
  Assert final target prices/fractions, original position size and no final
  trade plus an `insufficientTargetBalance` user-history entry when no target fits.
  Use a SHORT fixture whose smallest account passes entry minimums but fails
  target-price notional even at one target. A concrete boundary is entry/current
  price `1000`, quantity `0.01`, minimum quantity/lot `0.01`, minimum notional
  `10`, and first profit price `900`: entry notional is `10`, exit notional is `9`.
  Larger accounts must exercise reduced and configured allocations with the same
  signal. Do not use below-minimum entries to demonstrate planner exhaustion:
  they fail earlier with `orderOutsideExchangeLimits` (or a quantity error).
  Keep a separate regression confirming those entry failures do not acquire
  misleading target-balance notifications.
- [x] Cover a configured single target, percentage targets and optional exits.
  Assert reduction needs no additional exchange calls or account mutations.
- [x] Document the selection rule, exact rounding, preserved remainder behavior
  and account-specific diagnostics, including the intentional target-price
  notional check, retained effective quantity minimum, and one-target Pro guard.
- [x] Run `./scripts/test-signal-workflow.sh`, then `./scripts/verify.sh` with
  disposable services; review this feature's changes before completion.

Live signals, registry pushes and deployment are separate from implementation.

Verification: 40 isolated workflow scenarios; full verification passed 338 tests
with zero failures or ignored tests. Specification and code reviews found no issues.
