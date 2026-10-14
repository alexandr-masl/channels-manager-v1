# Signal workflow E2E

## Boundary

Launch the real app binary with disposable RabbitMQ, MongoDB and Redis. Publish
the original Telegram envelope to `tg_bot_channel_update`, let the app publish
and consume `satoshi-channel-updates.client-trade.bingx.futures`, then capture
the final message from `create-new-trusted-trade`.

Only BingX and Telegram HTTP responses are scripted. All application parsing,
Mongo queries, settings resolution, admission, signing, sizing, construction and
RabbitMQ delivery run through production code. Trading Station/order execution
is beyond this suite's publication boundary.

Use the approach in `../trading-station-rust/docs/advanced-lifecycle-e2e.md`:
isolated services, strict HTTP expectations, bounded waits, observable results,
request journals and cleanup on failure. Extend/extract our existing binary
fixture in `tests/rabbitmq.rs` rather than maintain a second service harness.

## Input fixtures

Store the three supplied texts unchanged, including decimal spelling and emoji.

| Signal | Required result |
| --- | --- |
| ADA entry 0.217, three targets, 3x | Final ADA-USDT LONG trade using 3x |
| SOL entry 71–72, four targets, 50x, position 0.5% | Both parsed entry endpoints, all four source targets, position fraction 0.005 and final SOL-USDT trade using 50x |
| ADA entry 0.2570, three targets, no leverage | Resolve leverage separately for each eligible client, then publish |

Seed deterministic channel target settings, exchange precision, prices and
balances so expected final targets and quantities can be calculated independently.
Final target allocation follows configured buy/sell target counts; it need not
copy the raw signal's target count without applying settings.

## Missing leverage prerequisite

The current parser rejects missing leverage. Make omission distinguishable from
an explicitly invalid value, then resolve before publishing each client job:

1. Explicit valid signal leverage.
2. Eligible active user/channel Futures override.
3. Channel Futures leverage.

Use a documented `futures.leverage` configuration field for this addition;
the current Rust settings resolver has no leverage fallback field. Apply existing
user ownership, `own_settings` and active Futures settings rules. Missing fallback
or invalid selected value rejects that client with a clear reason; malformed
explicit signal leverage never falls back. Do not assume 1x.

Keep the worker wire contract: every published job contains resolved leverage.
Do not mutate the shared parsed signal when creating jobs for multiple users.
This is an intentional behavior addition, not existing parser parity.

## Base scenarios

- Each supplied signal completes the full publication path.
- Matching exchange leverage sends no mutation; mismatched leverage sends one POST.
- Eligible One-Way account switches to Hedge, then changes leverage and publishes.
- Permitted One-Way route publishes `ONE_WAY_V1`; Hedge publishes `ORDER_LEDGER_V1`.
- Missing leverage uses channel configuration; two eligible users with different
  overrides receive different resolved leverage. Explicit signal leverage wins.
- Missing/invalid fallback, blocked leverage change, failed POST and expired work
  produce no final trade. Telegram reply failure does not block valid publication.

## Evidence per scenario

- Seed all three Mongo roles: channel/client context, active user validation and
  managed trades. Use synthetic credentials and deterministic business IDs.
- Confirm mandatory input publication; capture the actual final queue delivery.
- Compare the complete final envelope to independently authored expected fixtures:
  exactly `expires_at`, `trade_object`, `client_data`; check prices, quantities,
  target allocation, leverage, stop, strategy, routing and accounting model.
- Validate stable IDs by their contract. Bound runtime timestamps and verify the
  original expiry survives; do not blanket-ignore changing fields.
- Check one output per eligible client during this run, no unexpected outputs,
  retry/dead-letter messages, or exchange order-placement requests.
- Assert signed HTTP methods, paths, parameters and HMAC; reject extra requests
  and unused responses. Match parallel account GETs without assuming their order;
  enforce ordering between reads and mutations.
- Capture the `created ✅` reply via scripted Telegram HTTP.
- Wait for terminal processing and graceful consumer drain before asserting no
  output on rejection. Queue emptiness or a publisher confirm alone is insufficient.
- Print a compact scenario result and sanitized final JSON; include process and
  HTTP journals on failure. Logs support diagnosis, not exact wording assertions.

## Files and command

- `tests/signal_workflow_e2e.rs`: scenario matrix.
- `tests/support/signal_workflow.rs`: shared services, binary, seeding and capture.
- `tests/support/scripted_exchange.rs`: strict concurrent HTTP fixture.
- `tests/fixtures/signals/`: original texts and expected trade fixtures.
- `scripts/test-signal-workflow.sh`: single command; invokes the ignored
  suite sequentially, using the existing disposable service helpers.

Never load `.env.local`. Clear inherited app settings, use loopback endpoints and
temporary storage, bound startup/execution, and clean up on success/failure.
Include the suite in full verification; ordinary unit tests stay fast.

## Implementation order

1. Add missing-leverage resolution and focused precedence/validation tests.
2. Extract reusable binary/service helpers and strict HTTP fixtures.
3. Add the three positive workflows with complete output assertions.
4. Add the boundary/failure cases and single command; run full verification.

Prefer this isolated binary suite over extending the current log-heavy smoke test
or using live exchange credentials. It verifies the full application path with
repeatable inputs; it does not establish live BingX behavior or duplicate safety.

## Running the implemented suite

```sh
./scripts/test-signal-workflow.sh
```

Requires Cargo, RabbitMQ/Erlang, MongoDB and Redis on PATH (the script also checks
Homebrew binary locations). The suite runs 14 scenarios sequentially and prints
sanitized final payloads and a PASS journal for each. Ten final envelopes are
expected across the successful cases, including two separate user overrides.

The expired-job scenario deliberately injects an already-expired client job into
its worker queue to test that boundary without waiting a minute. Other scenarios
start with the raw Telegram envelope. The three Mongo roles use separate databases
on the disposable Mongo service. Signed BingX and Telegram responses are local
scripts; this suite does not call either live API.

The HTTP fixture has ordinary unit tests for bad signatures, wrong parameters,
phase ordering and unexpected requests. The integration matrix is ignored during
ordinary `cargo test` and included by `./scripts/verify.sh`.
