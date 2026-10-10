# Signal manager implementation plan

2026-10-10 · Base version · BingX Futures only.

Slices 1–4 implemented: intake prepares/publishes jobs; the worker validates
admission, builds trades and confirms publication before acknowledgement.
Slice 5 provides isolated acceptance coverage and the cutover runbook. Live staging
acceptance and deployment remain operator actions.

Source of truth: TypeScript `src/handlers/signal-manager.ts`,
`src/market-data/signal-market-data.ts`, and the BingX trade creation flow.

## Scope

Include signal preparation, per-account jobs, confirmed RabbitMQ publication,
and a separate account-execution consumer in this Rust application.

Exclude all trade-count limits (overall, per-user and team), Mongo execution claims, Redis execution
locks and duplicate-execution protection. Keep stable job/trade identities and
partition keys for wire compatibility. A direct Telegram acceptance reply is
implemented. Other notifications, exchanges, Spot, commands and health endpoints
remain separate work.

## Flow

`tg_bot_channel_update` → `ChannelUpdateManager` → `SignalManager`
→ `satoshi-channel-updates.client-trade.bingx.futures` → `ClientTradeWorker`
→ `create-new-trusted-trade` (or `RABBITMQ_QUEUE`) → Trading Station.

The signal manager prepares work for all eligible accounts. Each execution job
handles one account. Trading Station owns order execution and ongoing management.
Initially both consumers run in one process with separate prefetch/concurrency
limits and explicit queue registration; Telegram intake can be disabled for worker-only cutover.

## Interfaces and modules

```rust
// Proposed signatures; dependencies and clock are injected.
SignalManager::prepare(ChannelContext)
    -> Result<PreparationOutcome, PreparationError>;
ClientTradeWorker::execute(ClientTradeJob)
    -> Result<ExecutionOutcome, ExecutionError>;
```

| File | Responsibility |
| --- | --- |
| `signals/manager.rs` | Prepare signal context; classify outcomes |
| `signals/jobs.rs` | Pure construction of existing ClientTradeJob payloads |
| `signals/settings.rs` | Original channel/user/Futures settings precedence |
| `exchanges/bingx/market_data.rs` | Symbol mapping, shared price and metadata |
| `exchanges/bingx/client.rs` | Bounded API requests and sanitized errors |
| `exchanges/bingx/admission.rs` | Account mode, symbol and leverage checks |
| `exchanges/bingx/trade_builder.rs` | Position sizing and target calculations |
| `trading/execution.rs` | Coordinate one account's trade preparation |
| `trading/job_handler.rs` | Decode job, publish result, settle delivery |
| `telegram/handler.rs` | Call manager, publish jobs, settle source delivery |
| `mongo/repositories.rs` | Per-user OPENED trade context; no global count query |
| `rabbitmq/` | Add client-job publication and independent consumer registrations |

Reuse existing contracts, identity helpers, publisher, lifecycle and retry policy.
Do not add generic exchange frameworks or duplicate Mongo/RabbitMQ infrastructure.

## A. Signal preparation and publication

1. Receive authorized ChannelContext; skip an empty client list.
2. Load OPENED trades for unique eligible users and group them by user. Reuse
   channel/user settings already loaded by intake. No trade-count rejection.
3. Load one BingX Futures price/metadata snapshot shared by all jobs. Keep parser
   symbol BTCUSDT; map it to BTC-USDT for exchange requests. Use optional metadata
   cache with API fallback, fetching current price separately.
4. Produce one job per eligible account using the existing ClientTradeJob contract:
   original signal, channel settings, userConfig, client credentials, marketData,
   openedTrades, sourceCreatedAt, stable idempotencyKey and partitionKey.
   Preserve absent userConfig, ObjectIds as strings, ISO dates, breakOutEntry and
   position. Settings resolution and quantity calculation happen in execution.
5. Set tradeExpiresAt to acceptance time + 60 seconds, separately from Telegram
   source time. Serialize each publication once and reuse its bytes on retries.
6. Confirm all job publications before acknowledging the Telegram delivery.
   Partial publication may repeat jobs; stable IDs alone do not suppress execution.

Preparation outcomes: Prepared(batch), Skipped(reason), Rejected(reason), or
retryable dependency failure. Unsupported symbols are rejected before publication;
transient market-data errors retry. This classifies errors earlier than TypeScript,
which can place an error snapshot into a job for later rejection.

Expiry follows the original acceptance model: each prepared job keeps its expiry
through worker retries and final trade publication. A fresh raw Telegram redelivery
can start a new acceptance window, as in TypeScript. Persisting acceptance across
source redelivery is deferred with durable deduplication; this base version adds no
acceptance store or claim records.

## B. Per-account execution and publication

1. Validate job structure, identity and expiry.
2. Resolve settings: channel defaults, matching user overrides only when
   own_settings is enabled, then active Futures overrides. Signal position size
   takes precedence using the original balance-fraction semantics.
3. Perform BingX symbol/account-mode/balance/leverage checks. Preserve the original
   decision matrix, automatically switching eligible One-Way accounts to Hedge.
   A successful POST is sufficient; leverage adjustment follows admission.
4. Build quantities and entry/profit/stop targets using original precision, minimum
   size and sizing rules. BREAKOUT selects STOP_LOSS_LIMIT entry targets.
5. Produce the original trade_object/client_data envelope, stable trade ID and
   unchanged expires_at. Confirm publication to Trading Station before job ack.
6. Log a sanitized outcome. No execution claim or deduplication record is written.

Execution outcomes: PreparedTrade, Rejected, Expired or RetryableDependencyFailure.
Retry temporary read failures through the existing bounded policy. Mode-switch
errors/timeouts stop and ACK the job without automatic mutation retry or readback.
Preserve expiry before switching and after admission. Process loss can still cause
redelivery; this version has no execution claims or exactly-once guarantee.

## Implementation slices

1. **Signal preparation:** OPENED queries, market-data adapter, job builder,
   fixture tests and sanitized job-summary logging; accepted work continues to publication.
2. **Job publication:** client-job destination, confirms, bounded fan-out/retries,
   expiry checks and Telegram settlement; publication is always active.
3. **Worker settings/admission:** consumer wiring, job validation, configuration
   precedence and bounded BingX checks.
4. **Trade construction/publication:** sizing, target types, exact trade envelope,
   expiry, failure classification and worker settlement.
5. **Verification/cutover:** TypeScript comparison fixtures, isolated integration
   tests, exclusive queue ownership and updated deployment instructions.

## Required tests

- One job per account, shared market fetch and correct per-user trade grouping.
- Exact TypeScript job/trade JSON fixtures, identities and BSON conversions.
- Settings precedence, position override, LONG/SHORT, ranges and breakout targets.
- Empty context, invalid symbol/settings, expired jobs and dependency failures.
- Partial confirms, lost acknowledgements, queue-specific retries and diagnostics.
- Known duplicate-delivery behavior without claiming deduplication guarantees.
- No credentials in logs/errors; accepted work publishes without enablement flags.

Log channel/message IDs, canonical symbol, eligible/prepared/published counts,
expiry and static outcome codes. Never log full jobs, client records or API secrets.


## Preparation runtime

The terminal emits `Signal prepared: { ... }` with channel/message IDs, symbol,
prepared job/user counts, acceptance/expiry milliseconds and `published_jobs: 0`.
Transient database/API failures use the existing bounded retry policy; unsupported
symbols and invalid job context are rejected. No credentials or full jobs are logged.

The BingX HTTP client is reused across deliveries, with the configured runtime
operation timeout, a 1 MiB response cap, and no redirects. Only metadata uses Redis;
prices are fetched for each signal. Cache failure falls back to the exchange.
Production defaults to `https://open-api.bingx.com`. `BINGX_PUBLIC_API_BASE_URL`
may override it only with a numeric HTTP loopback origin for isolated tests. Normal
production deployments need no additional setting or API key for these requests.

Tests use local HTTP/Redis fixtures and the original payload shape. Reference for
quote routes and response fields: [BingX swap market API](https://github.com/BingX-API/api-ai-skills/blob/main/skills/swap-market/api-reference.md).

## Slice 2 implementation checklist

- [x] Add a client-job publication envelope/destination and ephemeral queue declaration.
- [x] Publish sequentially per signal; reuse serialized bytes for bounded definite
  rejection retries, checking expiry before every attempt. Stop on uncertain confirms.
- [x] Publish every prepared batch. ACK completed/expired batches; use bounded source retry for definite failures;
  leave uncertain deliveries unacknowledged for transport recovery.
- [x] Test byte reuse, partial failure, expiry, missing routes and automatic
  binary publication against isolated services; update operation instructions.

BingX reference: `../trading-station-rust/src/exchange/bingx/futures.rs` provides
signed requests, sensitive API-key headers, bounded responses and explicit uncertain
order outcomes. Account/order methods belong to slices 3–4; slice 2 adds no exchange calls.


## Publication runtime

Client jobs always publish to `satoshi-channel-updates.client-trade.bingx.futures`.
The Rust worker always consumes this queue. `Signal published` records confirmed counts.

Jobs use the default exchange, non-durable queue, non-persistent JSON messages,
mandatory routing and confirms. Fan-out is sequential per signal, also bounded by
the existing publisher semaphore across deliveries. All payloads are serialized
before sending. Each rejected job retries up to `RABBITMQ_RETRY_MAX_ATTEMPTS` with
`RABBITMQ_RETRY_DELAY_MS`, preserving bytes/identity/expiry and skipping already
confirmed jobs. Expiry is checked before every attempt and after publisher admission.

After definite failure exhausts local retries, intake uses its bounded retry/dead-letter
policy. Expired batches stop and ACK with a static reason and confirmed count.
Unknown confirms or failed sessions leave intake unacknowledged for lifecycle recovery.
A raw source redelivery may repeat earlier jobs and start a fresh acceptance window;
there is no execution deduplication or exactly-once guarantee in this slice.


## Acceptance reply

When `SATOSHI_TG_TOKEN` is set, `telegram/sender.rs` sends `created ✅` as a reply
to the source channel message after preparation, before job publication.
This is signal acceptance, not confirmed trade execution. The sender does not
consume Telegram updates, change webhooks or emit the original long trade announcement.

One bounded HTTP attempt (at most five seconds), no redirects, a 64 KiB response
limit and sanitized errors. Failures are logged and job processing continues;
notification errors alone never retry the source. Source redelivery may repeat
notifications. Replies require a configured token.
The default endpoint is `https://api.telegram.org`; `TELEGRAM_API_BASE_URL` allows
only numeric HTTP loopback origins for tests. No Redis notification channel is used.

## Slice 3 implementation checklist

- [x] Validate job schema, identity, expiry, required signal fields and shared market snapshot
  before dependencies; resolve channel/user/Futures settings and position override.
- [x] Read signed BingX mode, positions, orders, balance and leverage with sanitized
  errors. Combine with fresh managed-trade Mongo evidence for admission.
- [x] Automatically switch eligible One-Way accounts to Hedge with one signed POST.
  Accept success without confirmation GET; failures stop and ACK without switch retry.
- [x] Register independent Telegram and client-job consumers, queue-specific retry
  publishers, configurable prefetch, shared dependencies and bounded lifecycle cleanup.
- [x] Bound the entire admission by operation timeout and remaining job expiry;
  retry temporary read failures, reject invalid/expired jobs, log admitted summaries.
- [x] Prove settings, admission, dual-consumer settlement and recovery with fixtures
  and isolated services. The worker always runs and confirms trade publication before ACK.


## Worker admission runtime

`TELEGRAM_INTAKE_ENABLED=true` is the default; set it false for worker-only mode.
The worker always builds and publishes final trades, logging `Client trade published`
after confirmation. Trading Station owns order execution.
Disable the TypeScript consumer of this queue before starting Rust on a shared broker.
`CONSUMER_PREFETCH` bounds Telegram work; `CLIENT_TRADE_WORKER_PREFETCH` (default 2)
bounds account jobs. Each role has its own Rabbit session, publisher and retry queue;
Mongo, Redis and reusable HTTP clients are shared. Both roles recover and drain together.

Job validation checks version/provider/market, stable identity, credentials, expiry,
positive signal values and the normalized shared snapshot before network calls.
Worker validation preserves TypeScript numeric/string price arrays; Telegram text
parsing and its price-ordering checks are not rerun at this boundary. Settings preserve
explicit nulls, active market overrides, signal position priority, and Futures margin
(default isolated). Invalid numeric/boolean coercions are rejected with static reasons.

Admission loads fresh managed trades and five signed BingX GETs: position mode,
all positions, all open orders, USDT balance, and symbol leverage. It retains the
original managed lifecycle/ownership policy and supports Hedge or legacy One-Way
routes. Globally eligible One-Way accounts switch to Hedge automatically; requested-symbol
positions/orders or valid NEW/OPENED managed blockers reject with `migrationRequired`.
Activity only on other symbols retains One-Way. CLOSING trades do not block migration
but retain valid leverage ownership. Incomplete evidence stops admission.

The signed POST to `/openApi/swap/v1/positionSide/dual` sends `dualSidePosition=true`.
Success requires HTTP success, API code 0 and object data; no confirmation GET is sent.
Balance/leverage checks then continue using the loaded evidence and accepted Hedge mode.
Switch errors and timeouts reject/ACK without retry; a timeout may still have changed
account mode. There is no migration feature flag, readback or diagnostics queue flow.
The operation deadline prevents a switch after expiry and bounds the whole attempt.
Fresh evidence and the POST are not atomic; concurrent account changes remain possible.

Admission retains typed `position_configuration`: Hedge (existing or switched) maps
to `ORDER_LEDGER_V1`; permitted One-Way maps to `ONE_WAY_V1`. The tested
`AdmittedAccount::apply_to_trade_object` helper writes the nested
`trade_object.positionConfiguration.accountingModel` used by the builder.
When leverage differs, the worker sets the signal leverage unless a live position
or pending order exists on the affected symbol/side (`leverageChangeBlocked`).
Stored managed leverage alone does not reject. Hedge updates LONG/SHORT; One-Way
uses BOTH. The signed leverage POST must return the requested symbol and leverage.
Errors/timeouts reject and ACK without readback or automatic mutation retry.
See [leverage adjustment](leverage-admission-design.md) for the decision and tests.

Every admission is bounded by `RUNTIME_OPERATION_TIMEOUT_MS` and remaining
`tradeExpiresAt`. Temporary reads retry on the client queue with unchanged payload;
invalid/expired/rejected jobs ACK. All trade-count checks, execution claims and locks
are absent. Current price/metadata reuse the signal's shared snapshot. Account reads
use the same constrained BingX origin setting as public reads; tests use loopback only.


## Slice 4: trade construction and publication

- Pure `trade_builder.rs` / `targets.rs` preserve original sizing, target allocation,
  minimum/maximum quantity checks, quantity rounding, strategy and auto_Trade fields.
- Static margin uses the requested USDT amount; if free balance is lower, use
  `STATIC_LOW_BALANCE_FALLBACK_RATIO_FUTURES` (default 0.95). Signal size overrides
  channel sizing. Executable entry notional cannot exceed the static notional budget.
- BREAKOUT entries use STOP_LOSS_LIMIT. Small trades reduce profit-target count and
  redistribute fractions as in TypeScript. Preserve configured fraction wire types.
- Preserve stable trade ID, idempotencyKey, routing, client_data and upstream expiry.
  Target clientOrderIds derive from trade identity, role and index.
- Serialize once; definite Nack/unroutable/pre-send timeout retries reuse bytes and
  stop at expiry. Exhaustion uses the bounded client queue retry policy. Uncertain
  publication leaves the source unacknowledged for session recovery; no local replay.
- Invalid sizing/targets and expired work ACK with a sanitized reason. Broker
  confirmation precedes successful client-job ACK. No execution deduplication claim.

Original helper fixtures: `tests/fixtures/bingx-trades.json`, regenerated by
`node tests/fixtures/generate_bingx_trades.cjs ../satoshi-channel-updates-manager`.
Fixtures omit random original target IDs; Rust tests verify unique stable target IDs.
Numeric values compare within floating-point tolerance while wire types match exactly.
Integer market quotes use the tick's decimal width; decimal quotes preserve the
original quote-width formatting. A trade without any profit targets is rejected,
including the historical TypeScript reduction path that could omit sell_targets.
Percentage profit targets remain outside the existing numeric-target parser contract.


## Slice 5: verification and cutover

Repository checks cover original TypeScript fixtures, the full pipeline and worker-only
cutover with both accounting routes and untouched Telegram backlog. Deployment/Compose
explicitly enable the full BingX flow; Kubernetes starts one replica and KEDA tracks
both queues. Worker-only role/trigger changes, exclusive ownership and digest-based
rollback are in [verification and rollout](verification-and-rollout.md).
Live acceptance and cluster changes are not performed by this slice.
