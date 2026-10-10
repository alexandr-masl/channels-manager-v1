# Signal manager implementation plan

2026-10-10 · Base version · BingX Futures only.

Slices 1–2 implemented: live intake prepares jobs and optionally confirms publication
before acknowledgement. Slices 3–5 remain planned.

Source of truth: TypeScript `src/handlers/signal-manager.ts`,
`src/market-data/signal-market-data.ts`, and the BingX trade creation flow.

## Scope

Include signal preparation, per-account jobs, confirmed RabbitMQ publication,
and a separate account-execution consumer in this Rust application.

Exclude the global open-trade limit, Mongo execution claims, Redis execution
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
limits and explicit queue registration; either role can be disabled for cutover.

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
   channel/user settings already loaded by intake. No overall capacity rejection.
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
   business rules; do not silently change account mode. Trace any leverage mutation
   explicitly when implementing admission.
4. Build quantities and entry/profit/stop targets using original precision, minimum
   size and sizing rules. BREAKOUT selects STOP_LOSS_LIMIT entry targets.
5. Produce the original trade_object/client_data envelope, stable trade ID and
   unchanged expires_at. Confirm publication to Trading Station before job ack.
6. Log a sanitized outcome. No execution claim or deduplication record is written.

Execution outcomes: PreparedTrade, Rejected, Expired, RetryableDependencyFailure,
or UncertainSideEffect. Handlers map these to settlement. Retry read-only dependency
failures through the existing bounded policy; do not blindly rerun an exchange
mutation after an uncertain response. Park uncertain outcomes for diagnostics.
Publication retries reuse the prepared bytes while they remain available; after
process loss, this version does not guarantee identical reconstructed payloads or
exactly-once execution. Define and test the diagnostics mapping before enabling
exchange mutations.

## Implementation slices

1. **Signal preparation:** OPENED queries, market-data adapter, job builder,
   fixture tests and sanitized job-summary logging. Explicitly keep this stage
   in preparation-only mode; it acknowledges after logging without publishing.
2. **Job publication:** client-job destination, confirms, bounded fan-out/retries,
   expiry checks and Telegram settlement. Enable only with an intended consumer.
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
- No credentials in logs/errors; no job publication in preparation-only mode.

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
- [x] Gate publication with `CLIENT_TRADE_JOB_FANOUT_ENABLED=false` by default.
  ACK completed/expired batches; use bounded source retry for definite failures;
  leave uncertain deliveries unacknowledged for transport recovery.
- [x] Test byte reuse, partial failure, expiry, missing routes and enabled/disabled
  binary behavior against isolated services; update operation instructions.

BingX reference: `../trading-station-rust/src/exchange/bingx/futures.rs` provides
signed requests, sensitive API-key headers, bounded responses and explicit uncertain
order outcomes. Account/order methods belong to slices 3–4; slice 2 adds no exchange calls.


## Publication runtime

`CLIENT_TRADE_JOB_FANOUT_ENABLED` defaults to `false`. Enable it only with an
intended compatible consumer on `satoshi-channel-updates.client-trade.bingx.futures`.
This slice does not start a Rust client-job consumer. Preparation-only mode still
logs and ACKs; enabled mode logs `Signal published` with confirmed counts.

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
to the source channel message after preparation, before optional job publication.
This is signal acceptance, not confirmed trade execution. The sender does not
consume Telegram updates, change webhooks or emit the original long trade announcement.

One bounded HTTP attempt (at most five seconds), no redirects, a 64 KiB response
limit and sanitized errors. Failures are logged and job processing continues;
notification errors alone never retry the source. Source redelivery may repeat
notifications. Preparation-only mode also sends replies when a token is configured.
The default endpoint is `https://api.telegram.org`; `TELEGRAM_API_BASE_URL` allows
only numeric HTTP loopback origins for tests. No Redis notification channel is used.
