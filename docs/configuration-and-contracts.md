# Configuration and contracts

Stage 1 defines configuration and transport/storage contracts. It makes no network
connections and does not start consumers or trading logic.

## Configuration

`AppConfig::from_env()` reads the process environment. `from_lookup()` accepts an
isolated lookup for tests without mutating global environment variables.

- Required: `RABBIT_MQ`, `MONGO_PATH`, `TRADE_STATION_MONGO_PATH`, `REDIS`.
- Existing environment names and defaults are listed in [`.env.example`](../.env.example).
- `CONSUMER_PREFETCH` bounds unacknowledged deliveries (default 2, range 1–65535).
  The future worker must enforce that bound when scheduling handlers.
- Ports use 1–65535; millisecond settings use 1–2147483647. Explicit malformed
  values fail validation instead of silently falling back.
- Reconnect maximum must be at least the base; jitter must be finite in [0, 1].
- Blank `RABBITMQ_QUEUE` retains the TypeScript default. Reserved AMQP names,
  oversized names, and collisions with this worker's other queues are rejected.
- `CLIENT_TRADE_WORKER_QUEUES` defaults to the single BingX Futures queue. Explicit
  `all`, other queues, or lists are rejected to preserve the migration boundary.
- Connection strings redact `Debug` output; validation errors include only the
  setting name and reason. `REDIS` is a hostname/IP, not a URI.
- Shared Redis prefixes reject surrounding whitespace instead of silently
  changing the lock/cache keys used by TypeScript.
- Mongo validation covers URI scheme/host structure, including seed lists and SRV.
  Full Mongo option, authentication, and topology validation belongs to its driver.

Fixed source defaults remain contracts: Mongo pool size 10, server selection 5s;
trade publication deadline 5s; claim write/outer/terminal deadlines 5s/6s/2s;
account lease TTL/renewal/command/acquisition 30s/10s/1s/5s;
protected execution/cleanup deadlines 90s/2s. `PROCESSING_LOCK_TTL_MS` belongs to
the legacy generic lock and does not override the BingX account lease.

## Dependency requirements

| Component | Failure policy |
| --- | --- |
| Bot MongoDB, Trading Station MongoDB, claim index | Required; block work |
| RabbitMQ connection, publisher, BingX consumer | Required; block work |
| Redis account locks | Required; block work |
| Redis exchange metadata cache | Optional; fall back to local cache/API |
| Redis notification publisher | Required when used; propagate failure |

Accepted-signal and command Pub/Sub workflows remain in TypeScript. The Rust worker
writes trade-result notifications to bot MongoDB. Notification publication failures
propagate when that workflow is invoked. Account-validator MongoDB remains upstream.

## Compatibility

- `contracts::rabbitmq` defines the five worker queues and retry return routing.
  All are non-durable, non-exclusive, and not auto-deleted. Trade publication uses
  the default exchange, delivery mode 1, JSON, mandatory routing, and confirms.
- `contracts::messages` preserves camelCase job fields (including `channelID`),
  version 1, BingX/futures discriminants, optional source time/expiry/user config,
  and nested JSON. A missing job expiry remains missing; the migration must choose
  its explicit handling before executing jobs. The outgoing envelope always requires
  `expires_at`, `trade_object`, and `client_data` and never invents a new expiry.
- Nested business validation, input-hash canonicalization, account admission,
  source-time validation, and execution retry decisions come with worker logic.
  Deserializing an envelope alone does not authorize execution.
- Stable trade IDs use the TypeScript SHA-256 prefix; partition/idempotency keys
  retain their existing format. Preserve the upstream 60-second expiry on retries.
- `contracts::storage` matches JavaScript `encodeURIComponent` for shared Redis
  keys and the Mongoose collection names. Claims require a unique non-TTL
  `workId` index, majority/journaled writes, and majority duplicate reads.
- Payload types omit `Debug` to discourage accidentally logging client credentials.

## Sources and tests

Source: sibling `satoshi-channel-updates-manager`, especially
`docs/new-trade-publication.md`, `src/trade-processing/client-trade-job.ts`,
`idempotency.ts`, `bingx-account-lease.ts`, `src/rabbit-mq/rabbitmq-contracts.ts`,
and `src/data-base/telegram-bot-db/bingx-execution-claim-store.ts`.

Tests use synthetic TypeScript-shaped JSON, a Node-generated identity digest, and
Mongoose-resolved collection names. No production credentials or services are used.
