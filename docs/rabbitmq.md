# RabbitMQ

Slices 5–6 provide RabbitMQ transport, recovery, and delivery policy.
All inter-app messaging uses RabbitMQ. BingX trade admission and construction
remain separate work.

## Connections and queues

`RabbitMq::connect_and_declare` opens one connection and declares the five queues
from `contracts::rabbitmq`: BingX jobs, trade output, admission events, delayed
retry, and dead-letter diagnostics. All are non-durable, non-exclusive, and not
auto-deleted. Retry TTL returns messages to the original job queue.

Declaration, publisher, and consumer channels are separate. Heartbeats default
to 30 seconds (`RABBITMQ_HEARTBEAT_SECONDS` overrides the URI setting). Broker
events and channel status changes latch failures without requiring traffic. The
lifecycle owns reconnection and declaration replay; driver auto-recovery is disabled.

Startup and cleanup are bounded by runtime deadlines. The transport retains an
independent socket shutdown handle: handshake timeout, cancelled startup, or
manager drop closes the socket even when driver handles remain alive. AMQPS uses
certificate verification with native trust roots. Driver errors are sanitized.

## Publishing

`PreparedPublication::trade` serializes once and checks the supplied trade ID
against the existing idempotency-key hash. It preserves the original `expires_at`,
payload, AMQP message ID, headers, and timestamp. Reusing it preserves identical
bytes across attempts; expired trades are rejected before sending.

Publication uses the default exchange, delivery mode 1, JSON content type,
mandatory routing, and publisher confirms. Pending publications are bounded by
`CONSUMER_PREFETCH`; the deadline is five seconds including permit wait.

| Result | Meaning |
| --- | --- |
| Success | Broker confirmed acceptance without returning the message |
| `Unroutable` | Broker returned the message |
| `Nack` | Broker rejected publication |
| `PublishUncertain` | Send/confirmation failed, timed out, or was cancelled after starting |
| `Unavailable` / permit `Timeout` | This call did not begin sending |

Uncertain publication faults the session and blocks further sends until the
lifecycle rebuilds it. It does not authorize a blind retry or acknowledge the
input job. A confirmation reports broker acceptance, not trade creation.
See [RabbitMQ acknowledgements and confirms](https://www.rabbitmq.com/docs/confirms).

Admission, raw delayed-retry, and dead-letter publication constructors expose
the transport primitives. They do not choose retry attempts or delivery outcomes.

## Delivery outcomes

`WorkerServices::delivery_policy` settles explicit `DeliveryOutcome` values:

| Outcome | Settlement |
| --- | --- |
| Completed, rejected, suppressed | Acknowledge |
| Post-claim terminal | Acknowledge after the handler records the terminal claim |
| Pre-claim retry | Confirm delayed retry, then acknowledge original |
| Retry budget exhausted | Confirm diagnostic DLQ publication, then acknowledge original |
| Retry/DLQ return, nack, timeout or disconnect | Leave original unacknowledged, fault session and recover with backoff |

A timeout before claiming can produce `PreClaimRetry(Timeout)`. After claiming,
timeouts, lease loss and uncertain trade publication are terminal business
outcomes. The handler owns claim recording and classification; the transport
never infers execution phase from an error string. An unsettled delivery dropped
on cancellation returns through broker recovery and must pass Mongo claim checks
again. The non-TTL claim prevents executing claimed work again.

Retry defaults are five delayed attempts at one second each. The configured
maximum overrides incoming metadata. Invalid counters go directly to diagnostics.
Each retry retains original body bytes, expiry, message ID and custom headers;
first/last failure times and attempt headers follow the TypeScript contract.
Error metadata uses fixed codes, avoiding credentials in driver error strings.
DLQ payloads retain the original JSON value (or text for malformed JSON).

A confirmation and acknowledgement are separate operations: disconnect between
them can produce duplicates. Claims and stable IDs remain required. No automatic
trade replay is performed. Non-durable queues and non-persistent messages retain
the existing ephemeral delivery guarantees.

## Consumers and lifecycle

Consumers use manual acknowledgements and per-consumer prefetch. `InboundDelivery`
exposes original bytes/properties, redelivery status, and explicit `ack`/`requeue`.
Dropping an unsettled delivery faults the session; channel closure returns
unacknowledged messages to the broker. No handler result implicitly acknowledges.

`Infrastructure` accepts an optional `DeliveryHandler` with access to MongoDB,
account leases, metadata cache, publishing, and delivery policy. Active handlers are independently
bounded by prefetch. Quiesce stops intake; drain awaits handlers and settlement;
flush waits for active publications. Failed/aborted handlers are joined before
connections close. Handler futures must not detach background work.

The executable supplies no handler, so it starts infrastructure and declares
queues without consuming real jobs. `cargo run -- --check-config` validates
configuration without opening connections. Coordinated queue ownership is still
required when the eventual Rust worker takes over from TypeScript.

## Verification

`cargo test --test rabbitmq -- --include-ignored` starts isolated local brokers,
temporary data directories, and private ports. Install RabbitMQ/Erlang or set
`RABBITMQ_SERVER_BIN`. The lifecycle test additionally needs `mongod` and
`redis-server` (or `MONGOD_BIN` and `REDIS_SERVER_BIN`).

Tests cover queue compatibility, confirmed/returned/rejected publications, expiry,
immutable bytes, retry TTL routing, prefetch, settlement/redelivery, consumer
cancellation, idle connection failure, missing confirmations, stalled handshake
cleanup, handler concurrency, and graceful/forced shutdown. Slice 6 additionally
verifies capped retries, malformed metadata, DLQ publication failures, original
redelivery, and automatic idle disconnect/consumer-cancellation recovery before
any new traffic arrives. Required Redis outage tests verify intake cancellation
and backlog processing only after dependency recovery.
