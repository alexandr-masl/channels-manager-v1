# RabbitMQ

Slice 5 provides RabbitMQ transport and the concrete infrastructure lifecycle.
All inter-app messaging uses RabbitMQ. Delivery retry/DLQ decisions arrive in
slice 6; BingX trade admission and construction remain separate work.

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

## Consumers and lifecycle

Consumers use manual acknowledgements and per-consumer prefetch. `InboundDelivery`
exposes original bytes/properties, redelivery status, and explicit `ack`/`requeue`.
Dropping an unsettled delivery faults the session; channel closure returns
unacknowledged messages to the broker. No handler result implicitly acknowledges.

`Infrastructure` accepts an optional `DeliveryHandler` with access to MongoDB,
account leases, metadata cache, and publishing. Active handlers are independently
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
cleanup, handler concurrency, and graceful/forced shutdown.
