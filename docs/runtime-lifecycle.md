# Runtime lifecycle

`runtime::Lifecycle` owns one `LifecycleAdapter` and runs once. `Infrastructure`
composes MongoDB, Redis, and RabbitMQ. `main` starts this infrastructure and waits
for shutdown signals. It registers no consumers until a delivery handler exists;
`--check-config` performs offline validation and exits.

## Startup and recovery

Startup order is fixed:

1. Connect bot, Trading Station, and account-validator MongoDB clients.
2. Initialize and verify execution-claim indexes.
3. Connect required Redis clients.
4. Connect RabbitMQ and assert declarations.
5. Initialize publishers.
6. Start consumers.

Each step has an operation deadline. `Failure::retryable` retries that step with
capped exponential backoff and jitter until cancellation. `Failure::restart`
signals that an earlier dependency was lost and the entire pipeline must restart.
`Failure::permanent` initiates cleanup and returns an error.

While running, the adapter reports required dependency failures. The coordinator
immediately gates intake, cleans up previous resources, waits with backoff, and
repeats startup. Failed cleanup terminates the run to avoid overlapping consumers.
No new delivery or publish is needed to trigger recovery. Recovery attempts reset
after a running period of at least `STARTUP_RETRY_MAX_DELAY_MS`.

Optional cache failures remain local to the cache adapter. Internal `Phase`
subscriptions report the coordinator's current phase.

## Shutdown

`run_until_signal` installs SIGINT/SIGTERM handlers before startup. Tests and other
hosts can instead call `Lifecycle::run` with a `CancellationToken`. Cancellation
interrupts a startup operation, retry sleep, recovery wait, or idle service.

Shutdown gates intake, then stops consumers, drains handlers, flushes publisher
confirms, stops background tasks, and closes RabbitMQ → Redis → MongoDB. Failed
drain triggers `abort_in_flight` before publication flush. Individual cleanup
failures are collected while subsequent cleanup continues. A total deadline
bounds the sequence; remaining resources are released when the adapter drops.
Repeated cancellation requests do not run cleanup again.

| Setting | Default | Purpose |
| --- | --- | --- |
| `STARTUP_RETRY_DELAY_MS` | 5000 | Initial startup/recovery retry delay |
| `STARTUP_RETRY_MAX_DELAY_MS` | 30000 | Retry delay cap |
| `STARTUP_RETRY_JITTER_RATIO` | 0.2 | Random variation, range 0–1 |
| `RUNTIME_OPERATION_TIMEOUT_MS` | 10000 | Deadline per startup/cleanup operation |
| `SHUTDOWN_DRAIN_TIMEOUT_MS` | 10000 | Maximum handler drain time |
| `SHUTDOWN_TIMEOUT_MS` | 30000 | Total cleanup deadline |

The retry cap must be at least the initial delay. Total shutdown time must exceed
the drain budget. Jitter cannot produce a zero-delay retry loop.

## Adapter obligations

- Initialization and cleanup must be idempotent, nonblocking, and cancellation-safe.
  Dropping a timed-out operation must not leave detached connection attempts.
- Store partial connections/task handles in the adapter as soon as they exist.
  Cleanup must handle incomplete startup and resources that were never opened.
- `quiesce` synchronously closes the local intake gate and suppresses new reconnect
  work. Keep existing lease renewals running until handlers have drained.
- `abort_in_flight` cancels remaining handlers and prevents further application-side
  effects. Network operations already accepted remotely retain their ambiguity;
  claim and delivery contracts govern their eventual handling.
- `wait_for_failure` must latch required failures during startup and report them
  without new traffic. Before starting consumers, recheck current dependencies
  and pending failures. Return `Failure::restart` if an earlier dependency was lost;
  reopen the intake gate only after successful initialization.
- `Drop` must abort owned tasks and release handles left after a cleanup deadline.
- Convert driver errors to static, sanitized failure codes. Credentials and raw
  connection errors must not cross into lifecycle diagnostics.

The bootstrap validates `AppConfig`, constructs `Infrastructure`, creates `Lifecycle`
with `config.runtime`, and awaits `run_until_signal` inside Tokio. Supplying an
explicit `DeliveryHandler` enables bounded dispatch with manual settlement. Handler
errors and forced abort join all remaining handlers before connection teardown.

The [MongoDB module](mongodb.md) now provides `connect`, `initialize_indexes`,
and `close` for the corresponding lifecycle stages. The [Redis module](redis.md) provides required connection
startup, a latched failure signal, acquisition gating, and shutdown. Quiescing
keeps lease renewal alive until worker drain completes. [RabbitMQ](rabbitmq.md)
provides declaration, confirm publishing, consumer cancellation, flush, and close.
Broker/channel errors and Redis failures are latched; periodic Mongo connection
checks run concurrently with these signals. The lifecycle rebuilds all adapters
after a required failure. Message retry/DLQ policy comes in slice 6.

## Verification

`tests/lifecycle.rs` uses virtual time to test ordering, retries, recovery,
cancellation, timeouts, and cleanup errors. `tests/lifecycle_signals.rs` starts
isolated subprocesses to test real SIGTERM during operation and SIGINT during
blocked startup. These tests use no external infrastructure. `tests/rabbitmq.rs`
also exercises the concrete adapter with isolated MongoDB, Redis, and RabbitMQ,
including backlog safety, bounded work, handler failure, and forced drain cleanup.
