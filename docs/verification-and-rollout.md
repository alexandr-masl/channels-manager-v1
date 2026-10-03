# Verification and rollout

## Run the complete checks

Use a current stable Rust toolchain with rustfmt/Clippy, MongoDB 8.0, Redis 7,
and RabbitMQ 3.12 or newer with its compatible Erlang runtime. Put `mongod`,
`redis-server`, `rabbitmq-server`, and `epmd` on `PATH`. Alternatively set
`MONGOD_BIN`, `REDIS_SERVER_BIN`, and `RABBITMQ_SERVER_BIN` to executable paths.
On Debian/Ubuntu, use `/usr/lib/rabbitmq/bin/rabbitmq-server` to bypass the system
service wrapper and run the broker as the test user.

```sh
./scripts/verify.sh
```

The script checks formatting, Clippy, build, and **all tests**, including ignored
integration tests. Cargo uses the committed lockfile. Tests run sequentially to
limit concurrent Erlang and database processes. A normal `cargo test` skips the
local-service tests and is not the full acceptance check.

Tests start private loopback ports and temporary databases, including a single
node MongoDB replica set for majority claims and uncertain-write injection.
They do not load `.env` or connect to application environments. Fixtures own and
stop their child processes and remove temporary data. Do not supply service URLs;
only binary-path overrides are used. Run under your ordinary user, not root.

## CI

[Verify](../.github/workflows/verify.yml) runs on pushes, pull requests, and manual
workflow dispatches on Ubuntu 24.04. It installs MongoDB 8.0 and the distribution's
Redis/RabbitMQ packages, stops system instances, then runs the same script as
local development. No application credentials or deployment permissions are used.
Rust stable and OS package patch versions can advance; Cargo dependencies stay
locked. Check the workflow logs for installed versions when comparing environments.

Setup follows [MongoDB's Ubuntu installation instructions](https://www.mongodb.com/docs/v8.0/tutorial/install-mongodb-on-ubuntu/)
and [GitHub's Rust workflow guidance](https://docs.github.com/en/actions/tutorials/build-and-test-code/rust).
The hosted workflow must pass after pushing; local macOS execution does not prove
Ubuntu package installation or hosted CI execution.

## Acceptance coverage

| Scenario | Evidence |
| --- | --- |
| Startup outage and backlog | `startup_outage_preserves_backlog_until_required_dependency_recovers`: zero consumers, job stays queued, resumes after Redis returns |
| Broker restart, idle disconnect, consumer cancellation | `lifecycle_recovers_idle_disconnect_and_consumer_cancellation`: real broker restart, connection cut, deleted input queue; restored consumer and confirmed output before/after new traffic |
| Required dependency loss | Same lifecycle test: Redis loss cancels intake; queued work waits for recovery |
| Queue contracts, returns, nacks, expiry | `rabbitmq_stage5_contracts`: topology and externally retrieved messages |
| Lost confirm and stalled handshake | `lost_confirmation_is_uncertain_and_blocks_blind_replay`, `stalled_handshake_is_bounded_and_releases_socket` |
| Poison jobs and bounded retries | `bounded_delivery_retries_preserve_payload_then_dead_letter`, `malformed_retry_metadata_cannot_reset_budget`: unchanged bytes/IDs/expiry, capped attempts, diagnostic JSON |
| Failed retry/DLQ publication | `failed_retry_or_dead_letter_keeps_original_for_recovery`: original remains available for redelivery |
| Mongo claims and uncertain writes | `mongodb_stage3_contracts`: one winner across 12 claimants; uncertain insert becomes duplicate; owner-checked terminal record; verified non-TTL index |
| Redis lease loss and release races | `tests/redis.rs`: renewal, lost ownership, cancellation, atomic release, reconnect, isolated cache failure/fallback |
| Shutdown and bounded concurrency | `concrete_lifecycle_bounds_workers_and_joins_cleanup`, lifecycle and signal tests: bounded handlers, drain/abort/join, deadlines, SIGINT/SIGTERM |

Lifecycle phase assertions cover starting, retrying, running and recovery, plus
actual intake gating. HTTP health/readiness endpoints belong to a separate issue.
Tests use synthetic trade publication handlers; they do not validate BingX trade
admission/construction or execute exchange operations.

## Exclusive BingX Futures queue cutover

The current binary logs and acknowledges raw Telegram messages only. Replace
the logging handler and select the BingX worker queue before production cutover.
Complete and verify the business handler before this runbook is used for production. The queue is
`satoshi-channel-updates.client-trade.bingx.futures`; TypeScript retains parsing
and fan-out, and other provider queues remain owned by their existing workers.

1. Record the application revisions, broker/vhost, queue arguments, and all three
   Mongo database names. Validate Rust configuration with `--check-config`, then
   start infrastructure without a consumer and verify the claim index. Use the
   same claims, account lease keys and downstream queue as TypeScript.
2. Verify business-handler parity: stable work/trade identities, original
   `expires_at`, account admission, terminal claims, and downstream duplicate
   protection. Require a green complete verification run and staging evidence.
3. Disable the TypeScript **BingX Futures consumer registration** on every pod.
   Wait for its in-flight handlers to finish and its unacknowledged count to reach
   zero. Confirm RabbitMQ reports zero consumers for this queue. Do not disable
   all TypeScript parsing/fan-out or unrelated provider workers.
4. Enable the Rust handler on one pod. Confirm exactly one Rust consumer, expected
   prefetch, and no TypeScript consumers on the queue. Validate confirmed output,
   unchanged expiry/IDs, duplicate suppression and admission/rejection outcomes
   using controlled staging/canary work before increasing replicas.
5. Observe retry/DLQ counts, required-dependency failures and downstream results.
   Stop the rollout if duplicate execution, incompatible payloads, expired output,
   or lost ownership is observed. Retain revision and verification evidence.

Deployment-specific consumer toggles must be supplied by the worker migration;
there is no `--consume` flag in this binary. Queue ownership is operationally
exclusive across implementations; multiple Rust pods may share it after cutover.

## Rollback

Stop Rust intake first and wait for bounded drain/cleanup. Confirm Rust consumers
are gone and unacknowledged deliveries have settled or returned to the queue.
Then restore the previous TypeScript BingX Futures consumer registration and
verify its consumer count and output. Never run both implementations on this
queue during the switch. Preserve Mongo claims and Redis lease keys; deleting
claims to force replay can repeat exchange mutations.

A broker restart may discard these non-durable queues/non-persistent messages.
Do not republish expired signals to recreate backlog. A lost confirm may mean a
publication already reached Trading Station; reconcile using stable identity and
claims rather than manually replaying uncertain trade creation.
