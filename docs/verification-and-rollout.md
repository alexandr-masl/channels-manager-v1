# Verification and cutover

## Isolated acceptance checks

```sh
./scripts/verify.sh
```

Requires Rust with rustfmt/Clippy, `mongod`, `redis-server`, `rabbitmq-server` and
`epmd` on PATH. Binary overrides: `MONGOD_BIN`, `REDIS_SERVER_BIN`,
`RABBITMQ_SERVER_BIN`. On Debian/Ubuntu use `/usr/lib/rabbitmq/bin/rabbitmq-server`
to bypass the system service wrapper. Run as an ordinary user.

The script checks formatting, Clippy, build and all tests, including ignored service
tests. Fixtures use temporary databases, private loopback ports and simulated HTTP;
they do not read `.env.local`, use live accounts or change your running services.
A normal `cargo test` omits the service integration tests.

[CI](../.github/workflows/verify.yml) runs the same script on Ubuntu with installed
MongoDB/Redis/RabbitMQ binaries. Local success does not establish hosted CI success.

| Coverage | Evidence |
| --- | --- |
| TypeScript sizing/target/payload parity | 12 original-helper cases in `tests/fixtures/bingx-trades.json`; `tests/trade_builder.rs` |
| Hedge, automatic migration, legacy One-Way, rejection | `tests/bingx_admission.rs`, `tests/bingx_migration.rs` |
| Full intake → job → final trade | `binary_dual_consumers_publish_final_trades` |
| Worker-only ownership, both routes, custom destination, expiry/ID preservation | `worker_only_cutover_preserves_intake_backlog_and_publishes_both_routes` |
| Retry bytes, limits, expiry, uncertain sends | `tests/trade_publication.rs` and RabbitMQ confirm/recovery tests |
| Consumer recovery, source settlement, bounded shutdown | Dual-consumer, lifecycle and signal integration tests |
| Settings precedence, invalid work, credential redaction | Worker settings/execution and config tests |

Regenerate comparison fixtures, when reviewing original-app changes:

```sh
node tests/fixtures/generate_bingx_trades.cjs ../satoshi-channel-updates-manager
cargo test --locked --test trade_builder
```

The generator uses only original pure helpers. Its sibling TypeScript/numeral
packages must be installed. Commit fixture changes only after reviewing the diff.
No TypeScript checkout or Node installation is needed to run the committed fixtures.

## Current boundaries

- No trade-count limits, Mongo execution claims or Redis execution locks/deduplication.
  Existing claim/lease infrastructure tests do not imply the worker uses them.
- Leverage mismatch rejects; automatic leverage changes are not implemented.
- Eligible One-Way accounts switch automatically; a successful POST has no readback.
  Switch errors/timeouts stop and ACK the job; a timed-out write may have succeeded.
- Percentage profit targets, Spot, other exchanges and command workflows remain outside
  the current path. No health endpoints are added in this slice.
- Integer quotes use tick decimal width. Trades without profit targets reject rather
  than reproducing the original empty-target reduction path. Target IDs are stable.
- Broker confirmation means accepted publication, not successful exchange execution.
  Lost confirmation/redelivery can duplicate work; stable IDs alone do not prevent it.

## Choose queue ownership

| Mode | TELEGRAM_INTAKE_ENABLED |
| --- | --- |
| TypeScript intake → Rust BingX worker | false |
| Full Rust BingX pipeline (default) | true |

Client-job publication and the BingX worker are always active.

Worker-only mode leaves TypeScript parsing/fan-out and unrelated queues running.
Full mode transfers `tg_bot_channel_update` as well as
`satoshi-channel-updates.client-trade.bingx.futures` to Rust. Full mode supports only
the implemented BingX Futures flow; do not silently retire unrelated workflows.

## Cutover checklist — operator actions

1. Record source commits, the running image digest, original Deployment/KEDA manifests,
   namespace, broker/vhost, output queue and all three Mongo database names. Use
   `--check-config` for parsing validation; it does not prove dependency connectivity.
2. Complete controlled staging tests against intended account settings. Check LONG/SHORT,
   BREAKOUT, minimum balance rejection, Hedge/One-Way selection and expiry downstream.
   Keep a stable trade ID and Trading Station outcome as evidence. Do not assume dedupe.
3. Choose one ownership mode above. Disable the corresponding TypeScript consumers on
   every workload. Drain their in-flight jobs and confirm zero consumers and zero
   unacknowledged deliveries on each transferred queue before starting Rust.
4. Apply the prepared one-replica Rust deployment. Names/image match the original;
   if retaining TypeScript intake, it must run in another workload. Align KEDA triggers
   with enabled queues. See [build/deploy commands](deployment.md).
5. Confirm one Rust consumer per enabled queue and no TypeScript consumer there.
   Check raw intake remains untouched in worker-only mode. Submit controlled work and
   inspect final `expires_at`, stable `id`, `routing`, `client_data` and nested
   `positionConfiguration.accountingModel` in the isolated/staging destination.
6. Observe `Client trade published`, rejection/retry logs, queue depths and Trading
   Station results. Increase replicas only after checks pass. Record the tested image
   digest, outcome and operator approval; do not infer readiness from rollout status.

RabbitMQ consumer and unacknowledged counts are available through its management UI
or `rabbitmqctl list_queues -p <vhost> name consumers messages_ready messages_unacknowledged`.
Identify consumer connections as well as counts; another deployment can own a queue.

## Rollback

Stop Rust consumers and drain first; confirm their connections/consumers are gone.
Restore the saved TypeScript manifests with the recorded image digest, role selection
and KEDA triggers. Mutable `latest` makes `rollout undo` alone insufficient.
Restore each queue to exactly one implementation. Preserve existing stored state.

Already acknowledged work is not recovered by rollback. Reconcile uncertain published
trades with Trading Station using stable IDs before replaying anything. Do not replay
expired signals. Non-durable queues/non-persistent messages may disappear on restart.

## Release evidence

Automated repository verification is separate from live acceptance. Record:

- Commit and immutable image digest; CI run and local verification outcome.
- Chosen ownership mode and before/after consumer counts.
- Controlled trade IDs, expected accounting route and downstream outcomes.
- Rollback image/manifests and operator sign-off.

Live staging tests, image push, cluster cutover and operator sign-off remain manual.
