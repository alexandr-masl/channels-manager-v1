# Satoshi Channel Updates Manager

Rust application for the incremental migration of `satoshi-channel-updates-manager`.

The app connects MongoDB, Redis, and RabbitMQ through the runtime lifecycle.
The executable validates Telegram envelopes/source timestamps, authorizes channels,
parses signals, loads eligible BingX account context and prepares client jobs,
then logs and acknowledges messages from `tg_bot_channel_update`. BingX Futures trade processing follows separately.
See [AGENTS.md](AGENTS.md) for the migration boundary and source-of-truth documentation.
The proposed workflows and module layout are in [application architecture](docs/architecture.md).

## Development

Requires Rust and Cargo with support for Rust edition 2024.

```sh
cp .env.example .env.local
# Edit .env.local for your environment.
cargo run
# Validate settings without opening connections:
cargo run -- --check-config
```

Startup loads `.env.local` from the current working directory. Exported environment
variables take precedence. The file is optional; malformed or unreadable files
cause a sanitized configuration error. No credentials are printed.

Build, test, and lint:

```sh
cargo build
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

See [configuration and contracts](docs/configuration-and-contracts.md) for defaults,
compatibility boundaries, and dependency requirements.

The [runtime lifecycle](docs/runtime-lifecycle.md) coordinator implements ordered
startup, recovery, SIGINT/SIGTERM cancellation, and bounded shutdown. The concrete
adapters are composed in `infrastructure::Infrastructure` and connected to `main`.

Stage 3 provides [MongoDB pools and repositories](docs/mongodb.md) for all three
databases. Run its isolated integration test with a local `mongod` installed:

```sh
cargo test --test mongodb -- --ignored
```

Slice 4 provides [Redis cross-pod locks and optional API caching](docs/redis.md).
All app-to-app communication uses RabbitMQ. Run isolated Redis tests with a local
`redis-server` installed:

```sh
cargo test --test redis -- --ignored
```

Slice 5 adds [RabbitMQ channels and publishing](docs/rabbitmq.md). Its isolated
tests require `rabbitmq-server`/Erlang; the lifecycle test also uses MongoDB and Redis:

```sh
cargo test --test rabbitmq -- --include-ignored
```

## Complete verification and cutover

Docker, local Compose, and Kubernetes replacement commands are in
[container deployment](docs/deployment.md), using the original Docker Hub and cluster names.

Run `./scripts/verify.sh` with all three service binaries installed. It includes
all integration tests and matches the GitHub Actions verification job.
See [verification coverage and rollout](docs/verification-and-rollout.md) for setup,
acceptance checks, exclusive queue ownership, and rollback.

## Send the ADA Telegram example

Start the app, then send from a second terminal:

```sh
cargo run
# Wait for "Listening for messages on tg_bot_channel_update.".
```

```sh
cargo run --example send_telegram_signal
# Optional channel ID override:
cargo run --example send_telegram_signal -- -1001596367704
```

The sender uses `.env.local`, your ADA signal text, a fresh message ID/date, and
channel ID `-1001596367704`. It publishes to the original bot queue
`tg_bot_channel_update` on a local broker with confirms and a 60-second TTL.
The app validates the envelope/source time and logs `Signal parsed: ... result={...}`
for the base USDT Futures format. Other messages log a skip/rejection reason.
The terminal also shows `Signal prepared:` with job counts and `published_jobs: 0`.
By default, messages are acknowledged after preparation logging.
To publish jobs, set `CLIENT_TRADE_JOB_FANOUT_ENABLED=true` in `.env.local` and restart.
The terminal then shows `Signal published:` with the confirmed `published_jobs` count.
Jobs go to `satoshi-channel-updates.client-trade.bingx.futures`; Telegram messages
are acknowledged after all job confirms. The Rust consumer currently performs admission only; enable publication with an
intended compatible consumer.
An existing TypeScript worker can execute these jobs. Coordinate exclusive ownership
of Telegram intake and client-job consumption before using a shared broker.
The configured databases must contain the channel, connected BingX accounts and active
auto-trading subscriptions. Otherwise the workflow logs its skip reason.
`CLIENT_TRADE_WORKER_QUEUES` describes the later BingX client-job boundary and is
not the queue selected by `Infrastructure::for_telegram_intake`.

Use a local broker/vhost without the TypeScript consumer: consumers sharing the
same queue compete for messages. Keep publication disabled for preparation-only tests.


Signal preparation loads per-user open trades and one shared BingX market snapshot.
It preserves the original client-job payload, with no trade-count limits or
execution claims/locks. See [signal manager design](docs/signal-manager.md).


### Automatic Telegram acceptance reply

Set `SATOSHI_TG_TOKEN` in `.env.local` to the original bot token and restart
`cargo run`. The app sends `created ✅` as a reply to the original channel message
after successful preparation, before job publication. This also runs in
preparation-only mode. Without the token, replies are disabled.

The sender uses only Telegram's [sendMessage API](https://core.telegram.org/bots/api#sendmessage);
it does not poll updates or modify webhooks. The bot needs posting permission in
the channel and the source message must exist. The synthetic RabbitMQ example
uses a generated message ID, so use a real channel signal to test replies.

Logs show `Signal notification sent` or a sanitized failure reason. Requests have
a maximum five-second timeout and no automatic resend. Notification failures do
not block client jobs. Source redelivery may repeat a reply; deduplication remains
separate work. No live Telegram send was performed by the automated tests.

For containers, inject `SATOSHI_TG_TOKEN` through the runtime environment (a
Kubernetes Secret in the cluster); never include it in the image or manifest.


### Client-job admission worker (slice 3)

For an isolated workflow test, set:

```env
TELEGRAM_INTAKE_ENABLED=true
CLIENT_TRADE_JOB_FANOUT_ENABLED=true
CLIENT_TRADE_WORKER_ENABLED=true
CLIENT_TRADE_WORKER_PREFETCH=2
```

Restart `cargo run`. Accepted jobs log `Client job admitted` with `positionConfiguration`, margin mode
and expiry. Rejections and temporary dependency retries have separate logs.
The worker checks settings, symbol metadata, position mode, managed leverage owners,
USDT balance and requested leverage. Eligible flat One-Way accounts automatically
switch to Hedge with one signed POST; success needs no confirmation GET. Switch
errors/timeouts log and ACK the job without automatic retry.

**Admission only:** jobs are acknowledged after checking; no trades are created or
published until slice 4. Keep the worker disabled on a live execution queue. For a
worker-only test, set `TELEGRAM_INTAKE_ENABLED=false`; role prefetch and retry queues
are independent. Hedge switching is active whenever the worker is enabled. No
trade-count limits, leverage changes, execution claims or execution locks are applied. See [implementation plan](docs/signal-manager.md).
