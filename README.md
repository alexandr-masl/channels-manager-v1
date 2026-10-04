# Satoshi Channel Updates Manager

Rust application for the incremental migration of `satoshi-channel-updates-manager`.

The app connects MongoDB, Redis, and RabbitMQ through the runtime lifecycle.
The executable validates Telegram envelopes/source timestamps, then logs and
acknowledges messages from `tg_bot_channel_update`. BingX Futures trade processing follows separately.
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
Messages are acknowledged; client selection and trade execution are not implemented yet.
`CLIENT_TRADE_WORKER_QUEUES` describes the later BingX client-job boundary and is
not the queue selected by `Infrastructure::for_telegram_intake`.

Use a local broker/vhost without the TypeScript consumer: consumers sharing the
same queue compete for messages. This logger acknowledges messages after logging.
