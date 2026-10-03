# Satoshi Channel Updates Manager

Rust application for the incremental migration of `satoshi-channel-updates-manager`.

The app connects MongoDB, Redis, and RabbitMQ through the runtime lifecycle.
The executable runs infrastructure only; job consumption requires an explicit
delivery handler. BingX Futures trade processing follows separately.
See [AGENTS.md](AGENTS.md) for the migration boundary and source-of-truth documentation.

## Development

Requires Rust and Cargo with support for Rust edition 2024.

```sh
cp .env.example .env.local
# Edit .env.local for your environment, then export it in your shell:
set -a
. ./.env.local
set +a
cargo run
# Validate settings without opening connections:
cargo run -- --check-config
```

Environment files are not loaded automatically. Invalid settings cause a nonzero
exit with the setting name, without printing credentials.

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
