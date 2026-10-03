# Satoshi Channel Updates Manager

Rust application for the incremental migration of `satoshi-channel-updates-manager`.

Currently, the app validates configuration and exits without opening connections.
The first integration will consume
BingX Futures client-trade jobs and publish trade creation messages to Trading Station.
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
startup, recovery, SIGINT/SIGTERM cancellation, and bounded shutdown. Concrete
infrastructure adapters and their connection to `main` follow in stages 3–5.

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
