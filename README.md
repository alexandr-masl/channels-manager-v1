# Satoshi Channel Updates Manager

Rust application for the incremental migration of `satoshi-channel-updates-manager`.

Currently, the app prints its name and exits. The first integration will consume
BingX Futures client-trade jobs and publish trade creation messages to Trading Station.
See [AGENTS.md](AGENTS.md) for the migration boundary and source-of-truth documentation.

## Development

Requires Rust and Cargo with support for Rust edition 2024.

```sh
cargo run
```

Build and check formatting:

```sh
cargo build
cargo fmt --check
```
