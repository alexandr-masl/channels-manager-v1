#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Tests create their own services on loopback ports; no .env file is loaded.
for binary in cargo "${MONGOD_BIN:-mongod}" "${REDIS_SERVER_BIN:-redis-server}" "${RABBITMQ_SERVER_BIN:-rabbitmq-server}" epmd; do
  if ! command -v "$binary" >/dev/null 2>&1; then
    printf 'Missing test dependency: %s (see docs/verification-and-rollout.md)\n' "$binary" >&2
    exit 1
  fi
done
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked
# Sequential tests keep the local Erlang/MongoDB footprint bounded in CI.
cargo test --locked -- --include-ignored --test-threads=1
