# Configuration and Contracts Implementation Plan

**Goal:** Implement issue #1 stage 1 without opening infrastructure connections.

**Architecture:** Pure configuration parsing with a process-environment adapter;
typed, serializable transport envelopes; centralized broker/storage contracts.
Keep existing TypeScript names, defaults, and opaque nested JSON fields.

**Tech stack:** Rust 2024, serde/serde_json, url, sha2.

## Tasks

- [x] Add configuration tests: required settings, defaults/overrides, invalid
  ports/numbers/URIs, redacted errors, queue ownership, reconnect limits.
- [x] Implement `src/config.rs` and `src/config/read.rs`: injectable environment
  lookup, typed durations and positive limits, redacted connection strings.
- [x] Add fixture-based contract tests for JSON field names, optional job fields,
  unsupported jobs, identity hashes, exact expiry, queue arguments, Redis key
  encoding, Mongo collection names, and dependency classification.
- [x] Implement `src/contracts/{mod,rabbitmq,messages,storage,dependencies}.rs`.
  Use existing TypeScript contracts as test expectations; keep lease and claim
  deadlines fixed where the source does not expose environment overrides.
- [x] Wire `src/main.rs` to validate configuration and exit with a redacted error;
  add `.env.example`, configuration documentation, and CLI smoke tests.
- [x] Run `cargo test --offline`, `cargo fmt --check`, and
  `cargo clippy --offline --all-targets -- -D warnings`; review the final diff.

## Scope decisions

- Only the BingX Futures input queue is accepted. Explicit `all` is rejected.
- No account-validator connection, external calls, or consumers are started in stage 1.
- Malformed explicit settings fail instead of silently using defaults.
- Environment values are supplied by the caller; `.env` files are not auto-loaded.
- Mongo connection syntax receives structural validation here; the Mongo driver
  will validate topology/authentication options when its adapter is added.
