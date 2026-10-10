# Trade construction/publication implementation plan

Goal: complete the approved BingX Futures worker → Trading Station flow.

Architecture: pure sizing/target builder consumes `PreparedAccount`; the worker
serializes one `NewTradeMessage` and reuses it for confirmed publication retries.
Tech stack: Rust, existing JSON contracts, Tokio and RabbitMQ publisher.

- [x] Port `CreateTradeObject`, sizing/reconciliation and target rules into
  `exchanges/bingx/{trade_builder,targets}.rs`. Preserve wire types, static fallback
  (default 0.95), precision/minimum checks, target reduction and BREAKOUT.
- [x] Add fixture tests from original TypeScript helpers plus LONG/SHORT, range,
  fraction, static/dynamic, minimum/maximum and invalid-price cases. Preserve
  nested `positionConfiguration.accountingModel`, stable trade ID and expiry.
- [x] Add `trading/publication.rs`: serialize once, retry definite publication
  failures within expiry, stop on uncertainty, confirm before source ACK.
- [x] Wire builder/publication through `trading/job_handler.rs` and worker services.
  Add validated `STATIC_LOW_BALANCE_FALLBACK_RATIO_FUTURES`; no new enablement flag.
- [x] Update binary integration test to consume the final trade payload from an
  isolated RabbitMQ instance and verify source settlement and redacted logs.
- [x] Update runtime documentation; run focused tests, full isolated suite,
  `cargo fmt --check`, and `cargo clippy --offline --locked --all-targets -- -D warnings`.

Scope: no trade-count limits, execution claims/locks, leverage changes, live orders,
notifications beyond the existing acceptance reply, or deployment. Existing worker
flag now enables trade publication. Advanced delivery deduplication remains separate.

Verified: full isolated test suite (including ignored integration tests), formatting,
Clippy with warnings denied, and spec/runtime reviews. No live orders or deployment.
