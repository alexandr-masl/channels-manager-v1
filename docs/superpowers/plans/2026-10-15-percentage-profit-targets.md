# Percentage profit targets implementation plan

> For agentic workers: use superpowers:executing-plans to implement each slice.

**Goal:** Restore Futures percentage profit targets for
[issue #6](https://github.com/alexandr-masl/channels-manager-v1/issues/6).

**Architecture:** Preserve target intent through parsing and client jobs. Resolve
percentage targets during trade construction using each account's resolved
leverage and selected entry targets. Trading Station receives price targets.

**Tech stack:** Rust, Serde, existing BingX precision helpers and isolated workflow tests.

## Rules

- Each signal uses one profit-target format: all absolute prices or all percentages.
  Reject mixed formats in both Telegram parsing and incoming client jobs.
- Keep absolute targets as decimal strings. Represent percentages using the
  original contract: `{"type":"percent","value":100,"raw":"100%"}`.
  Use a Rust enum for the two representations and shared validation helpers.
- Accept finite positive decimal percentages, including `12.5%`. Preserve order;
  percentage targets must increase for both LONG and SHORT as profit increases.
- Keep targets and stop loss optional. Preserve existing price-only contracts.
- Resolve leverage using existing signal/user/channel precedence. Validate every
  supplied percentage against that leverage before account admission/mutations;
  one invalid target rejects the job even if allocation would later omit it.
- SHORT percentages must be below `leverage × 100`; reject nonpositive or
  nonfinite resolved prices. LONG has no equivalent percentage ceiling.
- After entry construction, use the fraction-weighted entry price. Match the
  original fallback: mean of valid entry prices without usable fractions, then
  current price if no valid entries exist. Preserve existing missing-entry checks.
- Convert with `movement = percentage / (leverage × 100)`:
  LONG `reference × (1 + movement)`; SHORT `reference × (1 − movement)`.
- Apply existing exchange precision and geometry checks to resolved prices.
  Reject wrong-side or collapsed/nonmonotonic targets after rounding.
- Keep existing allocation, sizing, identity, expiry and publication behavior.
  No new configuration, exchange requests or Trading Station schema changes.

Original references: TypeScript commit `342f2fc`,
`src/handlers/trading-signal-reader.ts` and `src/targets/targetsMethods.ts`.
These creation modules were subsequently removed from the command-only app.

## 1. Target model, parsing and job validation

Files: new `src/signals/targets.rs`, `src/signals/model.rs`, `src/signals/mod.rs`, `src/signals/parser.rs`,
`src/signals/validation.rs`, `src/signals/jobs.rs`, `src/trading/job.rs`;
tests: `tests/signals.rs`, `tests/signal_preparation.rs`, `tests/worker_settings.rs`,
`tests/worker_execution.rs`.

- [x] Add failing tests for the issue's XLM signal, decimal percentages, exact
  serialization, unchanged absolute targets and optional exits.
- [x] Add failures for mixed formats, malformed/zero/negative/nonfinite values,
  incorrect ordering and malformed percentage objects in direct client jobs.
- [x] Add the typed target representation and shared format/value validation.
  Parser validation checks intent; defer percentage price geometry to construction.
- [x] Preserve percentage objects in each prepared job. Validate with resolved
  per-client leverage, including missing-text leverage and SHORT boundary cases.
- [x] Run the focused parser, preparation and worker-settings tests.

## 2. Percentage conversion and builder validation

Files: `src/exchanges/bingx/targets.rs`, `src/exchanges/bingx/trade_builder.rs`;
tests: `tests/trade_builder.rs`.

- [x] Add failing builder tests for LONG/SHORT conversion, weighted entry ranges,
  configured leverage, rounding, invalid geometry and the SHORT ceiling.
- [x] Pass resolved leverage into exit construction and convert percentages
  using constructed entries before formatting target prices.
- [x] Verify the XLM SHORT example: entry `0.18892`, leverage `8x`, targets
  `100/200/300/400%` resolve to `0.165305/0.141690/0.118075/0.094460`
  before exchange rounding. Final target count still follows existing allocation.
- [x] Run builder regressions; existing absolute-price golden fixtures must pass.

## 3. Complete workflow and documentation

Files: `tests/signal_workflow_e2e.rs`, `tests/support/signal_workflow.rs`,
`tests/fixtures/signals/`, `docs/signal-parser-rules.md`,
`docs/signal-workflow-e2e-design.md`, `README.md`.

- [x] Add isolated RabbitMQ-to-final-envelope scenarios for XLM SHORT, LONG,
  entry ranges and different users' configured leverage.
- [x] Verify mixed formats and invalid SHORT percentages publish no final trade
  and trigger no account mutations; retain price-only and optional-exit scenarios.
- [x] Document ROI semantics, one-format rule, leverage resolution and validation.
  Parsed logs retain percentages; final published targets contain resolved prices.
- [x] Run `./scripts/test-signal-workflow.sh`, then `./scripts/verify.sh` with
  disposable services. Review this feature's changes before completion.

Implementation only; live publication, image push and deployment are separate.
