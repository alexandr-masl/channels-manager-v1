# Runtime Lifecycle Implementation Plan

**Goal:** Implement issue #1 stage 2: one coordinator for ordered startup,
recoverable failures, cancellation, and bounded shutdown.

**Architecture:** Tokio coordinator owns a lifecycle adapter. Adapters implement
idempotent startup and shutdown operations, latch required dependency failures,
and synchronously gate intake when interrupted. Concrete adapters arrive in
stages 3–5. The current binary continues to validate configuration until then.

**Tech stack:** Rust, Tokio, tokio-util cancellation tokens, rand.

## Tasks

- [x] Add runtime deadline/backoff settings and validation tests; preserve existing
  startup retry and shutdown drain environment names.
- [x] Add lifecycle contract and tests for startup order, bounded retry/jitter,
  cancellation during operations/backoff, permanent errors, required dependency
  recovery, and partial-startup cleanup.
- [x] Implement the coordinator, adapter interface, and bounded shutdown sequence.
  Stop intake before draining, flush publications before closing dependencies,
  continue cleanup after individual failures, and stop recovery if cleanup fails.
- [x] Add SIGINT/SIGTERM integration without detached signal tasks; test the
  process signal path using a test-only adapter executable.
- [x] Document adapter cancellation/ownership obligations and production wiring.
- [x] Review and verify tests, formatting, Clippy, and diff checks.

## Boundaries

- Adapters must own their tasks/resources and abort leftovers on drop.
- Operational retry limits apply to delay/attempt duration; recoverable dependency
  outages retry until cancellation. Permanent contract errors terminate cleanly.
- Startup/recovery orchestration is implemented here; delivery retry semantics
  and actual broker recovery are implemented with adapters in later stages.
