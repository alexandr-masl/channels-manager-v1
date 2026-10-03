# Slice 6: recovery and delivery handling

Use the existing lifecycle as the sole owner of reconnects and intake gating.
Add a delivery policy to worker services: completed, rejected, suppressed and
post-claim terminal outcomes acknowledge; only explicit pre-claim failures retry.
Timeouts before claim may retry; timeouts after claim are terminal. Handlers remain
responsible for recording terminal claims and classifying their execution phase.

Retry publication preserves original bytes, message identity, and expiry. Copy
retry metadata, cap attempts from trusted configuration, and publish diagnostic
JSON after exhaustion. Acknowledge only after a confirmed, routed publication.
On retry/DLQ publication failure, leave the original unacknowledged and fault the
session; lifecycle backoff prevents an immediate nack/requeue loop. Uncertain
trade publication is never blindly replayed by this policy.

- [x] Add policy tests for acknowledgement, capped retry, metadata, poison data,
  and publication failure, then implement the policy and worker-service wiring.
- [x] Exercise consumer cancellation and idle connection loss through the concrete
  lifecycle; assert restored topology, confirms and consumption without new traffic.
- [x] Run full tests, formatting, Clippy and review; update docs and issue #1.

No health endpoints or BingX business execution are added. The binary continues
waiting for an explicit handler before consuming jobs.

Verified: 67 tests passed with `cargo test --offline -- --include-ignored`.
Formatting, Clippy with warnings denied, and diff checks passed. Review found
an empty-message-ID retry loop; regression reproduced it and verified the fix.
Recovery tests also cover required Redis loss and backlog consumption after recovery.
