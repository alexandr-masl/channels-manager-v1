# Slice 7: verification and rollout

Reuse isolated process fixtures and the approved infrastructure scope. Test
lifecycle phases and external queue/database results; health HTTP endpoints stay
in their separate issue. Native services let tests inject disconnects, restarts,
and MongoDB write-concern failures without shared production infrastructure.

- [x] Add startup-outage/backlog and real RabbitMQ restart coverage to the concrete
  lifecycle integration tests; retain existing claims, leases and delivery tests.
- [x] Add `scripts/verify.sh` and GitHub Actions for locked builds, formatting,
  Clippy, and every test including ignored local-service integrations.
- [x] Document installation, coverage/evidence, exclusive queue cutover and rollback.
- [x] Run the complete checks, review changes, update issue #1 completion status.

The executable still has no business handler. This slice prepares verification
and rollout instructions; production worker cutover follows business migration.

Verification: `./scripts/verify.sh` passed locally: 68 tests, locked build,
formatting and Clippy. Workflow YAML and shell syntax parsed; diff check passed.
Review found no actionable issues. Hosted Ubuntu installation and CI execution
remain pending the first pushed workflow run; Docker was not running locally.
