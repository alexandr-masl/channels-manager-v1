# User failure notifications implementation plan

> For agentic workers: use superpowers:subagent-driven-development or superpowers:executing-plans. Follow tasks in order and verify each change before continuing.

**Goal:** Save actionable trade-creation failures in the user's existing main-menu
notification history for [issue #5](https://github.com/alexandr-masl/channels-manager-v1/issues/5).

**Architecture:** Channels Manager writes directly to
`user_configs.lastNotifications` using its existing Bot MongoDB connection.
An explicit allowlist selects user messages. The Telegram bot keeps reading the
existing history; no bot code change, new queue or private message is required.

**Tech stack:** Rust, Tokio, MongoDB/BSON; existing RabbitMQ integration fixtures.

Scope reference: [failure classification](../../user-failure-notifications.md).

## 1. Map business failures

Files: new `src/trading/notifications.rs`, `src/trading/mod.rs`,
new `tests/user_failure_notifications.rs`.

- [x] Write failing table tests for every allowlisted reason and representative
  internal/unknown reasons; run them before implementing the mapper.
- [x] Add a pure mapping returning `Option<UserFailure>` for:
  `insufficientBalance`, `positionExceedsBalance`, `insufficientTargetBalance`,
  `leverageLimitExceeded`, `leverageChangeBlocked`, `migrationRequired`.
- [x] Format fixed explanations with symbol/account name, escaping dynamic text
  for the bot's HTML rendering. Never include provider bodies, credentials,
  stack traces or raw internal codes.
- [x] Keep mixed size-limit errors, settings errors, expiry, infrastructure
  failures and unknown codes internal. Verify mapping/formatting tests pass.

Example: `⚠️ #ADA-USDT Auto-Trade was not created | BingX-1 | Insufficient available Futures balance.`

## 2. Preserve trusted reporting context

Files: `src/trading/execution.rs`, `src/trading/job_handler.rs`,
`tests/worker_execution.rs`.

- [x] Add failing tests proving allowlisted admission failures retain user,
  account and signal identity after validation; malformed jobs never produce
  a user notice.
- [x] Carry a credential-free notification context alongside eligible rejection
  outcomes. Build it only after existing job identity/client validation succeeds.
- [x] Use the validated prepared account for builder failures. Avoid parsing the
  original delivery again solely to choose a notification recipient.
- [x] Verify internal/retry outcomes and successful publication have no notice.

Context: user chat ID, account ID/name, normalized symbol, signal identity and
job idempotency key. Preserve existing admission, expiry and trade retry behavior.

## 3. Append history with bounded duplicate suppression

Files: `src/mongo/repositories.rs`, `src/mongo/mod.rs`,
`src/trading/notifications.rs`, `tests/mongodb.rs`.

- [x] Add real Mongo regressions for append, concurrent duplicate append, unknown
  user, newest-seven retention and untouched account settings.
- [x] Add a failure-specific repository method; preserve `add_user_notification`
  for its current callers.
- [x] Derive a stable notification ID from job identity plus failure reason.
  Store `{eventId, info, time}` in `lastNotifications`; existing bot readers use
  `info` and `time` and ignore the extra field.
- [x] Atomically filter by user and absence of the event ID, then `$push` with
  `$slice: -7`. Do not upsert a missing user configuration.
- [x] Distinguish appended, already present and user missing; treat database
  errors as internal. Verify all repository regressions pass.

Suppression covers entries retained in the seven-item window. Permanent receipt
storage, execution claims and execution locks are outside this issue.

## 4. Wire reporting into terminal rejection handling

Files: `src/trading/job_handler.rs`, `src/trading/notifications.rs`,
`tests/user_failure_notifications.rs`, `tests/support/signal_workflow.rs`.

- [x] Test that an eligible terminal rejection saves history before ACK and never
  publishes a final trade; internal rejections only log and ACK.
- [x] Try the history write once, with at most two retries for database errors,
  using 200 ms between attempts and one total `RUNTIME_OPERATION_TIMEOUT_MS`
  budget. Keep one notification ID and timestamp for the entire reporting attempt.
- [x] A successful append or duplicate settles the rejected job. A missing user
  logs once and settles it without creating a profile.
- [x] Exhausted/uncertain reporting logs `user_failure_notification_save_failed`
  and settles the rejected job. Do not route this failure through account-job
  retries or restart account mutations to report it.
- [x] Existing ACK-loss redelivery remains possible; the retained history ID
  suppresses the duplicate entry. Do not claim exactly-once execution or delivery.
- [x] Keep all original rejection diagnostics in terminal logs.

This base version can lose a history entry during an extended Mongo outage.
Durable reporting recovery is separate work; it must retry reporting only.

## 5. Verify the complete workflow and document it

Files: `tests/signal_workflow_e2e.rs`, `tests/support/signal_workflow.rs`,
`tests/support/services.rs`, `README.md`, `docs/user-failure-notifications.md`.

- [x] Seed actual Bot user configurations and add scenarios for insufficient
  balance, blocked leverage and leverage above limit. Inspect the saved history,
  assert its account/symbol/reason and verify no final trade was published.
- [x] Verify successful trades, regular messages, expired jobs, malformed payloads
  and internal API failures add no failure-history entries.
- [x] Prove history-write retries call only the notification repository and use
  the same notice; account/exchange calls do not repeat during reporting retries.
- [x] Document allowlist, newest-seven retention and reporting-failure behavior.
- [x] Run `./scripts/test-signal-workflow.sh`, then `./scripts/verify.sh` with
  disposable services. Review only this feature's changes before completion.

## Release

Build/push and roll out Channels Manager after verification. The existing Telegram
bot shows saved entries on its next main-menu render. No live trade or notification
is sent as part of implementation verification.
