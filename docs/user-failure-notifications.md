# User failure notifications

Save meaningful business rejections in `user_configs.lastNotifications` through
the existing `NotificationRepository`. The bot reads the same history on menu
render. Keep internal diagnostics in logs; use no new RabbitMQ notification queue
or private Telegram messages.

## Initial allowlist

| Current reason | User explanation |
| --- | --- |
| `insufficientBalance` | Insufficient available Futures balance to create the trade. |
| `positionExceedsBalance` | The configured position size exceeds available balance. |
| `insufficientTargetBalance` | The position is too small for the configured target allocation. |
| `leverageLimitExceeded` | Requested leverage exceeds the symbol's supported limit. |
| `leverageChangeBlocked` | Existing positions or orders prevent using the requested leverage. |
| `migrationRequired` | Existing account activity prevents using the required position mode for this symbol. |

Include symbol and account name. Use fixed human explanations; do not expose
internal code names, credentials, HTTP responses or stack traces. Notification
failure is itself internal.

## Reasons requiring more precise classification

- `orderOutsideExchangeLimits`: combines minimum quantity, maximum quantity and
  notional limits, including the builder's own size cap. Split into meaningful
  size reasons before showing it; do not label every instance insufficient balance.
- `InvalidPosition`, `InvalidMarginMode`, `InvalidLeverage`, `InvalidTargets`,
  `InvalidStrategy`, `InvalidXPro`: show only when a specific value is attributable
  to user/channel configuration. Missing or inconsistent application data remains
  internal. Per-client leverage fallback can fail before a worker job exists.
- `UnsupportedSymbol`: exchange metadata can establish unsupported/inactive
  symbols during shared preparation. Reporting per account needs trusted user
  context and suppression of repeated entries for the same signal/account.
- Exchange authentication/permission problems could be actionable, but the current
  `exchangeReadRejected` code also includes other provider failures. Add typed
  provider classification before exposing an account-credentials explanation.

Exclude these mixed reasons from the initial allowlist until their meaning and
attribution are established. Do not infer a business reason from an error string.

## Internal only

- Invalid payload, identity, client contract, job symbol, expiry contract or snapshot.
- `MissingSettings`, `InvalidUserConfig`, invalid/incomplete managed or exchange
  evidence and invalid admission targets.
- Mongo/Redis/RabbitMQ failures, publication rejection/uncertainty, reconnects,
  exhausted infrastructure retries and temporary API/rate-limit failures.
- `modeSwitchFailed`, `leverageChangeFailed`, mutation timeouts, admission timeouts,
  deadline exhaustion and repeated-mutation guards: these currently combine
  transport, malformed response and uncertain effects, not one user action.
- Expired work: expiry alone does not identify a user-correctable cause.
- Invalid calculation, precision, price, quantity, strategy/config invariants,
  static fallback environment settings and static executable-size inconsistencies.
- Parser failures: malformed source signals belong to channel/admin diagnostics,
  rather than account-specific user history. `NotSignal` and ineligible-account
  skips produce no failure entry.
- Pure `leverageChangeRequired` is a decision that the coordinator resolves through
  a POST; it is not a final user failure.

## Implementation rule

Use an explicit mapping returning `Option<UserFailure>`; every unknown reason
returns `None`. Notify only for a final allowlisted rejection with trusted user
and account context. Keep all internal reason codes in logs regardless of mapping.

## Storage and reporting

Admission and trade-construction rejections save `{eventId, info, time}` before
acknowledging the client job. `eventId` hashes the validated job idempotency key
and failure reason. The atomic append retains the newest seven entries and
suppresses an ID only while it remains in that window. Existing bot readers use
`info` and `time`; users see the entry on their next main-menu render.

Reporting tries once with at most two retries, 200 ms apart, within one total
`RUNTIME_OPERATION_TIMEOUT_MS` budget. Every attempt uses the same ID, text and
timestamp. These retries only write history; they do not rerun exchange checks,
account mutations or trade publication.

An appended or retained duplicate logs `user_failure_notification_saved`.
A missing user logs `user_failure_notification_user_missing`; no profile is
created. Exhaustion or timeout logs `user_failure_notification_save_failed`.
Each outcome settles the rejected job; original rejection diagnostics remain
in the terminal. An extended MongoDB outage can lose the notification. Durable
reporting recovery and permanent duplicate protection are separate work.

## Verification

`./scripts/test-signal-workflow.sh` exercises the real binary with isolated
RabbitMQ, MongoDB, Redis and scripted BingX/Telegram HTTP. It checks saved history
for insufficient balance, blocked leverage and leverage above the supported
limit, and checks that success, regular messages, expiry, malformed jobs and
internal API errors add no failure entries. MongoDB write failures prove history
retries do not repeat exchange requests. Focused reporting tests verify identical
entries across retries and bounded timeouts; MongoDB tests cover concurrent
duplicates, missing users and seven-entry retention.

Run `./scripts/verify.sh` for the complete suite. Tests use synthetic credentials
and disposable services; they do not send live trades or Telegram messages.
