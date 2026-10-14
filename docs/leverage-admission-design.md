# BingX leverage adjustment

## Goal

An otherwise admissible account with no blocking exchange activity must not reject a
signal merely because BingX currently has a different leverage setting.

## Decision

Keep existing position-mode admission. Leverage changes are blocked only by live
positions or pending orders for the affected symbol and side. Hedge checks the
requested LONG/SHORT side (BOTH also blocks); One-Way checks both sides. Zero-size
positions and unrelated symbols/opposite Hedge sides do not block.

| Condition | Action |
| --- | --- |
| Invalid evidence or requested leverage above the applicable maximum | Reject |
| Exchange leverage already matches | Continue without a POST |
| Leverage differs, affected live position or pending order exists | Reject `leverageChangeBlocked` |
| Leverage differs, no affected position or order | Set signal leverage, then continue |

Stored managed leverage alone never rejects or prevents an adjustment. Remove
`leverageConflict` and `ownedLeverageMismatch`. Managed context still participates
in the separate, unchanged position-mode decision.

## Implementation

1. Use `plan_admission` in `admission.rs` to return the validated account
   and an optional side to update; use live activity and limit validation.
2. Add a signed `set_leverage` method to the BingX client using the original
   `POST /openApi/swap/v2/trade/leverage` contract: symbol, leverage, side.
   Hedge uses LONG or SHORT; permitted One-Way uses BOTH.
3. Run this decision after position-mode admission/migration. Validate balance and
   leverage evidence before the leverage POST. A successful response requires HTTP
   success, API code 0, and matching returned symbol and leverage. Release the
   prepared admission result only after the POST succeeds.
4. Extend `AdmissionAttempt` to track mode and leverage writes separately. Allow
   one of each in the same attempt; any started mutation prevents an outer timeout
   from becoming an ordinary read retry. Preserve the existing job deadline.
5. Log symbol, signal identity, side, previous/requested leverage, and outcome
   (`unchanged`, `changed`, or rejection reason), without account credentials.

## Simple failure handling

Use the existing mode-switch approach: no feature flag, confirmation GET, readback,
or automatic mutation retry. Rejection, malformed response, or timeout stops the
job and ACKs it without publishing a trade. A timeout may leave leverage changed.
Temporary reads before mutations retain existing bounded retry behavior.
Never start a write or publish a trade after expiry.

This intentionally simplifies the original app's readback/reconciliation flow.
Claims and locks remain deferred; concurrent account changes are still possible.

## Tests

- Empty account, mismatched leverage: set 3x and publish the ADA trade.
- Mode migration followed by leverage change succeeds within one attempt.
- Matching leverage makes no POST; LONG/SHORT/BOTH select the correct fields.
- Affected live positions/orders, invalid evidence and exceeded limits reject.
- Unrelated symbols/opposite Hedge activity and differing stored leverage do not block.
- Validate signed request parameters and returned symbol/leverage.
- POST rejection, malformed response, timeout and expiry publish no trade and do
  not trigger automatic mutation retries.
- Binary RabbitMQ regression reaches `create-new-trusted-trade` after adjustment.

## Alternatives

Removing the mismatch check alone could create a trade with the wrong leverage.
Porting the full original confirmation/reconciliation flow adds requests and scope.
The change uses live activity with one conditional POST.

Sources: original `src/hedge-mode/bingx-leverage-admission.ts`,
`src/hedge-mode/bingx-leverage.ts`, and
`src/exchange/bing-x/bing-x-futures.ts`.
