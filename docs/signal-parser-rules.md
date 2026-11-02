# Signal extraction and optional exits

Status: implemented.

## Purpose

Extract trading data from Telegram messages while ignoring unrelated text.
Recognize ordinary channel messages separately. Support signals with targets,
stop loss, either one, or neither.

## Detection and extraction

1. Normalize whitespace, leading emoji, label case and Unicode range dashes.
2. Recognize structural symbol/direction headers and labelled ENTRY/BUY fields.
   A valid signal requires one USDT pair, one LONG/SHORT direction and entries.
   Separate symbol and direction header lines remain supported.
3. Extract only supported fields. Ignore banners, reference IDs, commentary and
   signatures before, between or after those fields. Commentary cannot supply a
   missing field or change order type.
4. Validate any supplied trading fields. Malformed recognized fields or attempted
   structural headers reject a candidate; they are not ignored as commentary.
5. Return `NotSignal`, `Parsed`, or `Rejected(reason)`.

Ordinary prose mentioning a coin, prices or LONG/SHORT without an explicit
ENTRY/BUY field is `NotSignal`. Recognizable incomplete trading instructions
with an entry field reject with the missing/invalid field reason. Multiple
structural headers, duplicate fields and conflicting directions reject.

Explicit management instructions such as CLOSE/CANCEL or supported edit commands
are detected as instructions, rather than ordinary mentions in prose. Standalone
management messages do not create signals. Mixing one into a signal rejects as
`AmbiguousInstruction`. Telegram button labels do not enter the text parser.

## Field contract

| Field | Rule |
| --- | --- |
| Symbol | Required USDT pair: BTC/USDT or BTCUSDT; exchange support checked later |
| Direction | Required LONG or SHORT |
| Entry | Required positive decimal price(s), including a range |
| Targets | Optional TG/TP list of prices or ROI percentages; one format per signal; numbered labels start at 1 and stay consecutive |
| Stop loss | Optional positive price via existing SL/STOP syntax |
| Leverage | Optional in text; explicit valid value wins, otherwise resolve active user/channel configuration per client |
| Position size | Optional percentage; 1% becomes 0.01 of balance |
| BREAKOUT | Recognized signal-header instruction for breakout entry construction |
| DAY/SWING/SCALP | Descriptive text, ignored |

Keep original decimal spelling. Supplied prices must be finite and positive.
Numbered and unnumbered TG/TP labels cannot be mixed.
When present, LONG stops must be below entries and price targets above entries in
increasing order; SHORT uses the reverse price relationships. Missing exits skip these
checks. Invalid supplied exits do not become absent exits. Resolved worker
leverage must be a positive integer within exchange limits.

## Percentage profit targets

`TG1 100%` expresses leveraged ROI. All profit targets in one signal must be
prices or percentages; mixed formats reject as `MixedTargetFormats`. Percentages
must be finite, positive and increasing for both LONG and SHORT. Decimal
percentages such as `12.5%` are supported. Entry and stop-loss fields remain prices.

Parsing and client jobs preserve percentage intent:

```json
{"type":"percent","value":100,"raw":"100%"}
```

Each client resolves leverage from the explicit signal, eligible active user
settings, then active channel settings. Every supplied SHORT percentage must be
below `leverage × 100`; validation happens before account admission or mutations,
even if allocation would later omit that target. An invalid client is skipped
without preventing other valid clients from proceeding.

After constructing entries, use their fraction-weighted price as the reference.
The original helper falls back to their mean without usable fractions, then the
current price without valid entries; the builder still requires valid entries.

```text
movement = percentage / (leverage × 100)
LONG price  = reference × (1 + movement)
SHORT price = reference × (1 − movement)
```

Apply existing price precision and allocation. Reject nonpositive/nonfinite prices,
prices inside the entry range and targets that collapse or lose order after
rounding. Calculated prices bypass legacy integer-price shorthand.

Example: XLM SHORT at `0.18892`, leverage `8x`, targets `100/200/300/400%`
resolve to `0.165305/0.141690/0.118075/0.094460` before exchange rounding.
Parsed logs retain percentage objects; published Trading Station targets contain
ordinary price strings. No additional exchange requests or configuration are needed.

## Optional exits through publication

- Parsed signal: `sell_targets: []` when absent; optional `stop_loss` omitted.
- Client job: same absence semantics; entry and resolved leverage remain required.
- Builder: construct only supplied exits, applying existing sizing/allocation and
  precision rules to them. Empty exits must not cause division by zero, minimum
  allocation errors or attempts to format a missing stop price.
- Final trade: `sell_targets: []` when absent; omit `stop_loss` when absent.
  Preserve the existing object shapes when either is supplied.
- Do not invent target/stop prices or manufacture a zero-price order.

Trading Station accepts empty target arrays and absent stops at creation. The
coordinated change in `../trading-station-rust/src/trades/strategies/advanced.rs`
keeps Advanced configured: after a profit fill, absent stop loss requests further
profit processing without a stop move or notification. An invalid supplied stop
still errors. Deploy that Trading Station change alongside support for stop-free
Advanced signals. Creation tests cover all four exit combinations.

## Reported signal

The VANERIX banner and VQA reference are ignored. The reported message extracts
BTCUSDT, LONG, entries 83827.1/83987.2, target 84087.2, leverage 5x,
position 0.01 and stop 83707.1.

`SINGLE_TARGET_MARKET` in a banner does not select MARKET entry type. `TEST ONLY`
is commentary and does not enable a dry-run mode.

## Implementation stages

1. Add line classification and signal detection; keep extraction and validation
   separate. Introduce the exact VANERIX regression before changing behavior.
2. Make exits optional in the parsed model and validators. Preserve the mandatory
   resolved leverage contract and existing settings precedence.
3. Update exit construction, quantity allocation and final serialization. Verify
   Trading Station compatibility for all four exit combinations.
4. Expand parser and RabbitMQ binary tests; update parser docs and logs.

## Acceptance tests

- Original signal and decorated signal produce identical extracted data and final
  trade objects, apart from runtime identities/timestamps validated separately.
- Test all four target/stop combinations through final RabbitMQ publication.
- Regular commentary, price updates and coin mentions remain `NotSignal`.
- Missing entry, malformed recognized fields, invalid supplied exit prices,
  conflicting directions, multiple signals and mixed management instructions reject.
- IDs and numbers in ignored text never become trade values; BREAKOUT inside
  commentary does not change entry type.
- Existing format, numeric, user settings, admission and publication tests pass.
- XLM percentage SHORT/LONG, weighted entry ranges and per-user leverage reach
  final RabbitMQ publication. Mixed formats and impossible SHORT percentages
  publish no trade and make no account calls.

Successful parsing logs extracted data and ignored-line count. Rejections report
reason, field and source line where available. Logs never include account secrets.
