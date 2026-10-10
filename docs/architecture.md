# Application architecture

**Version:** 1 · 2026-10-04 · Proposed target architecture.

Keep this document current as implementation decisions change. The TypeScript
`satoshi-channel-updates-manager` remains the behavioral source of truth.
Implemented through slice 3: Telegram intake validates structure and source time,
authorizes the channel, parses USDT Futures signals, selects eligible BingX accounts
and loads user settings. It logs context or skip/rejection reasons and acknowledges.
Job preparation and fan-out remain planned.
The workflows below extend the initial client-job migration to include signal intake.

## Workflows

### Telegram signal intake

Input: `tg_bot_channel_update`.

1. Decode the message; validate structure, source time, and channel authorization.
2. Parse text into a typed `TradingSignal`; validate prices, targets, side, and leverage.
3. Load channel/user settings, connected clients, eligibility, and market context.
4. Build one `ClientTradeJob` per eligible BingX Futures account.
5. Confirm publication to `satoshi-channel-updates.client-trade.bingx.futures`, then acknowledge the input.

### BingX trade execution

Input: `satoshi-channel-updates.client-trade.bingx.futures`.

1. Validate job identity and original expiry; resolve effective trading settings.
2. Check duplicate protection and acquire the MongoDB execution claim.
3. Acquire the Redis account lease; perform BingX admission and exchange checks.
4. Build the trade payload and confirm publication to `create-new-trusted-trade`
   (or `RABBITMQ_QUEUE`).
5. Record the terminal outcome, release resources, and settle the delivery.

Retain both queues within one application, with explicitly configured consumers.
This lets intake and account execution scale independently.

## Module responsibilities

| Module | Responsibility |
| --- | --- |
| `main.rs`, `infrastructure.rs` | Configuration, dependency wiring, worker lifecycle |
| `telegram/{message,handler,workflow,channel_update,context}.rs` | Telegram envelope, delivery settlement, channel validation and client fan-out |
| `signals/{model,parser,validation,settings}.rs` | Typed signals, pure text parsing, consistency checks, effective settings |
| `trading/{job_handler,execution}.rs` | Job delivery, claims, leases, deadlines and terminal outcomes |
| `exchanges/bingx/{client,admission,trade_builder,workflow}.rs` | HTTP/auth, account checks, trade calculations and BingX orchestration |
| `mongo/` | Existing repositories plus channel, profile and settings queries |
| `redis/` | Cross-pod account locks and optional API metadata cache |
| `rabbitmq/`, `contracts/` | Transport, confirms, delivery policy and external wire formats |

New module paths are planned; existing infrastructure is reused. Keep validators
with the module that owns their rules. Inject repositories and exchange clients;
keep parsing and calculations testable without network access.

## Invariants

- Handlers own RabbitMQ settlement; workflows return typed outcomes.
- Only pre-claim failures may request job retries. Post-claim failures are terminal.
- Preserve work/trade IDs, original expiry and serialized publication bytes on retry.
- Partial fan-out and lost acknowledgements can duplicate jobs; claims suppress repeated execution.
- Redis carries no inter-app messages. All inter-app communication uses RabbitMQ.
- Coordinate exclusive queue ownership with TypeScript during migration.
- `Infrastructure::for_telegram_intake` selects the raw queue and handler explicitly;
  the general constructor retains the separate BingX job contract.

## Implementation order

1. Extract the Telegram handler; decode, parse, validate and log the ADA signal fixture.
2. Add channel authorization, client selection, settings and job fan-out.
3. Add claimed BingX execution, admission, trade construction and publication.
4. Verify both workflows, recovery and duplicate handling before queue cutover.

### Suggested structure

```bash
src/
  main.rs                     # Startup and wiring
  infrastructure.rs           # Connections and worker lifecycle

  telegram/
    handler.rs                # RabbitMQ delivery → workflow → settlement
    message.rs                # Typed Telegram message
    workflow.rs               # Channel checks, context, client fan-out

  signals/
    model.rs                  # TradingSignal, direction, targets
    parser.rs                 # Telegram text → TradingSignal
    validation.rs             # Signal consistency checks
    settings.rs               # Channel/user settings resolution

  trading/
    job_handler.rs            # Client job delivery and settlement
    execution.rs              # Claims, leases, deadlines, terminal outcomes

  exchanges/
    bingx/
      client.rs               # HTTP/authentication and API responses
      admission.rs            # Account/position-mode checks
      trade_builder.rs        # Sizing and trade payload construction
      workflow.rs              # Coordinates BingX operations

  mongo/                      # Existing repositories + missing channel/config queries
  redis/                      # Existing leases and optional cache
  rabbitmq/                   # Existing transport and delivery policy
  contracts/                  # External message formats
```

## Intake validation (slice 1)

Require integer `message_id`, `date` (Unix seconds), and `chat.id`/`chat.type`.
Identifiers must fit JavaScript's safe integer range; message IDs must be positive
and chat IDs nonzero. Allow additional Telegram fields. Accept source timestamps
up to 10 minutes old or 2 minutes ahead, inclusive, using existing contract limits.
Skip non-channel messages, replies and absent/blank text. Reject malformed
payloads or invalid timestamps without logging their contents. Accepted text is
escaped for terminal output. Envelope validation does not imply channel authorization;
`ChannelUpdateManager` performs that check before parsing.


## Signal parsing (slice 2)

`signals::parse_signal` is pure and returns `Parsed`, `NotSignal`, or `Rejected`.
The base format uses separate lines: symbol/direction header, `ENTRY [ZONE]`
(or `BUY [ZONE]`) prices, `TG`/`TP` numbered price targets, `LEVERAGE`, and
`SL [Hard at]` or `STOP LOSS`. Labels are case-insensitive; leading emoji,
and `#ADA/USDT`/`ADAUSDT` are supported. Descriptive text in the symbol/direction
header (such as `DAY`, `SWING`, or other style labels) is ignored; it has no
effect on the trade. Symbols and directions still require validation.
`BREAKOUT` is a semantic header keyword, matched case-insensitively: it emits
`breakOutEntry: true` in parsed JSON, omitted for ordinary signals. It keeps
the same side and price validation. In TypeScript, `def_buy_targets` in
`src/targets/targetsMethods.ts` uses this flag to select `STOP_LOSS_LIMIT`
instead of `LIMIT`; Rust target construction will consume it in the later
trade workflow. Parsing and logging do not place orders.

Require one symbol and direction, entries, at least one target, stop and leverage.
Numbers must be finite positive plain decimals. LONG targets rise above all
entries with stop below; SHORT targets fall below all entries with stop above.
Numbered targets must start at 1 and be consecutive. Duplicate scalar fields,
ambiguous headers, unknown instructions and percentage profit targets are rejected.
Entry ranges (`71-72`, including spaced and Unicode dashes) become two entry
targets, preserving their order. Optional `POSITION SIZE 0.5%` becomes
`position: 0.005`, matching the original balance-fraction contract; omitted
position size stays absent. Missing required values are never inferred.
Decimal strings and leverage notation retain TypeScript wire compatibility.

`TelegramHandler` logs `Signal parsed: ... result=<JSON>` and the context summary
when eligible accounts exist. Other outcomes log a static skip/rejection reason.
Job fan-out and trading actions are not implemented yet.

### Compatibility trace (2026-10-04)

Source: TypeScript `src/handlers/trading-signal-reader.ts`, its
`test/trading-signal-reader.test.js`, and `src/trade-processing/position-size.ts`.
The SOL `71-72` / `POSITION SIZE 0.5%` regression matches the original parser's
JSON output. The original sizing resolver gives signal `position` precedence
over channel settings; applying that override belongs to the settings slice.

Remaining differences to migrate explicitly:

- Percentage profit targets use `{type: "percent", value, raw}` in TypeScript;
  Rust currently supports absolute price targets only.
- TypeScript accepts additional aliases (`OPEN`, `TAKE PROFIT`, `EXIT`,
  `STOPLOSS`, `S/L`, etc.) and inline text.
- TypeScript parses signals without a stop and classifies signals without
  leverage as Spot. Rust currently requires a complete Futures signal.
- Rust enforces target numbering, price ordering, and unambiguous fields more
  strictly. Do not copy TypeScript's permissive punctuation stripping or its
  accidental filtering of entry prices 1–9.

The parser is a supported subset, not full TypeScript parity. Regression tests
must include source-app examples as well as generated Rust-format cases.


## Channel update workflow (slice 3)

`telegram/channel_update.rs::ChannelUpdateManager` is the Rust equivalent of
TypeScript `MessageManager.handleChannelUpdate`. `ChannelRepository` is its test
seam; `MongoRepositories` supplies the real adapter. `main.rs` stays wiring only.

1. Look up `mcr_channels.id` in Bot MongoDB; a missing channel is unauthorized.
2. Parse with the existing signal parser; stop on non-signals or invalid signals.
3. Query `tradingprofiles` by `exchangeClients.connectedChannel`, then select
   matching BingX clients. Read only the original projected account fields.
4. Batch unique user IDs through Account Validator `users`; require
   `auto_trading: true` and `valid_till` strictly later than the workflow clock.
5. Load `user_configs` for eligible users. Preserve the original config and its
   matching `private_channels` entry, including `own_settings`; effective trading
   settings are resolved in the later execution slice.
6. Return `ChannelContext` with the original message identity/time, parsed signal,
   channel settings and eligible clients. Missing optional user settings are allowed.

Outcomes: `Ready`, `Skipped` (unauthorized, non-signal, no connected/eligible
accounts), `Rejected` (invalid signal/context), or a Mongo dependency error.
The handler logs only parsed signal data, IDs, eligible counts and static reasons.
Contexts and clients have no Debug/Serialize implementation because they contain
credentials. Mongo errors use the existing bounded retry/dead-letter policy;
lookup failures never become empty results. Ready/skip/reject are acknowledged.

Local signal tests now require an existing channel, a connected BingX account and
an active auto-trading subscription in the configured databases. Without those,
expect a skip log. No external account calls or trade/job publication occur yet.
