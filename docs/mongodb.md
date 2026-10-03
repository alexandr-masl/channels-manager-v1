# MongoDB

Stage 3 adds three reusable pools with at most 10 connections each and a 5-second
server-selection timeout:

| Environment variable | Collections used |
| --- | --- |
| `MONGO_PATH` | `bingx_futures_execution_claims`, `user_configs` |
| `TRADE_STATION_MONGO_PATH` | `trade_station_active_trades` |
| `ACCOUNT_VALIDATOR_MONGO_PATH` | `users` |

Each URI selects its database; an omitted name uses `test`, matching Mongoose.
Use an explicit name in deployments. Repository clones share the driver pools.

## Lifecycle

`MongoConnections::connect` opens and verifies all three connections. It retains
partial connections so cancellation and retries can clean up or reuse them.
`initialize_indexes` creates and verifies the unique, non-TTL `workId` index before
claims are accepted. A failed verification closes the claim gate for every clone.
Reconnect requires index verification again. `close` invalidates claims and closes
all pools after worker drain; repeated close is safe. Operations have deadlines,
and exposed errors contain only the database role and sanitized error kind.

These methods will be composed into the runtime adapter with Redis and RabbitMQ.
The binary still validates configuration and exits until that composition exists.
Health endpoints remain outside this issue.

## Repositories

- **Claims:** majority/journaled writes with 5-second write-concern timeout,
  6-second operation deadline, majority duplicate reads, immutable-input comparison,
  and 2-second owner-checked terminal updates. An uncertain acknowledgement never
  grants ownership. Claims are neither reclaimed nor expired. The future worker
  supplies canonical input and its hash.
- **Trades:** projected lookups by idempotency keys and active managed futures
  accounts. Preserve the upstream `_binance_futures_` marker and `state != FINISHED`
  filter, including trades still being created.
- **Notifications:** append timestamped messages atomically, retain the newest
  seven, and never create missing users.
- **Accounts:** filter requested Telegram IDs by `auto_trading: true`; project
  `tg_chat_id`, `valid_till`, and `auto_trading`. Expiry classification stays with
  the caller. Adding this repository does not add worker-side eligibility filtering.

## Verification

`cargo test --test mongodb -- --ignored` starts an isolated, temporary single-node
replica set on loopback. Install MongoDB 7+ or set `MONGOD_BIN` to its binary. The
test uses synthetic records and removes its process/data on completion. It verifies
concurrent claims, conflicts, write-concern ambiguity, index mismatch, projections,
notification retention, startup recovery, and shutdown.

Behavior follows the sibling TypeScript app's database repositories and
`docs/new-trade-publication.md`. Pool management follows the
[MongoDB Rust driver documentation](https://www.mongodb.com/docs/drivers/rust/current/connect/mongodb-client/).
