# Redis

Redis is used for cross-pod BingX account locks and optional shared API metadata.
All app-to-app communication uses RabbitMQ. Slice 4 removes Redis notification
configuration, dependency, channel, and message contracts.

## Lifecycle

`RedisConnections::connect_required` verifies the lock connection and starts a
managed connection monitor. The monitor detects idle outages with a bounded PING
every second and reconnects using the runtime's capped backoff and jitter.
`wait_for_failure` reports a latched required failure. Reconnection makes the
connection available, but acquisitions stay gated until the coordinator calls
`connect_required` successfully. Existing leases never regain validity after loss.

Commands have bounded deadlines and are never automatically replayed. Cancelling
a command after it takes the socket drops that connection and invalidates active
leases; cancellation while waiting for the socket sends no command. Errors
contain fixed codes only. Lock and cache operations use separate connections.
`quiesce` blocks new acquisitions while renewal continues during drain. `close`
invalidates leases and stops the monitor and connections; a later initialization
creates fresh connections. Dropping a manager aborts its monitor.

The full adapter and binary bootstrap will be composed with RabbitMQ in slice 5.

## Account leases

- Key: existing `account_lease_key` contract, including URI component encoding.
- Acquire: UUID owner token with `SET NX PX 30000`; known contention retries with
  jitter for at most five seconds. Unknown outcomes never grant ownership.
- Renew: every ten seconds, using atomic owner comparison and `PEXPIRE`.
- Verify/release: atomic token comparison; release cannot delete a newer owner.
- Lease commands use the lower of `REDIS_LOCK_COMMAND_TIMEOUT_MS` and one second.
- Local validity starts before the request, with a 100ms safety margin. Renewal
  cannot revive a lease after its local deadline or a connection failure.
- `AccountLease::run` drops a pending protected future on lease loss. Callers must
  keep work inside that future and call `assert_owned` before irreversible effects.
- Explicit `release` stops future renewals, lets a bounded in-flight renewal finish,
  and attempts bounded owner-checked deletion.
  Drop stops renewal; an unreleased key expires naturally.

These preserve the TypeScript single-Redis lease boundary. Redis data loss or
asynchronous replica promotion can lose locks; a lease cannot fence an external
operation already accepted remotely. Durable Mongo claims remain the execution
deduplication mechanism. See [Redis's lock guarantees](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/).

## Optional API cache

`MetadataCache::get_or_load` uses the existing BingX metadata key, JSON format, and
`EXCHANGE_METADATA_CACHE_TTL_MS`. A miss, malformed JSON, timeout, or Redis outage
calls the supplied API loader. Loader errors propagate; responses with a truthy
`err` field are not cached. Shared writes are best effort. Worker-local caching
can wrap this loader when the API workflow is implemented.

`EXCHANGE_METADATA_CACHE_ENABLED` defaults to `true`; `false` avoids opening a
cache connection. Cache connections are lazy and recover on subsequent use.
Cache failures never affect lock availability.

## Verification

Install `redis-server` or set `REDIS_SERVER_BIN`, then run:

```sh
cargo test --test redis -- --ignored
```

Tests spawn isolated loopback Redis processes without persistence and clean them
up on completion. They cover cross-pod contention, renewal during drain, owner
replacement, cancellation, outage/recovery, bounded commands, cache fallback and
expiry, disabled caching, and shutdown.
