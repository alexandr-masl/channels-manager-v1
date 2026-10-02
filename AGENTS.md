# Satoshi Channel Updates Manager (Rust)

This app is the Rust transition of the existing `satoshi-channel-updates-manager` service. The existing TypeScript app is the source of truth for current behavior while the transition is in progress.

## Purpose

Consume validated Telegram trading work and publish trade creation or trade management messages for Trading Station. Migrate the service incrementally, beginning with creation of BingX Futures trades from the per-client worker queue. TypeScript continues to parse Telegram signals and publish client jobs during this first stage.

## First integration boundary

- Input: `satoshi-channel-updates.client-trade.bingx.futures` client-trade jobs.
- Output: `create-new-trusted-trade` (or `RABBITMQ_QUEUE`) messages for Trading Station.
- Preserve the existing job identity, duplicate protection, BingX account admission, trade payload, and `expires_at` contract during migration.
- Coordinate queue ownership at cutover so only the Rust worker consumes BingX Futures jobs.

Refer to the TypeScript app's `AGENTS.md` and `docs/new-trade-publication.md` for the current behavior and RabbitMQ contract before implementing each migrated feature.
