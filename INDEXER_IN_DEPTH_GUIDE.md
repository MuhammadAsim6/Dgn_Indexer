# ZigIndexerV3 In-Depth Guide

This document explains how the current indexer works end-to-end, from RPC ingestion to final PostgreSQL state.

## 1. What This Indexer Does

The indexer reads CometBFT/Cosmos blocks, decodes tx messages/events, extracts domain-specific records, and writes them into partitioned PostgreSQL tables.

Main goals:
- historical backfill from a start height to an end height
- optional follow mode for live indexing
- resumable progress tracking
- chain-state bootstrap (genesis + startup snapshots)
- current-state maintenance (`bank.balances_current`, `stake.delegations_current`, `tokens.factory_supply_current`) via triggers
- reconciliation against RPC state for balance correctness

Core entrypoint: `src/index.ts`.

## 2. Runtime Lifecycle (Chronological)

1. Load and validate config.
- `src/config.ts`
- `src/config/schema.ts`

2. Initialize DB pool (Postgres sink only).
- `src/db/pg.ts`

3. Run genesis bootstrap (if genesis file exists and height 0 not already seeded).
- `src/scripts/genesis-bootstrap.ts`

4. Create RPC client and resolve start/end heights.
- uses `/status` for latest/earliest and resume logic
- progress state read from `core.indexer_progress`

5. Initialize decode worker pool.
- `src/decode/txPool.ts`
- workers in `src/decode/txWorker.ts`

6. Initialize sink.
- Postgres sink is `src/sink/postgres.ts`

7. Startup chain snapshots (Postgres mode).
- validators snapshot via ABCI
- network params snapshot via ABCI
- tokenwrapper module-info snapshot via ABCI

8. Backfill range sync (`syncRange`).
- `src/runner/syncRange.ts`
- fetch block + block_results + validators for each height
- decode txs in worker pool
- assemble unified block JSON
- write to sink in strict height order

9. Retry missing heights.
- `src/runner/retryMissing.ts`
- missing heights tracked in `core.missing_blocks`

10. Reconciliation cycle.
- `src/sink/pg/reconcile.ts`
- mode: `off` | `negative-only` | `full-once-then-negative`

11. Optional live follow loop.
- `src/runner/follow.ts`
- periodic missing retry + periodic reconcile loop

## 3. Data Sources and Exactly Where Data Comes From

The indexer uses these sources:

1. Block RPC
- `/block?height=...`
- provides block header, tx base64 list, commit signatures

2. Block Results RPC
- `/block_results?height=...`
- provides tx execution results, tx-level events, begin/end block events

3. Validators RPC for each indexed height
- `/validators?height=...`
- used for validator set snapshot per block

4. ABCI queries at startup and during flush/reconcile
- startup validators: `/cosmos.staking.v1beta1.Query/Validators`
- startup params:
  - `/cosmos.auth.v1beta1.Query/Params`
  - `/cosmos.bank.v1beta1.Query/Params`
  - `/cosmos.staking.v1beta1.Query/Params`
  - `/cosmos.distribution.v1beta1.Query/Params`
  - `/cosmos.mint.v1beta1.Query/Params`
  - `/cosmos.slashing.v1beta1.Query/Params`
  - `/zigchain.factory.Query/Params`
  - `/cosmos.gov.v1.Query/Params`
- startup wrapper snapshot: `/zigchain.tokenwrapper.Query/ModuleInfo`
- governance enrichment: `/cosmos.gov.v1.Query/Proposal` with v1beta1 fallback
- wasm contract enrichment: `/cosmwasm.wasm.v1.Query/ContractInfo`
- token registry metadata enrichment: `/cosmos.bank.v1beta1.Query/DenomMetadata`
- reconciliation:
  - `/cosmos.bank.v1beta1.Query/Balance`
  - `/cosmos.bank.v1beta1.Query/AllBalances`

5. Genesis data (one-time bootstrap)
- `genesis.json` app_state sources:
  - `bank.balances`
  - `bank.supply`
  - `tokenwrapper`
  - staking/vesting used for reporting context

## 4. Decode and Normalize Pipeline

1. Tx base64 decoding
- worker first tries known fast decoders (`src/generated/knownMsgs.ts`)
- fallback to dynamic protobuf root decoding
- fallback for unknown types: keep raw base64 payload

2. Friendly field conversion
- worker converts decoded objects to snake_case style (`makeFriendly`)
- keeps `@type` markers

3. Event normalization
- `src/normalize/events/*`
- decodes canonical base64 event attributes to UTF-8 when possible
- combines message logs and tx-level events safely

4. Assembled block object
- `src/assemble/blockJson.ts`
- each tx has hash, raw, decoded body/auth, tx_response events/logs

## 5. Sink Architecture (Postgres)

Main implementation: `src/sink/postgres.ts`.

Flow per block:
1. `extractRows(blockLine)` builds many row arrays by domain.
2. rows are buffered in memory.
3. `flushAll()` writes all buffered rows in one DB transaction.

Modes:
- `batch-insert` (default): flush when buffers cross thresholds.
- `block-atomic`: flush after every block (`persistBlockAtomic`).

Failure model:
- on flush error: rollback transaction
- restore snapshot back into buffers
- keep processing resiliently

## 6. Extraction Logic by Domain

### 6.1 Core Tables

From assembled txs/logs/events:
- `core.blocks`
- `core.transactions`
- `core.messages`
- `core.events`
- `core.event_attrs`

Also:
- unknown/undecoded messages -> `core.unknown_messages`

### 6.2 Bank and Transfers

Sources:
- tx `transfer` events
- `coin_received` / `coin_spent` events (including failed tx fee effects)
- begin/end block events for module-side balance changes

Outputs:
- `bank.transfers`
- `bank.balance_deltas`

Current state trigger:
- `bank.balance_deltas` insert triggers `bank.update_balances_current()`
- maintains `bank.balances_current` JSONB balances per account
- zero balances preserved as explicit `'0'`

### 6.3 Staking and Distribution

Message-driven extraction:
- delegate/undelegate/redelegate -> `stake.delegation_events`
- withdraw reward / set withdraw address -> `stake.distribution_events`

Current state trigger:
- `stake.delegation_events` updates `stake.delegations_current`

### 6.4 Governance

Sources:
- message types:
  - submit proposal
  - deposit
  - vote / weighted vote
- end_block events for lifecycle transitions and timestamps
- startup helper updates gov param cache
- flush-time ABCI proposal enrichment for accurate timestamps/title/summary

Outputs:
- `gov.proposals` (upserted and merged)
- `gov.deposits`
- `gov.votes`

### 6.5 IBC

Sources:
- packet lifecycle events:
  - `send_packet`
  - `recv_packet`
  - `acknowledge_packet`
  - `timeout_packet`
- channel/connection/client discovery events
- denom trace/denomination events
- msg transfer intents used to enrich packet fields

Outputs:
- `ibc.packets`
- `ibc.transfers`
- `ibc.channels`
- `ibc.clients`
- `ibc.connections`
- `ibc.denoms`

### 6.6 WASM

Sources:
- message types:
  - store/instantiate/migrate/update instantiate config
  - execute/sudo/update-admin/clear-admin
- wasm events + attrs from tx logs
- CW20 transfer heuristics from wasm events
- startup/flush ABCI contract-info fallback enrichment

Outputs:
- `wasm.codes`
- `wasm.contracts`
- `wasm.contract_migrations`
- `wasm.executions`
- `wasm.events`
- `wasm.event_attrs`
- `wasm.admin_changes`
- `tokens.cw20_transfers`
- `wasm.dex_swaps` (contract swap analytics)

### 6.7 Zigchain Custom Modules

Factory:
- `MsgCreateDenom`, `MsgUpdateDenomURI`
- URI and URI-hash sanitization
- writes `zigchain.factory_denoms`

DEX:
- pool create/add/remove liquidity from messages
- native `token_swapped` events for swaps
- writes:
  - `zigchain.dex_pools`
  - `zigchain.dex_swaps`
  - `zigchain.dex_liquidity`

Wrapper:
- module wallet fund/withdraw events -> `zigchain.wrapper_events`
- `MsgUpdateIbcSettings` -> wrapper event + settings upsert
- writes:
  - `zigchain.wrapper_events`
  - `zigchain.wrapper_settings`

Legacy wrapper payload handling:
- `normalizeWrapperIbcSettingsMessage` detects legacy shifted payloads and remaps fields before insert.
- skips settings insert if denom cannot be resolved.

### 6.8 Token Supply and Registry

Factory supply event capture:
- from factory msgs and module bank events (`coinbase`/`burn`)
- writes `tokens.factory_supply_events`

Current supply state:
- trigger from `tokens.factory_supply_events` updates `tokens.factory_supply_current`

Universal token catalog:
- writes `tokens.registry`
- registration points include transfers, balance deltas, staking, gov deposits, ibc denoms, swaps, pools, wrapper settings, factory supply events, genesis, startup wrapper snapshot

## 7. Token Registry Rules (Current Behavior)

Helper: `src/utils/token-registry.ts`.

Type inference:
- `factory/...` or `coin.zig...` -> `factory`
- `ibc/...` or `transfer/...` -> `ibc`
- `zig1...` without slash -> `cw20`
- else -> `native`

Wrapper source override:
- if source is `wrapper_settings` and inferred type is `native`, stored type becomes `ibc`

Symbol/base/decimals:
- native micro-denom rule strips leading `u` only for pure alnum micro denoms (e.g. `uzig -> ZIG`)
- denoms like `unit-zig` keep full uppercase symbol style
- default decimals fallback is 6

RPC enrichment (once per denom, cached):
- during flush, for new denoms in buffer, query `/cosmos.bank.v1beta1.Query/DenomMetadata`
- cache stores hit and miss, so each denom is requested at most once per process
- when metadata exists:
  - update decimals from display denom-unit exponent (or max exponent fallback)
  - update symbol/base_denom
  - merge full metadata JSON

First-seen semantics:
- in-block dedup keeps earliest observed height/tx per denom
- DB upsert keeps monotonic earliest `first_seen_height`/`first_seen_tx`
- alias policy is preserved: all observed denom strings are separate rows

## 8. Flush Order and Transactional Guarantees

`flushAll()` sequence (single DB transaction):
1. ensure needed partitions
2. core tables
3. gov votes/deposits, proposal enrichment, proposals upsert
4. authz/feegrant/cw20
5. staking + validator tables
6. ibc tables
7. wasm registry + wasm ABCI enrichment
8. bank balance deltas
9. wasm executions/events/attrs/admin
10. network params, unknown messages, factory supply events
11. wasm swaps
12. token registry
13. zigchain tables
14. progress upsert
15. commit

On error:
- rollback
- restore all snapshot buffers
- retry in future flush cycles

## 9. Database Schema Design and Partitioning

Primary schema file: `initdb/010-indexer-schema.sql`.

Partition strategy:
- mostly RANGE partitioned by `height` or `block_height`
- partition ranges configured in `util.height_part_ranges`
- partitions created just-in-time by `util.ensure_partition_for_height`

Partition bootstrap:
- initial p0 partitions are created at DB init (`initdb/040-bootstrap-partitions.sql`)

Archival operation:
- `scripts/detach_partitions_by_height.sql` detaches old partitions by range

## 10. Genesis Bootstrap Behavior

`src/scripts/genesis-bootstrap.ts`:
- runs once if no `bank.balance_deltas` row at height 0
- seeds:
  - `bank.balance_deltas` from genesis balances
  - `bank.balances_current` by calling population function
  - `tokens.factory_supply_events` baseline from genesis supply
  - `zigchain.wrapper_settings` if tokenwrapper state exists
  - `tokens.registry` from genesis bank/supply + wrapper denom

Important:
- wrapper-specific registry row now overrides generic row by denom in-memory, avoiding duplicate logical insertion while preserving wrapper classification.

## 11. Reconciliation Engine

File: `src/sink/pg/reconcile.ts`.

Modes:
- `negative-only`: fixes only negative balances in `bank.balances_current`
- `full-once-then-negative`: full account/denom comparison first, then negative-only cycles

Safety guards:
- skips when indexer lag is too high (`latest - indexed > maxLagBlocks`)
- skips when indexed height is older than RPC earliest (pruned node protection)

Correction mechanism:
- inserts correction deltas into `bank.balance_deltas` with `mode='merge'`
- triggers update `bank.balances_current`

## 12. Missing Block Recovery

Missing block table:
- `core.missing_blocks`

Recording:
- `syncRange` marks skipped heights after retry exhaustion

Recovery:
- `retryMissingBlocks` periodically retries and resolves records when successful

## 13. Configuration and Tuning

Main env/config source:
- `.env.example`
- `src/config.ts`

High-impact knobs:
- `CONCURRENCY`
- `RPS`
- `TIMEOUT_MS`
- `PG_BATCH_*`
- `PG_POOL_SIZE`
- `RECONCILE_*`
- `FOLLOW` and `FOLLOW_INTERVAL_MS`

Postgres sink mode:
- `PG_MODE=batch-insert` recommended
- `PG_MODE=block-atomic` for strict per-block flush behavior

## 14. Extending the Indexer Safely

When adding new extraction:
1. Add extraction in `src/sink/postgres.ts` (message and/or event path).
2. Push normalized rows into dedicated buffer arrays.
3. Add inserter + flusher if table is new.
4. Add schema DDL in `initdb/*.sql` with proper PK and indexes.
5. Register partition range in `initdb/030-util-partitions.sql` if partitioned.
6. Wire flush call order in `flushAll()`.
7. Add idempotent `ON CONFLICT` policy.
8. Add token registration calls if the new data has denoms.
9. Validate replay with fresh reindex.

## 15. Operational SQL Checks

1. Progress
```sql
SELECT * FROM core.indexer_progress;
```

2. Missing blocks
```sql
SELECT * FROM core.missing_blocks WHERE status = 'missing' ORDER BY height;
```

3. Token registry coverage against major denom sources
```sql
WITH src AS (
  SELECT denom FROM bank.transfers
  UNION
  SELECT denom FROM bank.balance_deltas
  UNION
  SELECT denom FROM tokens.factory_supply_events
  UNION
  SELECT base_denom AS denom FROM ibc.denoms
  UNION
  SELECT full_path AS denom FROM ibc.denoms
  UNION
  SELECT denom FROM zigchain.wrapper_settings
)
SELECT s.denom
FROM src s
LEFT JOIN tokens.registry r ON r.denom = s.denom
WHERE r.denom IS NULL
ORDER BY s.denom;
```

4. Wrapper settings sanity
```sql
SELECT * FROM zigchain.wrapper_settings ORDER BY updated_at_height, denom;
```

5. Negative balances
```sql
SELECT account, key AS denom, value
FROM bank.balances_current, jsonb_each_text(balances)
WHERE (value::numeric < 0)
ORDER BY account, denom;
```

## 16. Known Constraints and Current Gaps

1. `clickhouse` sink is declared but not implemented.
- `src/sink/clickhouse.ts`

2. `shards`/`shardId` config is validated and printed but not currently used in the processing scheduler.
- no shard partitioning logic in `syncRange`/`follow`

3. Test coverage is minimal and mostly blueprint-level for balance audits.
- `src/tests/balance_audit_tests.ts`

4. `util.refresh_all_analytics(...)` is referenced by `src/scripts/refresh-analytics.ts` but its SQL definition is not present in current `initdb` files.

## 17. File Map (Most Important)

- App entry and orchestration: `src/index.ts`
- Backfill runner: `src/runner/syncRange.ts`
- Follow runner: `src/runner/follow.ts`
- Missing retry runner: `src/runner/retryMissing.ts`
- RPC client: `src/rpc/client.ts`
- Decode pool/worker: `src/decode/txPool.ts`, `src/decode/txWorker.ts`
- Block assembly: `src/assemble/blockJson.ts`
- Main extraction + flushing: `src/sink/postgres.ts`
- Generic batch SQL helper: `src/sink/pg/batch.ts`
- Token registry helper: `src/utils/token-registry.ts`
- Reconcile engine: `src/sink/pg/reconcile.ts`
- Genesis bootstrap: `src/scripts/genesis-bootstrap.ts`
- Main schema: `initdb/010-indexer-schema.sql`
- Partition utilities: `initdb/030-util-partitions.sql`
- Triggers/state tables: `initdb/050-triggers.sql`, `initdb/061-fix-balance-trigger.sql`
- Token registry schema: `initdb/041-token-registry.sql`
- Zigchain module schema: `initdb/035-zigchain-schema.sql`, `initdb/036-zigchain-tokenwrapper.sql`

---
