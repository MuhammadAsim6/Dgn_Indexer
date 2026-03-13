-- initdb/081-dex-phase2.sql
-- Phase 2: Trades Pipeline & Pool State
-- Requires: 080-dex-foundation.sql (dex schema + enums + pools)
-- Requires: TimescaleDB extension (001-extensions-and-params.sql)

-- ============================================================
-- 1. dex.trades — Unified Trade History (Hypertable)
-- Captures swaps + LP events from both native DEX and WASM AMMs.
-- pool_id resolved from dex.pools(pair_contract) by the inserter.
-- ============================================================

CREATE TABLE IF NOT EXISTS dex.trades (
    trade_id             BIGSERIAL   NOT NULL,
    pool_id              BIGINT      REFERENCES dex.pools(pool_id),
    action               dex.trade_action    NOT NULL,
    direction            dex.trade_direction,          -- NULL for LP events
    -- Source identity (idempotency key)
    source_kind          TEXT        NOT NULL,          -- 'native_swap', 'wasm_swap', 'liquidity'
    msg_index            INT         NOT NULL DEFAULT -1,
    event_index          INT         NOT NULL DEFAULT -1,
    -- Trade data
    offer_asset_denom    TEXT,
    ask_asset_denom      TEXT,
    offer_amount_base    NUMERIC(78,0),
    return_amount_base   NUMERIC(78,0),
    height               BIGINT      NOT NULL,
    tx_hash              TEXT        NOT NULL,
    signer               TEXT,
    memo                 TEXT,
    is_degenter          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMPTZ NOT NULL,
    price_in_quote       NUMERIC(78,18),
    price_in_zig         NUMERIC(78,18),
    price_in_usd         NUMERIC(78,18),
    value_in_zig         NUMERIC(78,18),
    value_in_usd         NUMERIC(78,18),
    PRIMARY KEY (trade_id, created_at),
    -- Unique constraint for deduplication.
    -- TimescaleDB requires created_at (partitioning column) in ALL unique indexes.
    -- This is safe: created_at is derived from block time, so identical trades always get the same timestamp.
    CONSTRAINT dex_trades_upsert_key UNIQUE (tx_hash, source_kind, msg_index, event_index, created_at)
);

SELECT create_hypertable('dex.trades', by_range('created_at'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_dex_trades_pool   ON dex.trades (pool_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dex_trades_signer ON dex.trades (signer, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dex_trades_height ON dex.trades (height DESC);

-- ============================================================
-- 2. dex.pool_state — Live Pool Metrics
-- ============================================================

CREATE TABLE IF NOT EXISTS dex.pool_state (
    pool_id           BIGINT      PRIMARY KEY REFERENCES dex.pools(pool_id),
    last_price        NUMERIC(38,18),
    last_trade_height BIGINT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ============================================================
-- 4. Pool State Updates
-- NOTE: The trigger approach (REFERENCING NEW TABLE) does NOT work on
-- TimescaleDB hypertables. Pool state is updated explicitly from the
-- indexer code after insertDexTrades() completes.
-- ============================================================

-- Drop the broken trigger if it exists from a previous migration
DROP TRIGGER IF EXISTS trg_trades_to_pool_state ON dex.trades;
DROP FUNCTION IF EXISTS dex.refresh_pool_state();
