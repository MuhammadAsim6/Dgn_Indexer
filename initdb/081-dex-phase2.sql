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
    created_at           TIMESTAMPTZ NOT NULL,
    price_in_quote       NUMERIC(78,18),
    price_in_zig         NUMERIC(78,18),
    price_in_usd         NUMERIC(78,18),
    value_in_zig         NUMERIC(78,18),
    value_in_usd         NUMERIC(78,18),
    PRIMARY KEY (trade_id, created_at),
    -- Unique constraint for application deduplication
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
-- 3. dex.token_holders_stats — Aggregated Holder Counts
-- Updated by matrix-roller job from bank.balances_current.
-- ============================================================

CREATE TABLE IF NOT EXISTS dex.token_holders_stats (
    token_id       BIGINT      PRIMARY KEY REFERENCES tokens.registry(token_id),
    holders_count  BIGINT      NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 4. TRIGGER: dex.trades → dex.pool_state
-- Updates last price and height for pools with new swap trades.
-- ============================================================

CREATE OR REPLACE FUNCTION dex.refresh_pool_state()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO dex.pool_state (pool_id, last_price, last_trade_height, updated_at)
    SELECT DISTINCT ON (n.pool_id)
        n.pool_id, n.price_in_quote, n.height, NOW()
    FROM inserted_rows n
    WHERE n.pool_id IS NOT NULL AND n.action = 'swap'
    ORDER BY n.pool_id, n.height DESC, n.created_at DESC, n.trade_id DESC
    ON CONFLICT (pool_id) DO UPDATE SET
        last_price        = COALESCE(EXCLUDED.last_price, dex.pool_state.last_price),
        last_trade_height = GREATEST(EXCLUDED.last_trade_height, dex.pool_state.last_trade_height),
        updated_at        = NOW();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_trades_to_pool_state ON dex.trades;
CREATE TRIGGER trg_trades_to_pool_state
AFTER INSERT ON dex.trades
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT EXECUTE FUNCTION dex.refresh_pool_state();
