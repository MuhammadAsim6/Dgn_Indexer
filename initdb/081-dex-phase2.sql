-- initdb/081-dex-phase2.sql
-- Phase 2: Trades Pipeline & Holder Tracking
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
    msg_index            INT,
    event_index          INT         DEFAULT -1,
    -- Trade data
    offer_asset_denom    TEXT,
    ask_asset_denom      TEXT,
    offer_amount_base    NUMERIC(78,0),
    return_amount_base   NUMERIC(78,0),
    height               BIGINT      NOT NULL,
    tx_hash              TEXT        NOT NULL,
    signer               TEXT,
    created_at           TIMESTAMPTZ NOT NULL,
    price_in_quote       NUMERIC(38,18),
    price_in_zig         NUMERIC(38,18),               -- Phase 3
    price_in_usd         NUMERIC(38,18),               -- Phase 3
    value_in_zig         NUMERIC(38,8),                 -- Phase 3
    value_in_usd         NUMERIC(38,8),                 -- Phase 3
    UNIQUE (tx_hash, source_kind, msg_index, event_index, created_at),
    PRIMARY KEY (trade_id, created_at)
);

SELECT create_hypertable('dex.trades', by_range('created_at'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_dex_trades_pool   ON dex.trades (pool_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dex_trades_signer ON dex.trades (signer, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dex_trades_height ON dex.trades (height DESC);

-- ============================================================
-- 2. dex.pool_state — Live Pool Metrics (Phase 2: price + height only)
-- Reserves and 24h volume deferred to Phase 3 analytics jobs.
-- ============================================================

CREATE TABLE IF NOT EXISTS dex.pool_state (
    pool_id           BIGINT      PRIMARY KEY REFERENCES dex.pools(pool_id),
    last_price        NUMERIC(38,18),
    last_trade_height BIGINT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 3. dex.holders — Live Token Balances
-- Updated reactively by trigger on bank.balance_deltas.
-- ============================================================

CREATE TABLE IF NOT EXISTS dex.holders (
    token_id         BIGINT      NOT NULL REFERENCES tokens.registry(token_id),
    address          TEXT        NOT NULL,
    balance_base     NUMERIC(78,0) NOT NULL DEFAULT 0,
    last_seen_height BIGINT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_id, address)
);

CREATE INDEX IF NOT EXISTS idx_dex_holders_address ON dex.holders (address);
CREATE INDEX IF NOT EXISTS idx_dex_holders_balance ON dex.holders (token_id, balance_base DESC);

-- ============================================================
-- 4. dex.token_holders_stats — Aggregated Holder Counts
-- Updated reactively by trigger on dex.holders.
-- ============================================================

CREATE TABLE IF NOT EXISTS dex.token_holders_stats (
    token_id       BIGINT      PRIMARY KEY REFERENCES tokens.registry(token_id),
    holders_count  BIGINT      NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 5. dex.holder_changes — Balance Change Audit Trail (Hypertable)
-- Every balance_delta that resolves to a known token gets logged here.
-- ============================================================

CREATE TABLE IF NOT EXISTS dex.holder_changes (
    height       BIGINT         NOT NULL,
    address      TEXT           NOT NULL,
    token_id     BIGINT         REFERENCES tokens.registry(token_id),
    denom        TEXT           NOT NULL,
    delta_base   NUMERIC(78,0)  NOT NULL,
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('dex.holder_changes', by_range('height', 500000), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_holder_changes_addr  ON dex.holder_changes (address, height DESC);
CREATE INDEX IF NOT EXISTS idx_holder_changes_token ON dex.holder_changes (token_id, height DESC);

-- ============================================================
-- 6. Holder Subsystem Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS dex.holder_index_state (
    id          TEXT PRIMARY KEY DEFAULT 'default',
    last_height BIGINT,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dex.holder_tokens (
    token_id      BIGINT PRIMARY KEY REFERENCES tokens.registry(token_id),
    denom         TEXT   NOT NULL,
    tracked_since TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dex.holder_balances (
    token_id BIGINT NOT NULL REFERENCES tokens.registry(token_id),
    address  TEXT   NOT NULL,
    balance  NUMERIC(78,0) NOT NULL DEFAULT 0,
    PRIMARY KEY (token_id, address)
);

CREATE TABLE IF NOT EXISTS dex.holder_token_stats (
    token_id         BIGINT PRIMARY KEY REFERENCES tokens.registry(token_id),
    total_holders    BIGINT DEFAULT 0,
    non_zero_holders BIGINT DEFAULT 0,
    top_10_pct       NUMERIC(10,4),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 7. TRIGGER: bank.balance_deltas → dex.holders + dex.holder_changes
-- Set-based: processes entire transition table in two bulk operations.
-- ============================================================

CREATE OR REPLACE FUNCTION dex.apply_holder_delta()
RETURNS TRIGGER AS $$
BEGIN
    -- 1. Audit trail (bulk insert from transition table)
    INSERT INTO dex.holder_changes (height, address, token_id, denom, delta_base)
    SELECT n.height, n.account, tr.token_id, n.denom, n.delta
    FROM inserted_rows n
    JOIN tokens.registry tr ON tr.denom = n.denom
    WHERE n.delta != 0; -- ✅ Optimization: Skip zero changes

    -- 2. Live balance upsert (set-based, grouped by token+address)
    INSERT INTO dex.holders (token_id, address, balance_base, last_seen_height)
    SELECT tr.token_id, n.account, SUM(n.delta), MAX(n.height)
    FROM inserted_rows n
    JOIN tokens.registry tr ON tr.denom = n.denom
    WHERE n.delta != 0 -- ✅ Optimization: Only update live state for movement
    GROUP BY tr.token_id, n.account
    ON CONFLICT (token_id, address) DO UPDATE SET
        balance_base     = dex.holders.balance_base + EXCLUDED.balance_base,
        last_seen_height = GREATEST(dex.holders.last_seen_height, EXCLUDED.last_seen_height),
        updated_at       = NOW();

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_balance_deltas_to_holders ON bank.balance_deltas;
CREATE TRIGGER trg_balance_deltas_to_holders
AFTER INSERT ON bank.balance_deltas
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT EXECUTE FUNCTION dex.apply_holder_delta();

-- ============================================================
-- 8. TRIGGER: dex.holders → dex.token_holders_stats
-- Recalculates holder count for affected tokens.
-- ============================================================

CREATE OR REPLACE FUNCTION dex.refresh_token_holders_stats()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO dex.token_holders_stats (token_id, holders_count, updated_at)
    SELECT dt.token_id,
           (SELECT count(*) FROM dex.holders WHERE token_id = dt.token_id AND balance_base > 0),
           NOW()
    FROM (SELECT DISTINCT token_id FROM inserted_rows) dt
    ON CONFLICT (token_id) DO UPDATE SET
        holders_count = EXCLUDED.holders_count,
        updated_at    = NOW();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Split into two triggers: PG does not allow REFERENCING with multi-event triggers
DROP TRIGGER IF EXISTS trg_holders_insert_stats ON dex.holders;
CREATE TRIGGER trg_holders_insert_stats
AFTER INSERT ON dex.holders
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT EXECUTE FUNCTION dex.refresh_token_holders_stats();

DROP TRIGGER IF EXISTS trg_holders_update_stats ON dex.holders;
CREATE TRIGGER trg_holders_update_stats
AFTER UPDATE ON dex.holders
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT EXECUTE FUNCTION dex.refresh_token_holders_stats();

-- ============================================================
-- 9. TRIGGER: dex.trades → dex.pool_state
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
