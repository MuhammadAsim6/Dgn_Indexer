-- initdb/035-zigchain-schema.sql
-- ZIGCHAIN MODULES: Factory, DEX (Swaps + Liquidity), TokenWrapper.
-- REVISED: Fixed Primary Keys for Partitioning (Added block_height to PKs)

CREATE SCHEMA IF NOT EXISTS zigchain;

-- ============================================================================
-- 1. FACTORY MODULE (Token Creation)
-- ============================================================================
CREATE TABLE IF NOT EXISTS zigchain.factory_denoms (
    denom             TEXT PRIMARY KEY,
    creator_address   TEXT NOT NULL,
    sub_denom         TEXT,
    minting_cap       TEXT, 
    uri               TEXT,
    uri_hash          TEXT,
    description       TEXT,
    creation_tx_hash  TEXT NOT NULL,
    block_height      BIGINT NOT NULL,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_factory_creator ON zigchain.factory_denoms(creator_address);
CREATE INDEX IF NOT EXISTS idx_factory_height ON zigchain.factory_denoms(block_height);


-- ============================================================================
-- 2. DEX POOLS (Market Structure)
-- ============================================================================
CREATE TABLE IF NOT EXISTS zigchain.dex_pools (
    pool_id           TEXT PRIMARY KEY,
    creator_address   TEXT NOT NULL,
    pair_id           TEXT,            
    base_denom        TEXT NOT NULL,
    quote_denom       TEXT NOT NULL,
    lp_token_denom    TEXT,
    base_reserve      TEXT DEFAULT '0', 
    quote_reserve     TEXT DEFAULT '0',
    block_height      BIGINT NOT NULL,
    tx_hash           TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dex_pools_creator ON zigchain.dex_pools(creator_address);
CREATE INDEX IF NOT EXISTS idx_dex_pools_assets ON zigchain.dex_pools(base_denom, quote_denom);


-- ============================================================================
-- 3. DEX SWAPS (Trading Volume)
-- ============================================================================
CREATE TABLE IF NOT EXISTS zigchain.dex_swaps (
    tx_hash           TEXT NOT NULL,
    msg_index         INT NOT NULL,
    event_index       INT NOT NULL DEFAULT -1,
    pool_id           TEXT NOT NULL,  -- Removed FK: pool may not exist if indexing from later block
    sender_address    TEXT NOT NULL,
    token_in_denom    TEXT,
    token_in_amount   NUMERIC(80,0), 
    token_out_denom   TEXT,
    token_out_amount  NUMERIC(80,0), 
    -- Analytics columns (Mirrored from wasm.dex_swaps)
    pair_id           TEXT,
    effective_price   NUMERIC,
    total_fee         NUMERIC(80,0) DEFAULT 0,
    fee_denom         TEXT,
    price_impact      TEXT, 
    block_height      BIGINT NOT NULL,
    timestamp         TIMESTAMPTZ DEFAULT NOW(),
    -- Composite Primary Key (tx uniqueness + partition key)
    PRIMARY KEY (tx_hash, msg_index, event_index, block_height)
) PARTITION BY RANGE (block_height);

-- Auto-Partition: 0 to 500k Blocks
CREATE TABLE IF NOT EXISTS zigchain.dex_swaps_p0 PARTITION OF zigchain.dex_swaps FOR VALUES FROM (0) TO (500000);

-- ♻️ IMPROVED: Composite indexes with block_height for sorted queries
CREATE INDEX IF NOT EXISTS idx_dex_swaps_pool_height ON zigchain.dex_swaps(pool_id, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_dex_swaps_sender_height ON zigchain.dex_swaps(sender_address, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_dex_swaps_height ON zigchain.dex_swaps(block_height DESC);


-- ============================================================================
-- 4. DEX LIQUIDITY (Add/Remove Events)
-- ============================================================================
CREATE TABLE IF NOT EXISTS zigchain.dex_liquidity (
    tx_hash           TEXT NOT NULL,
    msg_index         INT NOT NULL,
    pool_id           TEXT NOT NULL,  -- Removed FK: pool may not exist if indexing from later block
    sender_address    TEXT NOT NULL,
    action_type       TEXT NOT NULL, 
    amount_0          TEXT,         
    amount_1          TEXT,          
    shares_minted_burned TEXT,       
    block_height      BIGINT NOT NULL,
    timestamp         TIMESTAMPTZ DEFAULT NOW(),
    -- Composite Primary Key (tx uniqueness + partition key)
    PRIMARY KEY (tx_hash, msg_index, block_height)
) PARTITION BY RANGE (block_height);

CREATE TABLE IF NOT EXISTS zigchain.dex_liquidity_p0 PARTITION OF zigchain.dex_liquidity FOR VALUES FROM (0) TO (500000);

-- ♻️ IMPROVED: Composite indexes with block_height for sorted queries
CREATE INDEX IF NOT EXISTS idx_dex_liq_pool_height ON zigchain.dex_liquidity(pool_id, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_dex_liq_sender_height ON zigchain.dex_liquidity(sender_address, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_dex_liq_height ON zigchain.dex_liquidity(block_height DESC);


-- ============================================================================
-- 5. TOKEN WRAPPER (Bridge/Wrap)
-- ============================================================================
CREATE TABLE IF NOT EXISTS zigchain.wrapper_settings (
    denom              TEXT PRIMARY KEY CHECK (btrim(denom) <> ''),
    native_client_id   TEXT,
    counterparty_client_id TEXT,
    native_port        TEXT,
    counterparty_port  TEXT,
    native_channel     TEXT,
    counterparty_channel TEXT,
    decimal_difference INT CHECK (decimal_difference IS NULL OR decimal_difference >= 0),
    updated_at_height  BIGINT
);
