-- initdb/038-wasm-dex-indexing.sql
-- WASM DEX Indexing for Zigchain
-- Tracks detailed swap data from CosmWasm AMM contracts

-- ============================================================================
-- 1. WASM SWAPS (Primary: CosmWasm DEX Activity)
-- ============================================================================
CREATE TABLE IF NOT EXISTS wasm.dex_swaps (
    tx_hash           TEXT NOT NULL,
    msg_index         INT NOT NULL,
    event_index       INT NOT NULL,
    contract          TEXT NOT NULL,    -- Pool contract address
    sender            TEXT NOT NULL,
    receiver          TEXT,
    offer_asset       TEXT,             -- e.g., "uzig"
    ask_asset         TEXT,             -- e.g., "coin.zig109f7g2...stzig"
    offer_amount      NUMERIC(80,0),    -- Changed: Use NUMERIC for analytics
    return_amount     NUMERIC(80,0),
    spread_amount     NUMERIC(80,0),
    commission_amount NUMERIC(80,0),
    maker_fee_amount  NUMERIC(80,0),
    fee_share_amount  NUMERIC(80,0),
    reserves          JSONB,            -- Pool reserves after swap (JSON)
    -- NEW: Analytics columns
    pair_id           TEXT,             -- Sorted pair identifier (e.g., "uzig-stzig")
    effective_price   NUMERIC(40,18),   -- return_amount / offer_amount
    price_impact      NUMERIC(40,18),   -- (spread_amount / offer_amount) * 100
    total_fee         NUMERIC(80,0),    -- commission + maker_fee + fee_share
    fee_denom         TEXT,
    block_height      BIGINT NOT NULL,
    timestamp         TIMESTAMPTZ,
    PRIMARY KEY (tx_hash, msg_index, event_index, block_height)

) PARTITION BY RANGE (block_height);

CREATE TABLE IF NOT EXISTS wasm.dex_swaps_p0 PARTITION OF wasm.dex_swaps 
    FOR VALUES FROM (0) TO (500000);
-- Additional partitions created automatically by util.ensure_partition_for_height()

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_contract ON wasm.dex_swaps (contract, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_sender ON wasm.dex_swaps (sender, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_offer ON wasm.dex_swaps (offer_asset, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_ask ON wasm.dex_swaps (ask_asset, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_height ON wasm.dex_swaps USING BRIN (block_height);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_pair ON wasm.dex_swaps (pair_id, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_wasm_swaps_timestamp ON wasm.dex_swaps (timestamp DESC);



-- ============================================================================
-- 4. HELPER: Update Pool Reserves (Trigger for dex_pools)
-- ============================================================================
CREATE OR REPLACE FUNCTION zigchain.update_pool_reserves()
RETURNS TRIGGER AS $$
BEGIN
    -- Parse reserves format: "denom1:amount1,denom2:amount2"
    -- Update dex_pools with latest reserves if contract matches a pool
    -- This is a placeholder for future implementation
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- COMMENT: Can enable trigger later for real-time pool tracking
-- CREATE TRIGGER trg_update_reserves AFTER INSERT ON wasm.dex_swaps
--     FOR EACH ROW EXECUTE FUNCTION zigchain.update_pool_reserves();
