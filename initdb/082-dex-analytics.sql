-- initdb/082-dex-analytics.sql
-- Phase 3: Analytics & Pricing
-- Requires: 080-dex-foundation.sql, 081-dex-phase2.sql

-- ============================================================
-- 1. dex.prices — Latest price per token/pool
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.prices (
    price_id       BIGSERIAL PRIMARY KEY,
    token_id       BIGINT REFERENCES tokens.registry(token_id),
    pool_id        BIGINT REFERENCES dex.pools(pool_id),
    price_in_zig   NUMERIC(38,18),
    price_in_usd   NUMERIC(38,18),               -- Added for alert-evaluator (Point 2)
    is_pair_native BOOLEAN DEFAULT FALSE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (token_id, pool_id)
);

CREATE INDEX IF NOT EXISTS idx_dex_prices_token ON dex.prices(token_id);
CREATE INDEX IF NOT EXISTS idx_dex_prices_pool  ON dex.prices(pool_id);


-- ============================================================
-- 3. dex.current_prices — Latest Oracle & External Rates
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.current_prices (
    symbol     TEXT PRIMARY KEY, -- 'ZIG_USD', 'CMC_ZIG_USD', etc.
    price      NUMERIC(38,18) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed ZIG_USD with a default value to prevent NULL calculations on fresh start
INSERT INTO dex.current_prices (symbol, price) 
VALUES ('ZIG_USD', 0.1) 
ON CONFLICT (symbol) DO NOTHING;


-- ============================================================
-- 5. dex.ohlcv_1m — 1-minute OHLCV candles (Continuous Aggregate)
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS dex.ohlcv_1m 
WITH (timescaledb.continuous) AS
SELECT
    pool_id,
    time_bucket('1 minute', created_at) AS bucket_start,
    -- ZIG prices
    first(price_in_zig, created_at)   AS open,
    max(price_in_zig)                 AS high,
    min(price_in_zig)                 AS low,
    last(price_in_zig, created_at)    AS close,
    sum(offer_amount_base)              AS volume_base,
    sum(return_amount_base)             AS volume_quote,
    -- USD prices (Denormalized from trades)
    first(price_in_usd, created_at)   AS open_usd,
    max(price_in_usd)                 AS high_usd,
    min(price_in_usd)                 AS low_usd,
    last(price_in_usd, created_at)    AS close_usd,
    sum(value_in_usd)                 AS volume_usd,
    count(*)                            AS trade_count
FROM dex.trades
WHERE action = 'swap' AND price_in_zig IS NOT NULL
GROUP BY pool_id, time_bucket('1 minute', created_at);

-- ⚠️ OHLCV refresh policy is DISABLED during initial backfill.
-- During 7M+ block sync, the 1-minute refresh competes with INSERTs and slows backfill ~30-50%.
--
-- AFTER backfill completes, run these commands manually:
--   CALL refresh_continuous_aggregate('dex.ohlcv_1m', NULL, NOW());
--   SELECT add_continuous_aggregate_policy('dex.ohlcv_1m',
--       start_offset => INTERVAL '2 hours',
--       end_offset => INTERVAL '1 minute',
--       schedule_interval => INTERVAL '1 minute',
--       if_not_exists => TRUE);

-- ============================================================
-- 6. dex.ohlcv_1m_usd — Unified View (Backward Compatibility)
-- ============================================================
CREATE OR REPLACE VIEW dex.ohlcv_1m_usd AS
SELECT * FROM dex.ohlcv_1m;


