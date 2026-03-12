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
-- 3. dex.exchange_rates — ZIG/USD rate from oracle (Hypertable)
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.exchange_rates (
    ts      TIMESTAMPTZ NOT NULL PRIMARY KEY,
    zig_usd NUMERIC(38,8) NOT NULL
);

SELECT create_hypertable('dex.exchange_rates', by_range('ts'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_dex_exchange_rates_ts ON dex.exchange_rates(ts DESC);

-- ============================================================
-- 4. dex.external_prices — CMC/CoinGecko per-token prices
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.external_prices (
    token_id   BIGINT NOT NULL REFERENCES tokens.registry(token_id),
    source     TEXT   NOT NULL,  -- 'cmc', 'coingecko'
    price_usd  NUMERIC(38,18),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_id, source)
);

-- ============================================================
-- 5. dex.ohlcv_1m — 1-minute OHLCV candles (Continuous Aggregate)
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS dex.ohlcv_1m 
WITH (timescaledb.continuous) AS
SELECT
    pool_id,
    time_bucket('1 minute', created_at) AS bucket_start,
    first(price_in_zig, created_at)   AS open,
    max(price_in_zig)                 AS high,
    min(price_in_zig)                 AS low,
    last(price_in_zig, created_at)    AS close,
    sum(offer_amount_base)              AS volume_base,
    sum(return_amount_base)             AS volume_quote,
    count(*)                            AS trade_count
FROM dex.trades
WHERE action = 'swap' AND price_in_zig IS NOT NULL
GROUP BY pool_id, time_bucket('1 minute', created_at);

-- Refresh policy: refresh the last 2 hours of data every 1 minute
SELECT add_continuous_aggregate_policy('dex.ohlcv_1m',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

-- ============================================================
-- 6. dex.ohlcv_1m_usd — USD-denominated OHLCV view
-- ============================================================
CREATE OR REPLACE VIEW dex.ohlcv_1m_usd AS
SELECT
    o.*,
    o.open  * er.zig_usd AS open_usd,
    o.high  * er.zig_usd AS high_usd,
    o.low   * er.zig_usd AS low_usd,
    o.close * er.zig_usd AS close_usd,
    o.volume_base * er.zig_usd AS volume_usd
FROM dex.ohlcv_1m o
LEFT JOIN LATERAL (
    SELECT zig_usd FROM dex.exchange_rates
    WHERE ts <= o.bucket_start ORDER BY ts DESC LIMIT 1
) er ON TRUE;


