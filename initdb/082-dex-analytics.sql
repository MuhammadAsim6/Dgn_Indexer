-- initdb/082-dex-analytics.sql
-- Phase 3: Analytics & Pricing (PostgreSQL 18 — no TimescaleDB)
-- Requires: 080-dex-foundation.sql, 081-dex-trades.sql

-- ============================================================
-- 1. dex.prices — Latest price per token/pool
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.prices (
    price_id       BIGSERIAL PRIMARY KEY,
    token_id       BIGINT REFERENCES tokens.registry(token_id),
    pool_id        BIGINT REFERENCES dex.pools(pool_id),
    price_in_zig   NUMERIC(38,18),
    is_pair_native BOOLEAN DEFAULT FALSE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (token_id, pool_id)
);

CREATE INDEX IF NOT EXISTS idx_dex_prices_token ON dex.prices(token_id);
CREATE INDEX IF NOT EXISTS idx_dex_prices_pool  ON dex.prices(pool_id);

-- ============================================================
-- 2. dex.price_ticks — Historical price snapshots
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.price_ticks (
    pool_id      BIGINT NOT NULL,
    token_id     BIGINT NOT NULL,
    price_in_zig NUMERIC(38,18),
    ts           TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pool_id, token_id, ts)
);

CREATE INDEX IF NOT EXISTS idx_dex_price_ticks_ts ON dex.price_ticks(ts DESC);
CREATE INDEX IF NOT EXISTS idx_dex_price_ticks_token ON dex.price_ticks(token_id, ts DESC);

-- ============================================================
-- 3. dex.exchange_rates — ZIG/USD rate from oracle
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.exchange_rates (
    ts      TIMESTAMPTZ NOT NULL PRIMARY KEY,
    zig_usd NUMERIC(38,8) NOT NULL
);

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
-- 5. dex.ohlcv_1m — 1-minute OHLCV candles (MATERIALIZED VIEW)
--    Refreshed periodically by job, NOT a continuous aggregate.
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS dex.ohlcv_1m AS
SELECT
    pool_id,
    date_trunc('minute', created_at)    AS bucket_start,
    (ARRAY_AGG(price_in_zig ORDER BY created_at ASC))[1]  AS open,
    MAX(price_in_zig)                 AS high,
    MIN(price_in_zig)                 AS low,
    (ARRAY_AGG(price_in_zig ORDER BY created_at DESC))[1] AS close,
    SUM(offer_amount_base)              AS volume_base,
    SUM(return_amount_base)             AS volume_quote,
    COUNT(*)                            AS trade_count
FROM dex.trades
WHERE action = 'swap' AND price_in_zig IS NOT NULL
GROUP BY pool_id, date_trunc('minute', created_at)
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_1m_pk ON dex.ohlcv_1m(pool_id, bucket_start);
CREATE INDEX IF NOT EXISTS idx_ohlcv_1m_bucket   ON dex.ohlcv_1m(bucket_start DESC);

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

-- ============================================================
-- 7. dex.pool_matrix — Aggregated pool metrics per window
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.pool_matrix (
    pool_id      BIGINT NOT NULL REFERENCES dex.pools(pool_id),
    bucket       dex.wallet_window NOT NULL,
    vol_buy_zig  NUMERIC(38,8) DEFAULT 0,
    vol_sell_zig NUMERIC(38,8) DEFAULT 0,
    tx_buy       BIGINT DEFAULT 0,
    tx_sell      BIGINT DEFAULT 0,
    tvl_zig      NUMERIC(38,8) DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pool_id, bucket)
);

-- ============================================================
-- 8. dex.token_matrix — Aggregated token metrics per window
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.token_matrix (
    token_id     BIGINT NOT NULL REFERENCES tokens.registry(token_id),
    bucket       dex.wallet_window NOT NULL,
    price_in_zig NUMERIC(38,18),
    mcap_zig     NUMERIC(38,8),
    fdv_zig      NUMERIC(38,8),
    holders      BIGINT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_id, bucket)
);
