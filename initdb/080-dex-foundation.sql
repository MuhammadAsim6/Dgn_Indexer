-- initdb/080-dex-foundation.sql
-- DEX Foundation: Registries, Views, Enums
-- Requires: 041-token-registry.sql (tokens.registry with token_id)

CREATE SCHEMA IF NOT EXISTS dex;

-- ============================================================
-- ENUMs (reused by all DEX phases)
-- ============================================================
DO $$ BEGIN CREATE TYPE dex.trade_action AS ENUM ('swap', 'provide_liquidity', 'withdraw_liquidity'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE dex.trade_direction AS ENUM ('buy', 'sell'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE dex.wallet_window AS ENUM ('24h', '7d', '30d', 'all'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ============================================================
-- 1. dex.pools — Liquidity Pool Registry
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.pools (
    pool_id         BIGSERIAL   PRIMARY KEY,
    pair_contract   TEXT        NOT NULL UNIQUE,
    base_token_id   BIGINT      REFERENCES tokens.registry(token_id),
    quote_token_id  BIGINT      REFERENCES tokens.registry(token_id),
    lp_token_denom  TEXT,
    pair_id         TEXT,
    pair_type       TEXT,
    is_uzig_quote   BOOLEAN NOT NULL DEFAULT FALSE,
    signer          TEXT,
    created_height  BIGINT,
    created_tx_hash TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dex_pools_pair_id        ON dex.pools(pair_id);
CREATE INDEX IF NOT EXISTS idx_dex_pools_base_token_id  ON dex.pools(base_token_id);
CREATE INDEX IF NOT EXISTS idx_dex_pools_quote_token_id ON dex.pools(quote_token_id);

-- ============================================================
-- 2. dex.wallets — Wallet Address Registry
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.wallets (
    wallet_id         BIGSERIAL   PRIMARY KEY,
    address           TEXT        NOT NULL UNIQUE,
    first_seen_height BIGINT,
    first_seen_at     TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dex_wallets_address ON dex.wallets(address);

-- ============================================================
-- 3. dex.ibc_tokens — IBC Token Extended Metadata
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.ibc_tokens (
    token_id     BIGINT      PRIMARY KEY REFERENCES tokens.registry(token_id) ON DELETE CASCADE,
    ibc_denom    TEXT        NOT NULL UNIQUE,
    base_denom   TEXT,
    source_chain TEXT,
    channel      TEXT,
    port         TEXT,
    cmc_id       INTEGER,
    coingecko_id TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 4. dex.index_state — View on core.indexer_progress
-- ============================================================
CREATE OR REPLACE VIEW dex.index_state AS
SELECT
    id           AS state_id,
    last_height,
    updated_at
FROM core.indexer_progress;

-- ============================================================
-- 5. dex.holders — VIEW normalizing bank.balances_current JSONB
-- ============================================================
CREATE OR REPLACE VIEW dex.holders AS
SELECT
    tr.token_id,
    bc.account AS address,
    kv.key AS denom,
    kv.value::NUMERIC(80,0) AS balance_base
FROM bank.balances_current bc
CROSS JOIN LATERAL jsonb_each_text(bc.balances) AS kv(key, value)
JOIN tokens.registry tr ON tr.denom = kv.key;

-- ============================================================
-- 6. Helper Views
-- ============================================================
CREATE OR REPLACE VIEW dex.v_token_holder_counts AS
SELECT token_id, COUNT(*) AS holder_count
FROM dex.holders
WHERE balance_base > 0
GROUP BY token_id;
