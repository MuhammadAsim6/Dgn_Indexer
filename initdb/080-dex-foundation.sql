-- initdb/080-dex-foundation.sql
-- Phase 1: DEX Foundation — Registries & Identifiers
-- Runs automatically on fresh container start.

CREATE SCHEMA IF NOT EXISTS dex;

-- ============================================================
-- ENUMs (reused by Phases 2-5)
-- ============================================================
-- Wrapped in DO blocks so re-running the migration is safe.
DO $$ BEGIN CREATE TYPE dex.token_type AS ENUM ('native', 'factory', 'ibc', 'cw20'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE dex.trade_action AS ENUM ('swap', 'provide_liquidity', 'withdraw_liquidity'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE dex.trade_direction AS ENUM ('buy', 'sell'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE dex.wallet_window AS ENUM ('24h', '7d', '30d', 'all'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ============================================================
-- 1. dex.pools — Liquidity Pool Registry
-- FKs point to tokens.registry(token_id) — the BIGSERIAL column added in 041
-- Populated live by MsgCreatePool + WASM swap discovery in postgres.ts
-- ============================================================

CREATE TABLE IF NOT EXISTS dex.pools (
    pool_id         BIGSERIAL   PRIMARY KEY,
    pair_contract   TEXT        NOT NULL UNIQUE, -- zigchain pool_id or WASM contract addr
    base_token_id   BIGINT      REFERENCES tokens.registry(token_id),
    quote_token_id  BIGINT      REFERENCES tokens.registry(token_id),
    lp_token_denom  TEXT,
    pair_id         TEXT,                         -- sorted "denomA-denomB"
    pair_type       TEXT,
    is_uzig_quote   BOOLEAN GENERATED ALWAYS AS (pair_id LIKE '%uzig%') STORED,
    signer          TEXT,                         -- pool creator address
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
-- Populated live from tx signers + transfer endpoints in postgres.ts
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
-- FK points to tokens.registry(token_id)
-- Populated live by ibc denom registration in postgres.ts
-- ============================================================

CREATE TABLE IF NOT EXISTS dex.ibc_tokens (
    token_id     BIGINT      PRIMARY KEY REFERENCES tokens.registry(token_id) ON DELETE CASCADE,
    ibc_denom    TEXT        NOT NULL UNIQUE, -- ibc/HASH
    base_denom   TEXT,                         -- original denom on source chain
    source_chain TEXT,
    channel      TEXT,
    port         TEXT,
    cmc_id       INTEGER,                      -- CoinMarketCap ID (filled later)
    coingecko_id TEXT,                         -- CoinGecko ID (filled later)
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 5. dex.index_state — Read-only view into indexer progress
-- ============================================================

CREATE OR REPLACE VIEW dex.index_state AS
SELECT
    id           AS state_id,
    last_height,
    updated_at
FROM core.indexer_progress;
