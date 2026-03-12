-- initdb/083-dex-wallet-analytics.sql
-- Phase 4: Wallet Analytics — PnL, Leaderboard, Portfolio Tracking
-- Requires: 080-dex-foundation.sql (dex.wallets, dex.pools, enums)
-- Requires: 081-dex-phase2.sql (dex.trades)

-- ============================================================
-- 1. dex.wallet_profiles — Social & Classification Metadata
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.wallet_profiles (
    wallet_id  BIGINT PRIMARY KEY REFERENCES dex.wallets(wallet_id),
    bio        TEXT,
    twitter    TEXT,
    telegram   TEXT,
    website    TEXT,
    tags       TEXT[],
    is_cex     BOOLEAN DEFAULT FALSE,
    is_bot     BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

