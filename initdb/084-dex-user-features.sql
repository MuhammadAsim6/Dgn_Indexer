-- initdb/084-dex-user-features.sql
-- Phase 5: User Features + Security + Social
-- Requires: 080-dex-foundation.sql (dex.wallets, dex.pools, tokens.registry)
-- Requires: 083-dex-wallet-analytics.sql (dex.wallet_profiles)

-- ============================================================
-- 1. dex.watchlist — User Token/Pool Watchlists
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.watchlist (
    id         BIGSERIAL   PRIMARY KEY,
    wallet_id  BIGINT      REFERENCES dex.wallets(wallet_id),
    token_id   BIGINT      REFERENCES tokens.registry(token_id),
    pool_id    BIGINT      REFERENCES dex.pools(pool_id),
    note       TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_watchlist_token UNIQUE (wallet_id, token_id),
    CONSTRAINT uq_watchlist_pool UNIQUE (wallet_id, pool_id)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_wallet ON dex.watchlist (wallet_id);
CREATE INDEX IF NOT EXISTS idx_watchlist_token  ON dex.watchlist (token_id);

-- ============================================================
-- 2. dex.alerts — User-Defined Alert Rules
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.alerts (
    alert_id       BIGSERIAL   PRIMARY KEY,
    wallet_id      BIGINT      REFERENCES dex.wallets(wallet_id),
    alert_type     TEXT        NOT NULL,  -- 'price_above','price_below','volume_spike','whale_trade'
    params         JSONB,                 -- { "token_id": 5, "threshold": 0.01 }
    is_active      BOOLEAN     DEFAULT TRUE,
    throttle_sec   INT         DEFAULT 300,
    last_triggered TIMESTAMPTZ,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_wallet ON dex.alerts (wallet_id);
CREATE INDEX IF NOT EXISTS idx_alerts_active ON dex.alerts (is_active) WHERE is_active = TRUE;

-- ============================================================
-- 3. dex.alert_events — Triggered Alert History
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.alert_events (
    id           BIGSERIAL   PRIMARY KEY,
    alert_id     BIGINT      REFERENCES dex.alerts(alert_id),
    wallet_id    BIGINT,
    kind         TEXT,        -- matches alert_type
    payload      JSONB,       -- { "current_price": 0.012, "threshold": 0.01 }
    triggered_at TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_events_alert  ON dex.alert_events (alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_events_wallet ON dex.alert_events (wallet_id, triggered_at DESC);

-- ============================================================
-- 4. dex.token_twitter — Token Social Media Metadata
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.token_twitter (
    token_id         BIGINT      PRIMARY KEY REFERENCES tokens.registry(token_id),
    handle           TEXT,
    user_id          TEXT,
    profile_url      TEXT,
    name             TEXT,
    is_blue_verified BOOLEAN,
    verified_type    TEXT,
    followers_count  BIGINT,
    following_count  BIGINT,
    statuses_count   BIGINT,
    media_count      BIGINT,
    description      TEXT,
    raw              JSONB,
    last_refreshed   TIMESTAMPTZ,
    last_error       TEXT,
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

-- (IBC holder tracking moved to ClickHouse)

-- ============================================================
-- 7. dex.user_profiles — User Accounts
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.user_profiles (
    user_id      BIGSERIAL   PRIMARY KEY,
    handle       TEXT        UNIQUE,
    display_name TEXT,
    bio          TEXT,
    image_url    TEXT,
    website      TEXT,
    twitter      TEXT,
    telegram     TEXT,
    tags         TEXT[],
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 8. dex.user_wallets — Link Users to Wallet Addresses
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.user_wallets (
    user_id    BIGINT  NOT NULL REFERENCES dex.user_profiles(user_id),
    wallet_id  BIGINT  NOT NULL REFERENCES dex.wallets(wallet_id),
    label      TEXT,
    is_primary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, wallet_id)
);

CREATE INDEX IF NOT EXISTS idx_user_wallets_wallet ON dex.user_wallets (wallet_id);



-- ============================================================
-- 11. Helper View: dex.v_wallet_holdings
--     Easy join of wallets → holdings → token metadata
-- ============================================================
CREATE OR REPLACE VIEW dex.v_wallet_holdings AS
SELECT
    w.wallet_id,
    w.address,
    h.token_id,
    tr.symbol,
    h.balance_base
FROM dex.wallets w
JOIN dex.holders h ON h.address = w.address
JOIN tokens.registry tr ON tr.token_id = h.token_id;
