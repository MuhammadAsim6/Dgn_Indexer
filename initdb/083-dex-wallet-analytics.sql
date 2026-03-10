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

-- ============================================================
-- 2. dex.wallet_activities — Denormalized Trade Log per Wallet
--    Hypertable partitioned by trade_created_at for efficient
--    time-range queries (portfolio history, PnL timeline).
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.wallet_activities (
    activity_id      BIGSERIAL    NOT NULL,
    wallet_id        BIGINT       REFERENCES dex.wallets(wallet_id),
    trade_id         BIGINT,
    trade_created_at TIMESTAMPTZ  NOT NULL,
    pool_id          BIGINT,
    action           dex.trade_action,
    direction        dex.trade_direction,
    token_in_id      BIGINT,
    token_out_id     BIGINT,
    amount_in_base   NUMERIC(78,0),
    amount_out_base  NUMERIC(78,0),
    price_in_zig     NUMERIC(78,18),
    price_in_usd     NUMERIC(78,18),
    value_zig        NUMERIC(78,18),
    value_usd        NUMERIC(78,18),
    tx_hash          TEXT         NOT NULL,
    msg_index        INT,
    realized_pnl_zig NUMERIC(78,18),
    realized_pnl_usd NUMERIC(78,18),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (activity_id, trade_created_at)
);

SELECT create_hypertable('dex.wallet_activities', by_range('trade_created_at'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_wallet_activities_wallet ON dex.wallet_activities (wallet_id, trade_created_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_activities_pool   ON dex.wallet_activities (pool_id, trade_created_at DESC);

-- ============================================================
-- 3. dex.wallet_token_positions — Live Cost Basis & PnL
--    Updated incrementally by wallet-roller job.
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.wallet_token_positions (
    wallet_id        BIGINT       NOT NULL REFERENCES dex.wallets(wallet_id),
    token_id         BIGINT       NOT NULL REFERENCES tokens.registry(token_id),
    amount_base      NUMERIC(78,0) DEFAULT 0,
    cost_basis_zig   NUMERIC(78,18),
    cost_basis_usd   NUMERIC(78,18),
    realized_pnl_zig NUMERIC(78,18),
    realized_pnl_usd NUMERIC(78,18),
    first_buy_at     TIMESTAMPTZ,
    last_trade_at    TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (wallet_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_wallet_positions_token ON dex.wallet_token_positions (token_id);

-- ============================================================
-- 4. dex.wallet_stats_window — Aggregated Stats per Wallet
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.wallet_stats_window (
    wallet_id           BIGINT       NOT NULL REFERENCES dex.wallets(wallet_id),
    win                 dex.wallet_window NOT NULL,
    as_of               TIMESTAMPTZ,
    volume_zig          NUMERIC(78,18),
    volume_usd          NUMERIC(78,18),
    tx_count            BIGINT,
    realized_pnl_usd    NUMERIC(78,18),
    win_rate            NUMERIC(10,4),
    portfolio_value_usd NUMERIC(78,18),
    PRIMARY KEY (wallet_id, win)
);

-- ============================================================
-- 5. dex.wallet_token_stats_window — Per-Token Stats per Wallet
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.wallet_token_stats_window (
    wallet_id        BIGINT            NOT NULL,
    token_id         BIGINT            NOT NULL,
    win              dex.wallet_window NOT NULL,
    as_of            TIMESTAMPTZ,
    tx_count         BIGINT,
    volume_usd       NUMERIC(78,18),
    bought_usd       NUMERIC(78,18),
    sold_usd         NUMERIC(78,18),
    realized_pnl_usd NUMERIC(78,18),
    avg_cost_usd     NUMERIC(78,18),
    PRIMARY KEY (wallet_id, token_id, win)
);

-- ============================================================
-- 6. dex.wallet_dirty — Dirty Flag Queue
--    Wallets are marked dirty by the trigger on dex.trades.
--    The wallet-roller job processes and removes entries.
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.wallet_dirty (
    wallet_id      BIGINT PRIMARY KEY REFERENCES dex.wallets(wallet_id),
    last_marked_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 7. dex.wallet_portfolio_snapshots — Daily Portfolio Value
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.wallet_portfolio_snapshots (
    wallet_id BIGINT      NOT NULL REFERENCES dex.wallets(wallet_id),
    ts        TIMESTAMPTZ NOT NULL,
    value_usd NUMERIC(78,18),
    PRIMARY KEY (wallet_id, ts)
);

-- ============================================================
-- 8. dex.leaderboard_traders — Top Traders per Window
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.leaderboard_traders (
    bucket        dex.wallet_window NOT NULL,
    address       TEXT              NOT NULL,
    trades_count  BIGINT            DEFAULT 0,
    volume_zig    NUMERIC(78,18)     DEFAULT 0,
    gross_pnl_zig NUMERIC(78,18)     DEFAULT 0,
    updated_at    TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bucket, address)
);

CREATE INDEX IF NOT EXISTS idx_leaderboard_volume ON dex.leaderboard_traders (bucket, volume_zig DESC);
CREATE INDEX IF NOT EXISTS idx_leaderboard_pnl    ON dex.leaderboard_traders (bucket, gross_pnl_zig DESC);

-- ============================================================
-- 9. dex.large_trades — Whale / Notable Trade Alerts
-- ============================================================
CREATE TABLE IF NOT EXISTS dex.large_trades (
    id          BIGSERIAL   PRIMARY KEY,
    bucket      dex.wallet_window,
    pool_id     BIGINT      REFERENCES dex.pools(pool_id),
    tx_hash     TEXT        NOT NULL,
    signer      TEXT,
    value_zig   NUMERIC(78,18),
    direction   dex.trade_direction,
    created_at  TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_large_trades_ts     ON dex.large_trades (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_large_trades_signer ON dex.large_trades (signer);

-- ============================================================
-- 10. TRIGGER: dex.trades → dex.wallet_dirty
--     Marks wallet as dirty when new trades are inserted,
--     so the wallet-roller job knows which wallets to recalculate.
-- ============================================================

CREATE OR REPLACE FUNCTION dex.mark_wallet_dirty()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO dex.wallet_dirty (wallet_id, last_marked_at)
    SELECT DISTINCT w.wallet_id, NOW()
    FROM inserted_rows n
    JOIN dex.wallets w ON w.address = n.signer
    WHERE n.signer IS NOT NULL
    ON CONFLICT (wallet_id) DO UPDATE SET last_marked_at = NOW();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_trades_mark_wallet_dirty ON dex.trades;
CREATE TRIGGER trg_trades_mark_wallet_dirty
AFTER INSERT ON dex.trades
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT EXECUTE FUNCTION dex.mark_wallet_dirty();
