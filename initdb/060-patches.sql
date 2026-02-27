-- 060-patches.sql
-- Purpose: Additional performance indexes and autovacuum settings.
-- Updated: 2026-02-20 — Replaced weak indexes with composite versions,
--          added critical missing indexes.

-- TRANSACTIONS: Optimization for success/failure queries + signers GIN
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'transactions') THEN
    CREATE INDEX IF NOT EXISTS idx_txs_success ON core.transactions (height DESC, tx_index) WHERE code = 0;
    CREATE INDEX IF NOT EXISTS idx_txs_time ON core.transactions (time DESC);
    -- 🔴 CRITICAL: GIN index for '$1 = ANY(signers)' queries
    CREATE INDEX IF NOT EXISTS idx_txs_signers_gin ON core.transactions USING GIN (signers);
  END IF;
END $$;

-- EVENTS: Composite indexes for event lookups
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'events') THEN
    -- ♻️ REPLACED: (event_type, msg_index) → (event_type, height DESC) for range scans
    CREATE INDEX IF NOT EXISTS idx_events_type_height ON core.events (event_type, height DESC);
    CREATE INDEX IF NOT EXISTS idx_events_attrs_gin ON core.events USING GIN (attributes);
    -- 🔴 HIGH: Lookup events by tx_hash
    CREATE INDEX IF NOT EXISTS idx_events_tx_hash ON core.events (tx_hash, height DESC);
  END IF;
END $$;

-- EVENT_ATTRS: Composite index for key-based lookups
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_attrs') THEN
    -- ♻️ REPLACED: (key) → (key, height DESC) for range scans
    CREATE INDEX IF NOT EXISTS idx_event_attrs_key_height ON core.event_attrs (key, height DESC);
  END IF;
END $$;




-- WASM: Contract lookup indexes
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'wasm' AND table_name = 'contracts') THEN
    CREATE INDEX IF NOT EXISTS idx_wasm_contracts_creator ON wasm.contracts (creator);
    CREATE INDEX IF NOT EXISTS idx_wasm_contracts_code_id ON wasm.contracts (code_id);
    -- 🟠 MEDIUM: Fuzzy label search (pg_trgm already enabled)
    CREATE INDEX IF NOT EXISTS idx_wasm_contracts_label_trgm ON wasm.contracts USING GIN (label gin_trgm_ops);
  END IF;
END $$;

-- ============================================================================
-- AUTOVACUUM: Aggressive settings for high-update tables
-- ============================================================================

-- bank.balances_current: Trigger causes frequent updates
ALTER TABLE bank.balances_current SET (
    autovacuum_vacuum_scale_factor = 0.05,  -- Vacuum after 5% dead rows (default: 20%)
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_vacuum_threshold = 50        -- Minimum 50 dead rows to trigger
);

-- core.indexer_progress: Updated every flush
ALTER TABLE core.indexer_progress SET (
    autovacuum_vacuum_scale_factor = 0.01,  -- Very aggressive
    autovacuum_vacuum_threshold = 10
);

-- core.validators: Updated on startup and runtime
ALTER TABLE core.validators SET (
    autovacuum_vacuum_scale_factor = 0.1,
    autovacuum_vacuum_threshold = 5
);
