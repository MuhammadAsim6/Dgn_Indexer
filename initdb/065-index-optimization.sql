-- 065-index-optimization.sql
-- Supplemental index creation for fresh and existing databases.
-- Safe to run multiple times (all statements are idempotent).
--
-- For EXISTING production databases, you can also run this manually:
--   docker exec -i $(docker ps -qf name=db) psql -U cosmos_indexer_user -d cosmos_indexer_db -f /path/to/065-index-optimization.sql

-- ============================================================================
-- STEP 1: DROP old/weak/redundant indexes (safe if they don't exist)
-- ============================================================================

-- Redundant: subsumed by idx_ibc_channel (channel_id_src, status)
DROP INDEX IF EXISTS ibc.idx_ibc_packets_channel;

-- Weak: replaced by composite versions in other init scripts
DROP INDEX IF EXISTS core.idx_events_type_msg;
DROP INDEX IF EXISTS core.idx_event_attrs_key;
DROP INDEX IF EXISTS zigchain.idx_dex_swaps_pool;
DROP INDEX IF EXISTS zigchain.idx_dex_swaps_sender;
DROP INDEX IF EXISTS zigchain.idx_dex_liq_pool;
DROP INDEX IF EXISTS zigchain.idx_dex_liq_sender;
DROP INDEX IF EXISTS tokens.idx_factory_supply_action;


-- ============================================================================
-- STEP 2: CREATE improved composite replacements
-- ============================================================================

-- core.events: (event_type, height DESC) replaces (event_type, msg_index)
CREATE INDEX IF NOT EXISTS idx_events_type_height
  ON core.events (event_type, height DESC);

-- core.events: lookup by tx_hash
CREATE INDEX IF NOT EXISTS idx_events_tx_hash
  ON core.events (tx_hash, height DESC);

-- core.event_attrs: (key, height DESC) replaces (key)
CREATE INDEX IF NOT EXISTS idx_event_attrs_key_height
  ON core.event_attrs (key, height DESC);

-- zigchain.dex_swaps: composite replacements
CREATE INDEX IF NOT EXISTS idx_dex_swaps_pool_height
  ON zigchain.dex_swaps (pool_id, block_height DESC);

CREATE INDEX IF NOT EXISTS idx_dex_swaps_sender_height
  ON zigchain.dex_swaps (sender_address, block_height DESC);

-- zigchain.dex_liquidity: composite replacements + height index
CREATE INDEX IF NOT EXISTS idx_dex_liq_pool_height
  ON zigchain.dex_liquidity (pool_id, block_height DESC);

CREATE INDEX IF NOT EXISTS idx_dex_liq_sender_height
  ON zigchain.dex_liquidity (sender_address, block_height DESC);

CREATE INDEX IF NOT EXISTS idx_dex_liq_height
  ON zigchain.dex_liquidity (block_height DESC);

-- tokens.factory_supply_events: (action, denom) replaces (action)
CREATE INDEX IF NOT EXISTS idx_factory_supply_action_denom
  ON tokens.factory_supply_events (action, denom);


-- ============================================================================
-- STEP 3: CREATE new missing indexes
-- ============================================================================

-- 🔴 CRITICAL: GIN for signers array lookups
CREATE INDEX IF NOT EXISTS idx_txs_signers_gin
  ON core.transactions USING GIN (signers);




-- 🟠 MEDIUM: WASM contract lookups
CREATE INDEX IF NOT EXISTS idx_wasm_contracts_creator
  ON wasm.contracts (creator);

CREATE INDEX IF NOT EXISTS idx_wasm_contracts_code_id
  ON wasm.contracts (code_id);

-- 🟠 MEDIUM: Fuzzy label search (requires pg_trgm extension — already enabled)
CREATE INDEX IF NOT EXISTS idx_wasm_contracts_label_trgm
  ON wasm.contracts USING GIN (label gin_trgm_ops);


-- ============================================================================
-- DONE. Verify with:
--   SELECT indexname, tablename FROM pg_indexes
--   WHERE schemaname IN ('core','bank','stake','ibc','wasm','zigchain','tokens')
--   ORDER BY schemaname, tablename, indexname;
-- ============================================================================
