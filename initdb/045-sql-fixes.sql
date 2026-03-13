-- initdb/045-sql-fixes.sql
-- Purpose: Tracking indexing progress.
-- FIXED: Table name matches Application (core.indexer_progress)

CREATE SCHEMA IF NOT EXISTS core;

-- 👇 Table Name: 'indexer_progress' (Not just 'progress')
-- 👇 Column Name: 'last_height' (Not just 'height')
CREATE TABLE IF NOT EXISTS core.indexer_progress (
    id          TEXT PRIMARY KEY,  -- e.g. 'zigchain-mainnet-v2'
    last_height BIGINT NOT NULL,   
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Optional: View for easier querying
CREATE OR REPLACE VIEW public.indexer_progress AS SELECT * FROM core.indexer_progress;
