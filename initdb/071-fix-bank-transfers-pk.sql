-- initdb/071-fix-bank-transfers-pk.sql
-- Purpose: Add event_index to bank.transfers PK to prevent data loss on multi-transfers in same msg
-- NOTE: This migration is now a NO-OP since 010-indexer-schema.sql already includes event_index
--       and token_id in the schema. Kept for compatibility with existing deployments.

DO $$
BEGIN
    -- 1. Add event_index column if missing (only for old DBs that don't have it)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'bank' AND table_name = 'transfers' AND column_name = 'event_index'
    ) THEN
        ALTER TABLE bank.transfers ADD COLUMN event_index INT NOT NULL DEFAULT -1;
    END IF;

    -- 2. Add token_id column if missing (only for old DBs that don't have it)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bank' AND table_name = 'transfers' AND column_name = 'token_id'
    ) THEN
        ALTER TABLE bank.transfers ADD COLUMN token_id BIGINT;
    END IF;

    -- 3. Add token_id to balance_deltas if missing
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bank' AND table_name = 'balance_deltas' AND column_name = 'token_id'
    ) THEN
        ALTER TABLE bank.balance_deltas ADD COLUMN token_id BIGINT;
    END IF;
END $$;
