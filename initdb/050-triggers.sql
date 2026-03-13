-- initdb/050-triggers.sql
-- Triggers for maintaining current state tables from event tables

-- ============================================================================
-- 1. BANK BALANCES CURRENT - Aggregate from balance_deltas
--    Uses FOR EACH STATEMENT with transition tables for performance.
--    Passes raw deltas through EXCLUDED and computes final balances in the
--    ON CONFLICT clause for concurrency safety.
-- ============================================================================

CREATE OR REPLACE FUNCTION bank.update_balances_current_batch()
RETURNS TRIGGER AS $$
BEGIN
    -- Step 1: Aggregate all inserted deltas by (account, denom)
    -- Step 2: Build per-account JSONB of RAW DELTAS (not final values)
    -- Step 3: Upsert — INSERT path stores delta as initial balance (correct for new accounts)
    --                   ON CONFLICT path adds delta to current balance using row-lock-protected read

    WITH aggregated AS (
        SELECT account, denom, SUM(delta) AS total_delta
        FROM inserted_rows
        GROUP BY account, denom
    ),
    per_account_deltas AS (
        SELECT
            account,
            jsonb_object_agg(denom, total_delta::TEXT) AS delta_balances
        FROM aggregated
        GROUP BY account
    )
    INSERT INTO bank.balances_current (account, balances)
    SELECT account, delta_balances
    FROM per_account_deltas
    ON CONFLICT (account) DO UPDATE
    SET balances = (
        SELECT jsonb_object_agg(key, new_value)
        FROM (
            -- Denoms in this batch: add delta to current balance
            SELECT
                key,
                (COALESCE((bank.balances_current.balances->>key)::NUMERIC(80,0), 0)
                 + value::NUMERIC(80,0))::TEXT AS new_value
            FROM jsonb_each_text(EXCLUDED.balances)
            UNION ALL
            -- Denoms NOT in this batch: preserve existing balance
            SELECT key, value AS new_value
            FROM jsonb_each_text(bank.balances_current.balances)
            WHERE key NOT IN (SELECT key FROM jsonb_each_text(EXCLUDED.balances))
        ) merged
    );

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if exists
DROP TRIGGER IF EXISTS trg_balance_deltas_current ON bank.balance_deltas;

-- Statement-level trigger with transition table (PostgreSQL 11+)
CREATE TRIGGER trg_balance_deltas_current
AFTER INSERT ON bank.balance_deltas
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT EXECUTE FUNCTION bank.update_balances_current_batch();


-- ============================================================================
-- 3. INITIAL POPULATION (Run once for existing data)
-- ============================================================================

-- Populate balances_current from historical balance_deltas
CREATE OR REPLACE FUNCTION bank.populate_balances_current()
RETURNS void AS $$
BEGIN
    TRUNCATE bank.balances_current;
    
    INSERT INTO bank.balances_current (account, balances)
    SELECT 
        account,
        jsonb_object_agg(denom, total::TEXT)
    FROM (
        SELECT account, denom, SUM(delta::NUMERIC(80,0)) as total
        FROM bank.balance_deltas
        GROUP BY account, denom
    ) aggregated
    GROUP BY account;
END;
$$ LANGUAGE plpgsql;

-- Run initial population
SELECT bank.populate_balances_current();
