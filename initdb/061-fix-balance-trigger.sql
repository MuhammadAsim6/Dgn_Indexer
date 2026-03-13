-- initdb/061-fix-balance-trigger.sql
-- FIXED: bank.update_balances_current logic to prevent data loss and support reconciliation

-- 1. Redefine the trigger function
CREATE OR REPLACE FUNCTION bank.update_balances_current()
RETURNS TRIGGER AS $$
BEGIN
    -- Atomic Upsert: Calculate new balance using cleaned input
    INSERT INTO bank.balances_current (account, balances)
    VALUES (
        NEW.account, 
        jsonb_build_object(NEW.denom, NEW.delta::TEXT)
    )
    ON CONFLICT (account) DO UPDATE
    SET balances = bank.balances_current.balances || 
        jsonb_build_object(
            NEW.denom, 
            (
                COALESCE((bank.balances_current.balances->>NEW.denom)::NUMERIC(80,0), 0) + 
                NEW.delta
            )::TEXT
        );

    -- cleanup: Ensure 0 balances are stored explicitly as "0" to preserve audit trail
    -- Replaces previous deletion logic
    UPDATE bank.balances_current
    SET balances = bank.balances_current.balances || jsonb_build_object(NEW.denom, '0')
    WHERE account = NEW.account
      AND (
          COALESCE((balances->>NEW.denom)::NUMERIC(80,0), 0) = 0
      );
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Redefine population function to include 0 balances
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
        -- ✅ REMOVED: HAVING SUM(delta::NUMERIC(80,0)) > 0
        -- We want to keep 0 balances to know the account exists but has 0 of that token
    ) aggregated
    GROUP BY account;
END;
$$ LANGUAGE plpgsql;
