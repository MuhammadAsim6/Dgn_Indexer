CREATE TABLE IF NOT EXISTS tokens.factory_supply_current (
    denom              TEXT PRIMARY KEY,
    total_minted       NUMERIC(80, 0) NOT NULL DEFAULT 0,
    total_burned       NUMERIC(80, 0) NOT NULL DEFAULT 0,
    net_supply         NUMERIC(80, 0) NOT NULL DEFAULT 0,
    updated_at_height  BIGINT         NOT NULL,
    updated_at_tx_hash TEXT           NULL
);

CREATE OR REPLACE FUNCTION tokens.update_supply_current()
RETURNS TRIGGER AS $$
DECLARE
    minted   NUMERIC(80, 0) := 0;
    burned   NUMERIC(80, 0) := 0;
    safe_amount NUMERIC(80, 0) := COALESCE(NEW.amount, 0);
BEGIN
    -- Determine delta based on action
    IF NEW.action = 'mint' THEN
        minted := safe_amount;
    ELSIF NEW.action = 'burn' THEN
        burned := safe_amount;
    ELSE
        -- Ignore other actions like set_metadata for supply calculation
        RETURN NEW;
    END IF;
    
    INSERT INTO tokens.factory_supply_current 
        (denom, total_minted, total_burned, net_supply, updated_at_height, updated_at_tx_hash)
    VALUES (NEW.denom, minted, burned, minted - burned, NEW.height, NEW.tx_hash)
    ON CONFLICT (denom) DO UPDATE
    SET total_minted = tokens.factory_supply_current.total_minted + minted,
        total_burned = tokens.factory_supply_current.total_burned + burned,
        net_supply = tokens.factory_supply_current.net_supply + (minted - burned),
        updated_at_height = GREATEST(tokens.factory_supply_current.updated_at_height, NEW.height),
        updated_at_tx_hash = CASE
            WHEN NEW.height >= tokens.factory_supply_current.updated_at_height THEN NEW.tx_hash
            ELSE tokens.factory_supply_current.updated_at_tx_hash
        END;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_factory_supply_current ON tokens.factory_supply_events;

CREATE TRIGGER trg_factory_supply_current
AFTER INSERT ON tokens.factory_supply_events
FOR EACH ROW EXECUTE FUNCTION tokens.update_supply_current();
