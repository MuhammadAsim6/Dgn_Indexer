-- Token Factory Supply Events Tracking
-- Tracks factory messages and module-level supply events (coinbase/burn)

CREATE TABLE IF NOT EXISTS tokens.factory_supply_events (
    height       BIGINT NOT NULL,
    tx_hash      TEXT NOT NULL,
    msg_index    INT NOT NULL,
    event_index  INT NOT NULL DEFAULT -1,
    denom        TEXT NOT NULL,
    action       TEXT NOT NULL, -- 'mint', 'burn', 'set_metadata'
    amount       NUMERIC(80,0),
    sender       TEXT,
    recipient    TEXT,
    metadata     JSONB,
    CONSTRAINT chk_factory_supply_action
        CHECK (action IN ('mint', 'burn', 'set_metadata')),
    CONSTRAINT chk_factory_supply_amount
        CHECK (
            (action IN ('mint', 'burn') AND amount IS NOT NULL AND amount >= 0)
            OR (action = 'set_metadata' AND amount IS NULL)
        ),
    CONSTRAINT chk_factory_supply_denom_non_empty
        CHECK (length(trim(denom)) > 0),
    PRIMARY KEY (height, tx_hash, msg_index, event_index, denom, action)
) PARTITION BY RANGE (height);

CREATE TABLE IF NOT EXISTS tokens.factory_supply_events_p0 PARTITION OF tokens.factory_supply_events 
    FOR VALUES FROM (0) TO (500000);

-- Index for searching supply history of a specific token
CREATE INDEX IF NOT EXISTS idx_factory_supply_denom ON tokens.factory_supply_events (denom);
-- ♻️ IMPROVED: action alone has only 3 values — composite with denom is selective
CREATE INDEX IF NOT EXISTS idx_factory_supply_action_denom ON tokens.factory_supply_events (action, denom);







