-- TokenWrapper Events Tracking
-- Tracks MsgFundModuleWallet, MsgUpdateIbcSettings, and related bridge activities

CREATE TABLE IF NOT EXISTS zigchain.wrapper_events (
    height       BIGINT NOT NULL,
    tx_hash      TEXT NOT NULL,
    msg_index    INT NOT NULL,
    event_index  INT NOT NULL DEFAULT -1,
    sender       TEXT NOT NULL,
    action       TEXT NOT NULL, -- 'fund_module', 'update_ibc_settings', etc.
    amount       NUMERIC(80,0),
    denom        TEXT,
    metadata     JSONB, -- For extra details like channel/port settings
    PRIMARY KEY (height, tx_hash, msg_index, event_index, action)
) PARTITION BY RANGE (height);

CREATE TABLE IF NOT EXISTS zigchain.wrapper_events_p0 PARTITION OF zigchain.wrapper_events 
    FOR VALUES FROM (0) TO (500000);

CREATE INDEX IF NOT EXISTS idx_wrapper_events_sender ON zigchain.wrapper_events (sender);
CREATE INDEX IF NOT EXISTS idx_wrapper_events_denom ON zigchain.wrapper_events (denom);
