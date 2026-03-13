-- initdb/011-unknown-messages.sql
-- Quarantine table for unknown/undecoded message types
-- Prevents garbage data from polluting domain tables

CREATE TABLE IF NOT EXISTS core.unknown_messages (
    tx_hash       TEXT NOT NULL,
    msg_index     INT NOT NULL,
    height        BIGINT NOT NULL,
    type_url      TEXT NOT NULL,
    raw_value     TEXT NOT NULL,  -- Base64 encoded binary
    signer        TEXT NULL,
    first_seen    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (height, tx_hash, msg_index)
);

-- Index for finding all occurrences of a specific unknown type
CREATE INDEX IF NOT EXISTS idx_unknown_messages_type ON core.unknown_messages (type_url);
CREATE INDEX IF NOT EXISTS idx_unknown_messages_height ON core.unknown_messages USING BRIN (height);
