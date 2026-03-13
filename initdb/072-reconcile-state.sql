-- initdb/072-reconcile-state.sql
-- Purpose: Persist full-reconcile completion per logical state ID.
-- This prevents "full-once-then-negative" from running full reconciliation after every restart.

CREATE TABLE IF NOT EXISTS core.reconcile_state (
    state_id          TEXT PRIMARY KEY,
    full_done         BOOLEAN NOT NULL DEFAULT FALSE,
    full_height       BIGINT NULL,
    full_completed_at TIMESTAMPTZ NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
