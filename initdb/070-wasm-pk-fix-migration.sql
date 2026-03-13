-- initdb/070-wasm-pk-fix-migration.sql
-- Legacy migration kept for backward compatibility.
-- Safe no-op when core.event_attrs table is not present.

DO $$
BEGIN
    -- If the table is gone (DEX-only mode), skip this migration.
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'event_attrs'
    ) THEN
        RAISE NOTICE 'Skipping 070 migration: core.event_attrs does not exist.';
        RETURN;
    END IF;

    -- Legacy path: table exists but old schema missing attr_index.
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'event_attrs' AND column_name = 'attr_index'
    ) THEN
        ALTER TABLE core.event_attrs ADD COLUMN attr_index INT;
        UPDATE core.event_attrs SET attr_index = 0 WHERE attr_index IS NULL;
        ALTER TABLE core.event_attrs ALTER COLUMN attr_index SET NOT NULL;

        RAISE NOTICE 'Added attr_index to core.event_attrs. Ensure PK includes attr_index.';
    END IF;
END $$;
