-- 001-extensions-and-params.sql
-- Runs automatically on first cluster init.

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ==================================================================
-- AUTO-PARTITION HELPER FUNCTION (New Addition)
-- Iska kaam hai automatically partitions banana (e.g., blocks_0_100000)
-- ==================================================================
CREATE SCHEMA IF NOT EXISTS partman;

CREATE OR REPLACE FUNCTION partman.create_partitions(p_schema text, p_table text, p_start bigint, p_end bigint, p_step bigint) 
RETURNS void AS $$
DECLARE
    v_start bigint;
    v_end bigint;
    v_table_name text;
BEGIN
    FOR v_start IN SELECT * FROM generate_series(p_start, p_end, p_step) LOOP
        v_end := v_start + p_step;
        v_table_name := p_table || '_' || v_start || '_' || v_end;
        
        -- Check if partition exists, if not create it
        IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = v_table_name AND n.nspname = p_schema) THEN
            EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%s) TO (%s)', 
                            p_schema, v_table_name, p_schema, p_table, v_start, v_end);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Optional tuning
DO $$ BEGIN
  PERFORM current_setting('shared_buffers');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Skipping ALTER SYSTEM due to permissions.';
END $$;
