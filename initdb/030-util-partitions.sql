-- initdb/030-util-partitions.sql
-- FIXED: Smart Partitioning (Only creates new tables when needed)

CREATE SCHEMA IF NOT EXISTS util;

CREATE TABLE IF NOT EXISTS util.height_part_ranges (
    schema_name TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    range_size  BIGINT NOT NULL,
    PRIMARY KEY (schema_name, table_name)
);

-- Configuration: Partition sizes (customized per table)
INSERT INTO util.height_part_ranges (schema_name, table_name, range_size) VALUES
 ('core', 'blocks', 500000), ('core', 'transactions', 500000), ('core', 'network_params', 500000),
 ('bank', 'transfers', 500000), ('bank', 'balance_deltas', 500000),
 ('tokens', 'cw20_transfers', 500000),
 ('wasm', 'executions', 500000), ('wasm', 'events', 500000),
 ('wasm', 'event_attrs', 500000), ('wasm', 'contract_migrations', 500000),
 ('wasm', 'dex_swaps', 500000), ('wasm', 'admin_changes', 500000),
 -- IBC: No longer partitioned (lifecycle merging requires simple PK)
 ('zigchain', 'dex_swaps', 500000), ('zigchain', 'dex_liquidity', 500000),
 ('zigchain', 'wrapper_events', 500000),
 ('tokens', 'factory_supply_events', 500000)
ON CONFLICT (schema_name, table_name) DO UPDATE SET range_size = EXCLUDED.range_size;

-- Remove unused entries
DELETE FROM util.height_part_ranges
WHERE (schema_name, table_name) NOT IN (
    ('core', 'blocks'), ('core', 'transactions'), ('core', 'network_params'),
    ('bank', 'transfers'), ('bank', 'balance_deltas'),
    ('tokens', 'cw20_transfers'),
    ('wasm', 'executions'), ('wasm', 'events'), ('wasm', 'event_attrs'),
    ('wasm', 'contract_migrations'),
    ('wasm', 'dex_swaps'), ('wasm', 'admin_changes'),
    ('zigchain', 'dex_swaps'), ('zigchain', 'dex_liquidity'),
    ('zigchain', 'wrapper_events'),
    ('tokens', 'factory_supply_events')
);

-- ✅ SMART FUNCTION: Creates partition ONLY if target height needs it
CREATE OR REPLACE FUNCTION util.ensure_partition_for_height(p_schema text, p_table text, p_target_height bigint)
RETURNS void AS $$
DECLARE
    v_range BIGINT;
    v_start BIGINT;
    v_end BIGINT;
    v_part_name TEXT;
    v_exists BOOLEAN;
BEGIN
    -- 1. Get range size (default 1M)
    SELECT range_size INTO v_range FROM util.height_part_ranges 
    WHERE schema_name = p_schema AND table_name = p_table;
    
    IF NOT FOUND THEN v_range := 500000; END IF;

    -- 2. Calculate which partition implies this height
    -- e.g. Height 500 -> Start 0. Height 1,500,000 -> Start 1,000,000.
    v_start := (p_target_height / v_range) * v_range;
    v_end   := v_start + v_range;
    
    -- 3. Construct Partition Name (e.g. blocks_p0, blocks_p1000000)
    v_part_name := p_table || '_p' || v_start;

    -- 4. Check if exists
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = p_schema
          AND c.relname = v_part_name
    ) INTO v_exists;

    -- 5. Create ONLY if missing
    IF NOT v_exists THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%s) TO (%s)',
            p_schema, v_part_name, p_schema, p_table, v_start, v_end
        );
    END IF;
END;
$$ LANGUAGE plpgsql;
