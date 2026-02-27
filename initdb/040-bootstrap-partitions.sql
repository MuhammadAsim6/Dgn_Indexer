-- 040-bootstrap-partitions.sql
-- FIXED: Elastic Setup. Only creates the INITIAL partition (0-500k for 500k-range tables).
-- Future partitions will be created automatically by the Indexer App.

DO $$
DECLARE
    r RECORD;
    h BIGINT;
    -- ✅ CHANGED: 10M -> 500k.
    -- Sirf pehla bucket banega. Baaki "Just-in-Time" banenge.
    v_max_bootstrap_height BIGINT := 500000; 
    v_range_size BIGINT;
BEGIN
    RAISE NOTICE 'Bootstrapping initial partitions...';

    -- Loop through all registered tables (Core, Zigchain, WASM, etc.)
    FOR r IN SELECT * FROM util.height_part_ranges LOOP
        
        v_range_size := r.range_size;
        
        -- Loop: Sirf 0 se Start hoga (0-500k for 500k-range tables)
        FOR h IN 0..(v_max_bootstrap_height / v_range_size) - 1 LOOP 
            PERFORM util.ensure_partition_for_height(r.schema_name, r.table_name, h * v_range_size);
        END LOOP;
        
        -- Safety Check: Ensure specifically 0 is covered (in case logic skips)
        PERFORM util.ensure_partition_for_height(r.schema_name, r.table_name, 0);
        
        RAISE NOTICE ' - Initial partition ready for %.% (0 to %)', r.schema_name, r.table_name, v_max_bootstrap_height;
    END LOOP;
END $$;


-- 2) Performance Tuning: Autovacuum settings
--    Optimizes disk cleanup for high-volume tables
DO $$
DECLARE
    base_tbl regclass;
    q text;
BEGIN
    FOR base_tbl IN
        SELECT 'core.transactions'::regclass
        UNION ALL SELECT 'core.events'::regclass
        UNION ALL SELECT 'core.event_attrs'::regclass
        UNION ALL SELECT 'zigchain.dex_swaps'::regclass     
        UNION ALL SELECT 'zigchain.dex_liquidity'::regclass 
        UNION ALL SELECT 'wasm.executions'::regclass        
        UNION ALL SELECT 'wasm.event_attrs'::regclass
        UNION ALL SELECT 'tokens.cw20_transfers'::regclass
        UNION ALL SELECT 'ibc.transfers'::regclass
    LOOP
        -- Apply settings to all partitions created above
        FOR q IN
            SELECT format('ALTER TABLE %s SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);',
                          c.oid::regclass)
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            WHERE i.inhparent = base_tbl
        LOOP
            EXECUTE q;
        END LOOP;
    END LOOP;
END $$;
