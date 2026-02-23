-- 067-retention-policy.sql
-- Automatic data retention: drops partitions older than N blocks.
-- Works with ANY partition size (100k, 500k, etc.) — reads actual bounds from pg_catalog.
-- Safe for running indexer — only drops partitions fully below the cutoff.
--
-- Usage:
--   SELECT * FROM util.drop_old_partitions(1000000);  -- keep latest 1M blocks
--   SELECT * FROM util.drop_old_partitions(2000000);  -- keep latest 2M blocks
--
-- Schedule (cron):
--   0 * * * * docker exec <db> psql -U postgres -d cosmos_indexer_db -c "SELECT * FROM util.drop_old_partitions(1000000);"

CREATE OR REPLACE FUNCTION util.drop_old_partitions(p_retention_blocks BIGINT DEFAULT 1000000)
RETURNS TABLE(dropped_partition TEXT, partition_bounds TEXT) AS $$
DECLARE
    v_max_height BIGINT;
    v_cutoff BIGINT;
    r RECORD;
    v_upper BIGINT;
    v_dropped INT := 0;
BEGIN
    -- 1. Get current max height from indexer progress
    SELECT MAX(last_height) INTO v_max_height FROM core.indexer_progress;
    IF v_max_height IS NULL OR v_max_height <= 0 THEN
        RAISE NOTICE 'Retention: No progress found, skipping.';
        RETURN;
    END IF;

    -- 2. Calculate cutoff height
    v_cutoff := v_max_height - p_retention_blocks;
    IF v_cutoff <= 0 THEN
        RAISE NOTICE 'Retention: max_height=% with retention=% → cutoff=% (nothing to drop)',
                      v_max_height, p_retention_blocks, v_cutoff;
        RETURN;
    END IF;

    RAISE NOTICE 'Retention: max_height=%, retention=%, cutoff=%',
                  v_max_height, p_retention_blocks, v_cutoff;

    -- 3. Find and drop all partitions whose UPPER bound <= cutoff
    --    This means the ENTIRE partition is below the retention window.
    --    We parse actual bounds from pg_catalog, so it works regardless of
    --    partition size (100k for events/event_attrs, 500k for everything else).
    FOR r IN
        SELECT c.relname AS part_name,
               n.nspname AS schema_name,
               parent.relname AS parent_name,
               pg_get_expr(c.relpartbound, c.oid, true) AS bounds
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_inherits inh ON inh.inhrelid = c.oid
        JOIN pg_catalog.pg_class parent ON parent.oid = inh.inhparent
        JOIN pg_catalog.pg_namespace pn ON pn.oid = parent.relnamespace
        -- Only touch tables registered in our partition config
        WHERE EXISTS (
            SELECT 1 FROM util.height_part_ranges hpr
            WHERE hpr.schema_name = pn.nspname AND hpr.table_name = parent.relname
        )
        -- Skip DEFAULT partitions
        AND pg_get_expr(c.relpartbound, c.oid, true) LIKE 'FOR VALUES FROM%'
        ORDER BY n.nspname, parent.relname, c.relname
    LOOP
        -- Extract upper bound: "FOR VALUES FROM ('1000000') TO ('1500000')" → 1500000
        BEGIN
            v_upper := (regexp_replace(r.bounds, '.*TO \(''?(\d+)''?\).*', '\1'))::bigint;
        EXCEPTION WHEN OTHERS THEN
            CONTINUE; -- Skip partitions with unparseable bounds
        END;

        -- Only drop if the ENTIRE partition is below cutoff
        IF v_upper <= v_cutoff THEN
            RAISE NOTICE '  DROP: %.% (%, parent=%.%)',
                          r.schema_name, r.part_name, r.bounds, r.schema_name, r.parent_name;

            EXECUTE format('DROP TABLE %I.%I', r.schema_name, r.part_name);

            dropped_partition := r.schema_name || '.' || r.part_name;
            partition_bounds := r.bounds;
            v_dropped := v_dropped + 1;
            RETURN NEXT;
        END IF;
    END LOOP;

    RAISE NOTICE 'Retention complete: dropped % partition(s)', v_dropped;
END;
$$ LANGUAGE plpgsql;
