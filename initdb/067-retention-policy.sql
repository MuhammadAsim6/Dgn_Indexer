-- 067-retention-policy.sql
-- Automatic data retention: drops partitions older than N blocks.
-- Works with ANY partition size (100k, 500k, etc.) — reads actual bounds from pg_catalog.
-- Safe for a LIVE running indexer:
--   Uses DETACH PARTITION (ShareUpdateExclusiveLock) then DROP TABLE on the
--   detached standalone table. Compatible with concurrent INSERT operations.
--   Note: DETACH CONCURRENTLY cannot run inside a function (PG restriction).
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

    -- 2. Calculate cutoff height with a 100k block safety buffer.
    --    This ensures we never touch partitions the indexer may still be writing to.
    v_cutoff := v_max_height - p_retention_blocks - 100000;
    IF v_cutoff <= 0 THEN
        RAISE NOTICE 'Retention: max_height=% with retention=% + buffer=100k → cutoff=% (nothing to drop)',
                      v_max_height, p_retention_blocks, v_cutoff;
        RETURN;
    END IF;

    RAISE NOTICE 'Retention: max_height=%, retention=%, cutoff=%',
                  v_max_height, p_retention_blocks, v_cutoff;

    -- 3. Find all eligible partitions and remove them using the two-phase approach:
    --      Phase A: DETACH PARTITION
    --               Requires only ShareUpdateExclusiveLock on the parent — compatible
    --               with concurrent INSERT/UPDATE on the parent. No deadlock risk.
    --      Phase B: DROP TABLE on the detached (now standalone) table.
    --               No longer a partition, so no parent lock needed. Safe.
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
        -- Protect critical p0 partition for token supply tracking (Bootstrap data)
        IF r.schema_name = 'tokens' AND r.part_name = 'factory_supply_events_p0' THEN
            RAISE NOTICE '  SKIP: %.% (Protected bootstrap partition)', r.schema_name, r.part_name;
            CONTINUE;
        END IF;

        -- Extract upper bound: "FOR VALUES FROM ('1000000') TO ('1500000')" → 1500000
        BEGIN
            v_upper := (regexp_replace(r.bounds, '.*TO \(''?(\d+)''?\).*', '\1'))::bigint;
        EXCEPTION WHEN OTHERS THEN
            CONTINUE; -- Skip partitions with unparseable bounds
        END;

        -- Only remove if the ENTIRE partition is below the cutoff
        IF v_upper <= v_cutoff THEN
            RAISE NOTICE '  DETACH+DROP: %.% (%, parent=%.%)',
                          r.schema_name, r.part_name, r.bounds, r.schema_name, r.parent_name;

            -- Phase A: Detach partition from parent (ShareUpdateExclusiveLock only).
            -- This is compatible with concurrent INSERTs on the parent table.
            EXECUTE format(
                'ALTER TABLE %I.%I DETACH PARTITION %I.%I',
                r.schema_name, r.parent_name,
                r.schema_name, r.part_name
            );

            -- Phase B: Drop the now-standalone table. Since it is no longer a
            -- partition, this does not lock the parent table at all.
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