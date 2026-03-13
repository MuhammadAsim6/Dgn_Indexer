-- Detach range partitions by height/window across all partitioned parent tables.
--
-- Usage examples:
--   Dry-run (default):
--     psql "$DATABASE_URL" -f scripts/detach_partitions_by_height.sql \
--       -v from_height=0 -v to_height=3999999
--
--   Execute detaches:
--     psql "$DATABASE_URL" -f scripts/detach_partitions_by_height.sql \
--       -v from_height=0 -v to_height=3999999 -v dry_run=false
--
-- Optional:
--   -v match_mode=full      -- detach partitions fully contained in range [from,to] (default)
--   -v match_mode=overlap   -- detach partitions that overlap [from,to]
--   -v include_parents='core.%'   -- pattern on parent "schema.table" (default: '%')
--   -v exclude_parents='ibc.%'    -- pattern to skip parents (default: '')
--
-- Notes:
-- - This script DETACHES partitions only; it does not DROP them.
-- - After detach, data stays in detached child tables until you archive/drop manually.
-- - Run at low traffic to reduce lock contention.

\set ON_ERROR_STOP on

\if :{?from_height}
\else
\set from_height 0
\endif

\if :{?to_height}
\else
\set to_height 0
\endif

\if :{?dry_run}
\else
\set dry_run true
\endif

\if :{?match_mode}
\else
\set match_mode full
\endif

\if :{?include_parents}
\else
\set include_parents %
\endif

\if :{?exclude_parents}
\else
\set exclude_parents
\endif

-- psql variables are not expanded inside DO $$ bodies.
-- Bridge values via session settings first, then read them in plpgsql.
SELECT set_config('cdi.detach.from_height', :'from_height', false);
SELECT set_config('cdi.detach.to_height', :'to_height', false);
SELECT set_config('cdi.detach.dry_run', :'dry_run', false);
SELECT set_config('cdi.detach.match_mode', :'match_mode', false);
SELECT set_config('cdi.detach.include_parents', :'include_parents', false);
SELECT set_config('cdi.detach.exclude_parents', :'exclude_parents', false);

DO $$
DECLARE
  v_from bigint := current_setting('cdi.detach.from_height')::bigint;
  v_to bigint := current_setting('cdi.detach.to_height')::bigint;
  v_dry_run boolean := current_setting('cdi.detach.dry_run')::boolean;
  v_match_mode text := lower(current_setting('cdi.detach.match_mode'));
  v_include_parents text := current_setting('cdi.detach.include_parents');
  v_exclude_parents text := current_setting('cdi.detach.exclude_parents');

  r record;
  v_detach_sql text;
  v_selected int := 0;
  v_executed int := 0;
BEGIN
  IF v_from > v_to THEN
    RAISE EXCEPTION 'Invalid range: from_height (%) must be <= to_height (%)', v_from, v_to;
  END IF;

  IF v_match_mode NOT IN ('full', 'overlap') THEN
    RAISE EXCEPTION 'Invalid match_mode: % (allowed: full, overlap)', v_match_mode;
  END IF;

  RAISE NOTICE 'Partition detach window: [%..%], dry_run=%, match_mode=%', v_from, v_to, v_dry_run, v_match_mode;
  RAISE NOTICE 'Parent filters: include_parents="%", exclude_parents="%"', v_include_parents, COALESCE(v_exclude_parents, '');

  FOR r IN
    WITH parts AS (
      SELECT
        pn.nspname AS parent_schema,
        p.relname AS parent_table,
        cn.nspname AS child_schema,
        c.relname AS child_table,
        pg_get_expr(c.relpartbound, c.oid) AS bound_expr,
        (
          regexp_match(
            replace(pg_get_expr(c.relpartbound, c.oid), '''', ''),
            'FROM \(([-0-9]+)[^)]*\) TO \(([-0-9]+)[^)]*\)'
          )
        )[1]::bigint AS range_from,
        (
          regexp_match(
            replace(pg_get_expr(c.relpartbound, c.oid), '''', ''),
            'FROM \(([-0-9]+)[^)]*\) TO \(([-0-9]+)[^)]*\)'
          )
        )[2]::bigint AS range_to
      FROM pg_inherits i
      JOIN pg_class c ON c.oid = i.inhrelid
      JOIN pg_namespace cn ON cn.oid = c.relnamespace
      JOIN pg_class p ON p.oid = i.inhparent
      JOIN pg_namespace pn ON pn.oid = p.relnamespace
      WHERE c.relkind = 'r'
    )
    SELECT *
    FROM parts
    WHERE range_from IS NOT NULL
      AND range_to IS NOT NULL
      AND (parent_schema || '.' || parent_table) LIKE v_include_parents
      AND (
        COALESCE(v_exclude_parents, '') = ''
        OR (parent_schema || '.' || parent_table) NOT LIKE v_exclude_parents
      )
      AND (
        (v_match_mode = 'full' AND range_from >= v_from AND (range_to - 1) <= v_to)
        OR
        (v_match_mode = 'overlap' AND range_from <= v_to AND range_to > v_from)
      )
    ORDER BY range_from, parent_schema, parent_table
  LOOP
    v_selected := v_selected + 1;
    v_detach_sql := format(
      'ALTER TABLE %I.%I DETACH PARTITION %I.%I',
      r.parent_schema, r.parent_table, r.child_schema, r.child_table
    );

    IF v_dry_run THEN
      RAISE NOTICE '[dry-run] %.%  child=%.%  range=[%..%]  sql=%',
        r.parent_schema, r.parent_table,
        r.child_schema, r.child_table,
        r.range_from, (r.range_to - 1),
        v_detach_sql;
    ELSE
      EXECUTE v_detach_sql;
      v_executed := v_executed + 1;
      RAISE NOTICE '[detached] %.%  child=%.%  range=[%..%]',
        r.parent_schema, r.parent_table,
        r.child_schema, r.child_table,
        r.range_from, (r.range_to - 1);
    END IF;
  END LOOP;

  IF v_selected = 0 THEN
    RAISE NOTICE 'No partitions matched the requested window and filters.';
  ELSE
    RAISE NOTICE 'Matched partitions: %, executed detaches: %', v_selected, v_executed;
  END IF;
END;
$$ LANGUAGE plpgsql;
