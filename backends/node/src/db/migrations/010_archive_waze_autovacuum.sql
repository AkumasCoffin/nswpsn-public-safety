-- 010_archive_waze_autovacuum.sql
--
-- Tighten autovacuum thresholds on archive_waze partitions so postgres
-- cleans up dead tuples more aggressively. Defaults wait until 20% of
-- the table is dead before triggering — on 290k rows that's 58k dead
-- tuples, exactly what the recent subcategory backfill produced.
-- autovacuum's I/O budget then can't catch up because waze ingest
-- keeps writing.
--
-- ⚠ IMPORTANT: storage parameters set on a partitioned PARENT do NOT
-- inherit to existing or future child partitions in Postgres 14-16.
-- Each partition needs its own ALTER TABLE. We apply to every existing
-- partition AND patch ensure_archive_partition() so future partitions
-- pick up the same settings on creation.

-- Per-partition settings.
DO $$
DECLARE
  partname TEXT;
  parent TEXT;
BEGIN
  FOR partname, parent IN
    SELECT child.relname::text, parent.relname::text
    FROM pg_inherits
    JOIN pg_class child  ON child.oid  = pg_inherits.inhrelid
    JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
    WHERE parent.relname IN ('archive_waze','archive_traffic','archive_misc',
                             'archive_rfs','archive_power')
  LOOP
    -- archive_waze partitions — most aggressive (heaviest writes).
    IF parent = 'archive_waze' THEN
      EXECUTE format(
        'ALTER TABLE %I SET (
           autovacuum_vacuum_scale_factor = 0.02,
           autovacuum_vacuum_threshold = 1000,
           autovacuum_vacuum_cost_limit = 2000,
           autovacuum_vacuum_cost_delay = 2,
           autovacuum_analyze_scale_factor = 0.05
         )', partname);
    -- archive_traffic / archive_misc — moderate (continuous polling).
    ELSIF parent IN ('archive_traffic','archive_misc') THEN
      EXECUTE format(
        'ALTER TABLE %I SET (
           autovacuum_vacuum_scale_factor = 0.1,
           autovacuum_vacuum_cost_limit = 500
         )', partname);
    END IF;
  END LOOP;
END $$;

-- Patch ensure_archive_partition so future partitions inherit the
-- right settings on creation. The function is `CREATE OR REPLACE`
-- so this safely upgrades the existing definition.
CREATE OR REPLACE FUNCTION ensure_archive_partition(
  parent_table TEXT,
  for_date DATE
) RETURNS TEXT AS $$
DECLARE
  start_date DATE := date_trunc('month', for_date)::date;
  end_date   DATE := (date_trunc('month', for_date) + INTERVAL '1 month')::date;
  part_name  TEXT := parent_table || '_' || to_char(start_date, 'YYYY_MM');
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
      part_name, parent_table, start_date, end_date
    );
    -- Apply per-table autovacuum tuning. archive_waze is the heaviest
    -- write target so we aggressive-tune it; others get moderate.
    IF parent_table = 'archive_waze' THEN
      EXECUTE format(
        'ALTER TABLE %I SET (
           autovacuum_vacuum_scale_factor = 0.02,
           autovacuum_vacuum_threshold = 1000,
           autovacuum_vacuum_cost_limit = 2000,
           autovacuum_vacuum_cost_delay = 2,
           autovacuum_analyze_scale_factor = 0.05
         )', part_name);
    ELSIF parent_table IN ('archive_traffic','archive_misc') THEN
      EXECUTE format(
        'ALTER TABLE %I SET (
           autovacuum_vacuum_scale_factor = 0.1,
           autovacuum_vacuum_cost_limit = 500
         )', part_name);
    END IF;
  END IF;
  RETURN part_name;
END;
$$ LANGUAGE plpgsql;
