-- 002_archive_partitions.sql
-- Per-source-family archive tables, RANGE-partitioned by `fetched_at`
-- monthly. Append-only — never UPDATEd. Each partition can be dropped
-- in O(1) when it ages out of retention.
--
-- Five tables instead of one monolith because:
--   - WHERE-clauses on hot read paths always include `source`, easy
--     to route at the application layer (data_history endpoints just
--     pick the right table)
--   - Per-family schema can grow typed columns (e.g. waze.uuid)
--     without forcing other families into JSONB
--   - Drop-table-per-family-per-day-partition is genuinely O(1);
--     a 25-index monolith makes that pay every dropped partition
--   - Five smaller autovacuum surfaces > one giant one
--
-- A nightly job (added later) calls ensure_archive_partition(d) for
-- next month's partition, and DETACH+DROPs partitions older than the
-- retention window.

-- Helper: ensure a monthly partition exists for the given date on the
-- given table. Idempotent — safe to call from a scheduler. Returns
-- the partition name. Date is rounded to the first of its month.
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
  END IF;
  RETURN part_name;
END;
$$ LANGUAGE plpgsql;

-- One archive table per source family. Same columns; only constraint
-- is `(id, fetched_at)` PK because partitioning requires the partition
-- key in every UNIQUE constraint.
DO $$
DECLARE
  fam TEXT;
BEGIN
  FOREACH fam IN ARRAY ARRAY['waze', 'traffic', 'rfs', 'power', 'misc']
  LOOP
    EXECUTE format($f$
      CREATE TABLE IF NOT EXISTS archive_%I (
        id          BIGSERIAL    NOT NULL,
        source      TEXT         NOT NULL,
        source_id   TEXT,
        fetched_at  TIMESTAMPTZ  NOT NULL,
        lat         DOUBLE PRECISION,
        lng         DOUBLE PRECISION,
        category    TEXT,
        subcategory TEXT,
        data        JSONB        NOT NULL,
        PRIMARY KEY (id, fetched_at)
      ) PARTITION BY RANGE (fetched_at)
    $f$, fam);

    -- Two indexes per family — only what we know we'll need:
    --   1) "give me last snapshot of this incident": (source, source_id, fetched_at DESC)
    --   2) "global recent feed":                     (fetched_at DESC)
    -- More can be added by later migrations once we measure real queries.
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS idx_archive_%I_src_sid_ts
       ON archive_%I (source, source_id, fetched_at DESC)',
      fam, fam
    );
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS idx_archive_%I_ts
       ON archive_%I (fetched_at DESC)',
      fam, fam
    );
  END LOOP;
END $$;

-- Seed the current month and next month for each family so the first
-- INSERTs after startup don't fail on "no partition for value".
-- The nightly maintenance job will keep extending the runway.
DO $$
DECLARE
  fam TEXT;
  this_month DATE := date_trunc('month', now())::date;
  next_month DATE := (date_trunc('month', now()) + INTERVAL '1 month')::date;
BEGIN
  FOREACH fam IN ARRAY ARRAY['waze', 'traffic', 'rfs', 'power', 'misc']
  LOOP
    PERFORM ensure_archive_partition('archive_' || fam, this_month);
    PERFORM ensure_archive_partition('archive_' || fam, next_month);
  END LOOP;
END $$;
