-- 009_stats_snapshots_align.sql
--
-- Normalise the stats_snapshots schema. The table can arrive in two
-- shapes depending on history:
--   - python-era: (timestamp BIGINT, data JSON) — created by
--     init_postgres.py before the Node migration runner ever ran.
--     Migration 001's `CREATE TABLE IF NOT EXISTS` was a no-op when
--     this table already existed, so its (ts TIMESTAMPTZ) shape never
--     materialised on python-first deployments.
--   - Node-era: (ts TIMESTAMPTZ, data JSONB) — created by migration
--     001 on a fresh install.
--
-- Code now writes/reads the python column name (`timestamp` BIGINT ms)
-- since /api/stats/history's response contract is a JS-compatible
-- millisecond integer. This migration ensures both shapes converge on
-- that schema, copying any existing data along the way.

DO $$
BEGIN
  -- Ensure the table exists with the python schema. No-op when the
  -- table is already there in either shape.
  CREATE TABLE IF NOT EXISTS stats_snapshots (
    timestamp BIGINT,
    data JSONB
  );

  -- Path A: legacy Node shape — `ts TIMESTAMPTZ` exists, no `timestamp`.
  -- Add `timestamp`, copy from `ts` (TIMESTAMPTZ → ms epoch), drop `ts`.
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'stats_snapshots' AND column_name = 'ts'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'stats_snapshots' AND column_name = 'timestamp'
  ) THEN
    ALTER TABLE stats_snapshots ADD COLUMN timestamp BIGINT;
    UPDATE stats_snapshots
       SET timestamp = (EXTRACT(EPOCH FROM ts) * 1000)::bigint;
    ALTER TABLE stats_snapshots ALTER COLUMN timestamp SET NOT NULL;
    ALTER TABLE stats_snapshots DROP COLUMN ts;
  END IF;

  -- Path B: timestamp column exists but is nullable. Make it NOT NULL.
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'stats_snapshots'
      AND column_name = 'timestamp'
      AND is_nullable = 'YES'
  ) THEN
    -- Drop any rows with NULL before tightening the constraint.
    DELETE FROM stats_snapshots WHERE timestamp IS NULL;
    ALTER TABLE stats_snapshots ALTER COLUMN timestamp SET NOT NULL;
  END IF;

  -- Path C: data column may be JSON (python) instead of JSONB.
  -- Convert if so — JSONB is what every reader assumes.
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'stats_snapshots'
      AND column_name = 'data'
      AND data_type = 'json'
  ) THEN
    ALTER TABLE stats_snapshots
      ALTER COLUMN data TYPE JSONB USING data::jsonb;
  END IF;
END $$;

-- Read index for /api/stats/history's `WHERE timestamp >= cutoff
-- ORDER BY timestamp ASC` filter. PRIMARY KEY would be cleaner but
-- could fail if duplicates exist — index is enough for the query and
-- doesn't constrain inserts.
CREATE INDEX IF NOT EXISTS idx_stats_snapshots_timestamp
  ON stats_snapshots (timestamp);
