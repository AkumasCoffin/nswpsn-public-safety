-- 018_archive_latest_dedupe.sql
--
-- Extend the archive_*_latest sidecars (migration 017) to support
-- write-time dedup: only INSERT a new parent row when the incident's
-- data has actually changed since we last stored it. Polls that bring
-- no new information just bump the sidecar's last_seen_at — no row
-- explosion in archive_waze etc., no over-counting in dropdowns.
--
-- New columns:
--   last_seen_at  — last poll where we saw this incident, regardless
--                   of whether the data changed. Mirrors python's
--                   "this incident is still alive" tracking.
--   data_hash     — SHA-1 of the data JSONB at the time of the most
--                   recent stored row. The writer compares incoming
--                   hashes against this to decide INSERT-vs-skip.
--
-- Defaults: NULL data_hash on existing rows is the "no comparison
-- baseline" sentinel — first poll after deploy will INSERT (because
-- existing_hash != incoming_hash holds vacuously) and from then on
-- the comparison works normally.
--
-- last_seen_at defaults to latest_fetched_at on existing rows: the
-- best approximation we have without re-polling. The first poll
-- after deploy bumps it to NOW() for any incident still alive.

DO $$
DECLARE
  archive_table text;
BEGIN
  FOREACH archive_table IN ARRAY ARRAY[
    'archive_waze',
    'archive_traffic',
    'archive_rfs',
    'archive_power',
    'archive_misc'
  ]
  LOOP
    EXECUTE format($f$
      ALTER TABLE %I
        ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS data_hash    TEXT
    $f$, archive_table || '_latest');

    -- Backfill last_seen_at from latest_fetched_at where NULL so the
    -- column has a usable value before any new poll bumps it.
    EXECUTE format($f$
      UPDATE %I
         SET last_seen_at = latest_fetched_at
       WHERE last_seen_at IS NULL
    $f$, archive_table || '_latest');

    EXECUTE format($f$
      ALTER TABLE %I
        ALTER COLUMN last_seen_at SET DEFAULT NOW(),
        ALTER COLUMN last_seen_at SET NOT NULL
    $f$, archive_table || '_latest');

    -- Cleanup prunes by last_seen_at now (see services/cleanup.ts).
    EXECUTE format($f$
      CREATE INDEX IF NOT EXISTS %I
        ON %I (last_seen_at)
    $f$, archive_table || '_latest_seen', archive_table || '_latest');
  END LOOP;
END $$;
