-- 019_archive_latest_recompute.sql
--
-- Post-deploy fix-up for migration 017+018: recompute latest_fetched_at
-- on every sidecar entry to reflect when each incident's data ACTUALLY
-- last changed, not when it was last polled.
--
-- Why this is needed: migration 017's backfill set
-- latest_fetched_at = max(fetched_at) per (source, source_id). Under
-- the old append-on-every-poll writer, max(fetched_at) for any active
-- incident was "right before deploy" — every live outage and warning
-- got a fresh row every minute. After this migration runs, the unique=1
-- list ordered by latest_fetched_at DESC clusters all live incidents
-- at the deploy timestamp, hiding 30 days of real-history spread that
-- the user wants to scroll through.
--
-- Algorithm: for each (source, source_id), use a window function to
-- compare each row's data hash to the next-older row's hash. The most
-- recent boundary is "when the data last changed". Set
-- latest_fetched_at to that timestamp.
--
-- md5(data::text) here doesn't have to match the SHA-1 prefix the
-- writer uses (archive.ts hashRowData) — we're only comparing rows
-- to their own immediate neighbours, not to anything stored.
-- Postgres-native md5 keeps the work server-side; running this from
-- the writer would mean shipping every parent row to Node and back.
--
-- Cost: one full scan per archive_* with a window function. archive_waze
-- is the slowest (largest); the migration runner sets
-- statement_timeout = 0 so it can run as long as needed. Expect 30-90s
-- on a busy host. One-shot — won't run again.

DO $$
DECLARE
  t text;
  affected bigint;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'archive_waze',
    'archive_traffic',
    'archive_rfs',
    'archive_power',
    'archive_misc'
  ]
  LOOP
    EXECUTE format($f$
      WITH numbered AS (
        SELECT source, source_id, fetched_at,
               md5(data::text) AS h,
               LAG(md5(data::text)) OVER (
                 PARTITION BY source, source_id
                 ORDER BY fetched_at
               ) AS prev_h
          FROM %I
         WHERE source_id IS NOT NULL AND source_id <> ''
      ),
      change_points AS (
        SELECT source, source_id,
               MAX(fetched_at) AS last_change_ts
          FROM numbered
         WHERE h IS DISTINCT FROM prev_h
         GROUP BY 1, 2
      )
      UPDATE %I AS l
         SET latest_fetched_at = c.last_change_ts
        FROM change_points c
       WHERE l.source = c.source
         AND l.source_id = c.source_id
         AND l.latest_fetched_at <> c.last_change_ts
    $f$, t, t || '_latest');
    GET DIAGNOSTICS affected = ROW_COUNT;
    RAISE NOTICE 'recomputed % sidecar rows for %', affected, t;
  END LOOP;
END $$;
