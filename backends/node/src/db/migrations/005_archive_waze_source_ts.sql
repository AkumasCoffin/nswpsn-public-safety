-- 005_archive_waze_source_ts.sql
-- Add (source, fetched_at DESC) compound indexes to the high-volume
-- archive families. Without this, queries like:
--
--   SELECT ... FROM archive_waze
--   WHERE source = 'waze_police' AND fetched_at >= now() - INTERVAL '30 days'
--   GROUP BY lat_bin, lng_bin
--
-- have to scan every row matching `source` (could be 50k+ for waze_police)
-- and filter `fetched_at` linearly. The existing (source, source_id,
-- fetched_at DESC) index puts source_id between source and fetched_at,
-- so the planner can't use it for a range scan on fetched_at without
-- pulling rows for every source_id first.
--
-- (source, fetched_at DESC) is the canonical index for "give me rows
-- for source X in the last N days" — exactly the heatmap, /api/data/history,
-- and DISTINCT-ON-with-time-window patterns hit.

DO $$
DECLARE
  fam TEXT;
BEGIN
  FOREACH fam IN ARRAY ARRAY['waze', 'traffic', 'rfs', 'power', 'misc']
  LOOP
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS idx_archive_%I_src_ts
       ON archive_%I (source, fetched_at DESC)',
      fam, fam
    );
  END LOOP;
END $$;
