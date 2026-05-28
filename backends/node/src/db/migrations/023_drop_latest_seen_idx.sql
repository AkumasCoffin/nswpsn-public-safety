-- 023_drop_latest_seen_idx.sql
--
-- Drop idx_archive_*_latest_seen (created in 018). This index is on
-- last_seen_at, the column every single sidecar UPSERT bumps. Its
-- presence forces a non-HOT row update on every poll — even when the
-- incident's data hash is unchanged and conceptually NOTHING has
-- changed except "we saw it again". Postgres has to rewrite both the
-- heap tuple AND the index entry every time.
--
-- The index was added to speed up cleanup.ts pruning:
--   DELETE FROM archive_*_latest WHERE last_seen_at < NOW() - INTERVAL X
-- but the sidecars are bounded by retention to ~77k rows worst case
-- (archive_waze_latest). A seq scan is sub-second; the per-poll write
-- cost of maintaining the index across hundreds of thousands of
-- upserts/hour is much larger.
--
-- Net effect: enables HOT updates for the dominant case (incident's
-- data hash unchanged, just bumping last_seen_at), reducing WAL
-- volume + bloat + autovacuum pressure.

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
      DROP INDEX IF EXISTS %I
    $f$, archive_table || '_latest_seen');
  END LOOP;
END $$;
