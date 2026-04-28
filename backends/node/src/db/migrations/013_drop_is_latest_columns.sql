-- 013_drop_is_latest_columns.sql
--
-- Drop the is_latest + is_live columns added in migration 012 plus
-- the partial indexes that depended on them. The is_latest experiment
-- created untenable I/O contention — the bulk UPDATE generated ~366k
-- dead tuples on archive_waze, and the periodic refresher kept making
-- more on every poll cycle, saturating disk I/O and timing out every
-- read+write.
--
-- ALTER TABLE DROP COLUMN doesn't rewrite the table (postgres just
-- marks the column as logically dropped; physical column removal
-- happens on the next VACUUM FULL). So this migration is FAST — just
-- a few catalog updates. The associated indexes are dropped
-- automatically when their column goes away.
--
-- After this migration:
-- - INSERTs are faster (fewer columns + fewer indexes to update).
-- - Code paths that filtered on is_latest no longer compile (already
--   reverted in the same commit).
-- - Run VACUUM FULL archive_waze separately to reclaim the disk space
--   from the dropped column + dead tuples.

DO $$
DECLARE
  parent TEXT;
BEGIN
  FOREACH parent IN ARRAY ARRAY['archive_waze','archive_traffic','archive_rfs',
                                'archive_power','archive_misc']
  LOOP
    EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS is_latest', parent);
    EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS is_live', parent);
  END LOOP;
END $$;

-- Defensive: drop any orphaned indexes by name. ALTER TABLE DROP
-- COLUMN should cascade-drop these but be explicit in case partition
-- inheritance left orphans.
DROP INDEX IF EXISTS idx_archive_waze_latest;
DROP INDEX IF EXISTS idx_archive_traffic_latest;
DROP INDEX IF EXISTS idx_archive_rfs_latest;
DROP INDEX IF EXISTS idx_archive_power_latest;
DROP INDEX IF EXISTS idx_archive_misc_latest;
DROP INDEX IF EXISTS idx_archive_waze_src_sid_latest;
DROP INDEX IF EXISTS idx_archive_traffic_src_sid_latest;
DROP INDEX IF EXISTS idx_archive_rfs_src_sid_latest;
DROP INDEX IF EXISTS idx_archive_power_src_sid_latest;
DROP INDEX IF EXISTS idx_archive_misc_src_sid_latest;

-- Remove the backfill markers from schema_migrations so they don't
-- re-trigger if the columns ever get added back.
DELETE FROM schema_migrations
 WHERE filename LIKE '_backfill_%is_latest%'
    OR filename LIKE '_backfill_%is_live%';
