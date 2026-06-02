-- 021_archive_latest_dims.sql
--
-- Promote category + subcategory from the parent archive table to the
-- archive_*_latest sidecars. Lets the filter-dropdown query at
-- /api/data/history/filters compute per-(source, category, subcategory)
-- counts purely from the sidecar (one row per incident, tens of K rows)
-- instead of JOINing back to the parent (millions of rows). The JOIN
-- was timing out at 30s on archive_waze for wide windows and forced a
-- "wide vs narrow" branch in filterCache.archiveFacetsFromSidecar; with
-- these columns the JOIN goes away entirely and the dim breakdown
-- works at every window size.
--
-- Backfill strategy:
--   1. ADD COLUMNs nullable. Writes from the next deploy populate them.
--   2. A startup task (services/archiveLatestBackfill.ts) walks rows
--      where category IS NULL and fills from the parent (idempotent —
--      safe to re-run; safe if interrupted mid-pass).
--   3. Once backfill completes for a table, the read path uses the
--      sidecar columns; rows still NULL during the backfill window fall
--      through to the parent JOIN, so there's never a "missing data"
--      period.
--
-- Indexes:
--   (source, category, subcategory) supports the GROUP BY scan used by
--   the dim breakdown query. last_seen_at filter is already covered by
--   idx_*_latest_seen (migration 018) so no need to repeat it here.

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
        ADD COLUMN IF NOT EXISTS category    TEXT,
        ADD COLUMN IF NOT EXISTS subcategory TEXT
    $f$, archive_table || '_latest');

    EXECUTE format($f$
      CREATE INDEX IF NOT EXISTS %I
        ON %I (source, category, subcategory)
    $f$, archive_table || '_latest_dim', archive_table || '_latest');
  END LOOP;
END $$;
