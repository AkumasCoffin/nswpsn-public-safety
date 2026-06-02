-- 022_archive_latest_jsonb_fields.sql
--
-- Promote title, location_text, status, severity, is_active from the
-- JSONB `data` blob in the parent archive to the archive_*_latest
-- sidecars. Same motivation as migration 021 (which promoted category
-- + subcategory): so the filter dropdown's dim breakdown and the
-- unique=1 list's WHERE predicates can hit indexed sidecar columns
-- instead of unindexed `(data->>'key')` JSONB extractions.
--
-- Impact:
--   * /api/data/history?unique=1&status=Advice — pre-JOIN sidecar
--     filter, planner uses idx_*_latest_status_dim instead of seq-
--     scanning the parent for the JSONB cast.
--   * /api/data/history/filters status/severity breakdown — sidecar
--     GROUP BY 1, 2 hits the dim index directly.
--   * /api/data/history?title=... ILIKE — runs against the sidecar's
--     ~77k-row title column instead of the parent's millions.
--
-- Backfill: same pattern as the 021 dims backfill. Walk NULL-title
-- sidecar rows in chunks, UPDATE...FROM the parent's
-- latest_fetched_at row.
--
-- Index choices:
--   * (source, status) — small distinct set, common dropdown filter
--   * (source, severity) — same shape as status
--   * (is_active) partial — boolean; partial index keeps it small
--   No index on title/location_text — at sidecar scale (~77k rows
--   per table), ILIKE seq scan is sub-100ms, and trigram extension
--   adds operational complexity for marginal gain.

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
        ADD COLUMN IF NOT EXISTS title         TEXT,
        ADD COLUMN IF NOT EXISTS location_text TEXT,
        ADD COLUMN IF NOT EXISTS status        TEXT,
        ADD COLUMN IF NOT EXISTS severity      TEXT,
        ADD COLUMN IF NOT EXISTS is_active     BOOLEAN
    $f$, archive_table || '_latest');

    EXECUTE format($f$
      CREATE INDEX IF NOT EXISTS %I
        ON %I (source, status)
        WHERE status IS NOT NULL
    $f$, archive_table || '_latest_status', archive_table || '_latest');

    EXECUTE format($f$
      CREATE INDEX IF NOT EXISTS %I
        ON %I (source, severity)
        WHERE severity IS NOT NULL
    $f$, archive_table || '_latest_severity', archive_table || '_latest');

    EXECUTE format($f$
      CREATE INDEX IF NOT EXISTS %I
        ON %I (source)
        WHERE is_active = true
    $f$, archive_table || '_latest_active', archive_table || '_latest');
  END LOOP;
END $$;
