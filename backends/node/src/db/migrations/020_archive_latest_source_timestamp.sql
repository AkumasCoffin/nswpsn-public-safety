-- 020_archive_latest_source_timestamp.sql
--
-- Add source_timestamp_unix to archive_*_latest sidecars so the
-- frontend can filter and sort by the upstream feed's own publish /
-- last-updated timestamp instead of our backend's ingest time.
--
-- Why: backend timestamps (fetched_at, last_seen_at) describe our
-- polling, not what users mean by "this incident happened at X".
-- An RFS bushfire alert published at 14:30 should show as "14:30",
-- not "14:32" (when our poller happened to fire). The data field
-- already carries the upstream timestamp inside JSONB; the writer
-- now extracts it (services/archiveExtract.ts extractSourceTimestampUnix)
-- and stores it on the sidecar so SQL filters can hit a top-level
-- indexed column instead of (data->>'k')::bigint casts.
--
-- NULLable: not every upstream payload exposes a timestamp (some
-- feeds carry just the incident state). Reads COALESCE with
-- last_seen_at as a fallback so the row stays visible.

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
        ADD COLUMN IF NOT EXISTS source_timestamp_unix BIGINT
    $f$, archive_table || '_latest');

    EXECUTE format($f$
      CREATE INDEX IF NOT EXISTS %I
        ON %I (source_timestamp_unix DESC NULLS LAST)
    $f$, archive_table || '_latest_src_ts_unix', archive_table || '_latest');
  END LOOP;
END $$;
