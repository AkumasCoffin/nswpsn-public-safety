-- 094_fix_interstate_source_timestamps.sql
--
-- The archive extractor picked the locale `updated` string
-- ("06/09/2026, 11:34:15 pm") over the machine `updatedISO` on every
-- interstate fire source, and Date.parse read it month-first — 6 Sep
-- became 9 Jun. With source_timestamp_unix months in the past, the
-- history window filter (COALESCE(source_timestamp_unix, last_seen_at))
-- dropped every interstate incident from the logs page, so QFD showed
-- 0 in 24h while archiving 39k rows a week.
--
-- The extractor now prefers updatedISO (and parses slash-dates
-- day-first as a fallback); this backfills the sidecar rows stamped
-- while the bug was live. The parent tables store no timestamp column —
-- only the sidecar needs repair.

UPDATE archive_rfs_latest l
   SET source_timestamp_unix = EXTRACT(EPOCH FROM sub.iso::timestamptz)::bigint
  FROM (
    SELECT DISTINCT ON (source, source_id)
           source, source_id, data->>'updatedISO' AS iso
      FROM archive_rfs
     WHERE source IN ('nt_fire', 'qld_fire', 'qld_warning', 'vic_emergency',
                      'wa_incident', 'wa_warning', 'sa_cfs', 'sa_mfs')
     ORDER BY source, source_id, fetched_at DESC
  ) sub
 WHERE l.source = sub.source
   AND l.source_id = sub.source_id
   AND sub.iso ~ '^\d{4}-\d{2}-\d{2}T';
