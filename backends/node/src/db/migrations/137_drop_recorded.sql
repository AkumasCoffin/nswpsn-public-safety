-- Drop what the recorded flag left behind.
--
-- The flag said whether a reception's audio reached central rdio, set by
-- matching each relayed upload back to one reception row (markRecorded), with
-- node_pending_recordings holding uploads that beat their event. The matching
-- was removed in the release before this one, so nothing writes or reads any
-- of the columns below or the pending table. Dropping them a release later
-- means this migrate step never runs under code that still uses them.
--
-- Every DROP COLUMN is catalogue-only: no rewrite, no scan. On the partitioned
-- node_radio_events it recurses to every partition, which takes ACCESS
-- EXCLUSIVE on each for the length of this transaction, so the lock_timeout
-- makes the deploy stop rather than queue every insert behind a long read.
-- Re-running the deploy is the recovery.
--
-- Per-node upload counts and bytes are unaffected: they come from the relay
-- itself, in node_call_stats.
SET LOCAL lock_timeout = '10s';

ALTER TABLE node_radio_events
  DROP COLUMN IF EXISTS recorded,
  DROP COLUMN IF EXISTS audio_bytes;

ALTER TABLE node_radio_hourly DROP COLUMN IF EXISTS audio_bytes;

ALTER TABLE node_radio_hourly_sys
  DROP COLUMN IF EXISTS recorded_calls,
  DROP COLUMN IF EXISTS recorded_calls_site;

ALTER TABLE node_radio_hourly_rx DROP COLUMN IF EXISTS recorded;

DROP TABLE IF EXISTS node_pending_recordings;

-- fillfactor 85 left room on each page for the recorded update to stay on the
-- page (a HOT update). With that update gone, the only one left is a patch
-- merge rewriting logical_call_id, which is indexed and so never HOT whatever
-- the room. The reserve is now 15 % of every day's heap held empty, so new days
-- go back to the default, and the empty days already created are reset. The
-- legacy partition keeps its setting until it is dropped. The autovacuum
-- factors stay.
CREATE OR REPLACE FUNCTION ensure_node_radio_events_partition(for_day date)
RETURNS text
LANGUAGE plpgsql
SET TimeZone = 'UTC'
AS $fn$
DECLARE
  part text := 'node_radio_events_' || to_char(for_day, 'YYYYMMDD');
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('ensure_node_radio_events_partition'));
  IF to_regclass(part) IS NOT NULL THEN
    RETURN NULL;
  END IF;
  EXECUTE format('CREATE TABLE %I (LIKE node_radio_events INCLUDING DEFAULTS)', part);
  EXECUTE format(
    'ALTER TABLE %I SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.05)',
    part);
  BEGIN
    EXECUTE format(
      'ALTER TABLE node_radio_events ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L)',
      part, for_day::timestamptz, (for_day + 1)::timestamptz);
  EXCEPTION WHEN invalid_object_definition THEN
    -- "would overlap partition": the legacy partition covers this day.
    EXECUTE format('DROP TABLE %I', part);
    RETURN NULL;
  END;
  RETURN part;
END
$fn$;

DO $$
DECLARE
  p regclass;
BEGIN
  FOR p IN
    SELECT i.inhrelid::regclass
      FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
     WHERE i.inhparent = to_regclass('node_radio_events')
       AND c.relname ~ '^node_radio_events_\d{8}$'
  LOOP
    EXECUTE format('ALTER TABLE %s RESET (fillfactor)', p);
  END LOOP;
END
$$;
