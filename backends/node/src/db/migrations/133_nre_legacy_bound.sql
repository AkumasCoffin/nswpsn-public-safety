-- 133: an upper bound on node_radio_events, ahead of turning it into the first
-- partition of a table partitioned by day (135).
--
-- ATTACH PARTITION has to prove that every row of the table it attaches fits
-- the partition's range. With a validated CHECK that already says so, it takes
-- the CHECK's word; without one, it scans ~6M rows while holding an exclusive
-- lock on the table — an ingest outage. This adds the CHECK NOT VALID (a
-- catalogue change: new rows are checked from here on, existing ones are not
-- yet), and 134 validates it without blocking anyone.
--
-- The bound is the start of the UTC day three days from now. Three because a
-- reception is accepted up to 48 h either side of server time
-- (services/nodeEvents.ts radioReceivedAt), and three days from midnight is
-- always more than 48 h from now: no node, however far ahead its clock, can
-- have stored — or store before the cutover — a row this rejects. Rows from
-- the bound on go to daily partitions created by 135.
--
-- UTC is explicit throughout: a bare 'YYYY-MM-DD 00:00' literal is read in the
-- session time zone, and partition bounds must not move with a server setting.
SET LOCAL TimeZone = 'UTC';
SET LOCAL lock_timeout = '10s';

DO $$
DECLARE
  bound timestamptz := (date_trunc('day', now() AT TIME ZONE 'UTC') + interval '3 days') AT TIME ZONE 'UTC';
BEGIN
  IF EXISTS (SELECT 1 FROM pg_constraint
              WHERE conrelid = 'node_radio_events'::regclass AND conname = 'nre_legacy_bound') THEN
    RETURN;
  END IF;
  -- Cheap through idx_nre_time, and turns an impossible row into a clear
  -- message here rather than a failed VALIDATE in 134.
  IF EXISTS (SELECT 1 FROM node_radio_events WHERE received_at >= bound) THEN
    RAISE EXCEPTION 'node_radio_events has rows at or after %; a node clock is far ahead. '
                    'Find them with: SELECT node_id, min(received_at), max(received_at) '
                    'FROM node_radio_events WHERE received_at >= %L GROUP BY 1', bound, bound;
  END IF;
  EXECUTE format(
    'ALTER TABLE node_radio_events ADD CONSTRAINT nre_legacy_bound CHECK (received_at < %L::timestamptz) NOT VALID',
    bound);
END $$;
