-- 135: node_radio_events becomes a table partitioned by UTC day.
--
-- Why: the table is ~6M rows on a 30-day retention, and keeping it there cost
-- ~36k single-row DELETEs an hour, every one of them leaving a dead tuple and a
-- dead entry in every index for autovacuum to find on a 5.5 GB table. The
-- documented history is two manual reclaims of 1-2.4 GB of dead index and a
-- concurrent-reindex script. With a partition per day, retention is one
-- DROP TABLE a day (services/nodeEventsPruner.ts) and the table cannot creep.
--
-- How, without an outage: the existing table is renamed and attached as the
-- first partition, node_radio_events_legacy, covering everything below 133's
-- bound. Every step here is a catalogue change:
--   - the CHECK validated by 134 proves the attach bound, so ATTACH skips its
--     scan;
--   - every index on the new parent is defined exactly like one the legacy
--     table already has (columns, uniqueness, INCLUDE, predicate — 131 and 132
--     built the two that were missing), so ATTACH adopts them instead of
--     building any;
--   - the foreign key is written exactly as 043 wrote it, so ATTACH adopts that
--     too instead of re-checking every row against nodes.
-- The legacy table keeps its other indexes (old pkey, the four partial ones
-- from 080/115/116); ATTACH tolerates extras, they still serve the legacy
-- days, and they go when that partition is dropped ~30 days after the bound.
--
-- Readers are untouched: the name node_radio_events now means the parent, and
-- every reader bounds received_at, so the planner reads only the days it needs.
--
-- Not on the parent, so new days never get them: idx_nre_rollup_time (the call
-- predicate matches 81 % of rows, so it is idx_nre_time with extra steps),
-- idx_nre_system_time_calls and idx_nre_site_time_calls (nre_sys_tg_time leads
-- on system, and a day partition bounds the time range they existed to order),
-- idx_nre_node_time_calls (same argument against nre_node_tg_time). The
-- three-column uq_nre_node_stream_event is dropped outright: its job moved to
-- 131's index, which a partitioned table requires.
--
-- The exclusive lock taken by the rename lasts for the rest of this
-- transaction, which is milliseconds; the lock_timeout makes the migration fail
-- (and the deploy stop before the restart) rather than wait behind a long staff
-- read while every insert waits behind it. Re-running the deploy is the
-- recovery: nothing is changed unless everything is.
SET LOCAL TimeZone = 'UTC';
SET LOCAL lock_timeout = '10s';

DO $$
DECLARE
  bound timestamptz;
BEGIN
  -- Already partitioned (a re-run after a later failure): nothing to do here.
  IF (SELECT relkind FROM pg_class WHERE oid = 'node_radio_events'::regclass) = 'p' THEN
    RETURN;
  END IF;

  -- What the rest relies on. An interrupted CONCURRENTLY build leaves an
  -- INVALID index that a re-run of its migration skips by name; see the
  -- recovery steps in 131 and 132.
  IF NOT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                  WHERE i.indrelid = 'node_radio_events'::regclass
                    AND c.relname = 'uq_nre_node_stream_event_ts' AND i.indisvalid) THEN
    RAISE EXCEPTION 'uq_nre_node_stream_event_ts is missing or invalid: see migration 131';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                  WHERE i.indrelid = 'node_radio_events'::regclass
                    AND c.relname = 'idx_nre_id' AND i.indisvalid) THEN
    RAISE EXCEPTION 'idx_nre_id is missing or invalid: see migration 132';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'node_radio_events'::regclass
                AND attname = 'id' AND attidentity <> '') THEN
    RAISE EXCEPTION 'node_radio_events.id is still an identity column: see migration 130';
  END IF;

  -- The bound exactly as 133 stored it, so the attach range is the CHECK's.
  SELECT substring(pg_get_constraintdef(oid) FROM '''([^'']+)''')::timestamptz
    INTO bound
    FROM pg_constraint
   WHERE conrelid = 'node_radio_events'::regclass AND conname = 'nre_legacy_bound' AND convalidated;
  IF bound IS NULL THEN
    RAISE EXCEPTION 'nre_legacy_bound is missing or not validated: see migrations 133/134';
  END IF;

  ALTER TABLE node_radio_events RENAME TO node_radio_events_legacy;
  DROP INDEX uq_nre_node_stream_event;

  -- INCLUDING DEFAULTS only. INCLUDING CONSTRAINTS would copy nre_legacy_bound
  -- onto the parent, and every row from the bound on would be refused.
  CREATE TABLE node_radio_events (
    LIKE node_radio_events_legacy INCLUDING DEFAULTS,
    CONSTRAINT nre_node_id_fkey FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
  ) PARTITION BY RANGE (received_at);

  CREATE UNIQUE INDEX nre_uq_node_stream_event_ts
    ON node_radio_events (node_id, stream_id, source_event_id, received_at);
  CREATE INDEX nre_id ON node_radio_events (id);
  CREATE INDEX nre_logical ON node_radio_events (logical_call_id);
  CREATE INDEX nre_time ON node_radio_events (received_at);
  CREATE INDEX nre_sys_tg_time ON node_radio_events (system, talkgroup, received_at);
  CREATE INDEX nre_unit_time ON node_radio_events (source_unit, received_at);
  CREATE INDEX nre_node_tg_time ON node_radio_events (node_id, talkgroup, received_at);
  CREATE INDEX nre_tg_time_calls ON node_radio_events (talkgroup, received_at)
    INCLUDE (system, wacn)
    WHERE event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%';

  EXECUTE format(
    'ALTER TABLE node_radio_events ATTACH PARTITION node_radio_events_legacy FOR VALUES FROM (MINVALUE) TO (%L)',
    bound);

  -- The partition bound enforces the same thing now.
  ALTER TABLE node_radio_events_legacy DROP CONSTRAINT nre_legacy_bound;
END $$;

-- One partition per UTC day, named node_radio_events_YYYYMMDD. Created and then
-- ATTACHed rather than CREATE ... PARTITION OF: ATTACH takes SHARE UPDATE
-- EXCLUSIVE on the parent where PARTITION OF takes ACCESS EXCLUSIVE, and this
-- runs hourly against a table taking inserts every second.
--
-- Storage parameters are set per partition because a partitioned parent's are
-- not inherited. fillfactor 85 and the 0.05 autovacuum factors are 124's
-- deliberate settings for this table (docs/components/backend.md).
--
-- A day the legacy partition still covers would overlap it; that is caught and
-- returns NULL, as does a day that already has its partition. Serialised so
-- two callers cannot race on one day.
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
    'ALTER TABLE %I SET (fillfactor = 85, autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.05)',
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

-- A runway before the restart. Days below the legacy bound are skipped by the
-- overlap rule above; the backend keeps a week ahead from here on.
SELECT ensure_node_radio_events_partition(d::date)
  FROM generate_series(current_date - 1, current_date + 10, interval '1 day') AS d;
