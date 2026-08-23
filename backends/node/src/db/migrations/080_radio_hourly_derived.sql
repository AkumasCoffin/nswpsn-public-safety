-- 080: the radio hourly rollups become DERIVED, not maintained.
--
-- WHY THE OLD SHAPE COULD NOT WORK
-- The counters were bumped once per event, at ingest, and then never revisited.
-- But the truth changes AFTER ingest: mergeAutomaticPatch folds several logical
-- calls into one seconds later, and a late reception joins a group whose first
-- member was already counted. An incrementally-maintained counter drifts from
-- the detail table by construction — which is why logical_calls was wrong, and
-- why nothing ever read these tables (api/node-data.ts re-sources window=all
-- from node_radio_events and says so). Two upserts per event, on the hot ingest
-- path, for numbers nobody could trust.
--
-- Now: services/nodeHourlyRollup.ts recomputes each COMPLETED hour from the
-- detail table, using the same call/reception definitions the read paths use.
-- DELETE-then-INSERT per hour, so it is idempotent — re-running is a no-op
-- rather than a doubling, which is what made migration 079 a one-shot.
--
-- The columns below are what the reads actually need. `calls` on the per-node
-- table keeps its name and meaning (receptions by that node) so nothing that
-- referenced it has to change.

ALTER TABLE node_radio_hourly
  ADD COLUMN IF NOT EXISTS logical_calls INTEGER NOT NULL DEFAULT 0;

ALTER TABLE node_radio_hourly_sys
  ADD COLUMN IF NOT EXISTS receptions      INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS encrypted_calls INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS recorded_calls  INTEGER NOT NULL DEFAULT 0;

-- The existing contents are the drifted per-event counts, including whatever
-- migration 079 added. They cannot be reconciled, only replaced: the first
-- rollup pass rebuilds every hour still covered by the 30-day detail table, so
-- what is dropped here is only what the detail table can no longer prove.
TRUNCATE node_radio_hourly, node_radio_hourly_sys;

-- How far the rollup has got. One row, so the job knows where to resume and
-- the pruner knows what it must not delete out from under it.
CREATE TABLE IF NOT EXISTS node_rollup_state (
  id           BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
  -- Every hour STRICTLY BEFORE this has been rolled up.
  rolled_up_to TIMESTAMPTZ,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO node_rollup_state (id, rolled_up_to) VALUES (true, NULL)
  ON CONFLICT (id) DO NOTHING;

-- The rollup groups the whole window by hour; without this it is a seq scan
-- per pass. Partial on the call predicate because that is all it ever reads.
CREATE INDEX IF NOT EXISTS idx_nre_rollup_time
  ON node_radio_events (received_at)
  WHERE event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%';
