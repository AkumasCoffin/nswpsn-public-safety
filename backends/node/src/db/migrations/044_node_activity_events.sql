-- 044: radio Data-tab rows now come from vce ACTIVITY events, not rdio
-- call uploads.
--
-- Why: activity events carry the REAL P25 identity (wacn/systemId) and
-- per-event site/action/encryption; rdio call uploads carry rdio-config
-- system numbers. Mixing both as row sources would double-count, so from
-- this migration on:
--   - /api/node-ingest/activity (JSON batches from the Go agent) is the
--     ONLY source of node_radio_events rows (services/nodeEvents.ts
--     recordActivityEvents);
--   - the rdio call-upload relay no longer inserts rows — it just marks
--     the closest matching event row recorded=true + audio_bytes
--     (markRecorded), meaning "audio exists in central rdio for this".
--
-- No production radio data exists yet, so the radio detail + radio hourly
-- tables are TRUNCATEd and re-keyed. Pager tables are untouched.
--
-- Semantic change: the `system` column now stores the P25 systemId (from
-- the decoded network), NOT the rdio-config system number.
--
-- Dropped: site_source. It recorded WHERE the relay-time site guess came
-- from (event|channel|context headers); activity events carry rfss/site/
-- nac natively per event, so the provenance concept is dead.
--
-- Kept (always NULL from ingest for now): talkgroup_label / system_label.
-- Activity events carry no labels; label resolution is planned to come
-- from the global agencies config at READ time later. Keeping the columns
-- keeps the read path stable.

-- Radio only — node_pager_events / node_pager_hourly must be preserved.
TRUNCATE node_radio_events, node_radio_hourly, node_radio_hourly_sys;

ALTER TABLE node_radio_events
  ADD COLUMN IF NOT EXISTS stream_id       TEXT,
  ADD COLUMN IF NOT EXISTS source_event_id BIGINT,
  ADD COLUMN IF NOT EXISTS action          TEXT,
  ADD COLUMN IF NOT EXISTS event_type      TEXT,
  ADD COLUMN IF NOT EXISTS timeslot        SMALLINT,
  ADD COLUMN IF NOT EXISTS encrypted       BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS wacn            INTEGER,
  ADD COLUMN IF NOT EXISTS recorded        BOOLEAN NOT NULL DEFAULT false,
  DROP COLUMN IF EXISTS site_source;

-- Idempotent re-sends: the agent may re-post a batch after a timeout; the
-- (node, stream, event-id) triple identifies one event exactly once.
-- recordActivityEvents INSERTs ON CONFLICT ... DO NOTHING against this.
-- (Both stream_id and source_event_id are always non-null on the ingest
-- path; NULLs — none exist post-truncate — would not conflict.)
CREATE UNIQUE INDEX IF NOT EXISTS uq_nre_node_stream_event
  ON node_radio_events(node_id, stream_id, source_event_id);

-- markRecorded lookup: closest unrecorded row for (node, talkgroup) near a
-- timestamp.
CREATE INDEX IF NOT EXISTS idx_nre_node_tg_time
  ON node_radio_events(node_id, talkgroup, received_at);

-- Pre-existing indexes (idx_nre_sys_tg_time, idx_nre_time, idx_nre_node_time,
-- idx_nre_unit_time, idx_nre_logical) reference no dropped columns and stay
-- valid as-is.
