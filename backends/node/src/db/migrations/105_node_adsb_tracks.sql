-- The paths aircraft took through one RECEIVER's coverage, kept across restarts.
--
-- The tracks map was memory-only, so every deploy emptied it and the eight-hour
-- window it advertises effectively never filled: a node with sixty-odd unique
-- aircraft in a day would show one, because the backend had restarted three
-- minutes earlier. The coverage envelope beside it was persisted and looked
-- fine, which made the map look broken rather than young.
--
-- Same shape as adsb_tracks (migration 103) — one row per aircraft per hour,
-- points as offsets into the hour — for the same reason: per-position rows
-- would be millions a day for something only ever read as a picture.
--
-- Why this is NOT just a filter over adsb_tracks, which already stores every
-- merged track with its `sources`: those rows only exist for aircraft that
-- reached the public merge, and a node whose feed is PAUSED contributes
-- nothing to it. The whole point of these diagnostics is that they keep
-- working when the feed is off, so a receiver's own tracks have to be recorded
-- from its own uploads.

CREATE TABLE IF NOT EXISTS node_adsb_tracks (
  -- TEXT, matching nodes.id and every other node-referencing table. Not uuid:
  -- Postgres rejects the FK outright.
  node_id     TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  hex         TEXT        NOT NULL,
  -- date_trunc('hour', ...) in UTC. Bounded first on every read, so the index
  -- below does the work.
  hour_bucket TIMESTAMPTZ NOT NULL,
  first_seen  TIMESTAMPTZ NOT NULL,
  last_seen   TIMESTAMPTZ NOT NULL,
  callsign    TEXT,
  -- How many uploads this aircraft appeared in during the hour, including the
  -- ones the decimation declines to store as points. The "how solidly was it
  -- held" signal a point count cannot give.
  reports     integer,
  -- Altitude is a per-HOUR scalar, not per point. The in-memory trace keeps
  -- lat/lon/time as parallel arrays and altitude as two scalars, deliberately
  -- — a fourth array would be 4000 aircraft x 240 altitudes stored to render
  -- one number per row of a table. The track map colours by aircraft, not by
  -- altitude, so nothing needs it per point.
  last_alt_ft real,
  max_alt_ft  real,
  -- [[secondsIntoTheHour, lat, lon, null], ...] in time order. The fourth
  -- slot keeps the shape of the global archive's points; it is always null
  -- here, for the reason above.
  points      JSONB       NOT NULL,
  PRIMARY KEY (node_id, hex, hour_bucket)
);

-- Idempotent, in case this migration already ran before the columns existed.
ALTER TABLE node_adsb_tracks ADD COLUMN IF NOT EXISTS reports     integer;
ALTER TABLE node_adsb_tracks ADD COLUMN IF NOT EXISTS last_alt_ft real;
ALTER TABLE node_adsb_tracks ADD COLUMN IF NOT EXISTS max_alt_ft  real;

-- The only read: "this receiver's tracks over the last N hours".
CREATE INDEX IF NOT EXISTS idx_node_adsb_tracks_node_hour
  ON node_adsb_tracks (node_id, hour_bucket DESC);
