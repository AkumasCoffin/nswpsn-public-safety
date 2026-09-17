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
  -- [[secondsIntoTheHour, lat, lon, altFt|null], ...] in time order.
  points      JSONB       NOT NULL,
  PRIMARY KEY (node_id, hex, hour_bucket)
);

-- The only read: "this receiver's tracks over the last N hours".
CREATE INDEX IF NOT EXISTS idx_node_adsb_tracks_node_hour
  ON node_adsb_tracks (node_id, hour_bucket DESC);
