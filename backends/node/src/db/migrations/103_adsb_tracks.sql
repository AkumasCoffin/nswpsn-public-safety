-- Persisted aircraft tracks, so the map can be scrubbed back in time.
--
-- ONE ROW PER AIRCRAFT PER HOUR — not one row per position.
--
-- A per-position table over Australia is millions of rows a day for something
-- nobody queries a single position of. What the historical view actually needs
-- is already assembled in memory: sources/adsb.ts keeps a per-hex trail,
-- Douglas-Peucker simplified and capped, built from the MERGED snapshot. This
-- table is that structure spilled to disk, not a second ingest pipeline.
--
-- Bucketing by hour is what keeps the write cost flat. Only the current hour's
-- row is ever rewritten, so a closed hour is immutable and the UPDATE churn
-- (and the autovacuum bloat that follows it) is bounded to one hour's worth of
-- live aircraft. Volume lands around 20-25k rows/day; at the default 31-day
-- DATA_RETENTION_DAYS that is well under a million rows.
--
-- Multiple feeder nodes need no special handling here, and that is a
-- consequence of WHERE the trail is built: updateTrails runs on the output of
-- mergeAircraft, which unions every aggregator and every node by hex before a
-- point is recorded. So one aircraft is one track however many receivers heard
-- it. Archiving each node's uploads instead would multiply rows per receiver
-- and force a dedupe on every read.
--
-- Estimated (dead-reckoned) positions never reach this table — see the filter
-- on updateTrails in sources/adsb.ts. Every point here was observed.

CREATE TABLE IF NOT EXISTS adsb_tracks (
  -- Lowercase ICAO hex, as served. readsb prefixes non-ICAO TIS-B ids with
  -- '~', which is kept: they are real targets and dropping the prefix would
  -- collide them with genuine ICAO addresses.
  hex         TEXT        NOT NULL,
  -- date_trunc('hour', ...) in UTC. The partition key in spirit: every read
  -- bounds on it first so the index range scan does the work.
  hour_bucket TIMESTAMPTZ NOT NULL,
  -- First and last point in THIS hour's slice, not the whole flight. A query
  -- for an instant filters on these to skip hours the aircraft was present in
  -- but not during the window asked for.
  first_seen  TIMESTAMPTZ NOT NULL,
  last_seen   TIMESTAMPTZ NOT NULL,
  -- Identity as known at flush time. Nullable and allowed to improve: an
  -- aircraft is usually tracked for a while before it transmits a callsign,
  -- so a later flush may fill in what the first one could not.
  callsign    TEXT,
  reg         TEXT,
  type        TEXT,
  es_tag      TEXT,
  -- The union of the source ids that contributed to this hour, including
  -- `node:<name>` for our own receivers. Stored so the map's "only my
  -- receiver" filter keeps working when scrubbing back; losing it would make
  -- history a downgrade from the live view.
  sources     TEXT[]      NOT NULL DEFAULT '{}',
  -- [[secondsIntoTheHour, lat, lon, altFt|null], ...] in time order.
  -- Seconds-into-the-hour rather than epoch ms: the values stay small
  -- integers, and the hour is already on the row.
  points      JSONB       NOT NULL,
  PRIMARY KEY (hex, hour_bucket)
);

-- The only read pattern: "every aircraft present between t0 and t1". The hour
-- bound comes first because it is what turns a table sweep into a range scan;
-- last_seen narrows within the two or three buckets that survive it.
CREATE INDEX IF NOT EXISTS idx_adsb_tracks_hour
  ON adsb_tracks (hour_bucket, last_seen);
