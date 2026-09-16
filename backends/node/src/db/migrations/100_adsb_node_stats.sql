-- ADS-B feeder node statistics, one row per node per day.
--
-- Deliberately an AGGREGATE, never per-position rows. A fleet of receivers
-- uploading a snapshot every 5s is ~17k uploads/node/day carrying hundreds of
-- aircraft each; storing positions would add millions of rows a day for
-- numbers nobody queries individually. The live picture already lives in
-- LiveStore (and on the map); this table exists purely to answer "how has this
-- receiver been performing" on the staff Data tab.
--
-- Counters accumulate (+=), maxima take GREATEST, so the ingest path can
-- upsert one row per flush with no read-modify-write.

CREATE TABLE IF NOT EXISTS node_adsb_daily (
  -- TEXT, matching nodes.id (gen_random_uuid()::text) and every other
  -- node-referencing table. Not uuid: Postgres rejects the FK outright.
  node_id      TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  -- Sydney-local date: the rest of the site reports days in local time, and a
  -- UTC boundary would split an evening's flying across two rows.
  day          date NOT NULL,
  -- Uploads accepted from this node (rate-limited drops are not counted).
  snapshots    integer NOT NULL DEFAULT 0,
  -- Aircraft-position records shipped across all of the day's snapshots.
  -- Counts REPORTS, not distinct aircraft — one aircraft seen for ten minutes
  -- contributes ~120 of these.
  positions    bigint  NOT NULL DEFAULT 0,
  -- Peak aircraft carried by a single snapshot: the honest "how busy did it
  -- get" number, unaffected by upload cadence.
  max_aircraft integer NOT NULL DEFAULT 0,
  -- Furthest aircraft the decoder reported, km. NULL until a node reports it;
  -- dump1090 only computes range when it knows its own lat/lon, which is why
  -- the exact antenna pin is mandatory for this kind.
  max_range_km real,
  -- Peak decoder message rate, messages/sec.
  msg_rate_max real,
  -- Unique aircraft tracked, from the decoder's own counter. That counter is
  -- monotonic PER DECODER RUN, so GREATEST across a restart undercounts the
  -- day (the post-restart run starts from zero). Accepted: it is a floor, and
  -- the alternative is tracking distinct hexes server-side per node per day.
  tracks_max   integer,
  PRIMARY KEY (node_id, day)
);

-- The Data tab reads one node over a trailing window, newest first.
CREATE INDEX IF NOT EXISTS idx_node_adsb_daily_node_day
  ON node_adsb_daily (node_id, day DESC);
