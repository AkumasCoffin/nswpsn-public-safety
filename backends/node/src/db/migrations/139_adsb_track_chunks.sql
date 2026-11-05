-- Aircraft track history, one row per MINUTE holding every aircraft heard in it.
--
-- Replaces adsb_tracks (migration 103), which kept one row per aircraft per
-- hour and rewrote it from the in-memory trail every minute: on production that
-- was ~43 rewrites of every row (1.4 M updates against 32 k inserts in two
-- days), 526 MB for ~10 MB a day of positions, and a stored hour that kept
-- getting coarser as the live trail simplified itself. The only history read is
-- "every aircraft between T - trail and T", which is a time slice, so the rows
-- are time slices: readsb's heatmap does the same, one block per interval.
--
-- A minute is written ONCE, shortly after it closes, and never updated. No dead
-- rows, no index churn, and a minute on disk is final, so a history response for
-- it can be cached hard. A 30-minute window is at most 31 primary-key rows.
--
-- `seq` lets more than one process contribute to a minute without coordination:
-- a graceful shutdown writes the minute it was part-way through, the next
-- process writes its part of the same minute, and the reader unions them. Live
-- writes take the next free seq; the one-off backfill from adsb_tracks writes
-- seq -1, so re-running it is a no-op.
--
-- `aircraft` is json, not jsonb: nothing queries inside it, and json keeps the
-- text as written, about half the size of the jsonb form. Shape:
--   { "<hex>": { "c": callsign, "r": reg, "t": type, "e": esTag,
--                "s": [sources heard this minute],
--                "p": [[msIntoMinute, lat, lon, altFt|null], ...] } }
CREATE TABLE IF NOT EXISTS adsb_track_chunks (
  minute   TIMESTAMPTZ NOT NULL,
  seq      SMALLINT    NOT NULL,
  aircraft JSON        NOT NULL,
  PRIMARY KEY (minute, seq)
);

-- lz4 decompresses several times faster than the default pglz, which is what a
-- history read pays for every row it touches.
ALTER TABLE adsb_track_chunks ALTER COLUMN aircraft SET COMPRESSION lz4;

-- Registration and type per aircraft, for the receiver hover labels
-- (services/nodes/adsbNodeView.ts). A receiver's decoder never knows a
-- registration; the aggregators' databases do. A time-sliced table cannot answer
-- "what is hex X's registration" by index, so it is kept here, written only when
-- it changes (and refreshed daily so retention never takes a live aircraft).
CREATE TABLE IF NOT EXISTS adsb_identities (
  hex        TEXT        PRIMARY KEY,
  reg        TEXT,
  type       TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
