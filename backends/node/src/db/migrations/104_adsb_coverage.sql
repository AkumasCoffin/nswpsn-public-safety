-- The coverage envelope of each ADS-B receiver: how far it hears, per bearing.
--
-- 72 NUMBERS A DAY PER NODE, not a track archive.
--
-- The first instinct was a per-node copy of adsb_tracks (migration 103). That
-- stores the same aircraft's geometry once per receiver that heard it — an
-- aircraft heard by four nodes written five times over, and a single busy
-- metropolitan receiver running several times the row count of the entire
-- national table, for a strictly narrower picture.
--
-- What the coverage view is actually for compresses to almost nothing. Its own
-- caption says it: "which bearings this antenna actually hears, and how far. A
-- gap in the fan is an obstruction; a short radius on one bearing is terrain."
-- That is a polar range plot — graphs1090's range chart — and a polar range
-- plot is one maximum per bearing. The live tracks stay in memory, where they
-- belong: they answer "what is crossing right now", which nobody needs after
-- the fact.
--
-- ONE ROW PER NODE PER DAY, rather than one accumulating row per node. An
-- all-time envelope can only ever grow, so it would go on claiming a lobe an
-- antenna stopped hearing weeks ago — exactly the fault the view exists to
-- reveal. Daily rows let a reader union a window AND compare recent against
-- older, which is what turns "your north-west lobe died three days ago" from
-- invisible into obvious.

CREATE TABLE IF NOT EXISTS node_adsb_coverage (
  -- TEXT, matching nodes.id (gen_random_uuid()::text) and every other
  -- node-referencing table. Not uuid: Postgres rejects the FK outright.
  node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  -- Sydney-local date, like node_adsb_daily. A UTC boundary would split an
  -- evening's flying across two rows.
  day     date NOT NULL,
  -- Exactly 72 entries. Index is the compass bearing from the receiver to the
  -- aircraft divided by 5 degrees, so index 0 is due north and index 18 due
  -- east; the value is the furthest aircraft heard on that bearing that day,
  -- in km, or null for a bearing nothing was heard on. A null is meaningful —
  -- it is the gap in the fan — so it is stored rather than collapsed to zero.
  buckets jsonb NOT NULL,
  PRIMARY KEY (node_id, day)
);

-- One receiver's envelope over a trailing window, newest first. The only read
-- pattern there is.
CREATE INDEX IF NOT EXISTS idx_node_adsb_coverage_node_day
  ON node_adsb_coverage (node_id, day DESC);
