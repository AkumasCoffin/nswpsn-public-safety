-- 142: the 3D coverage view ("lightcone") for ADS-B receivers.
--
-- The polar envelope (migration 104) answers "how far on each bearing" and
-- nothing else. The lightcone answers it per ALTITUDE as well, and adds what
-- the receiver should have heard but didn't, against what the terrain allows.
--
-- Three additions:

-- 1. How high the antenna is above the ground. Line of sight depends on it as
--    much as on the pin, and nothing recorded it. Null = not set; the backend
--    assumes a typical rooftop mast until the owner fills it in.
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS antenna_height_m real;

-- 2. The day's sparse voxel counts, next to the envelope they refine. Each is
--    a JSON object of { cellIndex: count }, where
--      cellIndex = (altitudeSlice * 72 + bearingBucket) * 60 + distanceRing
--    with 500 m altitude slices (0-13 km), 5-degree bearings and 10 km rings
--    (0-600 km). `heard` is what the receiver reported; `missed` is aircraft
--    the public feed saw inside the line-of-sight limit that this receiver did
--    not report at the time. Null on days from before this migration.
ALTER TABLE node_adsb_coverage ADD COLUMN IF NOT EXISTS heard jsonb;
ALTER TABLE node_adsb_coverage ADD COLUMN IF NOT EXISTS missed jsonb;

-- 3. The terrain line-of-sight limit for each receiver, computed once per pin
--    and antenna height from an elevation profile along each bearing. `limits`
--    is 26 arrays (one per 500 m altitude slice, by its top) of 72 distances
--    in km. Recomputed only when the pin or the height changes.
CREATE TABLE IF NOT EXISTS node_adsb_terrain (
  node_id     TEXT PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
  lat         double precision NOT NULL,
  lon         double precision NOT NULL,
  height_m    real NOT NULL,
  ground_m    real NOT NULL,
  limits      jsonb NOT NULL,
  computed_at timestamptz NOT NULL DEFAULT now()
);
