-- The Wire: Fleet tab. A directory of emergency-service vehicles posted by
-- Wire contributors -- each row is a reference sheet (callsign, agency,
-- specs, radio IDs, one photo), not a news post. Same permission model as
-- articles: canFeedMedia creates, author/admin edits, canModerateWire
-- reviews the pending queue and soft-removes.
--
-- One image only, stored as an R2 object key on the row itself -- no
-- wire_media children. Views only (no likes/comments); the dedup rides the
-- existing wire_views table, whose CHECK gains the 'fleet' parent type.

CREATE TABLE IF NOT EXISTS fleet_vehicles (
  id               TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  author_id        TEXT NOT NULL,
  author_name      TEXT,
  callsign         TEXT NOT NULL,          -- serves as the title
  state            TEXT NOT NULL,          -- NSW|VIC|QLD|WA|SA|TAS|ACT|NT
  lga              TEXT NOT NULL,
  suburb           TEXT,                   -- optional
  agency           TEXT NOT NULL,          -- department name (fleet-vocab.js)
  agency_category  TEXT,                   -- police|fire|ses|ambulance|marine|rescue|other
  station          TEXT,
  cad_code         TEXT,
  aerial_id        TEXT,
  vehicle_type     TEXT,
  registration     TEXT,
  make_model       TEXT,                   -- Make / Model / Cab Chassis
  production_year  INTEGER,
  crew_capacity    INTEGER,
  radio_ids        JSONB NOT NULL DEFAULT '{"cab":[],"mobile":[]}'::jsonb,
  specs            JSONB NOT NULL DEFAULT '{}'::jsonb,  -- water_tank_l, foam_tank_l, cafs, ba_sets, stretchers
  image_key        TEXT,                   -- single R2 object; URL derived
  views            BIGINT NOT NULL DEFAULT 0,
  status           TEXT NOT NULL DEFAULT 'published'
                     CHECK (status IN ('published','pending','rejected','removed')),
  review_note      TEXT,
  reviewed_by      TEXT,
  reviewed_by_name TEXT,
  reviewed_at      TIMESTAMPTZ,
  removed_by       TEXT,
  removed_by_name  TEXT,
  removed_at       TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fleet_vehicles_feed
  ON fleet_vehicles (status, created_at DESC);

-- View dedup rides the existing table; teach its CHECK the new parent type.
ALTER TABLE wire_views DROP CONSTRAINT IF EXISTS wire_views_parent_type_check;
ALTER TABLE wire_views ADD CONSTRAINT wire_views_parent_type_check
  CHECK (parent_type IN ('media_post','article','fleet'));
