-- "The Wire" — news & media feature. Contributors with the media_feeder role
-- publish media posts (photo/video sets) and articles (Markdown news pieces)
-- with unit tagging, agency tagging, a location (map pin OR NSW planning
-- region), a single optional linked incident, and view tracking.
--
-- Ownership is post-moderation: the author edits/deletes their own; an
-- owner/team_member can soft-remove any (status='removed'). The media_feeder
-- role itself is what's vetted at signup, so there is no per-post approval
-- queue (unlike agency_data_change in 048).
--
-- Storage: images live in Cloudflare Images (we store the image id and serve
-- named variants); videos live in Cloudflare R2 (we store the object key and
-- serve from a public base URL). Neither binary touches this Postgres or the
-- origin disk.

-- Photo/video sets. "units on scene" is NOT a column — it's derived as the
-- distinct non-null units of this post's wire_media rows.
CREATE TABLE IF NOT EXISTS media_posts (
  id             TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  author_id      TEXT NOT NULL,
  author_name    TEXT,
  title          TEXT NOT NULL,
  caption        TEXT NOT NULL DEFAULT '',
  location_type  TEXT NOT NULL DEFAULT 'pin' CHECK (location_type IN ('pin','region')),
  region         TEXT,                       -- region name when location_type='region'
  lat            DOUBLE PRECISION,           -- pin coords, or region centroid
  lng            DOUBLE PRECISION,
  agencies       JSONB NOT NULL DEFAULT '[]'::jsonb,   -- agency slugs from agency-data.json
  incident_id    TEXT,                       -- soft ref to incidents.id (no FK; incidents purge)
  views          BIGINT NOT NULL DEFAULT 0,
  status         TEXT NOT NULL DEFAULT 'published' CHECK (status IN ('published','removed')),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  removed_by     TEXT,
  removed_by_name TEXT,
  removed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_media_posts_feed
  ON media_posts (status, created_at DESC);

-- Markdown news articles. Same tagging as media_posts, plus slug/excerpt/body
-- and a draft->published lifecycle. cover image is the wire_media row flagged
-- is_cover.
CREATE TABLE IF NOT EXISTS articles (
  id             TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  author_id      TEXT NOT NULL,
  author_name    TEXT,
  title          TEXT NOT NULL,
  slug           TEXT NOT NULL UNIQUE,
  excerpt        TEXT NOT NULL DEFAULT '',
  body           TEXT NOT NULL DEFAULT '',    -- Markdown
  location_type  TEXT NOT NULL DEFAULT 'pin' CHECK (location_type IN ('pin','region')),
  region         TEXT,
  lat            DOUBLE PRECISION,
  lng            DOUBLE PRECISION,
  agencies       JSONB NOT NULL DEFAULT '[]'::jsonb,
  incident_id    TEXT,
  views          BIGINT NOT NULL DEFAULT 0,
  status         TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','published','removed')),
  published_at   TIMESTAMPTZ,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  removed_by     TEXT,
  removed_by_name TEXT,
  removed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_articles_feed
  ON articles (status, published_at DESC);

-- One row per image or video attached to a media_post or article. Per-item
-- unit tagging lives here; app logic enforces the caps (media_post: 6 img +
-- 2 vid; article: 4 img incl. 1 cover + 2 vid) and the single-cover rule.
CREATE TABLE IF NOT EXISTS wire_media (
  id                 TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  parent_type        TEXT NOT NULL CHECK (parent_type IN ('media_post','article')),
  parent_id          TEXT NOT NULL,
  kind               TEXT NOT NULL CHECK (kind IN ('image','video')),
  cf_image_id        TEXT,          -- Cloudflare Images id (kind='image', or a video poster)
  r2_key             TEXT,          -- R2 object key (kind='video')
  poster_cf_image_id TEXT,          -- optional Cloudflare Images poster for a video
  duration_seconds   INTEGER,
  is_cover           BOOLEAN NOT NULL DEFAULT false,
  unit               TEXT,          -- per-item callsign (uppercased), nullable
  sort_order         INTEGER NOT NULL DEFAULT 0,
  width              INTEGER,
  height             INTEGER,
  bytes              BIGINT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wire_media_parent
  ON wire_media (parent_type, parent_id, sort_order);

-- View de-duplication: at most one counted view per (item, viewer, day). The
-- viewer_hash is hash(ip + user-agent + VIEW_HASH_SALT); a first insert per
-- key bumps the parent's views counter.
CREATE TABLE IF NOT EXISTS wire_views (
  parent_type  TEXT NOT NULL CHECK (parent_type IN ('media_post','article')),
  parent_id    TEXT NOT NULL,
  viewer_hash  TEXT NOT NULL,
  day          DATE NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (parent_type, parent_id, viewer_hash, day)
);
