-- Rights handling for The Wire: per-post license, attribution/credit, a
-- publish-time rights affirmation (safe-harbor paper trail), and a DMCA-style
-- notice-and-takedown queue.
--
-- License codes (validated in app, not a DB CHECK, so the set can grow without
-- a migration): 'credit' (Credit required — DEFAULT), 'display' (All rights
-- reserved, display only), 'public' (Public domain).
--
-- Takedown model: an upheld notice soft-removes the target (status='removed')
-- AND stamps taken_down_at, which the public detail endpoint turns into a
-- tombstone ("removed in response to a rights complaint") instead of a 404,
-- so existing links explain themselves. The row is retained for the audit
-- trail. A normal moderator removal (no taken_down_at) still 404s.

ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS license         TEXT NOT NULL DEFAULT 'credit';
ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS credit          TEXT;
ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS rights_affirmed BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS taken_down_at   TIMESTAMPTZ;

ALTER TABLE articles ADD COLUMN IF NOT EXISTS license         TEXT NOT NULL DEFAULT 'credit';
ALTER TABLE articles ADD COLUMN IF NOT EXISTS credit          TEXT;
ALTER TABLE articles ADD COLUMN IF NOT EXISTS rights_affirmed BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE articles ADD COLUMN IF NOT EXISTS taken_down_at   TIMESTAMPTZ;

-- DMCA-style takedown notices. Public intake (anyone may file); reviewed by
-- owner/team_member on the staff Requests tab.
CREATE TABLE IF NOT EXISTS wire_takedowns (
  id               TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  target_type      TEXT NOT NULL CHECK (target_type IN ('media_post','article')),
  target_id        TEXT NOT NULL,
  target_title     TEXT,                    -- denormalised for the review queue
  reporter_name    TEXT NOT NULL,
  reporter_email   TEXT NOT NULL,
  reporter_org     TEXT,
  complaint        TEXT NOT NULL,            -- description of the alleged infringement
  original_url     TEXT,                     -- where the complainant's original work lives
  good_faith       BOOLEAN NOT NULL DEFAULT false,  -- "good-faith belief" statement
  accuracy         BOOLEAN NOT NULL DEFAULT false,  -- "accurate, and I'm authorised" statement
  status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','upheld','rejected','withdrawn')),
  action_note      TEXT,
  reviewed_by      TEXT,
  reviewed_by_name TEXT,
  reviewed_at      TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wire_takedowns_pending
  ON wire_takedowns (status, created_at DESC);
