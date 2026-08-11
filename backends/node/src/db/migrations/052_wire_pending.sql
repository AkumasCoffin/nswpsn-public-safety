-- The Wire switches to PRE-moderation: a contributor's post/article is 'pending'
-- until an owner/team_member approves it, at which point it becomes 'published'
-- and public. Owners/team_members posting publish instantly (they're the
-- reviewers). Rejected items go to 'rejected' (the author can edit to resubmit).
--
-- Expand the status sets (drop the inline CHECKs from 049/050 and re-add) and
-- add reviewer columns for the approve/reject audit trail.
ALTER TABLE media_posts DROP CONSTRAINT IF EXISTS media_posts_status_check;
ALTER TABLE media_posts ADD CONSTRAINT media_posts_status_check
  CHECK (status IN ('pending','published','removed','rejected'));

ALTER TABLE articles DROP CONSTRAINT IF EXISTS articles_status_check;
ALTER TABLE articles ADD CONSTRAINT articles_status_check
  CHECK (status IN ('draft','pending','published','removed','rejected'));

ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS reviewed_by      TEXT;
ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS reviewed_by_name TEXT;
ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS reviewed_at      TIMESTAMPTZ;
ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS review_note      TEXT;

ALTER TABLE articles ADD COLUMN IF NOT EXISTS reviewed_by      TEXT;
ALTER TABLE articles ADD COLUMN IF NOT EXISTS reviewed_by_name TEXT;
ALTER TABLE articles ADD COLUMN IF NOT EXISTS reviewed_at      TIMESTAMPTZ;
ALTER TABLE articles ADD COLUMN IF NOT EXISTS review_note      TEXT;

CREATE INDEX IF NOT EXISTS idx_media_posts_pending ON media_posts (status, created_at DESC) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_articles_pending ON articles (status, created_at DESC) WHERE status = 'pending';
