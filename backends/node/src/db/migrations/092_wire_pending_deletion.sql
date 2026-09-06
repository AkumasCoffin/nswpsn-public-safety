-- Deleting an article or fleet vehicle stops being instant: the post is
-- hidden immediately (deleted_at set) and only PURGED -- row, images, the
-- lot -- after a 5-day recovery window (services/wirePurge.ts sweeps
-- hourly). Until then the author can recover it from the Pending-deletion
-- area on their profile. Moderator removals (status='removed') are a
-- different thing and stay permanent tombstones.

ALTER TABLE articles       ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE fleet_vehicles ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_articles_pending_deletion
  ON articles (deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fleet_pending_deletion
  ON fleet_vehicles (deleted_at) WHERE deleted_at IS NOT NULL;
