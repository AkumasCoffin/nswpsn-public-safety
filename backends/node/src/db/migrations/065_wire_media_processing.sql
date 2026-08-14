-- Video post-processing state, for the server-side ffmpeg pipeline.
--
-- Videos are uploaded raw straight to R2 (presigned PUT, bypassing the origin's
-- 50Mbps uplink). The backend then pulls each one, transcodes it (burn the
-- watermark, normalise to 1080p/~2.5Mbps, strip metadata, cut a poster) and
-- swaps in the result.
--
-- The state lives on the media row rather than in a jobs table, following the
-- "decide from observable state" precedent in services/archiveLatestBackfill.ts.
-- That's what makes this survive a restart: nothing in this backend persists
-- in-flight work, so a row left 'pending' is simply picked up by the next sweep
-- instead of being lost.
--
--   NULL      images, and every video that predates this pipeline (left alone)
--   pending   uploaded, waiting for / mid transcode
--   done      transcoded; r2_key + poster_r2_key now point at the output
--   failed    gave up after repeated errors — the ORIGINAL file is still in
--             place and playable, just un-normalised
ALTER TABLE wire_media ADD COLUMN IF NOT EXISTS process_state    TEXT;
ALTER TABLE wire_media ADD COLUMN IF NOT EXISTS process_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE wire_media ADD COLUMN IF NOT EXISTS process_error    TEXT;
ALTER TABLE wire_media ADD COLUMN IF NOT EXISTS processed_at     TIMESTAMPTZ;

-- The worker's only lookup. Partial so it stays tiny however big wire_media
-- grows (mirrors idx_media_posts_pending in migration 052).
CREATE INDEX IF NOT EXISTS idx_wire_media_pending_video
  ON wire_media(created_at)
  WHERE kind = 'video' AND process_state = 'pending';
