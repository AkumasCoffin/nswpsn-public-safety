-- The Wire images moved from Cloudflare Images to R2 (same bucket as video),
-- to stay on R2's free tier. The browser downscales + re-encodes each photo to
-- WebP before upload (optimisation + EXIF/GPS stripping), so we just store the
-- R2 object key in wire_media.r2_key (already present). Video posters also move
-- to R2 — this adds a column for their key. The legacy cf_image_id /
-- poster_cf_image_id columns stay (nullable, unused for new rows).
ALTER TABLE wire_media ADD COLUMN IF NOT EXISTS poster_r2_key TEXT;
