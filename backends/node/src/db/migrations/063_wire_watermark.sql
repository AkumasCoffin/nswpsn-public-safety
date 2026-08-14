-- Per-post watermark flag.
--
-- Photos are watermarked at UPLOAD time (the composer re-encodes them through a
-- canvas, so the text is genuinely burned into the stored file and this column
-- is only a record of what was done). Video can't be burned in without a
-- transcode, so for video this flag is what tells the player to draw the
-- contributor's name over the clip — hence it has to be stored, not inferred.
ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS watermark BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE articles    ADD COLUMN IF NOT EXISTS watermark BOOLEAN NOT NULL DEFAULT false;
