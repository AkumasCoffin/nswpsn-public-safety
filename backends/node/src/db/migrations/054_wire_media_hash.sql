-- De-duplicate Wire images by content hash: the browser computes a SHA-256 of
-- the optimised image and sends it when requesting an upload URL. If that hash
-- already exists we reuse the stored R2 object instead of re-uploading. Because
-- an R2 object can now be shared by more than one wire_media row, deletion is
-- ref-counted (only delete the object when no row references its key).
ALTER TABLE wire_media ADD COLUMN IF NOT EXISTS hash TEXT;
CREATE INDEX IF NOT EXISTS idx_wire_media_hash ON wire_media (hash) WHERE hash IS NOT NULL;
