-- Whether a contributor wants their media watermarked BY DEFAULT.
--
-- The compose pages used to keep this in localStorage only, so the choice was
-- per-browser and silently reset on a new device. It is a real account
-- preference: stored here, editable from the profile modal, and used as the
-- initial state of the compose toggle (which still overrides it per post).
ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS watermark_default BOOLEAN NOT NULL DEFAULT false;
