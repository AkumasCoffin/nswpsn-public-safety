-- Short public bio on contributor profiles, shown on profile.html alongside
-- the avatar and social links. Plain text (rendered escaped) — no markdown, so
-- there's nothing to sanitise beyond the length cap enforced in api/profiles.ts.
ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS bio TEXT;
