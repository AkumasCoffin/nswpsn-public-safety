-- Personal referral codes for contributors (feeder:radio, feeder:pager,
-- wire:contributor, map:editor, owner — see canRefer in services/auth/roles.ts).
-- A contributor's profile modal shows /signup?as=contributor&ref=<code>;
-- signups submitted through it record who vouched for them.
--
-- Separate table (the user_tags/067 precedent) — NOT a user_profiles column,
-- because user_profiles is publicly readable via GET /api/profiles/:userId
-- and a referral code should only ever be shown to its owner.
CREATE TABLE IF NOT EXISTS referral_codes (
  user_id    TEXT PRIMARY KEY,
  code       TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Denormalised attribution on the signup request (agency_data_change
-- created_by/created_by_name precedent, 048). Attribution only — approval
-- flow is unchanged; the name is resolved once at submit time so the staff
-- list needs no join and survives the referrer being renamed.
ALTER TABLE editor_requests ADD COLUMN IF NOT EXISTS referred_by TEXT;
ALTER TABLE editor_requests ADD COLUMN IF NOT EXISTS referred_by_name TEXT;

-- Stats read (profile card): COUNT(*) / COUNT(*) FILTER (status='approved')
-- per referrer.
CREATE INDEX IF NOT EXISTS idx_editor_requests_referred_by
  ON editor_requests (referred_by) WHERE referred_by IS NOT NULL;
