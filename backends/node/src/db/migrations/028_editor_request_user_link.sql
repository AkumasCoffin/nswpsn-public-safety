-- Link access requests to an existing Supabase account (Discord OAuth signup).
-- When a signed-in user submits the request form, their verified user id is
-- stored here; approval then assigns roles to that account directly instead
-- of creating a fresh email/password account.
ALTER TABLE editor_requests ADD COLUMN IF NOT EXISTS supabase_user_id TEXT;
