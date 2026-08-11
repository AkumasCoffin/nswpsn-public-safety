-- Small runtime key/value settings the staff UI can toggle without a redeploy.
-- First use: 'wire_approval_required' — 'true' (default) means contributor posts
-- go to the pending queue for approval; 'false' means they publish instantly.
CREATE TABLE IF NOT EXISTS app_settings (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_by TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
