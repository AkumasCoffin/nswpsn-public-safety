-- Public contributor profiles: a custom profile picture (overrides the Discord
-- avatar) and social links, shown on their Wire posts. Keyed by the Supabase
-- user id. Lives in the backend Postgres (not Supabase user_metadata) so any
-- viewer can read another user's public profile — Supabase only exposes your
-- OWN metadata to the client.
CREATE TABLE IF NOT EXISTS user_profiles (
  user_id       TEXT PRIMARY KEY,
  display_name  TEXT,
  avatar_key    TEXT,          -- R2 object key for the custom pfp (served via public base)
  twitter       TEXT,
  facebook      TEXT,
  instagram     TEXT,
  youtube       TEXT,
  website       TEXT,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
