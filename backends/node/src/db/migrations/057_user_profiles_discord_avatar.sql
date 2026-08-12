-- Store the user's Discord avatar URL as a PUBLIC fallback so their profile
-- picture shows to other viewers even when they haven't set a custom pfp.
-- (The Discord avatar otherwise only lives in Supabase user_metadata, which is
-- readable by that user alone — so other viewers saw only the initial letter.)
-- The custom avatar_key still takes precedence when set.
ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS discord_avatar_url TEXT;
