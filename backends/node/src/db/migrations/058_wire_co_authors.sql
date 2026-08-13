-- Co-authors on Wire posts (credit-only): a contributor can credit other
-- media_feeder users on their post/article. Stored denormalised as a JSONB
-- array of {id, name} (same pattern as agencies) — the original author_id still
-- owns edit/delete; co-authors are shown + get the post on their profile.
ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS co_authors JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE articles    ADD COLUMN IF NOT EXISTS co_authors JSONB NOT NULL DEFAULT '[]'::jsonb;
