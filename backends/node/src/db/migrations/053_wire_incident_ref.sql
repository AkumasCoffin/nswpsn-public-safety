-- The Wire can link a post/article to ANY incident on the logs feed (RFS,
-- pager, traffic, user_incident, …), not just user-created ones. Store the logs
-- identity as JSONB {source, source_id, title} so the detail can name the
-- incident and deep-link into logs.html. The legacy incident_id column is kept
-- (= source_id) for back-compat.
ALTER TABLE media_posts ADD COLUMN IF NOT EXISTS incident JSONB;
ALTER TABLE articles ADD COLUMN IF NOT EXISTS incident JSONB;
