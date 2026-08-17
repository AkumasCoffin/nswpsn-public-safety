-- Manually-awarded profile tags (badges).
--
-- Separate from user_roles on purpose: roles GRANT PERMISSIONS and are checked
-- on every privileged request, tags are decoration and grant nothing. Keeping
-- them in one table would mean a mis-typed badge could widen someone's access,
-- and every role check would have to filter out the cosmetic rows.
--
-- The vocabulary lives in services/userTags.ts, not in a CHECK constraint here:
-- adding a badge should be a one-line code change and a deploy, not a
-- migration. Unknown tags are rejected at the API boundary instead.
--
-- granted_by/granted_at are kept because these are awarded by hand — when a
-- contributor asks why someone else has a badge, the answer should be
-- recoverable.
CREATE TABLE IF NOT EXISTS user_tags (
  user_id    TEXT NOT NULL,
  tag        TEXT NOT NULL,
  granted_by TEXT,
  granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, tag)
);

-- The only read pattern: every tag for a set of users (profile pages, and the
-- byline chips on Wire posts and comments, which fetch in bulk per feed page).
CREATE INDEX IF NOT EXISTS idx_user_tags_user ON user_tags (user_id);

-- "First Contributor" is awarded automatically to whoever posts first, so two
-- people publishing in the same moment could both see "nobody holds it yet"
-- and both be granted it. Enforcing single-holder here rather than in the
-- application makes the race resolve deterministically: one INSERT wins, the
-- other violates the index and is swallowed as a no-op.
--
-- It also means staff must remove the badge before re-awarding it, which is
-- the correct behaviour for a uniquely-held title. The API turns the
-- constraint violation into a readable error rather than a 500.
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_tags_first_contributor_unique
  ON user_tags (tag) WHERE tag = 'first_contributor';
