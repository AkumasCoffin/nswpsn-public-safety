-- Per-post edit log for The Wire.
--
-- Once something is public, changing it quietly is the problem: a reader who
-- saw the original has no way to know the caption, the agency tags or the
-- attached photos are no longer what they were. This records each edit made
-- AFTER publication so the post can show its own history.
--
-- Deliberately NOT a version store. It keeps a list of which fields changed —
-- with before/after values only for short ones — never a copy of the body. A
-- 100k-character article edited ten times would otherwise put a megabyte of
-- duplicated prose in this table to power a UI that only ever says "the body
-- was edited".
--
-- Drafts, pending and rejected posts are never logged: nothing public changed,
-- so there is nothing to disclose. The gate lives in api/wire.ts (the edit is
-- only recorded when the PREVIOUS status was 'published'), because that's where
-- the previous status is known.
CREATE TABLE IF NOT EXISTS wire_edits (
  id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  parent_type TEXT NOT NULL CHECK (parent_type IN ('media_post','article')),
  parent_id   TEXT NOT NULL,
  -- Who made the edit — the author, a co-author, or a moderator. Denormalised
  -- name so the log still reads correctly if the account is later removed
  -- (same reasoning as media_posts.author_name).
  editor_id   TEXT NOT NULL,
  editor_name TEXT,
  -- [{ field, label, from?, to? }] — see services/wireEdits.ts.
  changes     JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The only read: this post's history, newest first. Soft reference to the
-- parent (no FK), matching every other Wire relation.
CREATE INDEX IF NOT EXISTS idx_wire_edits_parent
  ON wire_edits (parent_type, parent_id, created_at DESC);
