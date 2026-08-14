-- The Wire: comments, likes, comment moderation and user notifications.
--
-- Keying follows the existing Wire child tables (wire_media, wire_views): post
-- ids are TEXT (uuid-as-text) and parents are referenced softly by
-- (parent_type, parent_id) with NO foreign keys, so a post delete doesn't need
-- to cascade through here and these rows survive for audit.

-- Comments are a FLAT list (no threading). A reply column can be added later
-- without touching what's here.
CREATE TABLE IF NOT EXISTS wire_comments (
  id              TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  parent_type     TEXT NOT NULL CHECK (parent_type IN ('media_post','article')),
  parent_id       TEXT NOT NULL,
  author_id       TEXT NOT NULL,
  author_name     TEXT,
  body            TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- SOFT delete (mirrors softRemove on posts): a removed comment stays readable
  -- to moderators next to the restriction it triggered, so the audit trail
  -- survives. Normal viewers never see the body once deleted_at is set.
  deleted_at      TIMESTAMPTZ,
  deleted_by      TEXT,
  deleted_by_name TEXT,
  delete_reason   TEXT
);
CREATE INDEX IF NOT EXISTS idx_wire_comments_parent
  ON wire_comments(parent_type, parent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_wire_comments_author ON wire_comments(author_id);

-- One row per (post, user). The composite PK makes the like toggle an
-- ON CONFLICT DO NOTHING / DELETE pair — the same trick wire_views uses.
-- Deliberately NO counter column on the post: COUNT(*) is cheap here and can't
-- drift out of sync the way a denormalised counter can.
CREATE TABLE IF NOT EXISTS wire_likes (
  parent_type  TEXT NOT NULL CHECK (parent_type IN ('media_post','article')),
  parent_id    TEXT NOT NULL,
  user_id      TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (parent_type, parent_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_wire_likes_parent ON wire_likes(parent_type, parent_id);

-- Commenting restrictions. kind='timeout' carries an expires_at and lapses on
-- its own (no cron: an expired row simply stops matching the "active" query).
-- kind='pause' has expires_at NULL and lasts until a manager lifts it.
CREATE TABLE IF NOT EXISTS comment_restrictions (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id         TEXT NOT NULL,
  kind            TEXT NOT NULL CHECK (kind IN ('timeout','pause')),
  reason          TEXT NOT NULL,          -- a key from the server-side list
  note            TEXT,                   -- optional free-text from the manager
  expires_at      TIMESTAMPTZ,            -- NULL = indefinite (pause)
  created_by      TEXT,
  created_by_name TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  lifted_at       TIMESTAMPTZ,
  lifted_by       TEXT,
  lifted_by_name  TEXT
);
-- "Active" lookups only ever scan un-lifted rows.
CREATE INDEX IF NOT EXISTS idx_comment_restrictions_active
  ON comment_restrictions(user_id, expires_at) WHERE lifted_at IS NULL;

-- In-app notifications (the sidebar bell). Written inline by whatever action
-- caused them; `link` deep-links back to the post.
CREATE TABLE IF NOT EXISTS notifications (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id     TEXT NOT NULL,
  type        TEXT NOT NULL,              -- wire.comment | wire.like | wire.moderation | …
  title       TEXT NOT NULL,
  body        TEXT,
  link        TEXT,
  meta        JSONB NOT NULL DEFAULT '{}'::jsonb,
  read_at     TIMESTAMPTZ,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Unread-first ordering for the bell, newest first.
CREATE INDEX IF NOT EXISTS idx_notifications_user
  ON notifications(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_unread
  ON notifications(user_id, created_at DESC) WHERE read_at IS NULL;
