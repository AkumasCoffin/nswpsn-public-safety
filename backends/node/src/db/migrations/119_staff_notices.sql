-- Manual notifications: the record of what staff sent, and to whom.
--
-- The notices themselves land in `notifications` like every other user-facing
-- message, one row per recipient. This table is the SEND, not the receipt:
-- recipients can clear their own rows (DELETE /api/notifications), so the
-- per-recipient rows cannot be the record of what went out. The actor lives on
-- the row — the comment_restrictions created_by/created_by_name pattern
-- (062_wire_comments.sql) — because there is no general staff audit table.
--
-- target_user_ids is kept for the people-picker audiences so "who exactly did
-- this go to" survives, and target_role for a role send. recipients is the
-- count at send time, which is the honest number: the roster moves.
CREATE TABLE IF NOT EXISTS staff_notices (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sent_by         TEXT NOT NULL,
  sent_by_name    TEXT,
  audience        TEXT NOT NULL CHECK (audience IN ('user', 'users', 'role', 'all')),
  target_role     TEXT,
  target_user_ids TEXT[],
  recipients      INTEGER NOT NULL DEFAULT 0,
  title           TEXT NOT NULL,
  body            TEXT NOT NULL,
  link            TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_staff_notices_created
  ON staff_notices (created_at DESC);
