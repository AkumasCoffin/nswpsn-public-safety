-- 128: bug reports and suggestions — a different thing from a ticket.
--
-- A support ticket (114) is a private conversation: subject, a message
-- thread, whose court the ball is in. A bug report or a suggestion is the
-- report and its description, plus a STATUS staff set — and it is public:
-- every one is posted to a Discord board channel with its status, and each
-- status change is relayed under that post. So they live apart from tickets,
-- and are never pruned (the board is the record).
--
-- They arrive two ways: the contact page (a logged-in account, source
-- 'site') or a message in a Discord intake channel (source 'discord', any
-- member of the server). A Discord reporter has no site account unless their
-- Discord is linked to one; then user_id is set as well, so the report shows
-- on their contact page and they get bell notifications.
--
-- When a report needs a back-and-forth, staff link it to a ticket
-- (ticket_id): they open one at the reporter, or link one that exists. The
-- conversation then happens on the ticket, privately, and the report keeps
-- its public status.
--
-- Status labels depend on the kind (api/feedback.ts):
--   in_progress  Looking into it / Planned
--   done         Fixed / Added
--   declined     Won't fix / Not added
--   duplicate    Duplicate
CREATE TABLE IF NOT EXISTS feedback (
  id                SERIAL PRIMARY KEY,
  kind              TEXT NOT NULL CHECK (kind IN ('bug', 'suggestion')),
  source            TEXT NOT NULL CHECK (source IN ('site', 'discord')),
  user_id           TEXT,                  -- site account, when known
  user_name         TEXT,                  -- the name shown on the board
  discord_user_id   TEXT,
  discord_user_name TEXT,
  subject           TEXT NOT NULL,
  body              TEXT NOT NULL,
  status            TEXT NOT NULL DEFAULT 'open'
                      CHECK (status IN ('open', 'in_progress', 'done', 'declined', 'duplicate')),
  status_note       TEXT,
  status_by         TEXT,
  status_by_name    TEXT,
  status_at         TIMESTAMPTZ,
  ticket_id         INTEGER REFERENCES support_tickets(id) ON DELETE SET NULL,
  board_guild_id    TEXT,                  -- the public board post, for "Open in Discord"
  board_channel_id  TEXT,
  board_message_id  TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_feedback_kind_status ON feedback (kind, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_feedback_user        ON feedback (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feedback_ticket      ON feedback (ticket_id);
