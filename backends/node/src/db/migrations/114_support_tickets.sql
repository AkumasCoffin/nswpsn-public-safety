-- Contact tickets: any logged-in account can open a ticket and hold a
-- conversation with staff (owner + the new 'support' role); staff can also
-- open a ticket AT a user (outreach). Subject/category/status live on the
-- ticket; the conversation is a flat message thread.
--
-- Status is about whose court the ball is in, which is what both sides'
-- lists sort and badge on:
--   open          — staff need to act (new ticket, or any user reply; a user
--                   reply to a CLOSED ticket lands here too — that IS reopen)
--   awaiting_user — staff replied, or staff opened the ticket at the user
--   closed        — resolved (closed_by_* records who)
-- Transitions are explicit UPDATEs in the same transaction as the message
-- INSERT — no triggers, per house style.
CREATE TABLE IF NOT EXISTS support_tickets (
  id               SERIAL PRIMARY KEY,
  user_id          TEXT NOT NULL,            -- ticket owner (Supabase user id)
  user_name        TEXT,                     -- denormalised at create; reads overlay current names
  subject          TEXT NOT NULL,
  category         TEXT NOT NULL CHECK (category IN
                     ('general','account','data_correction','feeder_node','bug','other')),
  status           TEXT NOT NULL DEFAULT 'open'
                     CHECK (status IN ('open','awaiting_user','closed')),
  created_by_staff BOOLEAN NOT NULL DEFAULT FALSE,
  closed_by        TEXT,
  closed_by_name   TEXT,
  closed_at        TIMESTAMPTZ,
  last_message_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_support_tickets_user   ON support_tickets (user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_support_tickets_status ON support_tickets (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS support_ticket_messages (
  id          SERIAL PRIMARY KEY,
  ticket_id   INTEGER NOT NULL REFERENCES support_tickets(id) ON DELETE CASCADE,
  author_id   TEXT NOT NULL,
  author_name TEXT,
  is_staff    BOOLEAN NOT NULL DEFAULT FALSE,
  body        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_support_ticket_messages_ticket
  ON support_ticket_messages (ticket_id, created_at);
