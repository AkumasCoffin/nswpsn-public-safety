-- API key requests: a user asks for a named key, owner|staff approve or
-- reject, and the USER then mints the key themselves (POST .../claim). The
-- plaintext is shown once to its owner at that moment and is never stored or
-- seen by staff — approval grants permission to mint, nothing more.
--
--   pending  — waiting on staff
--   approved — may be claimed; key_id is NULL until the user generates it,
--              then points at the api_keys row (120_api_keys.sql)
--   rejected — review_note carries the reason shown to the user
--
-- rate_limit_per_min / expires_at are what the reviewer granted and are
-- copied onto the key at claim time. One pending request per user (the
-- partial unique index below is the backstop; the API also holds a per-user
-- advisory lock); the active key cap is enforced in the API (api/apiKeys.ts).
CREATE TABLE IF NOT EXISTS api_key_requests (
  id                 SERIAL PRIMARY KEY,
  user_id            TEXT NOT NULL,
  user_name          TEXT,                 -- denormalised at create; reads overlay current names
  name               TEXT NOT NULL,        -- the key's label, chosen by the requester
  reason             TEXT NOT NULL,        -- what it will be used for
  status             TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending','approved','rejected')),
  rate_limit_per_min INTEGER CHECK (rate_limit_per_min IS NULL OR rate_limit_per_min > 0),
  expires_at         TIMESTAMPTZ,
  review_note        TEXT,
  reviewed_by        TEXT,
  reviewed_by_name   TEXT,
  reviewed_at        TIMESTAMPTZ,
  key_id             UUID REFERENCES api_keys(id),
  claimed_at         TIMESTAMPTZ,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_api_key_requests_user   ON api_key_requests (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_key_requests_status ON api_key_requests (status, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_api_key_requests_one_pending
  ON api_key_requests (user_id) WHERE status = 'pending';
