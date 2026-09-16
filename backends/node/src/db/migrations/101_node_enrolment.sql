-- Single-use enrolment codes, so an installer stops being a permanent credential.
--
-- Until now the installer had the node's long-lived token baked into it. Two
-- costs fell out of that:
--
--   1. The token is hashed at rest and never re-derivable, so re-downloading an
--      installer had to MINT A NEW ONE — silently breaking whatever agent was
--      already running on that node.
--   2. The downloaded .sh sat in a volunteer's home directory as a permanent
--      credential. Anyone who obtained that file owned the node indefinitely.
--
-- An enrolment code fixes both. It is short-lived and single-use: the agent
-- trades it for a real token on first run, and the code is spent. Downloading
-- an installer now mints a code and touches nothing else, so a running node
-- keeps working; and a leaked installer expires on its own.
--
-- Only ONE code is outstanding per node at a time — issuing another replaces
-- it, which is what an operator means by "download it again".

ALTER TABLE nodes
  -- sha256 of the code. The hash IS the lookup key: unlike the node token
  -- (whose prefix is logged to tell nodes apart) nothing ever needs to
  -- recognise a code without holding it, so no separate prefix column exists
  -- and no partial value is ever stored or logged.
  ADD COLUMN IF NOT EXISTS enrol_code_hash  TEXT,
  ADD COLUMN IF NOT EXISTS enrol_issued_at  TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS enrol_expires_at TIMESTAMPTZ;

-- Enrolment looks a node up BY the hash, so this is the hot path. Partial:
-- almost every row has no outstanding code at any moment.
CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_enrol_code_hash
  ON nodes (enrol_code_hash)
  WHERE enrol_code_hash IS NOT NULL;
