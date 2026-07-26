-- Per-node tokens (replaces the per-user feeder_tokens model).
--
-- Each node now has its OWN credential, minted when the node is created in the
-- panel and baked into that node's installer. The token is a random
-- npsn_<40 hex> (format unchanged so the installer parsers don't change); only
-- its sha256 (token_hash) + a lookup prefix (token_prefix) are stored on the
-- node row. Deleting a node hard-revokes its token (the agent can no longer
-- auto-link a new row); rotating replaces the hash. This gives per-node
-- revocation the old per-user token could not.
--
-- install_id becomes the TOFU-bound machine id: a pre-created node has it NULL
-- and binds it on first connect; a second machine presenting the same token is
-- rejected. UNIQUE(user_id, install_id) still holds — Postgres treats NULLs as
-- distinct, so many not-yet-connected nodes per user coexist.

ALTER TABLE nodes ADD COLUMN IF NOT EXISTS token_hash       TEXT;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS token_prefix     TEXT;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS token_rotated_at TIMESTAMPTZ;

-- Nodes are pre-created (name + type) before any machine connects, so the
-- machine id is unknown until first hello.
ALTER TABLE nodes ALTER COLUMN install_id DROP NOT NULL;

-- O(1) auth lookup: one prefix maps to exactly one node. Partial so legacy
-- rows (and pre-token rows) with a NULL prefix don't collide.
CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_token_prefix
  ON nodes(token_prefix) WHERE token_prefix IS NOT NULL;

-- Clean cutover: the per-user token model is gone. Any existing auto-linked
-- node row has token_hash IS NULL and can no longer authenticate (it is inert
-- until re-provisioned or deleted from the panel).
DROP TABLE IF EXISTS feeder_tokens;
