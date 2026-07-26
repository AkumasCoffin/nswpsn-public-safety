-- Feeder-node registry for the radio (and future aircraft/pager) feeder
-- system. See feeder-nodes/radio-node and the "Nodes" staff tab.
--
-- Provisioning is role-driven: any user holding the radio_contributor role
-- gets ONE long-lived feeder token (feeder_tokens), minted lazily the first
-- time they open feeder.html. A node row is created/upserted on the agent's
-- first WebSocket hello, keyed by (user_id, install_id) — one user can run
-- several machines, each its own node. Token validity is gated at auth time
-- on the user still holding radio_contributor, so removing the role
-- auto-disables their nodes without any hook here.

-- One durable credential per contributor. The agent needs a long-lived
-- secret (Supabase JWTs expire). The token itself is NOT stored: it is
-- HMAC(FEEDER_TOKEN_SECRET, "<user_id>:<token_version>") so the download
-- endpoint can regenerate it on demand, while the DB holds only its sha256
-- (token_hash, for the constant-time auth compare) and token_prefix (for
-- O(1) lookup and for logging WHICH contributor a request came from).
-- Rotation bumps token_version, which changes the HMAC input → a new token,
-- hash and prefix, instantly invalidating the old one.
CREATE TABLE IF NOT EXISTS feeder_tokens (
  user_id       TEXT PRIMARY KEY,
  token_hash    TEXT NOT NULL,
  token_prefix  TEXT NOT NULL,
  token_version INTEGER NOT NULL DEFAULT 1,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  rotated_at    TIMESTAMPTZ
);
-- Prefix is what the auth path looks up by; unique so one prefix maps to
-- exactly one contributor.
CREATE UNIQUE INDEX IF NOT EXISTS idx_feeder_tokens_prefix
  ON feeder_tokens(token_prefix);

-- One row per physical feeder machine. Created on first hello and kept in
-- sync from status heartbeats. config_override holds the owner/dev edits
-- (site + control frequencies, tuner gain/PPM) layered over the base
-- presets; config_version is the sha256 of the last config the agent ACKed
-- applying, so the UI can show in-sync / out-of-sync.
CREATE TABLE IF NOT EXISTS nodes (
  id               TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  kind             TEXT NOT NULL DEFAULT 'radio',
  user_id          TEXT NOT NULL,
  install_id       TEXT NOT NULL,
  name             TEXT NOT NULL DEFAULT '',
  enabled          BOOLEAN NOT NULL DEFAULT true,
  config_override  JSONB NOT NULL DEFAULT '{}'::jsonb,
  config_version   TEXT,
  agent_version    TEXT,
  sdrtrunk_version TEXT,
  rdio_version     TEXT,
  os               TEXT,
  arch             TEXT,
  last_seen_at     TIMESTAMPTZ,
  notes            TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, install_id)
);
CREATE INDEX IF NOT EXISTS idx_nodes_user ON nodes(user_id);

-- Per-node daily call rollups. Central rdio-scanner can't attribute calls
-- to a node (every node uploads through ONE server-held internal key), so
-- the relay endpoint counts them here instead.
CREATE TABLE IF NOT EXISTS node_call_stats (
  node_id  TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  day      DATE NOT NULL,
  calls    INTEGER NOT NULL DEFAULT 0,
  bytes    BIGINT  NOT NULL DEFAULT 0,
  PRIMARY KEY (node_id, day)
);
