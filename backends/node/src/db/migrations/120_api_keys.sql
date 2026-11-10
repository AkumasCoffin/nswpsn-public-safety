-- Named API keys: credentials for scripts and the command line.
--
-- Only the sha256 of the plaintext is stored, plus a lookup prefix — the
-- per-node token pattern (039_per_node_tokens.sql). The plaintext is shown
-- once at creation. A key belongs to a user, carries scopes (only `read` is
-- defined today), its own per-minute limit, an optional expiry, and a
-- revoked_at that ends it without deleting the record of it.
--
-- Keys are refused from web pages by the gate (a browser Origin header with a
-- key is a 403); browsers hold short-lived session tokens instead, which are
-- not stored anywhere — see services/auth/browserToken.ts.
CREATE TABLE IF NOT EXISTS api_keys (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id            TEXT NOT NULL,
  name               TEXT NOT NULL,
  prefix             TEXT NOT NULL UNIQUE,
  key_hash           TEXT NOT NULL,
  scopes             TEXT[] NOT NULL DEFAULT '{read}',
  rate_limit_per_min INTEGER NOT NULL DEFAULT 120 CHECK (rate_limit_per_min > 0),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at       TIMESTAMPTZ,
  expires_at         TIMESTAMPTZ,
  revoked_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys (user_id, created_at);
