-- 129: what a named API key may read, chosen per key.
--
-- Until now every key could read the same thing: the flat list of upstream
-- data feeds in services/auth/apiKey.ts. api_keys.scopes existed (migration
-- 120, default '{read}') and nothing read it. Scopes now decide
-- (services/auth/apiKeyScopes.ts): a key holds source:, type: and window:
-- scopes, and an endpoint declares which source, type and window it serves.
-- The literal 'read' stays, meaning exactly the old feed list, so every key
-- made before this keeps working unchanged.
--
-- A request carries two sets, as it already keeps what was asked apart from
-- what was granted (name and reason; rate_limit_per_min and expires_at):
--   requested_scopes  what the user asked for, optional
--   scopes            what staff granted on approval; the claim copies it to
--                     the key. Empty on a request approved before this
--                     migration — the claim treats that as 'read'.
ALTER TABLE api_key_requests ADD COLUMN IF NOT EXISTS requested_scopes TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE api_key_requests ADD COLUMN IF NOT EXISTS scopes           TEXT[] NOT NULL DEFAULT '{}';

-- A key with no scopes at all would be refused everything without saying
-- why. Checked against production before this was written: every key holds
-- '{read}'.
ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_scopes_nonempty;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_scopes_nonempty CHECK (cardinality(scopes) > 0);
