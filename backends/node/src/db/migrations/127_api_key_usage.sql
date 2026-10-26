-- 127: what each named API key has been doing, and keys an owner can re-issue
-- or delete.
--
-- Until now the only trace a key left was api_keys.last_used_at, written at
-- most once a minute. Staff could see that a key existed and roughly when it
-- was last used, but not how much it was used, on what, or how much of it
-- failed.
--
-- api_key_usage_hourly is one row per (key, hour, path, HTTP status). It is
-- written from an in-memory tally once a minute (services/auth/apiKeyUsage.ts),
-- not per request: a key at its default 120/min is 170k requests a day.
--
-- EVERY request that presents a key whose secret matches is counted, including
-- the ones the gate refuses: from a web page (403), revoked or expired (403),
-- over its per-minute limit (429), or to a path keys may not read (403). Those
-- refusals are what staff most need to see. A wrong secret against a real
-- prefix is NOT counted against the key: whoever sent it does not hold it.
--
-- path is normalised before it lands here (id-like segments become :id, a
-- method other than GET/HEAD is prefixed, 120 characters at most), and a key
-- may add at most 100 distinct paths in an hour; the rest are counted under
-- '(other paths)', so a key scanning random URLs cannot grow the table.
--
-- Rows are pruned after 90 days (services/cleanup.ts). requests_total on the
-- key is the lifetime count, bumped by the same flush, so "how many requests
-- has this key made" survives the prune.
--
-- reissued_at: the owner swapped the key's secret (and prefix) for a new one;
-- the row, its limits and its history carry on. deleted_at: the owner removed
-- the key from their list. A deleted key is always revoked too, and staff
-- still see it and its usage.
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS requests_total BIGINT NOT NULL DEFAULT 0;
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS reissued_at    TIMESTAMPTZ;
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS deleted_at     TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS api_key_usage_hourly (
  key_id   UUID        NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
  hour     TIMESTAMPTZ NOT NULL,
  path     TEXT        NOT NULL,
  status   SMALLINT    NOT NULL,
  requests INTEGER     NOT NULL DEFAULT 0,
  PRIMARY KEY (key_id, hour, path, status)
);

-- The prune, and the staff list's totals across every key for a window.
CREATE INDEX IF NOT EXISTS idx_api_key_usage_hour ON api_key_usage_hourly (hour);
