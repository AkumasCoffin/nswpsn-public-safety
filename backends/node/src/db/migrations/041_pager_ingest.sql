-- Central Pagermon ingest target for the PAGER feeder-node relay.
--
-- Pager nodes decode POCSAG locally and relay each message to the backend
-- (/api/node-ingest/pager-upload), which forwards it into ONE central Pagermon
-- with the server-held apikey. Storing the URL + key here (owner-set in the
-- staff panel) keeps the secret SERVER-SIDE — it is deliberately NOT part of
-- GlobalConfigSchema and is NEVER included in the config pushed to agents, so
-- an operator's node never sees the Pagermon key. Env PAGERMON_INGEST_URL /
-- PAGERMON_INGEST_API_KEY act as the fallback when these are null.
--
-- Reuses the existing feeder_global_config singleton (id = 1).
ALTER TABLE feeder_global_config ADD COLUMN IF NOT EXISTS pagermon_ingest_url     TEXT;
ALTER TABLE feeder_global_config ADD COLUMN IF NOT EXISTS pagermon_ingest_api_key TEXT;
