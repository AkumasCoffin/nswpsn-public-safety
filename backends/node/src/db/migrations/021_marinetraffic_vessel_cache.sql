-- 021_marinetraffic_vessel_cache.sql
--
-- Persistent cache for /api/marinetraffic/vessel/:id detail JSON.
-- The route already has a 30-min in-process Map cache, but that's
-- empty after every api-node restart and triggers a fresh ~2-3 s
-- browser navigation per ship. This table backs the in-process
-- cache so a restart can re-warm itself from disk and so detail
-- data persists across deploys.
--
-- One row per ship_id. The `data` column holds the raw vessel JSON
-- as returned to the front-end. `fetched_at` lets us decide
-- whether to serve from DB or re-fetch from MT.
CREATE TABLE IF NOT EXISTS marinetraffic_vessel_cache (
  ship_id    TEXT        PRIMARY KEY,
  data       JSONB       NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- For "purge stale rows older than X" sweeps (cleanup loop adds
-- one in a future migration if needed).
CREATE INDEX IF NOT EXISTS idx_marinetraffic_vessel_cache_fetched_at
  ON marinetraffic_vessel_cache (fetched_at);
