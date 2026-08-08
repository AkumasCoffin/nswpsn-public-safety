-- 047: deep P25 site metadata ("site view parity").
--
-- A RADIO node's Go agent forwards the sdrtrunk-vce GET /site/snapshots feed
-- (the stabilized p25_site_* summary tables) to
--   POST /api/node-ingest/site-snapshots
-- and each site object is upserted here (services/nodeEvents.ts
-- upsertSiteSnapshots). This is the source for the staff Data tab's
-- Radio → Systems → (system) → (site) deep metadata drill-down
-- (control channel, channel plan, neighbors, frequency bands, decode
-- quality) — NOT event/activity counts, which stay in node_radio_events.
--
-- Idempotent: re-POSTing a batch UPSERTs on the natural key
-- (node_id, system_id, rfss, site_id) — one row per (node, physical site),
-- never duplicated. The nested channels/neighbors/bands/quality/status ride
-- as JSONB (they are read whole, never queried column-wise).
--
-- Key columns can't be NULL (they're in a UNIQUE index), but vce may not
-- resolve system_id/rfss for a site; the store coalesces those to -1
-- ("unknown"), the same sentinel node_radio_hourly_sys uses. site_id is
-- always present for a "known site" (vce filters on a non-null site id).

CREATE TABLE IF NOT EXISTS node_site_snapshots (
  id                     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  node_id                TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  -- Natural site identity (P25). -1 = unknown (see header).
  system_id              INTEGER NOT NULL,
  rfss                   INTEGER NOT NULL,
  site_id                INTEGER NOT NULL,
  -- Scalar metadata.
  guid                   TEXT,
  system_name            TEXT,
  wacn                   INTEGER,
  nac                    INTEGER,
  lra                    INTEGER,
  channel_name           TEXT,
  control_frequency_mhz  DOUBLE PRECISION,
  control_lcn            TEXT,
  affiliated_radio_count INTEGER,
  observation_count      INTEGER,
  site_first_seen_ms     BIGINT,
  site_last_seen_ms      BIGINT,
  -- Nested facts (read whole). status/quality nullable; the lists default [].
  status                 JSONB,
  channels               JSONB NOT NULL DEFAULT '[]'::jsonb,
  neighbors              JSONB NOT NULL DEFAULT '[]'::jsonb,
  bands                  JSONB NOT NULL DEFAULT '[]'::jsonb,
  quality                JSONB,
  received_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (node_id, system_id, rfss, site_id)
);

-- Read path: latest snapshot for a (system_id, rfss, site_id) across nodes,
-- for /api/node-data/site meta + the /system sites[] control-freq/count
-- enrichment.
CREATE INDEX IF NOT EXISTS idx_nss_site_recent
  ON node_site_snapshots(system_id, rfss, site_id, received_at DESC);
