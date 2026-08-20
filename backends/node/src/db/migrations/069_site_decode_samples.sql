-- Decode-health time series per site, so the site drill-down can chart decode
-- quality over time rather than showing only the latest figure.
--
-- Why a new table: node_site_snapshots is UPSERTED on (node, system, rfss,
-- site) — exactly one row per site per node, overwritten every ~60s — so it
-- holds the CURRENT quality and no history at all. node_radio_events carries no
-- decode figures. Neither can be charted; the samples have to be kept as they
-- arrive or they are gone.
--
-- One row per site per snapshot poll (~1/min/site/node). Kept deliberately
-- narrow: three numeric readings plus identity, no JSONB, because this is the
-- one node table that grows without an upsert to bound it.
CREATE TABLE IF NOT EXISTS node_site_decode_samples (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  node_id       TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  system_id     INTEGER NOT NULL,
  rfss          INTEGER NOT NULL,
  site_id       INTEGER NOT NULL,
  decode_pct    DOUBLE PRECISION,
  signal_dbfs   DOUBLE PRECISION,
  invalid_frames BIGINT,
  sampled_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The only read pattern: one site's samples over a time window, oldest first.
CREATE INDEX IF NOT EXISTS idx_site_decode_site_time
  ON node_site_decode_samples(system_id, rfss, site_id, sampled_at DESC);

-- Retention sweep support (see nodeEventsPruner): delete by age across all
-- sites, so the prune predicate needs its own index rather than riding the
-- per-site one above.
CREATE INDEX IF NOT EXISTS idx_site_decode_sampled_at
  ON node_site_decode_samples(sampled_at);
