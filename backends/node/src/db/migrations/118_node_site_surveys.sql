-- RF site surveys: on first install (and on staff demand) a radio node
-- measures decode on the GRN sites in its LGA + neighbouring LGAs, and sites
-- at/above the pass threshold are added to its channel list. These tables are
-- the full audit: one row per survey, one row per site TESTED — passes,
-- failures, and sites skipped because their control channel could not be
-- parsed out of the GRN dataset. Append-only history; rows die with the node
-- (the node_site_decode_samples precedent, not the chanmgr ring).
CREATE TABLE IF NOT EXISTS node_site_surveys (
  id           SERIAL PRIMARY KEY,
  node_id      TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at  TIMESTAMPTZ,
  trigger      TEXT NOT NULL CHECK (trigger IN ('install','manual')),
  status       TEXT NOT NULL DEFAULT 'running'
               CHECK (status IN ('running','done','failed','timeout')),
  requested_by TEXT,
  -- How wide the LGA search went (1 = the node's LGA + direct neighbours).
  rings        INTEGER NOT NULL DEFAULT 1,
  candidates   INTEGER NOT NULL DEFAULT 0,
  added        INTEGER,
  note         TEXT
);
CREATE INDEX IF NOT EXISTS idx_node_site_surveys_node
  ON node_site_surveys (node_id, started_at DESC);

CREATE TABLE IF NOT EXISTS node_site_survey_results (
  id           SERIAL PRIMARY KEY,
  survey_id    INTEGER NOT NULL REFERENCES node_site_surveys(id) ON DELETE CASCADE,
  site_name    TEXT NOT NULL,
  grn_key      TEXT,
  freq_hz      BIGINT,
  alt_freq_hz  BIGINT,
  verdict      TEXT NOT NULL
               CHECK (verdict IN ('pass','fail','noLock','unmeasured','skipped')),
  median_pct   REAL,
  samples      INTEGER,
  signal_dbfs  REAL,
  added        BOOLEAN NOT NULL DEFAULT false,
  note         TEXT
);
CREATE INDEX IF NOT EXISTS idx_node_site_survey_results_survey
  ON node_site_survey_results (survey_id);
