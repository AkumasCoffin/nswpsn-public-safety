-- 043: per-event feeder-node capture + hourly forever rollups.
--
-- Every call/page relayed through /api/node-ingest/* is recorded as one
-- DETAIL row (30-day retention, pruned by services/nodeEventsPruner.ts)
-- attributed to the node that heard it, and rolled into hourly FOREVER
-- bucket tables at ingest time (services/nodeEvents.ts).
--
-- Logical grouping: the same over-the-air transmission heard by several
-- nodes arrives as several uploads. Ingest groups them: rows within ±4s
-- on the same (system, talkgroup) [radio] or (capcode, message_hash)
-- [pager] share one logical_call_id / logical_id (the id of the first
-- row in the group). Grouping is serialised per key with an advisory
-- xact lock so concurrent uploads can't each start their own group.
--
-- Site columns (P25 RFSS/site/NAC) arrive via X-Call-Site-* headers from
-- upgraded Go agents only — all nullable, everything works without them.
-- In the hourly sys rollup, "site unknown" is encoded as -1 (PK columns
-- can't be NULL).

-- ---------------------------------------------------------------------
-- Radio detail (30-day window)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS node_radio_events (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  node_id         TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  received_at     TIMESTAMPTZ NOT NULL,
  ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  system          INTEGER,
  talkgroup       INTEGER,
  source_unit     INTEGER,
  frequency       BIGINT,
  site_rfss       INTEGER,
  site_id         INTEGER,
  site_nac        INTEGER,
  site_source     TEXT,          -- event | channel | context
  talkgroup_label TEXT,
  system_label    TEXT,
  audio_bytes     INTEGER NOT NULL DEFAULT 0,
  logical_call_id BIGINT         -- id of the first row of the logical group
);
CREATE INDEX IF NOT EXISTS idx_nre_sys_tg_time
  ON node_radio_events(system, talkgroup, received_at);
CREATE INDEX IF NOT EXISTS idx_nre_time
  ON node_radio_events(received_at);
CREATE INDEX IF NOT EXISTS idx_nre_node_time
  ON node_radio_events(node_id, received_at);
CREATE INDEX IF NOT EXISTS idx_nre_unit_time
  ON node_radio_events(source_unit, received_at);
CREATE INDEX IF NOT EXISTS idx_nre_logical
  ON node_radio_events(logical_call_id);

-- ---------------------------------------------------------------------
-- Pager detail (30-day window)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS node_pager_events (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  node_id      TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  received_at  TIMESTAMPTZ NOT NULL,
  ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  capcode      TEXT NOT NULL,
  "function"   SMALLINT,
  freq_mhz     DOUBLE PRECISION,
  message      TEXT,
  message_hash TEXT NOT NULL,    -- sha256 hex of trimmed message
  logical_id   BIGINT            -- id of the first row of the logical group
);
CREATE INDEX IF NOT EXISTS idx_npe_cap_hash_time
  ON node_pager_events(capcode, message_hash, received_at);
CREATE INDEX IF NOT EXISTS idx_npe_time
  ON node_pager_events(received_at);
CREATE INDEX IF NOT EXISTS idx_npe_node_time
  ON node_pager_events(node_id, received_at);
CREATE INDEX IF NOT EXISTS idx_npe_logical
  ON node_pager_events(logical_id);

-- ---------------------------------------------------------------------
-- Hourly forever rollups (never pruned; kept small by bucketing)
-- ---------------------------------------------------------------------

-- Per-node reception volume: every upload counts here (raw receptions).
CREATE TABLE IF NOT EXISTS node_radio_hourly (
  hour        TIMESTAMPTZ NOT NULL,
  node_id     TEXT NOT NULL,
  system      INTEGER NOT NULL DEFAULT 0,
  talkgroup   INTEGER NOT NULL DEFAULT 0,
  calls       INTEGER NOT NULL DEFAULT 0,
  audio_bytes BIGINT  NOT NULL DEFAULT 0,
  PRIMARY KEY (hour, node_id, system, talkgroup)
);

-- Network-wide per-talkgroup(+site) volume; logical_calls counts each
-- over-the-air call once regardless of how many nodes heard it.
-- site_rfss/site_id = -1 means "unknown" (nullable columns can't be in a PK).
CREATE TABLE IF NOT EXISTS node_radio_hourly_sys (
  hour          TIMESTAMPTZ NOT NULL,
  system        INTEGER NOT NULL DEFAULT 0,
  talkgroup     INTEGER NOT NULL DEFAULT 0,
  site_rfss     INTEGER NOT NULL DEFAULT -1,
  site_id       INTEGER NOT NULL DEFAULT -1,
  calls         INTEGER NOT NULL DEFAULT 0,
  logical_calls INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (hour, system, talkgroup, site_rfss, site_id)
);

CREATE TABLE IF NOT EXISTS node_pager_hourly (
  hour          TIMESTAMPTZ NOT NULL,
  node_id       TEXT NOT NULL,
  capcode       TEXT NOT NULL DEFAULT '',
  pages         INTEGER NOT NULL DEFAULT 0,
  logical_pages INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (hour, node_id, capcode)
);
