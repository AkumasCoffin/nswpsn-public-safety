-- 001_init.sql
-- Initial schema for the Node backend. Tables that aren't archive-related
-- live here directly; archive tables are in separate migrations because
-- they're partitioned and need a helper function defined first.
--
-- Conservative on indexes — we add them only when a query needs one.
-- The Python codebase accumulated 25+ indexes on data_history of which
-- ~5 were used; not repeating that here.

-- Tracks user-submitted incidents (the manual-add flow on the map page).
-- One row per incident; updates create rows in incident_updates.
CREATE TABLE IF NOT EXISTS incidents (
  id            UUID PRIMARY KEY,
  source        TEXT NOT NULL DEFAULT 'user_incident',
  source_id     TEXT,
  title         TEXT,
  description   TEXT,
  category      TEXT,
  subcategory   TEXT,
  severity      TEXT,
  status        TEXT,
  lat           DOUBLE PRECISION,
  lng           DOUBLE PRECISION,
  location_text TEXT,
  data          JSONB DEFAULT '{}'::jsonb,
  created_by    TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  active        BOOLEAN NOT NULL DEFAULT true
);
CREATE INDEX IF NOT EXISTS idx_incidents_active_created
  ON incidents (active, created_at DESC) WHERE active = true;

-- Audit trail of edits/comments on user incidents.
CREATE TABLE IF NOT EXISTS incident_updates (
  id          UUID PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
  message     TEXT,
  data        JSONB DEFAULT '{}'::jsonb,
  created_by  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_incident_updates_incident
  ON incident_updates (incident_id, created_at DESC);

-- RBAC table — Supabase user_id -> role(s). Mirrors the Python
-- check-admin / check-editor flow.
CREATE TABLE IF NOT EXISTS user_roles (
  user_id    TEXT NOT NULL,
  role       TEXT NOT NULL,
  granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  granted_by TEXT,
  PRIMARY KEY (user_id, role)
);

-- Editor-access requests pending approval.
CREATE TABLE IF NOT EXISTS editor_requests (
  id         UUID PRIMARY KEY,
  user_id    TEXT NOT NULL,
  email      TEXT,
  status     TEXT NOT NULL DEFAULT 'pending',  -- pending | approved | rejected
  reason     TEXT,
  reviewer   TEXT,
  reviewed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  data       JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_editor_requests_status
  ON editor_requests (status, created_at DESC);

-- Periodic snapshots of system stats for the live dashboard tile.
CREATE TABLE IF NOT EXISTS stats_snapshots (
  ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
  data       JSONB NOT NULL,
  PRIMARY KEY (ts)
);
