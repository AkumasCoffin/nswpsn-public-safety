-- 001_init.sql
-- Initial schema for the Node backend.
--
-- Mirrors the schema python's init_postgres.py creates so a DB that
-- was set up by the python service stays compatible — no column
-- renames, no UUID-vs-TEXT id flips. The Node code reads/writes the
-- same columns python's external_api_proxy.py did.
--
-- All CREATE statements use IF NOT EXISTS so re-running on a populated
-- DB is a no-op. ALTER COLUMN clauses are not used here; if you need
-- to evolve a column type, ship a separate migration that handles
-- existing data explicitly.

-- editor_requests — pending editor-access submissions, reviewed by admins.
-- Mirrors init_postgres.py:62-82.
CREATE TABLE IF NOT EXISTS editor_requests (
  id                  SERIAL PRIMARY KEY,
  email               TEXT NOT NULL,
  discord_id          TEXT NOT NULL,
  website             TEXT,
  about               TEXT,
  request_type        TEXT,
  region              TEXT,
  background          TEXT,
  background_details  TEXT,
  status              TEXT DEFAULT 'pending',
  created_at          BIGINT NOT NULL,
  reviewed_at         BIGINT,
  notes               TEXT,
  has_existing_setup  TEXT,
  setup_details       TEXT,
  tech_experience     TEXT,
  experience_level    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_editor_requests_status ON editor_requests(status);
CREATE INDEX IF NOT EXISTS idx_editor_requests_email  ON editor_requests(email);

-- incidents — user-submitted emergency incidents.
-- Mirrors init_postgres.py:168-188. id is TEXT (UUID stored as text)
-- to match python's gen_random_uuid()::text default.
CREATE TABLE IF NOT EXISTS incidents (
  id                  TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  title               TEXT NOT NULL,
  description         TEXT DEFAULT '',
  lat                 DOUBLE PRECISION NOT NULL,
  lng                 DOUBLE PRECISION NOT NULL,
  location            TEXT DEFAULT '',
  type                JSONB DEFAULT '[]'::jsonb,
  status              TEXT DEFAULT 'Going',
  size                TEXT DEFAULT '-',
  responding_agencies JSONB DEFAULT '[]'::jsonb,
  created_at          TIMESTAMPTZ DEFAULT now(),
  updated_at          TIMESTAMPTZ DEFAULT now(),
  expires_at          TIMESTAMPTZ,
  is_rfs_stub         BOOLEAN DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_incidents_expires  ON incidents(expires_at);
CREATE INDEX IF NOT EXISTS idx_incidents_rfs_stub ON incidents(is_rfs_stub);
CREATE INDEX IF NOT EXISTS idx_incidents_status   ON incidents(status);

-- incident_updates — log entries for incidents.
-- Mirrors init_postgres.py:192-201.
CREATE TABLE IF NOT EXISTS incident_updates (
  id           TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  incident_id  TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
  message      TEXT NOT NULL,
  created_at   TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_incident_updates_incident ON incident_updates(incident_id);
CREATE INDEX IF NOT EXISTS idx_incident_updates_created  ON incident_updates(created_at);

-- user_roles — Supabase user_id → role(s). Mirrors init_postgres.py:205-217.
CREATE TABLE IF NOT EXISTS user_roles (
  id          SERIAL PRIMARY KEY,
  user_id     TEXT NOT NULL,
  role        TEXT NOT NULL,
  granted_by  TEXT DEFAULT 'system',
  request_id  INTEGER,
  created_at  TIMESTAMPTZ DEFAULT now(),
  UNIQUE(user_id, role)
);
CREATE INDEX IF NOT EXISTS idx_user_roles_user ON user_roles(user_id);
CREATE INDEX IF NOT EXISTS idx_user_roles_role ON user_roles(role);

-- stats_snapshots — periodic JSON snapshots of the dashboard tile.
-- Python doesn't ship this in init_postgres.py but the Node admin/db/stats
-- handler counts rows. Optional / informational table.
CREATE TABLE IF NOT EXISTS stats_snapshots (
  ts    TIMESTAMPTZ NOT NULL DEFAULT now(),
  data  JSONB NOT NULL,
  PRIMARY KEY (ts)
);
