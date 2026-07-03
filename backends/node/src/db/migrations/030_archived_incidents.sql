-- Permanent archive for major user incidents. Staff/owner archive a live
-- incident: a full snapshot (incident + its logs) lands here and the live
-- pin is soft-deleted. Rows in this table are exempt from data retention —
-- they stay forever and are searchable from the logs page.
CREATE TABLE IF NOT EXISTS archived_incidents (
  id          TEXT PRIMARY KEY,                     -- original incident id
  title       TEXT NOT NULL DEFAULT '',
  location    TEXT,
  incident    JSONB NOT NULL,                       -- normalised incident snapshot
  logs        JSONB NOT NULL DEFAULT '[]'::jsonb,   -- snapshot of incident_updates
  archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  archived_by TEXT
);
CREATE INDEX IF NOT EXISTS idx_archived_incidents_archived_at
  ON archived_incidents (archived_at DESC);
