-- Incident ownership + suggestion workflow.
--
-- Before this, any map_editor could edit/delete ANY incident. We add a
-- `created_by` column so edit/delete can be restricted to the incident's
-- creator (owner/team_member admins keep an override in the app layer),
-- and an `incident_suggestions` table so editors who DON'T own an incident
-- can propose a field edit or a note that the owner reviews and
-- approves/rejects, instead of editing/deleting directly.
--
-- The incidents / incident_updates tables were created by the legacy
-- python init_postgres.py (NOT a Node migration) and their column types
-- vary across deployments, so the ALTERs are guarded on table existence
-- and use ADD COLUMN IF NOT EXISTS to stay a no-op where already applied.

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = 'incidents') THEN
    ALTER TABLE incidents ADD COLUMN IF NOT EXISTS created_by TEXT;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = 'incident_updates') THEN
    -- Author of a log line, so a log can be edited/deleted by its author
    -- or the incident owner (not any editor).
    ALTER TABLE incident_updates ADD COLUMN IF NOT EXISTS created_by TEXT;
  END IF;
END $$;

-- Proposals from non-owner editors.
--   kind = 'edit' -> `changes` holds whitelisted incident field values the
--                    owner can one-click apply.
--   kind = 'note' -> `message` holds a proposed log line that becomes an
--                    official incident_update on approval.
-- No FK to incidents(id): that table lives outside the Node migration set
-- and its PK/type isn't guaranteed here; the delete handler removes a
-- pin's suggestions alongside the pin.
CREATE TABLE IF NOT EXISTS incident_suggestions (
  id                BIGSERIAL PRIMARY KEY,
  incident_id       TEXT NOT NULL,
  kind              TEXT NOT NULL,                    -- 'edit' | 'note'
  changes           JSONB NOT NULL DEFAULT '{}'::jsonb,
  message           TEXT,
  suggested_by      TEXT NOT NULL,                    -- Supabase user id (authoritative)
  suggested_by_name TEXT,                             -- display label (client-supplied, non-authoritative)
  status            TEXT NOT NULL DEFAULT 'pending',  -- 'pending' | 'approved' | 'rejected'
  reviewed_by       TEXT,
  reviewed_at       TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_incident_suggestions_incident
  ON incident_suggestions (incident_id, status, created_at DESC);
