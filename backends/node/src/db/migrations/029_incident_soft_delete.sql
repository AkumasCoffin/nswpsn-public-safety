-- Editor deletes become soft deletes: the row (and its logs/suggestions)
-- stays in the database, hidden from the API, until the hourly cleanup
-- purges it after DATA_RETENTION_DAYS.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_incidents_deleted_at
  ON incidents (deleted_at) WHERE deleted_at IS NOT NULL;
