-- Agency reference tables ("Extended" data) moved out of committed CSV/JSON into
-- Postgres. agency_extended stores each agency's whole {title,tag,badges,overview,
-- sections} blob; each table section carries an inline {headers,rows}. A one-time
-- seed imports the data/Extended CSV tree on startup when the table is empty;
-- after that the DB is authoritative and all edits are applied here.
CREATE TABLE IF NOT EXISTS agency_extended (
  slug        TEXT PRIMARY KEY,
  data        JSONB NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Row-level edit requests. Owner edits apply immediately and are recorded here as
-- status='approved'; data_feeder edits stay 'pending' until an owner/team_member
-- approves (then applied) or rejects.
CREATE TABLE IF NOT EXISTS agency_data_change (
  id               SERIAL PRIMARY KEY,
  slug             TEXT NOT NULL,
  section_key      TEXT NOT NULL,   -- stable section id (the source csv filename)
  section_title    TEXT,            -- denormalised for the review UI
  op               TEXT NOT NULL CHECK (op IN ('add','update','delete')),
  row_key          TEXT,            -- first-column value identifying the row
  before_cells     JSONB,           -- original row cells (update/delete)
  after_cells      JSONB,           -- new row cells (add/update)
  status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','approved','rejected')),
  created_by       TEXT NOT NULL,
  created_by_name  TEXT,
  reviewed_by      TEXT,
  reviewed_by_name TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  reviewed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_agency_data_change_pending
  ON agency_data_change (status, created_at DESC);
