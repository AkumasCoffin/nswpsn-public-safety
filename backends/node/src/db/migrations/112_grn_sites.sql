-- The GRN repeater dataset, promoted from a static JSON file into a table so
-- the owner can correct it — the source file is a community compilation with
-- missing and outdated fields, and edits need to outlive deploys. Seeded ONCE
-- from data/nswpsn/NSW GRN Version 1.json when empty (the agency_extended
-- pattern); after that the DB is authoritative and the file is only a seed.
--
-- One jsonb per site rather than a column per field, on purpose: the dataset's
-- fields are the community's vocabulary ("GRN Site ID #", "FAV NAME & QK#"),
-- they vary in presence, and the map renders them by name — a fixed schema
-- would be a second vocabulary to keep in sync for zero query benefit (the
-- table is 755 rows, always read whole).
CREATE TABLE IF NOT EXISTS grn_sites (
  id         SERIAL PRIMARY KEY,
  data       JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by TEXT
);
