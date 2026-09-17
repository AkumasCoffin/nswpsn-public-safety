-- The "Recently heard" scalars on the per-receiver track rows.
--
-- These three columns were added to 105_node_adsb_tracks.sql AFTER that file
-- may already have been applied: the runner keys the applied set on filename
-- alone, so a schema that ran the early 105 (CREATE TABLE without them) skips
-- the edited file forever, and every read and write of these columns fails
-- with 42703 — silently, because those failures were logged at debug.
--
-- Hence this migration: the same statements, under a filename no schema has
-- seen. Idempotent both ways — a schema whose 105 already carried the columns
-- no-ops here. The lesson it encodes: never edit an applied migration.

ALTER TABLE node_adsb_tracks ADD COLUMN IF NOT EXISTS reports     integer;
ALTER TABLE node_adsb_tracks ADD COLUMN IF NOT EXISTS last_alt_ft real;
ALTER TABLE node_adsb_tracks ADD COLUMN IF NOT EXISTS max_alt_ft  real;
