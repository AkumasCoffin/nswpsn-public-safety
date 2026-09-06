-- Fleet: Make / Model / Cab Chassis were one free-text line; split them into
-- three columns so the data stays queryable (and the composer can offer the
-- known body builders as suggestions). Any early combined value lands in
-- `make` for the author to redistribute on their next edit.

ALTER TABLE fleet_vehicles ADD COLUMN IF NOT EXISTS make        TEXT;
ALTER TABLE fleet_vehicles ADD COLUMN IF NOT EXISTS model       TEXT;
ALTER TABLE fleet_vehicles ADD COLUMN IF NOT EXISTS cab_chassis TEXT;

UPDATE fleet_vehicles SET make = make_model WHERE make_model IS NOT NULL;

ALTER TABLE fleet_vehicles DROP COLUMN IF EXISTS make_model;
