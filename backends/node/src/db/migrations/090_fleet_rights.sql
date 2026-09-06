-- Fleet vehicles carry the same rights model as articles: a license code,
-- an optional credit line, the publish-time rights affirmation, and the
-- watermark choice (burned into the photo client-side at upload).

ALTER TABLE fleet_vehicles ADD COLUMN IF NOT EXISTS license         TEXT NOT NULL DEFAULT 'credit';
ALTER TABLE fleet_vehicles ADD COLUMN IF NOT EXISTS credit          TEXT;
ALTER TABLE fleet_vehicles ADD COLUMN IF NOT EXISTS rights_affirmed BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE fleet_vehicles ADD COLUMN IF NOT EXISTS watermark       BOOLEAN NOT NULL DEFAULT false;
