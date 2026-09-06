-- Fleet vehicles: one photo becomes up to FOUR, each optionally labelled
-- with the side of the vehicle it shows (front / rear / left / right /
-- other). Stored as a jsonb array of {key, side} in upload order; the
-- single image_key column folds into it and goes away.

ALTER TABLE fleet_vehicles ADD COLUMN IF NOT EXISTS images JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE fleet_vehicles
   SET images = jsonb_build_array(jsonb_build_object('key', image_key, 'side', NULL))
 WHERE image_key IS NOT NULL AND images = '[]'::jsonb;

ALTER TABLE fleet_vehicles DROP COLUMN IF EXISTS image_key;
