-- Optional EXACT node antenna location, for coverage calculation and more
-- exact channel tuning. Set by the operator via the feeder panel; visible to
-- the operator + staff only, never on the public map. Null = unset.
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION;
