-- Optional, PRIVACY-FUZZED node location for map display.
--
-- The operator picks a rough location on a map with a 5km circle; the browser
-- then randomises the point to a uniformly-random spot WITHIN that circle and
-- sends ONLY the fuzzed coordinates — the true antenna location never reaches
-- the server. So lat/lon here is deliberately imprecise (up to ~5km off) and
-- must NOT be treated as an exact position.
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION;
