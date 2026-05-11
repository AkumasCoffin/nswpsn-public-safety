-- 007_backfill_waze_latlng.sql
-- The migration that pulled python's data_history into archive_waze
-- copied data_history.latitude/longitude into archive_waze.lat/lng,
-- but python only populated those dedicated columns for live (is_live=1)
-- rows; historical snapshots typically had NULL lat/lng even when the
-- coordinate was sitting in the data JSONB blob the whole time.
--
-- Symptom: /api/waze/police-heatmap returned ~4000 pings instead of
-- the ~30000 python showed, because the heatmap WHERE clause filters
-- on `lat IS NOT NULL AND lng IS NOT NULL`. All those rows have the
-- coords inside `data->'location'->'y'` / `'x'`; we just need to
-- promote them into the dedicated columns once so future queries find
-- them.
--
-- Update only rows where the column is NULL but the JSONB carries a
-- usable numeric coordinate. Bounded by source family so we don't pay
-- the O(N) JSONB cast on tables that never carry these fields.

UPDATE archive_waze
   SET lat = (data->'location'->>'y')::float8,
       lng = (data->'location'->>'x')::float8
 WHERE (lat IS NULL OR lng IS NULL)
   AND data ? 'location'
   AND (data->'location'->>'y') ~ '^-?[0-9.]+$'
   AND (data->'location'->>'x') ~ '^-?[0-9.]+$';
