-- 024_waze_sidecar_remap.sql
--
-- One-shot cleanup after the waze classification refactor (commits
-- cad4f27 + writer fix). Two changes to archive_waze_latest:
--
--   1. Re-normalise category for the override sources. waze-ingest.ts
--      now sets category from a per-source override (categoryForSource):
--        waze_police   → 'Police'      (was raw 'POLICE')
--        waze_roadwork → 'Roadwork'    (was raw 'HAZARD' — alerts come
--                                       through as type=HAZARD with the
--                                       CONSTRUCTION subtype)
--        waze_jam      → 'Traffic Jam' (was raw 'JAM')
--      Stable incidents (data_hash unchanged) keep the old category
--      under the old CASE logic so the dropdown showed BOTH the old
--      and new values for the same bucket. Bring them in line here so
--      the override is fully applied at deploy time instead of waiting
--      for the writer's EXCLUDED-differs branch to roll through.
--
--   2. Delete stale waze_hazard sidecar rows whose category/subcategory
--      indicates a JAM. JAM-typed alerts used to fall through
--      isHazardAlert into waze_hazard; commit cad4f27 now routes them
--      to waze_jam. But UPSERT keys on (source, source_id) so old
--      (waze_hazard, X) rows linger when new (waze_jam, X) rows are
--      created. The orphans show up in the Hazards subcategory list
--      as JAM_HEAVY_TRAFFIC / JAM_STAND_STILL_TRAFFIC. The parent
--      archive_waze table is append-only and unaffected; only the
--      sidecar (which feeds the filter dropdown + unique=1 reads) is
--      cleaned.
--
-- Idempotent — guarded by IS DISTINCT FROM / WHERE filters, so re-runs
-- are no-ops.

BEGIN;

UPDATE archive_waze_latest
   SET category = 'Police'
 WHERE source = 'waze_police'
   AND category IS DISTINCT FROM 'Police';

UPDATE archive_waze_latest
   SET category = 'Roadwork'
 WHERE source = 'waze_roadwork'
   AND category IS DISTINCT FROM 'Roadwork';

UPDATE archive_waze_latest
   SET category = 'Traffic Jam'
 WHERE source = 'waze_jam'
   AND category IS DISTINCT FROM 'Traffic Jam';

DELETE FROM archive_waze_latest
 WHERE source = 'waze_hazard'
   AND (category = 'JAM'
        OR subcategory = 'JAM'
        OR subcategory LIKE 'JAM\_%' ESCAPE '\');

COMMIT;
