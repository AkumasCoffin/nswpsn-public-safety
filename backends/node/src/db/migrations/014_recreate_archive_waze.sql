-- 014_recreate_archive_waze.sql
--
-- Recreate archive_waze from scratch. Production drop happened during
-- recovery: the table was so bloated (~2x the row count it should have
-- had, after the is_latest backfill + dead-tuple churn) that it was
-- faster to drop and start fresh than to VACUUM FULL through it.
--
-- This migration is the union of every archive_waze-touching piece of
-- the prior schema:
--   - 002 → CREATE TABLE archive_waze partitioned by month, base indexes
--   - 005 → idx_archive_waze_src_ts (compound source+ts)
--   - 010 → per-partition autovacuum tuning (handled by
--           ensure_archive_partition's CREATE OR REPLACE in the same
--           migration; archive_waze just needs the function to exist
--           which it already does from 002+010)
--
-- Idempotent — `IF NOT EXISTS` everywhere — so re-running on a healthy
-- DB is a no-op. The other archive_* tables aren't touched here.
--
-- After this migration runs the writer will resume INSERTs, but the
-- table starts empty. Historical waze data is gone (the user dropped
-- it intentionally on 2026-04-29 because the bloat was unrecoverable).

CREATE TABLE IF NOT EXISTS archive_waze (
  id          BIGSERIAL    NOT NULL,
  source      TEXT         NOT NULL,
  source_id   TEXT,
  fetched_at  TIMESTAMPTZ  NOT NULL,
  lat         DOUBLE PRECISION,
  lng         DOUBLE PRECISION,
  category    TEXT,
  subcategory TEXT,
  data        JSONB        NOT NULL,
  PRIMARY KEY (id, fetched_at)
) PARTITION BY RANGE (fetched_at);

-- Base indexes from migration 002.
CREATE INDEX IF NOT EXISTS idx_archive_waze_src_sid_ts
  ON archive_waze (source, source_id, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_archive_waze_ts
  ON archive_waze (fetched_at DESC);

-- Compound index from migration 005 — heatmap + data-history + DISTINCT
-- ON-with-time-window patterns all hit (source, fetched_at DESC).
CREATE INDEX IF NOT EXISTS idx_archive_waze_src_ts
  ON archive_waze (source, fetched_at DESC);

-- Seed current + next month partitions so the first INSERT after the
-- service restarts doesn't fail on "no partition for value found in
-- partitioned table". ensure_archive_partition (defined in 002, patched
-- in 010 to apply autovacuum tuning) handles partition creation +
-- per-partition autovacuum settings in one call.
DO $$
DECLARE
  this_month DATE := date_trunc('month', now())::date;
  next_month DATE := (date_trunc('month', now()) + INTERVAL '1 month')::date;
BEGIN
  PERFORM ensure_archive_partition('archive_waze', this_month);
  PERFORM ensure_archive_partition('archive_waze', next_month);
END $$;
