-- 019_archive_latest_recompute.sql
--
-- Superseded — original LAG-over-whole-table recompute moved out of
-- the migration runner into a chunked, throttled background task
-- (services/archiveLatestRecompute.ts) because the SQL took >47 min
-- of pure I/O on archive_waze and blocked the API the whole time.
--
-- This file stays as a no-op placeholder so the schema_migrations
-- ledger is consistent across boxes that already applied it manually
-- (after cancelling the heavy version).

SELECT 1;
