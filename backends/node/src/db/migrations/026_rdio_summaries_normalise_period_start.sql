-- One-off cleanup for the millisecond-boundary bug in llm.ts localHourStartUtc.
--
-- Before the fix, that helper leaked now's milliseconds into the hour boundary,
-- so period_start came out as HH:00:00.xxx with a different sub-second value
-- every run. Two consequences left junk in this table:
--   1. The boot catch-up's existence check (WHERE period_start = $1) never
--      matched an existing row, so every restart re-summarised the previous
--      hour — creating a fresh near-duplicate row each time (e.g. 15:00:00.286,
--      15:00:00.844, 15:00:00.878 alongside the 15:00:00.001 scheduled one).
--   2. UNIQUE(summary_type, period_start) couldn't dedupe them because the
--      sub-second parts differed.
--
-- This migration:
--   1. collapses the per-hour duplicates, keeping the most recently created row
--      (the latest catch-up carries the fullest call_count; the HH:55 prefetch
--      is the partial one), then
--   2. strips the sub-second noise so period_start is a clean HH:00:00 boundary
--      that the runtime check matches and the unique constraint can enforce.
--
-- Scoped to hourly rows (the only ones the buggy helper fed). Idempotent:
-- re-running finds no duplicates and nothing left to trim.

-- 1) Delete duplicate hourly rows, keeping the newest per (type, hour).
DELETE FROM rdio_summaries s
USING (
  SELECT id,
         row_number() OVER (
           PARTITION BY summary_type, date_trunc('second', period_start)
           ORDER BY created_at DESC, id DESC
         ) AS rn
  FROM rdio_summaries
  WHERE summary_type = 'hourly'
) ranked
WHERE s.id = ranked.id
  AND ranked.rn > 1;

-- 2) Normalise survivors to a clean second boundary (drops the leftover ms).
--    Post-dedupe there is exactly one survivor per truncated boundary, so this
--    can't collide with the UNIQUE(summary_type, period_start) constraint.
UPDATE rdio_summaries
SET period_start = date_trunc('second', period_start)
WHERE summary_type = 'hourly'
  AND period_start <> date_trunc('second', period_start);
