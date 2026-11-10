-- 126: who heard what, where — so the system and site drill-downs stop
-- reading a window of node_radio_events.
--
-- The staff Data tab's system and site pages, and the Radios list inside them,
-- grouped the raw detail table over the whole window on every request. On
-- production (2026-10-15) that failed at the 30 s statement timeout: one
-- site's 7 days is ~116k rows spread over ~40k heap pages, and on this storage
-- every page that is not already cached is a random read. The same queries
-- run one after another on a warm cache take 66-232 ms; the page fires five of
-- them at once against a cold one, and a second click queues five more.
--
-- The existing rollups answer most of the drill-down (receptions, calls,
-- talkgroups, the hourly series), but none of them carries the RADIO at a
-- site or the NODE at a site, which the Radios tile, the top-radios card, the
-- receiving-nodes table and the Radios list all need. This table does.
--
-- THE GRAIN is one reception's (node, system, site, talkgroup, radio), per
-- hour. That looks wide, but on this network a site is heard by one node, so
-- node and talkgroup add nothing over (site, radio): measured on three live
-- hours, 733 / 844 / 747 rows at the full grain against 636 / 729 / 624 for
-- (site, radio) alone, from 6.9-8.8k detail rows.
--
-- Each reception (call, node, site, talkgroup) is filed under exactly ONE radio
-- — the first that keyed it — and under 0 when none was decoded, so summing
-- receptions over any set of rows is exact and agrees with
-- node_radio_hourly_sys. last_heard is the exact time, for the "Last seen"
-- columns the hour would round down.
--
-- No primary key: rows are unique by construction (the derive GROUPs BY the
-- grain), and a seven-column key would be most of the table's size. The index
-- serves the per-range DELETE and every read, which is always an hour range.
--
-- Pruned at 32 days by the rollup job. The drill-downs offer 24h / 7d / 30d
-- and nothing reads it beyond that, so there is no reason to keep it growing.
CREATE TABLE IF NOT EXISTS node_radio_hourly_heard (
  hour        TIMESTAMPTZ NOT NULL,
  node_id     TEXT        NOT NULL,
  system      INTEGER     NOT NULL,
  site_rfss   INTEGER     NOT NULL,
  site_id     INTEGER     NOT NULL,
  talkgroup   INTEGER     NOT NULL,
  source_unit INTEGER     NOT NULL,
  receptions  INTEGER     NOT NULL,
  last_heard  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nrh_heard_hour ON node_radio_hourly_heard (hour);

-- Encrypted and recorded calls PER SITE. The existing columns attribute a call
-- once fleet-wide (to its lowest site), which is right for a system total and
-- wrong for a site: every other site in a simulcast group would read near
-- zero. Same third attribution migration 123 added for logical_calls_site.
-- Nullable: NULL is "not derived yet", which the rebuild below clears.
ALTER TABLE node_radio_hourly_sys
  ADD COLUMN IF NOT EXISTS encrypted_calls_site INTEGER,
  ADD COLUMN IF NOT EXISTS recorded_calls_site  INTEGER;

-- THE BACKFILL, WITHOUT MOVING THE SEAM.
--
-- Migrations 081/084/085/123 rebuilt history by rewinding rolled_up_to. That
-- cursor is also the seam every staff read splits on (api/node-data.ts
-- resolveRollupEdge): rewinding it a month put every Data view back on a
-- month of raw detail until the catch-up finished, and on this storage the
-- catch-up after 123 stalled.
--
-- rederive_below is a second cursor that walks the other way. The rollup job
-- re-derives the hours just below it, newest first, and lowers it, while
-- rolled_up_to — and with it the seam — stays where it is. Re-deriving an
-- hour is DELETE + INSERT in one transaction, so a read sees the old rows or
-- the new ones, never a gap. It stops at the oldest hour the detail table
-- still covers in full and sets itself back to NULL.
--
-- Until the walk passes an hour, that hour has no rows here and NULL in the
-- two new columns, so a 7 d or 30 d drill-down under-reports radios, nodes and
-- per-site encrypted calls for the older part of its window. Newest first, so
-- 24 h is whole after the first pass and 30 d within about a day.
ALTER TABLE node_rollup_state ADD COLUMN IF NOT EXISTS rederive_below TIMESTAMPTZ;
UPDATE node_rollup_state SET rederive_below = rolled_up_to, updated_at = now()
 WHERE id AND rolled_up_to IS NOT NULL AND rederive_below IS NULL;
