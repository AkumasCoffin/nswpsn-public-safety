-- 123: a THIRD attribution on the per-site table, so the fleet-wide systems
-- tree can report a site's call count.
--
-- node_radio_hourly_sys already carries two:
--
--   logical_calls     once per call FLEET-WIDE (rn = 1, migration 081). That
--                     is what makes a network total summable, and it must stay
--                     exactly as it is.
--   logical_calls_tg  once per (call, system, talkgroup) (migration 085), for
--                     the talkgroup card, where a patched call belongs to each
--                     of its talkgroups.
--
-- Neither answers "how many calls did THIS SITE hear". The fleet-wide sites
-- query summed logical_calls per site, and under the fleet-wide attribution
-- only the lowest (system, talkgroup, rfss, site) a call was heard on counts
-- it — so every other site in a simulcast group read far below its real
-- figure, something like "12 / 5,000" calls against receptions. It also
-- contradicted the node-scoped tree and the /api/node-data/site drill-down,
-- both of which count every call heard at the site from raw events.
--
-- So attribute once per (hour, call, system, talkgroup is NOT in the key —
-- site, rfss). Summed over the (system, site) rows the tree groups by, that
-- equals COUNT(DISTINCT logical_call_id) per site in the detail table, which
-- is exactly what the drill-down shows.
ALTER TABLE node_radio_hourly_sys
  ADD COLUMN IF NOT EXISTS logical_calls_site INTEGER NOT NULL DEFAULT 0;

-- Re-derive, as 081, 084 and 085 did — but NOT all of it. These tables are
-- derived, so a new column can only be filled by re-deriving; only
-- node_radio_hourly_sys gains one, but the rollup cursor is shared, so a
-- re-derive of one is a re-derive of all four, and the same pass also fixes
-- the boundary double-count the scan range reintroduced (see
-- services/nodeHourlyRollup.ts CALL_LOOKBACK).
--
-- The earlier migrations truncated outright. That is no longer safe: the
-- rollups are now OLDER than the detail they derive from. On this deployment
-- they reach back about nine weeks while node_radio_events keeps 30 days
-- (services/nodeEventsPruner.ts RETENTION_DAYS) — so a TRUNCATE would delete
-- five weeks of fleet history that nothing can rebuild. Outliving the detail
-- is the point of a rollup; destroying it to add a column is not a trade worth
-- making.
--
-- So clear only the hours the re-derive can actually prove, and leave the
-- older rows where they are. Their logical_calls_site stays at the column
-- default of 0, which the fleet-wide sites query reads as "no figure for that
-- hour" — the per-site number simply begins when the detail does. Every other
-- column on those rows is untouched, and they keep the pre-existing boundary
-- double-count, which is likewise unfixable without the detail.
--
-- The window is taken from the detail table itself rather than a hard-coded 30
-- days, so this stays correct if the retention changes. The pruner skips its
-- radio prune while the cursor is behind, which is its safety gate working.
-- Safe to re-run: it only clears derived data that will be rebuilt.
DO $$
DECLARE
  rebuild_from timestamptz;
BEGIN
  SELECT date_trunc('hour', min(received_at)) INTO rebuild_from FROM node_radio_events;
  IF rebuild_from IS NULL THEN
    -- No detail at all: nothing can be re-derived, so change nothing but the
    -- column. A later pass picks up from wherever the cursor already sits.
    RAISE NOTICE '123: node_radio_events is empty — keeping every rollup row as is';
    RETURN;
  END IF;
  DELETE FROM node_radio_hourly      WHERE hour >= rebuild_from;
  DELETE FROM node_radio_hourly_sys  WHERE hour >= rebuild_from;
  DELETE FROM node_radio_hourly_rx   WHERE hour >= rebuild_from;
  DELETE FROM node_radio_hourly_unit WHERE hour >= rebuild_from;
  UPDATE node_rollup_state SET rolled_up_to = rebuild_from, updated_at = now() WHERE id;
  RAISE NOTICE '123: rebuilding radio rollups from %, older rows kept', rebuild_from;
END $$;
