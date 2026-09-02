-- 084: an hourly rollup at the grain the overview tiles actually need.
--
-- /api/node-data/overview reads ~1.75M detail rows to render eight numbers.
-- Measured, the cost is linear in the window — ~1.7s per day — so 24h takes
-- ~1.9s, 7d ~12s, and 30d exceeds the 30s statement timeout and 500s. The
-- query itself is fine: run alone it is 660ms on a clean parallel index scan
-- with every buffer a cache hit. There is simply no reason to read a million
-- rows to produce eight integers.
--
-- The existing rollups cannot answer it. Of the eight tiles exactly one
-- (transmissions) is derivable from node_radio_hourly_sys, because:
--
--   * a reception is counted AFTER patch_members is unnested, and the sys
--     rollup never expands patches;
--   * every outcome hangs off the call's HOME talkgroup — the talkgroup of
--     its earliest event by id — and no rollup carries per-call ordering;
--   * the rn = 1 attribution that makes call counts summable deliberately
--     drops a call's other rows, which is exactly what dropped_site needs.
--
-- Hence a new grain. It is deliberately NARROW — hour and home talkgroup, and
-- nothing else — because the tiles are network-wide totals and every extra
-- key column multiplies the row count for no reader.
--
-- WHY home_talkgroup IS A KEY AND NOT A PRE-COMPUTED OUTCOME. The enc and
-- no_tgid buckets classify against encryptedTalkgroupIds() and
-- programmedTalkgroupIds(), both read live at request time. The programmed
-- list comes from central rdio with a 60s TTL and is edited by hand, and its
-- EMPTY case is load-bearing: empty means "we cannot classify", not "nothing
-- is programmed". Baking the outcome into a finished hour would permanently
-- mark every hour that happened to be rolled up while rdio was unreachable as
-- no_tgid. Keeping the talkgroup instead lets the read path classify exactly
-- as it does today, and re-classify history for free when the list changes.
--
-- dropped_patch and dropped_site need no columns of their own; they fall out:
--
--     dropped_patch = receptions      - receptions_home
--     dropped_site  = receptions_home - transmissions
--
-- Both identities hold over any set of rows containing all of each call's
-- receptions, which is what a window total is.

CREATE TABLE IF NOT EXISTS node_radio_hourly_rx (
  -- The hour of the call's FIRST event, so a call spanning a boundary is
  -- counted once rather than in both. Same rule as home_talkgroup: the call
  -- is the unit, and its earliest event defines it.
  hour            TIMESTAMPTZ NOT NULL,
  -- 0 is the sentinel for "no talkgroup", matching the other rollups' use of
  -- 0/-1 rather than NULL, which a primary key cannot carry.
  home_talkgroup  INTEGER NOT NULL DEFAULT 0,
  -- Patch-expanded DISTINCT (call, node, rfss, site, talkgroup).
  receptions      INTEGER NOT NULL DEFAULT 0,
  -- Of those, the ones on the call's own talkgroup. The remainder are the
  -- patch members it was simultaneously carried on.
  receptions_home INTEGER NOT NULL DEFAULT 0,
  -- Distinct calls, each attributed to exactly one row so that summing any
  -- set of rows is exact. See nodeHourlyRollup.ts — migration 081 exists
  -- because the first attempt counted calls per site and summed them, which
  -- read 406 calls as 822.
  transmissions   INTEGER NOT NULL DEFAULT 0,
  -- Of those transmissions, the ones any reception of which was recorded.
  recorded        INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (hour, home_talkgroup)
);

-- Wipe and rebuild, the way 081 did: the table is derived, so it can only be
-- replaced, not reconciled. Setting the cursor to NULL makes the next pass
-- (30s after boot) rebuild every hour the 30-day detail table can still
-- prove; it loops in 24-hour batches, so that is one pass and not 31 ticks.
--
-- The pruner will skip its radio prune while the cursor is null. That is its
-- safety gate working as intended — detail is the only thing the rollups can
-- be derived from, so nothing may be deleted until it has been summarised.
--
-- Safe to re-run: it only clears derived data.
TRUNCATE node_radio_hourly_rx;
UPDATE node_rollup_state SET rolled_up_to = NULL, updated_at = now() WHERE id;
