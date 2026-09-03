-- 085: the overview's LISTS come off the rollups too.
--
-- Migration 084 moved the eight tiles onto node_radio_hourly_rx and it worked:
-- the tiles cost a flat 43ms at every window. What it left behind is now the
-- whole of the endpoint's cost -- measured on prod after 084 deployed:
--
--     24h  total= 2,039ms  totals-rollup=43ms  queries-detail= 1,552ms
--     7d   total= 9,370ms  totals-rollup=42ms  queries-detail= 9,322ms
--     30d  total=22,532ms  totals-rollup=52ms  queries-detail=22,346ms
--
-- That remainder is five list/series queries, each a COUNT(DISTINCT (4-tuple))
-- over ~1.75M detail rows, run three at a time. Two of them -- top talkgroups
-- and top sites -- node_radio_hourly_sys can already answer by summing over
-- the key columns the card does not group by. Two more -- the activity chart
-- and the per-node card -- node_radio_hourly can answer. Only the top-units
-- list has no table at all, which is what this migration adds.

-- Top radios. The one grain no rollup covers, and the only genuinely new
-- table here.
--
-- FULL GRAIN, NOT TOP-N PER HOUR. The obvious worry is that a per-unit table
-- is barely a rollup, but distinct units in an hour is bounded by CALLS in an
-- hour (~400) and in practice sits well under it -- a radio transmits many
-- times an hour, it does not appear once per transmission. That is ~250k rows
-- for 30 days at the pessimistic end, against 1.75M detail rows, and unlike a
-- top-50-per-hour cap it is EXACT: a radio busy every hour and a radio busy in
-- one hour are both counted properly over a 30-day window.
--
-- No source_unit band filter here (RID_VALID lives in the read path, as
-- tgValid does for talkgroups) so the predicate can change without the stored
-- history being wrong. Decode noise is ~118 rows a week; it is not worth a
-- column of storage policy.
CREATE TABLE IF NOT EXISTS node_radio_hourly_unit (
  -- The hour the CALL started, as every rollup now buckets. See below.
  hour        TIMESTAMPTZ NOT NULL,
  source_unit INTEGER NOT NULL,
  -- DISTINCT (call, node, rfss, site, talkgroup) -- the exact tuple the
  -- detail query counts, so the two cannot report different figures.
  receptions  INTEGER NOT NULL DEFAULT 0,
  -- Latest talker alias seen for this radio in the hour, ignoring the ones
  -- that merely echo the RID back. The read path takes the most recent
  -- non-null across the window, keeping today's fallback to the bare id.
  alias       TEXT,
  PRIMARY KEY (hour, source_unit)
);

-- A SECOND attribution on the per-site table.
--
-- logical_calls attributes each call once FLEET-WIDE (rn = 1, migration 081).
-- That is what makes a network total summable and it must stay exactly as it
-- is. But the talkgroup card's "logical" column is COUNT(DISTINCT call) per
-- TALKGROUP, and a patched call genuinely belongs to each of its talkgroups --
-- under the fleet-wide attribution only the lowest one would count it, and the
-- others would read zero.
--
-- So attribute a second time, once per (hour, call, system, talkgroup).
-- Summed across a talkgroup's sites that equals the detail query exactly.
ALTER TABLE node_radio_hourly_sys
  ADD COLUMN IF NOT EXISTS logical_calls_tg INTEGER NOT NULL DEFAULT 0;

-- Rebuild everything, as 081 and 084 did: these tables are derived, so they
-- can only be replaced, never reconciled.
--
-- Beyond the new column there is a definition change that touches every row.
-- node_radio_hourly and node_radio_hourly_sys bucketed each EVENT into its own
-- hour, so a call spanning an hour boundary was counted in both. That is about
-- one call per boundary -- ~0.3% over 30 days -- and invisible while nothing
-- read the tables, but it means a sum of hours could never quite equal the
-- detail query it is replacing. node_radio_hourly_rx already buckets a call
-- into the hour it STARTED; the other two now do the same, so every rollup
-- agrees on what hour a call belongs to and summing any set of hours is exact.
--
-- The next pass (30s after boot) rebuilds every hour the 30-day detail table
-- can still prove, in 24-hour batches -- one pass, not 31 hourly ticks. The
-- pruner will skip its radio prune while the cursor is null; that is its
-- safety gate working, since detail is the only thing the rollups derive from.
--
-- Safe to re-run: it only clears derived data.
TRUNCATE node_radio_hourly, node_radio_hourly_sys, node_radio_hourly_rx,
         node_radio_hourly_unit;
UPDATE node_rollup_state SET rolled_up_to = NULL, updated_at = now() WHERE id;
