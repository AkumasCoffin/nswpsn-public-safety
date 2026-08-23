-- 081: rebuild the radio rollups after the call-attribution fix.
--
-- Migration 080's first pass counted distinct calls INSIDE each site row and
-- the read path summed those rows, which double-counts every call heard at
-- more than one site: measured against the detail table on one live hour, 406
-- calls were reported as 822. Receptions were right; the call counts were not.
--
-- services/nodeHourlyRollup.ts now attributes each call to exactly ONE of its
-- rows, so summing is exact. That changes how every existing row was computed,
-- and the rollup only builds hours ahead of its cursor — so the cursor is reset
-- and the tables cleared, and the next pass (30s after boot) rebuilds every
-- hour the 30-day detail table can still prove.
--
-- Safe to re-run: it only clears derived data.

TRUNCATE node_radio_hourly, node_radio_hourly_sys;
UPDATE node_rollup_state SET rolled_up_to = NULL, updated_at = now() WHERE id;
