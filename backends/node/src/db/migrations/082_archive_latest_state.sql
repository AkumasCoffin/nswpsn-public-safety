-- 082: give every archived incident a state/territory, so the logs page
-- can filter by one.
--
-- WHY A COLUMN AND NOT A QUERY
-- Nothing upstream reliably says which jurisdiction a record belongs to.
-- Most feeds are NSW-only and simply never mention it; the national ones
-- (LiveTraffic works, FIRMS) carry records from every state with no field
-- to tell them apart. The only trustworthy signal is the coordinate, and
-- resolving a coordinate to a state means point-in-polygon against real
-- boundaries — which this database cannot do (no PostGIS). So the answer
-- is computed in Node at write time (lib/stateMask.ts) and stored here,
-- where it can be indexed and filtered like any other dimension.
--
-- NULL means "not established": a row written before this migration and
-- not re-polled since, or a record with no usable point at all (the BOM
-- warnings are areas, not points). The logs page shows those as unknown
-- rather than guessing at one.
--
-- Backfilling is deliberately NOT done here. It cannot be — the polygon
-- test lives in application code. Live rows fill themselves in on the
-- next poll, because upsertLatestSidecar refreshes display fields
-- whenever the stored value is NULL; anything older is handled by
-- scripts/backfill-archive-state.ts.

ALTER TABLE archive_traffic_latest ADD COLUMN IF NOT EXISTS state text;
ALTER TABLE archive_rfs_latest     ADD COLUMN IF NOT EXISTS state text;
ALTER TABLE archive_power_latest   ADD COLUMN IF NOT EXISTS state text;
ALTER TABLE archive_misc_latest    ADD COLUMN IF NOT EXISTS state text;

-- Partial indexes: the filter is always "this source, these states", and
-- the NULL rows are never selected by a state filter, so leave them out.
CREATE INDEX IF NOT EXISTS archive_traffic_latest_state
  ON archive_traffic_latest (source, state) WHERE state IS NOT NULL;
CREATE INDEX IF NOT EXISTS archive_rfs_latest_state
  ON archive_rfs_latest (source, state) WHERE state IS NOT NULL;
CREATE INDEX IF NOT EXISTS archive_power_latest_state
  ON archive_power_latest (source, state) WHERE state IS NOT NULL;
CREATE INDEX IF NOT EXISTS archive_misc_latest_state
  ON archive_misc_latest (source, state) WHERE state IS NOT NULL;
