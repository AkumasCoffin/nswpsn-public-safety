-- Active P25 patch groups per site, from the node's vce /site/snapshots feed.
--
-- A patch group temporarily merges several talkgroups so they hear one another
-- (common during multi-agency incidents). Without it the traffic shows up split
-- across the member talkgroups with nothing to explain that they were operating
-- as one unit.
--
-- Shape mirrors the sibling channels/neighbors/bands lists — a JSONB array read
-- whole, never queried element-wise — so it needs no index and defaults to an
-- empty array rather than NULL. Existing rows backfill to '[]' and are then
-- overwritten by the next snapshot upsert (~60s), so no data migration is
-- needed: nodes still on an older runtime simply keep sending no patches, and
-- the column stays empty for them.
ALTER TABLE node_site_snapshots
  ADD COLUMN IF NOT EXISTS patches JSONB NOT NULL DEFAULT '[]'::jsonb;
