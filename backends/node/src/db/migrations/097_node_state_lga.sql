-- 097: per-node Australian state + LGA.
--
-- Pager feeder nodes are going Australia-wide (one central Pagermon per
-- state), so each node carries the state it operates in — the relay routes
-- its messages to that state's Pagermon, and the state also selects the
-- pager frequency plan pushed to the node. `lga` is the coarse locality tag
-- (same ABS LGA vocabulary the boundaries table / signup use); pager nodes
-- record state+LGA where radio nodes keep the NSW RFS `zone`.
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS state TEXT;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS lga TEXT;

-- Every pre-existing node (radio and pager alike) was a NSW deployment —
-- the site was NSW-only until this migration.
UPDATE nodes SET state = 'NSW' WHERE state IS NULL;
