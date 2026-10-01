-- Node-led index for the feeder owner's per-node stats.
--
-- Every query behind GET /api/feeder/nodes/:id/stats is scoped
-- `node_id = $2 AND received_at >= now() - $1 AND <call predicate>`, but the
-- only node-led index (idx_nre_node_time) is NOT partial on the call
-- predicate — and once migration 115 added the system/site indexes the
-- planner started preferring THOSE for these queries, skip-scanning them and
-- then throwing away every row belonging to another node (measured on prod:
-- 47,041 rows removed by filter, ~48k buffers touched). Warm that is ~0.5s;
-- cold it ran past the 30s statement timeout and the owner page 500ed.
--
-- This index is exactly the access path those queries want: one node's call
-- rows, already in time order. Partial on the same predicate every one of
-- them carries, so it stays small.
--
-- Plain CREATE INDEX, per migration 110/115: the runner wraps each file in a
-- transaction and CONCURRENTLY is not allowed there. Writers (ingest) block
-- during the build and buffer on the agents; readers are unaffected.
CREATE INDEX IF NOT EXISTS idx_nre_node_time_calls
  ON node_radio_events (node_id, received_at)
  WHERE event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%';
