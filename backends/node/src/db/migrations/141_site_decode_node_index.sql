-- noTransaction
--
-- 141: node_site_decode_samples by node and time.
--
-- The owner's decode chart on feeder.html reads one node's samples over 24h
-- or 7d. The table only had per-site and by-age indexes, so that read meant
-- walking every node's samples for the whole range. The staff site chart's
-- node filter benefits too.
--
-- CONCURRENTLY so sample writes keep flowing while it builds, and so the sole
-- statement here (see 131).
--
-- RECOVERY if the build is interrupted:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_site_decode_node_time;
--   DELETE FROM schema_migrations WHERE filename = '141_site_decode_node_index.sql';
-- and deploy again.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_site_decode_node_time
  ON node_site_decode_samples (node_id, sampled_at);
