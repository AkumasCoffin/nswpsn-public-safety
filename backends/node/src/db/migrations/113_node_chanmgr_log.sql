-- Audit log for the agents' automatic channel management: every auto-stop,
-- every retest with its verdict, every restore/release. The agent keeps the
-- same ring in memory and ships it on each status frame; this table makes the
-- history survive agent restarts and stay visible for offline nodes. Bounded
-- to the newest ~50 rows per node by the ingest path (same cap as the agent's
-- ring), so no time-based pruning is needed.
--
-- The primary key doubles as the idempotency key: the agent re-ships its whole
-- ring every 15s, and (node, at_ms, channel, kind) uniquely identifies an
-- event, so repeats land on ON CONFLICT DO NOTHING.
CREATE TABLE IF NOT EXISTS node_chanmgr_log (
  node_id TEXT   NOT NULL,
  at_ms   BIGINT NOT NULL,
  channel TEXT   NOT NULL,
  kind    TEXT   NOT NULL,
  text    TEXT   NOT NULL DEFAULT '',
  PRIMARY KEY (node_id, at_ms, channel, kind)
);
