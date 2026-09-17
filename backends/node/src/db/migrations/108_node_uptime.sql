-- How much of the time each node was actually there.
--
-- One row per node per hour, holding a SIXTY-BIT MASK: bit n is set if the node
-- was heard from during minute n of that hour. Every kind writes it — the WS
-- status heartbeat is common to radio, pager and ADS-B — so this is the one
-- presence record that works the same way for all of them.
--
-- A mask rather than a counter because merging is then an OR, which is both
-- idempotent and safe across a backend restart. A counter would have to know
-- which minutes it had already counted: after a restart the in-memory tally
-- begins again at zero, and either adding it (double-counting the overlap) or
-- taking the larger (losing everything before the restart) is wrong. Setting
-- the same bit twice is simply the same bit.
--
-- Bit 59 is the highest used, so this fits a signed bigint with room to spare
-- and needs no extension.
CREATE TABLE IF NOT EXISTS node_uptime_hourly (
  node_id   TEXT        NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  hour      TIMESTAMPTZ NOT NULL,
  seen_mask BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (node_id, hour)
);

-- "This node's last N hours", which is both the percentage and the series.
CREATE INDEX IF NOT EXISTS idx_node_uptime_node_hour
  ON node_uptime_hourly (node_id, hour DESC);
