-- Separate the "node is running" switch from the "forward calls to the central
-- rdio" switch. A node is enabled by default (it connects, decodes, and streams
-- spectrum/status so the operator can verify config + signal strength), but its
-- feed to the central rdio starts OFF so nothing reaches the live system until
-- the operator is happy and turns it on.
--
-- The relay endpoint (POST /api/node-ingest/call-upload) accepts + counts calls
-- regardless (so the agent's queue drains and per-node stats show reception) but
-- only forwards to the central rdio when feed_enabled is true.
ALTER TABLE nodes
  ADD COLUMN IF NOT EXISTS feed_enabled BOOLEAN NOT NULL DEFAULT false;
