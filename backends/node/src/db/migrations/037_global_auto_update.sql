-- Global "auto-update" switch for feeder nodes. When true (default), agents
-- self-update automatically (on start + every 6h). When paused (false) the
-- automatic checks are a no-op so the operator can push changes without nodes
-- updating out from under them — but MANUAL updates (the staff "Update" /
-- "Update all" actions → cmd{action:'update'}) still trigger regardless.
--
-- Stored on the feeder_global_config singleton; the node-update manifest
-- endpoint reflects it so agents can honour it.
ALTER TABLE feeder_global_config
  ADD COLUMN IF NOT EXISTS auto_update BOOLEAN NOT NULL DEFAULT true;
