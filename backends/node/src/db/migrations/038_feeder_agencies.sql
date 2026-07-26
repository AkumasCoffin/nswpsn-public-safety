-- Unified "Agency" model for the fleet-wide feeder config: one entity that owns
-- its SDR-Trunk alias + stream AND its rdio system + apiKey. Replaces the split
-- sdrtrunk_aliases / rdio_systems editors. The old columns are kept (NOT NULL
-- DEFAULT '[]') only so rows written before this migration still read (they're
-- derived into `agencies` on read and cleared on the next save).
ALTER TABLE feeder_global_config
  ADD COLUMN IF NOT EXISTS agencies JSONB NOT NULL DEFAULT '[]'::jsonb;
