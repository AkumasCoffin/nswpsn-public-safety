-- Global feeder configuration, synced to ALL radio feeder nodes.
--
-- P5 adds a single shared config that every feeder node receives: the SDR-Trunk
-- aliases and the rdio systems/talkgroups/units/groups/tags. Editing it in the
-- Nodes tab bumps `version` and re-pushes to every online node, so the whole
-- fleet stays in sync (per-node config — channels + tuner — stays on nodes.
-- config_override). This is a singleton row (id = 1), seeded lazily from the
-- on-disk presets the first time it's read if still empty.
--
-- rdio_systems is the rdio-scanner.json `systems` array (each system carries its
-- own talkgroups[] + units[]); rdio_groups/rdio_tags mirror the top-level
-- `groups`/`tags`. sdrtrunk_aliases is a structured form of default.xml's
-- <alias> blocks (see services/nodes/globalConfig.ts for the shape).
CREATE TABLE IF NOT EXISTS feeder_global_config (
  id               SMALLINT PRIMARY KEY DEFAULT 1,
  sdrtrunk_aliases JSONB       NOT NULL DEFAULT '[]'::jsonb,
  rdio_systems     JSONB       NOT NULL DEFAULT '[]'::jsonb,
  rdio_groups      JSONB       NOT NULL DEFAULT '[]'::jsonb,
  rdio_tags        JSONB       NOT NULL DEFAULT '[]'::jsonb,
  version          TEXT        NOT NULL DEFAULT '',
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by       TEXT,
  -- Enforce the singleton: only id = 1 may ever exist.
  CONSTRAINT feeder_global_config_singleton CHECK (id = 1)
);
