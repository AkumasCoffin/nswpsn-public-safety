-- Fleet-wide talkgroup defaults for the unified talkgroup model: the single
-- global alias colour and the default SDR-Trunk monitor priority. Both used to
-- be repeated on every alias (RFS orange on all 206 rows, FRNSW -1 on all of
-- its); under the unified model they are set once here and inherited, with a
-- per-agency priority override. Shape: {"color": string|null, "priority":
-- int|null} (services/nodes/globalConfig.ts TalkgroupDefaultsSchema).
ALTER TABLE feeder_global_config
  ADD COLUMN IF NOT EXISTS talkgroup_defaults JSONB NOT NULL DEFAULT '{}'::jsonb;
