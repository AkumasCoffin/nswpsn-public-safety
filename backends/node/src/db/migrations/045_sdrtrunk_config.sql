-- The sdrtrunk-vce config (alias lists + aliases + streams) imported from the
-- operator's sdrtrunk SQLite, stored SEPARATELY from the rdio side (`agencies`).
-- The two are independent configs for two programs, linked only by systemId; each
-- is imported/replaced on its own. sdrtrunk aliases used to be GENERATED from the
-- rdio talkgroups (agenciesToTalkgroupAliases) — that's replaced by this imported
-- list, which is pushed to sdrtrunk-vce verbatim.
ALTER TABLE feeder_global_config
  ADD COLUMN IF NOT EXISTS sdrtrunk_config JSONB NOT NULL
  DEFAULT '{"aliasLists":[],"aliases":[],"streams":[]}'::jsonb;
