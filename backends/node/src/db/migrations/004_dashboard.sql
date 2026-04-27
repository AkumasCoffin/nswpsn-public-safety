-- 004_dashboard.sql
-- Dashboard / Discord OAuth tables.
--
-- IMPORTANT: These tables live in the BOT_DATA_DATABASE_URL Postgres, not
-- the main DATABASE_URL. The runtime opens a third pool (botDb.ts) — but
-- our migration runner only knows about the main pool, so for now this
-- file is parked here for documentation purposes and so a deployer who
-- happens to use a single shared Postgres for both URLs gets the schema
-- for free. When BOT_DATA_DATABASE_URL points at a separate cluster the
-- discord-bot itself creates these tables on first run; the dashboard's
-- session put helper also runs CREATE TABLE IF NOT EXISTS at first use,
-- mirroring python's _dash_sessions_db_ensure() at line 16329.
--
-- Mirror of python external_api_proxy.py:16341-16361 (dash_sessions,
-- dashboard_users) plus the bot-owned tables the dashboard reads from
-- (alert_presets, guild_mute_state, channel_mute_state,
-- preset_fire_log, pending_bot_actions). Those last 5 are created and
-- written by the discord-bot in production; we declare them here as
-- IF NOT EXISTS so the dashboard's read paths are safe on a fresh DB.

-- Server-side session storage (cookie carries only {sid, exp}, the rest
-- lives here). Hydrated into the in-memory _DASH_SESSIONS map at startup.
CREATE TABLE IF NOT EXISTS dash_sessions (
  sid        TEXT PRIMARY KEY,
  data       JSONB NOT NULL,
  exp        INTEGER NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dash_sessions_exp ON dash_sessions(exp);

-- Persistent user table — survives session expiry. Lets the admin panel
-- show "every user that has ever logged in" not just current sessions.
CREATE TABLE IF NOT EXISTS dashboard_users (
  uid          TEXT PRIMARY KEY,
  username     TEXT,
  avatar       TEXT,
  first_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
  login_count  INTEGER NOT NULL DEFAULT 1
);

-- Bot-owned tables. Created and written by discord-bot/bot.py; the
-- dashboard only reads from / updates them. Declared here so a fresh
-- deploy doesn't 500 before the bot has booted.
CREATE TABLE IF NOT EXISTS alert_presets (
  id              BIGSERIAL PRIMARY KEY,
  guild_id        BIGINT NOT NULL,
  channel_id      BIGINT NOT NULL,
  name            TEXT NOT NULL,
  alert_types     TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  pager_enabled   BOOLEAN NOT NULL DEFAULT FALSE,
  pager_capcodes  TEXT,
  role_ids        BIGINT[] NOT NULL DEFAULT ARRAY[]::BIGINT[],
  enabled         BOOLEAN NOT NULL DEFAULT TRUE,
  enabled_ping    BOOLEAN NOT NULL DEFAULT TRUE,
  type_overrides  JSONB NOT NULL DEFAULT '{}'::jsonb,
  filters         JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT alert_presets_guild_channel_name_uq
    UNIQUE (guild_id, channel_id, name),
  CONSTRAINT alert_presets_nonempty
    CHECK (cardinality(alert_types) > 0 OR pager_enabled)
);
CREATE INDEX IF NOT EXISTS idx_alert_presets_guild ON alert_presets(guild_id);

CREATE TABLE IF NOT EXISTS guild_mute_state (
  guild_id     BIGINT PRIMARY KEY,
  enabled      BOOLEAN NOT NULL DEFAULT TRUE,
  enabled_ping BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS channel_mute_state (
  guild_id     BIGINT NOT NULL,
  channel_id   BIGINT NOT NULL,
  enabled      BOOLEAN NOT NULL DEFAULT TRUE,
  enabled_ping BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (guild_id, channel_id)
);

CREATE TABLE IF NOT EXISTS preset_fire_log (
  id         BIGSERIAL PRIMARY KEY,
  preset_id  BIGINT NOT NULL,
  fired_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_preset_fire_log_preset_fired
  ON preset_fire_log (preset_id, fired_at DESC);

-- Cross-process action queue: the dashboard enqueues, the bot drains.
CREATE TABLE IF NOT EXISTS pending_bot_actions (
  id            BIGSERIAL PRIMARY KEY,
  action        TEXT NOT NULL,
  params        JSONB NOT NULL DEFAULT '{}'::jsonb,
  status        TEXT NOT NULL DEFAULT 'pending',
  requested_by  TEXT,
  requested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at    TIMESTAMPTZ,
  completed_at  TIMESTAMPTZ,
  result        TEXT,
  error         TEXT
);
CREATE INDEX IF NOT EXISTS idx_pending_bot_actions_status
  ON pending_bot_actions (status, requested_at DESC);
