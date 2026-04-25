#!/usr/bin/env python3
"""
apply_schema_presets.py — apply the NSW PSN bot schema to BOT_DATABASE_URL.

Canonical, single-file entry point for bringing a bot DB up-to-date. Every
statement uses IF NOT EXISTS / CREATE OR REPLACE / ADD COLUMN IF NOT EXISTS,
so re-running is always safe. When the schema changes, update SCHEMA_SQL
below and re-run this script — no external .sql file needed.

Usage
-----
    python apply_schema_presets.py            # apply
    python apply_schema_presets.py --dry-run  # print SQL, do not execute
    python apply_schema_presets.py -f path/to/other.sql
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: python-dotenv is required. pip install python-dotenv", file=sys.stderr)
    sys.exit(1)

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 is required. pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


SCHEMA_SQL = r"""
-- =============================================================================
-- NSW PSN Discord bot — preset + mute + fire-log + dashboard schema
-- =============================================================================
-- Fully idempotent: every CREATE uses IF NOT EXISTS, triggers are dropped
-- and recreated, and the trg_touch_updated_at function uses CREATE OR REPLACE.
-- =============================================================================

-- ------------------------------------------------------------------------
-- alert_presets — multiple named preset bundles per channel
-- ------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_presets (
    id               SERIAL PRIMARY KEY,
    guild_id         BIGINT NOT NULL,
    channel_id       BIGINT NOT NULL,
    name             TEXT NOT NULL,
    alert_types      TEXT[] NOT NULL DEFAULT '{}',
    pager_enabled    BOOLEAN NOT NULL DEFAULT FALSE,
    pager_capcodes   TEXT,
    role_ids         BIGINT[] NOT NULL DEFAULT '{}',
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    enabled_ping     BOOLEAN NOT NULL DEFAULT TRUE,
    type_overrides   JSONB NOT NULL DEFAULT '{}'::jsonb,
    filters          JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (guild_id, channel_id, name),
    CHECK (coalesce(array_length(alert_types, 1), 0) > 0 OR pager_enabled = TRUE)
);

-- Live-DB migrations: add columns that were introduced after the initial schema
-- shipped. Safe on fresh installs (no-op since the CREATE TABLE above covers them).
ALTER TABLE alert_presets ADD COLUMN IF NOT EXISTS filters JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_alert_presets_guild       ON alert_presets(guild_id);
CREATE INDEX IF NOT EXISTS idx_alert_presets_channel     ON alert_presets(guild_id, channel_id);
CREATE INDEX IF NOT EXISTS idx_alert_presets_alert_types ON alert_presets USING GIN(alert_types);

-- ------------------------------------------------------------------------
-- Mute override tables
-- ------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS channel_mute_state (
    guild_id     BIGINT NOT NULL,
    channel_id   BIGINT NOT NULL,
    enabled      BOOLEAN NOT NULL DEFAULT TRUE,
    enabled_ping BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (guild_id, channel_id)
);

CREATE TABLE IF NOT EXISTS guild_mute_state (
    guild_id     BIGINT PRIMARY KEY,
    enabled      BOOLEAN NOT NULL DEFAULT TRUE,
    enabled_ping BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------------
-- Auto-touch updated_at on UPDATE
-- ------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trg_touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$;

DROP TRIGGER IF EXISTS alert_presets_touch_updated ON alert_presets;
DROP TRIGGER IF EXISTS channel_mute_touch_updated  ON channel_mute_state;
DROP TRIGGER IF EXISTS guild_mute_touch_updated    ON guild_mute_state;

CREATE TRIGGER alert_presets_touch_updated BEFORE UPDATE ON alert_presets
    FOR EACH ROW EXECUTE FUNCTION trg_touch_updated_at();
CREATE TRIGGER channel_mute_touch_updated BEFORE UPDATE ON channel_mute_state
    FOR EACH ROW EXECUTE FUNCTION trg_touch_updated_at();
CREATE TRIGGER guild_mute_touch_updated BEFORE UPDATE ON guild_mute_state
    FOR EACH ROW EXECUTE FUNCTION trg_touch_updated_at();

-- ------------------------------------------------------------------------
-- preset_fire_log — one row every time a preset delivers an alert.
-- Drives the "fired N× · last fire X ago" chips on the dashboard overview.
-- Older rows should be purged periodically (see cleanup_preset_fire_log).
-- ------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS preset_fire_log (
    id         BIGSERIAL PRIMARY KEY,
    preset_id  INTEGER NOT NULL REFERENCES alert_presets(id) ON DELETE CASCADE,
    alert_type TEXT NOT NULL,
    fired_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_pfl_preset_fired_at ON preset_fire_log(preset_id, fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_pfl_fired_at        ON preset_fire_log(fired_at);

-- ------------------------------------------------------------------------
-- dash_sessions — persistent dashboard sessions so logins survive restarts.
-- Written through by the backend's _dash_session_put / _dash_session_drop.
-- ------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dash_sessions (
    sid        TEXT PRIMARY KEY,
    data       JSONB NOT NULL,
    exp        INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dash_sessions_exp ON dash_sessions(exp);

-- ------------------------------------------------------------------------
-- Phase 3 cleanup: drop legacy alert_configs / pager_configs tables.
-- Their data was migrated into alert_presets in Phase 1 (see migrate_presets.py)
-- and no code reads or writes them anymore. Dropping is idempotent thanks to
-- IF EXISTS, so re-runs are safe.
-- ------------------------------------------------------------------------
DROP TABLE IF EXISTS alert_configs CASCADE;
DROP TABLE IF EXISTS pager_configs CASCADE;

-- ------------------------------------------------------------------------
-- pending_bot_actions — admin-triggered operations that need the bot process
-- (sync slash commands, send test alerts, wipe orphaned guilds). Backend
-- inserts a row; bot polls every 10s, claims, executes, writes the result.
-- ------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pending_bot_actions (
    id            BIGSERIAL PRIMARY KEY,
    action        TEXT NOT NULL,        -- 'sync' | 'test' | 'cleanup'
    params        JSONB NOT NULL DEFAULT '{}'::jsonb,
    status        TEXT NOT NULL DEFAULT 'pending',  -- pending | running | done | error
    requested_by  TEXT,                 -- Discord user id of the admin who triggered
    requested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    result        JSONB,
    error         TEXT
);
CREATE INDEX IF NOT EXISTS idx_pending_bot_actions_status ON pending_bot_actions(status, requested_at);

-- ------------------------------------------------------------------------
-- source_health — persisted upstream-source health counters so the
-- /admin source panel survives backend restarts. The backend keeps a
-- mirror in-memory and flushes here every 60s.
-- ------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_health (
    name           TEXT PRIMARY KEY,
    last_success   BIGINT,
    last_error     BIGINT,
    last_error_msg TEXT,
    consec_fails   INTEGER NOT NULL DEFAULT 0,
    total_success  BIGINT  NOT NULL DEFAULT 0,
    total_fail     BIGINT  NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply the NSW PSN bot schema to BOT_DATABASE_URL (idempotent).",
    )
    parser.add_argument("-f", "--file",
                        help="Apply a specific .sql file instead of the embedded schema.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the SQL without executing.")
    args = parser.parse_args()

    load_dotenv()
    db_url = os.environ.get("BOT_DATABASE_URL")
    if not db_url:
        print("ERROR: BOT_DATABASE_URL is not set in the environment.", file=sys.stderr)
        return 2

    if args.file:
        sql_path = Path(args.file).resolve()
        if not sql_path.exists():
            print(f"ERROR: SQL file not found: {sql_path}", file=sys.stderr)
            return 2
        sql = sql_path.read_text(encoding="utf-8")
        source = str(sql_path)
    else:
        sql = SCHEMA_SQL
        source = "embedded schema"

    if not sql.strip():
        print("ERROR: SQL is empty.", file=sys.stderr)
        return 2

    target = db_url.split('@', 1)[-1] if '@' in db_url else db_url
    print(f"Source : {source}")
    print(f"Target : {target}")
    print(f"Bytes  : {len(sql)}")

    if args.dry_run:
        print("\n--- DRY RUN — SQL that would be applied ---")
        print(sql)
        return 0

    conn = psycopg2.connect(db_url)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql)
        print("OK — schema applied.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
