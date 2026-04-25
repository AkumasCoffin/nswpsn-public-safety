"""
Database module for storing alert and pager configurations.
Uses PostgreSQL when BOT_DATABASE_URL is set, otherwise SQLite.
"""

import os
import re
import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

# Defensive: load .env here too so this module can be imported standalone
# (e.g. from migrate_sqlite_to_postgres.py or the Python REPL) and still
# pick up BOT_DATABASE_URL without relying on the caller having loaded it
# first. In the bot.py path this is a no-op since .env is already loaded.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger('nswpsn-bot.database')

# PostgreSQL connection URL (takes precedence over SQLite)
BOT_DATABASE_URL = os.getenv('BOT_DATABASE_URL')
# SQLite fallback - path to local file
DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), 'bot_config.db')
DB_PATH = os.getenv('BOT_DB_PATH', DEFAULT_DB_PATH)

USE_POSTGRES = bool(BOT_DATABASE_URL)
if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor, Json
else:
    import sqlite3


# Allow-list pattern for alert_type values that are string-formatted into
# jsonb_set path literals. Rejects anything outside [a-z0-9_] defensively —
# alert_type values in this codebase are lowercase snake_case identifiers.
_ALERT_TYPE_RE = re.compile(r'^[a-z0-9_]+$')


def _validate_alert_type_key(alert_type: str) -> str:
    if not isinstance(alert_type, str) or not _ALERT_TYPE_RE.match(alert_type):
        raise ValueError(f"invalid alert_type for jsonb key: {alert_type!r}")
    return alert_type


class Database:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
    
    def _connect(self):
        """Create a database connection"""
        if USE_POSTGRES:
            conn = psycopg2.connect(BOT_DATABASE_URL, cursor_factory=RealDictCursor)
            return conn
        else:
            # timeout = how long sqlite3 will poll while the DB is locked.
            # Under load the event-loop thread and the queue-processor thread
            # both hit the DB; without a timeout readers raise "database is
            # locked" as soon as a writer holds it.
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            # WAL lets readers and a single writer work concurrently — this is
            # the actual fix for the "database is locked" errors we were seeing
            # when poll_alerts fires while process_message_queue is committing
            # an incident_messages row. Pragma is persistent once set, so this
            # is effectively a no-op after the first connection.
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA busy_timeout=30000')
            return conn
    
    def init_db(self):
        """Initialize the database tables"""
        conn = self._connect()
        c = conn.cursor()
        
        if USE_POSTGRES:
            # PostgreSQL DDL — alert_configs / pager_configs were dropped in
            # Phase 3 (everything lives in alert_presets now).
            c.execute('''
                CREATE TABLE IF NOT EXISTS seen_alerts (
                    id SERIAL PRIMARY KEY,
                    alert_type TEXT NOT NULL,
                    alert_id TEXT NOT NULL,
                    first_seen TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS')),
                    UNIQUE(alert_type, alert_id)
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS seen_pager (
                    id SERIAL PRIMARY KEY,
                    message_hash TEXT NOT NULL UNIQUE,
                    first_seen TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS'))
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS incident_messages (
                    id SERIAL PRIMARY KEY,
                    incident_guid TEXT NOT NULL,
                    channel_id BIGINT NOT NULL,
                    message_url TEXT NOT NULL,
                    status TEXT,
                    created_at TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS')),
                    UNIQUE(incident_guid, channel_id, status)
                )
            ''')

            # ------------------------------------------------------------------
            # Phase 1: alert_presets + mute-state tables (Postgres only — uses
            # TEXT[], BIGINT[], JSONB, GIN, TIMESTAMPTZ). Mirrors schema_presets.sql.
            # ------------------------------------------------------------------
            c.execute('''
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
                )
            ''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_alert_presets_guild       ON alert_presets(guild_id)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_alert_presets_channel     ON alert_presets(guild_id, channel_id)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_alert_presets_alert_types ON alert_presets USING GIN(alert_types)')

            c.execute('''
                CREATE TABLE IF NOT EXISTS channel_mute_state (
                    guild_id     BIGINT NOT NULL,
                    channel_id   BIGINT NOT NULL,
                    enabled      BOOLEAN NOT NULL DEFAULT TRUE,
                    enabled_ping BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, channel_id)
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS guild_mute_state (
                    guild_id     BIGINT PRIMARY KEY,
                    enabled      BOOLEAN NOT NULL DEFAULT TRUE,
                    enabled_ping BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            ''')

            c.execute('''
                CREATE OR REPLACE FUNCTION trg_touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN NEW.updated_at = now(); RETURN NEW; END;
                $$
            ''')
            c.execute('DROP TRIGGER IF EXISTS alert_presets_touch_updated ON alert_presets')
            c.execute('DROP TRIGGER IF EXISTS channel_mute_touch_updated  ON channel_mute_state')
            c.execute('DROP TRIGGER IF EXISTS guild_mute_touch_updated    ON guild_mute_state')
            c.execute('''
                CREATE TRIGGER alert_presets_touch_updated BEFORE UPDATE ON alert_presets
                    FOR EACH ROW EXECUTE FUNCTION trg_touch_updated_at()
            ''')
            c.execute('''
                CREATE TRIGGER channel_mute_touch_updated BEFORE UPDATE ON channel_mute_state
                    FOR EACH ROW EXECUTE FUNCTION trg_touch_updated_at()
            ''')
            c.execute('''
                CREATE TRIGGER guild_mute_touch_updated BEFORE UPDATE ON guild_mute_state
                    FOR EACH ROW EXECUTE FUNCTION trg_touch_updated_at()
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS preset_fire_log (
                    id         BIGSERIAL PRIMARY KEY,
                    preset_id  INTEGER NOT NULL REFERENCES alert_presets(id) ON DELETE CASCADE,
                    alert_type TEXT NOT NULL,
                    fired_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            ''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_pfl_preset_fired_at ON preset_fire_log(preset_id, fired_at DESC)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_pfl_fired_at ON preset_fire_log(fired_at)')

            # Admin-triggered actions from the dashboard (/sync, /test, /cleanup).
            c.execute('''
                CREATE TABLE IF NOT EXISTS pending_bot_actions (
                    id            BIGSERIAL PRIMARY KEY,
                    action        TEXT NOT NULL,
                    params        JSONB NOT NULL DEFAULT '{}'::jsonb,
                    status        TEXT NOT NULL DEFAULT 'pending',
                    requested_by  TEXT,
                    requested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    claimed_at    TIMESTAMPTZ,
                    completed_at  TIMESTAMPTZ,
                    result        JSONB,
                    error         TEXT
                )
            ''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_pending_bot_actions_status ON pending_bot_actions(status, requested_at)')

            # Live-DB migration for the per-preset filters column.
            c.execute("ALTER TABLE alert_presets ADD COLUMN IF NOT EXISTS filters JSONB NOT NULL DEFAULT '{}'::jsonb")
        else:
            # SQLite DDL — alert_configs / pager_configs are gone (Phase 3).
            # SQLite is also no longer the supported runtime; the bot expects
            # Postgres. Kept for the seen_alerts/seen_pager/incident_messages
            # tables in case anyone still imports this module against SQLite.
            c.execute('''
                CREATE TABLE IF NOT EXISTS seen_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_type TEXT NOT NULL,
                    alert_id TEXT NOT NULL,
                    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(alert_type, alert_id)
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS seen_pager (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_hash TEXT NOT NULL UNIQUE,
                    first_seen TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS incident_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_guid TEXT NOT NULL,
                    channel_id INTEGER NOT NULL,
                    message_url TEXT NOT NULL,
                    status TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(incident_guid, channel_id, status)
                )
            ''')
        
        c.execute('CREATE INDEX IF NOT EXISTS idx_seen_alerts_type ON seen_alerts(alert_type)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_seen_pager_hash ON seen_pager(message_hash)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_incident_messages_guid ON incident_messages(incident_guid)')

        conn.commit()
        conn.close()

        logger.info(f"Database initialized ({'PostgreSQL' if USE_POSTGRES else self.db_path})")
    
    def _placeholders(self, n):
        return ','.join(['%s'] * n) if USE_POSTGRES else ','.join(['?'] * n)
    
    def _param(self, x):
        return '%s' if USE_POSTGRES else '?'
    
    # ==================== ROLE-IDS PARSER ====================

    @staticmethod
    def parse_role_ids(role_ids_value, legacy_role_id=None) -> List[int]:
        """Normalise role_ids column (CSV of ints) + legacy role_id into a list."""
        out = []
        if role_ids_value:
            for part in str(role_ids_value).split(','):
                part = part.strip()
                if not part:
                    continue
                try:
                    out.append(int(part))
                except ValueError:
                    continue
        if legacy_role_id is not None:
            try:
                rid = int(legacy_role_id)
                if rid and rid not in out:
                    out.append(rid)
            except (TypeError, ValueError):
                pass
        return out

    # ==================== ALERT PRESETS ====================

    @staticmethod
    def _require_postgres():
        if not USE_POSTGRES:
            raise RuntimeError("Alert presets require PostgreSQL (BOT_DATABASE_URL)")

    def create_preset(self, guild_id: int, channel_id: int, name: str,
                      alert_types: Optional[List[str]] = None,
                      pager_enabled: bool = False,
                      pager_capcodes: Optional[str] = None,
                      role_ids: Optional[List[int]] = None,
                      enabled: bool = True,
                      enabled_ping: bool = True) -> int:
        """Insert a new preset row, return id. Raises on UNIQUE violation."""
        self._require_postgres()
        alert_types = list(alert_types or [])
        role_ids = [int(r) for r in (role_ids or [])]
        conn = self._connect()
        c = conn.cursor()
        c.execute('''
            INSERT INTO alert_presets
                (guild_id, channel_id, name, alert_types, pager_enabled,
                 pager_capcodes, role_ids, enabled, enabled_ping)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (guild_id, channel_id, name, alert_types, pager_enabled,
              pager_capcodes, role_ids, enabled, enabled_ping))
        row = c.fetchone()
        preset_id = row['id']
        conn.commit()
        conn.close()
        logger.info(f"Created preset id={preset_id} guild={guild_id} channel={channel_id} name={name!r}")
        return preset_id

    def get_preset(self, preset_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single preset by id."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT * FROM alert_presets WHERE id = %s', (preset_id,))
        row = c.fetchone()
        conn.close()
        logger.debug(f"get_preset id={preset_id} -> {'hit' if row else 'miss'}")
        return dict(row) if row else None

    def get_preset_by_name(self, guild_id: int, channel_id: int, name: str) -> Optional[Dict[str, Any]]:
        """Fetch a preset by (guild, channel, name) tuple."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT * FROM alert_presets WHERE guild_id = %s AND channel_id = %s AND name = %s',
                  (guild_id, channel_id, name))
        row = c.fetchone()
        conn.close()
        logger.debug(f"get_preset_by_name guild={guild_id} channel={channel_id} name={name!r} -> {'hit' if row else 'miss'}")
        return dict(row) if row else None

    def list_presets_in_channel(self, guild_id: int, channel_id: int) -> List[Dict[str, Any]]:
        """All presets in a channel, ordered by name."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT * FROM alert_presets WHERE guild_id = %s AND channel_id = %s ORDER BY name',
                  (guild_id, channel_id))
        rows = c.fetchall()
        conn.close()
        logger.debug(f"list_presets_in_channel guild={guild_id} channel={channel_id} -> {len(rows)} rows")
        return [dict(row) for row in rows]

    def list_presets_in_guild(self, guild_id: int) -> List[Dict[str, Any]]:
        """All presets in a guild, ordered by (channel_id, name)."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT * FROM alert_presets WHERE guild_id = %s ORDER BY channel_id, name', (guild_id,))
        rows = c.fetchall()
        conn.close()
        logger.debug(f"list_presets_in_guild guild={guild_id} -> {len(rows)} rows")
        return [dict(row) for row in rows]

    def list_all_presets(self) -> List[Dict[str, Any]]:
        """Every preset in every guild."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT * FROM alert_presets ORDER BY guild_id, channel_id, name')
        rows = c.fetchall()
        conn.close()
        logger.debug(f"list_all_presets -> {len(rows)} rows")
        return [dict(row) for row in rows]

    def get_presets_for_alert_type(self, alert_type: str) -> List[Dict[str, Any]]:
        """All presets subscribed to alert_type (GIN array-contains). Mute state NOT applied."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT * FROM alert_presets WHERE alert_types @> ARRAY[%s]::TEXT[]', (alert_type,))
        rows = c.fetchall()
        conn.close()
        logger.debug(f"get_presets_for_alert_type {alert_type!r} -> {len(rows)} rows")
        return [dict(row) for row in rows]

    def get_presets_for_pager(self) -> List[Dict[str, Any]]:
        """All presets with pager_enabled = TRUE."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT * FROM alert_presets WHERE pager_enabled = TRUE')
        rows = c.fetchall()
        conn.close()
        logger.debug(f"get_presets_for_pager -> {len(rows)} rows")
        return [dict(row) for row in rows]

    def update_preset(self, preset_id: int, *,
                      name: Optional[str] = None,
                      alert_types: Optional[List[str]] = None,
                      pager_enabled: Optional[bool] = None,
                      pager_capcodes: Optional[str] = None,
                      role_ids: Optional[List[int]] = None,
                      enabled: Optional[bool] = None,
                      enabled_ping: Optional[bool] = None,
                      type_overrides: Optional[Dict[str, Any]] = None) -> bool:
        """Partial update — only fields that aren't None are written. Returns True on 1+ row updated."""
        self._require_postgres()
        sets: List[str] = []
        params: List[Any] = []
        if name is not None:
            sets.append('name = %s'); params.append(name)
        if alert_types is not None:
            sets.append('alert_types = %s'); params.append(list(alert_types))
        if pager_enabled is not None:
            sets.append('pager_enabled = %s'); params.append(bool(pager_enabled))
        if pager_capcodes is not None:
            sets.append('pager_capcodes = %s'); params.append(pager_capcodes)
        if role_ids is not None:
            sets.append('role_ids = %s'); params.append([int(r) for r in role_ids])
        if enabled is not None:
            sets.append('enabled = %s'); params.append(bool(enabled))
        if enabled_ping is not None:
            sets.append('enabled_ping = %s'); params.append(bool(enabled_ping))
        if type_overrides is not None:
            sets.append('type_overrides = %s'); params.append(Json(type_overrides))
        if not sets:
            return False
        params.append(preset_id)
        conn = self._connect()
        c = conn.cursor()
        c.execute(f"UPDATE alert_presets SET {', '.join(sets)} WHERE id = %s", params)
        updated = (c.rowcount or 0) > 0
        conn.commit()
        conn.close()
        logger.info(f"update_preset id={preset_id} fields={[s.split(' =')[0] for s in sets]} updated={updated}")
        return updated

    def set_preset_type_override(self, preset_id: int, alert_type: str, *,
                                 enabled: Optional[bool] = None,
                                 enabled_ping: Optional[bool] = None):
        """Set per-type override inside type_overrides JSONB. If both args None, removes the key."""
        self._require_postgres()
        _validate_alert_type_key(alert_type)
        path = '{' + alert_type + '}'
        conn = self._connect()
        c = conn.cursor()
        if enabled is None and enabled_ping is None:
            c.execute('UPDATE alert_presets SET type_overrides = type_overrides #- %s WHERE id = %s',
                      (path, preset_id))
        else:
            # Default missing halves to True so stored record is well-formed.
            value = {
                'enabled': True if enabled is None else bool(enabled),
                'enabled_ping': True if enabled_ping is None else bool(enabled_ping),
            }
            c.execute(
                'UPDATE alert_presets SET type_overrides = jsonb_set(type_overrides, %s, %s::jsonb, true) WHERE id = %s',
                (path, json.dumps(value), preset_id),
            )
        conn.commit()
        conn.close()
        logger.info(f"set_preset_type_override preset={preset_id} type={alert_type} enabled={enabled} ping={enabled_ping}")

    def clear_preset_type_override(self, preset_id: int, alert_type: str):
        """Remove a single per-type override key from type_overrides."""
        self._require_postgres()
        _validate_alert_type_key(alert_type)
        path = '{' + alert_type + '}'
        conn = self._connect()
        c = conn.cursor()
        c.execute('UPDATE alert_presets SET type_overrides = type_overrides #- %s WHERE id = %s',
                  (path, preset_id))
        conn.commit()
        conn.close()
        logger.info(f"clear_preset_type_override preset={preset_id} type={alert_type}")

    def delete_preset(self, preset_id: int) -> bool:
        """Delete one preset by id. Returns True if a row was deleted."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('DELETE FROM alert_presets WHERE id = %s', (preset_id,))
        deleted = (c.rowcount or 0) > 0
        conn.commit()
        conn.close()
        logger.info(f"delete_preset id={preset_id} deleted={deleted}")
        return deleted

    def delete_presets_in_channel(self, guild_id: int, channel_id: int) -> int:
        """Delete every preset in a channel. Returns count deleted."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('DELETE FROM alert_presets WHERE guild_id = %s AND channel_id = %s',
                  (guild_id, channel_id))
        count = c.rowcount or 0
        conn.commit()
        conn.close()
        logger.info(f"delete_presets_in_channel guild={guild_id} channel={channel_id} deleted={count}")
        return count

    # ==================== MUTE STATE ====================

    _DEFAULT_MUTE = {'enabled': True, 'enabled_ping': True}

    def get_guild_mute(self, guild_id: int) -> Dict[str, bool]:
        """Guild-level mute state. Defaults to (True, True) if no row."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT enabled, enabled_ping FROM guild_mute_state WHERE guild_id = %s', (guild_id,))
        row = c.fetchone()
        conn.close()
        logger.debug(f"get_guild_mute guild={guild_id} -> {'hit' if row else 'default'}")
        if not row:
            return dict(self._DEFAULT_MUTE)
        return {'enabled': bool(row['enabled']), 'enabled_ping': bool(row['enabled_ping'])}

    def set_guild_mute(self, guild_id: int, *,
                       enabled: Optional[bool] = None,
                       enabled_ping: Optional[bool] = None):
        """UPSERT guild_mute_state. Partial updates preserve the other column; no-op if both args are None."""
        self._require_postgres()
        if enabled is None and enabled_ping is None:
            return
        # Build insert defaults + conflict updater. For unspecified args on
        # INSERT, lean on DEFAULT TRUE; on UPDATE, preserve existing via COALESCE.
        ins_enabled = True if enabled is None else bool(enabled)
        ins_ping = True if enabled_ping is None else bool(enabled_ping)
        upd_enabled_sql = 'guild_mute_state.enabled' if enabled is None else 'EXCLUDED.enabled'
        upd_ping_sql = 'guild_mute_state.enabled_ping' if enabled_ping is None else 'EXCLUDED.enabled_ping'
        sql = f'''
            INSERT INTO guild_mute_state (guild_id, enabled, enabled_ping)
            VALUES (%s, %s, %s)
            ON CONFLICT (guild_id) DO UPDATE SET
                enabled = {upd_enabled_sql},
                enabled_ping = {upd_ping_sql}
        '''
        conn = self._connect()
        c = conn.cursor()
        c.execute(sql, (guild_id, ins_enabled, ins_ping))
        conn.commit()
        conn.close()
        logger.info(f"set_guild_mute guild={guild_id} enabled={enabled} ping={enabled_ping}")

    def clear_guild_mute(self, guild_id: int):
        """Drop guild_mute_state row (i.e. reset to defaults)."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('DELETE FROM guild_mute_state WHERE guild_id = %s', (guild_id,))
        conn.commit()
        conn.close()
        logger.info(f"clear_guild_mute guild={guild_id}")

    def get_channel_mute(self, guild_id: int, channel_id: int) -> Dict[str, bool]:
        """Channel-level mute state. Defaults to (True, True) if no row."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT enabled, enabled_ping FROM channel_mute_state WHERE guild_id = %s AND channel_id = %s',
                  (guild_id, channel_id))
        row = c.fetchone()
        conn.close()
        logger.debug(f"get_channel_mute guild={guild_id} channel={channel_id} -> {'hit' if row else 'default'}")
        if not row:
            return dict(self._DEFAULT_MUTE)
        return {'enabled': bool(row['enabled']), 'enabled_ping': bool(row['enabled_ping'])}

    def set_channel_mute(self, guild_id: int, channel_id: int, *,
                         enabled: Optional[bool] = None,
                         enabled_ping: Optional[bool] = None):
        """UPSERT channel_mute_state. Partial updates preserve the other column; no-op if both args are None."""
        self._require_postgres()
        if enabled is None and enabled_ping is None:
            return
        ins_enabled = True if enabled is None else bool(enabled)
        ins_ping = True if enabled_ping is None else bool(enabled_ping)
        upd_enabled_sql = 'channel_mute_state.enabled' if enabled is None else 'EXCLUDED.enabled'
        upd_ping_sql = 'channel_mute_state.enabled_ping' if enabled_ping is None else 'EXCLUDED.enabled_ping'
        sql = f'''
            INSERT INTO channel_mute_state (guild_id, channel_id, enabled, enabled_ping)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, channel_id) DO UPDATE SET
                enabled = {upd_enabled_sql},
                enabled_ping = {upd_ping_sql}
        '''
        conn = self._connect()
        c = conn.cursor()
        c.execute(sql, (guild_id, channel_id, ins_enabled, ins_ping))
        conn.commit()
        conn.close()
        logger.info(f"set_channel_mute guild={guild_id} channel={channel_id} enabled={enabled} ping={enabled_ping}")

    def clear_channel_mute(self, guild_id: int, channel_id: int):
        """Drop channel_mute_state row."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('DELETE FROM channel_mute_state WHERE guild_id = %s AND channel_id = %s',
                  (guild_id, channel_id))
        conn.commit()
        conn.close()
        logger.info(f"clear_channel_mute guild={guild_id} channel={channel_id}")

    def list_channel_mutes(self, guild_id: int) -> List[Dict[str, Any]]:
        """All channel mute rows for a guild."""
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT * FROM channel_mute_state WHERE guild_id = %s ORDER BY channel_id', (guild_id,))
        rows = c.fetchall()
        conn.close()
        logger.debug(f"list_channel_mutes guild={guild_id} -> {len(rows)} rows")
        return [dict(row) for row in rows]

    # ---- 4-tier mute resolution ----

    @staticmethod
    def resolve_preset_effective_state(
        preset: Dict[str, Any],
        alert_type: Optional[str],
        channel_mute: Optional[Dict[str, Any]],
        guild_mute: Optional[Dict[str, Any]],
    ) -> Dict[str, bool]:
        """Collapse guild/channel/preset/per-type mute settings into a single
        (enabled, enabled_ping) decision. AND semantics: a `False` at ANY tier
        disables that channel. alert_type may be None for pager or generic checks
        (in which case per-type overrides are skipped).

        Returns: {"enabled": bool, "enabled_ping": bool}
        """
        enabled = True
        enabled_ping = True

        def _and(state: Optional[Dict[str, Any]]):
            nonlocal enabled, enabled_ping
            if not state:
                return
            if 'enabled' in state and state['enabled'] is not None:
                enabled = enabled and bool(state['enabled'])
            if 'enabled_ping' in state and state['enabled_ping'] is not None:
                enabled_ping = enabled_ping and bool(state['enabled_ping'])

        _and(guild_mute)
        _and(channel_mute)
        _and(preset)

        if alert_type is not None:
            overrides = preset.get('type_overrides') if preset else None
            if isinstance(overrides, dict):
                per_type = overrides.get(alert_type)
                if isinstance(per_type, dict):
                    _and(per_type)

        if not enabled:
            enabled_ping = False
        return {'enabled': enabled, 'enabled_ping': enabled_ping}

    # ==================== FIRE LOG ====================

    def log_preset_fires(self, rows: List[tuple]):
        """Append (preset_id, alert_type) pairs to preset_fire_log. Best-effort:
        a failure here must not prevent alert delivery, so we swallow errors."""
        if not rows or not USE_POSTGRES:
            return
        try:
            conn = self._connect()
            c = conn.cursor()
            c.executemany(
                'INSERT INTO preset_fire_log (preset_id, alert_type) VALUES (%s, %s)',
                [(int(pid), str(at)) for pid, at in rows],
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"preset_fire_log insert failed: {e}")

    def cleanup_preset_fire_log(self, days: int = 30):
        if not USE_POSTGRES:
            return
        conn = self._connect()
        c = conn.cursor()
        c.execute("DELETE FROM preset_fire_log WHERE fired_at < NOW() - INTERVAL '1 day' * %s", (days,))
        conn.commit()
        conn.close()

    # ==================== BOT ACTION QUEUE ====================

    def enqueue_bot_action(self, action: str, params: Optional[Dict[str, Any]] = None,
                           requested_by: Optional[str] = None) -> int:
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute(
            'INSERT INTO pending_bot_actions (action, params, requested_by) '
            'VALUES (%s, %s::jsonb, %s) RETURNING id',
            (action, json.dumps(params or {}), requested_by),
        )
        row = c.fetchone()
        conn.commit()
        conn.close()
        return int(row['id'])

    def list_bot_actions(self, limit: int = 20) -> List[Dict[str, Any]]:
        self._require_postgres()
        conn = self._connect()
        c = conn.cursor()
        c.execute(
            'SELECT * FROM pending_bot_actions ORDER BY requested_at DESC LIMIT %s',
            (int(limit),),
        )
        rows = c.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def claim_next_bot_action(self) -> Optional[Dict[str, Any]]:
        """Atomically claim the oldest pending action, or None if the queue is
        empty. Uses SELECT ... FOR UPDATE SKIP LOCKED so multiple workers are
        safe."""
        if not USE_POSTGRES:
            return None
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute('''
                UPDATE pending_bot_actions
                   SET status = 'running', claimed_at = now()
                 WHERE id = (
                   SELECT id FROM pending_bot_actions
                    WHERE status = 'pending'
                 ORDER BY requested_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                 )
             RETURNING *
            ''')
            row = c.fetchone()
            conn.commit()
            return dict(row) if row else None
        finally:
            conn.close()

    def complete_bot_action(self, action_id: int, *, result: Optional[Dict[str, Any]] = None,
                            error: Optional[str] = None):
        if not USE_POSTGRES:
            return
        conn = self._connect()
        try:
            c = conn.cursor()
            status = 'error' if error else 'done'
            c.execute(
                'UPDATE pending_bot_actions SET status = %s, completed_at = now(), '
                'result = %s::jsonb, error = %s WHERE id = %s',
                (status, json.dumps(result) if result else None, error, int(action_id)),
            )
            conn.commit()
        finally:
            conn.close()

    def cleanup_old_bot_actions(self, days: int = 7):
        if not USE_POSTGRES:
            return
        conn = self._connect()
        c = conn.cursor()
        c.execute("DELETE FROM pending_bot_actions WHERE requested_at < NOW() - INTERVAL '1 day' * %s",
                  (days,))
        conn.commit()
        conn.close()

    # ==================== SEEN TRACKING METHODS ====================
    
    def is_alert_seen(self, alert_type: str, alert_id: str) -> bool:
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'SELECT 1 FROM seen_alerts WHERE alert_type = {p} AND alert_id = {p}', (alert_type, alert_id))
        result = c.fetchone() is not None
        conn.close()
        return result
    
    def mark_alert_seen(self, alert_type: str, alert_id: str):
        conn = self._connect()
        c = conn.cursor()
        now = datetime.now().isoformat()
        if USE_POSTGRES:
            c.execute('INSERT INTO seen_alerts (alert_type, alert_id, first_seen) VALUES (%s, %s, %s) ON CONFLICT (alert_type, alert_id) DO NOTHING', (alert_type, alert_id, now))
        else:
            c.execute('INSERT OR IGNORE INTO seen_alerts (alert_type, alert_id, first_seen) VALUES (?, ?, ?)', (alert_type, alert_id, now))
        conn.commit()
        conn.close()
    
    def filter_unseen_alerts(self, alerts: List[tuple]) -> List[tuple]:
        """Given a list of (alert_type, alert_id) tuples, return only those not yet seen.
        Uses a single DB query instead of one per alert."""
        if not alerts:
            return []
        conn = self._connect()
        c = conn.cursor()
        if USE_POSTGRES:
            # Build a VALUES list for a single query
            values_str = ','.join(c.mogrify('(%s,%s)', (at, ai)).decode() for at, ai in alerts)
            c.execute(f'SELECT alert_type, alert_id FROM seen_alerts WHERE (alert_type, alert_id) IN ({values_str})')
        else:
            placeholders = ','.join(['(?,?)'] * len(alerts))
            flat = [v for pair in alerts for v in pair]
            c.execute(f'SELECT alert_type, alert_id FROM seen_alerts WHERE (alert_type, alert_id) IN ({placeholders})', flat)
        seen = {(row[0] if isinstance(row, tuple) else row['alert_type'],
                 row[1] if isinstance(row, tuple) else row['alert_id']) for row in c.fetchall()}
        conn.close()
        return [(at, ai) for at, ai in alerts if (at, ai) not in seen]

    def mark_alerts_seen_batch(self, alerts: List[tuple]):
        if not alerts:
            return
        conn = self._connect()
        c = conn.cursor()
        now = datetime.now().isoformat()
        data = [(alert_type, alert_id, now) for alert_type, alert_id in alerts]
        if USE_POSTGRES:
            c.executemany('INSERT INTO seen_alerts (alert_type, alert_id, first_seen) VALUES (%s, %s, %s) ON CONFLICT (alert_type, alert_id) DO NOTHING', data)
        else:
            c.executemany('INSERT OR IGNORE INTO seen_alerts (alert_type, alert_id, first_seen) VALUES (?, ?, ?)', data)
        conn.commit()
        conn.close()
        logger.debug(f"Batch marked {len(alerts)} alerts as seen")
    
    def is_pager_seen(self, message_hash: str) -> bool:
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'SELECT 1 FROM seen_pager WHERE message_hash = {p}', (message_hash,))
        result = c.fetchone() is not None
        conn.close()
        return result
    
    def mark_pager_seen(self, message_hash: str):
        conn = self._connect()
        c = conn.cursor()
        now = datetime.now().isoformat()
        if USE_POSTGRES:
            c.execute('INSERT INTO seen_pager (message_hash, first_seen) VALUES (%s, %s) ON CONFLICT (message_hash) DO NOTHING', (message_hash, now))
        else:
            c.execute('INSERT OR IGNORE INTO seen_pager (message_hash, first_seen) VALUES (?, ?)', (message_hash, now))
        conn.commit()
        conn.close()
    
    def mark_pager_seen_batch(self, message_hashes: List[str]):
        if not message_hashes:
            return
        conn = self._connect()
        c = conn.cursor()
        now = datetime.now().isoformat()
        data = [(h, now) for h in message_hashes]
        if USE_POSTGRES:
            c.executemany('INSERT INTO seen_pager (message_hash, first_seen) VALUES (%s, %s) ON CONFLICT (message_hash) DO NOTHING', data)
        else:
            c.executemany('INSERT OR IGNORE INTO seen_pager (message_hash, first_seen) VALUES (?, ?)', data)
        conn.commit()
        conn.close()
        logger.debug(f"Batch marked {len(message_hashes)} pager messages as seen")
    
    def cleanup_old_seen(self, days: int = 7):
        conn = self._connect()
        c = conn.cursor()
        cutoff = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_str = cutoff.isoformat()
        
        if USE_POSTGRES:
            c.execute("DELETE FROM seen_alerts WHERE (first_seen::timestamp) < NOW() - INTERVAL '1 day' * %s", (days,))
            c.execute("DELETE FROM seen_pager WHERE (first_seen::timestamp) < NOW() - INTERVAL '1 day' * %s", (days,))
        else:
            c.execute("DELETE FROM seen_alerts WHERE date(first_seen) < date(?, '-' || ? || ' days')", (cutoff_str, days))
            c.execute("DELETE FROM seen_pager WHERE date(first_seen) < date(?, '-' || ? || ' days')", (cutoff_str, days))
        
        conn.commit()
        conn.close()
        logger.info(f"Cleaned up seen records older than {days} days")
    
    # ==================== INCIDENT MESSAGE TRACKING ====================
    
    def save_incident_message(self, incident_guid: str, channel_id: int, message_url: str, status: str = None):
        conn = self._connect()
        c = conn.cursor()
        now = datetime.now().isoformat()
        if USE_POSTGRES:
            c.execute('''
                INSERT INTO incident_messages (incident_guid, channel_id, message_url, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (incident_guid, channel_id, status) DO UPDATE SET message_url = EXCLUDED.message_url, created_at = EXCLUDED.created_at
            ''', (incident_guid, channel_id, message_url, status, now))
        else:
            c.execute('''
                INSERT OR REPLACE INTO incident_messages (incident_guid, channel_id, message_url, status, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (incident_guid, channel_id, message_url, status, now))
        conn.commit()
        conn.close()
    
    def get_previous_incident_message(self, incident_guid: str, channel_id: int) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'''SELECT * FROM incident_messages WHERE incident_guid = {p} AND channel_id = {p} ORDER BY created_at DESC LIMIT 1''', (incident_guid, channel_id))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def get_first_incident_message(self, incident_guid: str, channel_id: int) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'''SELECT * FROM incident_messages WHERE incident_guid = {p} AND channel_id = {p} ORDER BY created_at ASC LIMIT 1''', (incident_guid, channel_id))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def get_incident_message_count(self, incident_guid: str, channel_id: int) -> int:
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'SELECT COUNT(*) AS n FROM incident_messages WHERE incident_guid = {p} AND channel_id = {p}', (incident_guid, channel_id))
        row = c.fetchone()
        count = row['n'] if USE_POSTGRES else row[0]
        conn.close()
        return count
    
    def cleanup_old_incident_messages(self, days: int = 14):
        conn = self._connect()
        c = conn.cursor()
        if USE_POSTGRES:
            c.execute("DELETE FROM incident_messages WHERE (created_at::timestamp) < NOW() - INTERVAL '1 day' * %s", (days,))
        else:
            cutoff = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            cutoff_str = cutoff.isoformat()
            c.execute("DELETE FROM incident_messages WHERE date(created_at) < date(?, '-' || ? || ' days')", (cutoff_str, days))
        conn.commit()
        conn.close()
