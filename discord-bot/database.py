"""
Database module for storing alert and pager configurations.
Uses PostgreSQL when BOT_DATABASE_URL is set, otherwise SQLite.
"""

import os
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

logger = logging.getLogger('nswpsn-bot.database')

# PostgreSQL connection URL (takes precedence over SQLite)
BOT_DATABASE_URL = os.getenv('BOT_DATABASE_URL')
# SQLite fallback - path to local file
DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), 'bot_config.db')
DB_PATH = os.getenv('BOT_DB_PATH', DEFAULT_DB_PATH)

USE_POSTGRES = bool(BOT_DATABASE_URL)
if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor
else:
    import sqlite3


class Database:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
    
    def _connect(self):
        """Create a database connection"""
        if USE_POSTGRES:
            conn = psycopg2.connect(BOT_DATABASE_URL, cursor_factory=RealDictCursor)
            return conn
        else:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn
    
    def init_db(self):
        """Initialize the database tables"""
        conn = self._connect()
        c = conn.cursor()
        
        if USE_POSTGRES:
            # PostgreSQL DDL
            c.execute('''
                CREATE TABLE IF NOT EXISTS alert_configs (
                    id SERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    channel_id BIGINT NOT NULL,
                    alert_type TEXT NOT NULL,
                    role_id BIGINT,
                    created_at TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS')),
                    UNIQUE(guild_id, channel_id, alert_type)
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS pager_configs (
                    id SERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    channel_id BIGINT NOT NULL,
                    capcodes TEXT,
                    role_id BIGINT,
                    created_at TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS')),
                    UNIQUE(guild_id, channel_id)
                )
            ''')
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
        else:
            # SQLite DDL
            c.execute('''
                CREATE TABLE IF NOT EXISTS alert_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    alert_type TEXT NOT NULL,
                    role_id INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(guild_id, channel_id, alert_type)
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS pager_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    capcodes TEXT,
                    role_id INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(guild_id, channel_id)
                )
            ''')
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
        
        c.execute('CREATE INDEX IF NOT EXISTS idx_alert_configs_guild ON alert_configs(guild_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_alert_configs_type ON alert_configs(alert_type)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_pager_configs_guild ON pager_configs(guild_id)')
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
    
    # ==================== ALERT CONFIG METHODS ====================
    
    def add_config(self, guild_id: int, channel_id: int, alert_type: str, role_id: int = None) -> int:
        """Add a new alert configuration"""
        conn = self._connect()
        c = conn.cursor()
        now = datetime.now().isoformat()
        
        if USE_POSTGRES:
            c.execute('''
                INSERT INTO alert_configs (guild_id, channel_id, alert_type, role_id, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (guild_id, channel_id, alert_type) DO UPDATE SET role_id = EXCLUDED.role_id, created_at = EXCLUDED.created_at
                RETURNING id
            ''', (guild_id, channel_id, alert_type, role_id, now))
            row = c.fetchone()
            config_id = row['id'] if row else None
        else:
            c.execute('''
                INSERT OR REPLACE INTO alert_configs (guild_id, channel_id, alert_type, role_id, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (guild_id, channel_id, alert_type, role_id, now))
            config_id = c.lastrowid

        conn.commit()
        conn.close()
        logger.info(f"Added alert config: guild={guild_id}, channel={channel_id}, type={alert_type}")
        return config_id
    
    def get_config(self, guild_id: int, channel_id: int, alert_type: str) -> Optional[Dict[str, Any]]:
        """Get a specific alert configuration"""
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'''SELECT * FROM alert_configs WHERE guild_id = {p} AND channel_id = {p} AND alert_type = {p}''',
                  (guild_id, channel_id, alert_type))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def update_config(self, config_id: int, role_id: int = None):
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'UPDATE alert_configs SET role_id = {p} WHERE id = {p}', (role_id, config_id))
        conn.commit()
        conn.close()
    
    def remove_config(self, config_id: int):
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'DELETE FROM alert_configs WHERE id = {p}', (config_id,))
        conn.commit()
        conn.close()
        logger.info(f"Removed alert config: id={config_id}")
    
    def get_guild_configs(self, guild_id: int) -> List[Dict[str, Any]]:
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'SELECT * FROM alert_configs WHERE guild_id = {p}', (guild_id,))
        rows = c.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def get_all_alert_configs(self) -> List[Dict[str, Any]]:
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT * FROM alert_configs')
        rows = c.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def get_configs_for_alert_type(self, alert_type: str) -> List[Dict[str, Any]]:
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'SELECT * FROM alert_configs WHERE alert_type = {p}', (alert_type,))
        rows = c.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def count_configs(self) -> int:
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM alert_configs')
        count = c.fetchone()[0]
        conn.close()
        return count
    
    # ==================== PAGER CONFIG METHODS ====================
    
    def add_pager_config(self, guild_id: int, channel_id: int, capcodes: str = None, role_id: int = None) -> int:
        conn = self._connect()
        c = conn.cursor()
        now = datetime.now().isoformat()
        
        if USE_POSTGRES:
            c.execute('''
                INSERT INTO pager_configs (guild_id, channel_id, capcodes, role_id, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (guild_id, channel_id) DO UPDATE SET capcodes = EXCLUDED.capcodes, role_id = EXCLUDED.role_id, created_at = EXCLUDED.created_at
                RETURNING id
            ''', (guild_id, channel_id, capcodes, role_id, now))
            row = c.fetchone()
            config_id = row['id'] if row else None
        else:
            c.execute('''
                INSERT OR REPLACE INTO pager_configs (guild_id, channel_id, capcodes, role_id, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (guild_id, channel_id, capcodes, role_id, now))
            config_id = c.lastrowid
        
        conn.commit()
        conn.close()
        logger.info(f"Added pager config: guild={guild_id}, channel={channel_id}, capcodes={capcodes}")
        return config_id
    
    def get_pager_config(self, guild_id: int, channel_id: int) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'SELECT * FROM pager_configs WHERE guild_id = {p} AND channel_id = {p}', (guild_id, channel_id))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def update_pager_config(self, config_id: int, capcodes: str = None, role_id: int = None):
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'UPDATE pager_configs SET capcodes = {p}, role_id = {p} WHERE id = {p}', (capcodes, role_id, config_id))
        conn.commit()
        conn.close()
    
    def remove_pager_config(self, config_id: int):
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'DELETE FROM pager_configs WHERE id = {p}', (config_id,))
        conn.commit()
        conn.close()
        logger.info(f"Removed pager config: id={config_id}")
    
    def get_pager_configs(self) -> List[Dict[str, Any]]:
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT * FROM pager_configs')
        rows = c.fetchall()
        conn.close()
        configs = []
        for row in rows:
            config = dict(row)
            if config.get('capcodes'):
                config['capcodes'] = config['capcodes'].split(',')
            configs.append(config)
        return configs
    
    def get_guild_pager_configs(self, guild_id: int) -> List[Dict[str, Any]]:
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'SELECT * FROM pager_configs WHERE guild_id = {p}', (guild_id,))
        rows = c.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def count_pager_configs(self) -> int:
        conn = self._connect()
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM pager_configs')
        count = c.fetchone()[0]
        conn.close()
        return count
    
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
        c.execute(f'SELECT COUNT(*) FROM incident_messages WHERE incident_guid = {p} AND channel_id = {p}', (incident_guid, channel_id))
        count = c.fetchone()[0]
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
    
    # ==================== GUILD CLEANUP METHODS ====================
    
    def remove_guild_data(self, guild_id: int) -> Dict[str, int]:
        conn = self._connect()
        c = conn.cursor()
        p = self._param(1)
        c.execute(f'SELECT COUNT(*) FROM alert_configs WHERE guild_id = {p}', (guild_id,))
        alert_count = c.fetchone()[0]
        c.execute(f'DELETE FROM alert_configs WHERE guild_id = {p}', (guild_id,))
        c.execute(f'SELECT COUNT(*) FROM pager_configs WHERE guild_id = {p}', (guild_id,))
        pager_count = c.fetchone()[0]
        c.execute(f'DELETE FROM pager_configs WHERE guild_id = {p}', (guild_id,))
        conn.commit()
        conn.close()
        logger.info(f"Removed guild data for guild={guild_id}: {alert_count} alert configs, {pager_count} pager configs")
        return {'alerts': alert_count, 'pager': pager_count}
