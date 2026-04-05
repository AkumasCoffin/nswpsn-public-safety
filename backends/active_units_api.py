#!/usr/bin/env python3
"""Active Units, Talkgroups & Visitor Tracking API.

Reads SQLite DB defined in RDIO_SCANNER_DB environment variable.
Implements in-memory caching, visitor tracking, and clean logging.
"""

import os
import sqlite3
import logging
import time
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix

# Configuration
DB_PATH = os.environ.get("RDIO_SCANNER_DB", "/home/lynx/Desktop/rdio-scanner.db")
MAX_AGE = timedelta(hours=1)
CACHE_TIMEOUT = 30
VISITOR_TIMEOUT = 35

# Logging setup - suppress Flask/Werkzeug default logs
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Trust headers from reverse proxy (Cloudflare/Nginx)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

# In-memory storage
_cache = {}
_visitors = {}

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_cached_data(key):
    entry = _cache.get(key)
    if entry:
        timestamp, data = entry
        if time.time() - timestamp < CACHE_TIMEOUT:
            return data
    return None

def set_cached_data(key, data):
    _cache[key] = (time.time(), data)


def track_visitor():
    """Track visitor IP and clean up stale entries."""
    ip = request.headers.get('CF-Connecting-IP')
    if not ip:
        xff = request.headers.get('X-Forwarded-For', '')
        ip = (xff.split(',')[0].strip() if xff else None) or request.remote_addr
    
    now = time.time()
    _visitors[ip] = now

    # Remove visitors who haven't been seen in 5 mins
    expired_ips = [user_ip for user_ip, last_seen in _visitors.items() if now - last_seen > VISITOR_TIMEOUT]
    for user_ip in expired_ips:
        del _visitors[user_ip]

    return ip, len(_visitors)

def get_system_name_map(conn):
    """Get mapping of system IDs to names from database."""
    try:
        cur = conn.execute("PRAGMA table_info(rdioScannerSystems)")
        cols = [row[1] for row in cur.fetchall()]
        name_col = next((c for c in ("name", "label", "shortName", "short_name") if c in cols), None)
        if not name_col:
            return {}
        sql = f"SELECT id, {name_col} AS name FROM rdioScannerSystems"
        cur = conn.execute(sql)
        return {row["id"]: row["name"] for row in cur.fetchall()}
    except Exception:
        return {}


# Routes

@app.route("/api/active-units", methods=["GET"])
def active_units():
    """Get active units from the last hour."""
    current_ip, active_count = track_visitor()
    active_ips_list = list(_visitors.keys())

    cached = get_cached_data("active_units")
    if cached:
        return jsonify(cached)

    logging.info(f"⚡ DB Query | Request by: {current_ip} | Total Active: {active_count} | IPs: {active_ips_list}")

    now = datetime.utcnow()
    cutoff = now - MAX_AGE
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    conn = None
    try:
        conn = get_db()
        system_names = get_system_name_map(conn)

        sql = '''
            SELECT
              MIN(c.source)      AS unit_id,
              MAX(c.dateTime)    AS last_seen_dt,
              MIN(c.system)      AS system_id,
              u.label            AS unit_label,
              COUNT(*)           AS seen_count
            FROM rdioScannerCalls c
            JOIN rdioScannerUnits u
              ON u.systemId = c.system
             AND u.id       = c.source
            WHERE c.dateTime >= ?
              AND u.label IS NOT NULL
              AND TRIM(u.label) != ''
            GROUP BY u.label
            ORDER BY last_seen_dt DESC
            LIMIT 200;
        '''
        cur = conn.execute(sql, (cutoff_str,))
        rows = cur.fetchall()

        results = []
        for row in rows:
            unit_id = row["unit_id"]
            raw_last_seen = row["last_seen_dt"]
            system_id = row["system_id"]
            system_name = system_names.get(system_id, f"System {system_id}")
            
            last_seen_iso = None
            if raw_last_seen:
                try:
                    dt = datetime.fromisoformat(raw_last_seen)
                    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                    last_seen_iso = dt.isoformat()
                except: last_seen_iso = str(raw_last_seen)

            results.append({
                "unit_id": unit_id,
                "label": row["unit_label"],
                "system_id": system_id,
                "system_name": system_name,
                "seen_count": row["seen_count"],
                "last_seen_iso": last_seen_iso,
                "agency": system_name,
            })

        set_cached_data("active_units", results)
        return jsonify(results)

    except Exception as e:
        logging.error(f"Error in active_units: {e}", exc_info=True)
        return jsonify({"error": "internal_error"}), 500
    finally:
        if conn: conn.close()


@app.route("/api/active-talkgroups", methods=["GET"])
def active_talkgroups():
    cached = get_cached_data("active_talkgroups")
    if cached:
        return jsonify(cached)

    now = datetime.utcnow()
    cutoff = now - MAX_AGE
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    conn = None
    try:
        conn = get_db()
        system_names = get_system_name_map(conn)

        sql = '''
            SELECT
              MIN(c.talkgroup)   AS talkgroup_id,
              MAX(c.dateTime)    AS last_seen_dt,
              MIN(c.system)      AS system_id,
              t.label            AS talkgroup_label,
              COUNT(*)           AS seen_count
            FROM rdioScannerCalls c
            JOIN rdioScannerTalkgroups t
              ON t.systemId = c.system
             AND t.id       = c.talkgroup
            WHERE c.dateTime >= ?
              AND t.label IS NOT NULL
              AND TRIM(t.label) != ''
            GROUP BY t.label
            ORDER BY last_seen_dt DESC
            LIMIT 200;
        '''
        cur = conn.execute(sql, (cutoff_str,))
        rows = cur.fetchall()

        results = []
        for row in rows:
            talkgroup_id = row["talkgroup_id"]
            raw_last_seen = row["last_seen_dt"]
            system_id = row["system_id"]
            system_name = system_names.get(system_id, f"System {system_id}")

            last_seen_iso = None
            if raw_last_seen:
                try:
                    dt = datetime.fromisoformat(raw_last_seen)
                    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                    last_seen_iso = dt.isoformat()
                except: last_seen_iso = str(raw_last_seen)

            results.append({
                "talkgroup_id": talkgroup_id,
                "label": row["talkgroup_label"],
                "system_id": system_id,
                "system_name": system_name,
                "seen_count": row["seen_count"],
                "last_seen_iso": last_seen_iso,
                "agency": system_name,
            })

        set_cached_data("active_talkgroups", results)
        return jsonify(results)

    except Exception as e:
        logging.error(f"Error in active_talkgroups: {e}", exc_info=True)
        return jsonify({"error": "internal_error"}), 500
    finally:
        if conn: conn.close()

@app.route("/api/status", methods=["GET"])
def server_status():
    """Optional endpoint to check visitor count."""
    return jsonify({
        "active_visitors_5min": len(_visitors),
        "active_ips": list(_visitors.keys()),
        "cache_active": len(_cache) > 0
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5001"))
    logging.info(f"--- Starting NSW PSN API on port {port} ---")
    app.run(host="0.0.0.0", port=port)
    