#!/usr/bin/env python3
"""
Proxy server for external APIs that don't support CORS.
Fetches data from government APIs and transforms to GeoJSON format.
Includes data archival system for historical data storage.

Usage:
    python external_api_proxy.py          # Production mode (clean logs)
    python external_api_proxy.py --dev    # Dev mode (verbose logging)
    
PM2:
    pm2 start external_api_proxy.py --name API-Proxy --interpreter python3
    pm2 start external_api_proxy.py --name API-Proxy --interpreter python3 -- --dev
    
Environment:
    DEV_MODE=true to enable dev mode
"""

import re
import sys
import time
import json
import base64
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import threading
import signal
import os
import logging
import argparse
import hashlib
from math import cos, radians
from functools import wraps
from datetime import datetime, timedelta, timezone

# Load environment variables from .env file FIRST (from script dir so PM2/cwd-independent)
from dotenv import load_dotenv
_script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_script_dir, '.env'), override=True)

from db import get_conn, get_conn_dict, pool_stats as _db_pool_stats

# Parse command line arguments
parser = argparse.ArgumentParser(description='NSW PSN API Proxy Server')
parser.add_argument('--dev', action='store_true', help='Enable dev mode with verbose logging')
parser.add_argument('--port', type=int, default=8000, help='Port to run on (default: 8000)')
parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
args, _ = parser.parse_known_args()

# Check for dev mode from args OR environment
DEV_MODE = args.dev or os.environ.get('DEV_MODE', '').lower() in ('true', '1', 'yes')

import requests
import cloudscraper
from flask import Flask, jsonify, request
from flask_cors import CORS
try:
    from flask_compress import Compress
    _COMPRESS_AVAILABLE = True
except ImportError:
    _COMPRESS_AVAILABLE = False
    Compress = None

# Try to import curl_cffi for better Cloudflare bypass
try:
    from curl_cffi import requests as curl_requests
    CURL_CFFI_AVAILABLE = True
except ImportError:
    CURL_CFFI_AVAILABLE = False
    curl_requests = None

app = Flask(__name__)

# Response compression. Heavy JSON endpoints (essential/outages with
# polygon boundaries, traffic/incidents, roadwork) compress 5-10x with
# gzip; without this they ship hundreds of KB of repetitive coordinate
# data uncompressed. flask-compress is opt-in via Accept-Encoding so
# clients that can't decompress (rare) still get plain JSON.
# Optional dependency — if it's not installed we just skip compression
# and warn at startup.
if _COMPRESS_AVAILABLE:
    app.config['COMPRESS_MIN_SIZE'] = 1024  # don't bother for tiny responses
    app.config['COMPRESS_LEVEL'] = 6        # default; good size/CPU trade-off
    app.config['COMPRESS_MIMETYPES'] = [
        'application/json',
        'text/html',
        'text/css',
        'text/javascript',
        'application/javascript',
    ]
    Compress(app)

# Configure logging based on mode - MUST happen before any requests
if not DEV_MODE:
    # Production: suppress Flask/Werkzeug default request logs completely
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    logging.getLogger('werkzeug').disabled = True
    app.logger.setLevel(logging.ERROR)
    
    # Also disable Flask's built-in server log
    import click
    def secho(text, *args, **kwargs):
        pass
    def echo(text, *args, **kwargs):
        pass
    click.echo = echo
    click.secho = secho

# Configure CORS to allow all origins for public endpoints, but enable
# credentialed cross-origin for the dashboard (which lives on nswpsn.*
# while the API lives on api.*). `Access-Control-Allow-Origin: *` and
# credentialed cookies are incompatible per spec, so dashboard paths need
# an explicit origin list and `supports_credentials=True`.
CORS(app, resources={
    r"/api/dashboard/*": {
        "origins": [
            "https://nswpsn.forcequit.xyz",
            "https://www.nswpsn.forcequit.xyz",
            "http://localhost:8080",
            "http://127.0.0.1:8080",
        ],
        "methods": ["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Accept", "Authorization"],
        "supports_credentials": True,
        # Propagate our Set-Cookie on redirects / AJAX
        "expose_headers": ["Content-Type"],
    },
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Accept", "Authorization"],
        "supports_credentials": False,
    },
})


# ============== LOGGING SYSTEM ==============
# Centralized logging with categories and consistent formatting

class Log:
    """
    Centralized logging utility with categories.
    Thread-safe logging with a lock to prevent interleaved output.
    
    Usage:
        Log.info("Message")           # General info (always shown)
        Log.cache("Cache hit")        # Cache operations (dev only)
        Log.data("Stored 5 records")  # Data/history operations (dev only)  
        Log.prewarm("RFS refreshed")  # Prewarm operations (dev only)
        Log.api("Fetched from X")     # External API calls (dev only)
        Log.error("Something failed") # Errors (always shown)
        Log.startup("Server ready")   # Startup messages (always shown)
    """
    
    # Thread lock to prevent interleaved log output
    _lock = threading.Lock()
    
    # Category icons and whether they show in production
    CATEGORIES = {
        'info':    ('ℹ️ ', True),    # Always show
        'startup': ('🚀', True),    # Always show
        'error':   ('❌', True),    # Always show
        'warn':    ('⚠️ ', True),    # Always show
        'cache':   ('💾', False),   # Dev only
        'data':    ('📊', True),    # Always show - archive start/end with counts
        'prewarm': ('🔥', True),    # Always show - fetch start/end with counts
        'api':     ('🌐', False),   # Dev only
        'cleanup': ('🧹', False),   # Dev only - but cleanup summaries show in prod
        'live':    ('📡', False),   # Dev only - live tracking
    }
    
    @staticmethod
    def _log(category: str, message: str, force: bool = False):
        """Internal log method - thread-safe"""
        icon, show_in_prod = Log.CATEGORIES.get(category, ('', True))
        
        # Skip dev-only logs in production (unless forced)
        if not DEV_MODE and not show_in_prod and not force:
            return
        
        timestamp = datetime.now().strftime('%H:%M:%S')
        
        # Thread-safe printing
        with Log._lock:
            # Cleaner format for production
            if not DEV_MODE:
                print(f"[{timestamp}] {icon} {message}", flush=True)
            else:
                # More detailed format for dev
                print(f"[{timestamp}] {icon} [{category.upper()}] {message}", flush=True)
    
    @staticmethod
    def info(msg: str):
        Log._log('info', msg)
    
    @staticmethod
    def startup(msg: str):
        Log._log('startup', msg)
    
    @staticmethod
    def error(msg: str):
        Log._log('error', msg)
    
    @staticmethod
    def warn(msg: str):
        Log._log('warn', msg)
    
    @staticmethod
    def cache(msg: str):
        Log._log('cache', msg)
    
    @staticmethod
    def data(msg: str):
        Log._log('data', msg)
    
    @staticmethod
    def prewarm(msg: str):
        Log._log('prewarm', msg)
    
    @staticmethod
    def api(msg: str):
        Log._log('api', msg)
    
    @staticmethod
    def cleanup(msg: str, force: bool = False):
        """Cleanup logs - force=True shows summary in production"""
        Log._log('cleanup', msg, force=force)
    
    @staticmethod
    def live(msg: str):
        Log._log('live', msg)
    
    @staticmethod
    def viewer(msg: str):
        """Viewer join/leave logs - always shown"""
        Log._log('info', msg)

# ============== RATE LIMITING ==============
# Prevents abuse from spam reloading - still serves cached data to limited users

RATE_LIMIT_REQUESTS = 100  # Max requests per window
RATE_LIMIT_WINDOW = 60     # Window in seconds (1 minute)
RATE_LIMIT_BURST = 30      # Allow burst of requests on page load

# Track requests per IP: {ip: {'count': int, 'window_start': timestamp, 'burst_used': int}}
_rate_limit_data = {}
_rate_limit_lock = threading.Lock()

def _get_client_ip():
    """Get client IP from headers or request"""
    return request.headers.get('X-Forwarded-For', request.remote_addr)

def _check_rate_limit(ip):
    """
    Check if IP is rate limited.
    Returns: (is_limited, requests_remaining)
    """
    now = time.time()
    
    with _rate_limit_lock:
        if ip not in _rate_limit_data:
            _rate_limit_data[ip] = {
                'count': 0,
                'window_start': now,
                'burst_used': 0
            }
        
        data = _rate_limit_data[ip]
        
        # Reset window if expired
        if now - data['window_start'] > RATE_LIMIT_WINDOW:
            data['count'] = 0
            data['window_start'] = now
            data['burst_used'] = 0
        
        # Allow burst for page load (first N requests are free)
        if data['burst_used'] < RATE_LIMIT_BURST:
            data['burst_used'] += 1
            data['count'] += 1
            return False, RATE_LIMIT_REQUESTS - data['count']
        
        # Check if over limit
        if data['count'] >= RATE_LIMIT_REQUESTS:
            return True, 0
        
        data['count'] += 1
        return False, RATE_LIMIT_REQUESTS - data['count']

def _cleanup_rate_limits():
    """Remove stale rate limit entries (called periodically)"""
    now = time.time()
    stale_threshold = RATE_LIMIT_WINDOW * 2

    with _rate_limit_lock:
        stale_ips = [ip for ip, data in _rate_limit_data.items()
                     if now - data['window_start'] > stale_threshold]
        for ip in stale_ips:
            del _rate_limit_data[ip]


# ============== UPSTREAM SOURCE-HEALTH REGISTRY ==============
# In-memory only; survives via pm2 lifetime. After restart, sources show
# 'unknown' until the next successful poll. Powers the admin dashboard's
# "data sources" panel via /api/dashboard/admin/sources.
_SOURCE_HEALTH = {}   # name -> {last_success, last_error, last_error_msg, consec_fails, total_success, total_fail}
_SOURCE_HEALTH_LOCK = threading.Lock()

_SOURCE_THRESHOLDS = {
    # Thresholds reflect each source's expected SUCCESSFUL POLL cadence —
    # 'soft' = degraded (a warning), 'hard' = down (genuine concern).
    # Sources whose upstream is slow/rate-limited or can have legitimate
    # quiet periods get more headroom.
    'rfs':               {'soft': 600,  'hard': 1800, 'label': 'RFS incidents'},
    'bom':               {'soft': 600,  'hard': 1800, 'label': 'BOM warnings'},
    'traffic_incidents': {'soft': 600,  'hard': 1800, 'label': 'LiveTraffic — incidents'},
    'traffic_roadwork':  {'soft': 1800, 'hard': 3600, 'label': 'LiveTraffic — roadwork'},
    'traffic_flood':     {'soft': 1800, 'hard': 3600, 'label': 'LiveTraffic — flood'},
    'traffic_fire':      {'soft': 1800, 'hard': 3600, 'label': 'LiveTraffic — fire'},
    'traffic_major':     {'soft': 1800, 'hard': 3600, 'label': 'LiveTraffic — major events'},
    'power_endeavour':   {'soft': 1200, 'hard': 3600, 'label': 'Endeavour outages'},
    # Ausgrid KML is rate-limited and only refreshes when outages change.
    # Loose thresholds avoid flagging it Down during quiet weather.
    'power_ausgrid':     {'soft': 1800, 'hard': 5400, 'label': 'Ausgrid outages'},
    'waze':              {'soft': 600,  'hard': 1800, 'label': 'Waze'},
    # Pager hits are bursty — a quiet night can have 30+ minutes between
    # any pages. Polling itself happens every ~2 min, but a poll only
    # registers source_ok when the upstream HTTP responds; if Pagermon's
    # backend rate-limits or transiently fails, gaps are normal.
    'pager':             {'soft': 1200, 'hard': 3600, 'label': 'Pagermon'},
    # rdio summary scheduler runs once per hour. Soft threshold sits just
    # past the cycle (65 min) so a healthy hourly cadence doesn't read
    # "degraded"; hard threshold flags 3+ missed cycles (3.5h).
    'rdio':              {'soft': 3900, 'hard': 12600, 'label': 'rdio-scanner'},
}


_SOURCE_HEALTH_DIRTY = set()  # names whose row needs flushing to Postgres
_SOURCE_HEALTH_LOADED = False


def _source_health_load_from_db():
    """Restore _SOURCE_HEALTH from Postgres at startup. Best-effort —
    silently no-ops if BOT_DATA_DATABASE_URL isn't configured."""
    global _SOURCE_HEALTH_LOADED
    if _SOURCE_HEALTH_LOADED:
        return
    conn = _bot_db_conn() if 'bot_db_conn' in globals() or '_bot_db_conn' in globals() else None
    # _bot_db_conn is defined later in the file — guard against import-time
    # ordering by lazy resolution at the call site.
    try:
        conn = _bot_db_conn()
    except Exception:
        conn = None
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS source_health ('
                    'name TEXT PRIMARY KEY, last_success BIGINT, last_error BIGINT, '
                    'last_error_msg TEXT, consec_fails INTEGER NOT NULL DEFAULT 0, '
                    'total_success BIGINT NOT NULL DEFAULT 0, '
                    'total_fail BIGINT NOT NULL DEFAULT 0, '
                    'updated_at TIMESTAMPTZ NOT NULL DEFAULT now())')
        cur.execute('SELECT name, last_success, last_error, last_error_msg, '
                    'consec_fails, total_success, total_fail FROM source_health')
        with _SOURCE_HEALTH_LOCK:
            for r in cur.fetchall():
                _SOURCE_HEALTH[r['name']] = {
                    'last_success': r['last_success'],
                    'last_error': r['last_error'],
                    'last_error_msg': r['last_error_msg'],
                    'consec_fails': int(r['consec_fails'] or 0),
                    'total_success': int(r['total_success'] or 0),
                    'total_fail': int(r['total_fail'] or 0),
                }
        conn.commit()
        _SOURCE_HEALTH_LOADED = True
        Log.startup(f"source_health: restored {len(_SOURCE_HEALTH)} source(s) from DB")
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        Log.warn(f"source_health load failed: {e}")
    finally:
        conn.close()


def _source_health_flush():
    """Push every dirty source row to Postgres. Called by a 60s background
    thread so the hot path (source_ok/source_error) stays in-memory."""
    with _SOURCE_HEALTH_LOCK:
        if not _SOURCE_HEALTH_DIRTY:
            return
        names = list(_SOURCE_HEALTH_DIRTY)
        rows = [(n, dict(_SOURCE_HEALTH.get(n) or {})) for n in names]
        _SOURCE_HEALTH_DIRTY.clear()
    try:
        conn = _bot_db_conn()
    except Exception:
        conn = None
    if conn is None:
        # Re-mark as dirty so we'll retry next tick — DB might come back.
        with _SOURCE_HEALTH_LOCK:
            _SOURCE_HEALTH_DIRTY.update(names)
        return
    try:
        cur = conn.cursor()
        for name, s in rows:
            cur.execute(
                'INSERT INTO source_health (name, last_success, last_error, '
                'last_error_msg, consec_fails, total_success, total_fail, updated_at) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, now()) '
                'ON CONFLICT (name) DO UPDATE SET '
                '  last_success = EXCLUDED.last_success, '
                '  last_error = EXCLUDED.last_error, '
                '  last_error_msg = EXCLUDED.last_error_msg, '
                '  consec_fails = EXCLUDED.consec_fails, '
                '  total_success = EXCLUDED.total_success, '
                '  total_fail = EXCLUDED.total_fail, '
                '  updated_at = now()',
                (name, s.get('last_success'), s.get('last_error'),
                 s.get('last_error_msg'),
                 int(s.get('consec_fails') or 0),
                 int(s.get('total_success') or 0),
                 int(s.get('total_fail') or 0)),
            )
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        with _SOURCE_HEALTH_LOCK:
            _SOURCE_HEALTH_DIRTY.update(names)
        Log.warn(f"source_health flush failed: {e}")
    finally:
        conn.close()


def _source_health_clear_all():
    """Wipe both the in-memory dict and the DB row, for the admin Clear button."""
    with _SOURCE_HEALTH_LOCK:
        _SOURCE_HEALTH.clear()
        _SOURCE_HEALTH_DIRTY.clear()
    try:
        conn = _bot_db_conn()
    except Exception:
        conn = None
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute('DELETE FROM source_health')
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        Log.warn(f"source_health clear failed: {e}")
    finally:
        conn.close()


def _source_health_flusher_loop():
    """Background thread: flush dirty rows every 60s + try the initial load."""
    # Lazy-load on first tick (avoids module-import ordering issues).
    while True:
        try:
            _source_health_load_from_db()
            _source_health_flush()
        except Exception as e:
            Log.warn(f"source_health flusher error: {e}")
        time.sleep(60)


def source_ok(name):
    """Record a successful upstream fetch. Hot-path-safe (no logging)."""
    now = int(time.time())
    with _SOURCE_HEALTH_LOCK:
        s = _SOURCE_HEALTH.setdefault(name, {
            'last_success': None, 'last_error': None, 'last_error_msg': None,
            'consec_fails': 0, 'total_success': 0, 'total_fail': 0,
        })
        s['last_success'] = now
        s['consec_fails'] = 0
        s['total_success'] = (s.get('total_success') or 0) + 1
        _SOURCE_HEALTH_DIRTY.add(name)


def source_error(name, msg, exc=None):
    """Record a failed upstream fetch. Hot-path-safe (no logging)."""
    now = int(time.time())
    with _SOURCE_HEALTH_LOCK:
        s = _SOURCE_HEALTH.setdefault(name, {
            'last_success': None, 'last_error': None, 'last_error_msg': None,
            'consec_fails': 0, 'total_success': 0, 'total_fail': 0,
        })
        s['last_error'] = now
        s['last_error_msg'] = str(msg)[:200]
        s['consec_fails'] = (s.get('consec_fails') or 0) + 1
        s['total_fail'] = (s.get('total_fail') or 0) + 1
        _SOURCE_HEALTH_DIRTY.add(name)


# Ensure CORS headers are always set (backup in case reverse proxy interferes)
#
# Dashboard endpoints need a SPECIFIC origin (not *) so the browser will send
# the cross-site session cookie. `Access-Control-Allow-Origin: *` and
# credentialed requests are mutually exclusive per the CORS spec — so we
# whitelist the dashboard's origin(s) and echo them back for matching requests.
_DASHBOARD_ALLOWED_ORIGINS = {
    'https://nswpsn.forcequit.xyz',
    'https://www.nswpsn.forcequit.xyz',
    # Local dev; harmless to leave in production since nothing real listens
    # here — the session cookie is domain-scoped to forcequit.xyz.
    'http://localhost:8080',
    'http://127.0.0.1:8080',
}


@app.after_request
def add_cors_headers(response):
    path = request.path or ''
    origin = request.headers.get('Origin', '')
    if path.startswith('/api/dashboard/') and origin in _DASHBOARD_ALLOWED_ORIGINS:
        # Credentialed cross-origin: must echo the specific origin + allow creds.
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Access-Control-Allow-Credentials'] = 'true'
        response.headers['Vary'] = 'Origin'
    else:
        response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PATCH, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Accept, Authorization'
    # Preflight cache. 60s was hitting the browser with an OPTIONS round-trip
    # (~80-120ms) before nearly every fetch on a fresh page. Bumped to 7200
    # (Chrome's max — Firefox accepts up to 86400 but clamps to its own
    # default; this is the highest value Chrome respects). Map page load
    # used to do ~30 preflights costing several seconds; with caching the
    # second visit costs zero. Recovery from CORS config change is at most
    # 2 hours — acceptable trade for the latency win.
    response.headers['Access-Control-Max-Age'] = '7200'
    # Add rate limit headers
    if hasattr(request, '_rate_limit_remaining'):
        response.headers['X-RateLimit-Remaining'] = str(request._rate_limit_remaining)
        response.headers['X-RateLimit-Limit'] = str(RATE_LIMIT_REQUESTS)
    return response

# Request logging with source identification
# Skip noisy endpoints to reduce log spam
QUIET_ENDPOINTS = {'/api/heartbeat', '/api/health', '/api/config'}
DATA_ENDPOINTS = {
    '/api/rfs/incidents', '/api/traffic/incidents', '/api/traffic/roadwork',
    '/api/traffic/flood', '/api/traffic/fire', '/api/traffic/majorevent',
    '/api/endeavour/current', '/api/endeavour/future', '/api/ausgrid/outages',
    '/api/ausgrid/stats', '/api/essential/outages', '/api/essential/outages/current',
    '/api/essential/outages/future', '/api/bom/warnings',
    '/api/beachwatch', '/api/weather/current',
    '/api/stats/summary', '/api/news/rss', '/api/waze/alerts', '/api/waze/hazards',
    '/api/waze/police', '/api/waze/roadwork'
}

def get_request_source():
    """Identify request source and return (source_label, source_type, client_ip)"""
    user_agent = request.headers.get('User-Agent', '').lower()
    client_type = request.headers.get('X-Client-Type', '')
    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    
    if client_type == 'discord-bot':
        return ('🤖 Discord', 'discord', client_ip)
    elif 'python' in user_agent or 'aiohttp' in user_agent:
        return ('🔧 Bot/Script', 'bot', client_ip)
    elif any(b in user_agent for b in ['mozilla', 'chrome', 'safari', 'firefox', 'edge']):
        return ('🌐 Browser', 'browser', client_ip)
    else:
        return ('❓ Unknown', 'unknown', client_ip)

def get_session_for_ip(ip):
    """Find session ID for a given IP address"""
    with _page_sessions_lock:
        snap = list(active_page_sessions.items())
    for page_id, session in snap:
        if session.get('ip') == ip:
            # Show last 6 chars of session ID (more unique than first 8)
            short_id = page_id[-6:] if len(page_id) > 6 else page_id
            return short_id, session.get('page_type', '?')
    return None, None

# Friendly names for API endpoints (used in logging)
ENDPOINT_NAMES = {
    '/api/rfs/incidents': 'RFS Fires',
    '/api/rfs/major': 'RFS Major',
    '/api/rfs/fdr': 'Fire Danger',
    '/api/rfs/toban': 'Total Fire Bans',
    '/api/frnsw/incidents': 'FRNSW',
    '/api/ausgrid/outages': 'Ausgrid',
    '/api/endeavour/current': 'Endeavour',
    '/api/endeavour/future': 'Endeavour Planned',
    '/api/traffic/incidents': 'Traffic',
    '/api/traffic/roadwork': 'Roadwork',
    '/api/traffic/flood': 'Floods',
    '/api/traffic/cameras': 'Cameras',
    '/api/aviation/cameras': 'Airport Cameras',
    '/api/waze/alerts': 'Waze',
    '/api/waze/hazards': 'Waze Hazards',
    '/api/waze/police': 'Waze Police',
    '/api/waze/roadwork': 'Waze Roadwork',
    '/api/beachwatch': 'Beachwatch',
    '/api/beachsafe': 'Beachsafe',
    '/api/weather/radar': 'Radar',
    '/api/weather/current': 'Weather',
    '/api/weather/warnings': 'Warnings',
    '/api/news': 'News',
    '/api/active-units': 'Active Units',
    '/api/active-talkgroups': 'Talkgroups',
}

@app.before_request
def check_rate_limit():
    """Check rate limit before processing request"""
    path = request.path
    
    # Skip OPTIONS preflight
    if request.method == 'OPTIONS':
        return None
    
    # Only rate limit API requests
    if not path.startswith('/api/'):
        return None
    
    # Skip rate limiting for these endpoints
    skip_rate_limit = {
        '/api/heartbeat', '/api/health', '/api/config',
        '/api/cache/status', '/api/cache/stats',
        # Summary endpoints are cheap DB reads polled by every open live.html
        # tab — don't let them eat the user's 100/min budget.
        '/api/summaries/latest', '/api/summaries',
        # The Waze userscript fires one POST per region per rotation — at
        # 70+ regions in a few seconds it would saturate the IP budget on
        # its own. It carries its own X-Ingest-Key auth (matched against
        # WAZE_INGEST_KEY) so we let it bypass the IP limit entirely.
        '/api/waze/ingest',
    }
    if path in skip_rate_limit:
        return None

    # Dashboard endpoints: users may toggle several channels in quick
    # succession and the 100/min budget is for public API consumers, not
    # authenticated dashboard sessions.
    if path.startswith('/api/dashboard/'):
        return None

    # Skip rate limit for anyone carrying a valid NSWPSN_API_KEY — that's
    # the operator's own frontend (config.js), the Discord bot, and any
    # authenticated tooling. The IP-based limit is for anonymous public
    # abuse only; authenticated callers shouldn't be capped.
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        provided_key = auth_header[7:]
    else:
        provided_key = (request.headers.get('X-API-Key', '')
                        or request.args.get('api_key', ''))
    if provided_key and API_KEY and provided_key == API_KEY:
        return None

    # Check rate limit
    client_ip = _get_client_ip()
    is_limited, remaining = _check_rate_limit(client_ip)
    request._rate_limit_remaining = remaining
    
    if is_limited:
        Log.warn(f"Rate limited: {client_ip} on {path}")
        return jsonify({
            'error': 'Rate limit exceeded',
            'message': 'Too many requests. Please wait before retrying.',
            'retry_after': RATE_LIMIT_WINDOW
        }), 429

@app.before_request
def log_request_source():
    """Log requests with source identification"""
    path = request.path
    
    # Skip OPTIONS preflight
    if request.method == 'OPTIONS':
        return None
    
    # Only log API requests
    if not path.startswith('/api/'):
        return None
    
    # Skip quiet endpoints (heartbeat, health, config)
    if path in QUIET_ENDPOINTS:
        return None
    
    source_label, source_type, client_ip = get_request_source()
    
    # Get friendly name for endpoint
    friendly_name = ENDPOINT_NAMES.get(path, path.replace('/api/', ''))
    
    # In production: log Discord bot + browser data requests (condensed format)
    if not DEV_MODE:
        timestamp = datetime.now().strftime('%H:%M:%S')
        if source_type == 'discord':
            with Log._lock:
                print(f"[{timestamp}] 🤖 {friendly_name}", flush=True)
        elif source_type == 'browser':
            # Log browser requests for key data endpoints (shows refresh is working)
            data_endpoints = {'/api/rfs/incidents', '/api/traffic/incidents', '/api/waze/hazards', 
                           '/api/waze/police', '/api/waze/roadwork', '/api/traffic/roadwork',
                           '/api/traffic/flood', '/api/traffic/cameras'}
            if path in data_endpoints:
                short_name = friendly_name.split()[0] if ' ' in friendly_name else friendly_name
                with Log._lock:
                    print(f"[{timestamp}] 🌐 {short_name}", flush=True)
        return None
    
    # DEV_MODE: log all requests with session info
    timestamp = datetime.now().strftime('%H:%M:%S')
    session_id, page_type = get_session_for_ip(client_ip)
    
    if session_id:
        print(f"[{timestamp}] {source_label} [{session_id}@{page_type}] → {friendly_name}")
    else:
        print(f"[{timestamp}] {source_label} → {friendly_name} ({client_ip})")

# In-memory cache
cache = {}
CACHE_TTL = 60

# ============== DATA ARCHIVAL SYSTEM ==============
# PostgreSQL - single database, single data_history table
# Source types map to WHERE filters; locks retained for batch coordination
DB_PATH_HISTORY_WAZE = 'waze'
DB_PATH_HISTORY_TRAFFIC = 'traffic'
DB_PATH_HISTORY_RFS = 'rfs'
DB_PATH_HISTORY_POWER = 'power'
DB_PATH_HISTORY_PAGER = 'pager'
DB_PATH_HISTORY_WEATHER = 'weather'

SOURCE_TO_DB = {
    'waze_hazard': DB_PATH_HISTORY_WAZE, 'waze_police': DB_PATH_HISTORY_WAZE, 'waze_roadwork': DB_PATH_HISTORY_WAZE,
    'waze_jam': DB_PATH_HISTORY_WAZE,
    'traffic_incident': DB_PATH_HISTORY_TRAFFIC, 'traffic_roadwork': DB_PATH_HISTORY_TRAFFIC,
    'traffic_flood': DB_PATH_HISTORY_TRAFFIC, 'traffic_fire': DB_PATH_HISTORY_TRAFFIC,
    'traffic_majorevent': DB_PATH_HISTORY_TRAFFIC, 'livetraffic': DB_PATH_HISTORY_TRAFFIC,
    'rfs': DB_PATH_HISTORY_RFS,
    'endeavour_current': DB_PATH_HISTORY_POWER, 'endeavour_planned': DB_PATH_HISTORY_POWER,
    'endeavour': DB_PATH_HISTORY_POWER, 'ausgrid': DB_PATH_HISTORY_POWER,
    'essential_current': DB_PATH_HISTORY_POWER, 'essential_planned': DB_PATH_HISTORY_POWER,
    'essential_future': DB_PATH_HISTORY_POWER, 'essential': DB_PATH_HISTORY_POWER,
    'pager': DB_PATH_HISTORY_PAGER,
    'bom_warning': DB_PATH_HISTORY_WEATHER, 'bom_land': DB_PATH_HISTORY_WEATHER,
    'bom_marine': DB_PATH_HISTORY_WEATHER, 'bom': DB_PATH_HISTORY_WEATHER,
}

# Single table in PostgreSQL - iterate once
ALL_HISTORY_DBS = [None]

def get_history_db_for_source(source):
    """Get logical db key for source (for lock lookup)"""
    return SOURCE_TO_DB.get(source, DB_PATH_HISTORY_WAZE)

# Adaptive collection intervals (configurable via environment variables)
IDLE_INTERVAL = int(os.environ.get('COLLECTION_IDLE_INTERVAL', 300))        # Default: 5 minutes when no pages are open
ACTIVE_INTERVAL = int(os.environ.get('COLLECTION_ACTIVE_INTERVAL', 120))    # Default: 2 minutes when pages are open
HEARTBEAT_TIMEOUT = int(os.environ.get('COLLECTION_HEARTBEAT_TIMEOUT', 180)) # Default: 3 minutes without heartbeat

# API Cache TTLs (configurable via environment variables, values in seconds)
CACHE_TTL_AUSGRID = int(os.environ.get('CACHE_TTL_AUSGRID', 120))            # Default: 2 min
CACHE_TTL_ENDEAVOUR_CURRENT = int(os.environ.get('CACHE_TTL_ENDEAVOUR_CURRENT', 120))        # Default: 2 min
CACHE_TTL_ENDEAVOUR_MAINTENANCE = int(os.environ.get('CACHE_TTL_ENDEAVOUR_MAINTENANCE', 120))  # Default: 2 min
CACHE_TTL_ENDEAVOUR_FUTURE = int(os.environ.get('CACHE_TTL_ENDEAVOUR_FUTURE', 300))          # Default: 5 min
CACHE_TTL_TRAFFIC = int(os.environ.get('CACHE_TTL_TRAFFIC', 60))             # Default: 1 min
CACHE_TTL_TRAFFIC_ROADWORK = int(os.environ.get('CACHE_TTL_TRAFFIC_ROADWORK', 120))    # Default: 2 min
CACHE_TTL_TRAFFIC_CAMERAS = int(os.environ.get('CACHE_TTL_TRAFFIC_CAMERAS', 60))       # Default: 1 min
CACHE_TTL_TRAFFIC_LGA = int(os.environ.get('CACHE_TTL_TRAFFIC_LGA', 60))     # Default: 1 min
CACHE_TTL_RFS = int(os.environ.get('CACHE_TTL_RFS', 60))                     # Default: 1 min
CACHE_TTL_RFS_FDR = int(os.environ.get('CACHE_TTL_RFS_FDR', 300))            # Default: 5 min
CACHE_TTL_BOM = int(os.environ.get('CACHE_TTL_BOM', 300))                    # Default: 5 min
CACHE_TTL_BEACH = int(os.environ.get('CACHE_TTL_BEACH', 600))                # Default: 10 min
CACHE_TTL_WEATHER = int(os.environ.get('CACHE_TTL_WEATHER', 300))            # Default: 5 min
CACHE_TTL_STATS = int(os.environ.get('CACHE_TTL_STATS', 60))                 # Default: 1 min
CACHE_TTL_RSS = int(os.environ.get('CACHE_TTL_RSS', 300))                    # Default: 5 min
CACHE_TTL_PAGER = int(os.environ.get('CACHE_TTL_PAGER', 120))                # Default: 2 min
CACHE_TTL_CENTRALWATCH = int(os.environ.get('CACHE_TTL_CENTRALWATCH', 120))  # Default: 2 min
CACHE_TTL_ESSENTIAL = int(os.environ.get('CACHE_TTL_ESSENTIAL', 180))        # Default: 3 min

# Pagermon API configuration
PAGERMON_URL = os.environ.get('PAGERMON_URL', '')
PAGERMON_API_KEY = os.environ.get('PAGERMON_API_KEY', '')

# Discord webhook for editor requests notifications
EDITOR_REQUEST_WEBHOOK = os.environ.get('EDITOR_REQUEST_WEBHOOK', '')
EDITOR_REQUEST_PING_ID = os.environ.get('EDITOR_REQUEST_PING_ID', '')

# Supabase configuration for creating user accounts
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_SERVICE_ROLE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')

# =============================================================================
# Endeavour Energy Outages - Supabase API
# =============================================================================
# Endeavour Energy migrated from their Sitecore-based API to Supabase.
# No Cloudflare bypass needed - direct API access with anon key.
# =============================================================================

ENDEAVOUR_SUPABASE_URL = os.environ.get('ENDEAVOUR_SUPABASE_URL', '')
ENDEAVOUR_SUPABASE_KEY = os.environ.get('ENDEAVOUR_SUPABASE_KEY', '')

ENDEAVOUR_SUPABASE_HEADERS = {
    'apikey': ENDEAVOUR_SUPABASE_KEY,
    'Authorization': f'Bearer {ENDEAVOUR_SUPABASE_KEY}',
    'Content-Type': 'application/json',
}

def _fetch_endeavour_supabase(endpoint, params=None, method='GET', body=None, timeout=20):
    """Fetch data from Endeavour Energy's Supabase API.
    
    Args:
        endpoint: API path (e.g. '/rpc/get_outage_areas_fast' or '/outage-points')
        params: Query parameters dict
        method: HTTP method (GET or POST)
        body: JSON body for POST requests
        timeout: Request timeout in seconds
    
    Returns:
        Parsed JSON data or None on failure
    """
    url = f"{ENDEAVOUR_SUPABASE_URL}{endpoint}"
    try:
        if method == 'POST':
            r = requests.post(url, headers=ENDEAVOUR_SUPABASE_HEADERS,
                            json=body or {}, params=params, timeout=timeout)
        else:
            r = requests.get(url, headers=ENDEAVOUR_SUPABASE_HEADERS,
                           params=params, timeout=timeout)
        
        if r.status_code == 200:
            return r.json()
        else:
            Log.api(f"Endeavour Supabase: HTTP {r.status_code} for {endpoint}")
            return None
    except Exception as e:
        Log.api(f"Endeavour Supabase error ({endpoint}): {e}")
        return None

def _fetch_endeavour_all_outages():
    """Fetch all Endeavour outage data from Supabase and return normalized outages.
    
    Uses two API calls:
    1. get_outage_areas_fast (RPC) - aggregated incidents with GPS coordinates
    2. outage-points (table) - individual points for enrichment (cause, timing, suburb)
    
    Returns: dict with 'current' (unplanned), 'current_maintenance' (active planned),
             and 'future_maintenance' (scheduled planned) outage lists
    """
    # Step 1: Fetch aggregated outage areas (one record per incident)
    areas = _fetch_endeavour_supabase('/rpc/get_outage_areas_fast', method='POST', body={})
    if not areas or not isinstance(areas, list):
        source_error('power_endeavour', 'No data from get_outage_areas_fast')
        Log.prewarm("Endeavour: No data from get_outage_areas_fast")
        return {'current': [], 'current_maintenance': [], 'future_maintenance': []}
    
    # Step 2: Fetch outage points for enrichment data (cause, timing, suburb)
    # Select only needed columns to minimize transfer size
    enrichment_params = {
        'select': 'incident_id,cause,sub_cause,start_date_time,end_date_time,etr,cityname,postcode,street_name,updated_at',
        'order': 'incident_id.asc',
    }
    points = _fetch_endeavour_supabase('/outage-points', params=enrichment_params)
    
    # Build enrichment lookup (first record per incident_id)
    enrichment = {}
    if points and isinstance(points, list):
        for pt in points:
            iid = pt.get('incident_id')
            if iid and iid not in enrichment:
                enrichment[iid] = pt
    
    # Removed: redundant 'Endeavour: N incidents, N enriched' log. This
    # function is called once per consumer (current / planned / future)
    # so the line printed 3-4 times per prewarm cycle, while the per-
    # consumer 'Endeavour <kind>: N outages from Supabase' lines that
    # follow are more informative.


    # Step 3: Normalize and split into 3 categories
    current_outages = []           # Unplanned outages
    current_maintenance = []       # Planned maintenance currently active (start_date in past)
    future_maintenance = []        # Planned maintenance scheduled for future
    
    now = datetime.now(timezone.utc)
    
    for area in areas:
        incident_id = area.get('incident_id', '')
        outage_type_raw = (area.get('outage_type') or '').upper()
        is_planned = outage_type_raw == 'PLANNED'
        
        # Get enrichment data for this incident
        enrich = enrichment.get(incident_id, {})
        
        # Get dates and validate
        start_date = enrich.get('start_date_time') or ''
        end_date = enrich.get('end_date_time') or enrich.get('etr') or ''
        updated = enrich.get('updated_at') or ''
        
        if start_date and not is_valid_datetime(start_date):
            start_date = None
        if end_date and not is_valid_datetime(end_date):
            end_date = None
        
        # For planned outages, determine if currently active or future
        is_current_maintenance = False
        if is_planned:
            status_raw = (area.get('incident_status') or '').upper()
            # Active statuses mean the maintenance is currently underway
            if status_raw in ('SUBMITTED', 'NEW', 'DESPATCHED', 'DAMAGE ASSESSED', 'REPAIR', 'REPAIR CONTROL ROOM'):
                is_current_maintenance = True
            elif start_date:
                # If start time has passed, it's current maintenance
                try:
                    start_dt = datetime.fromisoformat(str(start_date).replace('Z', '+00:00'))
                    if start_dt <= now:
                        is_current_maintenance = True
                except (ValueError, TypeError):
                    pass  # Can't parse, default to future
        
        # Set outage type label
        if not is_planned:
            outage_type_label = 'Unplanned'
        elif is_current_maintenance:
            outage_type_label = 'Current Maintenance'
        else:
            outage_type_label = 'Future Maintenance'
        
        # Map incident_status to more readable format
        status_raw = (area.get('incident_status') or 'Active').upper()
        status_map = {
            'SUBMITTED': 'Active',
            'NEW': 'Active',
            'LODGED': 'Scheduled',
            'PREPARED': 'Scheduled',
            'SCHEDULED': 'Scheduled',
            'DESPATCHED': 'Crew Dispatched',
            'DAMAGE ASSESSED': 'Damage Assessed',
        }
        status = status_map.get(status_raw, status_raw.title())
        
        outage = {
            'id': incident_id,
            'suburb': (enrich.get('cityname') or '').title() or 'Unknown',
            'streets': enrich.get('street_name') or '',
            'customersAffected': area.get('customers_affected') or 0,
            'status': status,
            'cause': enrich.get('cause') or enrich.get('sub_cause') or ('Planned maintenance' if is_planned else ''),
            'outageType': outage_type_label,
            'startTime': start_date,
            'estimatedRestoration': end_date,
            'lastUpdated': updated,
            'latitude': area.get('center_lat'),
            'longitude': area.get('center_lng'),
            'postcode': enrich.get('postcode') or '',
            'hasGPS': bool(area.get('center_lat') and area.get('center_lng')),
        }
        
        if is_planned:
            outage['endTime'] = end_date
            outage['duration'] = ''
            if is_current_maintenance:
                current_maintenance.append(outage)
            else:
                future_maintenance.append(outage)
        else:
            current_outages.append(outage)
    
    source_ok('power_endeavour')
    return {'current': current_outages, 'current_maintenance': current_maintenance, 'future_maintenance': future_maintenance}

# Data retention settings (how long to keep historical data)
DATA_RETENTION_DAYS = int(os.environ.get('DATA_RETENTION_DAYS', 7))          # Default: 7 days
DATA_CLEANUP_INTERVAL = int(os.environ.get('DATA_CLEANUP_INTERVAL', 3600))   # Default: 1 hour

archive_thread = None
archive_running = False
last_heartbeat = 0       # Timestamp of last page heartbeat

# Separate write locks for each database - allows parallel writes to different DBs
# Each SQLite file can only have one writer, but now they don't block each other
_db_lock_cache = threading.Lock()    # For cache.db
_db_lock_stats = threading.Lock()    # For stats.db
_db_lock_config = threading.Lock()   # For config.db

# History database locks. Originally one Lock() per source type because
# each source had its own SQLite file. With Postgres they all hit one
# `data_history` table — separate Python locks left them free to run
# concurrent UPDATEs on overlapping rows, which deadlocked at the DB
# layer ("Cleanup history error: deadlock detected"). All aliases now
# point to a single master lock so writes serialise cleanly in Python
# and never reach the deadlock detector.
_db_lock_history_master = threading.Lock()
_db_lock_history_waze    = _db_lock_history_master
_db_lock_history_traffic = _db_lock_history_master
_db_lock_history_rfs     = _db_lock_history_master
_db_lock_history_power   = _db_lock_history_master
_db_lock_history_pager   = _db_lock_history_master
_db_lock_history_weather = _db_lock_history_master

# Map logical db keys to the same master lock (kept for API compatibility
# with code that calls get_history_lock_for_source).
_DB_LOCKS = {
    DB_PATH_HISTORY_WAZE: _db_lock_history_master,
    DB_PATH_HISTORY_TRAFFIC: _db_lock_history_master,
    DB_PATH_HISTORY_RFS: _db_lock_history_master,
    DB_PATH_HISTORY_POWER: _db_lock_history_master,
    DB_PATH_HISTORY_PAGER: _db_lock_history_master,
    DB_PATH_HISTORY_WEATHER: _db_lock_history_master,
}

def get_history_lock_for_source(source):
    """Get the write lock for a given source type"""
    db_path = get_history_db_for_source(source)
    return _DB_LOCKS.get(db_path, _db_lock_history_waze)


def get_history_dbs_for_sources(sources):
    """
    Get sources filter for PostgreSQL single-table query.
    Returns list of one element: None = all sources, or list of source names to filter.
    """
    if not sources:
        return [None]  # One "batch" = all sources
    src_list = [s.strip() for s in sources if s.strip()]
    return [src_list if src_list else None]


# ============== SOURCE HIERARCHY ==============
# Maps source names to their provider and type for better organization
# source_provider: The data provider organization
# source_type: The type of data within that provider

SOURCE_HIERARCHY = {
    # Waze - Note: waze_hazard contains hazards, jams, and accidents
    # The subcategory field provides more specific info (JAM_HEAVY_TRAFFIC, ACCIDENT_MAJOR, etc.)
    'waze_hazard': {'provider': 'Waze', 'type': 'Hazards'},
    'waze_police': {'provider': 'Waze', 'type': 'Police'},
    'waze_roadwork': {'provider': 'Waze', 'type': 'Roadwork'},
    'waze_jam': {'provider': 'Waze', 'type': 'Traffic Jams'},
    # LiveTraffic NSW (Transport for NSW)
    'traffic_incident': {'provider': 'LiveTraffic NSW', 'type': 'Incidents'},
    'traffic_roadwork': {'provider': 'LiveTraffic NSW', 'type': 'Roadwork'},
    'traffic_flood': {'provider': 'LiveTraffic NSW', 'type': 'Flooding'},
    'traffic_fire': {'provider': 'LiveTraffic NSW', 'type': 'Fires'},
    'traffic_majorevent': {'provider': 'LiveTraffic NSW', 'type': 'Major Events'},
    'livetraffic': {'provider': 'LiveTraffic NSW', 'type': 'Incidents'},
    # NSW Rural Fire Service
    'rfs': {'provider': 'NSW Rural Fire Service', 'type': 'Fires'},
    # Endeavour Energy
    'endeavour_current': {'provider': 'Endeavour Energy', 'type': 'Current Outages'},
    'endeavour_planned': {'provider': 'Endeavour Energy', 'type': 'Planned Outages'},
    'endeavour': {'provider': 'Endeavour Energy', 'type': 'Outages'},
    # Ausgrid
    'ausgrid': {'provider': 'Ausgrid', 'type': 'Outages'},
    # Essential Energy
    'essential_current': {'provider': 'Essential Energy', 'type': 'Current Outages'},
    'essential_planned': {'provider': 'Essential Energy', 'type': 'Planned Outages'},
    'essential_future': {'provider': 'Essential Energy', 'type': 'Future Outages'},
    'essential': {'provider': 'Essential Energy', 'type': 'Outages'},
    'essential_energy_cancelled': {'provider': 'Essential Energy', 'type': 'Cancelled'},  # Legacy: feed removed, maps stale DB records
    # Bureau of Meteorology
    'bom_warning': {'provider': 'Bureau of Meteorology', 'type': 'All Warnings'},
    'bom_land': {'provider': 'Bureau of Meteorology', 'type': 'Land'},
    'bom_marine': {'provider': 'Bureau of Meteorology', 'type': 'Marine'},
    'bom': {'provider': 'Bureau of Meteorology', 'type': 'All Warnings'},
    # Pager
    'pager': {'provider': 'Pager', 'type': 'Messages'},
}

# Deprecated sources — still mapped in SOURCE_HIERARCHY for grouping, but
# excluded from history API results (records may still exist in DB).
DEPRECATED_SOURCES = {'essential_energy_cancelled'}

# Provider display names for UI (using Font Awesome icon classes - without fa- prefix, added in frontend)
SOURCE_PROVIDERS = {
    'Waze': {'icon': 'car', 'color': '#00d4ff'},
    'LiveTraffic NSW': {'icon': 'road', 'color': '#f97316'},
    'NSW Rural Fire Service': {'icon': 'fire', 'color': '#ef4444'},
    'Endeavour Energy': {'icon': 'bolt', 'color': '#fbbf24'},
    'Ausgrid': {'icon': 'plug', 'color': '#f59e0b'},
    'Essential Energy': {'icon': 'bolt', 'color': '#f59e0b'},
    'Bureau of Meteorology': {'icon': 'cloud', 'color': '#3b82f6'},
    'Pager': {'icon': 'pager', 'color': '#8b5cf6'},
}


def get_source_hierarchy(source):
    """Get the provider and type for a source name"""
    hierarchy = SOURCE_HIERARCHY.get(source)
    if hierarchy:
        return hierarchy['provider'], hierarchy['type']
    # Fallback: try to parse from source name
    if '_' in source:
        parts = source.split('_', 1)
        return parts[0].title(), parts[1].replace('_', ' ').title()
    return source.title(), 'Other'


# ============== CANONICAL ALERT-TYPE / PROVIDER MAPS ==============
# Used by /api/data/history/filters to expose a stable provider/type
# nesting regardless of how the underlying data_history.source values
# evolve. Keep in sync with the frontend filter tree.

RAW_SOURCE_TO_ALERT_TYPE = {
    'rfs':                'rfs',
    'bom_marine':         'bom_marine',
    'bom_land':           'bom_land',
    'bom_warning':        'bom_land',     # legacy fold-in
    'bom':                'bom_land',     # legacy fold-in
    'traffic_incident':   'traffic_incident',
    'traffic_roadwork':   'traffic_roadwork',
    'traffic_flood':      'traffic_flood',
    'traffic_fire':       'traffic_fire',
    'traffic_majorevent': 'traffic_majorevent',
    'livetraffic':        'traffic_incident',  # legacy fold-in
    'endeavour_current':  'endeavour_current',
    'endeavour_planned':  'endeavour_planned',
    'endeavour':          'endeavour_current',  # legacy
    'ausgrid':            'ausgrid',
    'essential_current':  'essential_planned',  # fold "current" into "planned"
    'essential_planned':  'essential_planned',
    'essential_future':   'essential_future',
    'essential':          'essential_planned',  # legacy
    'waze_hazard':        'waze_hazard',
    'waze_jam':           'waze_jam',
    'waze_police':        'waze_police',
    'waze_roadwork':      'waze_roadwork',
    'pager':              'pager',
}

# Canonical alert_type -> (provider key, type display name)
ALERT_TYPE_PROVIDER = {
    'rfs':                ('rfs',         'Major Incidents'),
    'bom_land':           ('bom',         'Land Warnings'),
    'bom_marine':         ('bom',         'Marine Warnings'),
    'traffic_incident':   ('livetraffic', 'Incidents'),
    'traffic_roadwork':   ('livetraffic', 'Roadwork'),
    'traffic_flood':      ('livetraffic', 'Flooding'),
    'traffic_fire':       ('livetraffic', 'Fires'),
    'traffic_majorevent': ('livetraffic', 'Major Events'),
    'endeavour_current':  ('endeavour',   'Current Outages'),
    'endeavour_planned':  ('endeavour',   'Planned Outages'),
    'ausgrid':            ('ausgrid',     'Outages'),
    'essential_planned':  ('essential',   'Planned Outages'),
    'essential_future':   ('essential',   'Future Outages'),
    'waze_hazard':        ('waze',        'Hazards'),
    'waze_jam':           ('waze',        'Traffic Jams'),
    'waze_police':        ('waze',        'Police'),
    'waze_roadwork':      ('waze',        'Roadwork'),
    'pager':              ('pager',       'Messages'),
    'user_incident':      ('user',        'User Incidents'),
    'radio_summary':      ('rdio',        'Hourly Summaries'),
}

# Provider display metadata. Keys mirror ALERT_TYPE_PROVIDER's first tuple
# element; every provider that should appear in /filters must have an
# entry here.
PROVIDER_DISPLAY = {
    'rfs':         {'name': 'NSW Rural Fire Service',     'icon': 'fire',  'color': '#ef4444'},
    'bom':         {'name': 'Bureau of Meteorology',      'icon': 'cloud', 'color': '#3b82f6'},
    'livetraffic': {'name': 'LiveTraffic NSW',            'icon': 'road',  'color': '#f97316'},
    'endeavour':   {'name': 'Endeavour Energy',           'icon': 'bolt',  'color': '#fbbf24'},
    'ausgrid':     {'name': 'Ausgrid',                    'icon': 'plug',  'color': '#f59e0b'},
    'essential':   {'name': 'Essential Energy',           'icon': 'bolt',  'color': '#06b6d4'},
    'waze':        {'name': 'Waze',                       'icon': 'car',   'color': '#00d4ff'},
    'pager':       {'name': 'Pager',                      'icon': 'pager', 'color': '#8b5cf6'},
    'user':        {'name': 'NSW PSN User Submissions',   'icon': 'user',  'color': '#a855f7'},
    'rdio':        {'name': 'Radio Scanner',              'icon': 'radio', 'color': '#10b981'},
}

# Display order for the providers array — the frontend uses this as a
# stable sort so the panel doesn't reshuffle each request.
PROVIDER_ORDER = ['rfs', 'bom', 'livetraffic', 'endeavour', 'ausgrid',
                  'essential', 'waze', 'pager', 'user', 'rdio']

# Per-provider type ordering. Types not listed here fall to the end in
# alphabetical order — only the ones we explicitly want a fixed sequence
# for need to appear.
PROVIDER_TYPE_ORDER = {
    'bom':         ['bom_land', 'bom_marine'],
    'livetraffic': ['traffic_incident', 'traffic_roadwork',
                    'traffic_flood', 'traffic_fire', 'traffic_majorevent'],
    'endeavour':   ['endeavour_current', 'endeavour_planned'],
    'essential':   ['essential_planned', 'essential_future'],
    'waze':        ['waze_hazard', 'waze_jam', 'waze_police', 'waze_roadwork'],
}


def _canonical_alert_type(raw_source):
    """Resolve a data_history.source value to its canonical alert_type."""
    if not raw_source:
        return None
    return RAW_SOURCE_TO_ALERT_TYPE.get(raw_source, raw_source)

# Track active page sessions with unique IDs and timestamps
# Format: {page_id: {'last_seen': timestamp, 'user_agent': str, 'ip': str, 'page_type': str, 'is_data_page': bool}}
# Guarded by _page_sessions_lock (RLock so cleanup can be called from within
# other locked sections). Every iteration, addition, update, or deletion must
# hold the lock — Python's GIL makes single dict ops atomic but raises
# RuntimeError if the dict is mutated during iteration on another thread.
active_page_sessions = {}
_page_sessions_lock = threading.RLock()
PAGE_SESSION_TIMEOUT = 120  # Remove sessions inactive for 2 minutes

def init_archive_db():
    """Verify PostgreSQL connection and tables (run init_postgres.py once to create schema)"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM api_data_cache LIMIT 1")
        cur.close()
        conn.close()
    except Exception as e:
        Log.error(f"Database not initialized. Run: python init_postgres.py - {e}")
        raise
    Log.startup("Database connected (PostgreSQL)")

    # Cheap idempotent schema additions that should block startup — these
    # let `rdio_summaries` gain the `release_at` column on existing deployments
    # without requiring the user to re-run init_postgres.py.
    try:
        _c = get_conn()
        try:
            _cur = _c.cursor()
            _cur.execute('ALTER TABLE rdio_summaries ADD COLUMN IF NOT EXISTS release_at TIMESTAMPTZ')
            _cur.execute('CREATE INDEX IF NOT EXISTS idx_rdio_summaries_release ON rdio_summaries(release_at)')
            # Filter-dropdown cache — small, rebuilt periodically from is_latest=1.
            _cur.execute('''
                CREATE TABLE IF NOT EXISTS data_history_filter_cache (
                    kind TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT '',
                    value TEXT NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (kind, source, value)
                )
            ''')
            _cur.execute('CREATE INDEX IF NOT EXISTS idx_filter_cache_kind ON data_history_filter_cache(kind)')
            _cur.execute('CREATE INDEX IF NOT EXISTS idx_filter_cache_kind_source ON data_history_filter_cache(kind, source)')
            # Pre-aggregated police-heatmap cache. Refreshed in the background
            # so the request path never has to GROUP BY data_history rows
            # while archiving holds locks.
            _cur.execute('''
                CREATE TABLE IF NOT EXISTS police_heatmap_cache (
                    lat_bin     NUMERIC(8,3) NOT NULL,
                    lng_bin     NUMERIC(9,3) NOT NULL,
                    subcategory TEXT NOT NULL DEFAULT '',
                    count       INTEGER NOT NULL,
                    updated_at  TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (lat_bin, lng_bin, subcategory)
                )
            ''')
            _cur.execute('CREATE INDEX IF NOT EXISTS idx_police_heatmap_count ON police_heatmap_cache(count DESC)')
            _cur.execute('CREATE INDEX IF NOT EXISTS idx_police_heatmap_subcat ON police_heatmap_cache(subcategory)')
            _c.commit()
            _cur.close()
        finally:
            _c.close()
    except Exception as e:
        Log.error(f"rdio_summaries schema migration warning: {e}")

    # One-time data migrations run in a background thread — they UPDATE
    # data_history which can be a large scan and shouldn't block startup.
    def _run_migrations():
        try:
            migrate_endeavour_categories()
            migrate_bom_sources()
            migrate_bom_subcategories()
        except Exception as e:
            Log.error(f"Background migrations error: {e}")
    threading.Thread(target=_run_migrations, daemon=True, name='data-migrations').start()

    # Add partial indexes on data_history for the common list-page path
    # (is_latest=1). Uses CONCURRENTLY so it doesn't block writes on existing
    # large tables. Runs in a background thread because CONCURRENTLY must be
    # outside a transaction and can take minutes on a big table.
    def _ensure_partial_indexes():
        try:
            import psycopg2
            dsn = os.environ.get('DATABASE_URL', '')
            if not dsn:
                return
            conn = psycopg2.connect(dsn)
            try:
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                built_any = False
                for sql in (
                    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_latest_only_fetched ON data_history(fetched_at DESC) WHERE is_latest = 1',
                    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_latest_only_source ON data_history(source, fetched_at DESC) WHERE is_latest = 1',
                    # Partial index for the Waze bbox reconcile worker. The query
                    # filters on (source IN waze_*) + is_live=1 + lat/lng range,
                    # and was timing out at 15s under archiving contention.
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_waze_live_geo ON data_history (source, latitude, longitude) WHERE is_live = 1 AND source_id IS NOT NULL AND source IN ('waze_hazard','waze_police','waze_roadwork','waze_jam')",
                    # Covering partial index for the police-heatmap aggregation.
                    # The refresh scans is_latest=1 + source='waze_police' rows
                    # and groups by (lat_bin, lng_bin, subcategory). INCLUDE
                    # lets the planner do an index-only scan instead of fanning
                    # out to the heap for every row — the heap fetches were
                    # what pushed the 5-minute statement_timeout.
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_waze_police_heatmap ON data_history (fetched_at DESC) INCLUDE (latitude, longitude, subcategory) WHERE is_latest = 1 AND source = 'waze_police'",
                    # Partial UNIQUE index — guarantees at most one
                    # is_latest=1 row per (source, source_id). Without
                    # this, a write path that bypasses the master lock
                    # (or a retry after partial failure) can leave
                    # duplicate is_latest=1 rows that subsequent
                    # archive cycles can never fully clear.
                    "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uniq_data_history_latest ON data_history (source, source_id) WHERE is_latest = 1 AND source_id IS NOT NULL",
                    # Composite index for keyset (cursor) pagination on
                    # /api/data/history. Without `id` in the index, the
                    # planner has to heap-fetch every candidate row to
                    # evaluate the (fetched_at = X AND id < Y) tiebreaker
                    # in the cursor seek — making the per-page seek O(N)
                    # in tied rows instead of O(log N). With ~200k+ rows
                    # in the latest set this materially affects forward
                    # paging when many rows share a fetched_at second.
                    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_latest_fetched_id ON data_history(fetched_at DESC, id DESC) WHERE is_latest = 1',
                    # Source-scoped variant of the same. The list page
                    # filters by source AND orders by (fetched_at, id);
                    # without this index Postgres either re-sorts in
                    # memory using idx_data_latest_only_source (slow with
                    # an unbounded time range) or filter-scans the global
                    # idx_data_latest_fetched_id row-by-row. Either way
                    # the multi-source-no-time-filter query times out at
                    # 25s. Adding source as the leading column gives the
                    # planner a direct match for "WHERE source IN (...)
                    # AND is_latest=1 ORDER BY fetched_at DESC, id DESC".
                    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_latest_source_fetched_id ON data_history(source, fetched_at DESC, id DESC) WHERE is_latest = 1',
                ):
                    # Pull the index name from the word immediately after
                    # `EXISTS` — works for both `CREATE INDEX ... IF NOT
                    # EXISTS <name>` and `CREATE UNIQUE INDEX ... IF NOT
                    # EXISTS <name>` (the UNIQUE form has one extra word
                    # earlier, which threw off the previous split()[6]).
                    parts = sql.split()
                    try:
                        idx_name = parts[parts.index('EXISTS') + 1]
                    except (ValueError, IndexError):
                        idx_name = '<unknown>'
                    try:
                        Log.startup(f"Building partial index {idx_name} CONCURRENTLY (may take minutes)...")
                        cur.execute(sql)
                        Log.startup(f"✓ Partial index {idx_name} ready")
                        built_any = True
                    except Exception as e:
                        Log.error(f"Partial index create warn ({idx_name}): {e}")
                # ANALYZE so the planner picks up the new partial indexes.
                # Without this, pg_stat sees zero rows under the new index
                # and the planner may keep picking seqscan or a worse index.
                if built_any:
                    try:
                        Log.startup("Running ANALYZE data_history to refresh planner stats...")
                        cur.execute('ANALYZE data_history')
                        Log.startup("✓ ANALYZE complete")
                    except Exception as e:
                        Log.error(f"ANALYZE data_history failed: {e}")
                # Aggressive autovacuum settings for data_history. This table
                # sees continuous UPDATE/DELETE traffic from archive + cleanup
                # workers, which generates dead tuples faster than the default
                # autovacuum thresholds (20% / 10%) catch up with. Tightening
                # the scale factors plus raising the per-run cost limit lets
                # autovacuum trigger sooner and finish faster, keeping the
                # visibility map clean enough for index-only scans.
                #   - vacuum_scale_factor 0.05  -> trigger at 5% dead rows
                #   - analyze_scale_factor 0.02 -> trigger ANALYZE at 2% changed
                #   - vacuum_cost_limit 2000    -> let each run do more work
                #     before pausing (default 200, way too conservative)
                #   - vacuum_cost_delay 5ms     -> shorter pauses between
                #     work batches inside a single autovacuum run
                # ALTER TABLE SET (...) is idempotent, safe to run every
                # boot — Postgres just no-ops if values are unchanged.
                try:
                    cur.execute('''
                        ALTER TABLE data_history SET (
                            autovacuum_vacuum_scale_factor = 0.05,
                            autovacuum_analyze_scale_factor = 0.02,
                            autovacuum_vacuum_cost_limit = 2000,
                            autovacuum_vacuum_cost_delay = 5
                        )
                    ''')
                    Log.startup("✓ data_history autovacuum settings tuned (5%/2% triggers, 2000 cost limit)")
                except Exception as e:
                    Log.warn(f"data_history autovacuum tuning skipped: {e}")
                cur.close()
            finally:
                conn.close()
        except Exception as e:
            Log.error(f"Partial index migration error: {e}")
    threading.Thread(target=_ensure_partial_indexes, daemon=True, name='partial-index-migration').start()
    # Bot-DB index migration — separate thread so it doesn't block
    # the main DB migration.
    threading.Thread(target=_dash_bot_db_indexes_ensure, daemon=True, name='bot-db-index-migration').start()

    # Filter cache refresh scheduler — keeps /api/data/history/filters fast.
    threading.Thread(
        target=_filter_cache_scheduler,
        daemon=True,
        name='filter-cache-scheduler',
    ).start()
    # Buffered archive writer (Option B). Source workers push into
    # _archive_buffer via store_incidents_batch(); this single thread
    # drains every ARCHIVE_FLUSH_INTERVAL seconds and is the only caller
    # of _store_incidents_batch_inner — eliminates per-source DB-write
    # contention on prewarm.
    threading.Thread(
        target=_archive_writer_loop,
        daemon=True,
        name='archive-writer',
    ).start()
    # Hydrate the police-heatmap RAM cache from Postgres so the endpoint
    # is hot the moment the backend is up — avoids a "warming" window.
    try:
        _hydrate_police_heatmap_ram_from_db()
    except Exception as e:
        Log.warn(f"Police heatmap RAM hydrate error: {e}")
    # Police heatmap cache refresh scheduler — same pattern.
    threading.Thread(
        target=_police_heatmap_scheduler,
        daemon=True,
        name='police-heatmap-scheduler',
    ).start()
    # Restore the per-bbox Waze ingest cache from Postgres + start the
    # periodic snapshot writer. Same motivation as above: a restart
    # otherwise wipes the userscript-collected bboxes and /api/waze/* only
    # surfaces whichever regions get re-visited in the first few minutes.
    try:
        _hydrate_waze_ingest_cache_from_db()
    except Exception as e:
        Log.warn(f"Waze ingest cache hydrate error: {e}")
    threading.Thread(
        target=_waze_ingest_persist_loop,
        daemon=True,
        name='waze-ingest-persist',
    ).start()
    # Pre-warm the /api/data/history count cache for the logs page
    # default query (hours=24, unique=1, no filters). Without this the
    # first user click after a restart sees a 40s wait or stale-fallback
    # warning. Background thread so it doesn't block startup.
    threading.Thread(
        target=_prewarm_data_history_count_cache,
        daemon=True,
        name='count-cache-prewarm',
    ).start()
    # One-shot VACUUM right after boot to clean up the visibility map left
    # behind by previous archive cycles. The hourly cleanup-loop call covers
    # ongoing maintenance, but the first slow scan after a restart benefits
    # from this immediate pass. Delayed so it doesn't fight the prewarm.
    def _initial_vacuum():
        time.sleep(60)
        _vacuum_data_history()
    threading.Thread(target=_initial_vacuum, daemon=True, name='initial-vacuum').start()


# ==================== PERSISTENT DATA CACHE ====================
# SQLite-backed cache that survives restarts and is pre-warmed in background

def cache_set(endpoint, data, ttl=60, fetch_time_ms=0):
    """Store data in persistent PostgreSQL cache. Also seeds the RAM
    layer so a write is immediately visible to subsequent cache_get()
    callers without a Postgres round-trip."""
    conn = None
    timestamp = int(time.time())
    try:
        with _db_lock_cache:
            conn = get_conn()
            c = conn.cursor()
            c.execute('''
                INSERT INTO api_data_cache (endpoint, data, timestamp, ttl, fetch_time_ms)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (endpoint) DO UPDATE SET data = EXCLUDED.data, timestamp = EXCLUDED.timestamp, ttl = EXCLUDED.ttl, fetch_time_ms = EXCLUDED.fetch_time_ms
            ''', (endpoint, json.dumps(data), timestamp, ttl, fetch_time_ms))
            conn.commit()
        with _CACHE_GET_RAM_LOCK:
            _CACHE_GET_RAM[endpoint] = (data, timestamp, ttl, timestamp)
        return True
    except Exception as e:
        Log.cache(f"Set error for {endpoint}: {e}")
        return False
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass

# In-process RAM layer in front of the Postgres cache. Bot + browser
# poll most endpoints every 30-60s, and each handler calls cache_get()
# 1-2 times — that's 30-50 Postgres SELECTs/min on api_data_cache even
# when nothing changed. A 5-second RAM TTL dedupes rapid repeat polls
# down to a single DB read per endpoint per ~5s, with no impact on
# perceived data freshness (the underlying TTLs are 60s+).
_CACHE_GET_RAM = {}             # endpoint -> (data, db_timestamp, ttl, fetched_at)
_CACHE_GET_RAM_LOCK = threading.Lock()
_CACHE_GET_RAM_TTL = 5          # seconds — short, just dedupe burst traffic
_CACHE_GET_RAM_HITS = 0         # debug counters
_CACHE_GET_RAM_MISSES = 0


def _cache_get_ram_invalidate(endpoint):
    """Drop the RAM entry so the next reader hits Postgres for fresh data."""
    with _CACHE_GET_RAM_LOCK:
        _CACHE_GET_RAM.pop(endpoint, None)


def cache_get(endpoint):
    """
    Get data from persistent cache.
    Returns: (data, age_seconds, is_expired)

    Layered: 5-second in-process RAM cache in front of the Postgres
    cache, so repeated polls for the same endpoint within ~5s share
    a single DB read.
    """
    global _CACHE_GET_RAM_HITS, _CACHE_GET_RAM_MISSES
    now = int(time.time())

    # RAM hit path — instant, no DB.
    with _CACHE_GET_RAM_LOCK:
        ram = _CACHE_GET_RAM.get(endpoint)
        if ram is not None and (now - ram[3]) < _CACHE_GET_RAM_TTL:
            data, db_ts, ttl, _fetched_at = ram
            _CACHE_GET_RAM_HITS += 1
            if data is None:
                return None, 0, True
            age = now - db_ts
            return data, age, age >= ttl

    # RAM miss — fall through to Postgres.
    _CACHE_GET_RAM_MISSES += 1
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('SELECT data, timestamp, ttl FROM api_data_cache WHERE endpoint = %s', (endpoint,))
        row = c.fetchone()
        if row:
            data_str, timestamp, ttl = row
            data = json.loads(data_str)
            age = now - timestamp
            is_expired = age >= ttl
            # Compare-and-swap: only seed RAM if no fresher entry was
            # written by a concurrent cache_set() while we were querying.
            # Without this, a slow reader can clobber a freshly-written
            # value with the older row it just SELECTed.
            with _CACHE_GET_RAM_LOCK:
                existing = _CACHE_GET_RAM.get(endpoint)
                if existing is None or existing[1] <= timestamp:
                    _CACHE_GET_RAM[endpoint] = (data, timestamp, ttl, now)
            return data, age, is_expired
        # Negative cache the miss — but only if no fresher entry was
        # written concurrently. Otherwise we'd poison a real cache_set
        # result with our stale 'None' read.
        with _CACHE_GET_RAM_LOCK:
            existing = _CACHE_GET_RAM.get(endpoint)
            if existing is None or existing[0] is None:
                _CACHE_GET_RAM[endpoint] = (None, 0, 0, now)
        return None, 0, True
    except Exception as e:
        Log.cache(f"Get error for {endpoint}: {e}")
        return None, 0, True
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass

def cache_get_any(endpoint):
    """
    Get data from cache even if expired.
    Returns: (data, age_seconds) or (None, 0)
    """
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('SELECT data, timestamp FROM api_data_cache WHERE endpoint = %s', (endpoint,))
        row = c.fetchone()
        if row:
            return json.loads(row[0]), int(time.time()) - row[1]
        return None, 0
    except Exception as e:
        Log.cache(f"Get any error for {endpoint}: {e}")
        return None, 0
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass

def cache_stats():
    """Get cache statistics for debug endpoint"""
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''
            SELECT endpoint, timestamp, ttl, fetch_time_ms,
                   CASE WHEN (EXTRACT(EPOCH FROM NOW())::bigint - timestamp) < ttl THEN 'fresh' ELSE 'stale' END as status
            FROM api_data_cache
            ORDER BY endpoint
        ''')
        rows = c.fetchall()
        now = int(time.time())
        return [{
            'endpoint': row[0],
            'age_seconds': now - row[1],
            'ttl': row[2],
            'fetch_time_ms': row[3],
            'status': row[4]
        } for row in rows]
    except Exception as e:
        return [{'error': str(e)}]
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass


# ==================== HISTORICAL DATA STORAGE ====================
# Store all fetched data for historical analysis and search

def parse_source_timestamp(timestamp_str):
    """
    Parse various timestamp formats from external sources.
    Returns Unix timestamp (int) or None if parsing fails.
    """
    if not timestamp_str:
        return None
    
    # If it's already a number (milliseconds from epoch)
    if isinstance(timestamp_str, (int, float)):
        # If it looks like milliseconds, convert to seconds
        if timestamp_str > 1e12:
            return int(timestamp_str / 1000)
        return int(timestamp_str)
    
    # Try various string formats
    formats = [
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%a, %d %b %Y %H:%M:%S %z',  # RFC 2822 (RSS pubDate)
        '%a, %d %b %Y %H:%M:%S GMT',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(str(timestamp_str).strip(), fmt)
            return int(dt.timestamp())
        except (ValueError, TypeError):
            pass
    
    # Try ISO format with timezone
    try:
        from email.utils import parsedate_tz, mktime_tz
        parsed = parsedate_tz(str(timestamp_str))
        if parsed:
            return int(mktime_tz(parsed))
    except (ValueError, TypeError, OverflowError):
        pass
    
    return None


def store_incident(source, source_id, data, lat=None, lon=None, location_text=None,
                   title=None, category=None, subcategory=None, status=None, 
                   severity=None, source_timestamp=None, is_active=1):
    """
    Store a single incident/alert in the appropriate history database.
    
    Args:
        source: Data source (rfs, traffic_incident, waze_hazard, etc.)
        source_id: ID from the external API (guid, uuid, etc.)
        data: Full parsed data as dict (will be stored as JSON)
        lat/lon: Coordinates (optional)
        location_text: Human readable location
        title: Main title/headline
        category: Category/type (CRASH, HAZARD, Advice, etc.)
        subcategory: Sub-category if available
        status: Status (active, ended, etc.)
        severity: Severity/alert level
        source_timestamp: Timestamp from source (string or unix)
        is_active: 1 if currently active, 0 if ended
    """
    try:
        fetched_at = int(time.time())
        source_ts_unix = parse_source_timestamp(source_timestamp)
        source_ts_str = str(source_timestamp) if source_timestamp else None
        
        # Get the right database and lock for this source
        db_path = get_history_db_for_source(source)
        db_lock = get_history_lock_for_source(source)
        
        # Get hierarchical source info
        source_provider, source_type_val = get_source_hierarchy(source)
        
        with db_lock:
            conn = get_conn()
            try:
                c = conn.cursor()
                c.execute('''
                    INSERT INTO data_history 
                    (source, source_id, source_provider, source_type, fetched_at, source_timestamp, source_timestamp_unix,
                     latitude, longitude, location_text, title, category, subcategory, 
                     status, severity, data, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    source, source_id, source_provider, source_type_val, fetched_at, source_ts_str, source_ts_unix,
                    lat, lon, location_text, title, category, subcategory,
                    status, severity, json.dumps(data), is_active
                ))
                conn.commit()
            finally:
                conn.close()
        return True
    except Exception as e:
        Log.data(f"Store error for {source}/{source_id}: {e}")
        return False


def _compute_data_hash(inc):
    """
    Compute a hash of the key fields to detect if incident data has ACTUALLY changed.
    
    EXCLUDES timestamps - these change every API fetch even when incident is the same:
    - source_timestamp
    - fetched_at  
    - Any embedded updated/lastUpdated fields
    
    INCLUDES only content that changes when the incident truly changes:
    - title, location, status, category, severity
    """
    # Fields that indicate actual incident changes (not just timestamp updates)
    key_fields = (
        str(inc.get('source') or ''),
        str(inc.get('source_id') or ''),
        str(inc.get('title') or ''),
        str(inc.get('category') or ''),
        str(inc.get('subcategory') or ''),
        str(inc.get('status') or ''),
        str(inc.get('severity') or ''),
        # Round lat/lon to 5 decimal places to avoid float precision issues
        str(round(float(inc.get('lat') or 0), 5)),
        str(round(float(inc.get('lon') or 0), 5)),
        str(inc.get('location_text') or ''),
        str(inc.get('is_active', 1)),
    )
    hash_str = '|'.join(key_fields)
    return hashlib.md5(hash_str.encode()).hexdigest()[:16]


# ==================== BUFFERED ARCHIVE WRITER ====================
# Source workers used to call _store_incidents_batch_inner directly via
# store_incidents_batch, which meant ~9 workers all hitting data_history at
# once on prewarm (per-source DB write latency stacked). Option B routes
# every store_incidents_batch() call through this in-memory buffer, and a
# single dedicated writer thread (`archive-writer`) drains it every
# ARCHIVE_FLUSH_INTERVAL seconds. Within one drain cycle, batches for the
# same source are concatenated into one bigger DB call — fewer round trips,
# and zero lock contention because there is exactly one writer.
#
# Trade-off: up to ARCHIVE_FLUSH_INTERVAL seconds of records can be lost
# on hard crash. Live API endpoints are NOT affected — they read from
# api_data_cache (set synchronously by the prewarm fetch path), not from
# data_history.
ARCHIVE_FLUSH_INTERVAL = int(os.environ.get('ARCHIVE_FLUSH_INTERVAL', '30'))
_ARCHIVE_BUFFER_MAX_RECORDS = 50_000  # hard cap to bound RAM
_archive_buffer = []  # list of (records, source_type) tuples
_archive_buffer_records = 0  # running record count, kept in sync with the list
_archive_buffer_lock = threading.Lock()
# Wall-clock timestamps of the last successful archive-writer drain (any
# records or zero) and process start, exposed by /api/status so external
# monitors can detect a stuck writer thread.
_archive_writer_last_flush_at = 0.0
_PROCESS_START_TIME = time.time()

# Friendly names for archive logging — also used by the writer thread.
_ARCHIVE_NAMES = {
    'rfs': 'RFS',
    'traffic_incident': 'Traffic',
    'traffic_roadwork': 'Roadwork',
    'traffic_flood': 'Floods',
    'traffic_fire': 'Traffic Fire',
    'traffic_majorevent': 'Major Events',
    'waze_hazard': 'Waze Hazards',
    'waze_police': 'Waze Police',
    'waze_roadwork': 'Waze Roadwork',
    'waze_jam': 'Waze Jams',
    'endeavour_current': 'Endeavour',
    'endeavour_planned': 'Endeavour Planned',
    'ausgrid': 'Ausgrid',
    'bom_warning': 'BOM Warnings',
}


def store_incidents_batch(incidents, source_type=None):
    """
    Queue a batch of incidents for the dedicated archive writer thread.

    NOTE (Option B): this no longer writes to data_history synchronously.
    It appends (incidents, source_type) to an in-memory buffer; the
    `archive-writer` thread drains the buffer every ARCHIVE_FLUSH_INTERVAL
    seconds and calls _store_incidents_batch_inner there.

    Returns a synthetic stats dict so existing callers that read
    result['new'] / ['changed'] / ['ended'] / ['unchanged'] don't crash.
    The 'queued': True marker lets callers distinguish a deferred write
    from a synchronous one if they care; nothing today does.

    Args:
        incidents: List of dicts with keys matching store_incident params
        source_type: The source type (e.g., 'rfs', 'traffic_incident')

    Returns:
        dict: {'total': int, 'new': 0, 'changed': 0, 'unchanged': 0,
               'ended': 0, 'queued': True}
    """
    if not incidents:
        return {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0, 'queued': True}

    # Determine source for logging
    src = source_type or (incidents[0].get('source') if incidents else 'unknown')
    name = _ARCHIVE_NAMES.get(src, src)
    n = len(incidents)

    global _archive_buffer_records
    dropped = 0
    with _archive_buffer_lock:
        # Bounded memory: if appending this batch would blow past the cap,
        # drop oldest queued batches first. This is a coarse defense — the
        # writer drains every ARCHIVE_FLUSH_INTERVAL seconds, so under
        # normal load the buffer never gets close to the cap.
        while _archive_buffer and (_archive_buffer_records + n) > _ARCHIVE_BUFFER_MAX_RECORDS:
            old_records, _old_src = _archive_buffer.pop(0)
            dropped += len(old_records)
            _archive_buffer_records -= len(old_records)
        _archive_buffer.append((incidents, source_type))
        _archive_buffer_records += n

    if dropped:
        Log.warn(f"Archive buffer full ({_ARCHIVE_BUFFER_MAX_RECORDS} cap): "
                 f"dropped {dropped} oldest records to make room for {name}")

    # Log archive queueing in dev mode (verbose)
    if DEV_MODE:
        Log.data(f"📥 {name} queued {n} incidents")

    return {'total': n, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0, 'queued': True}


def _archive_writer_drain_once():
    """Swap the buffer out under the lock, group by source, write each group.

    Returns (sources_written, total_records, elapsed_ms). Called by the
    writer loop and once more at shutdown.
    """
    global _archive_buffer_records
    with _archive_buffer_lock:
        if not _archive_buffer:
            return (0, 0, 0)
        pending = _archive_buffer[:]
        _archive_buffer.clear()
        _archive_buffer_records = 0

    # Group by source so two batches for the same source within one cycle
    # become one DB call. Preserve insertion order within a source.
    grouped = {}
    order = []
    for records, src in pending:
        if src is None and records:
            src = records[0].get('source')
        if src not in grouped:
            grouped[src] = []
            order.append(src)
        grouped[src].extend(records)

    start = time.time()
    total_records = 0
    sources_written = 0
    for src in order:
        recs = grouped[src]
        if not recs:
            continue
        try:
            _store_incidents_batch_inner_with_retry(recs, src)
            sources_written += 1
            total_records += len(recs)
        except Exception as e:
            # One bad source must not break the others — log and move on.
            name = _ARCHIVE_NAMES.get(src, src)
            Log.error(f"Archive writer: {name} flush failed: {e}")

    elapsed_ms = int((time.time() - start) * 1000)
    return (sources_written, total_records, elapsed_ms)


def _store_incidents_batch_inner_with_retry(incidents, source_type):
    """Wrap _store_incidents_batch_inner with the same retry-on-deadlock
    logic that store_incidents_batch used to do synchronously. Lives here
    (not in the inner function) so the inner function stays pure-write.
    """
    max_retries = 3
    retry_delay = 1.0
    for attempt in range(max_retries):
        try:
            return _store_incidents_batch_inner(incidents, source_type)
        except psycopg2.OperationalError as e:
            msg = str(e).lower()
            transient = ('locked' in msg or 'deadlock' in msg
                         or 'serialization' in msg)
            if transient and attempt < max_retries - 1:
                Log.warn(f"Archive writer transient DB error, retrying in {retry_delay}s "
                         f"(attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise


def _archive_writer_loop():
    """Dedicated archive writer thread. Drains _archive_buffer every
    ARCHIVE_FLUSH_INTERVAL seconds; one final drain on shutdown."""
    Log.startup(f"Archive writer started (flush every {ARCHIVE_FLUSH_INTERVAL}s, "
                f"buffer cap {_ARCHIVE_BUFFER_MAX_RECORDS:,} records)")
    global _archive_writer_last_flush_at
    while not _shutdown_event.is_set():
        # _shutdown_event.wait returns True when set — exit loop, but still
        # do one last drain below to flush any buffered records.
        if _shutdown_event.wait(timeout=ARCHIVE_FLUSH_INTERVAL):
            break
        try:
            sources, total, elapsed = _archive_writer_drain_once()
            # Stamp the heartbeat even on empty drains — the watchdog wants
            # to know the thread is alive, not whether there was data.
            _archive_writer_last_flush_at = time.time()
            if sources or total:
                Log.data(f"📦 Archive flush: {sources} sources, {total} records [{elapsed}ms]")
        except Exception as e:
            Log.error(f"Archive writer loop error: {e}")

    # Shutdown drain — best effort, don't let an exception swallow the
    # process exit path.
    try:
        sources, total, elapsed = _archive_writer_drain_once()
        if sources or total:
            Log.data(f"📦 Archive flush (shutdown): {sources} sources, {total} records [{elapsed}ms]")
    except Exception as e:
        Log.error(f"Archive writer shutdown drain error: {e}")

def _store_incidents_batch_inner(incidents, source_type=None):
    """Inner function for store_incidents_batch - uses source-specific history DB.

    Option B: this is now invoked only from the dedicated `archive-writer`
    thread (via _store_incidents_batch_inner_with_retry). Source workers
    call store_incidents_batch() which only enqueues into _archive_buffer.
    Because there is exactly one writer, the previous lock-contention
    chokepoint is gone."""
    gone_ids = set()  # Track incidents that are no longer live
    try:
        fetched_at = int(time.time())
        
        # Determine source type from incidents if not provided
        if source_type is None and incidents:
            source_type = incidents[0].get('source')
        
        # Get the right database for this source type. The Python lock that
        # used to wrap this block was removed: with the partial UNIQUE INDEX
        # uniq_data_history_latest (commit ac58dcb) Postgres now enforces
        # "at most one is_latest=1 per (source, source_id)" structurally, so
        # the in-process lock is redundant and was the dominant chokepoint —
        # 9+ source workers serialized through it on restart.
        db_path = get_history_db_for_source(source_type)

        conn = get_conn()
        try:
            c = conn.cursor()

            # First, get the latest hash for each source+source_id we're about to insert
            # Build lookup of existing hashes
            source_ids = [(inc.get('source'), inc.get('source_id')) for inc in incidents if inc.get('source_id')]
            existing_hashes = {}

            if source_ids:
                # Query latest data_hash for each source+source_id
                placeholders = ','.join(['(%s, %s)' for _ in source_ids])
                flat_params = [item for pair in source_ids for item in pair]

                c.execute(f'''
                    SELECT source, source_id, data_hash
                    FROM data_history
                    WHERE (source, source_id) IN ({placeholders})
                    AND id IN (
                        SELECT MAX(id) FROM data_history
                        WHERE (source, source_id) IN ({placeholders})
                        GROUP BY source, source_id
                    )
                ''', flat_params + flat_params)

                for row in c.fetchall():
                    existing_hashes[(row[0], row[1])] = row[2]

            # Filter to only new or changed incidents
            rows_to_insert = []
            all_source_ids_in_batch = set()
            new_count = 0
            changed_count = 0
            skipped_count = 0

            for inc in incidents:
                source = inc.get('source')
                source_id = inc.get('source_id')
                data_hash = _compute_data_hash(inc)

                if source_id:
                    all_source_ids_in_batch.add((source, source_id))

                # Check if this is new or changed
                existing_hash = existing_hashes.get((source, source_id))

                if existing_hash is None:
                    # New incident - insert
                    new_count += 1
                elif existing_hash != data_hash:
                    # Data changed - insert new snapshot
                    changed_count += 1
                else:
                    # Same data - skip insert but still update last_seen
                    skipped_count += 1
                    continue

                source_ts_unix = parse_source_timestamp(inc.get('source_timestamp'))
                source_ts_str = str(inc.get('source_timestamp')) if inc.get('source_timestamp') else None

                # Get hierarchical source info
                src_provider = inc.get('source_provider')
                src_type = inc.get('source_type')
                if not src_provider or not src_type:
                    src_provider, src_type = get_source_hierarchy(source)

                rows_to_insert.append((
                    source,
                    source_id,
                    src_provider,
                    src_type,
                    fetched_at,
                    source_ts_str,
                    source_ts_unix,
                    inc.get('lat'),
                    inc.get('lon'),
                    inc.get('location_text'),
                    inc.get('title'),
                    inc.get('category'),
                    inc.get('subcategory'),
                    inc.get('status'),
                    inc.get('severity'),
                    json.dumps(inc.get('data', {})),
                    inc.get('is_active', 1),
                    1,  # is_live = True (currently in API response)
                    fetched_at,  # last_seen
                    data_hash
                ))

            if rows_to_insert:
                # Mark previous "latest" rows as not-latest for source_ids
                # we're inserting. ONE bulk UPDATE instead of N round
                # trips — for big batches (369 roadwork rows) this was
                # the dominant cost.
                source_ids_to_update = [(r[0], r[1]) for r in rows_to_insert if r[1]]
                if source_ids_to_update:
                    placeholders = ','.join(['(%s,%s)'] * len(source_ids_to_update))
                    flat_params = [v for pair in source_ids_to_update for v in pair]
                    c.execute(
                        f'UPDATE data_history SET is_latest = 0 '
                        f'WHERE is_latest = 1 AND (source, source_id) IN ({placeholders})',
                        flat_params,
                    )

                # Insert new rows with is_latest = 1
                c.executemany('''
                    INSERT INTO data_history
                    (source, source_id, source_provider, source_type, fetched_at, source_timestamp, source_timestamp_unix,
                     latitude, longitude, location_text, title, category, subcategory,
                     status, severity, data, is_active, is_live, last_seen, data_hash, is_latest)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                ''', rows_to_insert)

            # Update last_seen for ALL incidents in this batch (even if
            # data unchanged). Two prior bugs here:
            #   1. Loop of N round trips (one UPDATE per source_id) —
            #      replaced with a single bulk UPDATE.
            #   2. Missing is_latest=1 predicate meant the UPDATE
            #      touched every historical snapshot of each incident
            #      (an incident polled 100 times would have 100 rows
            #      written each tick). Only the is_latest=1 row carries
            #      the live last_seen; older snapshots should stay
            #      frozen at the time they were captured.
            if all_source_ids_in_batch:
                pairs = list(all_source_ids_in_batch)
                placeholders = ','.join(['(%s,%s)'] * len(pairs))
                flat_params = [v for pair in pairs for v in pair]
                c.execute(
                    f'UPDATE data_history '
                    f'SET last_seen = %s, is_live = 1 '
                    f'WHERE is_latest = 1 AND (source, source_id) IN ({placeholders})',
                    [fetched_at] + flat_params,
                )

            # Mark incidents NO LONGER in API response as is_live = 0
            # Only do this if we have a source_type and received at least some data
            #
            # Waze sources are exempt: with userscript ingest the prewarm batch
            # only reflects regions scraped in the last few minutes (a full
            # 129-region rotation takes ~11 min), so running the diff here
            # would flip every non-recently-visited incident to is_live=0 on
            # every cycle. Instead, cleanup_old_data() expires Waze records
            # by `last_seen` age (1h).
            waze_sources = {'waze_hazard', 'waze_police', 'waze_roadwork', 'waze_jam'}
            if source_type in waze_sources:
                pass
            elif source_type and all_source_ids_in_batch:
                # Get all source_ids we know about for this source_type that are still marked as live
                c.execute('''
                    SELECT DISTINCT source_id FROM data_history
                    WHERE source = %s AND is_live = 1 AND source_id IS NOT NULL
                ''', (source_type,))

                known_ids = {row[0] for row in c.fetchall()}
                current_ids = {sid for (src, sid) in all_source_ids_in_batch if src == source_type}

                # Find IDs that are no longer in the API response
                gone_ids = known_ids - current_ids

                if gone_ids:
                    # Mark these as no longer live
                    placeholders = ','.join(['%s' for _ in gone_ids])
                    c.execute(f'''
                        UPDATE data_history
                        SET is_live = 0
                        WHERE source = %s AND source_id IN ({placeholders})
                    ''', [source_type] + list(gone_ids))

                    Log.live(f"Marked {len(gone_ids)} {source_type} incidents as no longer live")

            conn.commit()
        finally:
            conn.close()

        return {
            'total': len(incidents),
            'new': new_count,
            'changed': changed_count,
            'unchanged': skipped_count,
            'ended': len(gone_ids)
        }
    except psycopg2.OperationalError:
        raise  # Let the retry logic handle this
    except Exception as e:
        Log.error(f"DataHistory inner error: {e}")
        return {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}


def cleanup_old_data():
    """
    Delete data older than DATA_RETENTION_DAYS from all history DBs and stats.db.
    Also marks pager hits older than 1 hour as ended (is_live = 0).
    Called periodically by the cleanup thread.
    """
    cutoff = int(time.time()) - (DATA_RETENTION_DAYS * 24 * 60 * 60)
    pager_cutoff = int(time.time()) - 3600  # 1 hour ago for pager hits
    deleted_history = 0
    deleted_stats = 0
    pager_ended = 0
    
    # Clean up history (single table in PostgreSQL)
    # Note: previously this whole block ran under _db_lock_history_waze
    # (which is the master lock). Holding the master lock around a
    # multi-second DELETE serialized every archive writer behind
    # cleanup. Postgres handles the row-level concurrency just fine on
    # its own, so we don't need application-level serialisation here.
    try:
        conn = get_conn()
        try:
            c = conn.cursor()

            # Mark pager hits as ended if older than 1 hour
            c.execute('''
                UPDATE data_history
                SET is_live = 0
                WHERE source = 'pager' AND is_live = 1
                AND COALESCE(source_timestamp_unix, fetched_at) < %s
            ''', (pager_cutoff,))
            pager_ended = c.rowcount
            if pager_ended > 0:
                Log.cleanup(f"Marked {pager_ended} pager hits as ended (>1 hour old)")

            # Waze failover: mark as ended if last_seen older than 1 hour.
            # Per-batch diffing is disabled for Waze (incomplete rotations would
            # flip everything off), so this is the only path that clears stale
            # Waze records from the live set.
            waze_cutoff = int(time.time()) - 3600
            c.execute('''
                UPDATE data_history
                SET is_live = 0
                WHERE source IN ('waze_hazard', 'waze_police', 'waze_roadwork', 'waze_jam')
                AND is_live = 1
                AND COALESCE(last_seen, fetched_at) < %s
            ''', (waze_cutoff,))
            waze_ended = c.rowcount
            if waze_ended > 0:
                Log.cleanup(f"Marked {waze_ended} Waze records as ended (>1 hour since last_seen)")

            # Delete old data_history entries
            c.execute('DELETE FROM data_history WHERE fetched_at < %s', (cutoff,))
            deleted_history += c.rowcount

            # Delete records from deprecated sources
            for dep_src in DEPRECATED_SOURCES:
                c.execute('DELETE FROM data_history WHERE source = %s', (dep_src,))
                if c.rowcount > 0:
                    Log.cleanup(f"Removed {c.rowcount} deprecated {dep_src} records")

            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        Log.error(f"Cleanup history error: {e}")

    # Clean up stats.db
    try:
        with _db_lock_stats:
            conn = get_conn()
            try:
                c = conn.cursor()

                # Delete old stats_snapshots (convert ms to seconds for comparison)
                c.execute('DELETE FROM stats_snapshots WHERE timestamp < %s', (cutoff * 1000,))
                deleted_stats = c.rowcount

                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        Log.error(f"Cleanup stats error: {e}")
    
    total = deleted_history + deleted_stats
    if total > 0:
        Log.cleanup(f"Deleted {deleted_history} history + {deleted_stats} stats (>{DATA_RETENTION_DAYS} days old)", force=True)
    
    return total


def migrate_endeavour_categories():
    """
    One-time migration to fix Endeavour outage categories (history_power.db).
    
    Rules:
    - endeavour_planned (future outages): ALWAYS 'planned' (these are scheduled outages)
    - endeavour_current: Use outageType field ("P"/"Planned" = planned, "U"/"Unplanned" = unplanned)
    """
    planned_source_fixed = 0
    current_planned_fixed = 0
    current_unplanned_fixed = 0
    try:
        with _db_lock_history_power:
            conn = get_conn()
            try:
                c = conn.cursor()

                # Check if migration is needed - look for any Endeavour records
                c.execute("SELECT COUNT(*) FROM data_history WHERE source LIKE 'endeavour%'")
                count = c.fetchone()[0]

                if count == 0:
                    return 0

                # RULE 1: endeavour_planned source = ALWAYS planned
                # These are from get-future-outage API which only contains scheduled/planned outages
                c.execute('''
                    UPDATE data_history
                    SET category = 'planned'
                    WHERE source = 'endeavour_planned'
                    AND category != 'planned'
                ''')
                planned_source_fixed = c.rowcount

                # RULE 2: endeavour_current with outageType = "P" or "Planned" → planned
                c.execute('''
                    UPDATE data_history
                    SET category = 'planned'
                    WHERE source = 'endeavour_current'
                    AND category != 'planned'
                    AND (data::json)->>'outageType' IN ('P', 'Planned')
                ''')
                current_planned_fixed = c.rowcount

                # RULE 3: endeavour_current with outageType = "U", "Unplanned", or null → unplanned
                c.execute('''
                    UPDATE data_history
                    SET category = 'unplanned'
                    WHERE source = 'endeavour_current'
                    AND category != 'unplanned'
                    AND ((data::json)->>'outageType' IN ('U', 'Unplanned') OR (data::json)->>'outageType' IS NULL)
                ''')
                current_unplanned_fixed = c.rowcount

                conn.commit()
            finally:
                conn.close()

        total_fixed = planned_source_fixed + current_planned_fixed + current_unplanned_fixed
        if total_fixed > 0:
            Log.info(f"🔧 Endeavour category migration: endeavour_planned→planned: {planned_source_fixed}, endeavour_current→planned: {current_planned_fixed}, endeavour_current→unplanned: {current_unplanned_fixed}")
        
        return total_fixed
    except Exception as e:
        Log.error(f"Endeavour migration error: {e}")
        return 0


def migrate_bom_sources():
    """
    One-time migration to split bom_warning into bom_land and bom_marine 
    based on the category field or title content in the stored data.
    """
    marine_fixed = 0
    land_fixed = 0
    try:
        with _db_lock_history_weather:
            conn = get_conn()
            try:
                c = conn.cursor()

                # Check if migration is needed
                c.execute("SELECT COUNT(*) FROM data_history WHERE source = 'bom_warning'")
                count = c.fetchone()[0]

                if count == 0:
                    return 0

                Log.info(f"🔧 BOM migration: {count} records to migrate...")

                # Update marine warnings based on category OR subcategory OR title content
                c.execute('''
                    UPDATE data_history
                    SET source = 'bom_marine',
                        source_type = 'Marine'
                    WHERE source = 'bom_warning'
                    AND (
                        category = 'marine'
                        OR subcategory = 'marine'
                        OR LOWER(title) LIKE '%marine%'
                        OR LOWER(title) LIKE '%surf%'
                        OR LOWER(title) LIKE '%hazardous surf%'
                        OR LOWER(title) LIKE '%gale%'
                        OR LOWER(title) LIKE '%wind warning summary%'
                    )
                ''')
                marine_fixed = c.rowcount

                # Update remaining bom_warning records to bom_land (everything else is land)
                c.execute('''
                    UPDATE data_history
                    SET source = 'bom_land',
                        source_type = 'Land'
                    WHERE source = 'bom_warning'
                ''')
                land_fixed = c.rowcount

                conn.commit()
            finally:
                conn.close()

        total_fixed = marine_fixed + land_fixed
        if total_fixed > 0:
            Log.info(f"🔧 BOM source migration complete: {land_fixed} → bom_land, {marine_fixed} → bom_marine")
        
        return total_fixed
    except Exception as e:
        Log.error(f"BOM migration error: {e}")
        return 0


def migrate_bom_subcategories():
    """
    One-time migration to update BOM warning subcategories from 'land'/'marine' 
    to proper warning types (Wind, Flood, Thunderstorm, etc.) based on title.
    """
    updated = 0
    try:
        with _db_lock_history_weather:
            conn = get_conn()
            try:
                c = conn.cursor()

                # Check if migration is needed - look for records with old-style subcategories
                c.execute("""
                    SELECT COUNT(*) FROM data_history
                    WHERE (source = 'bom_land' OR source = 'bom_marine')
                    AND (subcategory = 'land' OR subcategory = 'marine' OR subcategory IS NULL)
                """)
                count = c.fetchone()[0]

                if count == 0:
                    return 0

                Log.info(f"🔧 BOM subcategory migration: {count} records to update...")

                # Get all BOM records that need updating
                c.execute("""
                    SELECT id, title FROM data_history
                    WHERE (source = 'bom_land' OR source = 'bom_marine')
                    AND (subcategory = 'land' OR subcategory = 'marine' OR subcategory IS NULL)
                """)
                records = c.fetchall()

                for record_id, title in records:
                    warning_type = _extract_bom_warning_type(title)
                    c.execute("UPDATE data_history SET subcategory = %s WHERE id = %s", (warning_type, record_id))
                    updated += 1

                conn.commit()
            finally:
                conn.close()

        if updated > 0:
            Log.info(f"🔧 BOM subcategory migration complete: {updated} records updated with proper warning types")
        
        return updated
    except Exception as e:
        Log.error(f"BOM subcategory migration error: {e}")
        return 0


def get_data_history_stats():
    """Get statistics about stored historical data from all history databases"""
    try:
        total = 0
        by_source = {}
        min_ts = None
        max_ts = None
        last_24h = 0
        db_size = 0
        day_ago = int(time.time()) - 86400
        
        # Aggregate from all history databases
        for db_path in ALL_HISTORY_DBS:
            conn = None
            try:
                conn = get_conn()
                c = conn.cursor()

                # Total records
                c.execute('SELECT COUNT(*) FROM data_history')
                total += c.fetchone()[0]

                # Records by source
                c.execute('SELECT source, COUNT(*) FROM data_history GROUP BY source')
                for row in c.fetchall():
                    by_source[row[0]] = by_source.get(row[0], 0) + row[1]

                # Date range
                c.execute('SELECT MIN(fetched_at), MAX(fetched_at) FROM data_history')
                db_min, db_max = c.fetchone()
                if db_min and (min_ts is None or db_min < min_ts):
                    min_ts = db_min
                if db_max and (max_ts is None or db_max > max_ts):
                    max_ts = db_max

                # Records in last 24 hours
                c.execute('SELECT COUNT(*) FROM data_history WHERE fetched_at > %s', (day_ago,))
                last_24h += c.fetchone()[0]

                # Estimate size
                c.execute("SELECT pg_total_relation_size('data_history')")
                db_size += c.fetchone()[0]
            except Exception as e:
                Log.error(f"Stats error for {os.path.basename(db_path)}: {e}")
            finally:
                if conn is not None:
                    try: conn.close()
                    except Exception: pass
        
        # Sort by_source by count descending
        by_source = dict(sorted(by_source.items(), key=lambda x: x[1], reverse=True))
        
        return {
            'total_records': total,
            'by_source': by_source,
            'oldest_record': datetime.fromtimestamp(min_ts).isoformat() if min_ts else None,
            'newest_record': datetime.fromtimestamp(max_ts).isoformat() if max_ts else None,
            'records_last_24h': last_24h,
            'db_size_bytes': db_size,
            'db_size_mb': round(db_size / (1024 * 1024), 2) if db_size else 0,
            'retention_days': DATA_RETENTION_DAYS,
            'databases': len(ALL_HISTORY_DBS)
        }
    except Exception as e:
        return {'error': str(e)}


# Background cleanup thread
_cleanup_thread = None
_cleanup_running = False

# Global shutdown event for graceful termination
_shutdown_event = threading.Event()

def graceful_shutdown(signum, frame):
    """Handle shutdown signals (SIGTERM, SIGINT) gracefully"""
    global _cleanup_running, archive_running, _prewarm_running
    
    sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
    Log.info(f"⚠️ Received {sig_name}, shutting down gracefully...")
    
    # Stop all background threads
    _cleanup_running = False
    archive_running = False
    _prewarm_running = False
    _shutdown_event.set()  # Signal all threads to wake up
    
    # Shut down Central Watch browser worker if running
    try:
        _stop_cw_browser_worker()
    except Exception:
        pass

    # (Waze browser worker removed - userscript ingest replaces it)
    # (Endeavour browser worker removed - now uses Supabase API directly)
    
    # Give threads a moment to exit their loops
    time.sleep(0.5)
    
    Log.info("✅ Shutdown complete")
    
    # Use os._exit() to forcibly terminate - sys.exit() won't work if threads
    # are blocked on locks or HTTP requests
    os._exit(0)

def _cleanup_expired_cache():
    """Remove expired entries from the global cache dict to prevent unbounded growth"""
    now = time.time()
    expired = [k for k, v in cache.items() if now - v['time'] >= v.get('ttl', CACHE_TTL)]
    for k in expired:
        del cache[k]
    if expired and DEV_MODE:
        Log.cleanup(f"Cache cleanup: evicted {len(expired)} expired entries, {len(cache)} remaining")

def _cleanup_role_cache():
    """Remove expired entries from _role_cache to prevent unbounded growth"""
    now = time.time()
    expired = [k for k, v in _role_cache.items() if now - v['ts'] >= _role_cache_ttl]
    for k in expired:
        del _role_cache[k]
    if expired and DEV_MODE:
        Log.cleanup(f"Role cache cleanup: evicted {len(expired)} expired entries, {len(_role_cache)} remaining")

def _vacuum_data_history():
    """VACUUM (ANALYZE) data_history to reclaim dead tuples left behind by
    archive UPDATEs and cleanup DELETEs. Without this the table grows a
    visibility-map gap that makes index-only scans fall back to heap
    fetches, which is what makes COUNT(*) slow enough to time out.
    Plain VACUUM (no FULL) doesn't lock the table — readers and writers
    keep working through it."""
    try:
        import psycopg2
        dsn = os.environ.get('DATABASE_URL', '')
        if not dsn:
            return
        # VACUUM has to run outside a transaction; open a dedicated
        # autocommit connection for it.
        conn = psycopg2.connect(dsn)
        try:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            t0 = time.time()
            cur.execute('VACUUM (ANALYZE) data_history')
            cur.close()
            if DEV_MODE:
                Log.cleanup(f"VACUUM (ANALYZE) data_history complete [{int((time.time()-t0)*1000)}ms]")
        finally:
            conn.close()
    except Exception as e:
        Log.warn(f"VACUUM data_history skipped: {e}")


def cleanup_loop():
    """Background loop that periodically cleans up old data"""
    global _cleanup_running
    cleanup_counter = 0
    while _cleanup_running:
        cleanup_old_data()
        cleanup_stale_sessions()
        _cleanup_centralwatch_image_cache()
        _cleanup_expired_cache()
        cleanup_counter += 1
        # Clean up rate limit data every 5 cycles
        if cleanup_counter % 5 == 0:
            _cleanup_rate_limits()
            _cleanup_role_cache()
        # VACUUM data_history hourly. Without this, archive UPDATEs leave
        # the visibility map dirty and index-only scans regress to heap
        # fetches, which is what makes /api/data/history COUNT(*) time out.
        # DATA_CLEANUP_INTERVAL is 5 min by default, so every 12 cycles ≈ 1h.
        if cleanup_counter % 12 == 0:
            threading.Thread(
                target=_vacuum_data_history,
                daemon=True,
                name='vacuum-data-history',
            ).start()
        # Use short sleeps to allow faster shutdown
        if _shutdown_event.wait(timeout=DATA_CLEANUP_INTERVAL):
            break  # Shutdown signal received

def start_cleanup_thread():
    """Start the data cleanup background thread"""
    global _cleanup_thread, _cleanup_running
    if _cleanup_thread is None or not _cleanup_thread.is_alive():
        _cleanup_running = True
        _cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
        _cleanup_thread.start()
        Log.startup(f"Cleanup thread started (retention: {DATA_RETENTION_DAYS} days)")


def archive_current_stats():
    """Fetch current stats and archive them to the database"""
    conn = None
    try:
        # Get aggregated stats (reuse the stats_summary logic)
        stats = collect_stats_for_archive()
        timestamp = int(time.time() * 1000)  # JS-compatible timestamp

        with _db_lock_stats:
            conn = get_conn()
            c = conn.cursor()

            # Insert or replace stats snapshot
            c.execute('''
                INSERT INTO stats_snapshots (timestamp, data)
                VALUES (%s, %s)
                ON CONFLICT (timestamp) DO UPDATE SET data = EXCLUDED.data
            ''', (timestamp, json.dumps(stats)))

            conn.commit()

        if DEV_MODE:
            Log.data(f"Archived stats snapshot")
        return True
    except Exception as e:
        Log.error(f"Archive error: {e}")
        return False
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass

def collect_stats_for_archive():
    """Collect all stats for archival (matches frontend statsHistory structure)"""
    stats = {
        'power': {
            'ausgrid': {'unplanned': 0, 'planned': 0, 'customers': 0},
            'endeavour': {'current': 0, 'maintenance': 0, 'current_active': 0, 'future': 0, 'customers': 0},
            'essential': {'unplanned': 0, 'planned': 0, 'future': 0, 'total': 0, 'customers': 0}
        },
        'traffic': {
            'total': 0,
            'crashes': 0,
            'hazards': 0,
            'breakdowns': 0,
            'changed_conditions': 0,
            'roadwork': 0,
            'fires': 0,
            'floods': 0,
            'major_events': 0
        },
        'emergency': {
            'rfs': 0,
            'rfs_emergency': 0,
            'rfs_watch': 0,
            'rfs_advice': 0
        },
        'bom': {
            'land': 0,
            'marine': 0
        },
        'environment': {
            'beaches': 0,
            'goodQuality': 0,
            'poorQuality': 0
        }
    }
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # Ausgrid
    try:
        r = requests.get('https://www.ausgrid.com.au/webapi/outagemapdata/GetCurrentOutageStats', 
                        timeout=8, headers=headers)
        if r.status_code == 200:
            ad = r.json()
            stats['power']['ausgrid']['unplanned'] = (ad.get('OutageUnplannedPdoCount', 0) or 0) + (ad.get('OutageUnplannedRdoCount', 0) or 0)
            stats['power']['ausgrid']['planned'] = ad.get('OutagePlannedTodayCount', 0) or 0
            stats['power']['ausgrid']['customers'] = (ad.get('OutageUnplannedPdoCustomers', 0) or 0) + (ad.get('OutageUnplannedRdoCustomers', 0) or 0)
    except Exception as e:
        Log.error(f"Archive - Ausgrid error: {e}")
    
    # Endeavour - use Supabase API
    try:
        # Use Supabase statistics RPC for quick stats
        endeavour_stats = _fetch_endeavour_supabase('/rpc/get_outage_statistics', method='POST', body={})
        if endeavour_stats:
            stats['power']['endeavour']['current_active'] = endeavour_stats.get('active_outages', 0)
            stats['power']['endeavour']['customers'] = endeavour_stats.get('total_affected_customers', 0)
        
        # Get counts from cache if available
        cached_current, _, _ = cache_get('endeavour_current')
        cached_maintenance, _, _ = cache_get('endeavour_maintenance')
        cached_future, _, _ = cache_get('endeavour_future')
        if cached_current and isinstance(cached_current, list):
            stats['power']['endeavour']['current'] = len(cached_current)
        if cached_maintenance and isinstance(cached_maintenance, list):
            stats['power']['endeavour']['maintenance'] = len(cached_maintenance)
        if cached_future and isinstance(cached_future, list):
            stats['power']['endeavour']['future'] = len(cached_future)
    except Exception as e:
        Log.error(f"Archive - Endeavour error: {e}")
    
    # Essential Energy - use cached data from prewarm (all 3 feeds)
    try:
        cached_essential, _, _ = cache_get('essential_energy')
        if cached_essential and isinstance(cached_essential, list):
            stats['power']['essential']['total'] += len(cached_essential)
            stats['power']['essential']['unplanned'] = sum(1 for o in cached_essential if o.get('outageType') == 'unplanned')
            stats['power']['essential']['planned'] = sum(1 for o in cached_essential if o.get('outageType') == 'planned')
            stats['power']['essential']['customers'] += sum(o.get('customersAffected', 0) for o in cached_essential)
        
        cached_future, _, _ = cache_get('essential_energy_future')
        if cached_future and isinstance(cached_future, list):
            stats['power']['essential']['future'] = len(cached_future)
            stats['power']['essential']['total'] += len(cached_future)
            stats['power']['essential']['customers'] += sum(o.get('customersAffected', 0) for o in cached_future)
    except Exception as e:
        Log.error(f"Archive - Essential Energy error: {e}")
    
    # Traffic
    traffic_urls = {
        'incidents': 'https://www.livetraffic.com/traffic/hazards/incident.json',
        'roadwork': 'https://www.livetraffic.com/traffic/hazards/roadwork.json',
        'fires': 'https://www.livetraffic.com/traffic/hazards/fire.json',
        'floods': 'https://www.livetraffic.com/traffic/hazards/flood.json',
        'major_events': 'https://www.livetraffic.com/traffic/hazards/majorevent.json',
    }
    
    for key, url in traffic_urls.items():
        try:
            r = requests.get(url, timeout=8, headers=headers)
            if r.status_code == 200:
                data = r.json()
                features = data.get('features', []) if isinstance(data, dict) else data
                # Filter out ended incidents
                active = [f for f in features if not f.get('properties', f).get('ended', False)]
                
                if key == 'incidents':
                    # Categorize incidents with flexible matching
                    for f in active:
                        props = f.get('properties', f)
                        main_cat = (props.get('mainCategory', '') or '').upper()
                        sub_cat = (props.get('subCategory', '') or '').upper()
                        headline = (props.get('headline', '') or '').upper()
                        display_name = (props.get('displayName', '') or '').upper()
                        all_text = f"{main_cat} {sub_cat} {headline} {display_name}"
                        
                        if 'CRASH' in all_text or 'COLLISION' in all_text or 'ROLLOVER' in all_text:
                            stats['traffic']['crashes'] += 1
                        elif 'BREAKDOWN' in all_text or 'BROKEN DOWN' in all_text or 'DISABLED' in all_text:
                            stats['traffic']['breakdowns'] += 1
                        elif 'HAZARD' in all_text or 'DEBRIS' in all_text or 'OBSTRUCTION' in all_text or 'ANIMAL' in all_text:
                            stats['traffic']['hazards'] += 1
                        elif 'CHANGED TRAFFIC' in all_text:
                            stats['traffic']['changed_conditions'] += 1
                    # total = incidents only (matches frontend stat card)
                    stats['traffic']['total'] = len(active)
                else:
                    stats['traffic'][key] = len(active)
        except Exception as e:
            Log.error(f"Archive - Traffic {key} error: {e}")
    
    # RFS with alert level breakdown
    try:
        r = requests.get('https://www.rfs.nsw.gov.au/feeds/majorIncidents.xml', timeout=8, headers=headers)
        if r.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.content)
            items = root.findall('.//item')
            stats['emergency']['rfs'] = len(items)
            
            for item in items:
                category = (item.findtext('category', '') or '').lower()
                if 'emergency' in category:
                    stats['emergency']['rfs_emergency'] += 1
                elif 'watch' in category:
                    stats['emergency']['rfs_watch'] += 1
                else:
                    stats['emergency']['rfs_advice'] += 1
    except Exception as e:
        Log.error(f"Archive - RFS error: {e}")
    
    # BOM warnings count - save separately for each type
    bom_urls = {
        'land': 'https://www.bom.gov.au/fwo/IDZ00061.warnings_land_nsw.xml',
        'marine': 'https://www.bom.gov.au/fwo/IDZ00068.warnings_marine_nsw.xml'
    }
    
    for bom_type, url in bom_urls.items():
        try:
            r = requests.get(url, timeout=8, headers=headers)
            if r.status_code == 200:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(r.content)
                # BOM uses RSS format with <item> elements
                warning_count = len(root.findall('.//item'))
                # Also try <warning> elements for legacy format
                if warning_count == 0:
                    warning_count = len(root.findall('.//warning'))
                stats['bom'][bom_type] = warning_count
        except Exception as e:
            Log.error(f"Archive - BOM {bom_type} error: {e}")
    
    # Beaches - track quality distribution
    try:
        r = requests.get('https://api.beachwatch.nsw.gov.au/public/sites/geojson', timeout=8, headers=headers)
        if r.status_code == 200:
            features = r.json().get('features', [])
            stats['environment']['beaches'] = len(features)
            for f in features:
                quality = (f.get('properties', {}).get('latestResult', '') or '').lower()
                if quality in ['bad', 'poor']:
                    stats['environment']['poorQuality'] += 1
                elif quality in ['good', 'excellent']:
                    stats['environment']['goodQuality'] += 1
    except Exception as e:
        Log.error(f"Archive - Beachwatch error: {e}")
    
    return stats

def cleanup_stale_sessions():
    """Remove page sessions that haven't sent a heartbeat recently"""
    now = time.time()
    with _page_sessions_lock:
        stale_sessions = [
            (page_id, session) for page_id, session in active_page_sessions.items()
            if now - session['last_seen'] > PAGE_SESSION_TIMEOUT
        ]
        for page_id, _ in stale_sessions:
            active_page_sessions.pop(page_id, None)
        total = len(active_page_sessions)
        data = sum(1 for s in active_page_sessions.values() if s.get('is_data_page', False))

    # Log outside the lock to minimise hold time
    for page_id, _ in stale_sessions:
        short_id = page_id[-6:] if len(page_id) > 6 else page_id
        Log.cleanup(f"Session expired: ...{short_id}")
    if stale_sessions and not DEV_MODE:
        Log.cleanup(f"{len(stale_sessions)} session(s) expired (viewers: {total}, data: {data})")

    return len(stale_sessions)

def get_active_page_count():
    """Get count of all active page sessions after cleaning stale sessions"""
    cleanup_stale_sessions()
    with _page_sessions_lock:
        return len(active_page_sessions)

def get_data_page_count():
    """Get count of active DATA pages (pages that fetch live data)"""
    cleanup_stale_sessions()
    with _page_sessions_lock:
        return sum(1 for s in active_page_sessions.values() if s.get('is_data_page', False))

# Track last known mode for immediate mode change logging
_last_mode = None

def _check_mode_change():
    """Log mode change immediately when data page count changes"""
    global _last_mode
    data_count = get_data_page_count()
    current_mode = "active" if data_count > 0 else "idle"
    
    if _last_mode is not None and current_mode != _last_mode:
        total_count = get_active_page_count()
        mode_icon = "🟢" if current_mode == "active" else "🔴"
        Log.info(f"{mode_icon} {current_mode.upper()}: {total_count} viewers")
    
    _last_mode = current_mode

def is_page_active():
    """Check if any DATA page is currently active (triggers active collection mode)"""
    global last_heartbeat
    # Only data pages trigger active mode
    if get_data_page_count() > 0:
        return True
    return (time.time() - last_heartbeat) < HEARTBEAT_TIMEOUT

def identify_request_source(request):
    """Identify if request is from Discord bot, browser, or other service"""
    user_agent = request.headers.get('User-Agent', '').lower()
    
    # Check for custom header from Discord bot
    if request.headers.get('X-Client-Type') == 'discord-bot':
        return 'discord-bot'
    
    # Check User-Agent patterns
    if 'python' in user_agent or 'aiohttp' in user_agent or 'httpx' in user_agent:
        return 'bot-client'
    if 'discordbot' in user_agent:
        return 'discord-bot'
    if 'curl' in user_agent or 'wget' in user_agent:
        return 'cli-tool'
    if any(browser in user_agent for browser in ['mozilla', 'chrome', 'safari', 'firefox', 'edge']):
        return 'browser'
    
    return 'unknown'

def log_request(endpoint, source_type, extra_info=''):
    """Log request with source identification"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    source_icons = {
        'browser': '🌐',
        'discord-bot': '🤖',
        'bot-client': '🔧',
        'cli-tool': '💻',
        'unknown': '❓'
    }
    icon = source_icons.get(source_type, '❓')
    extra = f" - {extra_info}" if extra_info else ""
    print(f"{icon} [{timestamp}] {source_type}: {endpoint}{extra}")

def get_current_interval():
    """Get the current collection interval based on page activity"""
    return ACTIVE_INTERVAL if is_page_active() else IDLE_INTERVAL

def archive_loop():
    """Background loop that archives stats with adaptive intervals"""
    global archive_running
    last_collect_time = 0
    last_status = None
    
    while archive_running:
        current_interval = get_current_interval()
        time_since_last = time.time() - last_collect_time
        
        if time_since_last >= current_interval:
            archive_current_stats()
            last_collect_time = time.time()
            status = "active" if is_page_active() else "idle"
            total_count = get_active_page_count()
            data_count = get_data_page_count()
            
            if DEV_MODE:
                Log.data(f"Archived ({status}, {total_count} viewers, {data_count} data)")
            
            # Log mode changes (active/idle) in production
            if status != last_status:
                mode_icon = "🟢" if status == "active" else "🔴"
                Log.info(f"{mode_icon} {status.upper()}: {total_count} viewers")
            last_status = status
        
        # Use short sleeps to allow faster shutdown
        if _shutdown_event.wait(timeout=10):
            break  # Shutdown signal received

def start_archive_thread():
    """Start the background archival thread"""
    global archive_thread, archive_running
    if archive_thread is None or not archive_thread.is_alive():
        archive_running = True
        # Kick off the loop — it runs archive_current_stats() on its first iteration
        # (last_collect_time starts at 0). Doing it inline here blocks startup for
        # ~30s on external HTTP, so keep it async.
        archive_thread = threading.Thread(target=archive_loop, daemon=True)
        archive_thread.start()
        Log.startup("Archive thread started")

def stop_archive_thread():
    """Stop the background archival thread"""
    global archive_running
    archive_running = False
    Log.info("Archive thread stopped")


# ==================== CACHE PRE-WARMING SYSTEM ====================
# Background thread that keeps all API caches warm so frontend requests are instant.
# Fetches data from external APIs periodically and stores in SQLite cache.

_prewarm_thread = None
_prewarm_running = False

# Prewarm configuration: (cache_key, ttl_seconds, refresh_interval_seconds)
# TTL = how long data is considered fresh
# Refresh interval = how often to fetch new data (should be <= TTL)
# Prewarm intervals: (cache_key, ttl_seconds)
# Actual refresh interval is determined dynamically by is_page_active()
# - Active mode (someone on map): 60 seconds (1 min)
# - Idle mode (no one on page): 120 seconds (2 min)
PREWARM_ACTIVE_INTERVAL = 60   # 1 minute when map page is open
PREWARM_IDLE_INTERVAL = 120    # 2 minutes when no one on page

PREWARM_CONFIG = [
    # All endpoints - TTL should be longer than refresh interval
    ('rfs_incidents', 180),
    ('traffic_incidents', 180),
    ('traffic_cameras', 180),
    ('traffic_roadwork', 180),
    ('traffic_flood', 180),
    ('traffic_fire', 180),
    ('traffic_majorevent', 180),
    ('waze_hazards', 180),
    ('waze_police', 180),
    ('waze_roadwork', 180),
    ('aviation_cameras', 300),
    ('endeavour_current', 180),
    ('endeavour_maintenance', 180),
    ('endeavour_future', 300),
    ('ausgrid_outages', 180),
    ('essential_energy', 300),
    ('essential_energy_future', 300),
    ('pager', 180),
    ('ausgrid_stats', 180),
    ('beachwatch', 300),
    ('beachsafe', 300),
    ('beachsafe_details', 600),
    ('weather_current', 300),
    ('bom_warnings', 300),
    # Central Watch removed from prewarm - rate limited too aggressively, fetched separately
]


# -----------------------------------------------------------------------------
# Prewarm persistent-failure backoff
# -----------------------------------------------------------------------------
# After PREWARM_BACKOFF_THRESHOLD consecutive failures of the same cache key
# (exception or None return), park it for progressively longer intervals so we
# stop spamming a broken upstream every cycle. e.g. NSW Police RSS 403s every
# minute; backoff parks it after 3 fails for 5min→15min→30min→1h→1h...
PREWARM_BACKOFF_THRESHOLD = 3
PREWARM_BACKOFF_STEPS = [300, 900, 1800, 3600]  # 5m, 15m, 30m, 1h (then plateau)

_prewarm_fail_counts = {}       # cache_key -> int (consecutive fails)
_prewarm_backoff_until = {}     # cache_key -> unix ts (resume fetches after)
_prewarm_backoff_lock = threading.Lock()


def _format_cycle_summary(outcomes, stats, elapsed_ms, initial=False):
    """Single-line summary for a full or partial prewarm cycle."""
    label = "Initial fetch" if initial else "Cycle"
    parts = [f"{label}: {outcomes.get('success', 0)}✓"]
    if outcomes.get('failed', 0):
        parts.append(f"{outcomes['failed']}✗")
    if outcomes.get('skipped', 0):
        parts.append(f"{outcomes['skipped']}↩")
    change_bits = []
    if stats.get('new', 0):
        change_bits.append(f"+{stats['new']}")
    if stats.get('changed', 0):
        change_bits.append(f"Δ{stats['changed']}")
    if stats.get('ended', 0):
        change_bits.append(f"✗{stats['ended']}")
    tail = f" ({' '.join(change_bits)})" if change_bits else ""
    return f"{' '.join(parts)}{tail} [{elapsed_ms}ms]"


def _prewarm_in_backoff(cache_key):
    """Return seconds remaining if cache_key is parked, else 0."""
    with _prewarm_backoff_lock:
        until = _prewarm_backoff_until.get(cache_key, 0.0)
    remaining = until - time.time()
    return remaining if remaining > 0 else 0


def _prewarm_record_success(cache_key, name):
    """Clear fail state. If we were parked, log the recovery."""
    with _prewarm_backoff_lock:
        had_fails = _prewarm_fail_counts.get(cache_key, 0) > 0
        was_parked = _prewarm_backoff_until.get(cache_key, 0.0) > 0
        _prewarm_fail_counts.pop(cache_key, None)
        _prewarm_backoff_until.pop(cache_key, None)
    if had_fails and was_parked:
        Log.prewarm(f"{name}: ✓ recovered")


def _prewarm_record_failure(cache_key, name, reason):
    """Increment fail counter; apply exponential backoff once past threshold."""
    with _prewarm_backoff_lock:
        n = _prewarm_fail_counts.get(cache_key, 0) + 1
        _prewarm_fail_counts[cache_key] = n
        entered_backoff_for = 0
        if n >= PREWARM_BACKOFF_THRESHOLD:
            step_idx = min(n - PREWARM_BACKOFF_THRESHOLD, len(PREWARM_BACKOFF_STEPS) - 1)
            entered_backoff_for = PREWARM_BACKOFF_STEPS[step_idx]
            _prewarm_backoff_until[cache_key] = time.time() + entered_backoff_for
    if entered_backoff_for:
        mins = entered_backoff_for // 60
        Log.prewarm(f"{name}: ✗ {reason} — backoff {mins}m (fail #{n})")
    elif DEV_MODE:
        Log.prewarm(f"{name}: ✗ {reason} ({n}/{PREWARM_BACKOFF_THRESHOLD})")



def _prewarm_fetch_rfs():
    """Fetch and parse RFS incidents for cache"""
    features = []
    try:
        r = requests.get('https://www.rfs.nsw.gov.au/feeds/majorIncidents.xml',
                        timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            source_error('rfs', f'HTTP {r.status_code}')
        if r.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.content)
            ns = {'georss': 'http://www.georss.org/georss'}
            
            for item in root.findall('.//item'):
                point = item.find('.//{http://www.georss.org/georss}point')
                if point is not None and point.text:
                    try:
                        coords = point.text.strip().split()
                        if len(coords) >= 2:
                            lat, lon = float(coords[0]), float(coords[1])
                            desc = item.findtext('description', '')
                            parsed = parse_rfs_description(desc)
                            category = item.findtext('category', '')
                            if not parsed['alertLevel'] and category:
                                parsed['alertLevel'] = category
                            
                            # Extract polygon coordinates for fire boundaries
                            polygons = []
                            for poly in item.findall('.//{http://www.georss.org/georss}polygon'):
                                if poly.text:
                                    polygons.append(poly.text.strip())
                            
                            features.append({
                                'type': 'Feature',
                                'geometry': {'type': 'Point', 'coordinates': [lon, lat]},
                                'properties': {
                                    'title': item.findtext('title', ''),
                                    'link': item.findtext('link', ''),
                                    'guid': item.findtext('guid', ''),
                                    'description': re.sub(r'<[^>]+>', ' ', desc).strip(),
                                    'status': parsed['status'],
                                    'location': parsed['location'],
                                    'size': parsed['size'],
                                    'alertLevel': parsed['alertLevel'],
                                    'fireType': parsed['fireType'],
                                    'councilArea': parsed['councilArea'],
                                    'responsibleAgency': parsed['responsibleAgency'],
                                    # NOTE: pubDate is feed generation time (same for all items), NOT incident creation
                                    # The only incident-specific time is 'updated' from the description
                                    'updated': parsed['updated'],       # Display format: "7 Jan 2026 13:35"
                                    'updatedISO': parsed['updatedISO'], # ISO format: "2026-01-07T13:35:00+11:00"
                                    'polygons': polygons,  # Fire boundary polygons
                                    'source': 'rfs'
                                }
                            })
                    except (KeyError, TypeError, ValueError) as e:
                        pass
            source_ok('rfs')
    except Exception as e:
        source_error('rfs', e)
        Log.prewarm(f"RFS error: {e}")
    return {'type': 'FeatureCollection', 'features': features, 'count': len(features)}


def _prewarm_fetch_traffic(hazard_type):
    """Fetch and parse traffic data for cache"""
    url_map = {
        'incidents': 'https://www.livetraffic.com/traffic/hazards/incident.json',
        'roadwork': 'https://www.livetraffic.com/traffic/hazards/roadwork.json',
        'flood': 'https://www.livetraffic.com/traffic/hazards/flood.json',
        'fire': 'https://www.livetraffic.com/traffic/hazards/fire.json',
        'majorevent': 'https://www.livetraffic.com/traffic/hazards/majorevent.json',
    }
    # Map hazard_type → registry name for source-health tracking
    _src_name_map = {
        'incidents': 'traffic_incidents', 'roadwork': 'traffic_roadwork',
        'flood': 'traffic_flood', 'fire': 'traffic_fire',
        'majorevent': 'traffic_major',
    }
    src_name = _src_name_map.get(hazard_type)

    features = []
    try:
        r = requests.get(url_map[hazard_type], timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get('features', [])
            for item in items:
                props = item.get('properties', item)
                if props.get('ended', False):
                    continue
                feature = parse_traffic_item(item, hazard_type.title())
                if feature:
                    features.append(feature)
            if src_name:
                source_ok(src_name)
        else:
            if src_name:
                source_error(src_name, f'HTTP {r.status_code}')
    except Exception as e:
        if src_name:
            source_error(src_name, e)
        Log.prewarm(f"Traffic {hazard_type} error: {e}")
    return {'type': 'FeatureCollection', 'features': features, 'count': len(features)}


def _prewarm_fetch_traffic_cameras():
    """Fetch and parse traffic cameras for cache"""
    features = []
    try:
        r = requests.get('https://www.livetraffic.com/datajson/all-feeds-web.json',
                        timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            for item in (data if isinstance(data, list) else []):
                event_type = item.get('eventType', '').lower()
                event_category = item.get('eventCategory', '').lower()
                if 'livecam' not in event_type and 'livecam' not in event_category:
                    continue
                geometry = item.get('geometry', {})
                if geometry.get('type') != 'Point':
                    continue
                coords = geometry.get('coordinates', [])
                if len(coords) < 2:
                    continue
                props = item.get('properties', {})
                features.append({
                    'type': 'Feature',
                    'geometry': {'type': 'Point', 'coordinates': coords},
                    'properties': {
                        'id': item.get('id', ''),
                        'title': props.get('title', 'Traffic Camera'),
                        'view': props.get('view', ''),
                        'direction': props.get('direction', ''),
                        'region': props.get('region', ''),
                        'imageUrl': props.get('href', ''),
                        'source': 'livetraffic_cameras'
                    }
                })
    except Exception as e:
        Log.prewarm(f"Traffic cameras error: {e}")
    return {'type': 'FeatureCollection', 'features': features, 'count': len(features)}


def _prewarm_fetch_aviation_cameras():
    """Fetch and parse aviation cameras for cache"""
    features = []
    try:
        nonce = get_airservices_nonce()
        r = requests.get(
            'https://weathercams.airservicesaustralia.com/wp-admin/admin-ajax.php',
            params={'action': 'get_airports_list', 'filter': 'all', 'type': 'map',
                   'filter_type': 'normal', 'nonce': nonce},
            timeout=15, headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            data = r.json()
            for airport in data.get('airport_list', []):
                lat_str, lon_str = airport.get('lat', ''), airport.get('long', '')
                if lat_str and lon_str:
                    try:
                        features.append({
                            'type': 'Feature',
                            'geometry': {'type': 'Point', 'coordinates': [float(lon_str), float(lat_str)]},
                            'properties': {
                                'id': airport.get('id', ''),
                                'title': airport.get('title', 'Airport Camera'),
                                'name': airport.get('name', ''),
                                'state': airport.get('state', ''),
                                'state_full': airport.get('state_full', ''),
                                'link': airport.get('link', ''),
                                'imageUrl': airport.get('thumbnail', ''),
                                'source': 'airservices_australia'
                            }
                        })
                    except (KeyError, TypeError, ValueError) as e:
                        pass
    except Exception as e:
        Log.prewarm(f"Aviation cameras error: {e}")
    return {'type': 'FeatureCollection', 'features': features, 'count': len(features)}


def _prewarm_fetch_centralwatch_cameras():
    """Fetch and parse Central Watch fire tower cameras for cache"""
    cameras = []
    
    # Retry logic for rate limiting
    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = requests.get(
                'https://centralwatch.watchtowers.io/au/api/cameras',
                timeout=20,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json',
                    'Accept-Language': 'en-AU,en;q=0.9',
                    'Referer': 'https://centralwatch.watchtowers.io/au'
                }
            )
            
            Log.prewarm(f"Central Watch: HTTP {r.status_code}, Content-Type: {r.headers.get('Content-Type', 'unknown')}")
            
            if r.status_code == 200:
                try:
                    data = r.json()
                    # Log raw response structure for debugging
                    if isinstance(data, list):
                        cameras = data
                        Log.prewarm(f"Central Watch: Got {len(cameras)} cameras (array)")
                        if cameras and len(cameras) > 0:
                            # Log first camera's keys for debugging
                            Log.prewarm(f"Central Watch: First camera keys: {list(cameras[0].keys()) if isinstance(cameras[0], dict) else 'not a dict'}")
                    elif isinstance(data, dict):
                        # Log dict keys for debugging
                        Log.prewarm(f"Central Watch: Response is dict with keys: {list(data.keys())}")
                        # Try various common key names
                        cameras = data.get('cameras', data.get('data', data.get('features', data.get('items', []))))
                        if isinstance(cameras, list):
                            Log.prewarm(f"Central Watch: Got {len(cameras)} cameras (from object)")
                    else:
                        Log.prewarm(f"Central Watch: Unexpected data type: {type(data)}")
                except Exception as e:
                    Log.prewarm(f"Central Watch: JSON parse error: {e}, raw: {r.text[:200]}")
                break
            elif r.status_code == 429:
                wait_time = (attempt + 1) * 5  # 5, 10, 15 seconds
                Log.prewarm(f"Central Watch: Rate limited (429), waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
            else:
                Log.prewarm(f"Central Watch: HTTP {r.status_code}, Body: {r.text[:200]}")
                break
        except requests.exceptions.Timeout:
            Log.prewarm(f"Central Watch: Timeout (attempt {attempt + 1}/{max_retries})")
        except Exception as e:
            Log.prewarm(f"Central Watch cameras error: {e}")
            break
    
    return cameras


def _classify_waze_alert(alert):
    """Decide whether an alert is police / roadwork / neither.

    Roadwork on Waze's live map comes through several shapes, not just
    type=CONSTRUCTION. The filter has to check type, subtype, reportDescription,
    and provider together, otherwise most roadwork gets miscategorised as a
    generic hazard.

    Returns one of: 'police', 'roadwork', None.
    """
    alert_type = (alert.get('type') or '').upper()
    subtype_upper = (alert.get('subtype') or '').upper()
    desc_upper = (alert.get('reportDescription') or '').upper()
    provider_upper = (alert.get('provider') or '').upper()

    if alert_type == 'POLICE' or 'POLICE' in subtype_upper:
        return 'police'

    # type-level roadwork
    if alert_type == 'CONSTRUCTION':
        return 'roadwork'
    # subtype-level: HAZARD_ON_ROAD_CONSTRUCTION, ROAD_CLOSED_CONSTRUCTION, etc.
    if 'CONSTRUCTION' in subtype_upper:
        return 'roadwork'
    # Lane closures are almost always roadwork in practice
    if subtype_upper == 'HAZARD_ON_ROAD_LANE_CLOSED':
        return 'roadwork'
    # Government/LGA roadwork feeds: Waze surfaces scheduled closures via
    # providers named "NSW Australia_Waze Planned" and similar.
    if 'WAZE PLANNED' in provider_upper:
        return 'roadwork'
    # reportDescription text clues (LGA partners, EE Feed workers, etc.)
    if 'ROADWORK' in desc_upper or 'ROAD WORK' in desc_upper:
        return 'roadwork'
    if 'WORK CREW' in desc_upper:
        return 'roadwork'

    return None


def _prewarm_fetch_waze(category):
    """Fetch and parse Waze data for cache"""
    # Use existing fetch_waze_data which handles all regions
    alerts, jams = fetch_waze_data()
    features = []
    jam_features = []

    for alert in alerts:
        alert_type = (alert.get('type') or '').upper()
        classification = _classify_waze_alert(alert)

        # Filter based on category
        if category == 'hazards':
            if classification in ('police', 'roadwork'):
                continue
            if alert_type in {'HAZARD', 'ACCIDENT', 'JAM', 'ROAD_CLOSED'}:
                feature = parse_waze_alert(alert, 'Hazard')
                if feature:
                    features.append(feature)
        elif category == 'police':
            if classification == 'police':
                feature = parse_waze_alert(alert, 'Police')
                if feature:
                    features.append(feature)
        elif category == 'roadwork':
            if classification == 'roadwork':
                feature = parse_waze_alert(alert, 'Roadwork')
                if feature:
                    features.append(feature)
    
    # Parse jams for hazards category
    if category == 'hazards':
        for jam in jams:
            feature = parse_waze_jam(jam)
            if feature:
                jam_features.append(feature)
        return {
            'type': 'FeatureCollection', 
            'features': features, 
            'jams': jam_features,
            'count': len(features),
            'jamCount': len(jam_features)
        }
    
    return {'type': 'FeatureCollection', 'features': features, 'count': len(features)}


# Cached Endeavour Supabase data (all 3 types fetched together)
_endeavour_supabase_cache = {'current': [], 'current_maintenance': [], 'future_maintenance': [], 'timestamp': 0}
_endeavour_supabase_lock = threading.Lock()

def _prewarm_fetch_endeavour(outage_type):
    """Fetch Endeavour outages from Supabase API for cache.
    
    All types are fetched together in a single call and cached,
    since the Supabase API returns all outages at once.
    outage_type: 'current', 'current_maintenance', or 'future_maintenance'
    """
    global _endeavour_supabase_cache
    
    # Map legacy 'future' type to 'future_maintenance' for backward compatibility
    if outage_type == 'future':
        outage_type = 'future_maintenance'
    
    with _endeavour_supabase_lock:
        now = time.time()
        # If we fetched recently (within 30 seconds), return cached split
        if now - _endeavour_supabase_cache['timestamp'] < 30:
            return _endeavour_supabase_cache.get(outage_type, [])
    
    try:
        all_data = _fetch_endeavour_all_outages()
        
        with _endeavour_supabase_lock:
            _endeavour_supabase_cache = {
                'current': all_data.get('current', []),
                'current_maintenance': all_data.get('current_maintenance', []),
                'future_maintenance': all_data.get('future_maintenance', []),
                'timestamp': time.time(),
            }
        
        result = all_data.get(outage_type, [])
        Log.prewarm(f"Endeavour {outage_type}: {len(result)} outages from Supabase")
        return result
    except Exception as e:
        Log.prewarm(f"Endeavour {outage_type} error: {e}")
        return []


# Ausgrid network bounding box — covers Sydney + Central Coast + Hunter
# Valley (their full distribution territory). The outage-map endpoint now
# *requires* bbox + zoom params; without them upstream returns HTTP 500.
# Slight buffer on each edge so border outages don't get cut off.
_AUSGRID_BBOX = {
    'bottomleft.lat': '-34.55',
    'bottomleft.lng': '150.20',
    'topright.lat':   '-32.20',
    'topright.lng':   '152.80',
    'zoom':           '9',
}


# Per-outage detail cache. Outages persist for hours and their static fields
# (Streets, Cause, JobId) don't change once the outage is logged. Caching
# avoids 1 extra request per outage per prewarm cycle.
#
# Bounded eviction: WebIds churn over months as old outages are replaced by
# new ones. Without an upper bound this dict grows monotonically. The lock
# protects concurrent reads/writes — the prewarm thread writes while a
# request thread might call _normalise_ausgrid_outage on the live-fallback
# path.
_AUSGRID_DETAIL_CACHE = {}                # (web_id, display_type) -> (ts, detail_dict)
_AUSGRID_DETAIL_TTL = 300                 # 5 min freshness
_AUSGRID_DETAIL_CACHE_MAX = 500           # ~1 KB per row × 500 = ~500 KB worst case
_AUSGRID_DETAIL_CACHE_LOCK = threading.Lock()


def _ausgrid_detail_cache_evict_locked():
    """Trim the cache when it exceeds the size cap. Drops expired rows first;
    if still over the cap, drops the oldest entries by timestamp. Caller must
    hold _AUSGRID_DETAIL_CACHE_LOCK."""
    if len(_AUSGRID_DETAIL_CACHE) <= _AUSGRID_DETAIL_CACHE_MAX:
        return
    now = time.time()
    expired = [k for k, (ts, _) in _AUSGRID_DETAIL_CACHE.items()
               if (now - ts) >= _AUSGRID_DETAIL_TTL]
    for k in expired:
        _AUSGRID_DETAIL_CACHE.pop(k, None)
    if len(_AUSGRID_DETAIL_CACHE) <= _AUSGRID_DETAIL_CACHE_MAX:
        return
    # Still oversize — drop the oldest entries until we're at 80% capacity.
    target = int(_AUSGRID_DETAIL_CACHE_MAX * 0.8)
    by_age = sorted(_AUSGRID_DETAIL_CACHE.items(), key=lambda kv: kv[1][0])
    for k, _ in by_age[: max(0, len(_AUSGRID_DETAIL_CACHE) - target)]:
        _AUSGRID_DETAIL_CACHE.pop(k, None)


def _fetch_ausgrid_outage_detail(web_id, display_type, headers):
    """Fetch one outage's detail record (Streets, Reason, JobId, EndDateTime
    text Status) via GetOutage. Cached for 5 min. Returns None on failure."""
    if web_id is None:
        return None
    dt = (display_type or 'R').upper()
    key = (str(web_id), dt)
    now = time.time()
    with _AUSGRID_DETAIL_CACHE_LOCK:
        cached = _AUSGRID_DETAIL_CACHE.get(key)
        if cached and (now - cached[0]) < _AUSGRID_DETAIL_TTL:
            return cached[1]
    try:
        r = requests.get(
            'https://www.ausgrid.com.au/webapi/OutageMapData/GetOutage',
            timeout=10, headers=headers,
            params={'OutageDisplayType': dt, 'WebId': str(web_id)},
        )
        if r.status_code == 200:
            detail = r.json()
            if isinstance(detail, dict):
                with _AUSGRID_DETAIL_CACHE_LOCK:
                    _AUSGRID_DETAIL_CACHE[key] = (time.time(), detail)
                    _ausgrid_detail_cache_evict_locked()
                return detail
    except Exception:
        # Soft-fail — the marker payload alone still gives us the alert.
        pass
    return None


def _normalise_ausgrid_outage(item, detail=None):
    """Translate one outage from Ausgrid's new payload shape into the field
    names the bot's poller + embed builder expect. If `detail` is supplied
    (from GetOutage), its richer fields override the marker payload —
    Streets in particular is only available from the detail endpoint."""
    if not isinstance(item, dict):
        return None
    loc = item.get('MarkerLocation') or {}
    lat = loc.get('lat') if isinstance(loc, dict) else None
    lng = loc.get('lng') if isinstance(loc, dict) else None
    # OutageDisplayType: 'R' = reactive/unplanned, 'P' = planned.
    display = (item.get('OutageDisplayType') or '').upper()
    outage_type = 'Planned' if display == 'P' else 'Unplanned'

    # Detail can override several fields. Marker payload always wins on the
    # geo/lat/lng (those don't appear in detail) and on the numeric Status
    # (marker has 0/1/2; detail returns a free-text "Proceeding as scheduled").
    d = detail if isinstance(detail, dict) else {}
    cause_text = d.get('Cause') or item.get('Cause') or ''
    reason_text = d.get('Reason') or ''
    detail_text = d.get('Detail') or ''
    streets = d.get('Streets') or ''
    end_time = d.get('EndDateTime') or item.get('EstRestTime') or ''
    start_time = d.get('StartDateTime') or item.get('StartDateTime') or ''
    text_status = d.get('Status') if isinstance(d.get('Status'), str) else ''
    job_id = d.get('JobId') or ''

    raw_cust = d.get('Customers')
    if raw_cust is None:
        raw_cust = item.get('Customers')
    if raw_cust is None:
        raw_cust = item.get('CustomersAffectedText')
    try:
        customers = int(str(raw_cust).strip()) if raw_cust not in (None, '') else 0
    except (TypeError, ValueError):
        customers = 0

    return {
        'OutageId': item.get('WebId'),
        'outageId': item.get('WebId'),
        'JobId': job_id,
        'Suburb': item.get('Area') or '',
        'suburb': item.get('Area') or '',
        'StreetName': streets,
        'streetName': streets,
        'Streets': streets,
        'streets': streets,
        'Postcode': '',
        'postcode': '',
        'CustomersAffected': customers,
        'customersAffected': customers,
        'OutageType': outage_type,
        'outageType': outage_type,
        'Cause': cause_text,
        'cause': cause_text,
        'Reason': reason_text,
        'Detail': detail_text,
        'StatusText': text_status,
        'StartTime': start_time,
        'startTime': start_time,
        'EstRestoration': end_time,
        'estRestoration': end_time,
        'EndDateTime': end_time,
        'endDateTime': end_time,
        'Latitude': lat,
        'latitude': lat,
        'Longitude': lng,
        'longitude': lng,
        'Status': item.get('Status'),
        'Classification': item.get('Classification'),
        'Polygons': item.get('Polygons') or [],
    }


def _prewarm_fetch_ausgrid(data_type):
    """Fetch and parse Ausgrid data for cache.

    Ausgrid reworked their outage map API: the markers endpoint now requires
    a bbox + zoom query string covering the area you want, and returns a
    flat array of outages (rather than the old `{Markers, Polygons}` wrapper).
    We normalise the response into the legacy shape so the bot's poller and
    embed builder keep working without changes.
    """
    base = 'https://www.ausgrid.com.au/webapi/OutageMapData/GetCurrentUnplannedOutageMarkersAndPolygons'
    stats_url = 'https://www.ausgrid.com.au/webapi/outagemapdata/GetCurrentOutageStats'
    # Only the outages endpoint feeds alerts; stats is cosmetic. Tracking
    # both under one source name made outages look degraded whenever the
    # stats endpoint flickered.
    track_health = (data_type == 'outages')
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
        ),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-AU,en;q=0.9',
        'Referer': 'https://www.ausgrid.com.au/Outages/View-current-outages',
        'Origin': 'https://www.ausgrid.com.au',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
    }
    if data_type == 'outages':
        url = base
        params = _AUSGRID_BBOX
    else:
        url = stats_url
        params = None

    try:
        r = requests.get(url, timeout=15, headers=headers, params=params)
        if r.status_code == 200:
            try:
                payload = r.json()
                if track_health:
                    source_ok('power_ausgrid')
                # New schema: flat array of outages. Normalise to the legacy
                # `{Markers, Polygons}` wrapper so existing consumers don't
                # need to change. Stats response is left untouched.
                if data_type == 'outages':
                    if isinstance(payload, list):
                        markers = []
                        polygons = []
                        for item in payload:
                            # Enrich each outage with its detail record (Streets,
                            # Reason, JobId, etc). Cached for 5 min so the prewarm
                            # cycle doesn't re-fetch the same outage every tick.
                            detail = _fetch_ausgrid_outage_detail(
                                item.get('WebId'),
                                item.get('OutageDisplayType'),
                                headers,
                            )
                            normalised = _normalise_ausgrid_outage(item, detail=detail)
                            if normalised is None:
                                continue
                            markers.append(normalised)
                            polys = item.get('Polygons') or []
                            if isinstance(polys, list):
                                polygons.extend(polys)
                        return {'Markers': markers, 'Polygons': polygons}
                    # Already in old shape (e.g. cached response or hand-rolled).
                    if isinstance(payload, dict) and 'Markers' in payload:
                        return payload
                    return {'Markers': [], 'Polygons': []}
                return payload
            except Exception as parse_err:
                if track_health:
                    source_error('power_ausgrid', f'parse: {parse_err}')
                Log.prewarm(f"Ausgrid {data_type} parse failed: {parse_err}; "
                            f"first 200 bytes: {(r.text or '')[:200]!r}")
                raise
        # Non-200 path — surface the body so we can see *why* upstream rejected.
        body_preview = (r.text or '')[:200]
        if track_health:
            source_error('power_ausgrid', f'HTTP {r.status_code}: {body_preview[:80]}')
        Log.prewarm(f"Ausgrid {data_type} HTTP {r.status_code}; body: {body_preview!r}")
    except Exception as e:
        if track_health:
            source_error('power_ausgrid', e)
        Log.prewarm(f"Ausgrid {data_type} error: {e}")
    return {'Markers': [], 'Polygons': []} if data_type == 'outages' else {}


def _parse_essential_energy_kml(kml_content, feed_type='current'):
    """Parse Essential Energy KML outage feed into structured outage records.
    
    KML feeds:
    - current.kml: Active outages (planned + unplanned)
    - future.kml: Upcoming planned outages
    
    Each Placemark contains:
    - id attribute: Incident ID (e.g., INCD-118773-r)
    - styleUrl: Contains 'planned' or 'unplanned'
    - description: HTML with Time Off, Est. Time On, Customers, Reason, Last Updated
    - MultiGeometry > Point > coordinates: lon,lat
    - MultiGeometry > Polygon: Outage area boundary
    """
    import xml.etree.ElementTree as ET
    
    outages = []
    try:
        root = ET.fromstring(kml_content)
        # KML namespace
        ns = {'kml': 'http://earth.google.com/kml/2.1'}
        
        # Find all Placemarks (try with and without namespace)
        placemarks = root.findall('.//{http://earth.google.com/kml/2.1}Placemark')
        if not placemarks:
            placemarks = root.findall('.//Placemark')
        
        for pm in placemarks:
            try:
                incident_id = pm.get('id', '')
                
                # Extract name/title (suburb or location identifier)
                name_elem = pm.findtext('{http://earth.google.com/kml/2.1}name', '')
                if not name_elem:
                    name_elem = pm.findtext('name', '')
                placemark_name = (name_elem or '').strip()
                
                # Determine if planned or unplanned from styleUrl
                style_url = pm.findtext('{http://earth.google.com/kml/2.1}styleUrl', '')
                if not style_url:
                    style_url = pm.findtext('styleUrl', '')
                outage_type = 'planned' if 'planned' in style_url.lower() else 'unplanned'
                
                # Parse description HTML for details
                desc_elem = pm.find('{http://earth.google.com/kml/2.1}description')
                if desc_elem is None:
                    desc_elem = pm.find('description')
                desc_text = desc_elem.text if desc_elem is not None else ''
                
                # Extract fields from HTML description
                time_off = ''
                est_time_on = ''
                customers = 0
                reason = ''
                last_updated = ''
                
                if desc_text:
                    # Extract Time Off
                    m = re.search(r'Time Off:</span>(.*?)</div>', desc_text)
                    if m:
                        time_off = m.group(1).strip()
                    
                    # Extract Est. Time On
                    m = re.search(r'Est\. Time On:</span>(.*?)</div>', desc_text)
                    if m:
                        est_time_on = m.group(1).strip()
                    
                    # Extract customers affected
                    m = re.search(r'Customers affected:</span>\s*(\d+)', desc_text)
                    if m:
                        customers = int(m.group(1))
                    
                    # Extract reason
                    m = re.search(r'Reason:</span>(.*?)</div>', desc_text)
                    if m:
                        reason = m.group(1).strip()
                    
                    # Extract last updated
                    m = re.search(r'Last Updated:</span>(.*?)</div>', desc_text)
                    if m:
                        last_updated = m.group(1).strip()
                
                # Extract point coordinates (lon,lat format in KML)
                lat = None
                lon = None
                point = pm.find('.//{http://earth.google.com/kml/2.1}Point/{http://earth.google.com/kml/2.1}coordinates')
                if point is None:
                    point = pm.find('.//Point/coordinates')
                if point is not None and point.text:
                    coords = point.text.strip().split(',')
                    if len(coords) >= 2:
                        lon = float(coords[0])
                        lat = float(coords[1])
                
                # Extract polygon coordinates for outage area
                polygon_coords = []
                poly = pm.find('.//{http://earth.google.com/kml/2.1}Polygon//{http://earth.google.com/kml/2.1}coordinates')
                if poly is None:
                    poly = pm.find('.//Polygon//coordinates')
                if poly is not None and poly.text:
                    raw_coords = poly.text.strip().split()
                    for coord_str in raw_coords:
                        parts = coord_str.split(',')
                        if len(parts) >= 2:
                            polygon_coords.append([float(parts[0]), float(parts[1])])
                
                # Parse time_off into ISO format for source_timestamp
                source_timestamp = None
                if time_off:
                    try:
                        dt = datetime.strptime(time_off, '%d/%m/%Y %H:%M:%S')
                        source_timestamp = dt.strftime('%Y-%m-%dT%H:%M:%S')
                    except (ValueError, TypeError):
                        source_timestamp = time_off
                
                # Determine source and status based on feed type
                if feed_type == 'future':
                    source = 'essential_future'
                    status = 'scheduled'
                else:
                    # current feed: use planned/unplanned from styleUrl
                    source = 'essential_current' if outage_type == 'unplanned' else 'essential_planned'
                    status = 'active'
                
                outages.append({
                    'incidentId': incident_id,
                    'title': placemark_name or reason or 'Power Outage',
                    'suburb': placemark_name,
                    'outageType': outage_type,
                    'feedType': feed_type,
                    'latitude': lat,
                    'longitude': lon,
                    'cause': reason or 'Unknown',
                    'customersAffected': customers,
                    'timeOff': time_off,
                    'estTimeOn': est_time_on,
                    'lastUpdated': last_updated,
                    'status': status,
                    'source': source,
                    'sourceTimestamp': source_timestamp,
                    'polygon': polygon_coords if polygon_coords else None,
                    'provider': 'Essential Energy'
                })
            except (ValueError, TypeError, AttributeError) as e:
                continue
    except ET.ParseError as e:
        Log.error(f"Essential Energy KML parse error: {e}")
    except Exception as e:
        Log.error(f"Essential Energy KML error: {e}")
    
    return outages


ESSENTIAL_ENERGY_FEEDS = {
    'current': 'https://www.essentialenergy.com.au/Assets/kmz/current.kml',
    'future': 'https://www.essentialenergy.com.au/Assets/kmz/future.kml',
}

def _prewarm_fetch_essential_energy(feed_type='current'):
    """Fetch and parse Essential Energy KML outage feed for cache.
    
    Args:
        feed_type: 'current' or 'future'
    """
    url = ESSENTIAL_ENERGY_FEEDS.get(feed_type, ESSENTIAL_ENERGY_FEEDS['current'])
    try:
        r = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            outages = _parse_essential_energy_kml(r.content, feed_type=feed_type)
            Log.prewarm(f"Essential Energy ({feed_type}): {len(outages)} outages from KML")
            return outages
    except Exception as e:
        Log.prewarm(f"Essential Energy ({feed_type}) error: {e}")
    return []


def _prewarm_fetch_beachwatch():
    """Fetch beachwatch data for cache"""
    try:
        r = requests.get('https://api.beachwatch.nsw.gov.au/public/sites/geojson',
                        timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        Log.prewarm(f"Beachwatch error: {e}")
    return {'type': 'FeatureCollection', 'features': []}


def _prewarm_fetch_beachsafe():
    """Fetch beachsafe data for cache"""
    try:
        ne_lat, ne_lon = -28.0, 154.0
        sw_lat, sw_lon = -37.5, 149.0
        url = f'https://beachsafe.org.au/api/v4/map/beaches?neCoords[]={ne_lat}&neCoords[]={ne_lon}&swCoords[]={sw_lat}&swCoords[]={sw_lon}'
        r = requests.get(url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json',
            'Referer': 'https://beachsafe.org.au/'
        })
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and 'beaches' in data:
                beaches = data.get('beaches', [])
                normalized = []
                for b in beaches:
                    beach_url = b.get('url', '')
                    beach_slug = beach_url.rstrip('/').split('/')[-1] if beach_url else ''
                    patrol_today = b.get('is_patrolled_today', {})
                    if not isinstance(patrol_today, dict):
                        patrol_today = {}
                    normalized.append({
                        'id': b.get('id'),
                        'name': b.get('title', 'Unknown Beach'),
                        'lat': float(b.get('latitude', 0)) if b.get('latitude') else None,
                        'lng': float(b.get('longitude', 0)) if b.get('longitude') else None,
                        'url': beach_url,
                        'slug': beach_slug,
                        'patrolled': b.get('status', '').lower() == 'patrolled',
                        'status': b.get('status', 'Unknown'),
                        'hasToilet': bool(b.get('has_toilet')),
                        'hasParking': bool(b.get('has_parking')),
                        'dogsAllowed': bool(b.get('has_dogs_allowed')),
                        'image': b.get('image', ''),
                        'weather': b.get('weather', {}),
                        'hazards': b.get('hazards') or [],
                        'isPatrolledToday': patrol_today.get('flag', False),
                        'patrolStart': patrol_today.get('start', ''),
                        'patrolEnd': patrol_today.get('end', ''),
                        'patrol': b.get('patrol', 0),
                    })
                return normalized
            return data if isinstance(data, list) else []
    except Exception as e:
        Log.prewarm(f"Beachsafe error: {e}")
    return []


def _prewarm_fetch_beachsafe_details():
    """Fetch detailed data for all beaches from BeachSafe individual beach endpoints.
    Returns a dict keyed by slug with detail data for each beach."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Get the beachsafe list from cache to know which slugs to fetch
    cached_list, _, _ = cache_get('beachsafe')
    if not cached_list or not isinstance(cached_list, list):
        Log.prewarm("BeachSafe details: no beach list in cache, skipping")
        return {}

    # Extract slugs
    slugs = []
    for b in cached_list:
        slug = b.get('slug', '')
        if slug:
            slugs.append(slug)

    if not slugs:
        return {}

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://beachsafe.org.au/'
    }

    all_details = {}

    def fetch_one(slug):
        # Check if we already have a fresh cached version
        cached, age, expired = cache_get(f'beachsafe_detail_{slug}')
        if cached and not expired:
            return slug, cached

        try:
            url = f'https://beachsafe.org.au/api/v4/beach/{slug}'
            r = requests.get(url, timeout=10, headers=headers)
            if r.status_code == 200:
                data = r.json()
                beach = data.get('beach', {})

                attendances = beach.get('attendances', {})
                latest_attendance = None
                if attendances:
                    last_key = list(attendances.keys())[-1] if attendances else None
                    if last_key:
                        entries = attendances[last_key]
                        if entries:
                            latest_attendance = {'date': last_key, 'entries': entries}

                patrol_today = beach.get('is_patrolled_today', {})
                if not isinstance(patrol_today, dict):
                    patrol_today = {}

                result = {
                    'weather': beach.get('weather', {}),
                    'currentTide': beach.get('currentTide'),
                    'currentUV': beach.get('currentUV'),
                    'latestAttendance': latest_attendance,
                    'todays_marine_warnings': beach.get('todays_marine_warnings', []),
                    'patrol': beach.get('patrol', 0),
                    'patrolStart': patrol_today.get('start', ''),
                    'patrolEnd': patrol_today.get('end', ''),
                    'isPatrolledToday': patrol_today.get('flag', False),
                    'status': beach.get('status', 'Unknown'),
                    'hazard': beach.get('hazard', 0),
                }
                # Cache individual detail
                cache_set(f'beachsafe_detail_{slug}', result, 600)
                return slug, result
        except Exception:
            pass
        return slug, None

    # Fetch in parallel with limited workers to avoid hammering the API
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_one, slug): slug for slug in slugs}
        for f in as_completed(futures):
            try:
                slug, result = f.result()
                if result:
                    all_details[slug] = result
            except Exception:
                pass
            # Small delay between completions to be respectful
            time.sleep(0.05)

    if DEV_MODE:
        Log.prewarm(f"BeachSafe details: fetched {len(all_details)}/{len(slugs)} beaches")
    return all_details


def _prewarm_fetch_weather():
    """Fetch weather data for cache"""
    features = []
    # Use subset of locations for faster prewarm (NSW_WEATHER_LOCATIONS defined later in file)
    # We'll fetch Sydney + major cities for initial prewarm
    locations = [
        {"name": "Sydney CBD", "lat": -33.8688, "lon": 151.2093},
        {"name": "Parramatta", "lat": -33.8151, "lon": 151.0011},
        {"name": "Newcastle", "lat": -32.9283, "lon": 151.7817},
        {"name": "Wollongong", "lat": -34.4278, "lon": 150.8931},
        {"name": "Central Coast", "lat": -33.4245, "lon": 151.3419},
        {"name": "Penrith", "lat": -33.7506, "lon": 150.6944},
        {"name": "Campbelltown", "lat": -34.0650, "lon": 150.8142},
        {"name": "Coffs Harbour", "lat": -30.2963, "lon": 153.1157},
        {"name": "Dubbo", "lat": -32.2569, "lon": 148.6011},
        {"name": "Tamworth", "lat": -31.0927, "lon": 150.9320},
        {"name": "Wagga Wagga", "lat": -35.1082, "lon": 147.3598},
        {"name": "Albury", "lat": -36.0737, "lon": 146.9135},
        {"name": "Broken Hill", "lat": -31.9505, "lon": 141.4533},
        {"name": "Canberra", "lat": -35.2809, "lon": 149.1300},
        {"name": "Byron Bay", "lat": -28.6433, "lon": 153.6150},
    ]
    
    lats = ",".join([str(loc["lat"]) for loc in locations])
    lons = ",".join([str(loc["lon"]) for loc in locations])
    
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lats}&longitude={lons}&current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m&timezone=Australia%2FSydney"
        r = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            weather_list = data if isinstance(data, list) else [data]
            
            for i, loc in enumerate(locations):
                if i >= len(weather_list):
                    break
                current = weather_list[i].get('current', {})
                if not current:
                    continue
                weather_code = current.get('weather_code', 0)
                weather_desc, weather_icon = WEATHER_CODES.get(weather_code, ("Unknown", "❓"))
                
                features.append({
                    'type': 'Feature',
                    'geometry': {'type': 'Point', 'coordinates': [loc['lon'], loc['lat']]},
                    'properties': {
                        'name': loc['name'],
                        'temperature': current.get('temperature_2m'),
                        'feelsLike': current.get('apparent_temperature'),
                        'humidity': current.get('relative_humidity_2m'),
                        'windSpeed': current.get('wind_speed_10m'),
                        'weatherCode': weather_code,
                        'weatherDescription': weather_desc,
                        'weatherIcon': weather_icon
                    }
                })
    except Exception as e:
        Log.prewarm(f"Weather error: {e}")
    return {'type': 'FeatureCollection', 'features': features}


def _prewarm_fetch_bom_warnings():
    """Fetch BOM warnings for cache"""
    warnings = _fetch_all_bom_warnings()
    counts = {'land': 0, 'marine': 0}
    for w in warnings:
        cat = w.get('category', 'land')
        if cat in counts:
            counts[cat] += 1
    return {'warnings': warnings, 'count': len(warnings), 'counts': counts}


def _parse_pager_coords(message):
    """Extract [lon,lat] coordinates from pager message text."""
    text = message or ""
    
    # 1) Strict [lon,lat]
    m = re.search(r"\[(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)\]", text)
    if not m:
        # 2) Loose: optional [, optional ], end of string, etc.
        m = re.search(r"\[?(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)(?:\]|\s|$)", text)
    
    if not m:
        return None, None
    
    lon = float(m.group(1))
    lat = float(m.group(2))
    return lat, lon


def _parse_pager_incident_id(message):
    """Extract incident ID from pager message (e.g., 25-139605 or 0053-6653)."""
    text = (message or "").strip()
    
    # Strip odd zero-width chars
    text = re.sub(r"[\u200e\u200f\u202a\u202c]", "", text)
    
    # Normalise all dash variants to plain ASCII hyphen
    text_norm = re.sub(r"[\u2010-\u2015\u2212\u2043\u00ad]", "-", text, flags=re.IGNORECASE)
    
    # 1) Normal RFS style: 25-139605
    m = re.search(r"\b(\d{2}-\d{6})\b", text_norm)
    if m:
        return m.group(1)
    
    # 2) Fallback: 4-4 style codes (e.g. 0053-6653)
    m2 = re.search(r"\b(\d{4}-\d{4})\b", text_norm)
    if m2:
        return m2.group(1)
    
    return None


def _prewarm_fetch_pager():
    """Fetch pager messages from Pagermon API"""
    if not PAGERMON_URL or not PAGERMON_URL.startswith('http'):
        return {'messages': [], 'count': 0}  # PAGERMON_URL not configured
    try:
        url = f"{PAGERMON_URL}?limit=100"
        
        # Add browser-like headers to avoid 403 blocks
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        
        # Add API key if configured (optional)
        if PAGERMON_API_KEY:
            url = f"{PAGERMON_URL}?apikey={PAGERMON_API_KEY}&limit=100"
        
        # Log the URL once per process — was firing every prewarm cycle
        # because the prewarm scheduler hits this function on a loop.
        global _pagermon_url_logged
        try:
            _pagermon_url_logged
        except NameError:
            _pagermon_url_logged = False
        if DEV_MODE and not _pagermon_url_logged:
            Log.info(f"Pagermon URL: {url}")
            _pagermon_url_logged = True

        resp = requests.get(url, headers=headers, timeout=15)
        if not resp.ok:
            source_error('pager', f'HTTP {resp.status_code}')
            Log.error(f"Pagermon API error: {resp.status_code}")
            if DEV_MODE:
                Log.error(f"Pagermon response: {resp.text[:200]}")
            return {'messages': [], 'count': 0}

        data = resp.json()
        messages = data.get('messages', [])
        source_ok('pager')
        
        # Filter messages that have valid coordinates
        # Group by incident_id to share coords within incidents
        enriched = []
        for msg in messages:
            message_text = msg.get('message') or ''
            incident_id = _parse_pager_incident_id(message_text)
            lat, lon = _parse_pager_coords(message_text)
            
            enriched.append({
                'msg': msg,
                'incident_id': incident_id,
                'lat': lat,
                'lon': lon
            })
        
        # Group by incident_id to find canonical coords
        incidents = {}
        for item in enriched:
            inc = item['incident_id']
            msg = item['msg']
            
            if not inc:
                lat = item['lat']
                lon = item['lon']
                if lat is not None and lon is not None:
                    ts = msg.get('timestamp') or 0
                    capcode = msg.get('address') or 'nocap'
                    inc = f"noid-{ts}-{capcode}"
                    item['incident_id'] = inc
                else:
                    continue
            
            incidents.setdefault(inc, []).append(item)
        
        # Build final list with canonical coords per incident
        result_messages = []
        for inc_id, items in incidents.items():
            # Find canonical coords for this incident
            canonical_lat = None
            canonical_lon = None
            for item in items:
                if item['lat'] is not None and item['lon'] is not None:
                    canonical_lat = item['lat']
                    canonical_lon = item['lon']
                    break
            
            # Skip incidents without coords
            if canonical_lat is None or canonical_lon is None:
                continue
            
            # Add each message with canonical coords
            for item in items:
                msg = item['msg']
                pager_msg_id = msg.get('id')
                if pager_msg_id is None:
                    continue
                
                # Convert timestamp to ISO
                ts = msg.get('timestamp')
                try:
                    incident_time = datetime.fromtimestamp(int(ts)).isoformat() if ts else None
                except (ValueError, TypeError, OSError):
                    incident_time = None
                
                result_messages.append({
                    'id': pager_msg_id,
                    'incident_id': inc_id,
                    'capcode': msg.get('address'),
                    'alias': msg.get('alias'),
                    'agency': msg.get('agency'),
                    'source': msg.get('source'),
                    'message': msg.get('message') or '',
                    'lat': canonical_lat,
                    'lon': canonical_lon,
                    'incident_time': incident_time,
                    'timestamp': ts
                })
        
        return {'messages': result_messages, 'count': len(result_messages)}

    except Exception as e:
        source_error('pager', e)
        Log.error(f"Pagermon fetch error: {e}")
        return {'messages': [], 'count': 0}


def _generate_stable_id(source, lat, lon, title, created=None):
    """
    Generate a stable ID for incidents that don't have reliable source IDs.
    Uses location + title hash to create a consistent identifier.
    """
    # Round coordinates to avoid floating point differences
    lat_str = f"{float(lat):.5f}" if lat else ""
    lon_str = f"{float(lon):.5f}" if lon else ""
    title_str = str(title or "").strip()[:100]  # First 100 chars of title
    
    # Include created date if available (without time to allow for minor timestamp diffs)
    created_date = ""
    if created:
        try:
            # Try to extract just the date part
            created_str = str(created)
            if 'T' in created_str:
                created_date = created_str.split('T')[0]
            elif ' ' in created_str:
                created_date = created_str.split(' ')[0]
        except (ValueError, TypeError, AttributeError):
            pass
    
    id_string = f"{source}|{lat_str}|{lon_str}|{title_str}|{created_date}"
    return hashlib.md5(id_string.encode()).hexdigest()[:16]


def _extract_history_records(cache_key, data):
    """
    Extract individual records from prewarm data for historical storage.
    Returns a list of dicts ready for store_incidents_batch().
    """
    records = []
    
    # Skip non-incident data (stats, cameras, weather)
    skip_keys = ['traffic_cameras', 'aviation_cameras', 'weather_current', 
                 'ausgrid_stats', 'beachsafe', 'beachwatch']
    if cache_key in skip_keys:
        return []
    
    # Sources that need stable ID generation (their native IDs change each fetch)
    unstable_id_sources = ['traffic_incidents', 'traffic_roadwork', 'traffic_flood', 
                           'traffic_fire', 'traffic_majorevent', 'bom_warnings']
    
    # Handle GeoJSON FeatureCollections (RFS, Traffic, Waze)
    if isinstance(data, dict) and data.get('type') == 'FeatureCollection':
        features = data.get('features', [])
        for f in features:
            props = f.get('properties', {})
            coords = f.get('geometry', {}).get('coordinates', [])
            lon = coords[0] if len(coords) > 0 else None
            lat = coords[1] if len(coords) > 1 else None
            
            # Map cache_key to source name
            source_map = {
                'rfs_incidents': 'rfs',
                'traffic_incidents': 'traffic_incident',
                'traffic_roadwork': 'traffic_roadwork',
                'traffic_flood': 'traffic_flood',
                'traffic_fire': 'traffic_fire',
                'traffic_majorevent': 'traffic_majorevent',
                'waze_hazards': 'waze_hazard',
                'waze_police': 'waze_police',
                'waze_roadwork': 'waze_roadwork',
            }
            source = source_map.get(cache_key, cache_key)
            
            # Extract source timestamp
            source_ts = props.get('pubDate') or props.get('created') or props.get('lastUpdated')
            
            # Get title for stable ID generation
            title = props.get('title') or props.get('headline')
            
            # Determine source_id - use native ID if stable, otherwise generate one
            native_id = props.get('guid') or props.get('id') or props.get('incidentId')
            if cache_key in unstable_id_sources or not native_id:
                # Generate stable ID from location + title + created date
                source_id = _generate_stable_id(source, lat, lon, title, source_ts)
            else:
                source_id = native_id
            
            # Get hierarchical source info
            provider, source_type = get_source_hierarchy(source)
            
            records.append({
                'source': source,
                'source_id': source_id,
                'source_provider': provider,
                'source_type': source_type,
                'lat': lat,
                'lon': lon,
                'location_text': props.get('location') or props.get('roads') or props.get('street'),
                'title': title,
                'category': props.get('alertLevel') or props.get('mainCategory') or props.get('type'),
                'subcategory': props.get('subCategory') or props.get('wazeSubtype'),
                'status': props.get('status'),
                'severity': props.get('alertLevel') or props.get('severity'),
                'source_timestamp': source_ts,
                'is_active': 0 if props.get('ended') else 1,
                'data': props  # Full properties as JSON
            })

        # Waze jams live on the hazards FeatureCollection as a sibling 'jams' array.
        # Archive them as a separate source so they show up in history/stats.
        if cache_key == 'waze_hazards':
            for j in data.get('jams', []) or []:
                jprops = j.get('properties', {}) or {}
                jcoords = j.get('geometry', {}).get('coordinates', []) or []
                # Jam geometries are LineStrings — pick the midpoint for indexing
                if jcoords and isinstance(jcoords[0], list):
                    mid = jcoords[len(jcoords) // 2]
                    jlon = mid[0] if len(mid) > 0 else None
                    jlat = mid[1] if len(mid) > 1 else None
                else:
                    jlat = jlon = None
                jtitle = jprops.get('title')
                jsource_ts = jprops.get('created') or jprops.get('pubDate')
                # Waze gives jams numeric ids/uuids. source_id column is TEXT
                # and Postgres won't coerce int → text in a WHERE IN tuple, so
                # stringify.
                jnative = jprops.get('id') or jprops.get('uuid')
                jsource_id = str(jnative) if jnative else _generate_stable_id('waze_jam', jlat, jlon, jtitle, jsource_ts)
                jprovider, jsource_type = get_source_hierarchy('waze_jam')
                records.append({
                    'source': 'waze_jam',
                    'source_id': jsource_id,
                    'source_provider': jprovider,
                    'source_type': jsource_type,
                    'lat': jlat,
                    'lon': jlon,
                    'location_text': jprops.get('location') or jprops.get('street'),
                    'title': jtitle,
                    'category': jprops.get('wazeType') or 'JAM',
                    'subcategory': jprops.get('severity'),
                    'status': None,
                    'severity': jprops.get('level'),
                    'source_timestamp': jsource_ts,
                    'is_active': 1,
                    'data': jprops
                })

    # Handle power outage data (Endeavour, Ausgrid)
    elif cache_key in ('endeavour_current', 'endeavour_maintenance', 'endeavour_future', 'ausgrid_outages', 'essential_energy', 'essential_energy_future'):
        outages = []
        if cache_key.startswith('endeavour'):
            outages = data if isinstance(data, list) else data.get('outages', [])
        elif cache_key == 'ausgrid_outages':
            outages = data.get('outages', []) if isinstance(data, dict) else data
        elif cache_key.startswith('essential_energy'):
            outages = data if isinstance(data, list) else []
        
        for o in outages:
            # Use distinct source names for current vs planned to prevent cross-contamination
            if cache_key == 'endeavour_current':
                source = 'endeavour_current'
            elif cache_key in ('endeavour_maintenance', 'endeavour_future'):
                source = 'endeavour_planned'
            elif cache_key.startswith('essential_energy'):
                source = o.get('source', 'essential_current')
            else:
                source = 'ausgrid'
            
            lat = o.get('latitude') or o.get('lat')
            lon = o.get('longitude') or o.get('lon')
            title = o.get('title') or o.get('suburb') or o.get('cause') or o.get('location')
            source_ts = o.get('startTime') or o.get('created') or o.get('sourceTimestamp')
            
            # Use native ID if it looks stable, otherwise generate one
            native_id = o.get('incidentId') or o.get('id')
            # Endeavour/Essential outages have unstable IDs - generate stable ones
            if cache_key.startswith('endeavour') or cache_key.startswith('essential_energy') or not native_id:
                source_id = _generate_stable_id(source, lat, lon, title, source_ts)
            else:
                source_id = native_id
            
            # Categorization by source:
            # Endeavour: maintenance/future = planned, current = check outageType
            # Essential Energy: uses outageType from KML (planned/unplanned)
            if cache_key in ('endeavour_maintenance', 'endeavour_future'):
                # Both maintenance types are always planned
                category = 'planned'
            elif cache_key == 'essential_energy_future':
                category = 'future'
            elif cache_key.startswith('essential_energy'):
                # Essential Energy current KML uses outageType directly
                category = o.get('outageType', 'unplanned')
            else:
                # Current outages: check outageType field
                outage_type = str(o.get('outageType', '')).upper()
                if outage_type in ('P', 'PLANNED', 'CURRENT MAINTENANCE', 'FUTURE MAINTENANCE'):
                    category = 'planned'
                elif outage_type in ('U', 'UNPLANNED'):
                    category = 'unplanned'
                else:
                    # Default to unplanned for current outages with unknown type
                    category = 'unplanned'
            
            # Get hierarchical source info
            provider, source_type = get_source_hierarchy(source)
            
            records.append({
                'source': source,
                'source_id': source_id,
                'source_provider': provider,
                'source_type': source_type,
                'lat': lat,
                'lon': lon,
                'location_text': o.get('suburb') or o.get('location'),
                'title': title,
                'category': category,
                'subcategory': o.get('status'),
                'status': o.get('status'),
                'severity': None,
                'source_timestamp': source_ts,
                'is_active': 1,
                'data': o
            })
    
    # Handle BOM warnings
    elif cache_key == 'bom_warnings':
        warnings = data.get('warnings', []) if isinstance(data, dict) else data
        for w in warnings:
            title = w.get('title')
            area = w.get('area')
            source_ts = w.get('issued') or w.get('expiry')
            
            # Determine source based on category (land or marine)
            bom_category = w.get('category', 'land')
            if bom_category == 'marine':
                source = 'bom_marine'
            else:
                source = 'bom_land'
            
            # BOM warnings may have unstable IDs - generate stable ones
            native_id = w.get('id')
            if not native_id:
                # Use title + area + date for stable ID
                id_string = f"bom|{title or ''}|{area or ''}|{str(source_ts or '').split('T')[0] if source_ts else ''}"
                source_id = hashlib.md5(id_string.encode()).hexdigest()[:16]
            else:
                source_id = native_id
            
            # Get hierarchical source info
            provider, source_type = get_source_hierarchy(source)
            
            # Extract specific warning type from title for subcategory
            # This gives us: Wind, Flood, Thunderstorm, Surf, Heatwave, etc.
            warning_type = _extract_bom_warning_type(title)
            
            records.append({
                'source': source,
                'source_id': source_id,
                'source_provider': provider,
                'source_type': source_type,
                'lat': None,
                'lon': None,
                'location_text': area,
                'title': title,
                'category': bom_category,
                'subcategory': warning_type,
                'status': None,
                'severity': w.get('severity'),
                'source_timestamp': source_ts,
                'is_active': 1,
                'data': w
            })
    
    # Handle pager messages
    elif cache_key == 'pager':
        messages = data.get('messages', []) if isinstance(data, dict) else data
        for msg in messages:
            pager_id = msg.get('id')
            if not pager_id:
                continue
            
            # Use raw unix timestamp directly for better reliability
            # msg['timestamp'] is the raw unix timestamp from pagermon
            raw_ts = msg.get('timestamp')
            
            # Get hierarchical source info
            provider, source_type = get_source_hierarchy('pager')
            
            records.append({
                'source': 'pager',
                'source_id': str(pager_id),
                'source_provider': provider,
                'source_type': source_type,
                'lat': msg.get('lat'),
                'lon': msg.get('lon'),
                'location_text': None,  # Could extract from message later
                'title': msg.get('alias') or msg.get('capcode') or 'Pager Hit',
                'category': msg.get('agency'),
                'subcategory': msg.get('capcode'),
                'status': None,
                'severity': None,
                'source_timestamp': raw_ts,  # Pass raw unix timestamp directly
                'is_active': 1,
                'data': msg
            })
    
    return records


def prewarm_single(cache_key, ttl):
    """Prewarm a single endpoint and store in cache"""
    
    # Friendly names for logging
    PREWARM_NAMES = {
        'rfs_incidents': 'RFS',
        'traffic_incidents': 'Traffic',
        'traffic_cameras': 'Cameras',
        'traffic_roadwork': 'Roadwork',
        'traffic_flood': 'Floods',
        'traffic_fire': 'Traffic Fire',
        'traffic_majorevent': 'Major Events',
        'waze_hazards': 'Waze Hazards',
        'waze_police': 'Waze Police',
        'waze_roadwork': 'Waze Roadwork',
        'aviation_cameras': 'Aviation',
        'endeavour_current': 'Endeavour',
        'endeavour_maintenance': 'Endeavour Maintenance',
        'endeavour_future': 'Endeavour Planned',
        'ausgrid_outages': 'Ausgrid',
        'ausgrid_stats': 'Ausgrid Stats',
        'essential_energy': 'Essential Energy',
        'essential_energy_future': 'Essential Future',
        'beachwatch': 'Beachwatch',
        'beachsafe': 'Beachsafe',
        'beachsafe_details': 'BeachSafe Details',
        'weather_current': 'Weather',
        'bom_warnings': 'BOM Warnings',
        'pager': 'Pager',
        'centralwatch_cameras': 'Central Watch',
    }
    
    name = PREWARM_NAMES.get(cache_key, cache_key)
    start_time = time.time()
    data = None

    # Skip parked endpoints silently — the failure that parked them already logged.
    if _prewarm_in_backoff(cache_key) > 0:
        return 'skipped', 0, {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}

    try:
        if cache_key == 'rfs_incidents':
            data = _prewarm_fetch_rfs()
        elif cache_key == 'traffic_incidents':
            data = _prewarm_fetch_traffic('incidents')
        elif cache_key == 'traffic_cameras':
            data = _prewarm_fetch_traffic_cameras()
        elif cache_key == 'traffic_roadwork':
            data = _prewarm_fetch_traffic('roadwork')
        elif cache_key == 'traffic_flood':
            data = _prewarm_fetch_traffic('flood')
        elif cache_key == 'traffic_fire':
            data = _prewarm_fetch_traffic('fire')
        elif cache_key == 'traffic_majorevent':
            data = _prewarm_fetch_traffic('majorevent')
        elif cache_key == 'waze_hazards':
            data = _prewarm_fetch_waze('hazards')
        elif cache_key == 'waze_police':
            data = _prewarm_fetch_waze('police')
        elif cache_key == 'waze_roadwork':
            data = _prewarm_fetch_waze('roadwork')
        elif cache_key == 'aviation_cameras':
            data = _prewarm_fetch_aviation_cameras()
        elif cache_key == 'endeavour_current':
            data = _prewarm_fetch_endeavour('current')
        elif cache_key == 'endeavour_maintenance':
            data = _prewarm_fetch_endeavour('current_maintenance')
        elif cache_key == 'endeavour_future':
            data = _prewarm_fetch_endeavour('future_maintenance')
        elif cache_key == 'ausgrid_outages':
            data = _prewarm_fetch_ausgrid('outages')
        elif cache_key == 'ausgrid_stats':
            data = _prewarm_fetch_ausgrid('stats')
        elif cache_key == 'essential_energy':
            data = _prewarm_fetch_essential_energy('current')
        elif cache_key == 'essential_energy_future':
            data = _prewarm_fetch_essential_energy('future')
        elif cache_key == 'beachwatch':
            data = _prewarm_fetch_beachwatch()
        elif cache_key == 'beachsafe':
            data = _prewarm_fetch_beachsafe()
        elif cache_key == 'beachsafe_details':
            data = _prewarm_fetch_beachsafe_details()
        elif cache_key == 'weather_current':
            data = _prewarm_fetch_weather()
        elif cache_key == 'bom_warnings':
            data = _prewarm_fetch_bom_warnings()
        elif cache_key == 'pager':
            data = _prewarm_fetch_pager()
        elif cache_key == 'centralwatch_cameras':
            data = _prewarm_fetch_centralwatch_cameras()
        
        if data is not None:
            # Don't cache empty Waze results — browser worker may not be ready yet.
            # Treat as transient "empty success" so we don't trigger backoff when
            # fetch_waze_data's own cooldown is doing its job.
            fetch_time_ms = int((time.time() - start_time) * 1000)
            is_empty_waze = (
                cache_key.startswith('waze_')
                and isinstance(data, dict)
                and len(data.get('features', [])) == 0
            )
            if not is_empty_waze:
                cache_set(cache_key, data, ttl, fetch_time_ms)

            # Count items
            item_count = 0
            if isinstance(data, dict):
                if 'features' in data:
                    item_count = len(data.get('features', []))
                elif 'warnings' in data:
                    item_count = len(data.get('warnings', []))
                elif 'outages' in data:
                    item_count = len(data.get('outages', []))
                elif 'count' in data:
                    item_count = data.get('count', 0)
            elif isinstance(data, list):
                item_count = len(data)

            # Archive history
            history_records = _extract_history_records(cache_key, data)
            stats = {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}
            if history_records:
                # store_incidents_batch derives source_type from the first record for
                # its "no longer live" detection, so batches that mix sources lose
                # gone-tracking for every source but the first. The only mixed case
                # today is waze_hazards (hazards + jams), so split it.
                by_source = {}
                for r in history_records:
                    by_source.setdefault(r.get('source'), []).append(r)
                if len(by_source) > 1:
                    agg = {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}
                    for src, recs in by_source.items():
                        s = store_incidents_batch(recs, source_type=src)
                        for k in agg:
                            agg[k] += s.get(k, 0)
                    stats = agg
                else:
                    stats = store_incidents_batch(history_records)

            _prewarm_record_success(cache_key, name)

            # Log only when there's real change. Dev mode gets an extra
            # zero-activity line per source to confirm the loop is alive, but
            # only when the fetch took long enough to care about (>1s).
            has_real_change = stats['new'] > 0 or stats['changed'] > 0 or stats['ended'] > 0
            if has_real_change:
                ended_str = f" ✗{stats['ended']}" if stats['ended'] > 0 else ""
                Log.prewarm(f"{name}: +{stats['new']} Δ{stats['changed']}{ended_str} (≡{stats['unchanged']}) [{fetch_time_ms}ms]")
            elif DEV_MODE and fetch_time_ms > 1000:
                Log.prewarm(f"{name}: ≡{item_count} [{fetch_time_ms}ms]")

            return 'success', fetch_time_ms, stats

        # data is None (fetcher chose not to return data) — counts as a failure
        _prewarm_record_failure(cache_key, name, "no data")
    except Exception as e:
        # Keep the raw reason short and grep-friendly
        reason = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
        if len(reason) > 100:
            reason = reason[:100] + '…'
        _prewarm_record_failure(cache_key, name, reason)

    return 'failed', 0, {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}


def prewarm_loop():
    """Background loop that keeps all caches warm
    
    Refresh intervals:
    - Active mode (map page open): Every 60 seconds (1 min)
    - Idle mode (no one on page): Every 120 seconds (2 min)
    """
    global _prewarm_running
    last_fetch = {key: 0 for key, _ in PREWARM_CONFIG}

    Log.startup("Cache pre-warming started")

    # Initial prewarm of all endpoints (parallel)
    from concurrent.futures import ThreadPoolExecutor
    initial_start = time.time()

    # Aggregate stats + outcome counts
    total_stats = {'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}
    outcomes = {'success': 0, 'skipped': 0, 'failed': 0}

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for cache_key, ttl in PREWARM_CONFIG:
            futures.append(executor.submit(prewarm_single, cache_key, ttl))
        for f in futures:
            try:
                status, fetch_ms, stats = f.result()
                outcomes[status] = outcomes.get(status, 0) + 1
                total_stats['new'] += stats.get('new', 0)
                total_stats['changed'] += stats.get('changed', 0)
                total_stats['unchanged'] += stats.get('unchanged', 0)
                total_stats['ended'] += stats.get('ended', 0)
            except Exception:
                pass

    # Mark all as fetched
    now = time.time()
    for cache_key, _ in PREWARM_CONFIG:
        last_fetch[cache_key] = now

    elapsed_ms = int((time.time() - initial_start) * 1000)
    Log.prewarm(_format_cycle_summary(outcomes, total_stats, elapsed_ms, initial=True))
    
    # Start Central Watch browser worker thread for Vercel bypass
    _start_cw_browser_worker()
    
    # Note: Endeavour browser worker was started earlier (before initial fetch)
    
    # Start continuous CW image refresh worker (handles its own browser-ready wait)
    threading.Thread(target=_continuous_cw_image_worker, daemon=True, name='cw-images').start()
    
    # Refresh CW data from API in background (waits for browser)
    def _cw_startup_data_refresh():
        for _ in range(30):
            if _centralwatch_browser_ready or _shutdown_event.is_set():
                break
            time.sleep(1)
        _refresh_centralwatch_data()
    threading.Thread(target=_cw_startup_data_refresh, daemon=True).start()
    
    # Continuous refresh loop (CW images handled by _continuous_cw_image_worker thread)
    last_cw_data_refresh = time.time()
    last_cw_cookie_refresh = time.time()  # Vercel cookies refresh tracker
    last_active = None  # Track mode changes (None forces first log)
    while _prewarm_running:
        now = time.time()

        # Determine refresh interval based on whether any DATA page is open
        active = is_page_active()
        refresh_interval = PREWARM_ACTIVE_INTERVAL if active else PREWARM_IDLE_INTERVAL

        # Log only on mode transitions, not every cycle.
        if active != last_active and last_active is not None:
            Log.prewarm(f"Mode → {'ACTIVE (1m)' if active else 'IDLE (2m)'}")
        last_active = active
        
        # Check each endpoint - refresh all at the same interval
        needs_refresh = []
        for cache_key, ttl in PREWARM_CONFIG:
            if now - last_fetch.get(cache_key, 0) >= refresh_interval:
                needs_refresh.append((cache_key, ttl))
        
        # Refresh all stale endpoints in parallel
        if needs_refresh:
            refresh_start = time.time()

            cycle_stats = {'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}
            outcomes = {'success': 0, 'skipped': 0, 'failed': 0}

            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(prewarm_single, key, ttl): key for key, ttl in needs_refresh}
                for f in futures:
                    try:
                        status, fetch_ms, stats = f.result()
                        last_fetch[futures[f]] = now
                        outcomes[status] = outcomes.get(status, 0) + 1
                        cycle_stats['new'] += stats.get('new', 0)
                        cycle_stats['changed'] += stats.get('changed', 0)
                        cycle_stats['unchanged'] += stats.get('unchanged', 0)
                        cycle_stats['ended'] += stats.get('ended', 0)
                    except Exception:
                        pass

            elapsed_ms = int((time.time() - refresh_start) * 1000)
            # In prod: only log when there's meaningful change or a failure.
            # In dev: always log the cycle for rhythm, but keep it compact.
            has_change = cycle_stats['new'] > 0 or cycle_stats['changed'] > 0 or cycle_stats['ended'] > 0
            has_failure = outcomes.get('failed', 0) > 0
            if DEV_MODE or has_change or has_failure:
                Log.prewarm(_format_cycle_summary(outcomes, cycle_stats, elapsed_ms))
        
        # Restart Central Watch browser worker if it died
        if _playwright_available and now - last_cw_cookie_refresh >= 1800:
            last_cw_cookie_refresh = now
            if not _centralwatch_browser_ready:
                _start_cw_browser_worker()
        
        # Periodically refresh Central Watch data from API (every 10 minutes)
        # Updates JSON file on disk, in-memory data, and image timestamps
        if now - last_cw_data_refresh >= 600:
            last_cw_data_refresh = now
            threading.Thread(target=_refresh_centralwatch_data, daemon=True).start()
        
        # CW image refresh is handled by _continuous_cw_image_worker (every 30s)
        
        # Use short sleeps to allow faster shutdown
        if _shutdown_event.wait(timeout=5):
            break  # Shutdown signal received


def start_prewarm_thread():
    """Start the cache pre-warming thread"""
    global _prewarm_thread, _prewarm_running
    if _prewarm_thread is None or not _prewarm_thread.is_alive():
        _prewarm_running = True
        _prewarm_thread = threading.Thread(target=prewarm_loop, daemon=True)
        _prewarm_thread.start()
        Log.startup("Prewarm thread started")


def stop_prewarm_thread():
    """Stop the cache pre-warming thread"""
    global _prewarm_running
    _prewarm_running = False
    Log.info("Prewarm thread stopped")


def cached(ttl=CACHE_TTL):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = func.__name__ + str(args) + str(kwargs)
            now = time.time()
            if key in cache and now - cache[key]['time'] < ttl:
                return cache[key]['data']
            result = func(*args, **kwargs)
            cache[key] = {'data': result, 'time': now, 'ttl': ttl}
            return result
        return wrapper
    return decorator

# ============== API KEY AUTHENTICATION ==============
# Set your API key via environment variable or use the default
# Generate a secure key: python -c "import secrets; print(secrets.token_urlsafe(32))"
API_KEY = os.environ.get('NSWPSN_API_KEY', '')

# Public endpoints that don't require auth (health checks, etc)
# /api/debug/sessions and /api/debug/heartbeat-test used to be here — removed
# because they leak client IPs and user-agents. They now require NSWPSN_API_KEY.
PUBLIC_ENDPOINTS = {'/api/health', '/', '/api/config', '/api/heartbeat', '/api/editor-requests',
                    # /api/waze/ingest has its own auth (X-Ingest-Key matched against WAZE_INGEST_KEY);
                    # adding to public here means NSWPSN_API_KEY isn't required, so a userscript in
                    # a random user's browser can POST without knowing the full backend API key.
                    '/api/waze/ingest',
                    # Read-only filter catalogue — used by /logs and the dashboard to populate
                    # dropdowns. No PII, just aggregated category/source/severity counts.
                    '/api/data/history/filters'}
# Endpoints that start with these prefixes are public (for dynamic routes like /api/check-editor/<user_id>)
PUBLIC_ENDPOINT_PREFIXES = ['/api/check-editor/', '/api/centralwatch/image/', '/api/centralwatch/cameras',
                            # Dashboard endpoints use Discord OAuth2 session cookie auth
                            # instead of the NSWPSN_API_KEY header. See DASHBOARD section.
                            '/api/dashboard/']

def require_api_key(func):
    """Decorator to require API key authentication"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Skip auth for CORS preflight requests
        if request.method == 'OPTIONS':
            return '', 200
        
        # Check Authorization header first (preferred)
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            provided_key = auth_header[7:]  # Remove 'Bearer ' prefix
        else:
            # Fall back to X-API-Key header
            provided_key = request.headers.get('X-API-Key', '')
        
        # Also check query parameter as last resort
        if not provided_key:
            provided_key = request.args.get('api_key', '')
        
        if not provided_key:
            return jsonify({
                'error': 'API key required',
                'message': 'Provide API key via Authorization: Bearer <key> header or X-API-Key header'
            }), 401
        
        if provided_key != API_KEY:
            return jsonify({
                'error': 'Invalid API key',
                'message': 'The provided API key is not valid'
            }), 403
        
        return func(*args, **kwargs)
    return wrapper

# Apply API key auth to all /api/ routes automatically
@app.before_request
def check_api_key():
    """Check API key for all /api/ routes"""
    # Skip auth for public endpoints (exact match)
    if request.path in PUBLIC_ENDPOINTS:
        return None
    
    # Skip auth for public endpoint prefixes (dynamic routes)
    for prefix in PUBLIC_ENDPOINT_PREFIXES:
        if request.path.startswith(prefix):
            return None
    
    # Skip auth for non-API routes
    if not request.path.startswith('/api/'):
        return None
    
    # Skip OPTIONS requests (CORS preflight)
    if request.method == 'OPTIONS':
        return None
    
    # Check Authorization header first (preferred)
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        provided_key = auth_header[7:]
    else:
        provided_key = request.headers.get('X-API-Key', '')
    
    # Also check query parameter as last resort
    if not provided_key:
        provided_key = request.args.get('api_key', '')
    
    if not provided_key:
        return jsonify({
            'error': 'API key required',
            'message': 'Provide API key via Authorization: Bearer <key> header or X-API-Key header'
        }), 401
    
    if provided_key != API_KEY:
        return jsonify({
            'error': 'Invalid API key',
            'message': 'The provided API key is not valid'
        }), 403
    
    return None

@app.route('/api/ausgrid/outages')
@require_api_key
def ausgrid_outages():
    """Ausgrid power outages - returns markers and polygons"""
    # Check persistent cache first (populated by prewarm)
    cached_data, age, expired = cache_get('ausgrid_outages')
    if cached_data is not None:
        return jsonify(cached_data)
    
    # Fallback to live fetch if cache empty. Reuse the prewarm helper so the
    # bbox/zoom params, browser-style headers, and response normalisation
    # stay consistent with the prewarm path.
    try:
        data = _prewarm_fetch_ausgrid('outages')
        if data and (data.get('Markers') or data.get('Polygons')):
            cache_set('ausgrid_outages', data, CACHE_TTL_AUSGRID)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e), 'Markers': [], 'Polygons': []}), 200


@app.route('/api/ausgrid/stats')
@require_api_key
def ausgrid_stats():
    """Ausgrid current outage statistics"""
    # Check persistent cache first (populated by prewarm)
    cached_data, age, expired = cache_get('ausgrid_stats')
    if cached_data is not None:
        return jsonify(cached_data)
    
    # Fallback to live fetch if cache empty
    try:
        r = requests.get(
            'https://www.ausgrid.com.au/webapi/outagemapdata/GetCurrentOutageStats',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        data = r.json()
        cache_set('ausgrid_stats', data, CACHE_TTL_AUSGRID)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 200


@app.route('/api/essential/outages')
@require_api_key
def essential_outages():
    """Essential Energy power outages from KML feeds (current + future).
    Covers regional/rural NSW (complements Ausgrid + Endeavour for state-wide coverage).
    
    Query parameters:
    - type: filter by outage type ('planned', 'unplanned', 'future', default: all)
    - feed: filter by feed ('current', 'future', default: all)
    """
    all_outages = []
    
    # Collect from current + future feeds (cache populated by prewarm)
    for cache_key, feed_type in [('essential_energy', 'current'), ('essential_energy_future', 'future')]:
        cached_data, age, expired = cache_get(cache_key)
        if cached_data is not None and isinstance(cached_data, list):
            all_outages.extend(cached_data)
        elif cache_key == 'essential_energy':
            # Fallback to live fetch for current only if cache empty
            try:
                r = requests.get(ESSENTIAL_ENERGY_FEEDS['current'],
                                timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
                if r.status_code == 200:
                    outages = _parse_essential_energy_kml(r.content, feed_type='current')
                    cache_set('essential_energy', outages, CACHE_TTL_ESSENTIAL)
                    all_outages.extend(outages)
            except Exception as e:
                Log.error(f"Essential Energy error: {e}")
    
    # Apply feed filter if specified
    feed_filter = request.args.get('feed', '').lower()
    if feed_filter in ('current', 'future'):
        all_outages = [o for o in all_outages if o.get('feedType') == feed_filter]

    # Apply type filter if specified
    outage_type = request.args.get('type', '').lower()
    if outage_type in ('planned', 'unplanned'):
        all_outages = [o for o in all_outages if o.get('outageType') == outage_type]

    # Calculate summary stats
    planned_count = sum(1 for o in all_outages if o.get('outageType') == 'planned')
    unplanned_count = sum(1 for o in all_outages if o.get('outageType') == 'unplanned')
    future_count = sum(1 for o in all_outages if o.get('feedType') == 'future')
    total_customers = sum(o.get('customersAffected', 0) for o in all_outages)

    # Lite mode: strip the polygon coordinate arrays for callers that
    # only render point markers (the map). Polygons can be hundreds of
    # coordinate pairs each and dominate the response size; the bot
    # endpoints (/api/essential/planned, /api/essential/future) use a
    # different code path and keep their full data for embed/preset use.
    if request.args.get('lite') == '1':
        all_outages = [
            {k: v for k, v in o.items() if k != 'polygon'}
            for o in all_outages
        ]

    return jsonify({
        'outages': all_outages,
        'count': len(all_outages),
        'planned': planned_count,
        'unplanned': unplanned_count,
        'future': future_count,
        'totalCustomersAffected': total_customers
    })


@app.route('/api/essential/outages/current')
@require_api_key
def essential_outages_current():
    """Essential Energy current (active) outages only"""
    cached_data, age, expired = cache_get('essential_energy')
    outages = cached_data if cached_data and isinstance(cached_data, list) else []
    return jsonify({
        'outages': outages,
        'count': len(outages),
        'totalCustomersAffected': sum(o.get('customersAffected', 0) for o in outages)
    })


@app.route('/api/essential/outages/future')
@require_api_key
def essential_outages_future():
    """Essential Energy future planned outages"""
    cached_data, age, expired = cache_get('essential_energy_future')
    outages = cached_data if cached_data and isinstance(cached_data, list) else []
    return jsonify({
        'outages': outages,
        'count': len(outages),
        'totalCustomersAffected': sum(o.get('customersAffected', 0) for o in outages)
    })


@app.route('/api/essential/planned')
@require_api_key
def essential_planned():
    """Bot-canonical alias — planned outages across current + future feeds."""
    out = []
    for cache_key in ('essential_energy', 'essential_energy_future'):
        cached_data, age, expired = cache_get(cache_key)
        if cached_data and isinstance(cached_data, list):
            out.extend(o for o in cached_data
                       if (o.get('outageType') or '').lower() == 'planned')
    return jsonify({
        'outages': out,
        'count': len(out),
        'totalCustomersAffected': sum(o.get('customersAffected', 0) for o in out)
    })


@app.route('/api/essential/future')
@require_api_key
def essential_future():
    """Bot-canonical alias for /api/essential/outages/future."""
    cached_data, age, expired = cache_get('essential_energy_future')
    outages = cached_data if cached_data and isinstance(cached_data, list) else []
    return jsonify({
        'outages': outages,
        'count': len(outages),
        'totalCustomersAffected': sum(o.get('customersAffected', 0) for o in outages)
    })


@app.route('/api/essential/outages/raw')
@require_api_key
def essential_outages_raw():
    """Essential Energy raw KML data converted to JSON (all feeds)"""
    all_outages = []
    for feed_type, url in ESSENTIAL_ENERGY_FEEDS.items():
        try:
            r = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
            if r.status_code == 200:
                all_outages.extend(_parse_essential_energy_kml(r.content, feed_type=feed_type))
        except Exception as e:
            Log.error(f"Essential Energy raw ({feed_type}) error: {e}")
    return jsonify(all_outages)


@app.route('/api/endeavour/maintenance')
@require_api_key
def endeavour_maintenance():
    """Endeavour Energy current maintenance (planned outages currently active)"""
    # Check persistent cache first (populated by prewarm)
    cached_data, age, expired = cache_get('endeavour_maintenance')
    if cached_data is not None:
        return jsonify(cached_data)
    
    # Fallback to live fetch from Supabase
    try:
        all_data = _fetch_endeavour_all_outages()
        result = all_data.get('current_maintenance', [])
        cache_set('endeavour_maintenance', result, CACHE_TTL_ENDEAVOUR_MAINTENANCE)
        return jsonify(result)
    except Exception as e:
        Log.error(f"Endeavour maintenance error: {e}")
        return jsonify([]), 200


@app.route('/api/endeavour/future')
@require_api_key
def endeavour_future():
    """Endeavour Energy future/scheduled maintenance"""
    # Check persistent cache first (populated by prewarm)
    cached_data, age, expired = cache_get('endeavour_future')
    if cached_data is not None:
        return jsonify(cached_data)
    
    # Fallback to live fetch from Supabase
    try:
        all_data = _fetch_endeavour_all_outages()
        result = all_data.get('future_maintenance', [])
        cache_set('endeavour_future', result, CACHE_TTL_ENDEAVOUR_FUTURE)
        return jsonify(result)
    except Exception as e:
        Log.error(f"Endeavour future error: {e}")
        return jsonify([]), 200


@app.route('/api/endeavour/planned')
@require_api_key
def endeavour_planned():
    """Bot-canonical alias — current maintenance + future scheduled, flat list."""
    items = []
    for cache_key in ('endeavour_maintenance', 'endeavour_future'):
        cached_data, age, expired = cache_get(cache_key)
        if cached_data and isinstance(cached_data, list):
            items.extend(cached_data)
    if not items:
        try:
            all_data = _fetch_endeavour_all_outages()
            items.extend(all_data.get('current_maintenance', []) or [])
            items.extend(all_data.get('future_maintenance', []) or [])
        except Exception as e:
            Log.error(f"Endeavour planned error: {e}")
    return jsonify(items)


@app.route('/api/endeavour/future/raw')
@require_api_key
@cached(ttl=CACHE_TTL_ENDEAVOUR_FUTURE)
def endeavour_future_raw():
    """Endeavour Energy future outages - raw Supabase data"""
    try:
        areas = _fetch_endeavour_supabase('/rpc/get_outage_areas_fast', method='POST', body={})
        if areas:
            planned = [a for a in areas if (a.get('outage_type') or '').upper() == 'PLANNED']
            return jsonify(planned)
        return jsonify([])
    except Exception as e:
        Log.error(f"Endeavour future raw error: {e}")
        return jsonify([]), 200


@app.route('/api/endeavour/future/all')
@require_api_key
@cached(ttl=CACHE_TTL_ENDEAVOUR_FUTURE)
def endeavour_future_all():
    """Endeavour Energy future outages - all including completed"""
    try:
        all_data = _fetch_endeavour_all_outages()
        return jsonify(all_data.get('future', []))
    except Exception as e:
        Log.error(f"Endeavour future all error: {e}")
        return jsonify([]), 200


@app.route('/api/endeavour/postcodes')
@cached(ttl=3600)  # Cache for 1 hour - this data rarely changes
def endeavour_postcodes():
    """Endeavour Energy service area postcodes - fetches distinct postcodes from outage points"""
    try:
        params = {'select': 'postcode', 'limit': 5000}
        data = _fetch_endeavour_supabase('/outage-points', params=params)
        if data:
            postcodes = sorted(set(p.get('postcode') for p in data if p.get('postcode')))
            return jsonify({
                'postcodes': postcodes,
                'count': len(postcodes)
            })
        return jsonify({'postcodes': [], 'count': 0})
    except Exception as e:
        Log.error(f"Endeavour postcodes error: {e}")
        return jsonify({'postcodes': [], 'count': 0}), 200


@app.route('/api/traffic/lga-incidents')
@cached(ttl=CACHE_TTL_TRAFFIC_LGA)
def traffic_lga_incidents():
    """Live Traffic NSW - LGA regional incidents"""
    features = []
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/regional/lga-incidents.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get('features', [])
            for item in items:
                feature = parse_traffic_item(item, 'LGA Incident')
                if feature:
                    features.append(feature)
    except Exception as e:
        Log.error(f"Traffic LGA incidents error: {e}")
    
    return jsonify({'type': 'FeatureCollection', 'features': features})


def _categorize_bom_warning(title: str, description: str = '') -> str:
    """Categorize a BOM warning as land or marine based on content"""
    title_lower = (title or '').lower()
    desc_lower = (description or '').lower()
    combined = f"{title_lower} {desc_lower}"
    
    # Marine indicators
    marine_keywords = [
        'marine', 'surf', 'coastal', 'ocean', 'sea', 'wind warning summary',
        'gale', 'storm force', 'hurricane force', 'swell', 'wave',
        'coastal waters', 'offshore', 'boating', 'shipping'
    ]
    
    # Check for marine first (more specific)
    for keyword in marine_keywords:
        if keyword in combined:
            return 'marine'
    
    # Default to land for all other warnings
    return 'land'


def _extract_bom_warning_type(title: str) -> str:
    """
    Extract the specific warning type from a BOM warning title.
    Returns types like: Wind, Flood, Thunderstorm, Weather, Surf, Heatwave, etc.
    """
    if not title:
        return 'Warning'
    
    title_lower = title.lower()
    
    # Order matters - check more specific patterns first
    warning_patterns = [
        # Specific warning types
        ('thunderstorm', 'Thunderstorm'),
        ('flood', 'Flood'),
        ('heatwave', 'Heatwave'),
        ('sheep graziers', 'Sheep Graziers'),
        ('fire weather', 'Fire Weather'),
        ('damaging wind', 'Damaging Winds'),
        ('hazardous surf', 'Surf'),
        ('surf warning', 'Surf'),
        ('wind warning summary', 'Wind'),
        ('marine wind', 'Wind'),
        ('gale warning', 'Gale'),
        ('storm warning', 'Storm'),
        ('cyclone', 'Cyclone'),
        ('tsunami', 'Tsunami'),
        ('blizzard', 'Blizzard'),
        ('frost', 'Frost'),
        ('heat', 'Heat'),
        ('dust', 'Dust'),
        ('bushfire', 'Bushfire'),
        ('avalanche', 'Avalanche'),
        # General severe weather (check after more specific)
        ('severe weather', 'Severe Weather'),
        # Fallback for general warnings
        ('warning', 'Warning'),
    ]
    
    for pattern, warning_type in warning_patterns:
        if pattern in title_lower:
            return warning_type
    
    return 'Warning'


def _get_bom_warning_severity(title: str) -> str:
    """Determine warning severity level"""
    title_lower = (title or '').lower()
    
    if 'severe' in title_lower or 'emergency' in title_lower or 'extreme' in title_lower:
        return 'severe'
    elif 'warning' in title_lower:
        return 'warning'
    elif 'watch' in title_lower:
        return 'watch'
    elif 'advice' in title_lower or 'summary' in title_lower:
        return 'advice'
    else:
        return 'info'


def _fetch_all_bom_warnings():
    """Fetch all BOM warnings from IDZ00054 (contains all land and marine warnings)
    
    This is the master source - IDZ00061 (land) and IDZ00068 (marine) are subsets of this feed.
    Using only IDZ00054 prevents duplicate warnings.
    """
    warnings = []
    seen_titles = set()  # Additional dedup by title
    
    try:
        r = requests.get(
            'https://www.bom.gov.au/fwo/IDZ00054.warnings_nsw.xml',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code != 200:
            source_error('bom', f'HTTP {r.status_code}')
        if r.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.content)

            # Try <warning> elements first
            for warning in root.findall('.//warning'):
                title = warning.findtext('title', warning.findtext('headline', ''))
                description = warning.findtext('description', '')
                
                # Skip if we've seen this title
                title_key = title.strip().lower()
                if title_key in seen_titles:
                    continue
                seen_titles.add(title_key)
                
                category = _categorize_bom_warning(title, description)
                severity = _get_bom_warning_severity(title)
                
                warnings.append({
                    'title': title,
                    'type': warning.get('type', category),
                    'category': category,
                    'severity': severity,
                    'description': description,
                    'area': warning.findtext('area', ''),
                    'issued': warning.findtext('issued', warning.findtext('issue-time-local', '')),
                    'expiry': warning.findtext('expiry', warning.findtext('expiry-time-local', '')),
                    'link': '',
                    'source': 'bom'
                })
            
            # Try <item> elements (RSS format) if no <warning> elements found
            if not warnings:
                for item in root.findall('.//item'):
                    title = item.findtext('title', '')
                    description = item.findtext('description', '')
                    
                    # Skip if we've seen this title
                    title_key = title.strip().lower()
                    if title_key in seen_titles:
                        continue
                    seen_titles.add(title_key)
                    
                    category = _categorize_bom_warning(title, description)
                    severity = _get_bom_warning_severity(title)
                    
                    warnings.append({
                        'title': title,
                        'type': category,
                        'category': category,
                        'severity': severity,
                        'description': description,
                        'area': '',
                        'link': item.findtext('link', ''),
                        'issued': item.findtext('pubDate', ''),
                        'expiry': '',
                        'source': 'bom'
                    })
            source_ok('bom')
    except Exception as e:
        source_error('bom', e)
        Log.error(f"BOM warnings fetch error: {e}")

    return warnings


@app.route('/api/bom/warnings')
@require_api_key
def bom_warnings_combined():
    """Combined BOM Warnings - deduplicated from single source (uses persistent cache)
    
    This endpoint returns all NSW warnings without duplicates.
    Each warning includes a 'category' field: 'land' or 'marine'
    """
    cached_data, age, expired = cache_get('bom_warnings')
    
    # Return fresh cache if not expired
    if cached_data and not expired:
        return jsonify(cached_data)
    
    # Try to fetch fresh data
    try:
        warnings = _fetch_all_bom_warnings()
        
        # Count by category
        counts = {'land': 0, 'marine': 0}
        for w in warnings:
            cat = w.get('category', 'land')
            if cat in counts:
                counts[cat] += 1
        
        result = {
            'warnings': warnings,
            'count': len(warnings),
            'counts': counts
        }
        cache_set('bom_warnings', result, CACHE_TTL_BOM)
        return jsonify(result)
    except Exception as e:
        Log.error(f"BOM warnings fetch error: {e}")
        # Fall back to stale cache if fetch fails
        if cached_data:
            return jsonify(cached_data)
        return jsonify({'warnings': [], 'count': 0, 'counts': {'land': 0, 'marine': 0}})




@app.route('/api/beachwatch')
@require_api_key
def beachwatch():
    """NSW Beachwatch water quality data - GeoJSON"""
    # Check persistent cache first (populated by prewarm)
    cached_data, age, expired = cache_get('beachwatch')
    if cached_data is not None:
        return jsonify(cached_data)
    
    # Fallback to live fetch if cache empty
    try:
        r = requests.get(
            'https://api.beachwatch.nsw.gov.au/public/sites/geojson',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            data = r.json()
            cache_set('beachwatch', data, CACHE_TTL_BEACH)
            return jsonify(data)
    except Exception as e:
        Log.error(f"Beachwatch error: {e}")
    
    return jsonify({'type': 'FeatureCollection', 'features': []})


@app.route('/api/beachsafe')
@require_api_key
def beachsafe():
    """BeachSafe surf conditions and patrol data from Surf Life Saving Australia"""
    # Check persistent cache first (populated by prewarm)
    cached_data, age, expired = cache_get('beachsafe')
    if cached_data is not None:
        return jsonify(cached_data)
    
    # Fallback to live fetch if cache empty
    try:
        # NSW bounding box - covers entire state coastline
        ne_lat, ne_lon = -28.0, 154.0
        sw_lat, sw_lon = -37.5, 149.0
        
        url = f'https://beachsafe.org.au/api/v4/map/beaches?neCoords[]={ne_lat}&neCoords[]={ne_lon}&swCoords[]={sw_lat}&swCoords[]={sw_lon}'
        
        r = requests.get(
            url,
            timeout=15,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Referer': 'https://beachsafe.org.au/'
            }
        )
        if r.status_code == 200:
            data = r.json()
            # API returns {beaches: [...], place: ...} - extract the beaches array
            if isinstance(data, dict) and 'beaches' in data:
                beaches = data.get('beaches', [])
                # Normalize field names for frontend compatibility
                normalized = []
                for b in beaches:
                        # Extract slug from URL for detail lookups (e.g. /nsw/waverley/bondi-beach -> bondi-beach)
                    beach_url = b.get('url', '')
                    beach_slug = beach_url.rstrip('/').split('/')[-1] if beach_url else ''

                    patrol_today = b.get('is_patrolled_today', {})
                    if not isinstance(patrol_today, dict):
                        patrol_today = {}

                    normalized.append({
                        'id': b.get('id'),
                        'name': b.get('title', 'Unknown Beach'),
                        'lat': float(b.get('latitude', 0)) if b.get('latitude') else None,
                        'lng': float(b.get('longitude', 0)) if b.get('longitude') else None,
                        'url': beach_url,
                        'slug': beach_slug,
                        'patrolled': b.get('status', '').lower() == 'patrolled',
                        'status': b.get('status', 'Unknown'),
                        'hasToilet': bool(b.get('has_toilet')),
                        'hasParking': bool(b.get('has_parking')),
                        'dogsAllowed': bool(b.get('has_dogs_allowed')),
                        'image': b.get('image', ''),
                        'weather': b.get('weather', {}),
                        'hazards': b.get('hazards') or [],
                        'isPatrolledToday': patrol_today.get('flag', False),
                        'patrolStart': patrol_today.get('start', ''),
                        'patrolEnd': patrol_today.get('end', ''),
                        'patrol': b.get('patrol', 0),
                    })
                cache_set('beachsafe', normalized, CACHE_TTL_BEACH)
                return jsonify(normalized)
            elif isinstance(data, list):
                cache_set('beachsafe', data, CACHE_TTL_BEACH)
                return jsonify(data)
    except Exception as e:
        Log.error(f"BeachSafe error: {e}")
    
    return jsonify([])


@app.route('/api/beachsafe/beach/<slug>')
@require_api_key
def beachsafe_beach_detail(slug):
    """Fetch detailed beach data from BeachSafe for a specific beach"""
    # Sanitize slug
    slug = slug.strip().lower().replace(' ', '-')
    if not slug or len(slug) > 100:
        return jsonify({'error': 'Invalid slug'}), 400

    cache_key = f'beachsafe_detail_{slug}'
    cached_data, age, expired = cache_get(cache_key)
    if cached_data is not None and not expired:
        return jsonify(cached_data)

    try:
        url = f'https://beachsafe.org.au/api/v4/beach/{slug}'
        r = requests.get(
            url,
            timeout=10,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Referer': 'https://beachsafe.org.au/'
            }
        )
        if r.status_code == 200:
            data = r.json()
            beach = data.get('beach', {})

            # Extract latest attendance (most recent day)
            attendances = beach.get('attendances', {})
            latest_attendance = None
            if attendances:
                last_key = list(attendances.keys())[-1] if attendances else None
                if last_key:
                    entries = attendances[last_key]
                    if entries:
                        latest_attendance = {
                            'date': last_key,
                            'entries': entries
                        }

            patrol_today = beach.get('is_patrolled_today', {})
            if not isinstance(patrol_today, dict):
                patrol_today = {}

            result = {
                'weather': beach.get('weather', {}),
                'currentTide': beach.get('currentTide'),
                'currentUV': beach.get('currentUV'),
                'latestAttendance': latest_attendance,
                'todays_marine_warnings': beach.get('todays_marine_warnings', []),
                'patrol': beach.get('patrol', 0),
                'patrolStart': patrol_today.get('start', ''),
                'patrolEnd': patrol_today.get('end', ''),
                'isPatrolledToday': patrol_today.get('flag', False),
                'status': beach.get('status', 'Unknown'),
                'hazard': beach.get('hazard', 0),
            }
            cache_set(cache_key, result, 300)  # 5 min cache
            return jsonify(result)
    except Exception as e:
        Log.error(f"BeachSafe detail error for {slug}: {e}")

    # Return stale cache if available
    if cached_data is not None:
        return jsonify(cached_data)
    return jsonify({})


@app.route('/api/beachsafe/details')
@require_api_key
def beachsafe_all_details():
    """Return all pre-fetched beach detail data as a dict keyed by slug"""
    cached_data, age, expired = cache_get('beachsafe_details')
    if cached_data is not None:
        return jsonify(cached_data)
    return jsonify({})


# ============== NEWS RSS FEEDS ==============
# Australian news RSS feeds relevant to NSW emergency services

RSS_FEEDS = {
    'abc': {
        'name': 'ABC News',
        'url': 'https://www.abc.net.au/news/feed/2942460/rss.xml',
        'icon': '📰'
    },
    'news_com_au': {
        'name': 'news.com.au',
        'url': 'https://www.news.com.au/content-feeds/latest-news-national/',
        'icon': '📰'
    },
    'nine': {
        'name': '9News',
        'url': 'https://www.9news.com.au/rss',
        'icon': '📺'
    },
    'smh': {
        'name': 'Sydney Morning Herald',
        'url': 'https://www.smh.com.au/rss/feed.xml',
        'icon': '📄'
    },
    'sydney_sun': {
        'name': 'Sydney Sun',
        'url': 'http://feeds.sydneysun.com/rss/ae0def0d9b645403',
        'icon': '☀️'
    },
    'sbs': {
        'name': 'SBS News',
        'url': 'https://www.sbs.com.au/news/feed',
        'icon': '📡'
    },
    'nsw_police': {
        'name': 'NSW Police',
        'url': 'https://www.police.nsw.gov.au/news/nsw_police_news.rss',
        'icon': '👮'
    },
    'frnsw': {
        'name': 'Fire & Rescue NSW',
        'url': 'https://www.fire.nsw.gov.au/feeds/feed.rss',
        'icon': '🚒'
    }
}

# Keywords for auto-categorizing articles
CATEGORY_KEYWORDS = {
    # Strong emergency indicators - things that are clearly emergencies
    'emergency_strong': [
        # Emergency services
        'emergency', 'evacuate', 'evacuation', 'rescue', 'rescued',
        'ambulance', 'paramedic', 'triple zero', '000',
        'rfs', 'ses', 'frnsw', 'firefighter', 'firefighters',
        
        # Serious incidents
        'crash', 'collision', 'accident', 'fatal', 'fatality',
        'death', 'dead', 'killed', 'dies', 'died', 'body found',
        'injured', 'injury', 'injuries', 'critical condition', 'hospital',
        'missing person', 'search and rescue', 'found dead',
        'disaster', 'catastrophe', 'crisis',
        
        # Crime - violence
        'police', 'arrest', 'arrested', 'charged', 'custody', 'detained',
        'shooting', 'shot', 'gunman', 'gunmen', 'armed',
        'stabbing', 'stabbed', 'knife attack',
        'attack', 'attacked', 'assault', 'assaulted', 'bashing', 'bashed',
        'murder', 'murdered', 'homicide', 'manslaughter',
        'terror', 'terrorism', 'terrorist',
        'hostage', 'siege',
        'mauled', 'mauling', 'dog attack', 'bitten',
        
        # Crime - property/other
        'robbery', 'robbed', 'ram raid', 'raid', 'theft', 'stolen',
        'break-in', 'burglary', 'home invasion',
        'carjacking', 'carjacked',
        'drug bust', 'drug raid',
        
        # Fire
        'blaze', 'bushfire', 'wildfire', 'flames', 'inferno',
        'house fire', 'building fire', 'factory fire', 'car fire',
        
        # Infrastructure
        'power outage', 'blackout',
        'explosion', 'exploded', 'bomb',
        'derailed', 'derailment',
        
        # Maritime/aviation
        'capsized', 'sinking', 'aground', 'mayday',
        'plane crash', 'helicopter crash'
    ],
    # Weather-specific indicators - must be clearly weather-related phrases
    'weather': [
        'weather forecast', 'weather warning', 'weather conditions', 'weather event',
        'severe weather', 'wet weather', 'wild weather', 'extreme weather',
        'heatwave', 'heat wave', 'cold snap', 'cold front', 'warm front',
        'rainfall', 'downpour', 'heavy rain', 'rain warning',
        'thunderstorm', 'lightning', 'stormy weather',
        'cyclone', 'tropical cyclone', 'tornado', 'hurricane',
        'flood warning', 'flood watch',
        'bom', 'bureau of meteorology',
        'drought', 'dry conditions',
        'snowfall', 'frost warning', 'fog warning', 'hailstorm',
        'temperature', 'celsius', 'humidity'
    ],
    # Fire-related (check after weather for fire danger ratings)
    'fire': [
        'fire danger', 'total fire ban', 'fire ban', 'fire risk',
        'fire', 'burning'
    ],
    # Flood-related (can be weather or emergency depending on context)
    'flood': [
        'flood', 'flooding', 'floodwater', 'floods'
    ]
}

def _word_match(keyword, text):
    """Check if keyword exists as a whole word/phrase in text (not as substring)"""
    import re
    # Escape special regex characters in keyword
    pattern = r'\b' + re.escape(keyword) + r'\b'
    return bool(re.search(pattern, text))

def _detect_category(title, description):
    """Auto-detect article category based on keywords in title and description"""
    text = f"{title} {description}".lower()
    
    # Check for strong emergency indicators first (unambiguous emergencies)
    for keyword in CATEGORY_KEYWORDS['emergency_strong']:
        if _word_match(keyword, text):
            return 'emergency'
    
    # Check for weather context - if it has weather words, likely a weather article
    has_weather_context = any(_word_match(kw, text) for kw in CATEGORY_KEYWORDS['weather'])
    
    # Fire-related: check context
    has_fire = any(_word_match(kw, text) for kw in CATEGORY_KEYWORDS['fire'])
    if has_fire:
        # If it's about fire danger/warnings/ban, it's weather-related
        if any(_word_match(term, text) for term in ['fire danger', 'fire ban', 'fire risk', 'fire weather']):
            return 'weather'
        # Otherwise it's an emergency (actual fire)
        return 'emergency'
    
    # Flood-related: check context
    has_flood = any(_word_match(kw, text) for kw in CATEGORY_KEYWORDS['flood'])
    if has_flood:
        # If it mentions warning/watch or has weather context, it's weather
        if _word_match('warning', text) or _word_match('watch', text) or has_weather_context:
            return 'weather'
        # Otherwise it's an emergency (actual flood event)
        return 'emergency'
    
    # If it has weather context, classify as weather
    if has_weather_context:
        return 'weather'
    
    return 'general'


_rss_feed_backoff = {}        # url -> (backoff_until_ts, last_status)
_rss_feed_fail_counts = {}    # url -> consecutive fail count
_rss_feed_backoff_lock = threading.Lock()
_RSS_BACKOFF_THRESHOLD = 2
_RSS_BACKOFF_STEPS = [600, 1800, 3600, 14400]  # 10m, 30m, 1h, 4h

def _parse_rss_feed(url, source_name, source_icon):
    """Parse an RSS feed and return normalized items with auto-detected categories.
    Persistent HTTP failures (e.g. 403/404) are parked with exponential backoff
    so a permanently-broken feed (NSW Police 403, F&RNSW 404) doesn't fire every
    time /api/news/rss is called."""
    import xml.etree.ElementTree as ET

    items = []
    # Backoff gate: skip the fetch entirely if this URL is parked.
    now = time.time()
    with _rss_feed_backoff_lock:
        until, _ = _rss_feed_backoff.get(url, (0, 0))
    if until > now:
        return items

    try:
        r = requests.get(url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*'
        })

        if r.status_code != 200:
            with _rss_feed_backoff_lock:
                n = _rss_feed_fail_counts.get(url, 0) + 1
                _rss_feed_fail_counts[url] = n
                parked_for = 0
                if n >= _RSS_BACKOFF_THRESHOLD:
                    step = min(n - _RSS_BACKOFF_THRESHOLD, len(_RSS_BACKOFF_STEPS) - 1)
                    parked_for = _RSS_BACKOFF_STEPS[step]
                    _rss_feed_backoff[url] = (time.time() + parked_for, r.status_code)
            if parked_for:
                mins = parked_for // 60
                Log.warn(f"RSS feed {source_name} {r.status_code} — backoff {mins}m (fail #{n})")
            else:
                Log.warn(f"RSS feed {source_name} returned status {r.status_code} ({n}/{_RSS_BACKOFF_THRESHOLD})")
            return items

        # Success — clear any failure state
        with _rss_feed_backoff_lock:
            had_fails = _rss_feed_fail_counts.pop(url, 0) > 0
            was_parked = _rss_feed_backoff.pop(url, None) is not None
        if had_fails and was_parked:
            Log.info(f"RSS feed {source_name}: ✓ recovered")
        
        # Parse XML
        root = ET.fromstring(r.content)
        
        # Find channel and items (handle different RSS formats)
        channel = root.find('channel')
        if channel is None:
            # Might be Atom format
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            entries = root.findall('atom:entry', ns) or root.findall('entry')
            for entry in entries[:10]:  # Limit to 10 items per feed
                title = entry.findtext('atom:title', '', ns) or entry.findtext('title', '')
                link = entry.find('atom:link', ns)
                if link is not None:
                    link = link.get('href', '')
                else:
                    link = entry.findtext('link', '')
                summary = entry.findtext('atom:summary', '', ns) or entry.findtext('summary', '')
                published = entry.findtext('atom:published', '', ns) or entry.findtext('published', '') or entry.findtext('atom:updated', '', ns)
                
                # Clean up values
                title = title.strip() if title else ''
                description = summary.strip()[:300] if summary else ''
                
                items.append({
                    'title': title,
                    'link': link.strip() if link else '',
                    'description': description,
                    'published': published.strip() if published else '',
                    'source': source_name,
                    'icon': source_icon,
                    'category': _detect_category(title, description)
                })
        else:
            # Standard RSS format
            for item in channel.findall('item')[:10]:  # Limit to 10 items per feed
                title = item.findtext('title', '')
                link = item.findtext('link', '')
                description = item.findtext('description', '')
                pub_date = item.findtext('pubDate', '')
                
                # Clean up description (remove HTML tags)
                if description:
                    description = re.sub(r'<[^>]+>', '', description)
                    description = description.strip()[:300]
                
                # Clean up values
                title = title.strip() if title else ''
                description = description if description else ''
                
                items.append({
                    'title': title,
                    'link': link.strip() if link else '',
                    'description': description,
                    'published': pub_date.strip() if pub_date else '',
                    'source': source_name,
                    'icon': source_icon,
                    'category': _detect_category(title, description)
                })
    
    except ET.ParseError as e:
        Log.error(f"RSS XML parse error for {source_name}: {e}")
    except Exception as e:
        Log.error(f"RSS fetch error for {source_name}: {e}")
    
    return items


@app.route('/api/news/rss')
@cached(ttl=CACHE_TTL_RSS)
def news_rss():
    """Fetch and aggregate news from multiple RSS feeds
    
    Query parameters:
    - sources: comma-separated list of source keys (default: all)
    - category: filter results by auto-detected category (general, emergency, weather)
    - limit: max items per source (default: 8, max: 20)
    """
    # Parse query parameters
    requested_sources = request.args.get('sources', '')
    category_filter = request.args.get('category', '')
    limit = min(request.args.get('limit', 8, type=int), 20)
    
    # Determine which feeds to fetch
    if requested_sources:
        source_keys = [s.strip() for s in requested_sources.split(',')]
        feeds_to_fetch = {k: v for k, v in RSS_FEEDS.items() if k in source_keys}
    else:
        feeds_to_fetch = RSS_FEEDS.copy()
    
    # Fetch all feeds in parallel (using ThreadPoolExecutor)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    all_items = []
    sources_status = {}
    
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {}
        for key, feed in feeds_to_fetch.items():
            future = executor.submit(
                _parse_rss_feed, 
                feed['url'], 
                feed['name'], 
                feed['icon']
            )
            futures[future] = key
        
        try:
            for future in as_completed(futures, timeout=15):
                key = futures[future]
                try:
                    items = future.result()
                    sources_status[key] = {
                        'name': feeds_to_fetch[key]['name'],
                        'count': len(items),
                        'status': 'ok' if items else 'empty'
                    }
                    all_items.extend(items[:limit])  # Limit per source
                except Exception as e:
                    sources_status[key] = {
                        'name': feeds_to_fetch[key]['name'],
                        'count': 0,
                        'status': 'error',
                        'error': str(e)
                    }
        except TimeoutError:
            # Some feeds timed out - mark incomplete futures as timed out
            for future, key in futures.items():
                if key not in sources_status:
                    sources_status[key] = {
                        'name': feeds_to_fetch[key]['name'],
                        'count': 0,
                        'status': 'timeout',
                        'error': 'Feed fetch timed out'
                    }
            Log.warn(f"RSS feed timeout: {len([k for k, v in sources_status.items() if v['status'] == 'timeout'])} feeds timed out")
    
    # Sort by published date (most recent first)
    # Parse dates and add timestamp to each item for reliable sorting
    def parse_rss_date(date_str):
        """Parse various RSS date formats and return Unix timestamp"""
        if not date_str:
            return 0
        
        from email.utils import parsedate_tz, mktime_tz
        import calendar
        
        # Try RFC 2822 format first (most common in RSS)
        try:
            parsed = parsedate_tz(date_str)
            if parsed:
                return mktime_tz(parsed)
        except (ValueError, TypeError, OverflowError):
            pass
        
        # Try ISO 8601 format
        try:
            # Handle various ISO formats
            cleaned = date_str.replace('Z', '+00:00')
            dt = datetime.fromisoformat(cleaned)
            return dt.timestamp()
        except (ValueError, TypeError):
            pass
        
        # Try common date formats
        formats = [
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%d %b %Y %H:%M:%S',
            '%a, %d %b %Y %H:%M:%S',
            '%Y-%m-%d',
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip()[:25], fmt)
                return dt.timestamp()
            except (ValueError, TypeError):
                continue
        
        return 0  # Unknown format - sort to end
    
    # Add parsed timestamp to each item for sorting and frontend use
    for item in all_items:
        item['timestamp'] = parse_rss_date(item.get('published', ''))
    
    # Sort by timestamp (highest/most recent first)
    all_items.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
    
    # Apply category filter if specified (filters by auto-detected category)
    if category_filter:
        all_items = [item for item in all_items if item.get('category') == category_filter]
    
    # Count items by category for stats
    category_counts = {'general': 0, 'emergency': 0, 'weather': 0}
    for item in all_items:
        cat = item.get('category', 'general')
        if cat in category_counts:
            category_counts[cat] += 1
    
    return jsonify({
        'items': all_items,
        'count': len(all_items),
        'sources': sources_status,
        'category_counts': category_counts,
        'available_sources': list(RSS_FEEDS.keys()),
        'available_categories': ['general', 'emergency', 'weather']
    })


@app.route('/api/news/sources')
def news_sources():
    """List available RSS feed sources"""
    return jsonify({
        'sources': RSS_FEEDS,
        'categories': ['general', 'emergency', 'weather']
    })


@app.route('/api/stats/history')
def stats_history():
    """Get historical stats from stats.db"""
    try:
        # Get hours parameter (default 1 hour)
        hours = request.args.get('hours', 1, type=int)
        hours = min(hours, 168)  # Max 7 days
        
        # Calculate cutoff timestamp
        cutoff = int((time.time() - (hours * 3600)) * 1000)
        
        conn = get_conn()
        try:
            c = conn.cursor()
            
            # Query archived stats
            c.execute('''
                SELECT timestamp, data FROM stats_snapshots
                WHERE timestamp >= %s
                ORDER BY timestamp ASC
            ''', (cutoff,))
            
            rows = c.fetchall()
        finally:
            conn.close()
        
        # Transform to frontend-expected format
        result = []
        for ts, data_str in rows:
            try:
                data = json.loads(data_str)
                result.append({
                    'timestamp': ts,
                    'data': data
                })
            except (json.JSONDecodeError, ValueError, TypeError):
                pass
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e), 'data': []}), 200


@app.route('/api/stats/archive/status')
def archive_status():
    """Get archival system status (stats.db)"""
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()

        # Get total records
        c.execute('SELECT COUNT(*) FROM stats_snapshots')
        total_records = c.fetchone()[0]

        # Get oldest and newest timestamps
        c.execute('SELECT MIN(timestamp), MAX(timestamp) FROM stats_snapshots')
        oldest, newest = c.fetchone()

        # Get records in last hour
        hour_ago = int((time.time() - 3600) * 1000)
        c.execute('SELECT COUNT(*) FROM stats_snapshots WHERE timestamp >= %s', (hour_ago,))
        last_hour = c.fetchone()[0]
        
        # Add collection mode info
        mode = 'active' if is_page_active() else 'idle'
        current_interval = get_current_interval()
        active_count = get_active_page_count()
        
        return jsonify({
            'status': 'running' if archive_running else 'stopped',
            'collection_mode': mode,
            'current_interval_seconds': current_interval,
            'idle_interval_seconds': IDLE_INTERVAL,
            'active_interval_seconds': ACTIVE_INTERVAL,
            'active_pages': active_count,
            'active_sessions': list(active_page_sessions.keys()),
            'total_records': total_records,
            'records_last_hour': last_hour,
            'oldest_record': datetime.fromtimestamp(oldest/1000).isoformat() if oldest else None,
            'newest_record': datetime.fromtimestamp(newest/1000).isoformat() if newest else None,
            'db_path': DB_PATH
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 200
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass


@app.route('/api/stats/archive/trigger')
def archive_trigger():
    """Manually trigger an archive snapshot"""
    success = archive_current_stats()
    return jsonify({'success': success, 'timestamp': datetime.now().isoformat()})


@app.route('/api/heartbeat', methods=['GET', 'POST'])
def page_heartbeat():
    """
    Frontend sends this to indicate a page is active.
    When DATA pages are active, data collection happens every 2 minutes.
    When no data pages are active, collection happens every 10 minutes.
    
    Query params:
    - action: 'open', 'close', or 'ping' (default)
    - page_id: unique identifier for the browser tab/page (required for accurate tracking)
    - page_type: name of the page (e.g., 'live', 'map', 'index')
    - data_page: 'true' if page fetches live data (triggers active mode), 'false' for info pages
    """
    global last_heartbeat, active_page_sessions
    
    action = request.args.get('action', 'ping')
    page_id = request.args.get('page_id', '')
    page_type = request.args.get('page_type', 'unknown')
    is_data_page = request.args.get('data_page', 'false').lower() in ('true', '1', 'yes')
    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    user_agent = request.headers.get('User-Agent', 'unknown')[:100]
    
    # Only update main heartbeat timestamp for data pages
    if is_data_page:
        last_heartbeat = time.time()
    
    # Clean up stale sessions first
    stale_count = cleanup_stale_sessions()
    
    # Get short ID for logging (last 6 chars are more unique)
    short_id = page_id[-6:] if len(page_id) > 6 else page_id
    
    if action == 'open':
        if page_id:
            with _page_sessions_lock:
                existing = active_page_sessions.get(page_id, {})
                is_new = not existing
                old_page_type = existing.get('page_type')
                active_page_sessions[page_id] = {
                    'last_seen': time.time(),
                    'user_agent': user_agent,
                    'ip': client_ip,
                    'page_type': page_type,
                    'is_data_page': is_data_page,
                    'opened_at': existing.get('opened_at', time.time())
                }
                total_count = len(active_page_sessions)
            data_count = get_data_page_count()

            if is_new:
                Log.viewer(f"👤 Joined: ...{short_id} on {page_type} (viewers: {total_count}, data: {data_count})")
            elif old_page_type != page_type:
                Log.viewer(f"📄 ...{short_id}: {old_page_type} → {page_type} (viewers: {total_count}, data: {data_count})")

            _check_mode_change()
        else:
            Log.api(f"Page opened (no page_id) from {client_ip}")

    elif action == 'close':
        if page_id:
            with _page_sessions_lock:
                session_entry = active_page_sessions.get(page_id)
                if session_entry is None:
                    session_current_page = None
                    removed = False
                else:
                    session_current_page = session_entry.get('page_type', 'unknown')
                    if session_current_page == page_type:
                        del active_page_sessions[page_id]
                        removed = True
                        total_count = len(active_page_sessions)
                    else:
                        removed = False
            if removed:
                data_count = get_data_page_count()
                Log.viewer(f"👋 Left: ...{short_id} (viewers: {total_count}, data: {data_count})")
                _check_mode_change()
            elif session_current_page is not None:
                Log.api(f"Ignoring stale close from {page_type} (session on {session_current_page})")
        else:
            Log.api(f"Page close signal (no page_id) from {client_ip}")

    else:
        # Regular heartbeat ping - update session timestamp
        if page_id:
            with _page_sessions_lock:
                existing = active_page_sessions.get(page_id)
                if existing is not None:
                    existing['last_seen'] = time.time()
                else:
                    active_page_sessions[page_id] = {
                        'last_seen': time.time(),
                        'user_agent': user_agent,
                        'ip': client_ip,
                        'page_type': page_type,
                        'is_data_page': is_data_page,
                        'opened_at': time.time()
                    }

    with _page_sessions_lock:
        total_count = len(active_page_sessions)
    data_count = get_data_page_count()
    current_interval = get_current_interval()
    mode = "active" if is_page_active() else "idle"
    
    return jsonify({
        'status': 'ok',
        'mode': mode,
        'interval': current_interval,
        'total_viewers': total_count,
        'data_viewers': data_count,
        'page_id': page_id,
        'page_type': page_type,
        'is_data_page': is_data_page,
        'next_collection_seconds': current_interval,
        'data_retention_days': DATA_RETENTION_DAYS
    })


@app.route('/api/collection/status')
def collection_status():
    """Get current data collection status and mode"""
    total_count = get_active_page_count()
    data_count = get_data_page_count()
    with _page_sessions_lock:
        sessions_snap = list(active_page_sessions.items())
    return jsonify({
        'mode': 'active' if is_page_active() else 'idle',
        'interval_seconds': get_current_interval(),
        'total_viewers': total_count,
        'data_viewers': data_count,
        'sessions': [
            {
                'id': pid[:8] + '...',
                'page_type': session.get('page_type', 'unknown'),
                'is_data_page': session.get('is_data_page', False),
                'ip': session['ip'],
                'age_seconds': int(time.time() - session.get('opened_at', session['last_seen']))
            }
            for pid, session in sessions_snap
        ],
        'last_heartbeat': datetime.fromtimestamp(last_heartbeat).isoformat() if last_heartbeat > 0 else None,
        'idle_interval': IDLE_INTERVAL,
        'active_interval': ACTIVE_INTERVAL,
        'session_timeout': PAGE_SESSION_TIMEOUT,
        'data_retention_days': DATA_RETENTION_DAYS
    })


@app.route('/api/stats/summary')
@cached(ttl=CACHE_TTL_STATS)
def stats_summary():
    """Aggregated stats summary from all sources - comprehensive overview"""
    stats = {
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'power': {
            'ausgrid': {
                'unplanned': 0, 
                'planned': 0, 
                'customers_affected': 0,
                'total': 0
            },
            'endeavour': {
                'current': 0, 
                'current_active': 0,  # Excluding completed
                'future': 0,
                'customers_affected': 0
            },
            'essential': {
                'unplanned': 0,
                'planned': 0,
                'future': 0,
                'total': 0,
                'customers_affected': 0
            },
            'total_outages': 0,
            'total_customers': 0
        },
        'traffic': {
            'incidents': 0,
            'crashes': 0,
            'hazards': 0,
            'breakdowns': 0,
            'changed_conditions': 0,
            'roadwork': 0,
            'fires': 0,
            'floods': 0,
            'major_events': 0,
            'lga_incidents': 0,
            'cameras': 0,
            'total': 0
        },
        'emergency': {
            'rfs_incidents': 0,
            'rfs_by_level': {
                'emergency_warning': 0,
                'watch_and_act': 0,
                'advice': 0
            },
            'bom_warnings': {
                'land': 0, 
                'marine': 0,
                'total': 0
            }
        },
        'environment': {
            'beaches_monitored': 0,
            'beaches_good': 0,
            'beaches_poor': 0,
            'beachsafe_patrolled': 0
        }
    }
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # Fetch all data
    try:
        # Ausgrid stats
        ausgrid = requests.get('https://www.ausgrid.com.au/webapi/outagemapdata/GetCurrentOutageStats', timeout=10, headers=headers)
        if ausgrid.status_code == 200:
            ad = ausgrid.json()
            unplanned = (ad.get('OutageUnplannedPdoCount', 0) or 0) + (ad.get('OutageUnplannedRdoCount', 0) or 0)
            planned = ad.get('OutagePlannedTodayCount', 0) or 0
            customers = (ad.get('OutageUnplannedPdoCustomers', 0) or 0) + (ad.get('OutageUnplannedRdoCustomers', 0) or 0)
            stats['power']['ausgrid']['unplanned'] = unplanned
            stats['power']['ausgrid']['planned'] = planned
            stats['power']['ausgrid']['customers_affected'] = customers
            stats['power']['ausgrid']['total'] = unplanned + planned
            stats['power']['total_outages'] += unplanned + planned
            stats['power']['total_customers'] += customers
    except Exception as e:
        Log.error(f"Stats - Ausgrid error: {e}")
    
    try:
        # Endeavour - use Supabase statistics API
        endeavour_stats = _fetch_endeavour_supabase('/rpc/get_outage_statistics', method='POST', body={})
        if endeavour_stats:
            active = endeavour_stats.get('active_outages', 0)
            customers = endeavour_stats.get('total_affected_customers', 0)
            stats['power']['endeavour']['current_active'] = active
            stats['power']['endeavour']['customers_affected'] = customers
            stats['power']['total_outages'] += active
            stats['power']['total_customers'] += customers
        
        # Get detailed counts from cache
        cached_current, _, _ = cache_get('endeavour_current')
        cached_maintenance, _, _ = cache_get('endeavour_maintenance')
        cached_future, _, _ = cache_get('endeavour_future')
        if cached_current and isinstance(cached_current, list):
            stats['power']['endeavour']['current'] = len(cached_current)
        if cached_maintenance and isinstance(cached_maintenance, list):
            stats['power']['endeavour']['current_maintenance'] = len(cached_maintenance)
        if cached_future and isinstance(cached_future, list):
            stats['power']['endeavour']['future'] = len(cached_future)
    except Exception as e:
        Log.error(f"Stats - Endeavour error: {e}")
    
    # Essential Energy - use cached KML data (all 3 feeds)
    try:
        total_essential = 0
        total_essential_customers = 0
        
        cached_essential, _, _ = cache_get('essential_energy')
        if cached_essential and isinstance(cached_essential, list):
            unplanned = sum(1 for o in cached_essential if o.get('outageType') == 'unplanned')
            planned = sum(1 for o in cached_essential if o.get('outageType') == 'planned')
            customers = sum(o.get('customersAffected', 0) for o in cached_essential)
            stats['power']['essential']['unplanned'] = unplanned
            stats['power']['essential']['planned'] = planned
            total_essential += len(cached_essential)
            total_essential_customers += customers
        
        cached_future, _, _ = cache_get('essential_energy_future')
        if cached_future and isinstance(cached_future, list):
            stats['power']['essential']['future'] = len(cached_future)
            total_essential += len(cached_future)
            total_essential_customers += sum(o.get('customersAffected', 0) for o in cached_future)
        
        stats['power']['essential']['total'] = total_essential
        stats['power']['essential']['customers_affected'] = total_essential_customers
        stats['power']['total_outages'] += total_essential
        stats['power']['total_customers'] += total_essential_customers
    except Exception as e:
        Log.error(f"Stats - Essential Energy error: {e}")
    
    # Traffic incidents with category breakdown
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/incident.json', timeout=10, headers=headers)
        if r.status_code == 200:
            data = r.json()
            features = data.get('features', []) if isinstance(data, dict) else data
            for f in features:
                props = f.get('properties', f)
                if props.get('ended', False):
                    continue
                stats['traffic']['incidents'] += 1
                
                # Check multiple fields for category info
                main_cat = (props.get('mainCategory', '') or f.get('mainCategory', '') or '').upper()
                sub_cat = (props.get('subCategory', '') or props.get('subCategoryA', '') or '').upper()
                headline = (props.get('headline', '') or '').upper()
                display_name = (props.get('displayName', '') or '').upper()
                cat_icon = (props.get('CategoryIcon', '') or '').upper()
                
                all_text = f"{main_cat} {sub_cat} {headline} {display_name} {cat_icon}"
                
                if 'CRASH' in all_text or 'COLLISION' in all_text or 'ROLLOVER' in all_text:
                    stats['traffic']['crashes'] += 1
                elif 'BREAKDOWN' in all_text or 'BROKEN DOWN' in all_text or 'DISABLED' in all_text or 'STALLED' in all_text:
                    stats['traffic']['breakdowns'] += 1
                elif 'HAZARD' in all_text or 'DEBRIS' in all_text or 'OBSTRUCTION' in all_text or 'ANIMAL' in all_text or 'OBJECT' in all_text:
                    stats['traffic']['hazards'] += 1
                elif 'CHANGED TRAFFIC CONDITIONS' in all_text:
                    stats['traffic']['changed_conditions'] += 1
    except Exception as e:
        Log.error(f"Stats - Traffic incidents error: {e}")
    
    # Other traffic endpoints
    traffic_other = [
        ('roadwork', 'https://www.livetraffic.com/traffic/hazards/roadwork.json'),
        ('fires', 'https://www.livetraffic.com/traffic/hazards/fire.json'),
        ('floods', 'https://www.livetraffic.com/traffic/hazards/flood.json'),
        ('major_events', 'https://www.livetraffic.com/traffic/hazards/majorevent.json'),
    ]
    
    for key, url in traffic_other:
        try:
            r = requests.get(url, timeout=10, headers=headers)
            if r.status_code == 200:
                data = r.json()
                features = data.get('features', []) if isinstance(data, dict) else data
                count = sum(1 for f in features if not f.get('properties', f).get('ended', False))
                stats['traffic'][key] = count
        except Exception as e:
            Log.error(f"Stats - Traffic {key} error: {e}")
    
    # Calculate traffic total
    stats['traffic']['total'] = (
        stats['traffic']['incidents'] + 
        stats['traffic']['roadwork'] + 
        stats['traffic']['fires'] + 
        stats['traffic']['floods'] + 
        stats['traffic']['major_events']
    )
    
    # Traffic cameras count
    try:
        r = requests.get('https://www.livetraffic.com/datajson/all-feeds-web.json', timeout=10, headers=headers)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                camera_count = sum(1 for item in data if 'livecam' in (item.get('eventType', '') or '').lower())
                stats['traffic']['cameras'] = camera_count
    except Exception as e:
        Log.error(f"Stats - Cameras error: {e}")
    
    try:
        # RFS incidents with alert level breakdown
        r = requests.get('https://www.rfs.nsw.gov.au/feeds/majorIncidents.xml', timeout=10, headers=headers)
        if r.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.content)
            items = root.findall('.//item')
            stats['emergency']['rfs_incidents'] = len(items)
            
            for item in items:
                category = (item.findtext('category', '') or '').lower()
                if 'emergency' in category:
                    stats['emergency']['rfs_by_level']['emergency_warning'] += 1
                elif 'watch' in category:
                    stats['emergency']['rfs_by_level']['watch_and_act'] += 1
                else:
                    stats['emergency']['rfs_by_level']['advice'] += 1
    except Exception as e:
        Log.error(f"Stats - RFS error: {e}")
    
    # BOM Warnings
    bom_urls = {
        'land': 'https://www.bom.gov.au/fwo/IDZ00061.warnings_land_nsw.xml',
        'marine': 'https://www.bom.gov.au/fwo/IDZ00068.warnings_marine_nsw.xml'
    }
    
    for bom_type, url in bom_urls.items():
        try:
            r = requests.get(url, timeout=10, headers=headers)
            if r.status_code == 200:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(r.content)
                count = len(root.findall('.//item'))
                stats['emergency']['bom_warnings'][bom_type] = count
                stats['emergency']['bom_warnings']['total'] += count
        except Exception as e:
            Log.error(f"Stats - BOM {bom_type} error: {e}")
    
    try:
        # Beachwatch with quality breakdown
        r = requests.get('https://api.beachwatch.nsw.gov.au/public/sites/geojson', timeout=10, headers=headers)
        if r.status_code == 200:
            data = r.json()
            features = data.get('features', [])
            stats['environment']['beaches_monitored'] = len(features)
            for f in features:
                props = f.get('properties', {})
                result = (props.get('latestResult', '') or '').lower()
                if result in ['good', 'excellent']:
                    stats['environment']['beaches_good'] += 1
                elif result in ['bad', 'poor']:
                    stats['environment']['beaches_poor'] += 1
    except Exception as e:
        Log.error(f"Stats - Beachwatch error: {e}")
    
    try:
        # BeachSafe patrolled count
        ne_lat, ne_lon = -28.0, 154.0
        sw_lat, sw_lon = -37.5, 149.0
        url = f'https://beachsafe.org.au/api/v4/map/beaches?neCoords[]={ne_lat}&neCoords[]={ne_lon}&swCoords[]={sw_lat}&swCoords[]={sw_lon}'
        r = requests.get(url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json',
            'Referer': 'https://beachsafe.org.au/'
        })
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and 'beaches' in data:
                beaches = data.get('beaches', [])
                patrolled = sum(1 for b in beaches if b.get('is_patrolled_today', {}).get('flag', False))
                stats['environment']['beachsafe_patrolled'] = patrolled
    except Exception as e:
        Log.error(f"Stats - BeachSafe error: {e}")
    
    return jsonify(stats)


def is_valid_datetime(dt_str):
    """Check if a datetime string is valid and not a placeholder date like 3408-09-13"""
    if not dt_str:
        return False
    try:
        # Check for obviously invalid years (placeholder dates like 3408)
        if isinstance(dt_str, str):
            year_match = re.match(r'^(\d{4})', dt_str)
            if year_match:
                year = int(year_match.group(1))
                # Valid years should be between 2000 and 2100
                if year < 2000 or year > 2100:
                    return False
        return True
    except (ValueError, TypeError, AttributeError):
        return False


@app.route('/api/endeavour/current')
@require_api_key
def endeavour_current():
    """Endeavour Energy current (unplanned) outages"""
    # Check persistent cache first (populated by prewarm)
    cached_data, age, expired = cache_get('endeavour_current')
    if cached_data is not None:
        return jsonify(cached_data)
    
    # Fallback to live fetch from Supabase
    try:
        all_data = _fetch_endeavour_all_outages()
        result = all_data.get('current', [])
        cache_set('endeavour_current', result, CACHE_TTL_ENDEAVOUR_CURRENT)
        return jsonify(result)
    except Exception as e:
        Log.error(f"Endeavour current error: {e}")
        return jsonify([]), 200


@app.route('/api/endeavour/current/raw')
@cached(ttl=CACHE_TTL_ENDEAVOUR_CURRENT)
def endeavour_current_raw():
    """Endeavour Energy current outages - raw Supabase data"""
    try:
        areas = _fetch_endeavour_supabase('/rpc/get_outage_areas_fast', method='POST', body={})
        if areas:
            # Filter to non-planned (current/unplanned) outages
            current = [a for a in areas if (a.get('outage_type') or '').upper() != 'PLANNED']
            return jsonify(current)
        return jsonify([])
    except Exception as e:
        Log.error(f"Endeavour current raw error: {e}")
        return jsonify([]), 200


@app.route('/api/endeavour/current/all')
@cached(ttl=CACHE_TTL_ENDEAVOUR_CURRENT)
def endeavour_current_all():
    """Endeavour Energy current outages - all including completed"""
    try:
        all_data = _fetch_endeavour_all_outages()
        return jsonify(all_data.get('current', []))
    except Exception as e:
        Log.error(f"Endeavour current all error: {e}")
        return jsonify([]), 200


@app.route('/api/weather/radar')
@cached(ttl=CACHE_TTL_WEATHER)
def weather_radar():
    """Get latest radar timestamps from RainViewer API"""
    try:
        r = requests.get('https://api.rainviewer.com/public/weather-maps.json', timeout=10)
        if r.status_code == 200:
            data = r.json()
            return jsonify(data)
    except Exception as e:
        Log.error(f"RainViewer error: {e}")
    return jsonify({'radar': {'past': [], 'nowcast': []}})


# NSW locations for weather monitoring
NSW_WEATHER_LOCATIONS = [
    # Sydney Metro
    {"name": "Sydney CBD", "lat": -33.8688, "lon": 151.2093},
    {"name": "Parramatta", "lat": -33.8151, "lon": 151.0011},
    {"name": "Penrith", "lat": -33.7506, "lon": 150.6944},
    {"name": "Campbelltown", "lat": -34.0650, "lon": 150.8142},
    {"name": "Liverpool", "lat": -33.9200, "lon": 150.9256},
    {"name": "Blacktown", "lat": -33.7668, "lon": 150.9054},
    {"name": "Hornsby", "lat": -33.7025, "lon": 151.0990},
    {"name": "Manly", "lat": -33.7969, "lon": 151.2878},
    {"name": "Cronulla", "lat": -34.0587, "lon": 151.1520},
    {"name": "Bankstown", "lat": -33.9175, "lon": 151.0355},
    {"name": "Chatswood", "lat": -33.7969, "lon": 151.1803},
    {"name": "Bondi", "lat": -33.8915, "lon": 151.2767},
    {"name": "Richmond", "lat": -33.5997, "lon": 150.7517},
    
    # Greater Sydney / Illawarra
    {"name": "Wollongong", "lat": -34.4278, "lon": 150.8931},
    {"name": "Shellharbour", "lat": -34.5809, "lon": 150.8700},
    {"name": "Kiama", "lat": -34.6710, "lon": 150.8544},
    {"name": "Nowra", "lat": -34.8808, "lon": 150.6000},
    {"name": "Ulladulla", "lat": -35.3583, "lon": 150.4706},
    {"name": "Batemans Bay", "lat": -35.7082, "lon": 150.1744},
    
    # Central Coast / Hunter
    {"name": "Central Coast", "lat": -33.4245, "lon": 151.3419},
    {"name": "Newcastle", "lat": -32.9283, "lon": 151.7817},
    {"name": "Maitland", "lat": -32.7330, "lon": 151.5590},
    {"name": "Cessnock", "lat": -32.8340, "lon": 151.3560},
    {"name": "Lake Macquarie", "lat": -33.0333, "lon": 151.6333},
    {"name": "Port Stephens", "lat": -32.7178, "lon": 152.1122},
    {"name": "Singleton", "lat": -32.5697, "lon": 151.1694},
    {"name": "Muswellbrook", "lat": -32.2654, "lon": 150.8885},
    
    # Blue Mountains / Central Tablelands
    {"name": "Katoomba", "lat": -33.7139, "lon": 150.3113},
    {"name": "Springwood", "lat": -33.6994, "lon": 150.5647},
    {"name": "Lithgow", "lat": -33.4833, "lon": 150.1500},
    {"name": "Bathurst", "lat": -33.4193, "lon": 149.5775},
    {"name": "Orange", "lat": -33.2840, "lon": 149.1004},
    {"name": "Mudgee", "lat": -32.5942, "lon": 149.5878},
    {"name": "Cowra", "lat": -33.8283, "lon": 148.6919},
    {"name": "Young", "lat": -34.3111, "lon": 148.3011},
    {"name": "Parkes", "lat": -33.1306, "lon": 148.1764},
    {"name": "Forbes", "lat": -33.3847, "lon": 148.0106},
    
    # Central West / Orana
    {"name": "Dubbo", "lat": -32.2569, "lon": 148.6011},
    {"name": "Wellington", "lat": -32.5558, "lon": 148.9439},
    {"name": "Narromine", "lat": -32.2333, "lon": 148.2333},
    {"name": "Gilgandra", "lat": -31.7097, "lon": 148.6622},
    {"name": "Coonamble", "lat": -30.9544, "lon": 148.3878},
    {"name": "Nyngan", "lat": -31.5611, "lon": 147.1936},
    {"name": "Cobar", "lat": -31.4958, "lon": 145.8389},
    
    # New England / North West
    {"name": "Tamworth", "lat": -31.0927, "lon": 150.9320},
    {"name": "Armidale", "lat": -30.5130, "lon": 151.6690},
    {"name": "Glen Innes", "lat": -29.7333, "lon": 151.7333},
    {"name": "Tenterfield", "lat": -29.0492, "lon": 152.0200},
    {"name": "Inverell", "lat": -29.7756, "lon": 151.1122},
    {"name": "Moree", "lat": -29.4658, "lon": 149.8456},
    {"name": "Narrabri", "lat": -30.3228, "lon": 149.7836},
    {"name": "Gunnedah", "lat": -30.9833, "lon": 150.2500},
    {"name": "Quirindi", "lat": -31.5000, "lon": 150.6833},
    {"name": "Walcha", "lat": -31.0000, "lon": 151.6000},
    
    # North Coast
    {"name": "Port Macquarie", "lat": -31.4333, "lon": 152.9000},
    {"name": "Kempsey", "lat": -31.0833, "lon": 152.8333},
    {"name": "Coffs Harbour", "lat": -30.2963, "lon": 153.1157},
    {"name": "Grafton", "lat": -29.6908, "lon": 152.9331},
    {"name": "Ballina", "lat": -28.8667, "lon": 153.5667},
    {"name": "Lismore", "lat": -28.8133, "lon": 153.2750},
    {"name": "Byron Bay", "lat": -28.6433, "lon": 153.6150},
    {"name": "Tweed Heads", "lat": -28.1761, "lon": 153.5414},
    {"name": "Casino", "lat": -28.8667, "lon": 153.0500},
    {"name": "Maclean", "lat": -29.4500, "lon": 153.2000},
    {"name": "Yamba", "lat": -29.4333, "lon": 153.3500},
    {"name": "Forster", "lat": -32.1808, "lon": 152.5172},
    {"name": "Taree", "lat": -31.9000, "lon": 152.4500},
    
    # Riverina / Murray
    {"name": "Wagga Wagga", "lat": -35.1082, "lon": 147.3598},
    {"name": "Albury", "lat": -36.0737, "lon": 146.9135},
    {"name": "Griffith", "lat": -34.2833, "lon": 146.0333},
    {"name": "Leeton", "lat": -34.5500, "lon": 146.4000},
    {"name": "Narrandera", "lat": -34.7500, "lon": 146.5500},
    {"name": "Temora", "lat": -34.4500, "lon": 147.5333},
    {"name": "Cootamundra", "lat": -34.6500, "lon": 148.0333},
    {"name": "Junee", "lat": -34.8667, "lon": 147.5833},
    {"name": "Tumut", "lat": -35.3000, "lon": 148.2167},
    {"name": "Deniliquin", "lat": -35.5333, "lon": 144.9500},
    {"name": "Hay", "lat": -34.5167, "lon": 144.8500},
    {"name": "Finley", "lat": -35.6500, "lon": 145.5667},
    {"name": "Corowa", "lat": -35.9833, "lon": 146.3833},
    
    # Snowy / Southern Tablelands
    {"name": "Cooma", "lat": -36.2356, "lon": 149.1245},
    {"name": "Jindabyne", "lat": -36.4167, "lon": 148.6167},
    {"name": "Thredbo", "lat": -36.5050, "lon": 148.3069},
    {"name": "Perisher", "lat": -36.4000, "lon": 148.4167},
    {"name": "Goulburn", "lat": -34.7547, "lon": 149.7186},
    {"name": "Queanbeyan", "lat": -35.3547, "lon": 149.2311},
    {"name": "Yass", "lat": -34.8333, "lon": 148.9167},
    {"name": "Bega", "lat": -36.6736, "lon": 149.8428},
    {"name": "Merimbula", "lat": -36.8917, "lon": 149.9083},
    {"name": "Eden", "lat": -37.0667, "lon": 149.9000},
    {"name": "Bombala", "lat": -36.9000, "lon": 149.2333},
    
    # Far West
    {"name": "Broken Hill", "lat": -31.9505, "lon": 141.4533},
    {"name": "Wilcannia", "lat": -31.5558, "lon": 143.3778},
    {"name": "Bourke", "lat": -30.0903, "lon": 145.9378},
    {"name": "Brewarrina", "lat": -29.9667, "lon": 146.8500},
    {"name": "Lightning Ridge", "lat": -29.4333, "lon": 147.9667},
    {"name": "Walgett", "lat": -30.0167, "lon": 148.1167},
    {"name": "Menindee", "lat": -32.3939, "lon": 142.4178},
    {"name": "Ivanhoe", "lat": -32.9000, "lon": 144.3000},
    {"name": "White Cliffs", "lat": -30.8500, "lon": 143.0833},
    {"name": "Tibooburra", "lat": -29.4333, "lon": 142.0167},
    
    # ACT (nearby)
    {"name": "Canberra", "lat": -35.2809, "lon": 149.1300},
]

# WMO weather code descriptions
WEATHER_CODES = {
    0: ("Clear", "☀️"), 1: ("Mostly Clear", "🌤️"), 2: ("Partly Cloudy", "⛅"), 3: ("Overcast", "☁️"),
    45: ("Fog", "🌫️"), 48: ("Rime Fog", "🌫️"),
    51: ("Light Drizzle", "🌧️"), 53: ("Drizzle", "🌧️"), 55: ("Heavy Drizzle", "🌧️"),
    61: ("Light Rain", "🌧️"), 63: ("Rain", "🌧️"), 65: ("Heavy Rain", "🌧️"),
    71: ("Light Snow", "❄️"), 73: ("Snow", "❄️"), 75: ("Heavy Snow", "❄️"),
    80: ("Light Showers", "🌦️"), 81: ("Showers", "🌦️"), 82: ("Heavy Showers", "⛈️"),
    95: ("Thunderstorm", "⛈️"), 96: ("Thunderstorm + Hail", "⛈️"), 99: ("Severe Storm", "⛈️"),
}

@app.route('/api/weather/current')
@require_api_key
def weather_current():
    """Fetch current weather for major NSW locations from Open-Meteo"""
    # Check persistent cache first (populated by prewarm)
    cached_data, age, expired = cache_get('weather_current')
    if cached_data is not None:
        return jsonify(cached_data)
    
    # Fallback to live fetch if cache empty
    features = []
    
    # Build latitude and longitude strings for batch request
    lats = ",".join([str(loc["lat"]) for loc in NSW_WEATHER_LOCATIONS])
    lons = ",".join([str(loc["lon"]) for loc in NSW_WEATHER_LOCATIONS])
    
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lats}&longitude={lons}&current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m&timezone=Australia%2FSydney"
        
        r = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            
            # Handle both single and multiple location responses
            weather_list = data if isinstance(data, list) else [data]
            
            for i, loc in enumerate(NSW_WEATHER_LOCATIONS):
                if i >= len(weather_list):
                    break
                    
                current = weather_list[i].get('current', {})
                if not current:
                    continue
                
                weather_code = current.get('weather_code', 0)
                weather_desc, weather_icon = WEATHER_CODES.get(weather_code, ("Unknown", "❓"))
                
                temp = current.get('temperature_2m')
                feels_like = current.get('apparent_temperature')
                humidity = current.get('relative_humidity_2m')
                wind_speed = current.get('wind_speed_10m')
                wind_gusts = current.get('wind_gusts_10m')
                wind_dir = current.get('wind_direction_10m')
                precip = current.get('precipitation', 0)
                
                features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [loc['lon'], loc['lat']]
                    },
                    'properties': {
                        'name': loc['name'],
                        'temperature': temp,
                        'feelsLike': feels_like,
                        'humidity': humidity,
                        'windSpeed': wind_speed,
                        'windGusts': wind_gusts,
                        'windDirection': wind_dir,
                        'precipitation': precip,
                        'weatherCode': weather_code,
                        'weatherDescription': weather_desc,
                        'weatherIcon': weather_icon
                    }
                })
    except Exception as e:
        Log.error(f"Weather error: {e}")
    
    result = {
        'type': 'FeatureCollection',
        'features': features
    }
    cache_set('weather_current', result, CACHE_TTL_WEATHER)
    return jsonify(result)


def extract_incident_type_from_title(title):
    """Extract the incident type prefix from a traffic incident title.
    
    Examples:
        "HAZARD Road damage" -> "HAZARD"
        "CHANGED TRAFFIC CONDITIONS Warringah Freeway" -> "CHANGED TRAFFIC CONDITIONS"
        "CRASH Multi-vehicle" -> "CRASH"
        "TRAFFIC LIGHTS BLACKED OUT Pacific Hwy" -> "TRAFFIC LIGHTS BLACKED OUT"
    
    Returns tuple: (incident_type, remaining_title)
    """
    if not title:
        return ('', title or '')
    
    title = title.strip()
    
    # Known incident type prefixes in order of specificity (longer first)
    # These are extracted from the title as type badges
    # Order matters - compound/longer prefixes must come before shorter ones!
    INCIDENT_TYPE_PREFIXES = [
        'SPECIAL EVENT CLEARWAYS',  # Compound type - must be before SPECIAL EVENT
        'MAJOR EVENT CLEARWAYS',    # Compound type
        'TRAFFIC LIGHTS BLACKED OUT',
        'CHANGED TRAFFIC CONDITIONS',
        'HOLIDAY TRAFFIC EXPECTED',
        'ADVERSE WEATHER',
        'BUILDING FIRE',
        'EARLIER FIRE',
        'GRASS FIRE',
        'BUSH FIRE',
        'CLEARWAYS',
        'BREAKDOWN',
        'FLOODING',  # Must be before FLOOD (longer match first)
        'CRASH',
        'HAZARD',
        'LANDSLIDE',
        'SMOKE',
        'FIRE',
        'FLOOD',
        'ROADWORK',
        'ROAD CLOSURE',
        'SPECIAL EVENT',
        'MAJOR EVENT',
    ]
    
    title_upper = title.upper()
    
    for prefix in INCIDENT_TYPE_PREFIXES:
        if title_upper.startswith(prefix):
            # Word-boundary check: prefix must be followed by non-alpha or end of string
            # This prevents "FLOOD" matching "FLOODING", "FIRE" matching "FIREWORKS", etc.
            if len(title_upper) > len(prefix) and title_upper[len(prefix)].isalpha():
                continue
            # Extract the type and the remaining title
            remaining = title[len(prefix):].strip()
            # Clean up any leading punctuation or whitespace
            remaining = re.sub(r'^[\s\-:,]+', '', remaining).strip()
            # Drop short/code-like remnants (e.g. "ING" from "FLOOD ING")
            if remaining and (len(remaining) < 5 or re.match(r'^[A-Z]{2,4}$', remaining, re.IGNORECASE)):
                remaining = ''
            return (prefix, remaining if remaining else title)
    
    # No known prefix found - return empty type and full title
    return ('', title)


def parse_traffic_item(item, hazard_type):
    """Parse a traffic hazard item into GeoJSON feature with full details"""
    lat = None
    lon = None
    
    # Try different coordinate formats
    if 'geometry' in item and item['geometry']:
        coords = item['geometry'].get('coordinates', [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    elif 'latitude' in item and 'longitude' in item:
        lat = item['latitude']
        lon = item['longitude']
    elif 'lat' in item and 'lng' in item:
        lat = item['lat']
        lon = item['lng']
    
    if lat is None or lon is None:
        return None
    
    try:
        lat = float(lat)
        lon = float(lon)
    except (ValueError, TypeError):
        return None
    
    # Extract all available properties for detailed tooltips
    # Check both item level and properties level (Live Traffic format varies)
    raw_props = item.get('properties', {})
    # Merge top-level item fields with properties (top-level fields override for category info)
    props = {**raw_props}
    for k, v in item.items():
        if k not in ('geometry', 'properties', 'type') and v:
            props[k] = v
    
    # Roads info - can be array or string
    roads_info = props.get('roads', [])
    if isinstance(roads_info, list) and len(roads_info) > 0:
        road = roads_info[0] if isinstance(roads_info[0], dict) else {}
        roads_str = f"{road.get('mainStreet', '')} {road.get('suburb', '')}".strip()
        affected_direction = road.get('affectedDirection', '')
    else:
        roads_str = str(roads_info) if roads_info else ''
        affected_direction = ''
    
    # Extract incident type from title/headline
    raw_title = props.get('headline', props.get('title', props.get('displayName', '')))
    incident_type, clean_title = extract_incident_type_from_title(raw_title)
    
    return {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [lon, lat]
        },
        'properties': {
            'id': item.get('id', ''),
            'type': hazard_type,
            # Extracted incident type from title (e.g. HAZARD, CRASH, CHANGED TRAFFIC CONDITIONS)
            'incidentType': incident_type,
            # Preserve original category from Live Traffic for filtering
            'mainCategory': props.get('mainCategory', ''),
            'subCategory': props.get('subCategory', ''),
            'incidentKind': props.get('incidentKind', ''),
            # title contains the description after removing the type prefix
            'title': clean_title if clean_title else raw_title,
            'headline': props.get('headline', ''),
            'displayName': props.get('displayName', ''),
            'subtitle': props.get('subtitle', ''),
            'otherAdvice': props.get('otherAdvice', props.get('adviceA', '')),
            'adviceB': props.get('adviceB', ''),
            'roads': roads_str,
            'affectedDirection': affected_direction,
            'impactedLanes': props.get('impactedLanes', []),
            # Roadwork specific
            'speedLimit': props.get('speedLimit', ''),
            'expectedDelay': props.get('expectedDelay', props.get('delay', '')),
            'diversions': props.get('diversions', props.get('diversion', '')),
            'encodedPolyline': props.get('encodedPolyline', props.get('encodedPolylines', '')),
            # Timing
            'created': props.get('created', props.get('start', '')),
            'lastUpdated': props.get('lastUpdated', props.get('end', '')),
            'start': props.get('start', ''),
            'end': props.get('end', ''),
            # Status
            'isEnded': props.get('ended', props.get('isEnded', False)),
            'isMajor': props.get('isMajor', False),
            'arrangement': props.get('arrangement', ''),
            'periods': props.get('periods', []),
            'source': 'livetraffic'
        }
    }

def filter_active_traffic(features):
    """Filter out ended traffic incidents"""
    return [f for f in features if not f.get('properties', f).get('isEnded', False)]


@app.route('/api/traffic/incidents')
def traffic_incidents():
    """Live Traffic NSW - general incidents only (uses persistent cache)"""
    # Check persistent cache first
    cached_data, age, expired = cache_get('traffic_incidents')
    if cached_data and not expired:
        return jsonify(cached_data)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live
    features = []
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/incident.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get('features', [])
            for item in items:
                props = item.get('properties', item)
                if props.get('ended', False):
                    continue
                feature = parse_traffic_item(item, 'Incident')
                if feature:
                    features.append(feature)
    except Exception as e:
        Log.error(f"Traffic incidents error: {e}")
    
    result = {'type': 'FeatureCollection', 'features': features, 'count': len(features)}
    cache_set('traffic_incidents', result, CACHE_TTL_TRAFFIC)
    return jsonify(result)


@app.route('/api/traffic/incidents/raw')
@cached(ttl=CACHE_TTL_TRAFFIC)
def traffic_incidents_raw():
    """Live Traffic NSW - raw incident data"""
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/incident.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            return jsonify(r.json())
    except Exception as e:
        Log.error(f"Traffic incidents raw error: {e}")
    return jsonify({'type': 'FeatureCollection', 'features': []})

@app.route('/api/traffic/roadwork')
def traffic_roadwork():
    """Live Traffic NSW - roadwork only (uses persistent cache)"""
    cached_data, age, expired = cache_get('traffic_roadwork')
    if cached_data and not expired:
        return jsonify(cached_data)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live
    features = []
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/roadwork.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get('features', [])
            for item in items:
                props = item.get('properties', item)
                if props.get('ended', False):
                    continue
                feature = parse_traffic_item(item, 'Roadwork')
                if feature:
                    features.append(feature)
    except Exception as e:
        Log.error(f"Traffic roadwork error: {e}")
    
    result = {'type': 'FeatureCollection', 'features': features, 'count': len(features)}
    cache_set('traffic_roadwork', result, CACHE_TTL_TRAFFIC_ROADWORK)
    return jsonify(result)


@app.route('/api/traffic/roadwork/raw')
@cached(ttl=CACHE_TTL_TRAFFIC_ROADWORK)
def traffic_roadwork_raw():
    """Live Traffic NSW - raw roadwork data"""
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/roadwork.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            return jsonify(r.json())
    except Exception as e:
        Log.error(f"Traffic roadwork raw error: {e}")
    return jsonify({'type': 'FeatureCollection', 'features': []})

@app.route('/api/traffic/flood')
def traffic_flood():
    """Live Traffic NSW - flood hazards only (uses persistent cache)"""
    cached_data, age, expired = cache_get('traffic_flood')
    if cached_data and not expired:
        return jsonify(cached_data)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live
    features = []
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/flood.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get('features', [])
            for item in items:
                props = item.get('properties', item)
                if props.get('ended', False):
                    continue
                feature = parse_traffic_item(item, 'Flood')
                if feature:
                    features.append(feature)
    except Exception as e:
        Log.error(f"Traffic flood error: {e}")
    
    result = {'type': 'FeatureCollection', 'features': features, 'count': len(features)}
    cache_set('traffic_flood', result, CACHE_TTL_TRAFFIC)
    return jsonify(result)


@app.route('/api/traffic/flood/raw')
@cached(ttl=CACHE_TTL_TRAFFIC)
def traffic_flood_raw():
    """Live Traffic NSW - raw flood data"""
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/flood.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            return jsonify(r.json())
    except Exception as e:
        Log.error(f"Traffic flood raw error: {e}")
    return jsonify({'type': 'FeatureCollection', 'features': []})


@app.route('/api/traffic/fire')
def traffic_fire():
    """Live Traffic NSW - fire hazards only (uses persistent cache)"""
    cached_data, age, expired = cache_get('traffic_fire')
    if cached_data and not expired:
        return jsonify(cached_data)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live
    features = []
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/fire.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get('features', [])
            for item in items:
                props = item.get('properties', item)
                if props.get('ended', False):
                    continue
                feature = parse_traffic_item(item, 'Fire')
                if feature:
                    features.append(feature)
    except Exception as e:
        Log.error(f"Traffic fire error: {e}")
    
    result = {'type': 'FeatureCollection', 'features': features, 'count': len(features)}
    cache_set('traffic_fire', result, CACHE_TTL_TRAFFIC)
    return jsonify(result)


@app.route('/api/traffic/fire/raw')
@cached(ttl=CACHE_TTL_TRAFFIC)
def traffic_fire_raw():
    """Live Traffic NSW - raw fire data"""
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/fire.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            return jsonify(r.json())
    except Exception as e:
        Log.error(f"Traffic fire raw error: {e}")
    return jsonify({'type': 'FeatureCollection', 'features': []})


@app.route('/api/traffic/majorevent')
def traffic_majorevent():
    """Live Traffic NSW - major events only (uses persistent cache)"""
    cached_data, age, expired = cache_get('traffic_majorevent')
    if cached_data and not expired:
        return jsonify(cached_data)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live
    features = []
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/majorevent.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get('features', [])
            for item in items:
                props = item.get('properties', item)
                if props.get('ended', False):
                    continue
                feature = parse_traffic_item(item, 'Major Event')
                if feature:
                    features.append(feature)
    except Exception as e:
        Log.error(f"Traffic major event error: {e}")
    
    result = {'type': 'FeatureCollection', 'features': features, 'count': len(features)}
    cache_set('traffic_majorevent', result, CACHE_TTL_TRAFFIC)
    return jsonify(result)


@app.route('/api/traffic/majorevent/raw')
@cached(ttl=CACHE_TTL_TRAFFIC)
def traffic_majorevent_raw():
    """Live Traffic NSW - raw major event data"""
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/majorevent.json', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            return jsonify(r.json())
    except Exception as e:
        Log.error(f"Traffic majorevent raw error: {e}")
    return jsonify({'type': 'FeatureCollection', 'features': []})


@app.route('/api/traffic/cameras')
def traffic_cameras():
    """Live Traffic cameras (uses persistent cache)"""
    # Check persistent cache first
    cached_data, age, expired = cache_get('traffic_cameras')
    if cached_data and not expired:
        return jsonify(cached_data)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live
    features = []
    try:
        r = requests.get(
            'https://www.livetraffic.com/datajson/all-feeds-web.json',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            data = r.json()
            if not isinstance(data, list):
                data = []
            
            # Filter for camera entries only (eventType: liveCams or eventCategory: liveCams)
            for item in data:
                if not isinstance(item, dict):
                    continue
                
                # Check if it's a camera
                event_type = item.get('eventType', '').lower()
                event_category = item.get('eventCategory', '').lower()
                
                if 'livecam' not in event_type and 'livecam' not in event_category:
                    # Also accept items that have camera-like properties
                    href = item.get('properties', {}).get('href', '')
                    if not (href.endswith('.jpeg') or href.endswith('.jpg')):
                        continue
                
                # Get coordinates from geometry
                geometry = item.get('geometry', {})
                if geometry.get('type') != 'Point':
                    continue
                coords = geometry.get('coordinates', [])
                if len(coords) < 2:
                    continue
                
                lon, lat = coords[0], coords[1]
                props = item.get('properties', {})
                
                features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [lon, lat]
                    },
                    'properties': {
                        'id': item.get('id', ''),
                        'title': props.get('title', 'Traffic Camera'),
                        'view': props.get('view', ''),
                        'direction': props.get('direction', ''),
                        'region': props.get('region', ''),
                        'imageUrl': props.get('href', ''),
                        'path': item.get('path', ''),
                        'source': 'livetraffic_cameras'
                    }
                })
    except Exception as e:
        Log.error(f"Cameras error: {e}")
    
    return jsonify({
        'type': 'FeatureCollection',
        'features': features,
        'count': len(features)
    })


# ==================== AIRSERVICES AUSTRALIA NONCE AUTO-UPDATE ====================
# The weathercams API requires a nonce that changes periodically.
# This system automatically fetches the current nonce from their site.

_airservices_nonce_cache = {
    'nonce': None,
    'expires': 0
}
_airservices_nonce_lock = threading.Lock()

def get_airservices_nonce():
    """
    Fetch and cache the current nonce from Airservices Australia weathercams.
    The nonce is embedded in the page's JavaScript and changes periodically.
    Caches for 1 hour to avoid hammering their server.
    """
    global _airservices_nonce_cache
    
    with _airservices_nonce_lock:
        # Return cached nonce if still valid
        if _airservices_nonce_cache['nonce'] and time.time() < _airservices_nonce_cache['expires']:
            return _airservices_nonce_cache['nonce']
        
        try:
            # Fetch the main weathercams page
            r = requests.get(
                'https://weathercams.airservicesaustralia.com/',
                timeout=15,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            )
            
            if r.status_code == 200:
                # Look for nonce in the page - it's typically in a JavaScript variable or data attribute
                # Pattern 1: nonce: 'xxxxx' or nonce:'xxxxx' or "nonce":"xxxxx"
                patterns = [
                    r'["\']nonce["\']\s*:\s*["\']([a-f0-9]+)["\']',
                    r'nonce[=:]\s*["\']([a-f0-9]+)["\']',
                    r'&nonce=([a-f0-9]+)',
                    r'nonce%22%3A%22([a-f0-9]+)',
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, r.text, re.IGNORECASE)
                    if match:
                        nonce = match.group(1)
                        # Cache for 1 hour
                        _airservices_nonce_cache = {
                            'nonce': nonce,
                            'expires': time.time() + 3600
                        }
                        Log.api(f"Airservices nonce: {nonce}")
                        return nonce
                
                # If no nonce found in patterns, log warning
                if DEV_MODE:
                    Log.warn("Airservices: Could not find nonce, using fallback")
                    
        except Exception as e:
            if DEV_MODE:
                Log.error(f"Airservices nonce fetch error: {e}")
        
        # Fallback to last known working nonce
        fallback_nonce = 'da9010b391'
        _airservices_nonce_cache = {
            'nonce': fallback_nonce,
            'expires': time.time() + 300  # Only cache fallback for 5 mins
        }
        return fallback_nonce


@app.route('/api/aviation/cameras')
def aviation_cameras():
    """Airport weather cameras (uses persistent cache)"""
    # Check persistent cache first
    cached_data, age, expired = cache_get('aviation_cameras')
    if cached_data and not expired:
        return jsonify(cached_data)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live
    features = []
    nonce = get_airservices_nonce()
    try:
        r = requests.get(
            'https://weathercams.airservicesaustralia.com/wp-admin/admin-ajax.php',
            params={
                'action': 'get_airports_list',
                'filter': 'all',
                'type': 'map',
                'filter_type': 'normal',
                'nonce': nonce
            },
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            data = r.json()
            airport_list = data.get('airport_list', [])
            
            for airport in airport_list:
                if not isinstance(airport, dict):
                    continue
                
                # Get coordinates - skip if missing
                lat_str = airport.get('lat', '')
                lon_str = airport.get('long', '')
                if not lat_str or not lon_str:
                    continue
                
                try:
                    lat = float(lat_str)
                    lon = float(lon_str)
                except (ValueError, TypeError):
                    continue
                
                # Build image URL with cache-busting
                thumbnail = airport.get('thumbnail', '')
                
                features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [lon, lat]
                    },
                    'properties': {
                        'id': airport.get('id', ''),
                        'title': airport.get('title', 'Airport Camera'),
                        'name': airport.get('name', ''),
                        'state': airport.get('state', ''),
                        'state_full': airport.get('state_full', ''),
                        'link': airport.get('link', ''),
                        'imageUrl': thumbnail,
                        'source': 'airservices_australia'
                    }
                })
    except Exception as e:
        Log.error(f"Aviation cameras error: {e}")
    
    return jsonify({
        'type': 'FeatureCollection',
        'features': features,
        'count': len(features)
    })


@app.route('/api/aviation/cameras/<airport_name>')
@cached(ttl=120)  # 2 minute cache for detail views
def aviation_camera_detail(airport_name):
    """Get all camera angles for a specific airport from Airservices Australia"""
    nonce = get_airservices_nonce()
    try:
        r = requests.get(
            'https://weathercams.airservicesaustralia.com/wp-admin/admin-ajax.php',
            params={
                'action': 'get_airport_modal',
                'airport': airport_name,
                'nonce': nonce
            },
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            data = r.json()
            modal = data.get('modal', {})
            
            # Extract all available camera angles
            cameras = []
            directions = ['north', 'east', 'south', 'west']
            direction_labels = {
                'north': 'North',
                'east': 'East', 
                'south': 'South',
                'west': 'West'
            }
            
            for direction in directions:
                image_key = f'{direction}_image'
                thumb_key = f'{direction}_thumbnail'
                angle_key = f'{direction}_angle'
                timestamp_key = f'{direction}_timestamp'
                
                if modal.get(image_key):
                    cameras.append({
                        'direction': direction_labels[direction],
                        'angle': modal.get(angle_key, ''),
                        'imageUrl': modal.get(image_key, ''),
                        'thumbnail': modal.get(thumb_key, ''),
                        'timestamp': modal.get(timestamp_key, '')
                    })
            
            return jsonify({
                'title': modal.get('title', airport_name),
                'state': modal.get('state', ''),
                'name': modal.get('name', airport_name),
                'cameras': cameras,
                'count': len(cameras)
            })
    except Exception as e:
        Log.error(f"Aviation camera detail error for {airport_name}: {e}")
    
    return jsonify({
        'title': airport_name,
        'cameras': [],
        'count': 0,
        'error': 'Failed to fetch camera details'
    })


_centralwatch_last_fetch = {'time': 0, 'data': []}

# Central Watch camera data - loaded from JSON file
_centralwatch_json_path = os.path.join(os.path.dirname(__file__), 'data', 'centralwatch_cameras.json')
_centralwatch_static_sites = {}
_centralwatch_static_data = []
_centralwatch_camera_timestamps = {}  # camera_id -> ISO timestamp of latest known image

# =============================================================================
# Central Watch Browser Worker Thread
# =============================================================================
# Playwright's sync API is greenlet-bound: all calls MUST happen on the same
# thread that created the playwright instance. We use a dedicated worker thread
# with a queue so any thread can request browser fetches safely.
#
# Architecture:
#   - _cw_browser_worker_thread: dedicated thread that owns the playwright browser
#   - _cw_request_queue: other threads put (type, url, result_queue) tuples here
#   - The worker processes requests sequentially and puts results in result_queue
#   - _browser_fetch_json / _browser_fetch_image are thread-safe wrappers
# =============================================================================
import queue as _queue_mod

_cw_request_queue = _queue_mod.Queue()
_cw_browser_worker_thread = None
_centralwatch_browser_ready = False

# Check if playwright is available for solving Vercel Security Checkpoint
_playwright_available = False
try:
    from playwright.sync_api import sync_playwright
    _playwright_available = True
except ImportError:
    pass

# Fallback: requests session for when playwright is not available
_centralwatch_session = None
_centralwatch_session_lock = threading.Lock()

# API endpoint for Central Watch camera list.
# Note: /api/v1/public/cameras was removed upstream and now always 404s — every
# attempt counts toward the upstream rate limit, so we no longer try it.
_CENTRALWATCH_API_ENDPOINTS = [
    'https://centralwatch.watchtowers.io/au/api/cameras',
]

def _cw_browser_worker():
    """Dedicated thread that owns the playwright browser. Processes all CW fetch
    requests from the queue. This ensures all playwright calls happen on one thread.
    
    Lifecycle: init browser → solve Vercel challenge → process queue → shutdown.
    Requires: pip install playwright && python -m playwright install chromium
    """
    global _centralwatch_browser_ready
    
    pw = None
    browser = None
    page = None
    
    try:
        if DEV_MODE:
            Log.info("Central Watch: Starting browser worker...")
        pw = sync_playwright().start()
        browser = pw.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-cache',
                '--disk-cache-size=0',
                '--disable-background-networking',
                '--disable-backing-store-limit',
                '--aggressive-cache-discard',
            ]
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            locale='en-AU',
            timezone_id='Australia/Sydney',
        )
        page = context.new_page()
        page.add_init_script('Object.defineProperty(navigator, "webdriver", {get: () => undefined})')
        
        # Solve Vercel challenge
        page.goto('https://centralwatch.watchtowers.io/au', timeout=45000)
        try:
            page.wait_for_function(
                '() => !document.title.includes("Vercel") && !document.title.includes("Security")',
                timeout=30000
            )
            Log.info("Central Watch: Browser ready")
        except Exception:
            Log.warn(f"Central Watch: Vercel challenge timeout (title: {page.title()})")
        
        page.wait_for_timeout(2000)
        cookies = context.cookies('https://centralwatch.watchtowers.io')
        cookie_names = [c['name'] for c in cookies]
        if DEV_MODE:
            Log.info(f"Central Watch: Browser ready with {len(cookies)} cookies: {cookie_names}")
        
        _centralwatch_browser_ready = True
        
    except Exception as e:
        Log.error(f"Central Watch: Failed to start browser: {e}")
        # Clean up on failure
        try:
            if page: page.close()
        except Exception: pass
        try:
            if context: context.close()
        except Exception: pass
        try:
            if browser: browser.close()
        except Exception: pass
        try:
            if pw: pw.stop()
        except Exception: pass
        _centralwatch_browser_ready = False
        # Drain any pending requests with None results
        while not _cw_request_queue.empty():
            try:
                _, _, result_q = _cw_request_queue.get_nowait()
                result_q.put(None)
            except _queue_mod.Empty:
                break
        return
    
    # === Main request processing loop ===
    last_challenge_refresh = time.time()
    last_memory_cleanup = time.time()

    while True:
        try:
            # Wait for a request (with timeout so we can do periodic maintenance)
            try:
                task = _cw_request_queue.get(timeout=30)
            except _queue_mod.Empty:
                now = time.time()
                # Periodic: re-solve Vercel challenge every 15 min to keep cookies fresh
                if now - last_challenge_refresh >= 900:
                    try:
                        page.goto('https://centralwatch.watchtowers.io/au', timeout=45000)
                        page.wait_for_function(
                            '() => !document.title.includes("Vercel")',
                            timeout=30000
                        )
                        page.wait_for_timeout(2000)
                        last_challenge_refresh = time.time()
                        Log.info("Central Watch: Browser session refreshed")
                    except Exception as e:
                        Log.warn(f"Central Watch: Browser session refresh failed: {e}")
                # Periodic: clear browser memory caches every 5 min
                if now - last_memory_cleanup >= 300:
                    try:
                        cdp = context.new_cdp_session(page)
                        cdp.send('Network.clearBrowserCache')
                        cdp.detach()
                        last_memory_cleanup = now
                    except Exception:
                        pass
                continue
            
            if task is None:
                # Shutdown signal
                break
            
            fetch_type, url, result_q = task
            
            try:
                if fetch_type == 'json':
                    result = page.evaluate('''async ([url, timeout]) => {
                        const controller = new AbortController();
                        const timer = setTimeout(() => controller.abort(), timeout);
                        try {
                            const resp = await fetch(url, { signal: controller.signal });
                            clearTimeout(timer);
                            if (!resp.ok) return { ok: false, status: resp.status };
                            const data = await resp.json();
                            return { ok: true, status: resp.status, data: data };
                        } catch (e) {
                            clearTimeout(timer);
                            return { ok: false, error: e.toString() };
                        }
                    }''', [url, 20000])
                    
                    if result and result.get('ok'):
                        result_q.put(result.get('data'))
                    else:
                        status = result.get('status', '?') if result else '?'
                        if DEV_MODE:
                            Log.warn(f"Central Watch browser JSON: HTTP {status} for {url.split('.io')[-1][:60]}")
                        result_q.put(None)
                
                elif fetch_type == 'image':
                    result = page.evaluate('''async ([url, timeout]) => {
                        const controller = new AbortController();
                        const timer = setTimeout(() => controller.abort(), timeout);
                        try {
                            const resp = await fetch(url, { signal: controller.signal });
                            clearTimeout(timer);
                            if (!resp.ok) return { ok: false, status: resp.status };
                            const blob = await resp.blob();
                            if (!blob.type.startsWith('image/')) return { ok: false, status: resp.status, type: blob.type };
                            const buf = await blob.arrayBuffer();
                            const bytes = new Uint8Array(buf);
                            let binary = '';
                            for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
                            const dataUrl = 'data:' + blob.type + ';base64,' + btoa(binary);
                            return { ok: true, status: resp.status, contentType: blob.type, size: blob.size, data: dataUrl };
                        } catch (e) {
                            clearTimeout(timer);
                            return { ok: false, error: e.toString() };
                        }
                    }''', [url, 15000])
                    
                    if result and result.get('ok') and result.get('data'):
                        data_url = result['data']
                        _, b64_data = data_url.split(',', 1)
                        image_bytes = base64.b64decode(b64_data)
                        content_type = result.get('contentType', 'image/jpeg')
                        result_q.put((image_bytes, content_type))
                    else:
                        status = result.get('status', '?') if result else '?'
                        if DEV_MODE:
                            Log.warn(f"Central Watch browser image: HTTP {status} for {url.split('.io')[-1][:60]}")
                        result_q.put((None, None))
                
                elif fetch_type == 'batch_images':
                    # url parameter is actually a list of [camera_id, image_url] pairs
                    # Uses fetch() — kept as fallback for when we need HTTP status codes
                    image_list = url  # repurposed parameter
                    
                    result = page.evaluate('''async (imageList) => {
                        const TIMEOUT = 15000;
                        const results = await Promise.allSettled(imageList.map(async ([id, url]) => {
                            const controller = new AbortController();
                            const timer = setTimeout(() => controller.abort(), TIMEOUT);
                            try {
                                const resp = await fetch(url, { signal: controller.signal });
                                clearTimeout(timer);
                                if (!resp.ok) {
                                    const ra = resp.headers.get('Retry-After');
                                    return { id, ok: false, status: resp.status, retryAfter: ra ? (parseInt(ra) || null) : null };
                                }
                                const blob = await resp.blob();
                                if (!blob.type.startsWith('image/')) return { id, ok: false, type: blob.type };
                                // Use arrayBuffer instead of FileReader to avoid leaking reader objects
                                const buf = await blob.arrayBuffer();
                                const bytes = new Uint8Array(buf);
                                let binary = '';
                                for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
                                const dataUrl = 'data:' + blob.type + ';base64,' + btoa(binary);
                                return { id, ok: true, contentType: blob.type, size: blob.size, data: dataUrl };
                            } catch (e) {
                                clearTimeout(timer);
                                return { id, ok: false, error: e.toString() };
                            }
                        }));
                        return results.map(r => r.status === 'fulfilled' ? r.value : { id: null, ok: false, error: String(r.reason) });
                    }''', image_list)
                    
                    result_q.put(result or [])
                
                elif fetch_type == 'batch_images_dom':
                    # Load images via <img> DOM elements instead of fetch().
                    # Key difference: browser sends Sec-Fetch-Dest: image (not empty),
                    # which most rate limiters (Vercel, Cloudflare) treat as a normal
                    # resource load, not an API call — bypassing API rate limits.
                    # The CW website itself loads images this way, so it can't be blocked.
                    image_list = url  # repurposed parameter
                    
                    result = page.evaluate('''async (imageList) => {
                        const TIMEOUT = 25000;
                        const results = await Promise.allSettled(imageList.map(([id, url]) => {
                            return new Promise((resolve) => {
                                const timer = setTimeout(() => {
                                    img.src = '';
                                    resolve({ id, ok: false, error: 'timeout' });
                                }, TIMEOUT);

                                const img = new Image();

                                img.onload = () => {
                                    clearTimeout(timer);
                                    try {
                                        const c = document.createElement('canvas');
                                        c.width = img.naturalWidth;
                                        c.height = img.naturalHeight;
                                        const ctx = c.getContext('2d');
                                        ctx.drawImage(img, 0, 0);
                                        const dataUrl = c.toDataURL('image/jpeg', 0.92);
                                        // Release DOM resources
                                        img.src = '';
                                        c.width = 0;
                                        c.height = 0;
                                        resolve({
                                            id, ok: true,
                                            contentType: 'image/jpeg',
                                            size: img.naturalWidth * img.naturalHeight,
                                            data: dataUrl
                                        });
                                    } catch (e) {
                                        img.src = '';
                                        resolve({ id, ok: false, error: 'canvas: ' + e.toString() });
                                    }
                                };

                                img.onerror = () => {
                                    clearTimeout(timer);
                                    img.src = '';
                                    resolve({ id, ok: false, error: 'img_load_failed' });
                                };

                                img.src = url;
                            });
                        }));
                        return results.map(r => r.status === 'fulfilled' ? r.value : { id: null, ok: false, error: String(r.reason) });
                    }''', image_list)
                    
                    result_q.put(result or [])
                
                else:
                    result_q.put(None)
                    
            except Exception as e:
                Log.error(f"Central Watch browser worker error ({fetch_type}): {e}")
                result_q.put(None if fetch_type == 'json' else ([] if fetch_type in ('batch_images', 'batch_images_dom') else (None, None)))
                
        except Exception as e:
            Log.error(f"Central Watch browser worker loop error: {e}")
            time.sleep(1)
    
    # === Shutdown ===
    _centralwatch_browser_ready = False
    if DEV_MODE:
        Log.info("Central Watch: Browser worker shutting down...")
    try:
        if page: page.close()
    except Exception: pass
    try:
        if context: context.close()
    except Exception: pass
    try:
        if browser: browser.close()
    except Exception: pass
    try:
        if pw: pw.stop()
    except Exception: pass

def _start_cw_browser_worker():
    """Start the dedicated browser worker thread"""
    global _cw_browser_worker_thread
    if not _playwright_available:
        Log.warn("Central Watch: playwright not installed - images will not load")
        Log.warn("Central Watch: Install: pip install playwright && python -m playwright install chromium")
        return
    if _cw_browser_worker_thread and _cw_browser_worker_thread.is_alive():
        return  # Already running
    _cw_browser_worker_thread = threading.Thread(target=_cw_browser_worker, daemon=True, name='cw-browser')
    _cw_browser_worker_thread.start()

def _stop_cw_browser_worker():
    """Stop the browser worker thread"""
    global _centralwatch_browser_ready
    _centralwatch_browser_ready = False
    try:
        _cw_request_queue.put(None)  # Shutdown signal
    except Exception:
        pass

def _browser_fetch_json(url, timeout=20):
    """Thread-safe: fetch JSON via the browser worker. Any thread can call this.
    Returns parsed JSON dict/list, or None on failure."""
    if not _centralwatch_browser_ready:
        return None
    result_q = _queue_mod.Queue()
    _cw_request_queue.put(('json', url, result_q))
    try:
        return result_q.get(timeout=timeout + 10)
    except _queue_mod.Empty:
        Log.warn(f"Central Watch browser JSON fetch timed out for {url.split('.io')[-1][:60]}")
        return None

def _browser_fetch_image(url, timeout=15):
    """Thread-safe: fetch image via the browser worker. Any thread can call this.
    Returns (image_bytes, content_type) or (None, None) on failure."""
    if not _centralwatch_browser_ready:
        return None, None
    result_q = _queue_mod.Queue()
    _cw_request_queue.put(('image', url, result_q))
    try:
        return result_q.get(timeout=timeout + 10)
    except _queue_mod.Empty:
        Log.warn(f"Central Watch browser image fetch timed out for {url.split('.io')[-1][:60]}")
        return None, None

def _browser_batch_fetch_images(image_list, timeout=60):
    """Thread-safe: fetch ALL images in parallel via fetch() in the browser worker.
    Returns status codes on failure (useful for backoff logic).
    
    image_list: list of [camera_id, image_url] pairs
    Returns list of result dicts with 'id', 'ok', 'data', 'contentType', 'status', etc."""
    if not _centralwatch_browser_ready or not image_list:
        return []
    result_q = _queue_mod.Queue()
    _cw_request_queue.put(('batch_images', image_list, result_q))
    try:
        return result_q.get(timeout=timeout + 10)
    except _queue_mod.Empty:
        Log.warn(f"Central Watch browser batch fetch timed out ({len(image_list)} images)")
        return []

def _browser_dom_batch_fetch_images(image_list, timeout=60):
    """Thread-safe: fetch images using <img> DOM elements instead of fetch().
    
    This bypasses API rate limiting because <img> requests send Sec-Fetch-Dest: image
    (which rate limiters treat as resource loads, not API calls). The CW website itself
    loads camera images via <img> tags, so this request profile can't be blocked.
    
    Note: on failure, no HTTP status code is available (just 'img_load_failed').
    Use _browser_batch_fetch_images() when you need status codes for backoff.
    
    image_list: list of [camera_id, image_url] pairs
    Returns list of result dicts with 'id', 'ok', 'data', 'contentType', etc."""
    if not _centralwatch_browser_ready or not image_list:
        return []
    result_q = _queue_mod.Queue()
    _cw_request_queue.put(('batch_images_dom', image_list, result_q))
    try:
        return result_q.get(timeout=timeout + 10)
    except _queue_mod.Empty:
        Log.warn(f"Central Watch browser DOM batch fetch timed out ({len(image_list)} images)")
        return []

def _get_centralwatch_session():
    """Get or create a basic requests session (fallback when playwright not available)."""
    global _centralwatch_session
    with _centralwatch_session_lock:
        if _centralwatch_session is None:
            _centralwatch_session = requests.Session()
            _centralwatch_session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-AU,en;q=0.9,en-US;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://centralwatch.watchtowers.io/au',
                'Origin': 'https://centralwatch.watchtowers.io',
            })
            try:
                _centralwatch_session.get('https://centralwatch.watchtowers.io/au', timeout=10,
                    headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'})
            except Exception:
                pass
    return _centralwatch_session

def _reset_centralwatch_session():
    """Reset the requests session (fallback)"""
    global _centralwatch_session
    with _centralwatch_session_lock:
        if _centralwatch_session is not None:
            try:
                _centralwatch_session.close()
            except Exception:
                pass
        _centralwatch_session = None

def _centralwatch_api_fetch(timeout=20):
    """Fetch camera data from Central Watch, trying multiple endpoints.
    Uses playwright browser if available (bypasses Vercel), falls back to requests.
    Returns (data_dict, endpoint_used) or (None, None) on failure."""
    
    # Try browser-based fetch first (bypasses Vercel TLS fingerprinting)
    if _centralwatch_browser_ready:
        for endpoint in _CENTRALWATCH_API_ENDPOINTS:
            data = _browser_fetch_json(endpoint, timeout=timeout)
            if data and isinstance(data, dict) and (data.get('cameras') or data.get('ok')):
                Log.info(f"Central Watch: Got data via browser from {endpoint.split('.io')[-1]} ({len(data.get('cameras', []))} cameras)")
                return data, endpoint
            time.sleep(1)
        Log.warn("Central Watch: Browser fetch failed for all endpoints")
    
    # Try curl_cffi (TLS fingerprint spoofing) before plain requests
    if CURL_CFFI_AVAILABLE and curl_requests:
        for endpoint in _CENTRALWATCH_API_ENDPOINTS:
            try:
                r = curl_requests.get(endpoint, timeout=timeout, impersonate='chrome',
                    headers={'Accept': 'application/json', 'Accept-Language': 'en-AU,en;q=0.9',
                             'Referer': 'https://centralwatch.watchtowers.io/au'})
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, dict) and (data.get('cameras') or data.get('ok')):
                        Log.info(f"Central Watch: Got data via curl_cffi from {endpoint.split('.io')[-1]} ({len(data.get('cameras', []))} cameras)")
                        return data, endpoint
                elif r.status_code == 429:
                    Log.warn(f"Central Watch: curl_cffi 429 from {endpoint.split('.io')[-1]}, trying next...")
                    time.sleep(1)
                else:
                    Log.warn(f"Central Watch: curl_cffi HTTP {r.status_code} from {endpoint.split('.io')[-1]}")
            except Exception as e:
                Log.warn(f"Central Watch: curl_cffi error from {endpoint.split('.io')[-1]}: {e}")

    # Fallback to requests session
    session = _get_centralwatch_session()
    for endpoint in _CENTRALWATCH_API_ENDPOINTS:
        try:
            r = session.get(endpoint, timeout=timeout)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict) and (data.get('cameras') or data.get('ok')):
                    Log.info(f"Central Watch: Got data from {endpoint.split('.io')[-1]} ({len(data.get('cameras', []))} cameras)")
                    return data, endpoint
            elif r.status_code == 429:
                Log.warn(f"Central Watch: 429 from {endpoint.split('.io')[-1]}, trying next...")
                time.sleep(1)
                continue
            else:
                Log.warn(f"Central Watch: HTTP {r.status_code} from {endpoint.split('.io')[-1]}")
                continue
        except Exception as e:
            Log.warn(f"Central Watch: Error from {endpoint.split('.io')[-1]}: {e}")
            continue

    _reset_centralwatch_session()
    return None, None

def _load_centralwatch_json():
    """Load Central Watch camera data from JSON file and seed image timestamps"""
    global _centralwatch_static_sites, _centralwatch_static_data, _centralwatch_camera_timestamps
    try:
        if os.path.exists(_centralwatch_json_path):
            with open(_centralwatch_json_path, 'r') as f:
                data = json.load(f)
                _centralwatch_static_sites = data.get('sites', {})
                
                # Process cameras into the format expected by the API
                sites = data.get('sites', {})
                cameras = []
                timestamps_seeded = 0
                for cam in data.get('cameras', []):
                    site_id = cam.get('siteId')
                    site = sites.get(site_id, {})
                    if site:
                        cam_id = cam.get('id')
                        cam_time = cam.get('time', '')
                        # Normalize timestamp to Z format for URL
                        cam_time_z = cam_time
                        if cam_time_z and '+' in cam_time_z and not cam_time_z.endswith('Z'):
                            cam_time_z = cam_time_z.split('+')[0] + 'Z'
                        # All images served through our backend proxy (backend solves Vercel challenge)
                        cameras.append({
                            'id': cam_id,
                            'name': cam.get('name'),
                            'siteName': site.get('name', ''),
                            'siteId': site_id,
                            'latitude': site.get('latitude'),
                            'longitude': site.get('longitude'),
                            'altitude': site.get('altitude'),
                            'state': site.get('state', ''),
                            'imageUrl': f"/api/centralwatch/image/{cam_id}",
                            'time': cam_time,
                            'source': 'centralwatch'
                        })
                        # Seed image timestamps from JSON
                        if cam_id and cam_time:
                            _centralwatch_camera_timestamps[cam_id] = cam_time
                            timestamps_seeded += 1
                _centralwatch_static_data = cameras
                Log.startup(f"Loaded {len(cameras)} Central Watch cameras from JSON (seeded {timestamps_seeded} image timestamps)")
    except Exception as e:
        Log.error(f"Failed to load Central Watch JSON: {e}")

# Load on module import
_load_centralwatch_json()

_centralwatch_json_last_update = 0
_CENTRALWATCH_JSON_REFRESH_INTERVAL = 600  # Try to refresh JSON every 10 minutes (session-based requests avoid rate limits)

def _update_centralwatch_json(api_data):
    """Update the Central Watch JSON file with fresh data from the API.
    Called when we successfully fetch from the live API."""
    global _centralwatch_json_last_update, _centralwatch_static_sites, _centralwatch_static_data, _centralwatch_camera_timestamps
    try:
        if not api_data or not isinstance(api_data, dict):
            return False
        
        raw_sites = api_data.get('sites', [])
        raw_cameras = api_data.get('cameras', [])
        
        if not raw_sites or not raw_cameras:
            Log.warn(f"Central Watch JSON update skipped: {len(raw_sites)} sites, {len(raw_cameras)} cameras")
            return False
        
        # Build the JSON structure
        sites_dict = {}
        for site in raw_sites:
            sites_dict[site['id']] = {
                'name': site.get('name', ''),
                'latitude': site.get('latitude'),
                'longitude': site.get('longitude'),
                'altitude': site.get('altitude'),
                'state': site.get('state', ''),
            }
        
        cameras_list = []
        for cam in raw_cameras:
            cam_time = cam.get('time', '')
            # Normalize time format to Z suffix
            if cam_time and '+' in cam_time and not cam_time.endswith('Z'):
                cam_time = cam_time.split('+')[0] + 'Z'
            cameras_list.append({
                'id': cam.get('id'),
                'name': cam.get('name', ''),
                'siteId': cam.get('siteId', ''),
                'time': cam_time
            })
        
        from datetime import datetime as dt_cls
        json_data = {
            'lastUpdated': dt_cls.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'source': 'https://centralwatch.watchtowers.io/au/api/cameras',
            'sites': sites_dict,
            'cameras': cameras_list
        }
        
        # Write to disk atomically (write temp then rename)
        tmp_path = _centralwatch_json_path + '.tmp'
        with open(tmp_path, 'w') as f:
            json.dump(json_data, f, indent=2)
        os.replace(tmp_path, _centralwatch_json_path)
        
        _centralwatch_json_last_update = time.time()
        
        # Reload in-memory data from the fresh JSON
        _load_centralwatch_json()

        if DEV_MODE:
            Log.info(f"Central Watch JSON updated: {len(sites_dict)} sites, {len(cameras_list)} cameras")
        return True
    except Exception as e:
        Log.error(f"Failed to update Central Watch JSON: {e}")
        # Clean up temp file if it exists
        try:
            tmp_path = _centralwatch_json_path + '.tmp'
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except:
            pass
        return False

def _refresh_centralwatch_data():
    """Background task: fetch fresh data from Central Watch API and update JSON + in-memory data.
    Only runs every 30 minutes to avoid rate limiting.
    Uses session-based requests with cookie persistence and tries multiple endpoints."""
    global _centralwatch_json_last_update
    
    now = time.time()
    if now - _centralwatch_json_last_update < _CENTRALWATCH_JSON_REFRESH_INTERVAL:
        return False
    
    try:
        data, endpoint = _centralwatch_api_fetch(timeout=20)
        
        if data and isinstance(data, dict) and data.get('cameras'):
            # Process into normalized format for the cache
            cameras = _process_centralwatch_response(data)
            if cameras:
                # Update persistent cache
                cache_set('centralwatch_cameras', cameras, 21600)
                _centralwatch_last_fetch['data'] = cameras
                _centralwatch_last_fetch['time'] = now
            
            # Update JSON file on disk with raw API data
            _update_centralwatch_json(data)
            
            # Update image timestamps
            for cam in data.get('cameras', []):
                cam_id = cam.get('id')
                cam_time = cam.get('time', '')
                if cam_id and cam_time:
                    if '+' in cam_time and not cam_time.endswith('Z'):
                        cam_time = cam_time.split('+')[0] + 'Z'
                    _centralwatch_camera_timestamps[cam_id] = cam_time
            
            _centralwatch_json_last_update = now
            Log.info(f"Central Watch: Refreshed data from API ({len(cameras)} cameras)")
            return True
        else:
            Log.warn("Central Watch: All API endpoints failed on data refresh")
            _centralwatch_json_last_update = now  # Don't retry immediately
            return False
    except Exception as e:
        Log.error(f"Central Watch data refresh error: {e}")
        _centralwatch_json_last_update = now
        return False

def _process_centralwatch_response(data):
    """Process Central Watch API response into normalized camera list with coordinates"""
    cameras = []
    if not data or not isinstance(data, dict):
        return cameras
    
    sites = {}
    # Build sites lookup from response or use static
    for site in data.get('sites', []):
        sites[site['id']] = site
    
    # If no sites in response, use static
    if not sites:
        sites = _centralwatch_static_sites
    
    # Process cameras
    for cam in data.get('cameras', []):
        site_id = cam.get('siteId')
        site = sites.get(site_id) or _centralwatch_static_sites.get(site_id)
        
        if site and site.get('latitude') and site.get('longitude'):
            cam_id = cam.get('id')
            cam_time = cam.get('time', '')
            # Normalize timestamp to Z format for URL
            cam_time_z = cam_time
            if cam_time_z and '+' in cam_time_z and not cam_time_z.endswith('Z'):
                cam_time_z = cam_time_z.split('+')[0] + 'Z'
            # All images served through our backend proxy (backend solves Vercel challenge)
            cameras.append({
                'id': cam_id,
                'name': cam.get('name', 'Fire Watch Camera'),
                'siteName': site.get('name', ''),
                'siteId': site_id,
                'latitude': site.get('latitude'),
                'longitude': site.get('longitude'),
                'altitude': site.get('altitude'),
                'state': site.get('state', ''),
                'imageUrl': f"/api/centralwatch/image/{cam_id}",
                'time': cam_time,
                'source': 'centralwatch'
            })
    
    return cameras
_centralwatch_fetch_interval = 600  # Try to fetch every 10 minutes (session-based requests)

def _apply_fresh_centralwatch_timestamps(cameras):
    """Apply the latest known timestamps to camera data.
    Images are always served through our proxy, so imageUrl stays as proxy path."""
    if not _centralwatch_camera_timestamps or not cameras:
        return cameras
    
    updated = []
    for cam in cameras:
        cam_id = cam.get('id')
        latest_ts = _centralwatch_camera_timestamps.get(cam_id)
        if latest_ts and cam_id and latest_ts != cam.get('time'):
            cam_copy = dict(cam)
            cam_copy['time'] = latest_ts
            # imageUrl stays as proxy path - backend handles CW fetching
            cam_copy['imageUrl'] = f"/api/centralwatch/image/{cam_id}"
            updated.append(cam_copy)
        else:
            updated.append(cam)
    
    return updated

@app.route('/api/centralwatch/cameras')
def centralwatch_cameras():
    """Central Watch fire tower cameras (fetched infrequently due to aggressive rate limiting)"""
    global _centralwatch_last_fetch
    
    now = time.time()
    cameras = None
    
    # Check persistent cache first
    cached_data, age, expired = cache_get('centralwatch_cameras')
    if cached_data and len(cached_data) > 0:
        cameras = cached_data
    
    # Use in-memory cache if available
    if not cameras and _centralwatch_last_fetch['data'] and len(_centralwatch_last_fetch['data']) > 0:
        cameras = _centralwatch_last_fetch['data']
    
    # Use static fallback data if available
    if not cameras and _centralwatch_static_data and len(_centralwatch_static_data) > 0:
        Log.info(f"Central Watch: Using static fallback data ({len(_centralwatch_static_data)} cameras)")
        cameras = _centralwatch_static_data
    
    # If we have camera data, apply freshest timestamps and return
    if cameras:
        return jsonify(_apply_fresh_centralwatch_timestamps(cameras))
    
    # Only attempt fetch if enough time has passed (avoid rate limiting)
    if now - _centralwatch_last_fetch['time'] < _centralwatch_fetch_interval:
        Log.info(f"Central Watch: Skipping fetch, last attempt was {int(now - _centralwatch_last_fetch['time'])}s ago (waiting {_centralwatch_fetch_interval}s)")
        return jsonify([])
    
    # Attempt to fetch using session + multi-endpoint approach
    _centralwatch_last_fetch['time'] = now
    cameras = []
    
    try:
        Log.info("Central Watch: Attempting fetch (session + multi-endpoint)...")
        data, endpoint = _centralwatch_api_fetch(timeout=30)
        
        if data:
            # Process the response (joins cameras with sites to get coordinates)
            cameras = _process_centralwatch_response(data)
            
            if cameras:
                Log.info(f"Central Watch: Got {len(cameras)} cameras!")
                _centralwatch_last_fetch['data'] = cameras
                # Cache for 6 hours since we successfully got data
                cache_set('centralwatch_cameras', cameras, 21600)
        else:
            Log.warn("Central Watch: All endpoints failed")
    except Exception as e:
        Log.error(f"Central Watch cameras error: {e}")
    
    return jsonify(_apply_fresh_centralwatch_timestamps(cameras) if cameras else cameras)


# In-memory cache for Central Watch camera images
# Structure: { camera_id: { 'data': bytes, 'content_type': str, 'timestamp': float } }
_centralwatch_image_cache = {}
# Tracks (camera_id, status_code) tuples we've already logged a failure
# for, so the same camera failing the same way every retry doesn't
# spam the log. Cleared per-cid on successful fetch.
_failed_cameras_logged = {}
# Guards concurrent iteration/mutation of _centralwatch_image_cache and
# _centralwatch_camera_timestamps. Individual .get()/dict[x]=v ops are atomic
# under GIL, but iteration (cleanup loop) is not — must hold this lock for
# iteration + snapshot.
_centralwatch_dicts_lock = threading.Lock()
# On-demand stale TTL removed — batch worker is sole fetcher, proxy is cache-only
_CENTRALWATCH_IMAGE_MAX_AGE = 120   # 2 minutes - max age before forced refresh on next request
_CENTRALWATCH_PREFETCH_STALE_TTL = 120  # 2 minutes - balance freshness vs rate limits (CW updates ~1/min)
_centralwatch_image_fetch_lock = threading.Lock()
# _centralwatch_image_refreshing removed — batch worker is sole fetcher now
_last_timestamp_refresh_attempt = 0
_TIMESTAMP_REFRESH_COOLDOWN = 120  # Try to refresh timestamps every 2 minutes (session-based)


def _cleanup_centralwatch_image_cache():
    """Remove stale images from CW cache — images older than 5 minutes
    and images for cameras no longer in the active camera list."""
    now = time.time()
    max_age = 300  # 5 minutes
    active_ids = {cam.get('id') for cam in _centralwatch_static_data if cam.get('id')}

    with _centralwatch_dicts_lock:
        stale = [cid for cid, entry in _centralwatch_image_cache.items()
                 if (now - entry.get('timestamp', 0)) > max_age or (active_ids and cid not in active_ids)]
        for cid in stale:
            _centralwatch_image_cache.pop(cid, None)
        remaining = len(_centralwatch_image_cache)

    if stale and DEV_MODE:
        Log.cleanup(f"CW image cache: evicted {len(stale)} stale images, {remaining} remaining")


def _refresh_centralwatch_timestamps(force=False):
    """Fetch the camera list from API to get the latest image timestamps.
    Uses session-based requests with cookie persistence and tries multiple endpoints.
    Has a cooldown to avoid rate-limit spam."""
    global _last_timestamp_refresh_attempt
    now = time.time()
    
    # Don't spam the API - enforce cooldown unless forced
    if not force and (now - _last_timestamp_refresh_attempt) < _TIMESTAMP_REFRESH_COOLDOWN:
        return bool(_centralwatch_camera_timestamps)
    
    _last_timestamp_refresh_attempt = now
    
    try:
        data, endpoint = _centralwatch_api_fetch(timeout=15)
        
        if data:
            cameras = data.get('cameras', [])
            updated = 0
            for cam in cameras:
                cam_id = cam.get('id')
                cam_time = cam.get('time', '')
                if cam_id and cam_time:
                    # Convert time to Z format: "2026-02-26T02:25:55.366+00:00" -> "2026-02-26T02:25:55.366Z"
                    if '+' in cam_time:
                        cam_time = cam_time.split('+')[0] + 'Z'
                    _centralwatch_camera_timestamps[cam_id] = cam_time
                    updated += 1
            if updated:
                Log.info(f"Central Watch: Updated timestamps for {updated} cameras")
            return updated > 0
        else:
            Log.warn("Central Watch: All endpoints failed for timestamp refresh")
            return False
    except Exception as e:
        Log.error(f"Central Watch timestamp refresh error: {e}")
        return False

def _fetch_centralwatch_image(camera_id):
    """Fetch a single Central Watch camera image and cache it.
    Uses the persistent playwright browser (bypasses Vercel TLS fingerprinting).
    Falls back to requests session if browser not available.
    
    CW API requires timestamp in path; try (now-2min) first, then API timestamp if 404."""
    try:
        from datetime import datetime as dt_cls, timezone as tz_cls, timedelta as td_cls
        timestamp = (dt_cls.now(tz_cls.utc) - td_cls(minutes=2)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        urls_to_try = [
            f"https://centralwatch.watchtowers.io/au/api/cameras/{camera_id}/image/{timestamp}"
        ]
        api_ts = _centralwatch_camera_timestamps.get(camera_id)
        if api_ts:
            ts = api_ts.split('+')[0] if '+' in api_ts else api_ts
            if '.' not in ts:
                ts += '.000'
            if not ts.endswith('Z'):
                ts += 'Z'
            url_api = f"https://centralwatch.watchtowers.io/au/api/cameras/{camera_id}/image/{ts}"
            if url_api not in urls_to_try:
                urls_to_try.append(url_api)
        
        # Try browser-based fetch first (bypasses Vercel completely)
        if _centralwatch_browser_ready:
            for image_url in urls_to_try:
                image_bytes, content_type = _browser_fetch_image(image_url)
                if image_bytes and len(image_bytes) > 500:
                    _centralwatch_image_cache[camera_id] = {
                        'data': image_bytes,
                        'content_type': content_type or 'image/jpeg',
                        'timestamp': time.time()
                    }
                    if DEV_MODE:
                        Log.info(f"Central Watch image OK (browser): {camera_id} ({len(image_bytes)} bytes)")
                    return True
            if DEV_MODE:
                Log.warn(f"Central Watch image: browser fetch failed for {camera_id} ({len(urls_to_try)} URLs tried)")
        
        # Fallback to requests session (will likely fail due to Vercel)
        session = _get_centralwatch_session()
        for image_url in urls_to_try:
            try:
                r = session.get(image_url, timeout=15, headers={
                    'Accept': 'image/jpeg, image/png, image/*, */*',
                })
                content_type = r.headers.get('Content-Type', '')
                if r.status_code == 200 and 'image' in content_type and len(r.content) > 500:
                    _centralwatch_image_cache[camera_id] = {
                        'data': r.content,
                        'content_type': content_type,
                        'timestamp': time.time()
                    }
                    if DEV_MODE:
                        Log.info(f"Central Watch image OK (requests): {camera_id} ({len(r.content)} bytes)")
                    return True
                elif r.status_code == 429:
                    Log.warn(f"Central Watch image: 429 rate limited for {camera_id}")
                    return False
                elif r.status_code == 404:
                    continue  # Try next URL
            except Exception as e:
                if DEV_MODE:
                    Log.warn(f"Central Watch image: requests fallback error for {camera_id}: {e}")
        
        return False
    except Exception as e:
        Log.error(f"Central Watch image fetch error for {camera_id}: {e}")
        return False

# Timing constants for the two-phase image refresh strategy
_CW_DOM_CYCLE_DELAY = 60          # Seconds between DOM bulk cycles (was 20; CW rate-limits fast cycles)
_CW_DOM_RETRY_DELAY = 60          # Seconds before retrying DOM after failure
_CW_DOM_SUCCESS_THRESHOLD = 0.3   # Min success rate to stay in DOM (was 0.4)
_CW_DOM_MIN_BATCH_DEGRADATION = 5  # Don't switch to drip on small batches (0/2 = inconclusive)
_CW_DRIP_SUCCESS_DELAY = 12       # Seconds between successful drip-feed fetches
_CW_DRIP_429_BASE = 45            # Initial backoff after first 429
_CW_DRIP_429_MAX = 180            # Maximum backoff (3 minutes)
_CW_DRIP_429_MULTIPLIER = 1.5     # Backoff multiplier per consecutive 429
_CW_ALL_FRESH_DELAY = 30          # Wait when all images are cached and fresh

def _continuous_cw_image_worker():
    """Continuously refreshes Central Watch camera images using a two-phase strategy.
    
    Phase 1 — DOM bulk load (primary):
      Loads ALL stale images at once via <img> DOM elements. This sends requests
      with Sec-Fetch-Dest: image (browser resource load profile), which most WAFs
      and rate limiters treat differently from fetch() API calls. The CW website
      itself loads images this way, so this profile can't be rate-limited without
      breaking the site. All 27 images can load in ~5-10 seconds.
    
    Phase 2 — Drip-feed fallback:
      If DOM loading fails (success rate < 40%), falls back to single-image
      fetch() requests with exponential backoff on 429. This is slower but
      gives us HTTP status codes for proper backoff logic.
    
    The worker periodically retries DOM loading even after falling back, in case
    rate limits reset. For 404 cameras, retries with API-provided timestamps."""
    
    # Wait for browser to be ready
    for _ in range(60):
        if _centralwatch_browser_ready or _shutdown_event.is_set():
            break
        time.sleep(1)
    
    if not _centralwatch_browser_ready:
        Log.warn("Central Watch images: Browser not ready, falling back to sequential prefetch")
        _prefetch_centralwatch_images_sequential()
        return
    
    # Wait a few seconds for data refresh to complete (browser just became ready)
    for _ in range(15):
        if _centralwatch_static_data or _shutdown_event.is_set():
            break
        time.sleep(1)
    
    # Additional small delay to let browser cookies settle
    time.sleep(3)
    
    if DEV_MODE:
        Log.info("Central Watch images: Starting two-phase refresh")
    
    # Track cameras that persistently fail with 404 to try API timestamps
    _404_cameras = set()
    
    # Strategy state
    use_dom = True            # Start optimistically with DOM approach
    dom_tested = False        # Have we tested DOM yet?
    last_dom_retry = 0        # Last time we retried DOM after falling back
    
    # Drip-feed backoff state (used when DOM fails)
    drip_delay = _CW_DRIP_SUCCESS_DELAY
    consecutive_429s = 0
    last_summary_time = time.time()
    
    from datetime import datetime as dt_cls, timezone as tz_cls, timedelta as td_cls
    
    def _build_image_url(cid):
        """Build the image URL for a camera. CW API requires timestamp in path; try (now-2min)
        first, API timestamp for 404 cameras."""
        timestamp = (dt_cls.now(tz_cls.utc) - td_cls(minutes=2)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        if cid in _404_cameras and _centralwatch_camera_timestamps.get(cid):
            api_ts = _centralwatch_camera_timestamps[cid]
            if '+' in api_ts:
                api_ts = api_ts.split('+')[0]
                if '.' not in api_ts:
                    api_ts += '.000'
                api_ts += 'Z'
            return f"https://centralwatch.watchtowers.io/au/api/cameras/{cid}/image/{api_ts}"
        return f"https://centralwatch.watchtowers.io/au/api/cameras/{cid}/image/{timestamp}"
    
    def _cache_result(r):
        """Cache a successful image result. Returns True if cached."""
        try:
            cid = r.get('id')
            data_url = r.get('data', '')
            if not cid or not data_url or ',' not in data_url:
                return False
            _, b64_data = data_url.split(',', 1)
            image_bytes = base64.b64decode(b64_data)
            if len(image_bytes) > 500:
                _centralwatch_image_cache[cid] = {
                    'data': image_bytes,
                    'content_type': r.get('contentType', 'image/jpeg'),
                    'timestamp': time.time()
                }
                _404_cameras.discard(cid)
                return True
        except Exception:
            pass
        return False
    
    def _get_stale_ids(camera_ids):
        """Get list of (priority, camera_id) for stale/uncached images, sorted by priority."""
        now = time.time()
        stale = []
        for cid in camera_ids:
            cached = _centralwatch_image_cache.get(cid)
            if not cached:
                stale.append((0, cid))
            elif (now - cached['timestamp']) > _CENTRALWATCH_PREFETCH_STALE_TTL:
                stale.append((cached['timestamp'], cid))
        stale.sort(key=lambda x: x[0])  # Uncached first, then oldest
        return stale
    
    while not _shutdown_event.is_set():
        try:
            camera_ids = [cam.get('id') for cam in _centralwatch_static_data if cam.get('id')]
            if not camera_ids:
                _shutdown_event.wait(timeout=10)
                continue
            
            now = time.time()
            stale_ids = _get_stale_ids(camera_ids)
            
            if not stale_ids:
                cached_total = len(_centralwatch_image_cache)
                if now - last_summary_time >= 120:
                    if DEV_MODE:
                        Log.info(f"Central Watch images: All {cached_total}/{len(camera_ids)} cached and fresh")
                    last_summary_time = now
                _shutdown_event.wait(timeout=_CW_ALL_FRESH_DELAY)
                continue
            
            # ===================================================================
            # PHASE 1: DOM BULK LOAD — try loading ALL stale images at once
            # ===================================================================
            # <img> tags send Sec-Fetch-Dest: image which bypasses API rate limits
            # Periodically retry DOM even after falling back (every 5 min)
            should_try_dom = use_dom or (not dom_tested) or (now - last_dom_retry > 300)
            
            if should_try_dom and len(stale_ids) > 0:
                # Build image list for ALL stale cameras
                image_list = [[cid, _build_image_url(cid)] for _, cid in stale_ids]
                
                dom_results = _browser_dom_batch_fetch_images(image_list, timeout=45)
                
                dom_successes = 0
                dom_failures = 0
                for r in (dom_results or []):
                    if r and r.get('ok') and r.get('data'):
                        if _cache_result(r):
                            dom_successes += 1
                        else:
                            dom_failures += 1
                    else:
                        dom_failures += 1
                
                total_attempted = len(image_list)
                cached_total = len(_centralwatch_image_cache)
                success_rate = dom_successes / total_attempted if total_attempted > 0 else 0
                
                if not dom_tested:
                    # First test — log the result prominently
                    if success_rate >= _CW_DOM_SUCCESS_THRESHOLD:
                        if DEV_MODE:
                            Log.info(f"Central Watch images: DOM OK {dom_successes}/{total_attempted} ({cached_total}/{len(camera_ids)} cached)")
                        use_dom = True
                    elif total_attempted >= _CW_DOM_MIN_BATCH_DEGRADATION:
                        if DEV_MODE:
                            Log.info(f"Central Watch images: DOM poor ({dom_successes}/{total_attempted}), drip-feed")
                        use_dom = False
                    else:
                        if DEV_MODE:
                            Log.info(f"Central Watch images: DOM inconclusive ({dom_successes}/{total_attempted})")
                        use_dom = True  # Stay optimistic, small batch isn't conclusive
                    dom_tested = True
                    last_dom_retry = now
                else:
                    if success_rate >= _CW_DOM_SUCCESS_THRESHOLD:
                        if not use_dom:
                            if DEV_MODE:
                                Log.info(f"Central Watch images: DOM recovered {dom_successes}/{total_attempted} ({cached_total}/{len(camera_ids)} cached)")
                        else:
                            if DEV_MODE:
                                Log.info(f"Central Watch images: DOM refreshed {dom_successes}/{total_attempted} ({cached_total}/{len(camera_ids)} cached)")
                        use_dom = True
                        consecutive_429s = 0
                        drip_delay = _CW_DRIP_SUCCESS_DELAY
                    else:
                        # Only switch to drip when batch is large enough to be conclusive
                        if total_attempted >= _CW_DOM_MIN_BATCH_DEGRADATION:
                            if use_dom:
                                if DEV_MODE:
                                    Log.info(f"Central Watch images: DOM degraded ({dom_successes}/{total_attempted}), drip-feed")
                            use_dom = False
                        elif use_dom and dom_successes < total_attempted:
                            if DEV_MODE:
                                Log.info(f"Central Watch images: DOM poor ({dom_successes}/{total_attempted}), staying in DOM")
                    last_dom_retry = now
                
                if use_dom and (dom_successes >= total_attempted or total_attempted == 0):
                    # DOM loaded everything — wait and cycle again
                    _shutdown_event.wait(timeout=_CW_DOM_CYCLE_DELAY)
                    continue
                # Else: still have unloaded cameras — fall through to drip (e.g. small batch 0/2)
                
                # DOM failed or partial — fall through to drip-feed but don't re-fetch what we just got
                if dom_successes > 0:
                    # Recompute stale list since some may have been cached
                    stale_ids = _get_stale_ids(camera_ids)
                    if not stale_ids:
                        _shutdown_event.wait(timeout=_CW_ALL_FRESH_DELAY)
                        continue
            
            # ===================================================================
            # PHASE 2: DRIP-FEED FALLBACK — single image with adaptive backoff
            # ===================================================================
            _, cid = stale_ids[0]
            img_url = _build_image_url(cid)
            
            results = _browser_batch_fetch_images([[cid, img_url]], timeout=20)
            r = results[0] if results else None
            
            cached_total = len(_centralwatch_image_cache)
            
            if r and r.get('ok') and r.get('data'):
                if _cache_result(r):
                    cached_total = len(_centralwatch_image_cache)
                    # Recovery: this cid succeeded, so clear any stuck
                    # 'Failed' entries for it so the next failure is logged.
                    for k in [k for k in _failed_cameras_logged if k[0] == cid]:
                        _failed_cameras_logged.pop(k, None)
                    if consecutive_429s > 0:
                        if DEV_MODE:
                            Log.info(f"Central Watch images: Rate limit cleared after {consecutive_429s} 429s")
                    elif DEV_MODE:
                        Log.info(f"Central Watch images: {cid[:8]}.. ({cached_total}/{len(camera_ids)} cached)")
                    consecutive_429s = 0
                    drip_delay = _CW_DRIP_SUCCESS_DELAY
            
            elif r and r.get('status') == 429:
                consecutive_429s += 1
                retry_after = r.get('retryAfter')
                if retry_after and retry_after > 0:
                    drip_delay = min(retry_after + 5, _CW_DRIP_429_MAX)
                elif consecutive_429s == 1:
                    drip_delay = _CW_DRIP_429_BASE
                else:
                    drip_delay = min(drip_delay * _CW_DRIP_429_MULTIPLIER, _CW_DRIP_429_MAX)
                if consecutive_429s <= 1 or consecutive_429s % 5 == 0:
                    if DEV_MODE:
                        Log.info(f"Central Watch images: 429 #{consecutive_429s} — waiting {drip_delay:.0f}s ({cached_total}/{len(camera_ids)} cached, {len(stale_ids)} stale)")
            
            elif r and r.get('status') == 404:
                was_in_404 = cid in _404_cameras
                _404_cameras.add(cid)
                # Retry with alternate: if first was API ts use (now-2min), else use API ts
                alt_ts = (dt_cls.now(tz_cls.utc) - td_cls(minutes=2)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
                alt_url = None
                if was_in_404:
                    alt_url = f"https://centralwatch.watchtowers.io/au/api/cameras/{cid}/image/{alt_ts}"
                else:
                    api_ts = _centralwatch_camera_timestamps.get(cid)
                    if api_ts:
                        ts = api_ts.split('+')[0] if '+' in api_ts else api_ts
                        if '.' not in ts:
                            ts += '.000'
                        if not ts.endswith('Z'):
                            ts += 'Z'
                        alt_url = f"https://centralwatch.watchtowers.io/au/api/cameras/{cid}/image/{ts}"
                r2 = None
                if alt_url:
                    r2_list = _browser_batch_fetch_images([[cid, alt_url]], timeout=20)
                    r2 = r2_list[0] if r2_list else None
                if r2 and r2.get('ok') and r2.get('data'):
                    if _cache_result(r2):
                        cached_total = len(_centralwatch_image_cache)
                        _404_cameras.discard(cid)
                        if DEV_MODE:
                            Log.info(f"Central Watch images: 404→OK {cid[:8]}.. (alternate timestamp)")
                elif len(_404_cameras) >= 3 and (now - _last_timestamp_refresh_attempt) > 180:
                    # Trigger API refresh to get fresh timestamps (browser may bypass 429)
                    threading.Thread(target=lambda: _refresh_centralwatch_timestamps(force=True), daemon=True).start()
                if DEV_MODE and cid in _404_cameras:
                    Log.info(f"Central Watch images: 404 for {cid[:8]}..")
                drip_delay = _CW_DRIP_SUCCESS_DELAY

            else:
                status = r.get('status', '?') if r else '?'
                # Only log on state transition: the same camera retrying
                # every 30s with the same 403 was producing dozens of
                # identical lines per minute. _failed_cameras_logged
                # (module-level dict) tracks (cid, status) tuples we've
                # already complained about; a successful fetch above
                # clears the entry on recovery so the next failure logs.
                key = (cid, str(status))
                if DEV_MODE and key not in _failed_cameras_logged:
                    Log.info(f"Central Watch images: Failed {cid[:8]}.. (HTTP {status})")
                    _failed_cameras_logged[key] = True
                drip_delay = _CW_DRIP_SUCCESS_DELAY
            
            # Periodic summary
            if now - last_summary_time >= 120:
                mode = "DOM" if use_dom else "drip"
                if DEV_MODE:
                    Log.info(f"Central Watch images [{mode}]: {cached_total}/{len(camera_ids)} cached, {len(stale_ids)} stale, delay={drip_delay:.0f}s, 429s={consecutive_429s}")
                last_summary_time = now
            
        except Exception as e:
            Log.error(f"Central Watch image refresh error: {e}")
            drip_delay = _CW_DRIP_429_BASE
        
        # Wait before next attempt (adaptive for drip-feed)
        _shutdown_event.wait(timeout=drip_delay)

def _prefetch_centralwatch_images_sequential():
    """Fallback: sequential pre-fetch when browser batch is not available.
    Uses adaptive delays to stay under the CW rate limit.
    Note: works with or without browser - _fetch_centralwatch_image handles both."""
    with _centralwatch_image_fetch_lock:
        camera_ids = [cam.get('id') for cam in _centralwatch_static_data if cam.get('id')]
        now = time.time()
        to_fetch = [cid for cid in camera_ids
                    if not _centralwatch_image_cache.get(cid)
                    or (now - _centralwatch_image_cache[cid]['timestamp']) > _CENTRALWATCH_PREFETCH_STALE_TTL]
        
        if not to_fetch:
            return
        
        Log.info(f"Central Watch images: Sequential pre-fetching {len(to_fetch)} of {len(camera_ids)} images")
        fetched = 0
        for cid in to_fetch:
            if _shutdown_event.is_set():
                break
            if _fetch_centralwatch_image(cid):
                fetched += 1
                time.sleep(3)
            else:
                time.sleep(10)
        Log.info(f"Central Watch images: Sequential pre-fetched {fetched}/{len(to_fetch)} ({len(_centralwatch_image_cache)} cached)")

@app.route('/api/centralwatch/image/<camera_id>')
def centralwatch_image_proxy(camera_id):
    """Proxy Central Watch camera images — CACHE-ONLY.
    
    This endpoint ONLY serves from the in-memory cache. It NEVER triggers
    fetches to Central Watch. The background batch worker (_continuous_cw_image_worker)
    is the sole system responsible for fetching and refreshing images.
    This prevents on-demand requests from competing with the batch worker
    for the CW rate limit budget."""
    from flask import Response
    
    cached = _centralwatch_image_cache.get(camera_id)
    
    if cached:
        age = time.time() - cached['timestamp']
        cache_status = 'HIT' if age <= _CENTRALWATCH_PREFETCH_STALE_TTL else 'STALE'
        return Response(
            cached['data'],
            mimetype=cached['content_type'],
            headers={
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0',
                'Access-Control-Allow-Origin': '*',
                'X-Cache': cache_status,
                'X-Cache-Age': str(int(age))
            }
        )
    
    # No cached image — return placeholder (batch worker will populate cache soon)
    placeholder_svg = '''<svg xmlns="http://www.w3.org/2000/svg" width="640" height="480" viewBox="0 0 640 480">
      <rect width="640" height="480" fill="#1e293b"/>
      <text x="320" y="220" text-anchor="middle" fill="#94a3b8" font-family="sans-serif" font-size="18">🔥 Fire Watch Camera</text>
      <text x="320" y="260" text-anchor="middle" fill="#64748b" font-family="sans-serif" font-size="14">Image loading... please wait</text>
    </svg>'''
    return Response(
        placeholder_svg,
        mimetype='image/svg+xml',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
            'X-Cache': 'PLACEHOLDER'
        }
    )


@app.route('/api/centralwatch/sites')
def centralwatch_sites():
    """Get Central Watch cameras grouped by site for multi-camera view"""
    # Get all cameras
    cameras = _centralwatch_static_data
    
    # Check if we have fresher data in cache
    cached_data, age, expired = cache_get('centralwatch_cameras')
    if cached_data and len(cached_data) > 0:
        cameras = cached_data
    elif _centralwatch_last_fetch['data'] and len(_centralwatch_last_fetch['data']) > 0:
        cameras = _centralwatch_last_fetch['data']
    
    # Group cameras by siteId
    sites = {}
    for cam in cameras:
        site_id = cam.get('siteId', cam.get('id'))  # Use camera id if no siteId
        if site_id not in sites:
            sites[site_id] = {
                'siteId': site_id,
                'siteName': cam.get('siteName', 'Unknown Site'),
                'latitude': cam.get('latitude'),
                'longitude': cam.get('longitude'),
                'altitude': cam.get('altitude'),
                'state': cam.get('state'),
                'cameras': []
            }
        sites[site_id]['cameras'].append({
            'id': cam.get('id'),
            'name': cam.get('name'),
            'imageUrl': cam.get('imageUrl'),
        })
    
    return jsonify(list(sites.values()))


@app.route('/api/traffic/all-feeds')
@cached(ttl=CACHE_TTL_TRAFFIC)
def traffic_all_feeds():
    """All Live Traffic data from all-feeds-web.json - raw data"""
    try:
        r = requests.get(
            'https://www.livetraffic.com/datajson/all-feeds-web.json',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                # Group by event type for easier consumption
                grouped = {}
                for item in data:
                    event_type = item.get('eventType', item.get('eventCategory', 'unknown'))
                    if event_type not in grouped:
                        grouped[event_type] = []
                    grouped[event_type].append(item)
                
                return jsonify({
                    'raw': data,
                    'grouped': grouped,
                    'eventTypes': list(grouped.keys()),
                    'totalCount': len(data)
                })
        return jsonify({'raw': [], 'grouped': {}, 'eventTypes': [], 'totalCount': 0})
    except Exception as e:
        Log.error(f"All feeds error: {e}")
        return jsonify({'error': str(e), 'raw': [], 'grouped': {}, 'totalCount': 0}), 200


# ============================================================================
# WAZE API ENDPOINTS
# ============================================================================

# Waze alert type mappings for categorization
# Type values from Waze API: ROAD_CLOSED, HAZARD, POLICE, JAM, CONSTRUCTION, ACCIDENT
# Subtypes provide more detail, e.g. HAZARD_ON_ROAD_POT_HOLE, POLICE_HIDING, etc.

# Traffic Hazards - includes road closures (except construction), accidents, jams, hazards
WAZE_HAZARD_TYPES = {
    'HAZARD': True,
    'ACCIDENT': True,
    'JAM': True,
    'ROAD_CLOSED': True,  # Road closures go to hazards (except construction-related)
}

WAZE_HAZARD_SUBTYPES = {
    # Road hazards
    'HAZARD_ON_ROAD': True,
    'HAZARD_ON_ROAD_POT_HOLE': True,
    'HAZARD_ON_ROAD_OBJECT': True,
    'HAZARD_ON_ROAD_CAR_STOPPED': True,
    'HAZARD_ON_ROAD_LANE_CLOSED': True,
    'HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT': True,
    'HAZARD_ON_ROAD_CONSTRUCTION': False,  # Goes to roadwork
    'HAZARD_ON_SHOULDER': True,
    'HAZARD_ON_SHOULDER_CAR_STOPPED': True,
    'HAZARD_ON_SHOULDER_ANIMALS': True,
    'HAZARD_ON_SHOULDER_MISSING_SIGN': True,
    'HAZARD_WEATHER': True,
    'HAZARD_WEATHER_FOG': True,
    'HAZARD_WEATHER_HAIL': True,
    'HAZARD_WEATHER_HEAVY_RAIN': True,
    'HAZARD_WEATHER_FLOOD': True,
    'HAZARD_WEATHER_MONSOON': True,
    'HAZARD_WEATHER_TORNADO': True,
    'HAZARD_WEATHER_HEAT_WAVE': True,
    'HAZARD_WEATHER_HURRICANE': True,
    'HAZARD_WEATHER_FREEZING_RAIN': True,
    'HAZARD_ON_ROAD_ICE': True,
    'HAZARD_ON_ROAD_OIL': True,
    # Road closures (non-construction)
    'ROAD_CLOSED_EVENT': True,
    'ROAD_CLOSED_HAZARD': True,
}

# Roadwork - construction-related alerts
WAZE_ROADWORK_TYPES = {'CONSTRUCTION'}
WAZE_ROADWORK_SUBTYPES = {
    'HAZARD_ON_ROAD_CONSTRUCTION': True,
    'ROAD_CLOSED_CONSTRUCTION': True,
    'CONSTRUCTION': True,
    'CONSTRUCTION_MINOR': True,
    'CONSTRUCTION_MAJOR': True,
}

WAZE_POLICE_TYPES = {'POLICE'}
WAZE_POLICE_SUBTYPES = {
    'POLICE_VISIBLE': True,
    'POLICE_HIDING': True,
    'POLICE_WITH_MOBILE_CAMERA': True,  # Mobile speed camera
}

# NSW regions for Waze requests. Waze's georss API caps at ~200 alerts/bbox
# so dense areas need tighter boxes; rural NSW gets consolidated into one wide
# box because alert density is ~0. Reduced from 16 → 6 regions to cut proxy
# bandwidth by ~60% (each region = one full page reload in interception mode).
NSW_REGIONS = [
    # Sydney metro (highest alert density — keep tight)
    {'name': 'Sydney Metro', 'top': -33.65, 'bottom': -34.25, 'left': 150.6, 'right': 151.55},

    # Central Coast + Newcastle + Hunter
    {'name': 'Hunter', 'top': -32.4, 'bottom': -33.65, 'left': 150.8, 'right': 152.2},

    # Illawarra + Shoalhaven (Wollongong south to Jervis Bay)
    {'name': 'Illawarra', 'top': -34.15, 'bottom': -35.4, 'left': 150.15, 'right': 151.15},

    # Blue Mountains + Central West (Katoomba to Dubbo/Orange/Bathurst)
    {'name': 'Central West', 'top': -31.5, 'bottom': -34.1, 'left': 147.5, 'right': 150.85},

    # Northern NSW (coast + inland: Port Macquarie, Tamworth, Armidale, Tweed)
    {'name': 'Northern NSW', 'top': -28.15, 'bottom': -32.0, 'left': 149.5, 'right': 153.65},

    # Southern NSW + ACT + South Coast (Canberra region to Eden, plus Riverina)
    {'name': 'Southern NSW', 'top': -34.0, 'bottom': -37.1, 'left': 143.5, 'right': 150.5},
]

# --- Userscript ingest mode --------------------------------------------------
# When a Tampermonkey userscript running in a real browser POSTs scraped
# georss data to /api/waze/ingest, we serve that data instead of trying the
# blocked direct fetch. Setup: see docs/waze-userscript.md
WAZE_INGEST_ENABLED = os.environ.get('WAZE_INGEST_ENABLED', 'false').lower() in ('1', 'true', 'yes')
WAZE_INGEST_KEY = os.environ.get('WAZE_INGEST_KEY', '').strip()
# Cached ingest per-bbox is evicted after this many seconds. The current
# userscript rotation is ~5s × 190 regions ≈ 16 min. The userscript also
# reloads itself every 30 min (absolute backstop) which causes a 30-60s
# gap; if a region was just visited before that reload, the next visit
# can be up to ~16 min later. Worst-case gap between visits is therefore
# rotation + reload window ≈ 17-18 min. 40 min gives ~2× headroom so
# brief glitches don't prune entries that the script is about to refresh.
# Tune down with WAZE_INGEST_MAX_AGE if memory pressure matters more.
WAZE_INGEST_MAX_AGE = int(os.environ.get('WAZE_INGEST_MAX_AGE', 2400))
# How often we snapshot _waze_ingest_cache to api_data_cache so a backend
# restart doesn't lose all the bbox data the userscript spent ~16 min
# rotating to collect. 0 disables the feature.
WAZE_INGEST_PERSIST_INTERVAL = int(os.environ.get('WAZE_INGEST_PERSIST_INTERVAL', 90))
_WAZE_INGEST_PERSIST_KEY = '_waze_ingest_snapshot_v1'
# Alert if no ingest POST has arrived in this many seconds (0 = disabled).
WAZE_INGEST_STALE_SECS = int(os.environ.get('WAZE_INGEST_STALE_SECS', '600'))
# Optional Discord webhook — fired once when staleness first crosses threshold.
WAZE_STALE_WEBHOOK = os.environ.get('WAZE_STALE_WEBHOOK', '').strip()
# keyed by (top,bottom,left,right) rounded tuple → {'alerts':[...], 'jams':[...], 'users':[...], 'ts': float}
_waze_ingest_cache = {}
_waze_ingest_lock = threading.Lock()
# Tracks wall-clock of the most recent ingest POST (any bbox). Used by the
# staleness watcher thread so we don't have to walk _waze_ingest_cache on every
# tick. 0.0 means "never".
_waze_last_ingest_at = 0.0
# Latches so the watcher only logs/pings once per stale→healthy cycle.
_waze_stale_alerted = False
# Mirror of the most recent ingest snapshot — read by /api/waze/metrics so it
# can report freshness alongside the rolling block-rate window.
_waze_cache = {'alerts': [], 'jams': [], 'timestamp': 0}

# Rolling block-rate monitor. Each fetch appends (timestamp_unix, outcome)
# where outcome is 'success' | 'block' | 'error'. Entries older than
# _WAZE_METRICS_WINDOW seconds are pruned on access. If the current block
# rate exceeds _WAZE_BLOCK_RATE_LIMIT, new fetches are gated — we serve
# stale cache (if any) instead of hammering upstream.
from collections import deque as _waze_deque
_WAZE_METRICS_WINDOW = 1800  # 30-minute rolling window
_WAZE_METRICS_MAX = 500       # hard cap on stored outcomes
_WAZE_BLOCK_RATE_LIMIT = 2.0  # percent — if >= this, gate new fetches
_WAZE_METRICS_MIN_SAMPLES = 20  # need at least this many samples before gating
_waze_metrics = _waze_deque(maxlen=_WAZE_METRICS_MAX)
_waze_metrics_lock = threading.Lock()


def _waze_metrics_record(outcome: str):
    """Append an outcome ('success' | 'block' | 'error') to the rolling window."""
    with _waze_metrics_lock:
        _waze_metrics.append((time.time(), outcome))


def _waze_metrics_snapshot(window_seconds: int = _WAZE_METRICS_WINDOW):
    """Return a dict summarising the current rolling window."""
    cutoff = time.time() - window_seconds
    with _waze_metrics_lock:
        # Prune old entries from the left
        while _waze_metrics and _waze_metrics[0][0] < cutoff:
            _waze_metrics.popleft()
        total = len(_waze_metrics)
        success = sum(1 for _, o in _waze_metrics if o == 'success')
        block = sum(1 for _, o in _waze_metrics if o == 'block')
        error = sum(1 for _, o in _waze_metrics if o == 'error')
        last_success = max((ts for ts, o in _waze_metrics if o == 'success'), default=0)
        last_block = max((ts for ts, o in _waze_metrics if o == 'block'), default=0)
    rate = (block / total * 100.0) if total else 0.0
    return {
        'total': total,
        'success': success,
        'block': block,
        'error': error,
        'block_rate_percent': round(rate, 2),
        'window_seconds': window_seconds,
        'last_success_at': last_success or None,
        'last_block_at': last_block or None,
        'gate_threshold_percent': _WAZE_BLOCK_RATE_LIMIT,
        'gate_active': (total >= _WAZE_METRICS_MIN_SAMPLES and rate >= _WAZE_BLOCK_RATE_LIMIT),
    }


def fetch_waze_data():
    """Fetch Waze alerts and jams for all NSW regions from the userscript ingest cache.

    Waze data is now exclusively delivered by a Violentmonkey userscript that
    POSTs georss payloads to /api/waze/ingest. If WAZE_INGEST_ENABLED is false,
    Waze is treated as disabled and we return empty lists.
    Result is shared across waze_hazards/waze_police/waze_roadwork callers."""
    global _waze_cache

    if not WAZE_INGEST_ENABLED:
        return [], []

    alerts, jams = _waze_ingest_snapshot()
    # Cache for the metrics endpoint so it can report freshness.
    _waze_cache = {'alerts': alerts, 'jams': jams, 'timestamp': time.time()}
    if alerts or jams:
        source_ok('waze')
    return alerts, jams


def fetch_waze_alerts():
    """Fetch Waze alerts for NSW region (legacy function for compatibility)"""
    alerts, _ = fetch_waze_data()
    return alerts


def parse_waze_alert(alert, category):
    """Parse a Waze alert into GeoJSON feature format"""
    location = alert.get('location', {})
    # Waze georss API uses location.x (lon) and location.y (lat); fallback to lat/lon keys
    lat = location.get('y') or location.get('latitude') or alert.get('lat')
    lon = location.get('x') or location.get('longitude') or alert.get('lon')
    
    if lat is None or lon is None:
        return None
    
    alert_type = alert.get('type', '')
    subtype = alert.get('subtype', '')
    street = alert.get('street', '')
    city = alert.get('city', '')
    description = alert.get('reportDescription', '')
    thumbs_up = alert.get('nThumbsUp', 0)
    reliability = alert.get('reliability', 0)
    pub_millis = alert.get('pubMillis', 0)
    
    # Format timestamp
    created = ''
    if pub_millis:
        try:
            created = datetime.fromtimestamp(pub_millis / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        except (ValueError, TypeError, OSError):
            pass
    
    # Build readable type string
    display_type = subtype.replace('_', ' ').title() if subtype else alert_type.replace('_', ' ').title()
    
    # Build location string
    location_str = ', '.join(filter(None, [street, city]))
    
    return {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [lon, lat]
        },
        'properties': {
            'id': alert.get('uuid', alert.get('id', '')),
            'type': category,
            'wazeType': alert_type,
            'wazeSubtype': subtype,
            'title': description if description else display_type,
            'displayType': display_type,
            'street': street,
            'city': city,
            'location': location_str,
            'thumbsUp': thumbs_up,
            'reliability': reliability,
            'created': created,
            'reportBy': alert.get('reportBy', ''),
            'source': 'waze'
        }
    }


def parse_waze_jam(jam):
    """Parse a Waze jam (traffic) into GeoJSON feature format"""
    # Jams have a 'line' property with coordinates
    line = jam.get('line', [])
    if not line or len(line) < 2:
        return None
    
    # Get jam properties
    street = jam.get('street', '')
    city = jam.get('city', '')
    speed = jam.get('speed', 0)  # Current speed in km/h
    speed_kmh = jam.get('speedKMH', speed)
    length = jam.get('length', 0)  # Length in meters
    delay = jam.get('delay', 0)  # Delay in seconds
    level = jam.get('level', 0)  # Severity 0-5
    pub_millis = jam.get('pubMillis', 0)
    
    # Convert line to coordinates [lon, lat]
    coordinates = [[point.get('x'), point.get('y')] for point in line if point.get('x') is not None and point.get('y') is not None]
    if len(coordinates) < 2:
        return None
    
    # Format timestamp
    created = ''
    if pub_millis:
        try:
            created = datetime.fromtimestamp(pub_millis / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        except (ValueError, TypeError, OSError):
            pass
    
    # Severity description
    severity_map = {
        0: 'Free Flow',
        1: 'Light Traffic',
        2: 'Moderate Traffic',
        3: 'Heavy Traffic',
        4: 'Standstill',
        5: 'Blocked'
    }
    severity = severity_map.get(level, f'Level {level}')
    
    # Build location string
    location_str = ', '.join(filter(None, [street, city]))
    
    # Calculate delay in minutes
    delay_mins = round(delay / 60) if delay else 0
    
    return {
        'type': 'Feature',
        'geometry': {
            'type': 'LineString',
            'coordinates': coordinates
        },
        'properties': {
            'id': jam.get('uuid', jam.get('id', '')),
            'type': 'Jam',
            'wazeType': 'JAM',
            'wazeSubtype': '',
            'title': f'{severity} on {street}' if street else severity,
            'displayType': severity,
            'street': street,
            'city': city,
            'location': location_str,
            'speed': speed_kmh,
            'length': length,
            'lengthKm': round(length / 1000, 2) if length else 0,
            'delay': delay,
            'delayMins': delay_mins,
            'level': level,
            'severity': severity,
            'created': created,
            'source': 'waze'
        }
    }


# Reconcile queue — one worker thread pulls bbox-diff tasks off this and
# processes them one at a time. Ingest endpoints enqueue and return immediately;
# if the queue fills up, new tasks get dropped (the 1-hour last_seen sweep is
# the fallback). This stops the userscript's 5s cadence from stacking up on the
# waze DB lock and starving the pool.
import queue as _queue_module
# Bigger buffer + per-bbox coalescing so the userscript revisiting the same
# region back-to-back doesn't pile multiple reconciles on top of each other.
# We queue bbox_keys; the actual payload (bbox + current_ids) is held in
# _waze_reconcile_pending and only the latest version is processed.
_waze_reconcile_queue = _queue_module.Queue(maxsize=256)
_waze_reconcile_pending = {}     # bbox_key -> (bbox_raw, current_ids)
_waze_reconcile_pending_lock = threading.Lock()
_waze_reconcile_thread_started = False
_waze_reconcile_thread_lock = threading.Lock()


def _waze_reconcile_worker():
    while not _shutdown_event.is_set():
        try:
            bbox_key = _waze_reconcile_queue.get(timeout=1)
        except Exception:
            continue
        if bbox_key is None:
            continue
        # Pop the latest snapshot for this bbox. If the userscript revisited
        # in the interim, we run on the freshest data.
        with _waze_reconcile_pending_lock:
            payload = _waze_reconcile_pending.pop(bbox_key, None)
        try:
            if payload is not None:
                _waze_reconcile_bbox_sync(*payload)
        except Exception as e:
            Log.error(f"Waze reconcile worker error: {e}")
        finally:
            try:
                _waze_reconcile_queue.task_done()
            except Exception:
                pass


def _ensure_reconcile_worker():
    global _waze_reconcile_thread_started
    with _waze_reconcile_thread_lock:
        if _waze_reconcile_thread_started:
            return
        _waze_reconcile_thread_started = True
        threading.Thread(
            target=_waze_reconcile_worker,
            daemon=True,
            name='waze-reconcile-worker',
        ).start()


def _waze_reconcile_bbox(bbox_raw, alerts, jams):
    """Enqueue a per-region 'gone' reconcile. Non-blocking; coalesces by
    bbox key so revisits don't pile up — the worker always processes the
    freshest payload for each region."""
    _ensure_reconcile_worker()
    # Extract just the fields the sync worker needs — don't hold references
    # to the full payload alerts[] on the queue.
    current_ids = set()
    for a in alerts or []:
        uid = a.get('uuid') or a.get('id')
        if uid is not None:
            current_ids.add(str(uid))
    for j in jams or []:
        uid = j.get('uuid') or j.get('id')
        if uid is not None:
            current_ids.add(str(uid))
    try:
        bbox_key = (
            round(float(bbox_raw.get('top', 0)),    3),
            round(float(bbox_raw.get('bottom', 0)), 3),
            round(float(bbox_raw.get('left', 0)),   3),
            round(float(bbox_raw.get('right', 0)),  3),
        )
    except (TypeError, ValueError):
        return
    with _waze_reconcile_pending_lock:
        already_queued = bbox_key in _waze_reconcile_pending
        # Always store the latest snapshot — the worker pops by key.
        _waze_reconcile_pending[bbox_key] = (dict(bbox_raw), current_ids)
    if already_queued:
        # Worker hasn't picked up the previous task yet; it'll see our newer
        # data when it does. No need to push another queue entry.
        return
    try:
        _waze_reconcile_queue.put_nowait(bbox_key)
    except _queue_module.Full:
        # Backlog. Drop both the queue slot AND the pending payload so we
        # don't leak memory waiting for a worker that won't get to it.
        with _waze_reconcile_pending_lock:
            _waze_reconcile_pending.pop(bbox_key, None)
        if DEV_MODE:
            Log.warn("Waze reconcile queue full — dropping task (fallback: 1h sweep)")


def _waze_reconcile_bbox_sync(bbox_raw, current_ids):
    """Immediate per-region 'gone' detection.

    When the userscript POSTs an ingest for a bbox, any Waze record in
    data_history whose lat/lon falls inside that bbox AND whose source_id
    isn't in this payload is considered cleared — we flip is_live=0 right
    away. The 1-hour last_seen sweep in cleanup_old_data() is the fallback
    for regions that stop being visited entirely (userscript crash).
    """
    try:
        top = float(bbox_raw.get('top', 0))
        bottom = float(bbox_raw.get('bottom', 0))
        left = float(bbox_raw.get('left', 0))
        right = float(bbox_raw.get('right', 0))
    except (TypeError, ValueError):
        return 0
    lat_hi = max(top, bottom)
    lat_lo = min(top, bottom)
    lon_hi = max(left, right)
    lon_lo = min(left, right)
    if lat_hi == lat_lo or lon_hi == lon_lo:
        return 0

    waze_sources = ('waze_hazard', 'waze_police', 'waze_roadwork', 'waze_jam')
    # Note: the master lock previously wrapped this block. Archive writes
    # no longer hold it (commit 29595ea), and the reconcile worker is
    # already a single-threaded queue consumer (_waze_reconcile_worker),
    # so the lock served no purpose here — it just queued reconciles
    # behind any other code path that did happen to grab it. Postgres
    # row-level locking handles the SELECT/UPDATE concurrency safely.
    #
    # However, the archive writer's batch UPDATEs (last_seen + is_live=1)
    # touch overlapping rows via a different index, so the two transactions
    # can grab row locks in opposite orders and deadlock. Postgres aborts
    # the loser; we retry it. Sorting the source_id lists also gives a
    # stable lock acquisition order for repeat conflicts.
    n_gone = 0
    by_source = {}
    max_attempts = 3
    for attempt in range(max_attempts):
        n_gone = 0
        by_source = {}
        conn = None
        try:
            conn = get_conn()
            try:
                c = conn.cursor()
                # 30s budget — bigger than the old 15s now that we don't
                # serialise behind a Python lock. The query uses the
                # idx_data_waze_live_geo partial index (source, lat, lng
                # WHERE is_live = 1 AND source IN (waze_*)).
                c.execute("SET LOCAL statement_timeout = '30s'")
                # Faster deadlock detection so the loser aborts quickly
                # and we can retry within budget. Default is 1s.
                c.execute("SET LOCAL deadlock_timeout = '200ms'")
                c.execute('''
                    SELECT source, source_id FROM data_history
                    WHERE source IN %s
                    AND is_live = 1
                    AND source_id IS NOT NULL
                    AND latitude BETWEEN %s AND %s
                    AND longitude BETWEEN %s AND %s
                ''', (waze_sources, lat_lo, lat_hi, lon_lo, lon_hi))
                rows = c.fetchall()
                for src, sid in rows:
                    if sid not in current_ids:
                        by_source.setdefault(src, []).append(sid)
                for src, sids in by_source.items():
                    # Sort for deterministic lock acquisition order across
                    # retries — same set of rows, predictable order.
                    sids.sort()
                    placeholders = ','.join(['%s'] * len(sids))
                    # AND is_live = 1 so we only touch the rows that actually
                    # need flipping — historical rows already have is_live = 0
                    # and shouldn't be rewritten. Cuts the UPDATE's scan/lock
                    # surface to the live set.
                    c.execute(
                        f'UPDATE data_history SET is_live = 0 '
                        f'WHERE source = %s AND is_live = 1 '
                        f'AND source_id IN ({placeholders})',
                        [src] + sids,
                    )
                    n_gone += len(sids)
                if n_gone:
                    conn.commit()
            finally:
                conn.close()
            break
        except psycopg2.Error as e:
            # 40P01 = deadlock_detected. Postgres already rolled back the
            # losing transaction; retry on a fresh connection.
            if getattr(e, 'pgcode', None) == '40P01' and attempt < max_attempts - 1:
                time.sleep(0.05 * (attempt + 1))
                continue
            Log.error(f"Waze bbox reconcile DB error: {e}")
            return 0
        except Exception as e:
            Log.error(f"Waze bbox reconcile DB error: {e}")
            return 0
    if n_gone and DEV_MODE:
        breakdown = ', '.join(f'{k}:{len(v)}' for k, v in by_source.items() if v)
        Log.live(f"Waze ✗ bbox reconcile — {n_gone} gone ({breakdown})")
    return n_gone


def _waze_ingest_snapshot():
    """Collect all fresh ingested georss data into a single alerts+jams+users set,
    deduplicated by UUID. Returns (alerts_list, jams_list) like fetch_waze_data."""
    now = time.time()
    all_alerts = {}
    all_jams = {}
    with _waze_ingest_lock:
        # Prune stale entries
        for key in list(_waze_ingest_cache.keys()):
            if now - _waze_ingest_cache[key].get('ts', 0) > WAZE_INGEST_MAX_AGE:
                del _waze_ingest_cache[key]
        # Merge all remaining by UUID
        for entry in _waze_ingest_cache.values():
            for a in entry.get('alerts', []) or []:
                uuid = a.get('uuid') or a.get('id')
                if uuid and uuid not in all_alerts:
                    all_alerts[uuid] = a
            for j in entry.get('jams', []) or []:
                uuid = j.get('uuid') or j.get('id')
                if uuid and uuid not in all_jams:
                    all_jams[uuid] = j
    return list(all_alerts.values()), list(all_jams.values())


def _hydrate_waze_ingest_cache_from_db():
    """Restore _waze_ingest_cache from the persistent api_data_cache row
    written by _waze_ingest_persist_loop. Without this, a restart drops
    every bbox the userscript collected and /api/waze/* serves only the
    handful of regions visited since boot until the rotation completes
    (~7-16 min). Stale per-bbox entries are pruned during the merge so we
    never restore data the watcher would have already dropped."""
    try:
        payload, _age = cache_get_any(_WAZE_INGEST_PERSIST_KEY)
    except Exception as e:
        Log.warn(f"Waze ingest cache hydrate read failed: {e}")
        return
    if not payload or not isinstance(payload, list):
        return
    now = time.time()
    restored = 0
    pruned = 0
    with _waze_ingest_lock:
        for row in payload:
            try:
                key = tuple(row.get('key') or ())
                if len(key) != 4:
                    continue
                ts = float(row.get('ts') or 0)
                if now - ts > WAZE_INGEST_MAX_AGE:
                    pruned += 1
                    continue
                _waze_ingest_cache[key] = {
                    'alerts': row.get('alerts') or [],
                    'jams': row.get('jams') or [],
                    'users': row.get('users') or [],
                    'ts': ts,
                }
                restored += 1
            except Exception:
                continue
    if restored:
        Log.startup(
            f"Waze ingest cache hydrated from Postgres — "
            f"{restored} regions restored, {pruned} expired"
        )


def _waze_ingest_persist_loop():
    """Periodically snapshot _waze_ingest_cache to a single api_data_cache
    row so a backend restart can resume with the bboxes the userscript
    already collected, instead of starting from zero. The dict is copied
    under the ingest lock; serialization and the DB write happen outside
    the lock so we don't slow ingests."""
    if WAZE_INGEST_PERSIST_INTERVAL <= 0:
        return
    while True:
        try:
            time.sleep(WAZE_INGEST_PERSIST_INTERVAL)
            with _waze_ingest_lock:
                snapshot = [
                    {
                        'key': list(key),
                        'alerts': entry.get('alerts') or [],
                        'jams': entry.get('jams') or [],
                        'users': entry.get('users') or [],
                        'ts': entry.get('ts') or 0,
                    }
                    for key, entry in _waze_ingest_cache.items()
                ]
            if not snapshot:
                continue
            # Long TTL — pruning is driven by the per-entry ts vs
            # WAZE_INGEST_MAX_AGE on hydrate, not by cache_get's TTL.
            cache_set(_WAZE_INGEST_PERSIST_KEY, snapshot, ttl=86400)
        except Exception as e:
            Log.warn(f"Waze ingest persist tick error: {e}")


@app.route('/api/waze/ingest', methods=['POST'])
def waze_ingest():
    """Accept scraped georss data from a Tampermonkey userscript running in a
    real browser. Auth via X-Ingest-Key header matched against WAZE_INGEST_KEY.
    Payload shape mirrors Waze's native /api/georss response plus a bbox:
      { "bbox": {"top":..,"bottom":..,"left":..,"right":..},
        "alerts": [...], "jams": [...], "users": [...] }
    """
    if not WAZE_INGEST_ENABLED:
        return jsonify({'error': 'ingest disabled'}), 403
    supplied = request.headers.get('X-Ingest-Key', '')
    if not WAZE_INGEST_KEY or supplied != WAZE_INGEST_KEY:
        return jsonify({'error': 'unauthorized'}), 401
    try:
        payload = request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify({'error': 'bad json'}), 400

    bbox = payload.get('bbox') or {}
    try:
        key = (
            round(float(bbox.get('top', 0)), 3),
            round(float(bbox.get('bottom', 0)), 3),
            round(float(bbox.get('left', 0)), 3),
            round(float(bbox.get('right', 0)), 3),
        )
    except (TypeError, ValueError):
        return jsonify({'error': 'bad bbox'}), 400

    alerts = payload.get('alerts') or []
    jams = payload.get('jams') or []
    users = payload.get('users') or []

    global _waze_last_ingest_at, _waze_stale_alerted
    now_ts = time.time()
    with _waze_ingest_lock:
        _waze_ingest_cache[key] = {
            'alerts': alerts,
            'jams': jams,
            'users': users,
            'ts': now_ts,
        }
        n_regions = len(_waze_ingest_cache)
        _waze_last_ingest_at = now_ts
        if _waze_stale_alerted:
            _waze_stale_alerted = False
            Log.info("Waze ✓ Ingest resumed — clearing stale flag")

    # Per-ingest line was firing every ~3s in DEV_MODE — too noisy. Now
    # accumulate into a rolling counter and emit one summary every 30s.
    if DEV_MODE:
        global _waze_ingest_summary
        try:
            _waze_ingest_summary
        except NameError:
            _waze_ingest_summary = {'count': 0, 'alerts': 0, 'jams': 0,
                                     'regions': 0, 'last_log': 0.0}
        _waze_ingest_summary['count']   += 1
        _waze_ingest_summary['alerts']  += len(alerts)
        _waze_ingest_summary['jams']    += len(jams)
        _waze_ingest_summary['regions']  = n_regions
        if (now_ts - _waze_ingest_summary.get('last_log', 0)) >= 30:
            s = _waze_ingest_summary
            Log.info(
                f"Waze ▶ Ingest summary (last 30s): {s['count']} ingests, "
                f"{s['alerts']} alerts, {s['jams']} jams "
                f"(regions cached: {s['regions']})"
            )
            _waze_ingest_summary = {'count': 0, 'alerts': 0, 'jams': 0,
                                     'regions': n_regions, 'last_log': now_ts}

    # Per-region 'gone' reconcile: any archived Waze record inside this bbox
    # whose uuid isn't in the payload is cleared immediately. Failures here
    # must not break ingest, so swallow exceptions.
    try:
        _waze_reconcile_bbox(bbox, alerts, jams)
    except Exception as e:
        Log.error(f"Waze bbox reconcile failed: {e}")

    return jsonify({'ok': True, 'alerts': len(alerts), 'jams': len(jams), 'regions_cached': n_regions})


def _waze_staleness_watcher():
    """Background loop that fires a warning (and optional Discord ping) when
    the userscript stops delivering ingest POSTs for longer than
    WAZE_INGEST_STALE_SECS. One alert per stale→healthy cycle."""
    global _waze_stale_alerted
    # Give the browser a chance to boot and deliver its first POST before we
    # start counting. Matches the practical cold-start of Firefox + Waze.
    startup_grace = 120
    check_interval = 60
    started_at = time.time()
    time.sleep(startup_grace)
    while not _shutdown_event.is_set():
        try:
            last = _waze_last_ingest_at
            now = time.time()
            if last == 0.0:
                # No ingest yet. Count staleness from the grace-period end so we
                # don't alert on a backend that just started.
                age = now - (started_at + startup_grace)
            else:
                age = now - last
            if age > WAZE_INGEST_STALE_SECS and not _waze_stale_alerted:
                _waze_stale_alerted = True
                mins = int(age // 60)
                msg = (
                    f"Waze ingest stale: no POST in {mins}m. "
                    "Check the scraper browser (Firefox + Tampermonkey)."
                )
                Log.warn(f"⚠ {msg}")
                if WAZE_STALE_WEBHOOK:
                    try:
                        requests.post(
                            WAZE_STALE_WEBHOOK,
                            json={'content': f'⚠️ NSWPSN: {msg}'},
                            headers={'Content-Type': 'application/json'},
                            timeout=10,
                        )
                    except Exception as e:
                        Log.error(f"Waze staleness webhook failed: {e}")
        except Exception as e:
            Log.error(f"Waze staleness watcher error: {e}")
        # Sleep in 5s slices so shutdown is responsive
        for _ in range(check_interval // 5):
            if _shutdown_event.is_set():
                return
            time.sleep(5)


@app.route('/api/waze/alerts')
def waze_alerts():
    """Waze alerts for NSW - raw categorized data (uses persistent cache)"""
    # This endpoint combines all categories, so check if any individual cache exists
    hazard_data, _, _ = cache_get('waze_hazards')
    police_data, _, _ = cache_get('waze_police')
    roadwork_data, _, _ = cache_get('waze_roadwork')
    
    # If all caches exist, combine them
    if hazard_data and police_data and roadwork_data:
        return jsonify({
            'type': 'FeatureCollection',
            'features': {
                'hazards': hazard_data.get('features', []),
                'police': police_data.get('features', []),
                'roadwork': roadwork_data.get('features', [])
            },
            'counts': {
                'hazards': len(hazard_data.get('features', [])),
                'police': len(police_data.get('features', [])),
                'roadwork': len(roadwork_data.get('features', []))
            }
        })
    
    # Fallback: fetch live
    alerts, jams = fetch_waze_data()
    
    hazards = []
    police = []
    roadwork = []
    other = []
    jam_features = []
    
    for alert in alerts:
        alert_type = alert.get('type', '')
        subtype = alert.get('subtype', '')
        
        # Categorize alerts - check police first
        if alert_type in WAZE_POLICE_TYPES or subtype in WAZE_POLICE_SUBTYPES:
            feature = parse_waze_alert(alert, 'Police')
            if feature:
                police.append(feature)
        # Check construction/roadwork subtypes (not just types)
        elif subtype in WAZE_ROADWORK_SUBTYPES or alert_type == 'CONSTRUCTION':
            feature = parse_waze_alert(alert, 'Roadwork')
            if feature:
                roadwork.append(feature)
        # Everything else that matches hazard types goes to hazards
        elif alert_type in WAZE_HAZARD_TYPES or WAZE_HAZARD_SUBTYPES.get(subtype, False):
            feature = parse_waze_alert(alert, 'Hazard')
            if feature:
                hazards.append(feature)
        else:
            feature = parse_waze_alert(alert, 'Other')
            if feature:
                other.append(feature)
    
    # Parse jams
    for jam in jams:
        feature = parse_waze_jam(jam)
        if feature:
            jam_features.append(feature)
    
    return jsonify({
        'hazards': {'type': 'FeatureCollection', 'features': hazards, 'count': len(hazards)},
        'police': {'type': 'FeatureCollection', 'features': police, 'count': len(police)},
        'roadwork': {'type': 'FeatureCollection', 'features': roadwork, 'count': len(roadwork)},
        'jams': {'type': 'FeatureCollection', 'features': jam_features, 'count': len(jam_features)},
        'other': {'type': 'FeatureCollection', 'features': other, 'count': len(other)},
        'totalCount': len(alerts),
        'jamCount': len(jam_features)
    })


@app.route('/api/waze/hazards')
def waze_hazards():
    """Waze road hazards (uses persistent cache)"""
    cached_data, age, expired = cache_get('waze_hazards')
    if cached_data and not expired:
        return jsonify(cached_data)

    # Stale or missing — rebuild from the ingest snapshot so new regions
    # are picked up. See waze_police() for the full explanation.
    alerts, jams = fetch_waze_data()
    features = []
    jam_features = []
    for alert in alerts:
        alert_type = alert.get('type', '').upper()
        subtype = alert.get('subtype', '') or ''
        subtype_upper = subtype.upper()
        is_police = (alert_type == 'POLICE' or 'POLICE' in subtype_upper)
        if is_police:
            continue
        is_roadwork = (alert_type == 'CONSTRUCTION' or 'CONSTRUCTION' in subtype_upper)
        if is_roadwork:
            continue
        if alert_type in {'HAZARD', 'ACCIDENT', 'JAM', 'ROAD_CLOSED'}:
            feature = parse_waze_alert(alert, 'Hazard')
            if feature:
                features.append(feature)
    for jam in jams:
        feature = parse_waze_jam(jam)
        if feature:
            jam_features.append(feature)

    result = {
        'type': 'FeatureCollection',
        'features': features,
        'jams': jam_features,
        'count': len(features),
        'jamCount': len(jam_features),
    }
    if features or jam_features:
        cache_set('waze_hazards', result, 120)
        return jsonify(result)
    if cached_data:
        return jsonify(cached_data)
    return jsonify(result)


@app.route('/api/waze/police')
def waze_police():
    """Waze police reports (uses persistent cache)"""
    cached_data, age, expired = cache_get('waze_police')
    if cached_data and not expired:
        return jsonify(cached_data)

    # Stale or missing — rebuild from the in-memory ingest snapshot. This
    # is fast (no DB calls; just merges the per-bbox dict) and is the only
    # way new regions ingested after the first cache_set make it into the
    # response. Previous bug: a stale cache was returned without refresh,
    # so once the cache populated with whatever bboxes were available at
    # that moment (typically just Sydney right after a restart), regional
    # data never surfaced again.
    alerts, _ = fetch_waze_data()
    features = []
    for alert in alerts:
        alert_type = alert.get('type', '').upper()
        subtype = alert.get('subtype', '') or ''
        is_police = (alert_type == 'POLICE' or 'POLICE' in subtype.upper())
        if is_police:
            feature = parse_waze_alert(alert, 'Police')
            if feature:
                features.append(feature)

    result = {'type': 'FeatureCollection', 'features': features, 'count': len(features)}
    if features:
        cache_set('waze_police', result, 120)
        return jsonify(result)
    # Live rebuild produced nothing (e.g. ingest cache empty mid-restart);
    # fall back to whatever we had cached so the map isn't blank.
    if cached_data:
        return jsonify(cached_data)
    return jsonify(result)


# --- Police heatmap -------------------------------------------------------
# Two-tier cache: RAM (read path) backed by Postgres (restart durability).
#   - Refresh worker aggregates data_history → writes to BOTH the in-memory
#     dict and the police_heatmap_cache table.
#   - Endpoint reads only the RAM dict — never touches Postgres on the
#     request path, so write contention from archiving can't slow it down.
#   - On startup we hydrate the RAM dict from the Postgres cache so the
#     cache is hot the moment the backend is up, no warming window.

_POLICE_HEATMAP_BIN_DEG = 0.001   # ~110 m at NSW latitudes
# Output cap. Without a bbox, we ship the top N hottest bins in NSW. With
# 41k+ bins live in the cache the old 5k cap silently dropped low-count
# suburban/regional bins (count=1 in a quiet area), so a pin would render
# without any hex underneath. Raise the safety net so the full set ships.
# When the client passes ?bbox= we filter to the viewport BEFORE the cap
# so bandwidth stays low on tight zooms.
_POLICE_HEATMAP_MAX_BINS = 60000
_POLICE_HEATMAP_WINDOW_DAYS = 30  # rolling window when refreshing the cache
_POLICE_VALID_SUBTYPES = {
    'POLICE_VISIBLE', 'POLICE_HIDING', 'POLICE_WITH_MOBILE_CAMERA',
}
POLICE_HEATMAP_REFRESH_INTERVAL = int(
    os.environ.get('POLICE_HEATMAP_REFRESH_INTERVAL', 600))  # 10 min

# In-memory cache. Populated on startup from Postgres (instant warm-up)
# and refreshed by the background scheduler. Protected by a lock so a
# refresh swap is atomic from the reader's POV.
_POLICE_HEATMAP_RAM = {
    'rows':       [],     # list of (lat, lng, subcategory, count)
    'updated_at': None,   # datetime of last successful refresh
}
_POLICE_HEATMAP_RAM_LOCK = threading.Lock()


@app.route('/api/waze/police-heatmap')
@require_api_key
def waze_police_heatmap():
    """Heatmap of recent Waze police pings, served entirely from the
    in-memory dict. The DB is never read on the request path."""
    raw_subtypes = (request.args.get('subtypes') or '').strip()
    if raw_subtypes:
        wanted = {s.strip().upper() for s in raw_subtypes.split(',') if s.strip()}
        wanted &= _POLICE_VALID_SUBTYPES
    else:
        wanted = None  # None = all (no filter)

    # Optional viewport filter: bbox=south,west,north,east (decimal degrees).
    # Lets the client request only bins that intersect the current map view,
    # which keeps the response small on tight zooms and prevents the top-N
    # cap from silently dropping low-count bins under visible pins.
    bbox = None
    raw_bbox = (request.args.get('bbox') or '').strip()
    if raw_bbox:
        try:
            parts = [float(p) for p in raw_bbox.split(',')]
            if len(parts) == 4:
                s, w, n, e = parts
                if s < n and w < e:
                    bbox = (s, w, n, e)
        except (TypeError, ValueError):
            bbox = None

    # Atomically grab the current snapshot. Refresh writes happen with the
    # same lock held, so we always see a consistent set of rows.
    with _POLICE_HEATMAP_RAM_LOCK:
        rows = _POLICE_HEATMAP_RAM['rows']
        updated_at = _POLICE_HEATMAP_RAM['updated_at']

    # Group (lat_bin, lng_bin) → summed count, applying the subtype filter.
    # Police alerts with no subtype (Waze can send bare type=POLICE) are
    # treated as POLICE_VISIBLE — mirrors the frontend pin default in
    # getPoliceCategory() so the heatmap and pins agree on coverage.
    bins = {}
    for lat_v, lng_v, sub, cnt in rows:
        eff = sub or 'POLICE_VISIBLE'
        if wanted is not None and eff not in wanted:
            continue
        if bbox is not None:
            s, w, n, e = bbox
            if lat_v < s or lat_v > n or lng_v < w or lng_v > e:
                continue
        key = (lat_v, lng_v)
        bins[key] = bins.get(key, 0) + cnt

    # Top N hottest bins.
    items = sorted(bins.items(), key=lambda x: x[1], reverse=True)[:_POLICE_HEATMAP_MAX_BINS]
    points = [[round(k[0], 5), round(k[1], 5), v] for k, v in items]
    max_count = items[0][1] if items else 0
    total_records = sum(v for _, v in items)

    cache_status = 'ok' if updated_at is not None else 'warming'

    return jsonify({
        'points': points,
        'total_records': total_records,
        'bin_size_deg': _POLICE_HEATMAP_BIN_DEG,
        'max_count': max_count,
        'days': _POLICE_HEATMAP_WINDOW_DAYS,
        'subtypes': sorted(wanted) if wanted else sorted(_POLICE_VALID_SUBTYPES),
        'cache_updated_at': updated_at.isoformat() if updated_at else None,
        'cache_status': cache_status,
    })


def _hydrate_police_heatmap_ram_from_db():
    """Populate the RAM cache from the Postgres cache on startup. Called
    once during init_archive_db so the heatmap is hot the moment the
    backend is up — even if the first scheduled refresh is still running."""
    rows = []
    updated_at = None
    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SET LOCAL statement_timeout = '10s'")
            cur.execute(
                'SELECT lat_bin, lng_bin, subcategory, count, updated_at '
                'FROM police_heatmap_cache'
            )
            for lat_b, lng_b, sub, cnt, ts in cur.fetchall():
                try:
                    rows.append((float(lat_b), float(lng_b), sub or '', int(cnt)))
                    if ts is not None and (updated_at is None or ts > updated_at):
                        updated_at = ts
                except (TypeError, ValueError):
                    continue
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        Log.warn(f"Police heatmap RAM hydrate skipped: {e}")
        return
    with _POLICE_HEATMAP_RAM_LOCK:
        _POLICE_HEATMAP_RAM['rows'] = rows
        _POLICE_HEATMAP_RAM['updated_at'] = updated_at
    if rows and DEV_MODE:
        Log.startup(f"Police heatmap RAM hydrated from Postgres — {len(rows)} bins")


def _refresh_police_heatmap_cache():
    """Rebuild the police heatmap cache from data_history.

    Three phases:
      1. Slow read aggregates data_history into a Python list. No locks
         held on either cache (RAM or Postgres) during this — readers
         keep seeing the previous snapshot.
      2. RAM swap (microseconds): atomic update under the RAM lock.
         Reads after this point see the new data immediately.
      3. Postgres swap (milliseconds): TRUNCATE + bulk insert so the
         disk cache survives a restart. If this fails, RAM still has
         the new data — the read path doesn't notice.
    """
    cutoff = int(time.time()) - (_POLICE_HEATMAP_WINDOW_DAYS * 86400)
    bin_deg = _POLICE_HEATMAP_BIN_DEG

    # ---- Phase 1: slow aggregation, no cache locks held ------------------
    raw = []
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '300s'")
        cur.execute(
            '''
            SELECT
                ROUND(latitude::numeric  / %s) * %s AS lat_bin,
                ROUND(longitude::numeric / %s) * %s AS lng_bin,
                COALESCE(subcategory, '') AS sub,
                COUNT(*) AS cnt
            FROM data_history
            WHERE is_latest = 1
              AND source = 'waze_police'
              AND fetched_at >= %s
              AND latitude  IS NOT NULL
              AND longitude IS NOT NULL
            GROUP BY ROUND(latitude::numeric  / %s) * %s,
                     ROUND(longitude::numeric / %s) * %s,
                     COALESCE(subcategory, '')
            ''',
            [bin_deg, bin_deg, bin_deg, bin_deg, cutoff,
             bin_deg, bin_deg, bin_deg, bin_deg],
        )
        raw = cur.fetchall()
        cur.close()
    except Exception as e:
        Log.error(f"Police heatmap aggregation error: {e}")
        return
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # Normalise + ditch any rows that can't be coerced to numbers.
    rows = []
    for r in raw:
        try:
            rows.append((float(r[0]), float(r[1]), r[2] or '', int(r[3])))
        except (TypeError, ValueError):
            continue

    # ---- Phase 2: RAM swap — instant, atomic ----------------------------
    now = datetime.now()
    with _POLICE_HEATMAP_RAM_LOCK:
        _POLICE_HEATMAP_RAM['rows'] = rows
        _POLICE_HEATMAP_RAM['updated_at'] = now
    if DEV_MODE:
        Log.cleanup(f"Police heatmap RAM refreshed — {len(rows)} bins")

    # ---- Phase 3: Postgres swap — durability across restarts -----------
    if not rows:
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCAL statement_timeout = '30s'")
        cur.execute("SET LOCAL lock_timeout = '10s'")
        cur.execute('TRUNCATE police_heatmap_cache')
        try:
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                'INSERT INTO police_heatmap_cache '
                '(lat_bin, lng_bin, subcategory, count, updated_at) VALUES %s',
                [(r[0], r[1], r[2], r[3], now) for r in rows],
                page_size=1000,
            )
        except ImportError:
            args_str = b','.join(
                cur.mogrify('(%s,%s,%s,%s,now())',
                            (r[0], r[1], r[2], r[3])) for r in rows
            ).decode('utf-8')
            cur.execute(
                'INSERT INTO police_heatmap_cache '
                '(lat_bin, lng_bin, subcategory, count, updated_at) VALUES '
                + args_str
            )
        conn.commit()
        cur.close()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        # Postgres swap failed — RAM is still up to date, so the heatmap
        # keeps working. Worst case, after a restart we lose the freshest
        # snapshot and rehydrate from the previous DB state.
        Log.warn(f"Police heatmap Postgres swap failed (RAM still fresh): {e}")
    finally:
        conn.close()


def _police_heatmap_scheduler():
    """Background loop that refreshes the police heatmap cache every N
    seconds (POLICE_HEATMAP_REFRESH_INTERVAL). First refresh fires almost
    immediately so the cache populates as soon as possible after restart."""
    # Tiny stagger so we don't slam the DB the second the app starts.
    time.sleep(5)
    while not _shutdown_event.is_set():
        try:
            _refresh_police_heatmap_cache()
        except Exception as e:
            Log.error(f"Police heatmap scheduler error: {e}")
        if _shutdown_event.wait(timeout=POLICE_HEATMAP_REFRESH_INTERVAL):
            return


@app.route('/api/waze/roadwork')
def waze_roadwork():
    """Waze construction and road closures (uses persistent cache)"""
    cached_data, age, expired = cache_get('waze_roadwork')
    if cached_data and not expired:
        return jsonify(cached_data)

    # Stale or missing — rebuild from ingest snapshot. See waze_police().
    alerts, _ = fetch_waze_data()
    features = []
    for alert in alerts:
        alert_type = alert.get('type', '').upper()
        subtype = alert.get('subtype', '') or ''
        subtype_upper = subtype.upper()
        is_roadwork = (alert_type == 'CONSTRUCTION' or
                       subtype in WAZE_ROADWORK_SUBTYPES or
                       'CONSTRUCTION' in subtype_upper)
        if is_roadwork:
            feature = parse_waze_alert(alert, 'Roadwork')
            if feature:
                features.append(feature)

    result = {'type': 'FeatureCollection', 'features': features, 'count': len(features)}
    if features:
        cache_set('waze_roadwork', result, CACHE_TTL_TRAFFIC)
        return jsonify(result)
    if cached_data:
        return jsonify(cached_data)
    return jsonify(result)


@app.route('/api/waze/types')
def waze_types():
    """Debug endpoint - show unique alert types and subtypes from Waze"""
    alerts, jams = fetch_waze_data()
    types = {}
    for alert in alerts:
        alert_type = alert.get('type', 'UNKNOWN')
        subtype = alert.get('subtype', '')
        if alert_type not in types:
            types[alert_type] = {'count': 0, 'subtypes': {}}
        types[alert_type]['count'] += 1
        if subtype:
            types[alert_type]['subtypes'][subtype] = types[alert_type]['subtypes'].get(subtype, 0) + 1
    return jsonify({
        'types': types, 
        'totalAlerts': len(alerts),
        'totalJams': len(jams),
        'regionsQueried': len(NSW_REGIONS),
        'note': 'Data fetched from multiple NSW regions for better coverage'
    })


@app.route('/api/waze/raw')
@cached(ttl=CACHE_TTL_TRAFFIC)
def waze_raw():
    """Raw Waze alerts data for debugging"""
    alerts = fetch_waze_alerts()
    return jsonify({'alerts': alerts, 'count': len(alerts)})


@app.route('/api/waze/metrics')
def waze_metrics():
    """Return the Waze fetch block-rate snapshot.

    The backend counts every userscript ingest attempt in a 30-minute rolling
    window and classifies it as `success`, `block`, or `error`. `gate_active`
    means the block rate has crossed the 2% threshold.
    """
    snap = _waze_metrics_snapshot()
    # Also expose cache freshness so operators can see whether the cache is
    # keeping the public endpoints healthy.
    cache_age = int(time.time() - _waze_cache['timestamp']) if _waze_cache['timestamp'] else None
    return jsonify({
        **snap,
        'cache_alert_count': len(_waze_cache['alerts']),
        'cache_jam_count': len(_waze_cache['jams']),
        'cache_age_seconds': cache_age,
    })


@app.route('/api/waze/debug')
def waze_debug():
    """Debug Waze API - bypass cache, fetch one region, return raw response structure.
    Use to diagnose when Waze returns 0 items (API changes, blocking, rate limits)."""
    region = NSW_REGIONS[0]  # Sydney CBD
    try:
        url = 'https://www.waze.com/live-map/api/georss'
        params = {
            'top': region['top'], 'bottom': region['bottom'],
            'left': region['left'], 'right': region['right'],
            'env': 'row', 'types': 'alerts,traffic'
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.waze.com/live-map'
        }
        # Direct upstream fetch is expected to fail (Cloud Armor + reCAPTCHA);
        # this endpoint only exists to confirm what Waze returns when poked.
        method_used = 'requests'
        if CURL_CFFI_AVAILABLE and curl_requests:
            try:
                r = curl_requests.get(url, params=params, timeout=15, impersonate='chrome', headers=headers)
                method_used = 'curl_cffi'
            except Exception:
                r = requests.get(url, params=params, timeout=15, headers=headers)
        else:
            r = requests.get(url, params=params, timeout=15, headers=headers)
        content_type = r.headers.get('Content-Type', '')
        is_json = 'json' in content_type.lower()
        data = None
        if r.status_code == 200:
            try:
                data = r.json() if is_json else None
            except Exception:
                data = None
        # Detect if Waze returned HTML (e.g. captcha/block page) instead of JSON
        body_preview = r.text[:200] if r.text else ''
        if r.status_code == 200 and (not is_json or data is None):
            body_preview = (r.text or '')[:500].replace('\n', ' ')
        return jsonify({
            'status_code': r.status_code,
            'content_type': content_type,
            'method': method_used,
            'region': region['name'],
            'response_keys': list(data.keys()) if isinstance(data, dict) else None,
            'alerts_count': len(data.get('alerts', [])) if data else 0,
            'jams_count': len(data.get('jams', [])) if data else 0,
            'sample_alert_keys': list(data['alerts'][0].keys())[:15] if data and data.get('alerts') else None,
            'raw_preview': {k: (len(v) if isinstance(v, (list, dict)) else v) for k, v in (data or {}).items()},
            'body_preview': body_preview if not is_json or not data else None
        })
    except Exception as e:
        return jsonify({'error': str(e), 'type': type(e).__name__}), 500


def parse_rfs_local_time(time_str):
    """Parse RFS local time string (e.g., '7 Jan 2026 13:35') to ISO format with timezone.
    
    RFS times are in Australian Eastern Time (AEST/AEDT).
    Returns ISO format string with timezone, e.g., '2026-01-07T13:35:00+11:00'
    """
    if not time_str:
        return ''
    
    try:
        # Try parsing format like "7 Jan 2026 13:35"
        from datetime import datetime
        from zoneinfo import ZoneInfo
        
        # Parse the time string
        dt = datetime.strptime(time_str.strip(), '%d %b %Y %H:%M')
        
        # Assume Sydney timezone (handles AEST/AEDT automatically)
        sydney_tz = ZoneInfo('Australia/Sydney')
        dt_local = dt.replace(tzinfo=sydney_tz)
        
        # Return ISO format with timezone
        return dt_local.isoformat()
    except Exception:
        # If parsing fails, return the original string
        return time_str


def parse_rfs_description(desc):
    """Parse RFS incident description into structured fields
    
    Format: ALERT LEVEL: xxx <br />LOCATION: xxx <br />COUNCIL AREA: xxx <br />...
    Or after cleaning: ALERT LEVEL: xxx LOCATION: xxx COUNCIL AREA: xxx ...
    """
    result = {
        'alertLevel': '',
        'location': '',
        'councilArea': '',
        'status': '',
        'fireType': '',
        'size': '',
        'responsibleAgency': '',
        'updated': '',
        'updatedISO': ''  # ISO format with timezone for proper parsing
    }
    
    if not desc:
        return result
    
    # Clean HTML tags but preserve structure for parsing
    # Replace <br /> and similar with a delimiter we can use
    clean_desc = re.sub(r'<br\s*/?>', ' | ', desc)
    clean_desc = re.sub(r'<[^>]+>', '', clean_desc)
    clean_desc = re.sub(r'\s+', ' ', clean_desc).strip()
    
    # Try to extract ALERT LEVEL from the start or category
    alert_match = re.match(r'^(Advice|Watch and Act|Emergency Warning|Emergency)\s*[:|]?\s*', clean_desc, re.IGNORECASE)
    if alert_match:
        result['alertLevel'] = alert_match.group(1).strip()
    
    # Also check for ALERT LEVEL: prefix format
    alert_level_match = re.search(r'ALERT\s*LEVEL:\s*([^|]+?)(?=\s*\||$)', clean_desc, re.IGNORECASE)
    if alert_level_match:
        result['alertLevel'] = alert_level_match.group(1).strip()
    
    # Extract fields - look for FIELD: value patterns
    field_patterns = [
        ('location', r'LOCATION:\s*([^|]+?)(?=\s*\||COUNCIL|STATUS|TYPE|FIRE|SIZE|RESPONSIBLE|UPDATED|$)'),
        ('councilArea', r'COUNCIL\s*AREA:\s*([^|]+?)(?=\s*\||STATUS|TYPE|FIRE|SIZE|RESPONSIBLE|UPDATED|$)'),
        ('status', r'STATUS:\s*([^|]+?)(?=\s*\||TYPE|FIRE|SIZE|RESPONSIBLE|UPDATED|$)'),
        ('fireType', r'TYPE:\s*([^|]+?)(?=\s*\||FIRE:|SIZE|RESPONSIBLE|UPDATED|$)'),
        ('size', r'SIZE:\s*([^|]+?)(?=\s*\||RESPONSIBLE|UPDATED|$)'),
        ('responsibleAgency', r'RESPONSIBLE\s*AGENCY:\s*([^|]+?)(?=\s*\||UPDATED|$)'),
        ('updated', r'UPDATED:\s*([^|]+?)(?=\s*\||$)'),
    ]
    
    for field_name, pattern in field_patterns:
        match = re.search(pattern, clean_desc, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            # Clean up any trailing pipes or whitespace
            value = re.sub(r'\s*\|\s*$', '', value)
            result[field_name] = value
    
    # Convert updated time to ISO format with timezone
    if result['updated']:
        result['updatedISO'] = parse_rfs_local_time(result['updated'])
    
    return result


@app.route('/api/rfs/incidents')
def rfs_incidents():
    """RFS Major Incidents - parse XML to GeoJSON (uses persistent cache)"""
    # Check persistent cache first (instant response!)
    cached_data, age, expired = cache_get('rfs_incidents')
    if cached_data and not expired:
        return jsonify(cached_data)
    
    # Return stale cache if available (prewarm thread will refresh)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live if cache is completely empty
    features = []
    try:
        r = requests.get(
            'https://www.rfs.nsw.gov.au/feeds/majorIncidents.xml',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.content)
            
            # Handle namespaces
            ns = {'georss': 'http://www.georss.org/georss'}
            
            for item in root.findall('.//item'):
                title = item.findtext('title', '')
                link = item.findtext('link', '')
                desc = item.findtext('description', '')
                # Note: pubDate is the feed refresh time, not incident creation time
                # The actual incident-specific time is in the UPDATED field of the description
                guid = item.findtext('guid', '')
                category = item.findtext('category', '')  # Alert level from category
                
                # Get point coordinates
                point = item.find('.//georss:point', ns)
                if point is None:
                    point = item.find('.//{http://www.georss.org/georss}point')
                if point is None:
                    point = item.find('.//point')
                
                # Get polygon if available
                polygons = []
                for poly in item.findall('.//{http://www.georss.org/georss}polygon'):
                    if poly.text:
                        polygons.append(poly.text.strip())
                
                if point is not None and point.text:
                    try:
                        coords = point.text.strip().split()
                        if len(coords) >= 2:
                            lat = float(coords[0])
                            lon = float(coords[1])
                            
                            # Parse description for details using improved parser
                            parsed = parse_rfs_description(desc)
                            
                            # Use category as alert level if not parsed from description
                            if not parsed['alertLevel'] and category:
                                parsed['alertLevel'] = category
                            
                            # Clean the description of HTML for raw display
                            clean_desc = re.sub(r'<[^>]+>', ' ', desc or '').strip()
                            clean_desc = re.sub(r'\s+', ' ', clean_desc)
                            
                            features.append({
                                'type': 'Feature',
                                'geometry': {
                                    'type': 'Point',
                                    'coordinates': [lon, lat]
                                },
                                'properties': {
                                    'title': title,
                                    'link': link,
                                    'guid': guid,
                                    'description': clean_desc,  # Include for display/bot parsing
                                    'status': parsed['status'],
                                    'location': parsed['location'],
                                    'size': parsed['size'],
                                    'alertLevel': parsed['alertLevel'],
                                    'fireType': parsed['fireType'],
                                    'councilArea': parsed['councilArea'],
                                    'responsibleAgency': parsed['responsibleAgency'],
                                    # NOTE: pubDate is feed generation time (same for all items), NOT incident creation
                                    # The only incident-specific time is 'updated' from the description
                                    'updated': parsed['updated'],       # Display format: "7 Jan 2026 13:35"
                                    'updatedISO': parsed['updatedISO'], # ISO format: "2026-01-07T13:35:00+11:00"
                                    'polygons': polygons,      # Fire boundary polygons
                                    'source': 'rfs'
                                }
                            })
                    except (ValueError, IndexError) as e:
                        Log.error(f"RFS parse error for {title}: {e}")
    except Exception as e:
        Log.error(f"RFS error: {e}")
    
    return jsonify({
        'type': 'FeatureCollection',
        'features': features,
        'count': len(features)
    })


@app.route('/api/rfs/incidents/raw')
@cached(ttl=CACHE_TTL_RFS)
def rfs_incidents_raw():
    """RFS Major Incidents - raw XML converted to JSON without processing"""
    try:
        r = requests.get(
            'https://www.rfs.nsw.gov.au/feeds/majorIncidents.xml',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.content)
            
            ns = {'georss': 'http://www.georss.org/georss'}
            
            items = []
            for item in root.findall('.//item'):
                point_elem = item.find('.//{http://www.georss.org/georss}point')
                point_text = point_elem.text.strip() if point_elem is not None and point_elem.text else None
                
                items.append({
                    'title': item.findtext('title', ''),
                    'link': item.findtext('link', ''),
                    'description': item.findtext('description', ''),
                    'pubDate': item.findtext('pubDate', ''),
                    'guid': item.findtext('guid', ''),
                    'category': item.findtext('category', ''),
                    'point': point_text
                })
            
            return jsonify({
                'channel': {
                    'title': root.findtext('.//channel/title', ''),
                    'description': root.findtext('.//channel/description', ''),
                    'pubDate': root.findtext('.//channel/pubDate', '')
                },
                'items': items,
                'count': len(items)
            })
    except Exception as e:
        Log.error(f"RFS raw error: {e}")
    
    return jsonify({'channel': {}, 'items': [], 'count': 0})


@app.route('/api/rfs/fdr')
@cached(ttl=CACHE_TTL_RFS_FDR)
def rfs_fdr():
    """RFS Fire Danger Ratings - parse XML to JSON"""
    ratings = []
    try:
        r = requests.get(
            'https://www.rfs.nsw.gov.au/feeds/fdrToban.xml',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.content)
            
            for item in root.findall('.//item'):
                title = item.findtext('title', '')
                desc = item.findtext('description', '')
                
                ratings.append({
                    'title': title,
                    'description': re.sub(r'<[^>]+>', '', desc or ''),
                    'source': 'rfs_fdr'
                })
    except Exception as e:
        Log.error(f"RFS FDR error: {e}")
    
    return jsonify({'ratings': ratings})






# ==================== WHAT3WORDS API ====================

@app.route('/api/w3w/convert-to-coordinates')
@require_api_key
def w3w_convert_to_coordinates():
    """Convert what3words address to GPS coordinates.
    Query params: words (e.g. 'lower.elder.truck')
    Uses mapapi.what3words.com (no API key required).
    """
    words = request.args.get('words', '').strip().lower()
    if not words or words.count('.') != 2:
        return jsonify({'error': 'Invalid what3words address. Expected format: word.word.word'}), 400
    
    try:
        r = requests.get(
            'https://mapapi.what3words.com/api/convert-to-coordinates',
            params={'words': words, 'format': 'json'},
            timeout=10
        )
        data = r.json()
        if r.status_code != 200 or 'error' in data:
            err_msg = data.get('error', {}).get('message', 'Unknown error')
            return jsonify({'error': err_msg}), r.status_code if r.status_code != 200 else 400
        return jsonify(data)
    except Exception as e:
        Log.error(f"W3W convert-to-coordinates error: {e}")
        return jsonify({'error': 'Failed to contact What3Words API'}), 502


@app.route('/api/w3w/convert-to-3wa')
@require_api_key
def w3w_convert_to_3wa():
    """Convert GPS coordinates to what3words address.
    Query params: coordinates (e.g. '-33.8688,151.2093') or lat & lon separately
    Uses mapapi.what3words.com (no API key required).
    """
    coordinates = request.args.get('coordinates', '').strip()
    if not coordinates:
        lat = request.args.get('lat', '')
        lon = request.args.get('lon', '')
        if lat and lon:
            coordinates = f"{lat},{lon}"
    
    if not coordinates:
        return jsonify({'error': 'Missing coordinates parameter'}), 400
    
    try:
        r = requests.get(
            'https://mapapi.what3words.com/api/convert-to-3wa',
            params={'coordinates': coordinates, 'language': 'en', 'format': 'json'},
            timeout=10
        )
        data = r.json()
        if r.status_code != 200 or 'error' in data:
            err_msg = data.get('error', {}).get('message', 'Unknown error')
            return jsonify({'error': err_msg}), r.status_code if r.status_code != 200 else 400
        return jsonify(data)
    except Exception as e:
        Log.error(f"W3W convert-to-3wa error: {e}")
        return jsonify({'error': 'Failed to contact What3Words API'}), 502


_w3w_grid_cache = {}          # key: rounded bbox string → value: (json_str, timestamp)
_W3W_GRID_CACHE_TTL = 86400   # 24 hours — grid lines never change
_W3W_GRID_CACHE_MAX = 500     # max cached tiles to avoid unbounded memory

def _round_bbox(bbox_str, precision=3):
    """Round bbox coordinates to reduce cache fragmentation.
    precision=3 ≈ ~111 m granularity, good for zoom 15+."""
    try:
        parts = [round(float(x.strip()), precision) for x in bbox_str.split(',')]
        return ','.join(str(p) for p in parts)
    except Exception:
        return bbox_str

@app.route('/api/w3w/grid-section')
@require_api_key
def w3w_grid_section():
    """Get what3words grid section for map display.
    Query params: bounding-box (e.g. 'south_lat,west_lng,north_lat,east_lng')
    Returns GeoJSON of the grid lines for the given bounding box.
    Uses mapapi.what3words.com (no API key required).
    Responses are cached in-memory for 24h since the grid never changes.
    """
    bounding_box = request.args.get('bounding-box', '').strip()
    if not bounding_box:
        return jsonify({'error': 'Missing bounding-box parameter'}), 400
    
    cache_key = _round_bbox(bounding_box)
    now = time.time()
    
    # Check in-memory cache
    cached = _w3w_grid_cache.get(cache_key)
    if cached:
        json_str, ts = cached
        if now - ts < _W3W_GRID_CACHE_TTL:
            return app.response_class(json_str, mimetype='application/json')
    
    try:
        r = requests.get(
            'https://mapapi.what3words.com/api/grid-section',
            params={'bounding-box': bounding_box, 'format': 'geojson'},
            timeout=10
        )
        data = r.json()
        if r.status_code != 200 or 'error' in data:
            err_msg = data.get('error', {}).get('message', 'Unknown error')
            return jsonify({'error': err_msg}), r.status_code if r.status_code != 200 else 400
        
        # Cache the response
        json_str = json.dumps(data)
        # Evict oldest entries if cache is full
        if len(_w3w_grid_cache) >= _W3W_GRID_CACHE_MAX:
            oldest_key = min(_w3w_grid_cache, key=lambda k: _w3w_grid_cache[k][1])
            del _w3w_grid_cache[oldest_key]
        _w3w_grid_cache[cache_key] = (json_str, now)
        
        return app.response_class(json_str, mimetype='application/json')
    except Exception as e:
        Log.error(f"W3W grid-section error: {e}")
        return jsonify({'error': 'Failed to contact What3Words API'}), 502


@app.route('/api/debug/test-all')
def debug_test_all():
    """Test all API endpoints and return status"""
    results = {}
    
    apis_to_test = [
        # Power
        ('Ausgrid Outages', 'https://www.ausgrid.com.au/webapi/OutageMapData/GetCurrentUnplannedOutageMarkersAndPolygons'),
        ('Ausgrid Stats', 'https://www.ausgrid.com.au/webapi/OutageMapData/GetOutageStatistics'),
        ('Endeavour Energy (Supabase)', f'{ENDEAVOUR_SUPABASE_URL}/rpc/get_outage_statistics'),
        # Traffic
        ('Live Traffic Incidents', 'https://www.livetraffic.com/traffic/hazards/incident.json'),
        ('Live Traffic Roadwork', 'https://www.livetraffic.com/traffic/hazards/roadwork.json'),
        ('Live Traffic Fire', 'https://www.livetraffic.com/traffic/hazards/fire.json'),
        ('Live Traffic Flood', 'https://www.livetraffic.com/traffic/hazards/flood.json'),
        ('Live Traffic Major Event', 'https://www.livetraffic.com/traffic/hazards/majorevent.json'),
        ('Live Traffic Cameras', 'https://www.livetraffic.com/cameras/cameras.json'),
        # Emergency
        ('RFS Major Incidents', 'https://www.rfs.nsw.gov.au/feeds/majorIncidents.xml'),
        ('RFS Fire Danger Ratings', 'https://www.rfs.nsw.gov.au/feeds/fdrToban.xml'),
        # Weather
        ('BOM Warnings', 'https://www.bom.gov.au/fwo/IDZ00054.warnings_nsw.xml'),
        ('Open-Meteo Weather', 'https://api.open-meteo.com/v1/forecast?latitude=-33.87&longitude=151.21&current=temperature_2m'),
        ('RainViewer Radar', 'https://api.rainviewer.com/public/weather-maps.json'),
        # Environment
        ('Beachwatch', 'https://api.beachwatch.nsw.gov.au/public/sites/geojson'),
        ('BeachSafe', 'https://beachsafe.org.au/api/v4/map/beaches?neCoords[]=-28.0&neCoords[]=154.0&swCoords[]=-37.5&swCoords[]=149.0'),
    ]
    
    for name, url in apis_to_test:
        try:
            # Use Supabase headers for Endeavour
            if 'supabase.co' in url:
                r = requests.post(url, headers=ENDEAVOUR_SUPABASE_HEADERS, json={}, timeout=10)
            else:
                r = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            content_type = r.headers.get('Content-Type', '')
            
            sample = None
            if 'json' in content_type:
                data = r.json()
                if isinstance(data, list):
                    sample = f"Array with {len(data)} items"
                    if len(data) > 0:
                        sample += f", first item keys: {list(data[0].keys()) if isinstance(data[0], dict) else type(data[0]).__name__}"
                elif isinstance(data, dict):
                    sample = f"Object with keys: {list(data.keys())[:10]}"
            else:
                sample = f"Content type: {content_type}, length: {len(r.content)}"
            
            results[name] = {
                'status': r.status_code,
                'ok': r.status_code == 200,
                'sample': sample
            }
        except Exception as e:
            results[name] = {
                'status': 'error',
                'ok': False,
                'error': str(e)
            }
    
    return jsonify(results)


@app.route('/api/config')
def get_config():
    """
    Return frontend configuration including API key.
    This endpoint is public so frontend can fetch the key on load.
    Note: The API key will still be visible in network requests - this just
    keeps it out of the HTML source code and git repository.
    """
    return jsonify({
        'apiKey': API_KEY,
        'version': '2.0'
    })

@app.route('/api/health')
def health():
    return jsonify({
        'status': 'ok',
        'mode': 'dev' if DEV_MODE else 'production',
        'cache_keys': list(cache.keys()),
        'active_viewers': get_active_page_count()
    })


# Thresholds for /api/status. Tunable via env so we can tighten/loosen
# without a redeploy. Defaults are generous — they only fire when something
# is genuinely wrong, not on transient hiccups.
STATUS_DB_TIMEOUT_SECS         = int(os.environ.get('STATUS_DB_TIMEOUT_SECS', 3))
STATUS_WRITER_STALE_SECS       = int(os.environ.get('STATUS_WRITER_STALE_SECS', 300))    # 5 min
STATUS_WAZE_STALE_SECS         = int(os.environ.get('STATUS_WAZE_STALE_SECS', 900))      # 15 min
STATUS_BUFFER_WARN_RECORDS     = int(os.environ.get('STATUS_BUFFER_WARN_RECORDS', 10_000))
STATUS_HEATMAP_STALE_SECS      = int(os.environ.get('STATUS_HEATMAP_STALE_SECS', 1800))  # 30 min
STATUS_FILTER_CACHE_STALE_SECS = int(os.environ.get('STATUS_FILTER_CACHE_STALE_SECS', 1800))


def _status_source_summary(now):
    """Roll up _SOURCE_HEALTH into a per-source dict that's easy to query
    with JSONata: sources.<name>.ok, sources.<name>.last_success_age_secs,
    etc. Status per source is ok | degraded | down | unknown, derived from
    the soft/hard thresholds in _SOURCE_THRESHOLDS."""
    out = {}
    counts = {'ok': 0, 'degraded': 0, 'down': 0, 'unknown': 0}
    with _SOURCE_HEALTH_LOCK:
        snapshot = {k: dict(v) for k, v in _SOURCE_HEALTH.items()}
    # Walk threshold table so sources we know about but never heard from
    # still appear (as 'unknown') — otherwise an Uptime Kuma monitor
    # configured on `sources.rfs.ok` would silently miss "RFS never
    # polled" right after a restart.
    for name, t in _SOURCE_THRESHOLDS.items():
        h = snapshot.get(name, {})
        last_success = h.get('last_success')
        last_error = h.get('last_error')
        consec = int(h.get('consec_fails') or 0)
        success_age = int(now - last_success) if last_success else None
        error_age = int(now - last_error) if last_error else None
        if last_success is None:
            src_status = 'unknown'
            ok = False
        elif success_age >= t['hard']:
            src_status = 'down'
            ok = False
        elif success_age >= t['soft']:
            src_status = 'degraded'
            ok = False
        else:
            src_status = 'ok'
            ok = True
        counts[src_status] += 1
        out[name] = {
            'ok': ok,
            'status': src_status,
            'label': t.get('label', name),
            'last_success_age_secs': success_age,
            'last_error_age_secs': error_age,
            'last_error_msg': h.get('last_error_msg'),
            'consec_fails': consec,
            'total_success': int(h.get('total_success') or 0),
            'total_fail': int(h.get('total_fail') or 0),
            'soft_threshold_secs': t['soft'],
            'hard_threshold_secs': t['hard'],
        }
    return out, counts


@app.route('/api/status')
def status_endpoint():
    """Health endpoint shaped for Uptime Kuma JSON-Query (JSONata) monitors.

    HTTP status reflects critical backend state only:
      - 200 when the backend is functional (DB reachable, writer thread alive)
      - 503 when a critical subsystem is broken

    The JSON body has a finer-grained status of ok | degraded | down plus a
    per-check breakdown and per-source roll-up. The tree is shaped so
    common JSONata expressions stay short:

        status                                      → 'ok' | 'degraded' | 'down'
        checks.database.ok                          → true / false
        checks.database.latency_ms < 200            → boolean
        checks.archive_writer.ok                    → true / false
        checks.waze_ingest.regions_cached >= 150    → boolean
        sources.rfs.ok                              → true / false
        sources.rfs.consec_fails < 5                → boolean
        summary.sources_down                        → number
        summary.sources_down + summary.sources_degraded   → number

    Sources that have never reported are exposed with status='unknown' so
    a "is RFS healthy" monitor doesn't silently pass right after a restart.
    """
    now = time.time()
    checks = {}
    critical_failed = False
    degraded = False

    # ---- 1. Database connectivity + pool occupancy ----------------------
    db_ok = False
    db_latency_ms = None
    db_error = None
    db_t0 = time.time()
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute(f"SET LOCAL statement_timeout = '{STATUS_DB_TIMEOUT_SECS * 1000}ms'")
        c.execute('SELECT 1')
        c.fetchone()
        db_ok = True
        db_latency_ms = int((time.time() - db_t0) * 1000)
    except Exception as e:
        db_error = str(e)[:200]
        critical_failed = True
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass
    pool_info = _db_pool_stats()
    checks['database'] = {
        'ok': db_ok,
        'latency_ms': db_latency_ms,
        'pool_in_use': pool_info.get('in_use'),
        'pool_idle': pool_info.get('idle'),
        'pool_max': pool_info.get('max'),
        'error': db_error,
    }

    # ---- 2. Archive writer heartbeat ------------------------------------
    writer_age = now - _archive_writer_last_flush_at if _archive_writer_last_flush_at else None
    writer_ok = writer_age is not None and writer_age <= STATUS_WRITER_STALE_SECS
    # Boot grace: first ARCHIVE_FLUSH_INTERVAL after start, no heartbeat yet.
    if writer_age is None and (now - _PROCESS_START_TIME) < (ARCHIVE_FLUSH_INTERVAL + 30):
        writer_ok = True
    if not writer_ok:
        critical_failed = True
    checks['archive_writer'] = {
        'ok': writer_ok,
        'last_flush_age_secs': int(writer_age) if writer_age is not None else None,
        'threshold_secs': STATUS_WRITER_STALE_SECS,
        'flush_interval_secs': ARCHIVE_FLUSH_INTERVAL,
    }

    # ---- 3. Archive buffer occupancy ------------------------------------
    with _archive_buffer_lock:
        buf_records = _archive_buffer_records
    buffer_ok = buf_records <= STATUS_BUFFER_WARN_RECORDS
    if not buffer_ok:
        degraded = True
    checks['archive_buffer'] = {
        'ok': buffer_ok,
        'records': buf_records,
        'warn_threshold': STATUS_BUFFER_WARN_RECORDS,
        'hard_cap': _ARCHIVE_BUFFER_MAX_RECORDS,
    }

    # ---- 4. Waze ingest freshness + userscript health -------------------
    if WAZE_INGEST_ENABLED:
        waze_age = now - _waze_last_ingest_at if _waze_last_ingest_at else None
        waze_ok = waze_age is not None and waze_age <= STATUS_WAZE_STALE_SECS
        if waze_age is None and (now - _PROCESS_START_TIME) < 120:
            waze_ok = True
        if not waze_ok:
            degraded = True
        try:
            with _waze_ingest_lock:
                regions_cached = len(_waze_ingest_cache)
        except Exception:
            regions_cached = None
        try:
            metrics = _waze_metrics_snapshot()
            block_rate = metrics.get('block_rate_percent')
            gate_active = bool(metrics.get('gate_active'))
        except Exception:
            block_rate = None
            gate_active = False
        checks['waze_ingest'] = {
            'ok': waze_ok,
            'enabled': True,
            'last_ingest_age_secs': int(waze_age) if waze_age is not None else None,
            'threshold_secs': STATUS_WAZE_STALE_SECS,
            'regions_cached': regions_cached,
            'block_rate_pct': block_rate,
            'gate_active': gate_active,
        }
    else:
        checks['waze_ingest'] = {'ok': True, 'enabled': False}

    # ---- 5. Police heatmap freshness ------------------------------------
    try:
        with _POLICE_HEATMAP_RAM_LOCK:
            hm_updated_at = _POLICE_HEATMAP_RAM.get('updated_at')
            hm_bins = len(_POLICE_HEATMAP_RAM.get('rows') or [])
    except Exception:
        hm_updated_at = None
        hm_bins = 0
    if hm_updated_at is not None:
        try:
            hm_ts = hm_updated_at.timestamp() if hasattr(hm_updated_at, 'timestamp') else float(hm_updated_at)
            hm_age = int(now - hm_ts)
        except Exception:
            hm_age = None
    else:
        hm_age = None
    hm_ok = hm_age is not None and hm_age <= STATUS_HEATMAP_STALE_SECS
    # Boot grace — first refresh runs ~POLICE_HEATMAP_REFRESH_INTERVAL after start.
    if hm_age is None and (now - _PROCESS_START_TIME) < 900:
        hm_ok = True
    if not hm_ok:
        degraded = True
    checks['police_heatmap'] = {
        'ok': hm_ok,
        'bins': hm_bins,
        'last_refresh_age_secs': hm_age,
        'threshold_secs': STATUS_HEATMAP_STALE_SECS,
    }

    # ---- 6. Filter cache freshness --------------------------------------
    fc_age = int(now - _filter_cache_last_refresh_at) if _filter_cache_last_refresh_at else None
    fc_ok = fc_age is not None and fc_age <= STATUS_FILTER_CACHE_STALE_SECS
    if fc_age is None and (now - _PROCESS_START_TIME) < (FILTER_CACHE_REFRESH_INTERVAL + 60):
        fc_ok = True
    if not fc_ok:
        degraded = True
    checks['filter_cache'] = {
        'ok': fc_ok,
        'last_refresh_age_secs': fc_age,
        'threshold_secs': STATUS_FILTER_CACHE_STALE_SECS,
        'refresh_interval_secs': FILTER_CACHE_REFRESH_INTERVAL,
    }

    # ---- 7. RAM cache layer hit rate (informational, no ok/fail) --------
    try:
        ram_hits = int(_CACHE_GET_RAM_HITS)
        ram_misses = int(_CACHE_GET_RAM_MISSES)
    except Exception:
        ram_hits = ram_misses = 0
    total_cache_lookups = ram_hits + ram_misses
    hit_rate_pct = round(ram_hits / total_cache_lookups * 100.0, 2) if total_cache_lookups else None
    checks['ram_cache'] = {
        'hits': ram_hits,
        'misses': ram_misses,
        'hit_rate_pct': hit_rate_pct,
    }

    # ---- 8. Per-source rollup ------------------------------------------
    sources, source_counts = _status_source_summary(now)
    if source_counts['down'] > 0:
        degraded = True  # source-level outage doesn't 503 the backend
    elif source_counts['degraded'] > 0:
        degraded = True

    # ---- Summary aggregate ---------------------------------------------
    failed_checks = sum(
        1 for k, v in checks.items()
        if isinstance(v, dict) and v.get('ok') is False
    )
    summary = {
        'checks_failed': failed_checks,
        'sources_total': len(_SOURCE_THRESHOLDS),
        'sources_ok': source_counts['ok'],
        'sources_degraded': source_counts['degraded'],
        'sources_down': source_counts['down'],
        'sources_unknown': source_counts['unknown'],
    }

    if critical_failed:
        overall = 'down'
        http_code = 503
    elif degraded:
        overall = 'degraded'
        http_code = 200
    else:
        overall = 'ok'
        http_code = 200

    return jsonify({
        'status': overall,
        'uptime_secs': int(now - _PROCESS_START_TIME),
        'started_at': int(_PROCESS_START_TIME),
        'now': int(now),
        'mode': 'dev' if DEV_MODE else 'production',
        'summary': summary,
        'checks': checks,
        'sources': sources,
    }), http_code

@app.route('/api/cache/clear')
def clear_cache():
    """Clear all cached data"""
    cache.clear()
    return jsonify({'status': 'ok', 'message': 'Cache cleared'})

@app.route('/')
def index():
    return jsonify({
        'name': 'NSW PSN External API Proxy',
        'version': '2.2',
        'mode': 'dev' if DEV_MODE else 'production',
        'features': ['API Proxy', 'Data Archival', 'Historical Stats', 'Filtered/Raw Endpoints'],
        'usage': {
            'production': 'python external_api_proxy.py',
            'dev_mode': 'python external_api_proxy.py --dev',
            'pm2_prod': 'pm2 start external_api_proxy.py --interpreter python3',
            'pm2_dev': 'pm2 start external_api_proxy.py --interpreter python3 -- --dev',
            'env_var': 'DEV_MODE=true python external_api_proxy.py'
        },
        'endpoints': {
            'power': {
                'ausgrid': [
                    '/api/ausgrid/outages',
                    '/api/ausgrid/stats'
                ],
                'endeavour': [
                    '/api/endeavour/current',
                    '/api/endeavour/current/raw',
                    '/api/endeavour/current/all',
                    '/api/endeavour/future',
                    '/api/endeavour/future/raw',
                    '/api/endeavour/future/all',
                    '/api/endeavour/postcodes'
                ]
            },
            'traffic': [
                '/api/traffic/incidents',
                '/api/traffic/incidents/raw',
                '/api/traffic/roadwork',
                '/api/traffic/roadwork/raw',
                '/api/traffic/flood',
                '/api/traffic/flood/raw',
                '/api/traffic/fire',
                '/api/traffic/fire/raw',
                '/api/traffic/majorevent',
                '/api/traffic/majorevent/raw',
                '/api/traffic/lga-incidents',
                '/api/traffic/cameras',
                '/api/traffic/all-feeds'
            ],
            'aviation': [
                '/api/aviation/cameras',
                '/api/aviation/cameras/<airport_name>'
            ],
            'waze': [
                '/api/waze/alerts',
                '/api/waze/hazards',
                '/api/waze/police',
                '/api/waze/roadwork',
                '/api/waze/raw'
            ],
            'emergency': [
                '/api/rfs/incidents',
                '/api/rfs/incidents/raw',
                '/api/rfs/fdr'
            ],
            'weather': [
                '/api/bom/warnings',
                '/api/weather/current',
                '/api/weather/radar'
            ],
            'environment': [
                '/api/beachwatch',
                '/api/beachsafe'
            ],
            'news': [
                '/api/news/rss',
                '/api/news/sources'
            ],
            'stats': [
                '/api/stats/summary',
                '/api/stats/history?hours=N',
                '/api/stats/archive/status',
                '/api/stats/archive/trigger'
            ],
            'system': [
                '/api/health',
                '/api/cache/clear',
                '/api/config',
                '/api/heartbeat',
                '/api/collection/status'
            ],
            'debug': [
                '/api/debug/test-all',
                '/api/debug/traffic-raw',
                '/api/debug/sessions',
                '/api/debug/heartbeat-test'
            ]
        }
    })


@app.route('/api/debug/ratelimit')
def debug_ratelimit():
    """Debug endpoint to view rate limit status"""
    client_ip = _get_client_ip()
    now = time.time()
    
    with _rate_limit_lock:
        my_data = _rate_limit_data.get(client_ip, {})
        all_limited = []
        for ip, data in _rate_limit_data.items():
            if data['count'] >= RATE_LIMIT_REQUESTS:
                all_limited.append({
                    'ip': ip[:10] + '...' if len(ip) > 10 else ip,
                    'count': data['count'],
                    'window_age': int(now - data['window_start'])
                })
    
    return jsonify({
        'your_ip': client_ip,
        'your_requests': my_data.get('count', 0),
        'your_burst_used': my_data.get('burst_used', 0),
        'limit': RATE_LIMIT_REQUESTS,
        'burst_limit': RATE_LIMIT_BURST,
        'window_seconds': RATE_LIMIT_WINDOW,
        'window_remaining': max(0, RATE_LIMIT_WINDOW - (now - my_data.get('window_start', now))) if my_data else RATE_LIMIT_WINDOW,
        'is_limited': my_data.get('count', 0) >= RATE_LIMIT_REQUESTS,
        'total_tracked_ips': len(_rate_limit_data),
        'currently_limited': all_limited
    })


@app.route('/api/debug/sessions')
def debug_sessions():
    """Debug endpoint to view active page sessions (requires API key)."""
    active_count = get_active_page_count()  # This also cleans stale sessions
    data_count = get_data_page_count()

    sessions_detail = []
    now = time.time()
    with _page_sessions_lock:
        snap = list(active_page_sessions.items())
    for page_id, session in snap:
        sessions_detail.append({
            'page_id': page_id,
            'page_type': session.get('page_type', 'unknown'),
            'is_data_page': session.get('is_data_page', False),
            'ip': session['ip'],
            'user_agent': session['user_agent'][:50] + '...' if len(session.get('user_agent', '')) > 50 else session.get('user_agent', ''),
            'last_seen_seconds_ago': int(now - session['last_seen']),
            'session_age_seconds': int(now - session.get('opened_at', session['last_seen']))
        })
    
    return jsonify({
        'active_count': active_count,
        'data_page_count': data_count,
        'session_timeout_seconds': PAGE_SESSION_TIMEOUT,
        'heartbeat_timeout_seconds': HEARTBEAT_TIMEOUT,
        'is_page_active': is_page_active(),
        'collection_mode': 'active' if is_page_active() else 'idle',
        'current_interval_seconds': get_current_interval(),
        'last_heartbeat': datetime.fromtimestamp(last_heartbeat).isoformat() if last_heartbeat > 0 else None,
        'sessions': sessions_detail
    })


@app.route('/api/debug/heartbeat-test')
def debug_heartbeat_test():
    """Debug endpoint to test heartbeat parameters - shows exactly what values are received"""
    action = request.args.get('action', 'ping')
    page_id = request.args.get('page_id', '')
    page_type = request.args.get('page_type', 'unknown')
    data_page_raw = request.args.get('data_page', 'false')
    is_data_page = data_page_raw.lower() in ('true', '1', 'yes')
    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    user_agent = request.headers.get('User-Agent', 'unknown')[:100]
    
    # Show what would happen
    short_id = page_id[-6:] if len(page_id) > 6 else page_id
    session_exists = page_id in active_page_sessions if page_id else False
    
    return jsonify({
        'received_params': {
            'action': action,
            'page_id': page_id,
            'page_id_short': short_id,
            'page_type': page_type,
            'data_page_raw': data_page_raw,
            'is_data_page_parsed': is_data_page,
        },
        'headers': {
            'client_ip': client_ip,
            'user_agent': user_agent
        },
        'session_info': {
            'session_exists': session_exists,
            'current_session': active_page_sessions.get(page_id, None) if page_id else None,
            'total_sessions': len(active_page_sessions),
            'data_sessions': get_data_page_count()
        },
        'state': {
            'is_page_active': is_page_active(),
            'collection_mode': 'active' if is_page_active() else 'idle',
            'current_interval': get_current_interval(),
            'last_heartbeat': datetime.fromtimestamp(last_heartbeat).isoformat() if last_heartbeat > 0 else None
        },
        'test_result': 'OK - Use /api/heartbeat to actually register a heartbeat'
    })


@app.route('/api/debug/traffic-raw')
def debug_traffic_raw():
    """Debug endpoint to see raw traffic incident data structure"""
    try:
        r = requests.get('https://www.livetraffic.com/traffic/hazards/incident.json', 
                        timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            data = r.json()
            features = data.get('features', []) if isinstance(data, dict) else data
            
            # Get first 5 incidents with all their fields
            sample = []
            for i, f in enumerate(features[:5]):
                props = f.get('properties', f)
                sample.append({
                    'index': i,
                    'all_keys': list(f.keys()),
                    'properties_keys': list(props.keys()) if isinstance(props, dict) else 'N/A',
                    'mainCategory': props.get('mainCategory') or f.get('mainCategory'),
                    'subCategory': props.get('subCategory') or f.get('subCategory'),
                    'headline': props.get('headline') or f.get('headline'),
                    'displayName': props.get('displayName') or f.get('displayName'),
                    'incidentKind': props.get('incidentKind') or f.get('incidentKind'),
                    'type': props.get('type') or f.get('type'),
                    'full_properties': props
                })
            
            return jsonify({
                'total_count': len(features),
                'is_list': isinstance(data, list),
                'has_features': 'features' in data if isinstance(data, dict) else False,
                'sample_incidents': sample
            })
        return jsonify({'error': f'Status {r.status_code}'}), 502
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/cache/status')
@app.route('/api/cache/stats')  # Alias
def cache_status():
    """Debug endpoint to view persistent cache status"""
    entries = cache_stats()
    
    # Calculate totals
    fresh_count = sum(1 for e in entries if e.get('status') == 'fresh')
    stale_count = sum(1 for e in entries if e.get('status') == 'stale')
    total_fetch_time = sum(e.get('fetch_time_ms', 0) for e in entries)
    
    return jsonify({
        'prewarm_running': _prewarm_running,
        'total_endpoints': len(entries),
        'fresh_count': fresh_count,
        'stale_count': stale_count,
        'total_fetch_time_ms': total_fetch_time,
        'db_path': DB_PATH_CACHE,
        'entries': entries
    })


# /api/data/history count cache. The plain COUNT(*) over data_history with
# is_latest=1 + 24h fetched_at filter can take 15-30s when archive UPDATEs
# leave the visibility map dirty (Postgres falls back to heap fetches even
# for an index-only scan). Cache the result by (where_clause, params) so
# repeat polls — same filters, different page — return instantly, and a
# slow scan failing doesn't zero out pagination on every refresh.
_DATA_HISTORY_COUNT_CACHE = {}        # key -> {'total','live','ended','ts'}
_DATA_HISTORY_COUNT_CACHE_LOCK = threading.Lock()
_DATA_HISTORY_COUNT_CACHE_TTL = 600   # 10 min — long window so we don't retry
                                       # the slow scan every minute. Counts
                                       # only need to be roughly right for
                                       # pagination; precision isn't critical.
_DATA_HISTORY_COUNT_CACHE_MAX = 256   # cap entries to bound memory


def _data_history_count_cache_get(key, allow_stale=False):
    with _DATA_HISTORY_COUNT_CACHE_LOCK:
        entry = _DATA_HISTORY_COUNT_CACHE.get(key)
    if entry is None:
        return None
    age = time.time() - entry['ts']
    if not allow_stale and age >= _DATA_HISTORY_COUNT_CACHE_TTL:
        return None
    return entry


def _data_history_count_cache_set(key, total, live, ended):
    with _DATA_HISTORY_COUNT_CACHE_LOCK:
        if len(_DATA_HISTORY_COUNT_CACHE) >= _DATA_HISTORY_COUNT_CACHE_MAX:
            # Drop the oldest entry — bound memory in case of high cardinality.
            oldest_key = min(_DATA_HISTORY_COUNT_CACHE,
                             key=lambda k: _DATA_HISTORY_COUNT_CACHE[k]['ts'])
            _DATA_HISTORY_COUNT_CACHE.pop(oldest_key, None)
        _DATA_HISTORY_COUNT_CACHE[key] = {
            'total': total, 'live': live, 'ended': ended, 'ts': time.time(),
        }


# Track in-flight async refreshes per cache key so the same query
# isn't re-issued repeatedly when many users land on the page at once.
_DATA_HISTORY_COUNT_INFLIGHT = set()
_DATA_HISTORY_COUNT_INFLIGHT_LOCK = threading.Lock()


# Cap concurrent refresh workers. Each runs a 90s COUNT(*) and holds
# a pooled connection — without this cap, a traffic spike can spawn
# dozens of parallel refreshes and exhaust the connection pool.
_DATA_HISTORY_COUNT_INFLIGHT_MAX = 4


def _async_refresh_data_history_count(cache_key, actual_where, params,
                                       live_only, historical_only, pager_cutoff):
    """Kick off a background thread that runs the slow COUNT(*) and
    populates the cache. The user request returns immediately with the
    stale or estimated value — refresh is fire-and-forget."""
    with _DATA_HISTORY_COUNT_INFLIGHT_LOCK:
        if cache_key in _DATA_HISTORY_COUNT_INFLIGHT:
            return  # another worker is already on it
        if len(_DATA_HISTORY_COUNT_INFLIGHT) >= _DATA_HISTORY_COUNT_INFLIGHT_MAX:
            return  # too many workers already running — don't pile on
        _DATA_HISTORY_COUNT_INFLIGHT.add(cache_key)

    def _worker():
        try:
            total = 0
            live = 0
            ended = 0
            try:
                conn = get_conn()
                try:
                    c = conn.cursor()
                    c.execute("SET LOCAL statement_timeout = '90s'")
                    c.execute(f'SELECT COUNT(*) FROM data_history WHERE {actual_where}', params)
                    total = c.fetchone()[0] or 0
                finally:
                    conn.close()
            except Exception as e:
                if DEV_MODE:
                    Log.info(f"Async count refresh skipped (still bloated): {e}")
                return  # leave the cache as-is
            if live_only:
                live = total
            elif historical_only:
                ended = total
            else:
                try:
                    conn = get_conn()
                    try:
                        c = conn.cursor()
                        c.execute("SET LOCAL statement_timeout = '90s'")
                        c.execute(f'''
                            SELECT
                                COUNT(*) FILTER (WHERE
                                    (source = 'pager' AND COALESCE(source_timestamp_unix, fetched_at) >= {pager_cutoff})
                                    OR (source != 'pager' AND is_live = 1)
                                ),
                                COUNT(*) FILTER (WHERE
                                    (source = 'pager' AND COALESCE(source_timestamp_unix, fetched_at) < {pager_cutoff})
                                    OR (source != 'pager' AND (is_live = 0 OR is_live IS NULL))
                                )
                            FROM data_history WHERE {actual_where}
                        ''', params)
                        row = c.fetchone()
                        live = row[0] or 0
                        ended = row[1] or 0
                    finally:
                        conn.close()
                except Exception:
                    pass  # keep the total; breakdown stays at 0
            _data_history_count_cache_set(cache_key, total, live, ended)
        finally:
            with _DATA_HISTORY_COUNT_INFLIGHT_LOCK:
                _DATA_HISTORY_COUNT_INFLIGHT.discard(cache_key)

    threading.Thread(target=_worker, daemon=True, name='count-refresh').start()


def _data_history_reltuples_estimate():
    """Instant row-count estimate from the planner's stats. Returns the
    pg_class.reltuples value — refreshed by ANALYZE / autovacuum, so it
    drifts a bit but is always near-correct for a high-churn table.
    Used as a fallback when a real COUNT(*) times out."""
    try:
        conn = get_conn()
        try:
            c = conn.cursor()
            c.execute("SET LOCAL statement_timeout = '2s'")
            c.execute("SELECT reltuples::bigint FROM pg_class WHERE relname = 'data_history'")
            row = c.fetchone()
            return int(row[0]) if row and row[0] else 0
        finally:
            conn.close()
    except Exception:
        return 0


def _prewarm_data_history_count_cache():
    """Seed the count cache for the logs-page default query (hours=24,
    unique=1, no other filters). Strategy: prefer the instant pg_class
    reltuples estimate so the cache is hot in milliseconds, then try
    a real COUNT(*) for accuracy — if that times out we keep the
    estimate. Either way the user's first click never sees total=0."""
    time.sleep(15)
    cutoff = int(time.time()) - 24 * 3600
    actual_where = "(fetched_at >= %s) AND is_latest = 1"
    params = [cutoff]
    # Match the quantization done at the request site so the prewarmed
    # entry actually matches a real user query's cache_key.
    cache_key = (actual_where, ((cutoff // 60) * 60,), 'all')
    pager_cutoff = int(time.time()) - 3600

    # Phase 1: instant estimate. Uses reltuples for the whole table —
    # an over-estimate vs. is_latest=1 + 24h, but better than zero and
    # gives pagination something to chew on. Real count overwrites later.
    est = _data_history_reltuples_estimate()
    if est > 0:
        _data_history_count_cache_set(cache_key, est, 0, 0)
        if DEV_MODE:
            Log.startup(f"data/history count cache seeded with reltuples estimate — ~{est}")

    # Phase 2: try the real count. May still time out under heavy load —
    # if so we keep the estimate from phase 1.
    try:
        conn = get_conn()
        try:
            c = conn.cursor()
            c.execute("SET LOCAL statement_timeout = '90s'")
            c.execute(f'SELECT COUNT(*) FROM data_history WHERE {actual_where}', params)
            total = c.fetchone()[0] or 0
            c.execute(f'''
                SELECT
                    COUNT(*) FILTER (WHERE
                        (source = 'pager' AND COALESCE(source_timestamp_unix, fetched_at) >= {pager_cutoff})
                        OR (source != 'pager' AND is_live = 1)
                    ),
                    COUNT(*) FILTER (WHERE
                        (source = 'pager' AND COALESCE(source_timestamp_unix, fetched_at) < {pager_cutoff})
                        OR (source != 'pager' AND (is_live = 0 OR is_live IS NULL))
                    )
                FROM data_history WHERE {actual_where}
            ''', params)
            row = c.fetchone()
            live, ended = (row[0] or 0), (row[1] or 0)
            _data_history_count_cache_set(cache_key, total, live, ended)
            if DEV_MODE:
                Log.startup(f"data/history count cache prewarmed (exact) — total={total}")
        finally:
            conn.close()
    except Exception as e:
        Log.warn(f"data/history exact count prewarm skipped (estimate retained): {e}")


# --- Cursor pagination helpers --------------------------------------------
# Keyset pagination encodes the position as (fetched_at, id) pairs because
# fetched_at alone is not unique — an archive flush can write thousands of
# rows with the same timestamp. The id tiebreak guarantees a stable forward
# walk; the index `idx_data_latest_fetched_id` lets the seek stay O(log N).

# Beyond this offset, /api/data/history rejects the request with a 400 and
# directs the caller to `cursor`. Without a cap, deep offsets (we've seen
# 245k+ in the wild) force a seq-scan-and-skip that runs until the 25s
# statement_timeout and 500s. 10k is well past any realistic UI use:
# pageSize=20 -> page 500, pageSize=100 -> page 100.
_DATA_HISTORY_MAX_OFFSET = 10000


def _encode_history_cursor(fetched_at, row_id):
    raw = f"{int(fetched_at)}:{int(row_id)}".encode('ascii')
    return base64.urlsafe_b64encode(raw).decode('ascii').rstrip('=')


def _decode_history_cursor(cursor):
    """Returns (fetched_at, id) or None on malformed input."""
    if not cursor:
        return None
    try:
        padded = cursor + '=' * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode('ascii')).decode('ascii')
        fa_str, id_str = raw.split(':', 1)
        return int(fa_str), int(id_str)
    except Exception:
        return None


@app.route('/api/data/history')
def data_history():
    """
    Query historical data from history databases (split by source type).

    Query parameters:
        source: Filter by source (rfs, traffic_incident, waze_hazard, waze_police, etc.) - supports comma-separated
        source_id: Filter by specific source ID
        category: Filter by category (e.g., CRASH, HAZARD, Advice) - supports comma-separated
        subcategory: Filter by subcategory (e.g., POLICE_VISIBLE, POLICE_HIDING) - supports comma-separated
        status: Filter by status - supports comma-separated
        severity: Filter by severity - supports comma-separated
        
        Time filters (multiple options - use whichever is convenient):
        since: Unix timestamp - get records after this time
        until: Unix timestamp - get records before this time
        date_from: ISO date/datetime string (e.g., '2024-01-15' or '2024-01-15T09:00:00')
        date_to: ISO date/datetime string (e.g., '2024-01-15' or '2024-01-15T18:00:00')
        hours: Get records from last N hours (e.g., hours=24)
        days: Get records from last N days (e.g., days=7)
        today: If '1', get today's records only
        
        since_source: Filter by source timestamp (Unix)
        until_source: Filter by source timestamp (Unix)
        limit: Max records to return (default 100, max 1000)
        offset: Pagination offset
        active_only: If '1', only return records marked active by source
        live_only: If '1', only return live incidents (still in API responses)
        historical_only: If '1', only return historical incidents (no longer in API)
        search: Full-text search in title and location
        title: Search in title only
        location: Search in location only
        lat/lon/radius: Geo-filter (lat, lon, radius in km)
        order: 'asc' or 'desc' (default desc)
        unique: If '1', return only latest snapshot per source_id
        
    Response fields:
        is_active: True if source reports incident as active (from source data)
        is_live: True if incident is still appearing in API responses (live)
        last_seen: Unix timestamp when incident was last seen in API response
        last_seen_iso: ISO format of last_seen
        
    Examples:
        /api/data/history?source=waze_police&hours=24
        /api/data/history?source=waze_police&subcategory=POLICE_VISIBLE
        /api/data/history?source=rfs&date_from=2024-01-15&date_to=2024-01-16
        /api/data/history?search=highway&days=7
        /api/data/history?today=1&source=waze_hazard
    """
    try:
        source = request.args.get('source')
        source_id = request.args.get('source_id')
        category = request.args.get('category')
        subcategory = request.args.get('subcategory')
        status = request.args.get('status')
        severity = request.args.get('severity')
        
        # Time filtering - multiple options for convenience
        since = request.args.get('since', type=int)
        until = request.args.get('until', type=int)
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        hours = request.args.get('hours', type=int)
        days = request.args.get('days', type=int)
        today_only = request.args.get('today') == '1'
        
        # Convert human-readable dates to timestamps
        if date_from and not since:
            try:
                # Support both date and datetime formats
                if 'T' in date_from or ' ' in date_from:
                    dt = datetime.fromisoformat(date_from.replace(' ', 'T'))
                else:
                    dt = datetime.strptime(date_from, '%Y-%m-%d')
                since = int(dt.timestamp())
            except (ValueError, TypeError):
                pass
        
        if date_to and not until:
            try:
                if 'T' in date_to or ' ' in date_to:
                    dt = datetime.fromisoformat(date_to.replace(' ', 'T'))
                else:
                    # End of day for date-only format
                    dt = datetime.strptime(date_to, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
                until = int(dt.timestamp())
            except (ValueError, TypeError):
                pass
        
        # Relative time filters
        if hours and not since:
            since = int(time.time()) - (hours * 3600)
        
        if days and not since:
            since = int(time.time()) - (days * 86400)
        
        if today_only and not since:
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            since = int(today_start.timestamp())
        
        since_source = request.args.get('since_source', type=int)
        until_source = request.args.get('until_source', type=int)
        limit = min(request.args.get('limit', 100, type=int), 1000)
        offset = request.args.get('offset', 0, type=int)
        # Cursor-based pagination is the preferred path — constant time
        # regardless of how deep the client has paged. Offset is still
        # honoured for backwards compatibility, but capped at
        # _DATA_HISTORY_MAX_OFFSET so a runaway client can't grind out a
        # 25s seq-scan.
        cursor_raw = (request.args.get('cursor') or '').strip()
        cursor_decoded = _decode_history_cursor(cursor_raw) if cursor_raw else None
        if cursor_decoded is None and offset > _DATA_HISTORY_MAX_OFFSET:
            return jsonify({
                'error': 'offset_too_large',
                'message': (
                    f'offset={offset} exceeds {_DATA_HISTORY_MAX_OFFSET}. '
                    f'Use ?cursor=<next_cursor from previous response> for '
                    f'forward pagination, or narrow the result set with '
                    f'date_from/hours/source.'
                ),
                'max_offset': _DATA_HISTORY_MAX_OFFSET,
            }), 400
        active_only = request.args.get('active_only') == '1'
        # Include the full `data` JSONB blob only when asked. Skipping it cuts
        # the payload dramatically for list pages that just show title + source.
        include_data = request.args.get('full') == '1'
        search = request.args.get('search')
        title_search = request.args.get('title')
        location_search = request.args.get('location')
        lat = request.args.get('lat', type=float)
        lon = request.args.get('lon', type=float)
        radius_km = request.args.get('radius', 10, type=float)
        order = request.args.get('order', 'desc').lower()
        unique = request.args.get('unique') == '1'
        
        # Determine which database(s) to query based on source filter
        source_list = [s.strip() for s in source.split(',')] if source else None
        dbs_to_query = get_history_dbs_for_sources(source_list)
        
        # Build query conditions (shared across all DB queries)
        def build_conditions():
            conditions = []
            params = []
            
            # Support comma-separated values for multi-select filters
            if source:
                sources = [s.strip() for s in source.split(',')]
                if len(sources) == 1:
                    conditions.append('source = %s')
                    params.append(sources[0])
                else:
                    placeholders = ','.join(['%s' for _ in sources])
                    conditions.append(f'source IN ({placeholders})')
                    params.extend(sources)
            return conditions, params
        
        conditions, params = build_conditions()
        
        # Exclude deprecated sources from results (unless explicitly requested)
        if not source or not any(s.strip() in DEPRECATED_SOURCES for s in source.split(',')):
            if DEPRECATED_SOURCES:
                dep_placeholders = ','.join(['%s' for _ in DEPRECATED_SOURCES])
                conditions.append(f'source NOT IN ({dep_placeholders})')
                params.extend(DEPRECATED_SOURCES)
        
        # Continue building conditions (same for all DBs)
        # Support comma-separated values for multi-select filters
        if source:
            pass  # Already handled above
        
        if source_id:
            conditions.append('source_id = %s')
            params.append(source_id)
        
        if category:
            categories = [c.strip() for c in category.split(',')]
            if len(categories) == 1:
                conditions.append('category = %s')
                params.append(categories[0])
            else:
                placeholders = ','.join(['%s' for _ in categories])
                conditions.append(f'category IN ({placeholders})')
                params.extend(categories)
        
        if subcategory:
            subcats = [s.strip() for s in subcategory.split(',')]
            if len(subcats) == 1:
                conditions.append('subcategory = %s')
                params.append(subcats[0])
            else:
                placeholders = ','.join(['%s' for _ in subcats])
                conditions.append(f'subcategory IN ({placeholders})')
                params.extend(subcats)
        
        if status:
            statuses = [s.strip() for s in status.split(',')]
            if len(statuses) == 1:
                conditions.append('status = %s')
                params.append(statuses[0])
            else:
                placeholders = ','.join(['%s' for _ in statuses])
                conditions.append(f'status IN ({placeholders})')
                params.extend(statuses)
        
        if severity:
            severities = [s.strip() for s in severity.split(',')]
            if len(severities) == 1:
                conditions.append('severity = %s')
                params.append(severities[0])
            else:
                placeholders = ','.join(['%s' for _ in severities])
                conditions.append(f'severity IN ({placeholders})')
                params.extend(severities)
        
        if since:
            conditions.append('fetched_at >= %s')
            params.append(since)
        if until:
            conditions.append('fetched_at <= %s')
            params.append(until)
        if since_source:
            conditions.append('source_timestamp_unix >= %s')
            params.append(since_source)
        if until_source:
            conditions.append('source_timestamp_unix <= %s')
            params.append(until_source)
        if active_only:
            conditions.append('is_active = 1')
        
        # Filter by is_live (still in API responses)
        live_only = request.args.get('live_only') == '1'
        historical_only = request.args.get('historical_only') == '1'
        if live_only:
            conditions.append('is_live = 1')
        elif historical_only:
            conditions.append('is_live = 0')
        
        # Text search options
        if search:
            conditions.append('(title LIKE %s OR location_text LIKE %s)')
            params.extend([f'%{search}%', f'%{search}%'])
        if title_search:
            conditions.append('title LIKE %s')
            params.append(f'%{title_search}%')
        if location_search:
            conditions.append('location_text LIKE %s')
            params.append(f'%{location_search}%')
        
        # Simple geo-filter using bounding box (approximate)
        if lat is not None and lon is not None:
            # ~111km per degree of latitude, ~111*cos(lat) for longitude
            lat_delta = radius_km / 111.0
            lon_delta = radius_km / (111.0 * abs(cos(radians(lat))))
            conditions.append('latitude BETWEEN %s AND %s')
            conditions.append('longitude BETWEEN %s AND %s')
            params.extend([lat - lat_delta, lat + lat_delta])
            params.extend([lon - lon_delta, lon + lon_delta])
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        order_dir = 'ASC' if order == 'asc' else 'DESC'
        pager_cutoff = int(time.time()) - 3600  # 1 hour ago for pager live check
        
        # Modify WHERE clause for unique mode
        actual_where = f'({where_clause}) AND is_latest = 1' if unique else where_clause
        
        # Aggregate counts across all relevant databases
        total = 0
        live_count = 0
        ended_count = 0
        all_rows = []

        # Two-step strategy: cheap plain COUNT(*) first (uses the partial
        # idx_data_latest_only_fetched index when unique=1), then optional
        # live/ended breakdown. The breakdown's FILTER expressions can't use
        # an index and force per-row CPU work; if it times out we still keep
        # the pagination total instead of returning zero.
        # TTL-cached per (where, params, mode) so repeat polls don't hit
        # the DB and a slow scan doesn't break pagination on every refresh.
        # Quantize timestamp-shaped params (anything > 1 billion seconds)
        # to 60s buckets so back-to-back requests for ?hours=24 share a
        # cache key — without this `since` shifts every second and every
        # request misses the cache.
        def _ck_bucket(p):
            if isinstance(p, int) and p > 1_000_000_000:
                return (p // 60) * 60
            return p
        cache_key = (
            actual_where,
            tuple(_ck_bucket(p) for p in params),
            'live' if live_only else ('hist' if historical_only else 'all'),
        )
        # Stale-while-revalidate: the request NEVER blocks on the slow
        # count. We always serve the cached value (or a reltuples estimate
        # if there's no cache yet), and if the cache is stale we kick off
        # a background refresh. Pagination never stutters even if the DB
        # is too bloated to count in 40s.
        cached_fresh = _data_history_count_cache_get(cache_key)
        cached_stale = _data_history_count_cache_get(cache_key, allow_stale=True)
        if cached_fresh is not None:
            total = cached_fresh['total']
            live_count = cached_fresh['live']
            ended_count = cached_fresh['ended']
        elif cached_stale is not None:
            total = cached_stale['total']
            live_count = cached_stale['live']
            ended_count = cached_stale['ended']
            _async_refresh_data_history_count(
                cache_key, actual_where, list(params), live_only, historical_only, pager_cutoff)
        else:
            total = _data_history_reltuples_estimate()
            _async_refresh_data_history_count(
                cache_key, actual_where, list(params), live_only, historical_only, pager_cutoff)

        # For single-DB queries, use OFFSET directly. For multi-DB, need to fetch more and sort in memory.
        # `data` is the heaviest column — pull it only if ?full=1.
        data_col = 'data' if include_data else "''::text AS data"

        # Cursor seek clause. The (fetched_at, id) row-value comparison
        # gives a stable forward walk: rows with fetched_at strictly past
        # the cursor's, plus rows with the same fetched_at and an id past
        # the cursor's id. Order direction flips the comparator. When a
        # cursor is supplied we ignore offset entirely — keyset and
        # offset don't compose, and the cursor is already the position.
        seek_clause = ''
        seek_params = []
        if cursor_decoded is not None:
            cursor_fa, cursor_id = cursor_decoded
            cmp = '<' if order_dir == 'DESC' else '>'
            seek_clause = f' AND (fetched_at {cmp} %s OR (fetched_at = %s AND id {cmp} %s))'
            seek_params = [cursor_fa, cursor_fa, cursor_id]
        effective_offset = 0 if cursor_decoded is not None else offset

        if len(dbs_to_query) == 1:
            # Single DB - simple query with proper pagination
            conn = get_conn()
            try:
                c = conn.cursor()
                c.execute("SET LOCAL statement_timeout = '25s'")
                c.execute(f'''
                    SELECT id, source, source_id, fetched_at, source_timestamp, source_timestamp_unix,
                           latitude, longitude, location_text, title, category, subcategory,
                           status, severity, {data_col}, is_active, is_live, last_seen
                    FROM data_history
                    WHERE {actual_where}{seek_clause}
                    ORDER BY fetched_at {order_dir}, id {order_dir}
                    LIMIT %s OFFSET %s
                ''', params + seek_params + [limit, effective_offset])
                all_rows = c.fetchall()
            finally:
                conn.close()
        else:
            # Multi-DB query - fetch from each, merge and paginate in memory
            # This is less efficient but necessary for cross-DB queries
            for db_path in dbs_to_query:
                try:
                    conn = get_conn()
                    try:
                        c = conn.cursor()
                        # Fetch enough records for pagination (offset + limit)
                        c.execute(f'''
                            SELECT id, source, source_id, fetched_at, source_timestamp, source_timestamp_unix,
                                   latitude, longitude, location_text, title, category, subcategory,
                                   status, severity, {data_col}, is_active, is_live, last_seen
                            FROM data_history
                            WHERE {actual_where}{seek_clause}
                            ORDER BY fetched_at {order_dir}, id {order_dir}
                            LIMIT %s
                        ''', params + seek_params + [effective_offset + limit])
                        all_rows.extend(c.fetchall())
                    finally:
                        conn.close()
                except Exception as e:
                    Log.error(f"Query error: {e}")

            # Sort merged results and apply pagination
            reverse = (order_dir == 'DESC')
            all_rows.sort(key=lambda r: (r[3] or 0, r[0] or 0), reverse=reverse)
            all_rows = all_rows[effective_offset:effective_offset + limit]
        
        records = []
        for row in all_rows:
            records.append({
                'id': row[0],
                'source': row[1],
                'source_id': row[2],
                'fetched_at': row[3],
                'fetched_at_iso': datetime.fromtimestamp(row[3]).isoformat() if row[3] else None,
                'source_timestamp': row[4],
                'source_timestamp_unix': row[5],
                'latitude': row[6],
                'longitude': row[7],
                'location_text': row[8],
                'title': row[9],
                'category': row[10],
                'subcategory': row[11],
                'status': row[12],
                'severity': row[13],
                # `data` is empty when ?full=1 wasn't passed (list view).
                'data': (json.loads(row[14]) if row[14] else {}) if include_data else {},
                'is_active': row[15] == 1,
                'is_live': row[16] == 1 if row[16] is not None else True,
                'last_seen': row[17],
                'last_seen_iso': datetime.fromtimestamp(row[17]).isoformat() if row[17] else None
            })
        
        # Build query info for debugging/display
        query_info = {
            'filters_applied': {}
        }
        if source:
            query_info['filters_applied']['source'] = source.split(',')
        if category:
            query_info['filters_applied']['category'] = category.split(',')
        if subcategory:
            query_info['filters_applied']['subcategory'] = subcategory.split(',')
        if status:
            query_info['filters_applied']['status'] = status.split(',')
        if severity:
            query_info['filters_applied']['severity'] = severity.split(',')
        if search:
            query_info['filters_applied']['search'] = search
        if since:
            query_info['filters_applied']['since'] = datetime.fromtimestamp(since).isoformat()
        if until:
            query_info['filters_applied']['until'] = datetime.fromtimestamp(until).isoformat()
        if live_only:
            query_info['filters_applied']['live_only'] = True
        if historical_only:
            query_info['filters_applied']['historical_only'] = True
        if unique:
            query_info['filters_applied']['unique'] = True
        
        # Forward cursor for the next page. Only emit if the response is
        # full — a short page means we've hit the end of the result set.
        # Clients page sequentially by passing this back as ?cursor=…,
        # which avoids the deep-offset performance cliff entirely.
        next_cursor = None
        if len(all_rows) >= limit:
            last = all_rows[-1]
            next_cursor = _encode_history_cursor(last[3] or 0, last[0] or 0)

        return jsonify({
            'total': total,
            'live_count': live_count,
            'ended_count': ended_count,
            'limit': limit,
            'offset': offset,
            'count': len(records),
            'records': records,
            'next_cursor': next_cursor,
            'query': query_info
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/data/history/stats')
def data_history_stats():
    """Get statistics about stored historical data"""
    stats = get_data_history_stats()
    return jsonify(stats)


@app.route('/api/data/history/sources')
def data_history_sources():
    """Get list of all data sources and their record counts (all history databases)"""
    try:
        # Aggregate from all history databases
        source_data = {}  # source -> {count, oldest, newest}
        
        for db_path in ALL_HISTORY_DBS:
            conn = None
            try:
                conn = get_conn()
                c = conn.cursor()
                c.execute('''
                    SELECT source, COUNT(*), MIN(fetched_at), MAX(fetched_at)
                    FROM data_history
                    GROUP BY source
                ''')
                for row in c.fetchall():
                    source_name = row[0]
                    if source_name in DEPRECATED_SOURCES:
                        continue
                    if source_name not in source_data:
                        source_data[source_name] = {'count': 0, 'oldest': None, 'newest': None}
                    source_data[source_name]['count'] += row[1]
                    if row[2] and (source_data[source_name]['oldest'] is None or row[2] < source_data[source_name]['oldest']):
                        source_data[source_name]['oldest'] = row[2]
                    if row[3] and (source_data[source_name]['newest'] is None or row[3] > source_data[source_name]['newest']):
                        source_data[source_name]['newest'] = row[3]
            except Exception as e:
                Log.error(f"Sources error: {e}")
            finally:
                if conn is not None:
                    try: conn.close()
                    except Exception: pass
        
        # Sort by count descending
        sources = []
        for source_name, data in sorted(source_data.items(), key=lambda x: x[1]['count'], reverse=True):
            sources.append({
                'source': source_name,
                'count': data['count'],
                'oldest': datetime.fromtimestamp(data['oldest']).isoformat() if data['oldest'] else None,
                'newest': datetime.fromtimestamp(data['newest']).isoformat() if data['newest'] else None
            })
        
        return jsonify({'sources': sources})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/pager/hits')
def pager_hits():
    """
    Get pager hits for map display.
    Optimized endpoint for map pages - simpler than full history query.
    
    Query parameters:
        hours: Lookback window in hours (default 24, max 168/7 days)
        capcode: Filter by specific capcode
        incident_id: Filter by incident ID
        limit: Max results (default 500, max 2000)
        
    Returns GeoJSON-like format for easy map integration.
    """
    try:
        hours = min(request.args.get('hours', 24, type=int), 168)  # Max 7 days
        capcode = request.args.get('capcode')
        incident_id = request.args.get('incident_id')
        limit = min(request.args.get('limit', 500, type=int), 2000)
        
        cutoff = int(time.time()) - (hours * 3600)
        
        conn = get_conn()
        try:
            c = conn.cursor()
            
            # Build query - filter by source_timestamp_unix (actual pager incident time), not fetched_at
            conditions = ['source = %s', 'source_timestamp_unix >= %s']
            params = ['pager', cutoff]
            
            if capcode:
                conditions.append('subcategory = %s')  # capcode stored in subcategory
                params.append(capcode)
            
            if incident_id:
                conditions.append("(data::json)->>'incident_id' = %s")
                params.append(incident_id)
            
            where_clause = ' AND '.join(conditions)
            
            # Get unique pager hits (latest per source_id)
            c.execute(f'''
                SELECT source_id, latitude, longitude, title, category, subcategory,
                       source_timestamp, data, fetched_at, is_live
                FROM data_history d1
                WHERE {where_clause}
                  AND fetched_at = (
                      SELECT MAX(fetched_at) FROM data_history d2 
                      WHERE d2.source = 'pager' AND d2.source_id = d1.source_id
                  )
                ORDER BY fetched_at DESC
                LIMIT %s
            ''', params + [limit])
            
            rows = c.fetchall()
        finally:
            conn.close()
        
        # Format as GeoJSON-like features for map
        features = []
        for row in rows:
            source_id, lat, lon, title, category, subcategory, source_ts, data_json, fetched_at, is_live = row
            
            if lat is None or lon is None:
                continue
            
            try:
                data = json.loads(data_json) if data_json else {}
            except (json.JSONDecodeError, ValueError, TypeError):
                data = {}
            
            features.append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lon, lat]
                },
                'properties': {
                    'id': source_id,
                    'pager_msg_id': data.get('id'),
                    'incident_id': data.get('incident_id'),
                    'capcode': subcategory or data.get('capcode'),
                    'alias': title or data.get('alias'),
                    'agency': category or data.get('agency'),
                    'message': data.get('message', ''),
                    'incident_time': source_ts or data.get('incident_time'),
                    'fetched_at': fetched_at,
                    'is_live': is_live == 1,
                    'lat': lat,
                    'lon': lon
                }
            })
        
        return jsonify({
            'type': 'FeatureCollection',
            'features': features,
            'count': len(features),
            'hours': hours
        })
    except Exception as e:
        Log.error(f"Pager hits error: {e}")
        return jsonify({'error': str(e)}), 500


# =========================================================================
# Filter-cache refresh: rebuild data_history_filter_cache from is_latest=1
# rows so /api/data/history/filters never has to GROUPING-SETS data_history.
# Refresh runs on a 5-min timer in a background thread. Idempotent + atomic.
# =========================================================================

FILTER_CACHE_REFRESH_INTERVAL = int(os.environ.get('FILTER_CACHE_REFRESH_INTERVAL', 300))
# Wall-clock of the last successful filter-cache refresh, exposed by
# /api/status. 0.0 means "never".
_filter_cache_last_refresh_at = 0.0


def _refresh_filter_cache():
    """Rebuild data_history_filter_cache from is_latest=1 rows.

    Does 5 small GROUP BY queries (one per dimension), collects results,
    and atomically swaps the cache contents in a single transaction. The
    whole thing is usually <1s because is_latest=1 is a tiny fraction of
    data_history.
    """
    try:
        deprecated = list(DEPRECATED_SOURCES) or ['__nothing__']
        dep_ph = ','.join(['%s'] * len(deprecated))

        rows_to_insert = []  # (kind, source, value, count)

        conn = get_conn()
        try:
            c = conn.cursor()
            # Background task — give it a generous budget. The 5 GROUP BYs
            # over is_latest=1 rows can run long when archive UPDATEs leave
            # the visibility map dirty; failing the refresh just leaves the
            # filter dropdown stale, so it's worth waiting longer rather
            # than hammering retries.
            c.execute("SET LOCAL statement_timeout = '180s'")

            # Sources (no source scope — top-level list)
            c.execute(f'''
                SELECT source, COUNT(*) FROM data_history
                WHERE is_latest = 1 AND source NOT IN ({dep_ph})
                GROUP BY source
            ''', deprecated)
            for src, cnt in c.fetchall():
                if src:
                    rows_to_insert.append(('source', '', src, int(cnt)))

            # Each dimension, scoped by source
            for kind in ('category', 'subcategory', 'status', 'severity'):
                c.execute(f'''
                    SELECT source, {kind}, COUNT(*) FROM data_history
                    WHERE is_latest = 1
                      AND source NOT IN ({dep_ph})
                      AND {kind} IS NOT NULL
                      AND {kind} <> ''
                    GROUP BY source, {kind}
                ''', deprecated)
                for src, val, cnt in c.fetchall():
                    if not (src and val):
                        continue
                    val_s = str(val).strip()
                    if not val_s:
                        continue
                    # Pager stores its capcode in subcategory — pure-numeric
                    # strings like "1160008", "0781008" aren't useful as dropdown
                    # filter options and flood the list with noise. Skip them.
                    if kind == 'subcategory' and val_s.isdigit():
                        continue
                    rows_to_insert.append((kind, src, val_s, int(cnt)))

            # Atomic swap. DELETE + insert everything inside one transaction —
            # on any error we roll back and keep the old cache.
            c.execute('DELETE FROM data_history_filter_cache')
            if rows_to_insert:
                args_str = b','.join(
                    c.mogrify('(%s,%s,%s,%s,now())', row) for row in rows_to_insert
                ).decode('utf-8')
                c.execute(
                    'INSERT INTO data_history_filter_cache (kind, source, value, count, updated_at) VALUES '
                    + args_str
                )
            conn.commit()
            global _filter_cache_last_refresh_at
            _filter_cache_last_refresh_at = time.time()
            if DEV_MODE:
                Log.cleanup(f"Filter cache refreshed — {len(rows_to_insert)} rows")
        finally:
            conn.close()
    except Exception as e:
        Log.error(f"Filter cache refresh error: {e}")


def _filter_cache_scheduler():
    """Background loop that refreshes the filter cache every N seconds."""
    # Let the DB settle after startup, then do the first refresh.
    time.sleep(30)
    while not _shutdown_event.is_set():
        try:
            _refresh_filter_cache()
        except Exception as e:
            Log.error(f"Filter cache scheduler error: {e}")
        if _shutdown_event.wait(timeout=FILTER_CACHE_REFRESH_INTERVAL):
            return


def _filter_cache_has_data():
    """Returns True if the cache has any rows — so we can serve from it."""
    try:
        conn = get_conn()
        try:
            c = conn.cursor()
            c.execute('SELECT 1 FROM data_history_filter_cache LIMIT 1')
            return c.fetchone() is not None
        finally:
            conn.close()
    except Exception:
        return False


def _filters_from_cache(source_filter, hours):
    """Serve /api/data/history/filters from the filter cache table.

    Returns the new provider/type-nested shape. `hours` is accepted for
    API compatibility but the cache already represents current-state
    (is_latest=1) options, so it is not applied.

    `source_filter` may be either a canonical alert_type
    (`waze_hazard`, `traffic_incident`, …) or a raw `data_history.source`
    value (`livetraffic`, `bom_warning`, …). It scopes the response to a
    single provider/type pair.
    """
    try:
        conn = get_conn()
        try:
            c = conn.cursor()
            c.execute("SET LOCAL statement_timeout = '10s'")

            # Per-source counts (one row per source)
            c.execute(
                "SELECT value, count FROM data_history_filter_cache "
                "WHERE kind = 'source'"
            )
            source_counts = {v: int(cnt) for v, cnt in c.fetchall()}

            # Per-source / per-dimension breakdowns. We always pull the
            # full table — it's small (<10k rows) and the caller-side
            # nesting is simpler with everything in memory.
            c.execute(
                "SELECT kind, source, value, count FROM data_history_filter_cache "
                "WHERE kind IN ('category','subcategory','status','severity')"
            )
            per_source_dims = {}  # alert_type -> kind -> {value: count}
            for kind, src, value, cnt in c.fetchall():
                alert_type = _canonical_alert_type(src)
                if not alert_type:
                    continue
                per_source_dims.setdefault(alert_type, {}) \
                               .setdefault(kind, {})
                d = per_source_dims[alert_type][kind]
                d[value] = d.get(value, 0) + int(cnt)

            # Aggregate raw-source counts onto canonical alert_types.
            type_counts = {}
            for raw_src, cnt in source_counts.items():
                alert_type = _canonical_alert_type(raw_src)
                if not alert_type:
                    continue
                type_counts[alert_type] = type_counts.get(alert_type, 0) + int(cnt)

            # rdio_summaries: counted directly (it's not in the cache table).
            try:
                c.execute("SET LOCAL statement_timeout = '3s'")
                c.execute("SELECT COUNT(*) FROM rdio_summaries "
                          "WHERE release_at IS NULL OR release_at <= now()")
                rdio_count = int(c.fetchone()[0] or 0)
            except Exception:
                rdio_count = 0
            type_counts['radio_summary'] = rdio_count

            # user_incident: count rows in the user-submissions `incidents`
            # table. Best-effort — if the table is missing, fall back to 0.
            try:
                c.execute("SELECT COUNT(*) FROM incidents")
                user_count = int(c.fetchone()[0] or 0)
            except Exception:
                user_count = 0
            type_counts['user_incident'] = user_count

            # Date range over data_history. idx_data_fetched supports MIN/MAX
            # in O(log n).
            c.execute("SET LOCAL statement_timeout = '5s'")
            c.execute('SELECT MIN(fetched_at), MAX(fetched_at) FROM data_history')
            min_ts, max_ts = c.fetchone() or (None, None)
        finally:
            try: conn.close()
            except Exception: pass
    except Exception as e:
        Log.error(f"Filter cache read error: {e}")
        return jsonify({'error': str(e)}), 500

    return jsonify(_build_filters_response(
        type_counts, per_source_dims, min_ts, max_ts, source_filter,
    ))


def _merge_case_insensitive(d):
    """Merge a {value: count} dict so case-only duplicates collapse.

    `active` and `Active` come from different sources but mean the same
    filter; same with `Hazard`/`HAZARD`. We keep the most-common casing
    as the displayed label and sum the counts.
    """
    totals = {}        # lowercase -> summed count
    variants = {}      # lowercase -> {original_case: seen_count}
    for k, v in d.items():
        lk = k.lower()
        totals[lk] = totals.get(lk, 0) + v
        variants.setdefault(lk, {})[k] = variants.get(lk, {}).get(k, 0) + v
    merged = {}
    for lk, total in totals.items():
        # Prefer the casing with the highest count (so "Hazard" wins over
        # "HAZARD" when Waze dominates). Ties: lexicographic.
        best_case = max(variants[lk].items(), key=lambda x: (x[1], x[0]))[0]
        merged[best_case] = total
    return merged


def _resolve_filter_target(source_filter):
    """Map a `?source=` query value to (alert_type, provider_key) or None.

    Accepts both canonical alert_types (waze_hazard, traffic_incident, …)
    and raw data_history.source values (livetraffic, bom_warning, …) so
    legacy frontends keep working during the migration.
    """
    if not source_filter:
        return None
    s = str(source_filter).strip()
    if not s:
        return None
    # Canonical alert_type lookup wins.
    if s in ALERT_TYPE_PROVIDER:
        return s, ALERT_TYPE_PROVIDER[s][0]
    # Raw source -> canonical alert_type.
    canonical = RAW_SOURCE_TO_ALERT_TYPE.get(s)
    if canonical and canonical in ALERT_TYPE_PROVIDER:
        return canonical, ALERT_TYPE_PROVIDER[canonical][0]
    return None


def _build_filters_response(type_counts, per_source_dims, min_ts, max_ts,
                            source_filter):
    """Format the provider/type-nested response for /api/data/history/filters.

    Args:
        type_counts: dict[alert_type -> int] total record count per type.
        per_source_dims: dict[alert_type -> dict[kind -> dict[value -> count]]]
            categories/subcategories/statuses/severities per type.
        min_ts, max_ts: integer unix seconds bracket for data_history.
        source_filter: optional ?source= query value (alert_type or raw).
    """
    target = _resolve_filter_target(source_filter)

    # Build the full provider map, then filter at the end so the
    # always-include-all-10 rule still applies in the unfiltered case.
    providers_map = {pkey: {'types': []} for pkey in PROVIDER_ORDER}

    # Group alert_types by provider
    by_provider = {}
    for alert_type, (provider_key, _disp) in ALERT_TYPE_PROVIDER.items():
        by_provider.setdefault(provider_key, []).append(alert_type)

    for provider_key in PROVIDER_ORDER:
        alert_types = by_provider.get(provider_key, [])
        # Stable type ordering: explicit list first, then any leftovers
        # alphabetically.
        explicit = PROVIDER_TYPE_ORDER.get(provider_key, [])
        ordered = [t for t in explicit if t in alert_types]
        ordered += sorted(t for t in alert_types if t not in explicit)

        types_out = []
        for alert_type in ordered:
            cnt = int(type_counts.get(alert_type, 0) or 0)
            dims = per_source_dims.get(alert_type, {}) if per_source_dims else {}
            categories_d = _merge_case_insensitive(dims.get('category', {}) or {})
            subcategories_d = _merge_case_insensitive(dims.get('subcategory', {}) or {})
            statuses_d = _merge_case_insensitive(dims.get('status', {}) or {})
            severities_d = _merge_case_insensitive(dims.get('severity', {}) or {})

            categories = [{'value': k, 'count': v}
                          for k, v in sorted(categories_d.items(),
                                             key=lambda x: (-x[1], x[0]))]
            subcategories = [{'value': k, 'count': v}
                             for k, v in sorted(subcategories_d.items(),
                                                key=lambda x: (-x[1], x[0]))][:100]
            statuses = [{'value': k, 'count': v}
                        for k, v in sorted(statuses_d.items(),
                                           key=lambda x: (-x[1], x[0]))]
            severities = [{'value': k, 'count': v}
                          for k, v in sorted(severities_d.items(),
                                             key=lambda x: (-x[1], x[0]))]

            type_disp = ALERT_TYPE_PROVIDER[alert_type][1]
            types_out.append({
                'alert_type': alert_type,
                'name': type_disp,
                'count': cnt,
                'categories': categories,
                'subcategories': subcategories,
                'statuses': statuses,
                'severities': severities,
            })
        providers_map[provider_key]['types'] = types_out

    # Apply ?source= filter if given.
    if source_filter:
        if not target:
            providers_out = []
        else:
            target_type, target_provider = target
            kept_types = [
                t for t in providers_map[target_provider]['types']
                if t['alert_type'] == target_type
            ]
            if not kept_types:
                providers_out = []
            else:
                disp = PROVIDER_DISPLAY[target_provider]
                providers_out = [{
                    'key': target_provider,
                    'name': disp['name'],
                    'icon': disp['icon'],
                    'color': disp['color'],
                    'count': sum(t['count'] for t in kept_types),
                    'types': kept_types,
                }]
    else:
        providers_out = []
        for provider_key in PROVIDER_ORDER:
            disp = PROVIDER_DISPLAY[provider_key]
            types_out = providers_map[provider_key]['types']
            providers_out.append({
                'key': provider_key,
                'name': disp['name'],
                'icon': disp['icon'],
                'color': disp['color'],
                'count': sum(t['count'] for t in types_out),
                'types': types_out,
            })

    return {
        'providers': providers_out,
        'date_range': {
            'oldest': datetime.fromtimestamp(min_ts).isoformat() if min_ts else None,
            'newest': datetime.fromtimestamp(max_ts).isoformat() if max_ts else None,
            'oldest_unix': min_ts,
            'newest_unix': max_ts,
        },
    }


@app.route('/api/data/history/filters')
def data_history_filters():
    """
    Provider/type-nested filter tree for the history search UI.

    Default path reads from data_history_filter_cache (refreshed every
    5 min from is_latest=1 rows). That keeps this endpoint under 100ms
    even on a busy DB. Pass ?fresh=1 to bypass the cache and scan
    data_history directly (slower, but respects `hours` exactly).

    Query parameters:
        source: Optional. Either a canonical alert_type
                (waze_hazard, traffic_incident, …) or a raw
                data_history.source value. Restricts the response to
                that single provider/type pair.
        hours:  Optional, only honored on the live-scan path. Restricts
                the count to records fetched within the last N hours.
        fresh:  If '1', bypass cache and scan data_history.
        all_history: Same effect as fresh=1 (legacy alias).

    Response shape (also when ?source= matches):
        {
            "providers": [
                {
                    "key": "waze",
                    "name": "Waze",
                    "icon": "car",
                    "color": "#00d4ff",
                    "count": 1234,
                    "types": [
                        {
                            "alert_type": "waze_hazard",
                            "name": "Hazards",
                            "count": 567,
                            "categories":    [{"value": "...", "count": 5}, ...],
                            "subcategories": [...],
                            "statuses":      [...],
                            "severities":    [...]
                        }, ...
                    ]
                }, ...
            ],
            "date_range": { "oldest": ISO, "newest": ISO,
                            "oldest_unix": int, "newest_unix": int }
        }
    """
    try:
        source_filter = request.args.get('source')
        hours = request.args.get('hours', type=int)
        # Fast path: read from cache unless explicitly told to scan live.
        use_cache = (
            request.args.get('fresh') != '1'
            and request.args.get('all_history') != '1'
            and _filter_cache_has_data()
        )
        if use_cache:
            return _filters_from_cache(source_filter, hours)

        # Live-scan fallback. Scope to is_latest=1 — the list view always
        # uses unique=1 so any option only existing on historical (non-
        # latest) rows would never match a visible record anyway.
        time_condition_sql = ""
        time_params = []
        if hours:
            cutoff = int(time.time()) - (hours * 3600)
            time_condition_sql = "AND fetched_at >= %s"
            time_params = [cutoff]

        type_counts = {}      # alert_type -> count
        per_source_dims = {}  # alert_type -> kind -> {value: count}
        min_ts = None
        max_ts = None

        try:
            conn = get_conn()
            try:
                c = conn.cursor()
                c.execute("SET LOCAL statement_timeout = '25s'")

                deprecated = list(DEPRECATED_SOURCES) or ['__nothing__']
                dep_ph = ','.join(['%s'] * len(deprecated))

                # Per-source totals for the live-scan window.
                c.execute(
                    f"SELECT source, COUNT(*) FROM data_history "
                    f"WHERE is_latest = 1 AND source NOT IN ({dep_ph}) "
                    f"{time_condition_sql} GROUP BY source",
                    deprecated + time_params,
                )
                for src, cnt in c.fetchall():
                    alert_type = _canonical_alert_type(src)
                    if not alert_type:
                        continue
                    type_counts[alert_type] = type_counts.get(alert_type, 0) + int(cnt)

                # Per-source/per-dimension breakdowns. Pager-style numeric
                # subcategories ("1160008", …) are dropped on the live path
                # too, matching the cache-refresh behavior.
                for kind in ('category', 'subcategory', 'status', 'severity'):
                    c.execute(
                        f"SELECT source, {kind}, COUNT(*) FROM data_history "
                        f"WHERE is_latest = 1 AND source NOT IN ({dep_ph}) "
                        f"  AND {kind} IS NOT NULL AND {kind} <> '' "
                        f"  {time_condition_sql} "
                        f"GROUP BY source, {kind}",
                        deprecated + time_params,
                    )
                    for src, val, cnt in c.fetchall():
                        if not (src and val):
                            continue
                        val_s = str(val).strip()
                        if not val_s:
                            continue
                        if kind == 'subcategory' and val_s.isdigit():
                            continue
                        alert_type = _canonical_alert_type(src)
                        if not alert_type:
                            continue
                        per_source_dims.setdefault(alert_type, {}) \
                                       .setdefault(kind, {})
                        d = per_source_dims[alert_type][kind]
                        d[val_s] = d.get(val_s, 0) + int(cnt)

                # rdio_summaries (live, hours-aware).
                try:
                    if hours:
                        c.execute(
                            "SELECT COUNT(*) FROM rdio_summaries "
                            "WHERE (release_at IS NULL OR release_at <= now()) "
                            "  AND created_at >= now() - (%s::int || ' hours')::interval",
                            (hours,),
                        )
                    else:
                        c.execute(
                            "SELECT COUNT(*) FROM rdio_summaries "
                            "WHERE release_at IS NULL OR release_at <= now()"
                        )
                    type_counts['radio_summary'] = int(c.fetchone()[0] or 0)
                except Exception:
                    type_counts['radio_summary'] = 0

                # user_incident: total rows in the user-submission table.
                try:
                    c.execute("SELECT COUNT(*) FROM incidents")
                    type_counts['user_incident'] = int(c.fetchone()[0] or 0)
                except Exception:
                    type_counts['user_incident'] = 0

                # Date range (cheap via idx_data_fetched).
                c.execute('SELECT MIN(fetched_at), MAX(fetched_at) FROM data_history')
                row = c.fetchone() or (None, None)
                min_ts, max_ts = row
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as e:
            Log.error(f"Filters live-scan error: {e}")

        return jsonify(_build_filters_response(
            type_counts, per_source_dims, min_ts, max_ts, source_filter,
        ))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/data/history/incident/<source>/<source_id>')
def data_history_incident(source, source_id):
    """Get full history of a specific incident by source and source_id"""
    try:
        # Get the right database for this source
        db_path = get_history_db_for_source(source)
        
        conn = get_conn()
        try:
            c = conn.cursor()
            # Cap so a hot incident with thousands of snapshots can't OOM the
            # response. 5000 is well above any real history (RFS leaders top
            # out around ~1500 snapshots).
            c.execute('''
                SELECT id, fetched_at, source_timestamp, source_timestamp_unix,
                       latitude, longitude, location_text, title, category, subcategory,
                       status, severity, data, is_active, is_live, last_seen
                FROM data_history
                WHERE source = %s AND source_id = %s
                ORDER BY fetched_at ASC
                LIMIT 5000
            ''', (source, source_id))

            rows = c.fetchall()
        finally:
            try: conn.close()
            except Exception: pass
        
        history = []
        for row in rows:
            history.append({
                'id': row[0],
                'fetched_at': row[1],
                'fetched_at_iso': datetime.fromtimestamp(row[1]).isoformat() if row[1] else None,
                'source_timestamp': row[2],
                'source_timestamp_unix': row[3],
                'latitude': row[4],
                'longitude': row[5],
                'location_text': row[6],
                'title': row[7],
                'category': row[8],
                'subcategory': row[9],
                'status': row[10],
                'severity': row[11],
                'data': json.loads(row[12]) if row[12] else {},
                'is_active': row[13] == 1,
                'is_live': row[14] == 1 if row[14] is not None else True,
                'last_seen': row[15],
                'last_seen_iso': datetime.fromtimestamp(row[15]).isoformat() if row[15] else None
            })
        
        # Get current live status (from most recent record)
        is_currently_live = history[-1]['is_live'] if history else False
        
        return jsonify({
            'source': source,
            'source_id': source_id,
            'is_live': is_currently_live,
            'snapshots': len(history),
            'history': history
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============== DATABASE MAINTENANCE ==============

@app.route('/api/admin/db/cleanup-duplicates', methods=['POST'])
@require_api_key
def cleanup_duplicate_history():
    """
    Remove duplicate rows from all data_history databases, keeping only the FIRST row 
    for each unique source+source_id combination (grouped by data_hash).
    
    This endpoint requires API key and should be called manually when needed.
    """
    try:
        total_before = 0
        total_unique = 0
        total_deleted = 0

        with _db_lock_history_waze:
            conn = get_conn()
            try:
                c = conn.cursor()

                c.execute('SELECT COUNT(*) FROM data_history')
                total_before = c.fetchone()[0]

                c.execute('SELECT COUNT(DISTINCT source || COALESCE(source_id, \'\')) FROM data_history')
                total_unique = c.fetchone()[0]

                c.execute('''
                    DELETE FROM data_history
                    WHERE id NOT IN (
                        SELECT MIN(id)
                        FROM data_history
                        GROUP BY source, source_id, data_hash
                    )
                ''')
                total_deleted = c.rowcount

                conn.commit()
            finally:
                try: conn.close()
                except Exception: pass

        count_after = total_before - total_deleted
        Log.info(f"Cleanup: deleted {total_deleted} duplicate rows ({total_before} -> {count_after})")
        
        return jsonify({
            'success': True,
            'count_before': total_before,
            'count_after': count_after,
            'deleted': total_deleted,
            'unique_incidents': total_unique,
            'databases_processed': 1
        })
    except Exception as e:
        Log.error(f"Cleanup error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/db/stats')
@require_api_key
def db_stats():
    """Get detailed database statistics for all databases"""
    try:
        stats = {}
        
        # Aggregate history database stats
        total_rows = 0
        unique_incidents = 0
        by_source = {}
        live_count = 0
        history_db_stats = {}
        
        conn = None
        try:
            conn = get_conn()
            c = conn.cursor()

            # Combine all 4 small COUNTs into a single round-trip — fewer
            # statement overheads on a hot dashboard panel.
            c.execute('''
                SELECT
                    COUNT(*) AS total,
                    COUNT(DISTINCT source || COALESCE(source_id, '')) AS unique_n,
                    COUNT(*) FILTER (WHERE is_live = 1) AS live_n,
                    pg_total_relation_size('data_history') AS size_bytes
                FROM data_history
            ''')
            row = c.fetchone()
            db_rows = row[0] or 0
            total_rows = db_rows
            db_unique = row[1] or 0
            unique_incidents = db_unique
            db_live = row[2] or 0
            live_count = db_live
            db_size = row[3] or 0

            c.execute('SELECT source, COUNT(*) FROM data_history GROUP BY source')
            for r in c.fetchall():
                by_source[r[0]] = by_source.get(r[0], 0) + r[1]

            history_db_stats['data_history'] = {
                'rows': db_rows,
                'unique_incidents': db_unique,
                'live_count': db_live,
                'size_mb': round(db_size / (1024 * 1024), 2)
            }
        except Exception as e:
            history_db_stats['data_history'] = {'error': str(e)}
        finally:
            if conn is not None:
                try: conn.close()
                except Exception: pass
        
        stats['data_history'] = {
            'total_rows': total_rows,
            'unique_incidents': unique_incidents,
            'by_source': by_source,
            'live_count': live_count,
            'databases': history_db_stats
        }
        
        # api_data_cache stats (from cache.db)
        conn = None
        try:
            conn = get_conn()
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM api_data_cache')
            stats['api_data_cache'] = {'total_rows': c.fetchone()[0]}
            c.execute('SELECT endpoint, ttl, fetch_time_ms FROM api_data_cache ORDER BY endpoint')
            stats['api_data_cache']['endpoints'] = [
                {'endpoint': r[0], 'ttl': r[1], 'fetch_ms': r[2]}
                for r in c.fetchall()
            ]
        except Exception as e:
            stats['api_data_cache'] = {'error': str(e)}
        finally:
            if conn is not None:
                try: conn.close()
                except Exception: pass

        # stats_snapshots stats (from stats.db)
        conn = None
        try:
            conn = get_conn()
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM stats_snapshots')
            stats['stats_snapshots'] = {'total_rows': c.fetchone()[0]}
        except Exception as e:
            stats['stats_snapshots'] = {'error': str(e)}
        finally:
            if conn is not None:
                try: conn.close()
                except Exception: pass
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/db/vacuum', methods=['POST'])
@require_api_key
def vacuum_db():
    """Run VACUUM on PostgreSQL tables to reclaim space and optimize"""
    try:
        results = {}
        tables = ['api_data_cache', 'stats_snapshots', 'editor_requests', 'data_history']
        total_saved = 0
        
        conn = get_conn()
        try:
            # VACUUM cannot run inside a transaction block — use autocommit.
            conn.autocommit = True
            cur = conn.cursor()
            for table in tables:
                try:
                    cur.execute("SELECT pg_total_relation_size(%s)", (table,))
                    size_before = cur.fetchone()[0] / (1024 * 1024)
                    cur.execute(f'VACUUM "{table}"')
                    cur.execute("SELECT pg_total_relation_size(%s)", (table,))
                    size_after = cur.fetchone()[0] / (1024 * 1024)
                    saved = size_before - size_after
                    total_saved += saved
                    results[table] = {
                        'size_before_mb': round(size_before, 2),
                        'size_after_mb': round(size_after, 2),
                        'saved_mb': round(saved, 2)
                    }
                except Exception as e:
                    results[table] = {'error': str(e)}
            cur.close()
        finally:
            try: conn.close()
            except Exception: pass
        
        Log.info(f"VACUUM complete: saved {total_saved:.2f}MB total")
        
        return jsonify({
            'success': True,
            'databases': results,
            'total_saved_mb': round(total_saved, 2)
        })
    except Exception as e:
        Log.error(f"VACUUM error: {e}")
        return jsonify({'error': str(e)}), 500


# ============== EDITOR REQUESTS ==============

def send_editor_request_discord_notification(email, discord_id, website, about, request_type, region, background, background_details, has_existing_setup, setup_details, tech_experience, experience_level, request_id):
    """Send Discord webhook notification for new editor request"""
    if not EDITOR_REQUEST_WEBHOOK:
        Log.warn("EDITOR_REQUEST_WEBHOOK not configured - skipping Discord notification")
        return False
    
    try:
        # Truncate about text if too long for Discord embed
        about_preview = about[:300] + "..." if about and len(about) > 300 else (about or "Not provided")
        
        # Format request types nicely
        type_labels = {
            'editor': '📝 Map Editor',
            'pager_feeder': '📟 Pager Feeder',
            'radio_feeder': '📻 Radio Feeder'
        }
        request_types_formatted = ", ".join([type_labels.get(t, t) for t in (request_type or [])]) or "Not specified"
        
        # Format background with details
        bg_labels = {
            'rfs': 'RFS',
            'frnsw': 'Fire & Rescue NSW',
            'ses': 'SES',
            'ambulance': 'NSW Ambulance',
            'police': 'NSW Police',
            'vra': 'VRA',
            'marine_rescue': 'Marine Rescue',
            'scanner_hobbyist': 'Scanner Hobbyist',
            'other_emergency': 'Other Emergency',
            'none': 'General Interest'
        }
        background_formatted = bg_labels.get(background, background) if background else "Not specified"
        if background_details:
            bg_details_preview = background_details[:200] + "..." if len(background_details) > 200 else background_details
            background_formatted += f"\n*{bg_details_preview}*"
        
        # Format setup info for pager/radio feeders
        setup_formatted = None
        if has_existing_setup:
            if has_existing_setup == 'yes':
                setup_formatted = "✅ Has existing setup"
                if setup_details:
                    setup_preview = setup_details[:200] + "..." if len(setup_details) > 200 else setup_details
                    setup_formatted += f"\n*{setup_preview}*"
            else:
                setup_formatted = "🆕 No setup yet (interested)"
        
        fields = [
            {"name": "🎯 Request Type", "value": request_types_formatted, "inline": False},
            {"name": "📧 Email", "value": email, "inline": True},
            {"name": "💬 Discord ID", "value": discord_id, "inline": True},
            {"name": "📍 Region", "value": region or "Not specified", "inline": True},
        ]
        
        # Add setup info if applicable (pager/radio feeder)
        if setup_formatted:
            fields.append({"name": "📡 Feeder Setup", "value": setup_formatted, "inline": False})
        
        # Add tech experience if provided (it's a list now)
        if tech_experience:
            tech_labels = {
                'linux': '🐧 Linux/CLI',
                'networking': '🌐 Networking',
                'docker': '🐳 Docker',
                'raspberry_pi': '🥧 Raspberry Pi',
                'sdr': '📻 SDR/Radio',
                'programming': '💻 Programming',
                'none': '🆕 Willing to learn'
            }
            if isinstance(tech_experience, list):
                tech_formatted = ', '.join([tech_labels.get(t, t) for t in tech_experience])
            else:
                # Handle legacy string format
                tech_list = tech_experience.split(',') if tech_experience else []
                tech_formatted = ', '.join([tech_labels.get(t.strip(), t.strip()) for t in tech_list])
            fields.append({"name": "💻 Technical Experience", "value": tech_formatted, "inline": False})
        
        # Add experience level if provided
        if experience_level:
            stars = '⭐' * experience_level
            level_labels = {1: 'Beginner', 2: 'Basic', 3: 'Intermediate', 4: 'Advanced', 5: 'Expert'}
            fields.append({"name": "📊 Experience Level", "value": f"{stars} ({level_labels.get(experience_level, 'Unknown')})", "inline": True})
        
        fields.extend([
            {"name": "🏥 Background & Experience", "value": background_formatted, "inline": False},
            {"name": "🔗 Website", "value": website or "Not provided", "inline": False},
            {"name": "📋 About", "value": about_preview, "inline": False},
            {"name": "🆔 Request ID", "value": str(request_id), "inline": True}
        ])
        
        embed = {
            "title": "📝 New Access Request",
            "color": 0xf97316,  # Orange
            "fields": fields,
            "footer": {"text": "NSW PSN Request System"},
            "timestamp": datetime.now().isoformat()
        }
        
        # Build content with optional ping
        content = "A new editor access request has been submitted!"
        if EDITOR_REQUEST_PING_ID:
            content = f"<@{EDITOR_REQUEST_PING_ID}> {content}"
        
        payload = {
            "embeds": [embed],
            "content": content
        }
        
        response = requests.post(
            EDITOR_REQUEST_WEBHOOK,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code in [200, 204]:
            Log.info(f"Discord notification sent for editor request #{request_id}")
            return True
        else:
            Log.error(f"Discord webhook failed: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        Log.error(f"Error sending Discord notification: {e}")
        return False


@app.route('/api/editor-requests', methods=['POST', 'OPTIONS'])
def submit_editor_request():
    """Submit a new editor access request"""
    if request.method == 'OPTIONS':
        return '', 204
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'Invalid JSON data'}), 400
        
        email = data.get('email', '').strip().lower()
        discord_id = data.get('discord_id', '').strip()
        website = data.get('website', '').strip() if data.get('website') else None
        about = data.get('about', '').strip() if data.get('about') else None
        request_type = data.get('request_type', [])  # List of types: editor, pager_feeder, radio_feeder
        region = data.get('region', '').strip() if data.get('region') else None
        background = data.get('background', '').strip() if data.get('background') else None
        background_details = data.get('background_details', '').strip() if data.get('background_details') else None
        has_existing_setup = data.get('has_existing_setup', '').strip() if data.get('has_existing_setup') else None
        setup_details = data.get('setup_details', '').strip() if data.get('setup_details') else None
        tech_experience = data.get('tech_experience', [])  # List of tech skills
        experience_level = data.get('experience_level')  # 1-5 star rating
        if experience_level is not None:
            try:
                experience_level = int(experience_level)
                if experience_level < 1 or experience_level > 5:
                    experience_level = None
            except (ValueError, TypeError):
                experience_level = None
        
        # Normalize tech_experience to list
        if isinstance(tech_experience, str):
            tech_experience = [tech_experience] if tech_experience else []
        
        # Normalize request_type to list
        if isinstance(request_type, str):
            request_type = [request_type] if request_type else []
        
        # Validation
        if not email or '@' not in email:
            return jsonify({'error': 'Valid email is required'}), 400
        
        if not discord_id:
            return jsonify({'error': 'Discord ID is required'}), 400
        
        if not about:
            return jsonify({'error': 'Please tell us about yourself'}), 400
        
        if not request_type:
            return jsonify({'error': 'Please select at least one request type'}), 400
        
        # Check for existing pending request with same email (config.db)
        conn = get_conn_dict()
        try:
            c = conn.cursor()
            
            c.execute('''
                SELECT id FROM editor_requests 
                WHERE email = %s AND status = 'pending'
            ''', (email,))
            
            existing = c.fetchone()
            if existing:
                conn.close()
                return jsonify({'error': 'A pending request with this email already exists'}), 409
            
            # Store request_type and tech_experience as comma-separated strings
            request_type_str = ','.join(request_type) if request_type else None
            tech_experience_str = ','.join(tech_experience) if tech_experience else None
            
            # Insert new request
            created_at = int(time.time())
            c.execute('''
                INSERT INTO editor_requests (email, discord_id, website, about, request_type, region, background, background_details, has_existing_setup, setup_details, tech_experience, experience_level, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s)
            RETURNING id
            ''', (email, discord_id, website, about, request_type_str, region, background, background_details, has_existing_setup, setup_details, tech_experience_str, experience_level, created_at))
            
            request_id = c.fetchone()['id']
            conn.commit()
        finally:
            conn.close()
        
        type_labels = {'editor': 'Editor', 'pager_feeder': 'Pager Feeder', 'radio_feeder': 'Radio Feeder'}
        type_str = ', '.join([type_labels.get(t, t) for t in request_type])
        Log.info(f"New request #{request_id} ({type_str}) from {email} (Discord: {discord_id})")
        
        # Send Discord notification
        send_editor_request_discord_notification(email, discord_id, website, about, request_type, region, background, background_details, has_existing_setup, setup_details, tech_experience, experience_level, request_id)
        
        return jsonify({
            'success': True,
            'message': 'Request submitted successfully',
            'request_id': request_id
        }), 201
        
    except Exception as e:
        Log.error(f"Error submitting editor request: {e}")
        return jsonify({'error': 'Failed to submit request'}), 500


@app.route('/api/editor-requests', methods=['GET'])
@require_api_key
def list_editor_requests():
    """List all editor requests (admin only)"""
    try:
        status_filter = request.args.get('status', None)
        
        conn = get_conn_dict()
        try:
            c = conn.cursor()
            
            if status_filter:
                c.execute('''
                    SELECT * FROM editor_requests 
                    WHERE status = %s
                    ORDER BY created_at DESC
                ''', (status_filter,))
            else:
                c.execute('''
                    SELECT * FROM editor_requests 
                    ORDER BY created_at DESC
                ''')
            
            rows = c.fetchall()
        finally:
            conn.close()
        
        requests_list = []
        for row in rows:
            # Parse request_type back to list
            request_type_str = row['request_type'] if 'request_type' in row.keys() else None
            request_type = request_type_str.split(',') if request_type_str else []
            
            requests_list.append({
                'id': row['id'],
                'email': row['email'],
                'discord_id': row['discord_id'],
                'website': row['website'],
                'about': row['about'] if 'about' in row.keys() else None,
                'request_type': request_type,
                'region': row['region'] if 'region' in row.keys() else None,
                'background': row['background'] if 'background' in row.keys() else None,
                'background_details': row['background_details'] if 'background_details' in row.keys() else None,
                'has_existing_setup': row['has_existing_setup'] if 'has_existing_setup' in row.keys() else None,
                'setup_details': row['setup_details'] if 'setup_details' in row.keys() else None,
                'tech_experience': row['tech_experience'] if 'tech_experience' in row.keys() else None,
                'experience_level': row['experience_level'] if 'experience_level' in row.keys() else None,
                'status': row['status'],
                'created_at': row['created_at'],
                'reviewed_at': row['reviewed_at'],
                'notes': row['notes']
            })
        
        return jsonify({
            'requests': requests_list,
            'count': len(requests_list)
        })
        
    except Exception as e:
        Log.error(f"Error listing editor requests: {e}")
        return jsonify({'error': 'Failed to list requests'}), 500


@app.route('/api/editor-requests/<int:request_id>/approve', methods=['POST'])
@require_api_key
def approve_editor_request(request_id):
    """Approve an editor request with roles and optionally create Supabase account"""
    try:
        import secrets
        import string
        
        data = request.get_json() or {}
        roles = data.get('roles', [])
        create_account = data.get('create_account', False)
        
        conn = get_conn_dict()
        try:
            c = conn.cursor()
            
            # Get the request
            c.execute('SELECT * FROM editor_requests WHERE id = %s', (request_id,))
            req = c.fetchone()

            if not req:
                return jsonify({'error': 'Request not found'}), 404

            if req['status'] != 'pending':
                return jsonify({'error': f'Request is already {req["status"]}'}), 400

            temp_password = None
            supabase_account_created = False
            supabase_error = None
            supabase_user_id = None
            
            # Create Supabase account if requested
            if create_account:
                # Generate temporary password: Changeme-XXXXXX
                random_suffix = ''.join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(6))
                temp_password = f"Changeme-{random_suffix}"
                
                if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
                    try:
                        # Use Supabase Admin API to create user
                        create_user_url = f"{SUPABASE_URL}/auth/v1/admin/users"
                        headers = {
                            'apikey': SUPABASE_SERVICE_ROLE_KEY,
                            'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
                            'Content-Type': 'application/json'
                        }
                        user_data = {
                            'email': req['email'],
                            'password': temp_password,
                            'email_confirm': True,
                            'user_metadata': {
                                'discord_id': req['discord_id'],
                                'approved_request_id': request_id,
                                'roles': roles,
                                'force_password_change': True
                            }
                        }
                        
                        response = requests.post(create_user_url, json=user_data, headers=headers, timeout=15)
                        
                        if response.status_code in [200, 201]:
                            supabase_account_created = True
                            user_response = response.json()
                            supabase_user_id = user_response.get('id')
                            Log.info(f"Created Supabase account for {req['email']} (ID: {supabase_user_id})")
                            
                            # Insert roles into user_roles table
                            if supabase_user_id and roles:
                                try:
                                    conn = get_conn()
                                    try:
                                        cur = conn.cursor()
                                        for role in roles:
                                            cur.execute(
                                                "INSERT INTO user_roles (user_id, role, granted_by, request_id) VALUES (%s, %s, 'system', %s) ON CONFLICT (user_id, role) DO NOTHING",
                                                (supabase_user_id, role, request_id)
                                            )
                                        conn.commit()
                                        cur.close()
                                    finally:
                                        conn.close()
                                    Log.info(f"Assigned roles to user: {', '.join(roles)}")
                                except Exception as role_error:
                                    Log.warn(f"Error inserting roles (non-fatal): {role_error}")
                        else:
                            error_detail = response.json() if response.text else {}
                            supabase_error = error_detail.get('message', error_detail.get('msg', f'Status {response.status_code}'))
                            Log.error(f"Failed to create Supabase account: {supabase_error}")
                            
                    except Exception as e:
                        supabase_error = str(e)
                        Log.error(f"Error creating Supabase account: {e}")
            
            # Update request status
            reviewed_at = int(time.time())
            roles_str = ','.join(roles) if roles else ''
            notes = f'Roles: {roles_str}'
            if temp_password:
                notes += f' | Temp password: {temp_password}'
            if supabase_account_created:
                notes += ' | Supabase account created'
            elif supabase_error:
                notes += f' | Supabase error: {supabase_error}'
            elif create_account and not SUPABASE_SERVICE_ROLE_KEY:
                notes += ' | Supabase not configured'
                
            c.execute('''
                UPDATE editor_requests 
                SET status = 'approved', reviewed_at = %s, notes = %s
                WHERE id = %s
            ''', (reviewed_at, notes, request_id))
            
            conn.commit()
        finally:
            conn.close()
        
        Log.info(f"Approved editor request #{request_id} for {req['email']} with roles: {roles_str}")
        
        result = {
            'success': True,
            'email': req['email'],
            'discord_id': req['discord_id'],
            'roles': roles,
            'supabase_account_created': supabase_account_created
        }
        
        if temp_password:
            result['temp_password'] = temp_password
            
        if supabase_error:
            result['supabase_error'] = supabase_error
            
        return jsonify(result)
        
    except Exception as e:
        Log.error(f"Error approving editor request: {e}")
        return jsonify({'error': 'Failed to approve request'}), 500


@app.route('/api/editor-requests/<int:request_id>/reject', methods=['POST'])
@require_api_key
def reject_editor_request(request_id):
    """Reject an editor request"""
    try:
        data = request.get_json() or {}
        reason = data.get('reason', '')
        
        conn = get_conn_dict()
        try:
            c = conn.cursor()
            
            # Get the request
            c.execute('SELECT * FROM editor_requests WHERE id = %s', (request_id,))
            req = c.fetchone()

            if not req:
                return jsonify({'error': 'Request not found'}), 404

            if req['status'] != 'pending':
                return jsonify({'error': f'Request is already {req["status"]}'}), 400

            # Update request status
            reviewed_at = int(time.time())
            c.execute('''
                UPDATE editor_requests
                SET status = 'rejected', reviewed_at = %s, notes = %s
                WHERE id = %s
            ''', (reviewed_at, reason or 'Rejected', request_id))
            
            conn.commit()
        finally:
            conn.close()
        
        Log.info(f"Rejected editor request #{request_id} for {req['email']}")
        
        return jsonify({
            'success': True,
            'message': 'Request rejected'
        })
        
    except Exception as e:
        Log.error(f"Error rejecting editor request: {e}")
        return jsonify({'error': 'Failed to reject request'}), 500


# ============== USER MANAGEMENT ==============

# Simple in-memory cache for role checks (avoids repeated Supabase calls)
_role_cache = {}
_role_cache_ttl = 60  # Cache roles for 60 seconds

@app.route('/api/check-editor/<user_id>', methods=['GET'])
def check_editor_status(user_id):
    """Check if a user has map_editor role (for map-editor.html access)"""
    try:
        # Check cache first
        cache_key = f"editor_{user_id}"
        cached = _role_cache.get(cache_key)
        if cached and time.time() - cached['ts'] < _role_cache_ttl:
            return jsonify(cached['data'])
        
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT role FROM user_roles WHERE user_id = %s", (user_id,))
            roles_data = cur.fetchall()
            cur.close()
        finally:
            conn.close()
        user_roles = [r[0] for r in roles_data]
        
        is_owner = 'owner' in user_roles
        is_team_member = 'team_member' in user_roles
        is_map_editor = 'map_editor' in user_roles
        
        # Map Editor access: requires map_editor or owner role
        # Team Members do NOT get map editor access (they only manage requests)
        has_access = is_map_editor or is_owner
        
        result = {
            'user_id': user_id,
            'has_access': has_access,
            'is_owner': is_owner,
            'is_team_member': is_team_member,
            'is_map_editor': is_map_editor,
            'roles': user_roles
        }
        
        # Cache the result
        _role_cache[cache_key] = {'ts': time.time(), 'data': result}
        
        return jsonify(result)
        
    except Exception as e:
        Log.error(f"Error checking editor status: {e}")
        return jsonify({'error': 'Failed to check editor status'}), 500


@app.route('/api/check-admin/<user_id>', methods=['GET'])
@require_api_key
def check_admin_status(user_id):
    """Check user's admin access level (owner / team_member / dev).

    Returns per-tab visibility flags under `tabs` so the admin page can
    render exactly the tabs each role is allowed to see:
      - owner:        all three tabs (requests, users, dev)
      - team_member:  requests + users (no dev)
      - dev:          dev only (no requests, no users)
    """
    try:
        # Check cache first
        cache_key = f"admin_{user_id}"
        cached = _role_cache.get(cache_key)
        if cached and time.time() - cached['ts'] < _role_cache_ttl:
            return jsonify(cached['data'])
        
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT role FROM user_roles WHERE user_id = %s", (user_id,))
            roles_data = cur.fetchall()
            cur.close()
        finally:
            conn.close()
        user_roles = [r[0] for r in roles_data]

        is_owner = 'owner' in user_roles
        is_team_member = 'team_member' in user_roles
        is_dev = 'dev' in user_roles
        is_admin = is_owner or is_team_member or is_dev

        # Fallback: If NO owner exists anywhere, check if this is the first setup
        # This prevents lockout during initial setup
        if not is_admin:
            conn = get_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT user_id FROM user_roles WHERE role = 'owner'")
                all_owners = cur.fetchall()
                cur.close()
            finally:
                conn.close()
            if len(all_owners) == 0:
                # No owners exist - grant first-time owner access
                Log.warn(f"No owners exist in system - granting first-time owner to {user_id}")
                is_admin = True
                is_owner = True

        # Permission matrix (per-tab):
        # - Owner:        all three tabs.
        # - Team Member:  Requests + Users. Cannot assign privileged roles
        #                 (team_member/dev/owner) — only map_editor /
        #                 pager_contributor / radio_contributor.
        # - Dev:          Dev tab only (status + future backend controls).
        can_view_requests = is_owner or is_team_member
        can_view_users    = is_owner or is_team_member
        can_view_dev      = is_owner or is_dev
        result = {
            'user_id': user_id,
            'is_admin': is_admin,
            'is_owner': is_owner,
            'is_team_member': is_team_member,
            'is_dev': is_dev,
            # Kept for backwards compatibility with older clients. Now means
            # "can the Users tab be opened" — owner OR team_member.
            'can_manage_users': can_view_users,
            # Owner-only — gates which role checkboxes are visible in the
            # Users tab. Team members can edit users but not promote them.
            'can_assign_privileged_roles': is_owner,
            'tabs': {
                'requests': can_view_requests,
                'users':    can_view_users,
                'dev':      can_view_dev,
            },
            'roles': user_roles
        }
        
        # Cache the result
        _role_cache[cache_key] = {'ts': time.time(), 'data': result}
        
        return jsonify(result)
        
    except Exception as e:
        Log.error(f"Error checking admin status: {e}")
        return jsonify({'error': 'Failed to check admin status'}), 500


@app.route('/api/users', methods=['GET'])
@require_api_key
def list_users():
    """List all Supabase users with their roles"""
    try:
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
            return jsonify({'error': 'Supabase not configured'}), 503
        
        # Get all users from Supabase Auth Admin API
        users_url = f"{SUPABASE_URL}/auth/v1/admin/users"
        headers = {
            'apikey': SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
            'Content-Type': 'application/json'
        }
        
        users_response = requests.get(users_url, headers=headers, timeout=15)
        
        if users_response.status_code != 200:
            Log.error(f"Failed to fetch users: {users_response.text}")
            return jsonify({'error': 'Failed to fetch users from Supabase'}), 500
        
        users_data = users_response.json()
        users_list = users_data.get('users', [])
        
        # Get all roles from user_roles table (PostgreSQL)
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT user_id, role, created_at, granted_by, id FROM user_roles")
            all_roles = cur.fetchall()
            cur.close()
        finally:
            conn.close()

        user_roles_map = {}
        for row in all_roles:
            uid, role, created_at, granted_by, role_id = row
            if uid not in user_roles_map:
                user_roles_map[uid] = []
            user_roles_map[uid].append({
                'role': role,
                'granted_at': str(created_at) if created_at else None,
                'granted_by': granted_by,
                'id': role_id
            })
        
        # Combine users with their roles
        result = []
        for user in users_list:
            user_id = user.get('id')
            result.append({
                'id': user_id,
                'email': user.get('email'),
                'created_at': user.get('created_at'),
                'last_sign_in': user.get('last_sign_in_at'),
                'email_confirmed': user.get('email_confirmed_at') is not None,
                'roles': user_roles_map.get(user_id, [])
            })
        
        # Sort by email
        result.sort(key=lambda u: (u.get('email') or '').lower())
        
        return jsonify({
            'users': result,
            'count': len(result)
        })
        
    except Exception as e:
        Log.error(f"Error listing users: {e}")
        return jsonify({'error': 'Failed to list users'}), 500


@app.route('/api/users/<user_id>/roles', methods=['PUT', 'OPTIONS'])
@require_api_key
def update_user_roles(user_id):
    """Update roles for an existing user (replace all roles)"""
    try:
        data = request.get_json() or {}
        new_roles = data.get('roles', [])

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM user_roles WHERE user_id = %s", (user_id,))
            for role in new_roles:
                cur.execute(
                    "INSERT INTO user_roles (user_id, role, granted_by) VALUES (%s, %s, 'admin') ON CONFLICT (user_id, role) DO NOTHING",
                    (user_id, role)
                )
            conn.commit()
            cur.close()
        finally:
            conn.close()
        roles_added = new_roles
        
        Log.info(f"Updated roles for user {user_id}: {', '.join(roles_added)}")
        
        # Invalidate role cache for this user
        _role_cache.pop(f"editor_{user_id}", None)
        _role_cache.pop(f"admin_{user_id}", None)
        
        return jsonify({
            'success': True,
            'user_id': user_id,
            'roles': roles_added
        })
        
    except Exception as e:
        Log.error(f"Error updating user roles: {e}")
        return jsonify({'error': 'Failed to update roles'}), 500


@app.route('/api/users/<user_id>/roles/<role>', methods=['DELETE', 'OPTIONS'])
@require_api_key
def remove_user_role(user_id, role):
    """Remove a single role from a user"""
    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM user_roles WHERE user_id = %s AND role = %s", (user_id, role))
            conn.commit()
            cur.close()
        finally:
            conn.close()

        Log.info(f"Removed role {role} from user {user_id}")
        
        return jsonify({
            'success': True,
            'user_id': user_id,
            'removed_role': role
        })
        
    except Exception as e:
        Log.error(f"Error removing user role: {e}")
        return jsonify({'error': 'Failed to remove role'}), 500


@app.route('/api/users/<user_id>/roles', methods=['POST'])
@require_api_key
def add_user_role(user_id):
    """Add a role to an existing user"""
    try:
        data = request.get_json() or {}
        role = data.get('role')

        if not role:
            return jsonify({'error': 'Role is required'}), 400

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO user_roles (user_id, role, granted_by) VALUES (%s, %s, 'admin') ON CONFLICT (user_id, role) DO NOTHING",
                (user_id, role)
            )
            conn.commit()
            cur.close()
        finally:
            conn.close()

        Log.info(f"Added role {role} to user {user_id}")
        
        return jsonify({
            'success': True,
            'user_id': user_id,
            'added_role': role
        })
        
    except Exception as e:
        Log.error(f"Error adding user role: {e}")
        return jsonify({'error': 'Failed to add role'}), 500


# ============== INCIDENTS CRUD ==============

@app.route('/api/incidents', methods=['GET', 'OPTIONS'])
@require_api_key
def get_incidents():
    """Return all incidents, optionally filtered to active only."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            if request.args.get('active') == 'true':
                cur.execute("SELECT * FROM incidents WHERE expires_at > now() ORDER BY created_at DESC")
            else:
                cur.execute("SELECT * FROM incidents ORDER BY created_at DESC")
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            cur.close()
        finally:
            conn.close()
        # Convert JSONB columns and datetime to serializable format
        for r in rows:
            for k in ['type', 'responding_agencies']:
                if isinstance(r.get(k), str):
                    r[k] = json.loads(r[k])
            for k in ['created_at', 'updated_at', 'expires_at']:
                if r.get(k):
                    r[k] = r[k].isoformat()
        return jsonify(rows)
    except Exception as e:
        Log.error(f"Error fetching incidents: {e}")
        return jsonify({'error': 'Failed to fetch incidents'}), 500


@app.route('/api/incidents', methods=['POST', 'OPTIONS'])
@require_api_key
def create_incident():
    """Create a new incident."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        data = request.get_json() or {}
        conn = get_conn()
        try:
            cur = conn.cursor()
            if data.get('id'):
                cur.execute("""INSERT INTO incidents (id, title, lat, lng, location, type, description, status, size, responding_agencies, expires_at, is_rfs_stub)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING RETURNING id""",
                    (data['id'], data.get('title',''), data.get('lat',0), data.get('lng',0), data.get('location',''),
                     json.dumps(data.get('type',[])), data.get('description',''), data.get('status','Going'),
                     data.get('size','-'), json.dumps(data.get('responding_agencies',[])),
                     data.get('expires_at'), data.get('is_rfs_stub', False)))
            else:
                cur.execute("""INSERT INTO incidents (title, lat, lng, location, type, description, status, size, responding_agencies, expires_at, is_rfs_stub)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                    (data.get('title',''), data.get('lat',0), data.get('lng',0), data.get('location',''),
                     json.dumps(data.get('type',[])), data.get('description',''), data.get('status','Going'),
                     data.get('size','-'), json.dumps(data.get('responding_agencies',[])),
                     data.get('expires_at'), data.get('is_rfs_stub', False)))
            result = cur.fetchone()
            conn.commit()
            cur.close()
        finally:
            conn.close()
        return jsonify({'id': result[0] if result else None, 'success': True}), 201
    except Exception as e:
        Log.error(f"Error creating incident: {e}")
        return jsonify({'error': 'Failed to create incident'}), 500


@app.route('/api/incidents/<incident_id>', methods=['PUT', 'OPTIONS'])
@require_api_key
def update_incident(incident_id):
    """Update an existing incident."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        data = request.get_json() or {}
        allowed = ['title','description','lat','lng','location','type','status','size','responding_agencies','expires_at','updated_at']
        sets = []
        vals = []
        for key in allowed:
            if key in data:
                val = data[key]
                if key in ('type', 'responding_agencies'):
                    val = json.dumps(val)
                sets.append(f"{key} = %s")
                vals.append(val)
        if not sets:
            return jsonify({'error': 'No fields to update'}), 400
        vals.append(incident_id)
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"UPDATE incidents SET {', '.join(sets)} WHERE id = %s", vals)
            conn.commit()
            cur.close()
        finally:
            conn.close()
        return jsonify({'success': True})
    except Exception as e:
        Log.error(f"Error updating incident {incident_id}: {e}")
        return jsonify({'error': 'Failed to update incident'}), 500


@app.route('/api/incidents/<incident_id>', methods=['DELETE', 'OPTIONS'])
@require_api_key
def delete_incident(incident_id):
    """Delete an incident (cascades to updates)."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM incidents WHERE id = %s", (incident_id,))
            conn.commit()
            cur.close()
        finally:
            conn.close()
        return jsonify({'success': True})
    except Exception as e:
        Log.error(f"Error deleting incident {incident_id}: {e}")
        return jsonify({'error': 'Failed to delete incident'}), 500


# ============== INCIDENT UPDATES CRUD ==============

@app.route('/api/incidents/<incident_id>/updates', methods=['GET', 'OPTIONS'])
@require_api_key
def get_incident_updates(incident_id):
    """Get all log entries for an incident."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM incident_updates WHERE incident_id = %s ORDER BY created_at DESC", (incident_id,))
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            cur.close()
        finally:
            conn.close()
        for r in rows:
            if r.get('created_at'):
                r['created_at'] = r['created_at'].isoformat()
        return jsonify(rows)
    except Exception as e:
        Log.error(f"Error fetching updates for incident {incident_id}: {e}")
        return jsonify({'error': 'Failed to fetch incident updates'}), 500


@app.route('/api/incidents/<incident_id>/updates', methods=['POST', 'OPTIONS'])
@require_api_key
def create_incident_update(incident_id):
    """Add a log entry to an incident."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        data = request.get_json() or {}
        message = data.get('message', '')
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO incident_updates (incident_id, message) VALUES (%s, %s) RETURNING id", (incident_id, message))
            result = cur.fetchone()
            conn.commit()
            cur.close()
        finally:
            conn.close()
        return jsonify({'id': result[0], 'success': True}), 201
    except Exception as e:
        Log.error(f"Error creating update for incident {incident_id}: {e}")
        return jsonify({'error': 'Failed to create incident update'}), 500


@app.route('/api/incidents/updates/<update_id>', methods=['PUT', 'OPTIONS'])
@require_api_key
def update_incident_update(update_id):
    """Update a log entry."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        data = request.get_json() or {}
        message = data.get('message', '')
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("UPDATE incident_updates SET message = %s WHERE id = %s", (message, update_id))
            conn.commit()
            cur.close()
        finally:
            conn.close()
        return jsonify({'success': True})
    except Exception as e:
        Log.error(f"Error updating incident update {update_id}: {e}")
        return jsonify({'error': 'Failed to update incident update'}), 500


@app.route('/api/incidents/updates/<update_id>', methods=['DELETE', 'OPTIONS'])
@require_api_key
def delete_incident_update(update_id):
    """Delete a log entry."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM incident_updates WHERE id = %s", (update_id,))
            conn.commit()
            cur.close()
        finally:
            conn.close()
        return jsonify({'success': True})
    except Exception as e:
        Log.error(f"Error deleting incident update {update_id}: {e}")
        return jsonify({'error': 'Failed to delete incident update'}), 500


# ============== RDIO-SCANNER SUMMARIES ==============
# Hourly Gemini summaries of rdio-scanner transcripts.
#
# Data flow:
#   rdio-scanner Postgres (RDIO_DATABASE_URL) --> fetch transcripts in the last hour
#   --> group by system/talkgroup LABELS (no numeric IDs leak to the model)
#   --> Gemini chat completions (OpenAI-compatible) --> store into rdio_summaries.
#
# Hour slot convention: 1..24 (1 = 00:00-01:00 local, 24 = 23:00-24:00 local).
# Scheduler: top of hour + 2min for the just-finished clock hour.
# Ad-hoc summaries can be triggered manually via /api/summaries/trigger.

from psycopg2 import pool as _pg_pool
try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except ImportError:
    _ZoneInfo = None

_RDIO_POOL = None
_RDIO_POOL_LOCK = threading.Lock()
_RDIO_LABELS = {'systems': {}, 'talkgroups': {}, 'fetched_at': 0.0}
_RDIO_LABELS_LOCK = threading.Lock()
_RDIO_LABELS_TTL = 300  # seconds

# Google Gemini via its OpenAI-compatible endpoint.
_LLM_URL = 'https://generativelanguage.googleapis.com/v1beta/openai/chat/completions'
_LLM_DEFAULT_MODEL = 'gemini-2.5-flash'
_SUMMARY_TZ_NAME = os.environ.get('SUMMARY_TZ', 'Australia/Sydney')
# Gemini 2.5 Flash has a 1M-token context. Budget for up to ~5000 calls/hour
# comfortably: ~800k chars (~200k tokens) leaves plenty of room for the ~10KB
# system prompt and 16k-token output budget, well under 1M.
_SUMMARY_MAX_PROMPT_CHARS = 800_000
_PROMPTS_DIR = os.path.join(_script_dir, 'prompts')
_REFERENCE_DIR = os.path.join(_script_dir, 'reference')

# Radio-ID → human-readable label lookup. Populated from CSVs in reference/
# at startup; used to annotate transcript lines before sending to Gemini so
# the model doesn't have to guess what a numeric RID means.
_RDIO_UNIT_LABELS = {}


def _load_rdio_unit_labels():
    """Load RID → label mappings from reference CSVs.

    Two files, merged (later files win on duplicate RIDs):
      reference/rdio_units.csv       — rdioScannerUnits export (has header)
      reference/unit_callsigns.csv   — supplemental callsign list (no header)

    Format for both: first col = numeric radio id, second col = label.
    """
    import csv as _csv
    global _RDIO_UNIT_LABELS
    labels = {}
    for fname in ('rdio_units.csv', 'unit_callsigns.csv'):
        path = os.path.join(_REFERENCE_DIR, fname)
        if not os.path.exists(path):
            continue
        try:
            with open(path, 'r', encoding='utf-8', newline='') as f:
                reader = _csv.reader(f)
                for i, row in enumerate(reader):
                    if not row:
                        continue
                    first_cell = row[0].strip().strip('"')
                    # Skip header row if the id column isn't numeric
                    if i == 0 and not first_cell.isdigit():
                        continue
                    if not first_cell.isdigit():
                        continue
                    try:
                        rid = int(first_cell)
                    except ValueError:
                        continue
                    label = row[1].strip().strip('"') if len(row) > 1 else ''
                    if label:
                        labels[rid] = label
        except Exception as e:
            Log.error(f"Unit label load error ({fname}): {e}")
    _RDIO_UNIT_LABELS = labels
    if labels:
        Log.startup(f"Loaded {len(labels)} radio unit labels")


_load_rdio_unit_labels()

_HOURLY_PROMPT_FALLBACK = (
    "You are an emergency-services dispatch analyst. You are given transcripts of "
    "public-safety radio calls, grouped by agency (system) and talkgroup label. "
    "Write a concise, factual summary of the last hour of activity. Rules: "
    "1) Never invent details. 2) Do not include numeric IDs. "
    "3) Organise by agency/talkgroup. 4) Highlight notable incidents. "
    "5) Keep it under ~300 words, bullet points preferred."
)
_rdio_summary_thread = None


def _rdio_log(msg):
    Log.info(f"[rdio-summary] {msg}")


def _rdio_is_configured():
    return bool(os.environ.get('RDIO_DATABASE_URL'))


def _rdio_get_pool():
    global _RDIO_POOL
    if _RDIO_POOL is not None:
        return _RDIO_POOL
    url = os.environ.get('RDIO_DATABASE_URL')
    if not url:
        return None
    with _RDIO_POOL_LOCK:
        if _RDIO_POOL is None:
            _RDIO_POOL = _pg_pool.ThreadedConnectionPool(1, 5, url)
    return _RDIO_POOL


class _RdioConn:
    """Context manager: lease a connection from the rdio-scanner pool."""
    def __enter__(self):
        pool = _rdio_get_pool()
        if pool is None:
            raise RuntimeError("RDIO_DATABASE_URL not configured")
        self.pool = pool
        self.conn = pool.getconn()
        self.conn.autocommit = True
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        try:
            self.pool.putconn(self.conn)
        except Exception:
            pass


def _rdio_fetch_calls_between(start_utc, end_utc, min_transcript_len=2):
    """Return calls with transcripts in [start_utc, end_utc).

    rdio-scanner's `dateTime` column is a naive `timestamp` holding UTC values.
    To avoid session-timezone pitfalls, we strip tz-info from our aware bounds
    before comparing (both sides end up as naive UTC).
    """
    # Aware UTC → naive UTC so Postgres compares like-for-like against a naive col
    start_naive = start_utc.astimezone(timezone.utc).replace(tzinfo=None) if start_utc.tzinfo else start_utc
    end_naive = end_utc.astimezone(timezone.utc).replace(tzinfo=None) if end_utc.tzinfo else end_utc
    try:
        with _RdioConn() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            try:
                cur.execute(
                    '''
                    SELECT "id" AS call_id, "dateTime" AS date_time, "system", "talkgroup", "transcript",
                           "source", "sources"
                    FROM "rdioScannerCalls"
                    WHERE "dateTime" >= %s AND "dateTime" < %s
                      AND "transcript" IS NOT NULL
                      AND length(btrim("transcript")) >= %s
                    ORDER BY "dateTime" ASC
                    ''',
                    (start_naive, end_naive, min_transcript_len),
                )
                rows = cur.fetchall()
                source_ok('rdio')
                return rows
            finally:
                cur.close()
    except Exception as e:
        source_error('rdio', e)
        raise


def _rdio_refresh_labels_locked():
    """(holding _RDIO_LABELS_LOCK) refresh the system/talkgroup label cache.
    Uses the v6+ rdio-scanner schema where talkgroups live in their own table.
    """
    systems = {}
    talkgroups = {}
    with _RdioConn() as conn:
        cur = conn.cursor()
        try:
            cur.execute('SELECT "id", "label" FROM "rdioScannerSystems"')
            for sys_id, sys_label in cur.fetchall():
                systems[int(sys_id)] = sys_label or f"System {sys_id}"

            cur.execute(
                'SELECT "systemId", "id", "label", "name" FROM "rdioScannerTalkgroups"'
            )
            for sys_id, tg_id, label, name in cur.fetchall():
                if sys_id is None or tg_id is None:
                    continue
                talkgroups[(int(sys_id), int(tg_id))] = {
                    'label': label or '',
                    'name': name or '',
                }
        finally:
            cur.close()
    _RDIO_LABELS['systems'] = systems
    _RDIO_LABELS['talkgroups'] = talkgroups
    _RDIO_LABELS['fetched_at'] = time.time()


def _rdio_resolve_labels(system_id, talkgroup_id):
    """Return (system_label, talkgroup_display) — NO numeric IDs in the output."""
    with _RDIO_LABELS_LOCK:
        if (time.time() - _RDIO_LABELS['fetched_at']) > _RDIO_LABELS_TTL:
            _rdio_refresh_labels_locked()
        systems = _RDIO_LABELS['systems']
        talkgroups = _RDIO_LABELS['talkgroups']
    sys_label = systems.get(int(system_id)) if system_id is not None else None
    tg = talkgroups.get((int(system_id), int(talkgroup_id))) if system_id is not None and talkgroup_id is not None else None
    tg_display = (tg.get('name') or tg.get('label')) if tg else None
    return sys_label, tg_display


def _summary_local_tz():
    if _ZoneInfo is None:
        return timezone.utc
    try:
        return _ZoneInfo(_SUMMARY_TZ_NAME)
    except Exception:
        return timezone.utc


def _load_rdio_prompt(kind, fallback):
    """Read prompts/rdio_<kind>.txt on every call so edits hot-apply.
    Overridable via env RDIO_PROMPT_HOURLY.
    """
    path = os.environ.get(f'RDIO_PROMPT_{kind.upper()}') or os.path.join(_PROMPTS_DIR, f'rdio_{kind}.txt')
    try:
        with open(path, 'r', encoding='utf-8') as f:
            text = f.read().strip()
            if text:
                return text
    except FileNotFoundError:
        pass
    except Exception as e:
        _rdio_log(f"prompt load error ({path}): {e}")
    return fallback


def _extract_radio_id(row):
    """Return the primary radio (unit) ID for a call, or None."""
    src = row.get('source')
    if src:
        try:
            return int(src)
        except (TypeError, ValueError):
            pass
    # Fall back to the first id in the `sources` JSON array.
    raw = row.get('sources')
    if raw:
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(data, list) and data:
                for item in data:
                    if isinstance(item, dict):
                        sid = item.get('src') or item.get('source')
                        if sid:
                            try:
                                return int(sid)
                            except (TypeError, ValueError):
                                continue
        except Exception:
            pass
    return None


_DEDUP_TEXT_RE = re.compile(r'[^a-z0-9 ]+')


def _normalize_transcript(text):
    """Lowercase, strip punctuation, collapse whitespace.

    Used to compare transcripts for pre-LLM dedup. Different Whisper passes
    of the same audio often differ only in punctuation/capitalisation.
    """
    s = (text or '').lower()
    s = _DEDUP_TEXT_RE.sub(' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def _dedupe_calls(calls, window_seconds=180):
    """Drop near-duplicate transmissions before sending to the LLM.

    A row is considered a duplicate when ANOTHER row in the same batch has:
      - same talkgroup, AND
      - same radio ID (source RID) when both are present, AND
      - the shorter row's normalized transcript is equal to OR a prefix of the
        longer row's normalized transcript, AND
      - the time gap between them is ≤ `window_seconds` (default 3 min).

    The longer/more-informative row wins. Returns calls in original order.
    """
    kept = []  # list of (row, ts, text_norm, rid, talkgroup)
    skipped_ids = set()
    for row in calls:
        text_norm = _normalize_transcript(row.get('transcript'))
        if not text_norm:
            kept.append((row, None, '', None, None))
            continue
        dt = row.get('date_time')
        ts = dt.timestamp() if (dt and hasattr(dt, 'timestamp')) else None
        rid = _extract_radio_id(row)
        tg = row.get('talkgroup')

        replaced = False
        for idx, (k_row, k_ts, k_text, k_rid, k_tg) in enumerate(kept):
            if not k_text:
                continue
            if k_tg != tg:
                continue
            # If both sides have RIDs, they must match. If either is missing,
            # still allow dedup on same talkgroup + same text.
            if rid is not None and k_rid is not None and rid != k_rid:
                continue
            if ts is not None and k_ts is not None and abs(ts - k_ts) > window_seconds:
                continue
            # Text match: equal OR one is a prefix of the other
            a, b = k_text, text_norm
            short, long_ = (a, b) if len(a) <= len(b) else (b, a)
            if long_.startswith(short):
                # This row and the kept row are dupes; keep the longer one.
                if len(text_norm) > len(k_text):
                    # Replace kept entry with this richer one; note old id
                    old_cid = k_row.get('call_id')
                    if old_cid is not None:
                        skipped_ids.add(int(old_cid) if old_cid is not None else None)
                    kept[idx] = (row, ts, text_norm, rid, tg)
                else:
                    cid = row.get('call_id')
                    if cid is not None:
                        skipped_ids.add(int(cid))
                replaced = True
                break
        if not replaced:
            kept.append((row, ts, text_norm, rid, tg))

    if skipped_ids:
        _rdio_log(f"pre-LLM dedup: dropped {len(skipped_ids)} near-duplicate call(s)")
    return [k[0] for k in kept]


def _format_rdio_prompt(calls, period_label):
    """Build the user message and return (prompt_text, total_transcript_chars).
    Each line carries `[time #<call_id> RID:<radio_id>]` so the LLM can
    reference specific audio recordings and link transmissions from the same
    radio across the period.
    """
    groups = {}
    total_chars = 0
    local_tz = _summary_local_tz()
    for row in calls:
        text = (row.get('transcript') or '').strip()
        if not text:
            continue
        sys_label, tg_display = _rdio_resolve_labels(row.get('system'), row.get('talkgroup'))
        sys_label = sys_label or 'Unknown System'
        tg_display = tg_display or 'Unknown Talkgroup'
        radio_id = _extract_radio_id(row)
        call_id = row.get('call_id')
        groups.setdefault((sys_label, tg_display), []).append(
            (row['date_time'], text, radio_id, call_id)
        )
        total_chars += len(text)

    lines = [f"Period: {period_label}", ""]
    for (sys_label, tg_display), items in sorted(groups.items()):
        lines.append(f"=== {sys_label} — {tg_display} ({len(items)} transmissions) ===")
        for dt, text, radio_id, call_id in items:
            try:
                # rdio-scanner stores UTC in a naive timestamp; tag as UTC
                # before converting, else Python assumes system-local.
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                t = dt.astimezone(local_tz).strftime('%H:%M:%S')
            except Exception:
                t = str(dt)
            cid_tag = f" #{call_id}" if call_id else ""
            if radio_id:
                lbl = _RDIO_UNIT_LABELS.get(radio_id)
                rid_tag = f" {lbl} (RID:{radio_id})" if lbl else f" RID:{radio_id}"
            else:
                rid_tag = ""
            lines.append(f"[{t}{cid_tag}{rid_tag}] {text}")
        lines.append("")

    prompt = "\n".join(lines)
    if len(prompt) > _SUMMARY_MAX_PROMPT_CHARS:
        prompt = prompt[:_SUMMARY_MAX_PROMPT_CHARS] + "\n\n[... truncated ...]"
    return prompt, total_chars


def _call_llm(system_prompt, user_prompt, model, json_mode=True, max_tokens=60000):
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")
    payload = {
        'model': model,
        'temperature': 0.2,
        'max_tokens': max_tokens,
        'messages': [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt},
        ],
    }
    if json_mode:
        payload['response_format'] = {'type': 'json_object'}

    # Retry on transient errors:
    #   429 = per-minute rate limit
    #   503 = model overloaded (Google side)
    #   500/502/504 = transient server error
    #   ReadTimeout / ConnectTimeout = network hiccup or slow model
    transient = {429, 500, 502, 503, 504}
    max_attempts = 4
    last_resp = None
    last_err = None
    for attempt in range(max_attempts):
        try:
            resp = requests.post(
                _LLM_URL,
                headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                json=payload,
                # Large prompts through 2.5-flash-lite can take several minutes.
                timeout=(30, 600),  # (connect, read)
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            if attempt < max_attempts - 1:
                wait = min(60, 10 * (2 ** attempt))  # 10, 20, 40, 60
                _rdio_log(f"Gemini network error (attempt {attempt + 1}/{max_attempts}): {e}; retrying in {wait:.0f}s")
                time.sleep(wait)
                continue
            raise RuntimeError(f"Gemini network error after {max_attempts} attempts: {e}")

        last_resp = resp
        if resp.ok:
            data = resp.json()
            try:
                choice = data['choices'][0]
                finish = choice.get('finish_reason') or choice.get('native_finish_reason') or ''
                if finish and finish not in ('stop', 'STOP'):
                    _rdio_log(f"LLM finish_reason={finish} (output may be truncated)")
            except Exception:
                pass
            return data['choices'][0]['message']['content'].strip()
        if resp.status_code in transient and attempt < max_attempts - 1:
            # Honour Retry-After if Gemini sent one; otherwise exponential backoff
            try:
                wait = float(resp.headers.get('Retry-After', '0'))
            except ValueError:
                wait = 0
            if wait <= 0:
                wait = min(60, 5 * (2 ** attempt))  # 5, 10, 20, 40
            _rdio_log(f"Gemini {resp.status_code} (attempt {attempt + 1}/{max_attempts}), waiting {wait:.0f}s")
            time.sleep(wait)
            continue
        # Non-transient error or out of retries
        body = resp.text[:1000] if resp.text else ''
        raise RuntimeError(f"Gemini HTTP {resp.status_code}: {body}")
    body = last_resp.text[:1000] if last_resp is not None and last_resp.text else ''
    raise RuntimeError(f"Gemini still {last_resp.status_code if last_resp else 'unreachable'} after {max_attempts} attempts: {body}")


_NATO_ALPHABET = {
    'alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf',
    'hotel', 'india', 'juliet', 'juliett', 'kilo', 'lima', 'mike', 'november',
    'oscar', 'papa', 'quebec', 'romeo', 'sierra', 'tango', 'uniform',
    'victor', 'whiskey', 'whisky', 'xray', 'x-ray', 'yankee', 'zulu',
}


_NSWPF_PATTERNS = re.compile(
    r'\b(nswpf|nsw\s*police|police\s*(?:officer|car|patrol|unit|vehicle)s?|'
    r'vkg|vka|highway\s*patrol|traffic\s*&?\s*highway|pursuit|'
    r'pol(?:air|police)|constable|sergeant|detective)\b',
    re.IGNORECASE,
)


def _validate_structured_against_transcripts(structured, calls):
    """Strip hallucinated content from the model output (new transcripts[] schema).

    Schema per incident (see prompts/rdio_hourly.txt):
      {title, type, status, severity, summary, locations[], agencies[],
       codes[], units[], window{start,end}, transcripts[], transcripts_truncated}
    where transcripts[] = [{time, call_id: int, text: str}, ...].

    This validator:
      - Ignores legacy `timeline[]` if present (logs a warning).
      - Drops transcripts[] entries whose call_id isn't in the hour's real calls.
      - Strips NSWPF-themed content from summary/title/transcripts[].text,
        and drops whole incidents that are exclusively NSWPF-themed.
      - Merges duplicate incidents via transcripts[].call_id overlap (>=50% or subset).
      - Hard-drops NATO-alphabet-only entries in units[] (Whisper hallucinations).
      - Drops units[] entries not mentioned in any transcripts[].text OR the
        bracketed unit labels from the raw call list (_RDIO_UNIT_LABELS).
      - Does NOT truncate transcripts[] — the LLM manages that itself and sets
        transcripts_truncated.
    """
    if not isinstance(structured, dict):
        return structured
    incidents = structured.get('incidents')
    if not isinstance(incidents, list):
        return structured

    # Build the set of genuine call_ids for this hour so we can reject any
    # transcripts[] row that references a fabricated id. The SQL query
    # aliases "id" AS call_id, so each row dict uses the 'call_id' key;
    # fall back to 'id' in case callers pass a differently-shaped list.
    known_call_ids = set()
    for c in calls:
        if not isinstance(c, dict):
            continue
        cid = c.get('call_id')
        if cid is None:
            cid = c.get('id')
        if cid is None:
            continue
        try:
            known_call_ids.add(int(cid))
        except (TypeError, ValueError):
            continue

    # Labels from rdio-scanner's units DB — authoritative for unit mentions
    # (the DB form often differs from the spoken/Whispered form).
    known_labels = {lbl.lower() for lbl in _RDIO_UNIT_LABELS.values() if lbl}

    digit_words = {
        '0': 'zero', '1': 'one', '2': 'two', '3': 'three', '4': 'four',
        '5': 'five', '6': 'six', '7': 'seven', '8': 'eight', '9': 'nine',
    }

    def _is_nato_only(uid: str) -> bool:
        tokens = (uid or '').strip().lower().split()
        if not tokens or len(tokens) > 2:
            return False
        if tokens[0] not in _NATO_ALPHABET:
            return False
        if len(tokens) == 1:
            return True
        return tokens[1].replace('-', '').isdigit()

    def _mentioned(uid: str, corpus: str, corpus_nosep: str) -> bool:
        u = (uid or '').strip().lower()
        if not u:
            return False
        if u in known_labels:
            return True
        if u in corpus:
            return True
        if u.replace('-', '').replace(' ', '') in corpus_nosep:
            return True
        if u.isdigit():
            spelled = ' '.join(digit_words[d] for d in u)
            if spelled in corpus:
                return True
        return False

    def _is_nswpf_text(s: str) -> bool:
        if not s or not isinstance(s, str):
            return False
        return bool(_NSWPF_PATTERNS.search(s))

    dropped_units = 0
    dropped_nato = 0
    dropped_nswpf = 0
    dropped_bad_cids = 0
    dropped_nswpf_incidents = 0
    legacy_timeline_seen = 0
    ALLOWED_AGENCIES = {'FRNSW', 'NSWA', 'RFS', 'SES'}

    incidents_in = [i for i in incidents if isinstance(i, dict)]

    # Pass 1: normalise per-incident — drop legacy timeline[], validate
    # transcripts[].call_id against the real hour, strip NSWPF content.
    normalised = []
    for inc in incidents_in:
        if 'timeline' in inc and 'transcripts' not in inc:
            # Mixed-schema fallback: the model is still emitting old shape.
            # We don't try to translate — just warn and drop the timeline; the
            # incident may still be usable via its other fields.
            legacy_timeline_seen += 1
            inc.pop('timeline', None)
        elif 'timeline' in inc:
            legacy_timeline_seen += 1
            inc.pop('timeline', None)

        # Validate + clean transcripts[]
        raw_trs = inc.get('transcripts')
        cleaned_trs = []
        if isinstance(raw_trs, list):
            for t in raw_trs:
                if not isinstance(t, dict):
                    dropped_bad_cids += 1
                    continue
                cid_raw = t.get('call_id')
                try:
                    cid = int(cid_raw) if cid_raw is not None else None
                except (TypeError, ValueError):
                    cid = None
                if cid is None or cid not in known_call_ids:
                    # Per spec: log each drop at debug, not warn. We don't
                    # have a debug-level logger here; fold into the summary
                    # rollup line emitted at the end of the function.
                    dropped_bad_cids += 1
                    continue
                t['call_id'] = cid
                # NSWPF strip on transcript text
                text = t.get('text')
                if isinstance(text, str) and _is_nswpf_text(text):
                    # Drop NSWPF-only transmissions from transcripts[]
                    dropped_nswpf += 1
                    continue
                cleaned_trs.append(t)
        inc['transcripts'] = cleaned_trs

        # NSWPF strip on summary/title
        summary_txt = inc.get('summary') or ''
        title_txt = inc.get('title') or ''
        if _is_nswpf_text(summary_txt):
            dropped_nswpf += 1
            inc['summary'] = _NSWPF_PATTERNS.sub('[redacted]', summary_txt)
        if _is_nswpf_text(title_txt):
            dropped_nswpf += 1
            inc['title'] = _NSWPF_PATTERNS.sub('[redacted]', title_txt)

        # Filter agencies to allowed set
        agencies = inc.get('agencies')
        if isinstance(agencies, list):
            filtered = [a for a in agencies if a in ALLOWED_AGENCIES]
            if len(filtered) != len(agencies):
                dropped_nswpf += (len(agencies) - len(filtered))
            inc['agencies'] = filtered

        # Whole-incident NSWPF drop: if after cleaning there are no transcripts
        # AND the surface fields are police-themed, drop the whole incident.
        surface_blob = ' '.join(str(inc.get(k) or '') for k in ('title', 'summary', 'type'))
        has_non_nswpf_transcript = any(
            isinstance(t, dict) and isinstance(t.get('text'), str) and not _is_nswpf_text(t['text'])
            for t in inc.get('transcripts') or []
        )
        if not has_non_nswpf_transcript and _is_nswpf_text(surface_blob):
            dropped_nswpf_incidents += 1
            continue

        normalised.append(inc)

    if legacy_timeline_seen:
        _rdio_log(f"validator warning: legacy timeline[] present in {legacy_timeline_seen} incident(s); ignored")

    # Pass 2: deduplicate incidents by transcripts[].call_id overlap.
    def _ids_of(inc):
        got = set()
        for t in inc.get('transcripts') or []:
            if not isinstance(t, dict):
                continue
            cid = t.get('call_id')
            if cid is None:
                continue
            try:
                got.add(int(cid))
            except (TypeError, ValueError):
                continue
        return got

    def _richness(inc):
        score = len((inc.get('summary') or ''))
        score += len(inc.get('transcripts') or []) * 50
        score += len(inc.get('units') or []) * 20
        return score

    id_sets = [_ids_of(i) for i in normalised]
    keep_idx = set(range(len(normalised)))
    dropped_dupes = 0
    for i in range(len(normalised)):
        if i not in keep_idx:
            continue
        for j in range(i + 1, len(normalised)):
            if j not in keep_idx:
                continue
            a, b = id_sets[i], id_sets[j]
            if not a or not b:
                continue
            smaller = min(len(a), len(b))
            overlap = len(a & b)
            if (overlap == smaller) or (overlap / smaller >= 0.5):
                loser = j if _richness(normalised[i]) >= _richness(normalised[j]) else i
                keep_idx.discard(loser)
                dropped_dupes += 1
                if loser == i:
                    break

    deduped = [normalised[k] for k in sorted(keep_idx)]

    # Pass 3: clean units[]. In the new schema units[] is a list of strings
    # (e.g. "HP 77"), but be lenient and accept legacy dicts too.
    for inc in deduped:
        # Build corpus from this incident's transcripts[].text
        trs = inc.get('transcripts') or []
        corpus_parts = []
        for t in trs:
            if isinstance(t, dict) and isinstance(t.get('text'), str):
                corpus_parts.append(t['text'].lower())
        corpus = ' '.join(corpus_parts)
        corpus_nosep = corpus.replace('-', '').replace(' ', '').replace('.', '')

        units = inc.get('units')
        if not isinstance(units, list):
            continue
        kept = []
        for u in units:
            if isinstance(u, str):
                uid = u
                carrier = u
            elif isinstance(u, dict):
                if u.get('agency') == 'NSWPF':
                    dropped_nswpf += 1
                    continue
                uid = u.get('id') or ''
                carrier = u
            else:
                dropped_units += 1
                continue
            if not uid:
                dropped_units += 1
                continue
            if _is_nato_only(uid):
                dropped_nato += 1
                continue
            if _mentioned(uid, corpus, corpus_nosep):
                kept.append(carrier)
            else:
                dropped_units += 1
        inc['units'] = kept

    # Also clean agency_stats of any NSWPF key (top-level, not per-incident)
    stats = structured.get('agency_stats')
    if isinstance(stats, dict) and 'NSWPF' in stats:
        stats.pop('NSWPF', None)
        dropped_nswpf += 1

    structured['incidents'] = deduped

    if dropped_bad_cids:
        # Per spec: log individual drops at debug. We don't have a debug level
        # logger here; emit a single rollup line instead so the console isn't
        # spammed for every hallucinated id.
        _rdio_log(f"validator dropped {dropped_bad_cids} transcripts[] row(s) with unknown call_id")

    bits = []
    if dropped_units:
        bits.append(f"{dropped_units} unit(s) not in transcripts")
    if dropped_nato:
        bits.append(f"{dropped_nato} NATO-alphabet hallucination(s)")
    if dropped_nswpf:
        bits.append(f"{dropped_nswpf} NSWPF ref(s)")
    if dropped_nswpf_incidents:
        bits.append(f"{dropped_nswpf_incidents} NSWPF-only incident(s)")
    if dropped_dupes:
        bits.append(f"{dropped_dupes} duplicate incident(s)")
    if bits:
        _rdio_log("validator dropped " + ", ".join(bits))

    structured['incident_count'] = len(deduped)
    return structured


import re as _summary_re


def _salvage_truncated_incidents(text):
    """If the LLM output was truncated mid-object inside `incidents[]`, walk
    the brace structure to find the last COMPLETE incident and synthesise a
    closing `]}`. Returns a repaired JSON string, or None if we can't salvage.
    """
    idx = text.find('"incidents"')
    if idx < 0:
        return None
    arr_start = text.find('[', idx)
    if arr_start < 0:
        return None

    depth = 0
    in_string = False
    escape = False
    last_complete_end = -1  # index (exclusive) after the last complete top-level {...}

    i = arr_start + 1
    n = len(text)
    while i < n:
        c = text[i]
        if escape:
            escape = False
        elif c == '\\':
            escape = True
        elif c == '"':
            in_string = not in_string
        elif not in_string:
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    last_complete_end = i + 1
            elif c == ']' and depth == 0:
                # Array closed naturally — caller's whole-text parse should
                # have succeeded. Nothing to salvage here.
                return None
        i += 1

    if last_complete_end < 0:
        return None  # no complete incident at all
    # Close the incidents array + outer object.
    return text[:last_complete_end] + ']}'


def _scrub_llm_typos(text):
    """Clean up common JSON typos Gemini occasionally emits, like a stray
    double opening quote before a field name (`""foo":` -> `"foo":`)."""
    # Collapse 2+ opening quotes before a field-name-like token
    return _summary_re.sub(r'"{2,}([A-Za-z_][\w]*)"\s*:', r'"\1":', text)


def _parse_summary_output(text):
    """Robustly extract a JSON object from an LLM response.

    Handles markdown code fences, BOMs, stray Gemini typos, and truncated
    outputs (by salvaging the last complete incident). Returns
    (overview_text, structured_dict_or_None). Logs a diagnostic line on
    total failure.
    """
    if not text:
        return '', None

    def _finish(data):
        if not isinstance(data, dict):
            return None
        overview = data.get('overview') or ''
        if not overview and data.get('quiet_hour'):
            overview = 'Quiet hour — no significant incidents detected.'
        return overview, data

    # Strip BOM + whitespace
    cleaned = text.lstrip('﻿').strip()

    # Strip markdown code fences
    if cleaned.startswith('```'):
        cleaned = cleaned.split('\n', 1)[-1] if '\n' in cleaned else cleaned[3:]
        if cleaned.endswith('```'):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()
    if cleaned.lower().startswith('json\n'):
        cleaned = cleaned[5:].strip()

    attempts = [('raw', cleaned)]

    # Attempt: extract the span between first { and last }
    first = cleaned.find('{')
    last = cleaned.rfind('}')
    if first >= 0 and last > first:
        attempts.append(('braces', cleaned[first:last + 1]))

    # Attempt: after scrubbing typos like `""foo":`
    scrubbed = _scrub_llm_typos(cleaned)
    if scrubbed != cleaned:
        attempts.append(('scrubbed', scrubbed))

    # Attempt: salvage truncated incidents[]
    salvaged = _salvage_truncated_incidents(scrubbed)
    if salvaged:
        attempts.append(('salvaged', salvaged))

    errors = []
    for label, candidate in attempts:
        try:
            result = _finish(json.loads(candidate))
            if result is not None:
                if label != 'raw':
                    _rdio_log(f"summary recovered via '{label}'")
                return result
            errors.append(f"{label}=not a dict")
        except Exception as e:
            errors.append(f"{label}={str(e)[:150]}")

    _rdio_log(
        f"summary parse failed; attempts={len(attempts)}; "
        f"errors={' | '.join(errors)}; len={len(text)}; "
        f"head={text[:120].replace(chr(10), ' ')!r}; "
        f"tail={text[-120:].replace(chr(10), ' ')!r}"
    )
    return text, None


def _save_rdio_summary(summary_type, period_start, period_end, day_date, hour_slot,
                      summary, call_count, transcript_chars, model, details,
                      release_at=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            '''
            INSERT INTO rdio_summaries
                (summary_type, period_start, period_end, day_date, hour_slot,
                 summary, call_count, transcript_chars, model, details, release_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (summary_type, period_start) DO UPDATE SET
                period_end = EXCLUDED.period_end,
                day_date = EXCLUDED.day_date,
                hour_slot = EXCLUDED.hour_slot,
                summary = EXCLUDED.summary,
                call_count = EXCLUDED.call_count,
                transcript_chars = EXCLUDED.transcript_chars,
                model = EXCLUDED.model,
                details = EXCLUDED.details,
                release_at = EXCLUDED.release_at,
                created_at = now()
            ''',
            (summary_type, period_start, period_end, day_date, hour_slot,
             summary, call_count, transcript_chars, model, json.dumps(details),
             release_at),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def generate_rdio_hourly_summary(hour_start_local, force=False, release_at=None):
    """Summarise the hour [hour_start_local, +1h). Returns dict or None if skipped.

    release_at: optional tz-aware UTC datetime. If set, the saved row won't be
    served by /api/summaries/latest until that time. Used by the scheduler so
    a prefetched summary (generated at :55) becomes visible at :59:50.
    """
    model = os.environ.get('LLM_MODEL', _LLM_DEFAULT_MODEL)
    local_tz = _summary_local_tz()
    hour_start_local = hour_start_local.astimezone(local_tz).replace(minute=0, second=0, microsecond=0)
    hour_end_local = hour_start_local + timedelta(hours=1)
    hour_slot = hour_end_local.hour if hour_end_local.hour != 0 else 24

    start_utc = hour_start_local.astimezone(timezone.utc)
    end_utc = hour_end_local.astimezone(timezone.utc)

    calls = _rdio_fetch_calls_between(start_utc, end_utc)
    calls = _dedupe_calls(calls)
    # Skip only when called manually with no data AND no release gate.
    # Scheduled runs (release_at set) must ALWAYS save something so that
    # /api/summaries/latest reflects the current hour — even if the upstream
    # pipe went quiet and there were zero transcripts.
    if not calls and not force and release_at is None:
        _rdio_log(f"hourly {hour_start_local.isoformat()}: no transcripts, skipping")
        return None

    period_label = (
        f"{hour_start_local.strftime('%Y-%m-%d %H:%M %Z')} "
        f"to {hour_end_local.strftime('%H:%M %Z')}"
    )
    prompt, total_chars = _format_rdio_prompt(calls, period_label)
    if not calls:
        summary_text = "No radio traffic with transcripts was recorded during this hour."
        structured = None
    else:
        raw = _call_llm(
            _load_rdio_prompt('hourly', _HOURLY_PROMPT_FALLBACK),
            prompt,
            model,
        )
        summary_text, structured = _parse_summary_output(raw)
        structured = _validate_structured_against_transcripts(structured, calls)

    details = {'period_label': period_label, 'tz': _SUMMARY_TZ_NAME}
    if structured is not None:
        details['structured'] = structured
    _save_rdio_summary(
        'hourly', start_utc, end_utc, hour_start_local.date(), hour_slot,
        summary_text, len(calls), total_chars, model, details,
        release_at=release_at,
    )
    rel_str = f", releases at {release_at.isoformat()}" if release_at else ""
    _rdio_log(f"hourly {hour_start_local.isoformat()} saved ({len(calls)} calls, {total_chars} chars{rel_str})")
    return {'hour_slot': hour_slot, 'call_count': len(calls)}


def generate_rdio_recent_summary(n=500, force=False):
    """
    One-off: summarise the most recent N transcripts regardless of clock hour.
    Saves as summary_type='adhoc' with period_start=now so it never collides
    with scheduled hourly rows. Uses the hourly prompt.
    """
    n = max(1, min(int(n), 5000))
    model = os.environ.get('LLM_MODEL', _LLM_DEFAULT_MODEL)
    local_tz = _summary_local_tz()

    with _RdioConn() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(
                '''
                SELECT "id" AS call_id, "dateTime" AS date_time, "system", "talkgroup", "transcript",
                       "source", "sources"
                FROM "rdioScannerCalls"
                WHERE "transcript" IS NOT NULL
                  AND length(btrim("transcript")) >= 2
                ORDER BY "dateTime" DESC
                LIMIT %s
                ''',
                (n,),
            )
            calls = list(reversed(cur.fetchall()))
        finally:
            cur.close()

    calls = _dedupe_calls(calls)

    if not calls and not force:
        _rdio_log(f"recent N={n}: no transcripts, skipping")
        return None

    # rdio-scanner `dateTime` is naive UTC — tag before converting.
    def _as_utc(d):
        return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)
    start_utc = _as_utc(calls[0]['date_time']) if calls else datetime.now(timezone.utc)
    end_utc = _as_utc(calls[-1]['date_time']) if calls else datetime.now(timezone.utc)
    start_local = start_utc.astimezone(local_tz)
    end_local = end_utc.astimezone(local_tz)
    period_label = (
        f"Last {len(calls)} transcripts "
        f"({start_local.strftime('%Y-%m-%d %H:%M %Z')} → {end_local.strftime('%H:%M %Z')})"
    )
    prompt, total_chars = _format_rdio_prompt(calls, period_label)
    try:
        raw = _call_llm(
            _load_rdio_prompt('hourly', _HOURLY_PROMPT_FALLBACK),
            prompt,
            model,
        )
        summary_text, structured = _parse_summary_output(raw)
        structured = _validate_structured_against_transcripts(structured, calls)
    except Exception as e:
        _rdio_log(f"recent llm error: {e}")
        return None

    now_utc = datetime.now(timezone.utc)
    details = {
        'tz': _SUMMARY_TZ_NAME,
        'source': 'last_n',
        'n': len(calls),
        'requested_n': n,
        'period_label': period_label,
        'transcripts_start': start_utc.isoformat(),
        'transcripts_end': end_utc.isoformat(),
    }
    if structured is not None:
        details['structured'] = structured
    _save_rdio_summary(
        'adhoc', now_utc, now_utc,
        start_local.date(), None,
        summary_text, len(calls), total_chars, model, details,
    )
    _rdio_log(f"recent saved (N={len(calls)}, {total_chars} chars)")
    return {
        'call_count': len(calls),
        'requested_n': n,
        'transcript_chars': total_chars,
        'transcripts_start': start_utc.isoformat(),
        'transcripts_end': end_utc.isoformat(),
        'summary': summary_text,
        'structured': structured,
    }


def _rdio_summary_catchup():
    """On startup: fill missing hourly summaries.

    - Always try the previous hour (release immediately if missing).
    - If we're already past :55 of the current hour, also try the current
      hour (with the same release_at timing the scheduler would have used).
    """
    local_tz = _summary_local_tz()
    now_local = datetime.now(local_tz)
    prev_hour_start = (now_local - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    def _row_exists(hour_start_local):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM rdio_summaries WHERE summary_type = 'hourly' AND period_start = %s",
                (hour_start_local.astimezone(timezone.utc),),
            )
            return cur.fetchone() is not None
        finally:
            cur.close()
            conn.close()

    try:
        if not _row_exists(prev_hour_start):
            # force=True so empty hours write the preset stub instead of silently
            # skipping — the UI card should never be missing a completed hour.
            # Catch-up rows land with no release_at so they're live immediately.
            generate_rdio_hourly_summary(prev_hour_start, force=True)
    except Exception as e:
        _rdio_log(f"catch-up prev-hour error: {e}")

    # If we've restarted inside the prefetch window (HH:55+), also kick off the
    # current hour's summary so it still lands in time for the top-of-hour
    # release. Mirrors the scheduler's release_at gating.
    if now_local.minute >= 55:
        current_hour_start = now_local.replace(minute=0, second=0, microsecond=0)
        next_hour_top = current_hour_start + timedelta(hours=1)
        release_at = next_hour_top.astimezone(timezone.utc)
        try:
            if not _row_exists(current_hour_start):
                generate_rdio_hourly_summary(
                    current_hour_start, force=True, release_at=release_at,
                )
        except Exception as e:
            _rdio_log(f"catch-up current-hour error: {e}")


def _rdio_summary_loop():
    _rdio_log("scheduler thread started")
    _rdio_summary_catchup()
    local_tz = _summary_local_tz()
    while not _shutdown_event.is_set():
        # Prefetch pattern:
        #   • Fire at HH:55 of the in-progress hour [HH, HH+1).
        #   • Send to Gemini immediately — typically returns by HH:57.
        #   • Save with release_at = HH+1:00 so /api/summaries/latest keeps
        #     serving the previous summary until the hour flips, then pivots
        #     to the new one the instant the clock hits the top.
        # force=True still applies so empty hours get a preset stub.
        now_local = datetime.now(local_tz)
        fire_at = now_local.replace(minute=55, second=0, microsecond=0)
        if fire_at <= now_local:
            fire_at += timedelta(hours=1)
        wait_secs = max(5, (fire_at - now_local).total_seconds())
        if _shutdown_event.wait(timeout=wait_secs):
            break

        # fire_at is HH:55 in local time. The hour we're summarising is
        # [HH, HH+1) and will finish at next_hour_top.
        hour_start_local = fire_at.replace(minute=0, second=0, microsecond=0)
        next_hour_top = hour_start_local + timedelta(hours=1)
        release_at = next_hour_top.astimezone(timezone.utc)

        start_wall = time.time()
        try:
            generate_rdio_hourly_summary(
                hour_start_local, force=True, release_at=release_at,
            )
        except Exception as e:
            _rdio_log(f"hourly job error: {e}")
        else:
            elapsed = time.time() - start_wall
            budget_left = (release_at - datetime.now(timezone.utc)).total_seconds()
            _rdio_log(
                f"hourly {hour_start_local.strftime('%H:%M')}–"
                f"{next_hour_top.strftime('%H:%M')} done in {elapsed:.1f}s "
                f"(releases in {budget_left:.0f}s)"
            )
            if budget_left < 0:
                _rdio_log(
                    f"WARNING: hourly finished {-budget_left:.0f}s past "
                    f"release_at — summary live immediately"
                )


def start_rdio_summary_thread():
    """Start the background scheduler. No-op if DB/API key missing."""
    global _rdio_summary_thread
    if _rdio_summary_thread and _rdio_summary_thread.is_alive():
        return
    if not _rdio_is_configured():
        _rdio_log("RDIO_DATABASE_URL not set — summary scheduler disabled")
        return
    if not os.environ.get('GEMINI_API_KEY'):
        _rdio_log("GEMINI_API_KEY not set — summary scheduler disabled")
        return
    _rdio_summary_thread = threading.Thread(
        target=_rdio_summary_loop, daemon=True, name='rdio-summary')
    _rdio_summary_thread.start()
    Log.startup("rdio-scanner summary scheduler started")


def _row_to_summary(row):
    """Map a rdio_summaries row (dict) to API shape."""
    release_at = row.get('release_at') if hasattr(row, 'get') else row['release_at'] if 'release_at' in row else None
    return {
        'id': row['id'],
        'type': row['summary_type'],
        'period_start': row['period_start'].isoformat() if row['period_start'] else None,
        'period_end': row['period_end'].isoformat() if row['period_end'] else None,
        'day_date': row['day_date'].isoformat() if row['day_date'] else None,
        'hour_slot': row['hour_slot'],
        'summary': row['summary'],
        'call_count': row['call_count'],
        'transcript_chars': row['transcript_chars'],
        'model': row['model'],
        'details': row['details'] or {},
        'release_at': release_at.isoformat() if release_at else None,
        'created_at': row['created_at'].isoformat() if row['created_at'] else None,
    }


@app.route('/api/summaries/latest')
def summaries_latest():
    """Return the most recent hourly-or-adhoc summary.

    The `hourly` field returns whichever is more recent: the last scheduled
    hourly run or a manual ad-hoc trigger.
    """
    try:
        conn = get_conn_dict()
        try:
            cur = conn.cursor()
            # Order by created_at DESC, not period_start: an ad-hoc row has
            # period_start = now() (the trigger time), which would otherwise
            # always beat a scheduled hourly with period_start = hour-top.
            # "Latest" should mean "most recently generated".
            try:
                cur.execute(
                    "SELECT * FROM rdio_summaries "
                    "WHERE summary_type IN ('hourly', 'adhoc') "
                    "  AND (release_at IS NULL OR release_at <= now()) "
                    "ORDER BY created_at DESC LIMIT 1"
                )
            except psycopg2.errors.UndefinedColumn:
                # release_at column doesn't exist yet — fall back and let the
                # startup migration fix it on the next restart.
                conn.rollback()
                Log.warn("rdio_summaries.release_at missing; serving unfiltered 'latest'")
                cur.execute(
                    "SELECT * FROM rdio_summaries "
                    "WHERE summary_type IN ('hourly', 'adhoc') "
                    "ORDER BY created_at DESC LIMIT 1"
                )
            hourly = cur.fetchone()
            cur.close()
        finally:
            conn.close()
        return jsonify({
            'hourly': _row_to_summary(hourly) if hourly else None,
        })
    except Exception as e:
        Log.error(f"/api/summaries/latest error: {type(e).__name__}: {e}")
        return jsonify({'error': f"{type(e).__name__}: {e}"}), 500


@app.route('/api/summaries')
def summaries_search():
    """
    Search summaries by date and hour.

    Query params:
        type      optional - 'hourly' | 'adhoc' (default: any)
        date      optional - YYYY-MM-DD (filters day_date)
        hour      optional - 1..24 hour slot (implies type=hourly)
        date_from optional - YYYY-MM-DD (inclusive)
        date_to   optional - YYYY-MM-DD (inclusive)
        limit     optional - default 50, max 500
        offset    optional - default 0
    """
    try:
        stype = request.args.get('type')
        date = request.args.get('date')
        hour = request.args.get('hour', type=int)
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        limit = min(max(request.args.get('limit', default=50, type=int), 1), 500)
        offset = max(request.args.get('offset', default=0, type=int), 0)

        clauses = []
        params = []
        if hour is not None:
            if hour < 1 or hour > 24:
                return jsonify({'error': 'hour must be 1..24'}), 400
            clauses.append("hour_slot = %s")
            params.append(hour)
            if stype is None:
                stype = 'hourly'
        if stype:
            if stype not in ('hourly', 'adhoc'):
                return jsonify({'error': "type must be 'hourly' or 'adhoc'"}), 400
            clauses.append("summary_type = %s")
            params.append(stype)
        if date:
            clauses.append("day_date = %s")
            params.append(date)
        if date_from:
            clauses.append("day_date >= %s")
            params.append(date_from)
        if date_to:
            clauses.append("day_date <= %s")
            params.append(date_to)

        # Hide embargoed rows (prefetched summaries not yet released).
        clauses.append("(release_at IS NULL OR release_at <= now())")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            f"SELECT * FROM rdio_summaries {where} "
            f"ORDER BY period_start DESC LIMIT %s OFFSET %s"
        )
        params.extend([limit, offset])

        count_sql = f"SELECT COUNT(*) AS n FROM rdio_summaries {where}"
        count_params = params[:-2]

        conn = get_conn_dict()
        try:
            cur = conn.cursor()
            cur.execute(count_sql, count_params)
            total = cur.fetchone()['n']
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()

        return jsonify({
            'total': total,
            'limit': limit,
            'offset': offset,
            'results': [_row_to_summary(r) for r in rows],
        })
    except Exception as e:
        Log.error(f"/api/summaries error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/rdio/transcripts/search')
def rdio_transcripts_search():
    """Full-text search over rdio-scanner call transcripts.

    Query params:
        q           keyword(s) to ILIKE against transcript text (optional if call_id given)
        call_id     fetch a specific call by its rdio-scanner ID (optional)
        system      numeric system id filter (optional)
        talkgroup   numeric talkgroup id filter (optional)
        date        YYYY-MM-DD (local day, uses SUMMARY_TZ) — shortcut for date_from/date_to
        date_from   YYYY-MM-DD (inclusive, UTC)
        date_to     YYYY-MM-DD (inclusive, UTC)
        time_from   HH:MM local — narrows date/date_from..date_to to after this time of day
        time_to     HH:MM local — narrows to before this time of day
        limit       default 20, max 200
        offset      pagination offset, default 0
        order       'asc' | 'desc' (default 'desc' = newest first)

    Returns:
        { total, limit, offset, results: [ {id, datetime, system, system_label,
          talkgroup, talkgroup_label, transcript, radio_id, radio_label,
          call_url} ] }
    """
    if not _rdio_is_configured():
        return jsonify({'error': 'RDIO_DATABASE_URL not configured'}), 503
    try:
        q = (request.args.get('q') or '').strip()
        call_id = request.args.get('call_id', type=int)
        if not q and not call_id:
            return jsonify({'error': 'q (keyword) or call_id is required'}), 400

        system_id = request.args.get('system', type=int)
        talkgroup_id = request.args.get('talkgroup', type=int)
        date = request.args.get('date')
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        time_from = request.args.get('time_from')
        time_to = request.args.get('time_to')
        limit = min(max(request.args.get('limit', default=20, type=int), 1), 200)
        offset = max(request.args.get('offset', default=0, type=int), 0)
        order = 'ASC' if (request.args.get('order', 'desc').lower() == 'asc') else 'DESC'

        local_tz = _summary_local_tz()

        def _local_date_to_utc_bounds(date_str, start_of_day=True):
            """Convert YYYY-MM-DD in local tz to a naive UTC datetime."""
            try:
                d = datetime.strptime(date_str, '%Y-%m-%d')
            except (TypeError, ValueError):
                return None
            if start_of_day:
                dt = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=local_tz)
            else:
                dt = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=local_tz)
            return dt.astimezone(timezone.utc).replace(tzinfo=None)

        # Build WHERE clause
        clauses = []
        params = []
        if call_id is not None:
            clauses.append('"id" = %s')
            params.append(call_id)
        else:
            clauses.append('"transcript" IS NOT NULL')
            # Comma-separated `q` becomes an OR of ILIKE patterns — "fire,crash,
            # police" matches any transcript mentioning any of the three. Single
            # terms still work (no commas → one ILIKE). Empty/short fragments
            # (len < 2) are dropped so a stray trailing comma doesn't blow up.
            terms = [t.strip() for t in q.split(',')]
            terms = [t for t in terms if len(t) >= 2]
            if not terms:
                return jsonify({'error': 'q must contain at least one term of 2+ chars'}), 400
            if len(terms) == 1:
                clauses.append('"transcript" ILIKE %s')
                params.append(f'%{terms[0]}%')
            else:
                ilike_parts = ['"transcript" ILIKE %s'] * len(terms)
                clauses.append('(' + ' OR '.join(ilike_parts) + ')')
                params.extend(f'%{t}%' for t in terms)

        if system_id is not None:
            clauses.append('"system" = %s')
            params.append(system_id)
        if talkgroup_id is not None:
            clauses.append('"talkgroup" = %s')
            params.append(talkgroup_id)

        # `date` is a convenience shortcut when both from+to would equal
        if date and not date_from and not date_to:
            date_from = date
            date_to = date

        if date_from:
            dt = _local_date_to_utc_bounds(date_from, start_of_day=True)
            if dt is None:
                return jsonify({'error': 'date_from must be YYYY-MM-DD'}), 400
            clauses.append('"dateTime" >= %s')
            params.append(dt)
        if date_to:
            dt = _local_date_to_utc_bounds(date_to, start_of_day=False)
            if dt is None:
                return jsonify({'error': 'date_to must be YYYY-MM-DD'}), 400
            clauses.append('"dateTime" <= %s')
            params.append(dt)

        # Narrow by local time-of-day (applied to each matching day)
        def _parse_hm(s):
            try:
                h, m = s.split(':')
                h, m = int(h), int(m)
                if 0 <= h < 24 and 0 <= m < 60:
                    return h, m
            except Exception:
                pass
            return None
        if time_from:
            hm = _parse_hm(time_from)
            if hm is None:
                return jsonify({'error': 'time_from must be HH:MM'}), 400
            clauses.append(
                "EXTRACT(HOUR FROM \"dateTime\" AT TIME ZONE 'UTC' AT TIME ZONE %s) * 60 "
                "+ EXTRACT(MINUTE FROM \"dateTime\" AT TIME ZONE 'UTC' AT TIME ZONE %s) >= %s"
            )
            params.extend([_SUMMARY_TZ_NAME, _SUMMARY_TZ_NAME, hm[0] * 60 + hm[1]])
        if time_to:
            hm = _parse_hm(time_to)
            if hm is None:
                return jsonify({'error': 'time_to must be HH:MM'}), 400
            clauses.append(
                "EXTRACT(HOUR FROM \"dateTime\" AT TIME ZONE 'UTC' AT TIME ZONE %s) * 60 "
                "+ EXTRACT(MINUTE FROM \"dateTime\" AT TIME ZONE 'UTC' AT TIME ZONE %s) <= %s"
            )
            params.extend([_SUMMARY_TZ_NAME, _SUMMARY_TZ_NAME, hm[0] * 60 + hm[1]])

        where = 'WHERE ' + ' AND '.join(clauses)

        with _RdioConn() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            try:
                cur.execute(f'SELECT COUNT(*) AS n FROM "rdioScannerCalls" {where}', params)
                total = cur.fetchone()['n']
                cur.execute(
                    f'SELECT "id", "dateTime" AS date_time, "system", "talkgroup", '
                    f'"transcript", "source", "sources" FROM "rdioScannerCalls" '
                    f'{where} ORDER BY "dateTime" {order} LIMIT %s OFFSET %s',
                    params + [limit, offset],
                )
                rows = cur.fetchall()
            finally:
                cur.close()

        results = []
        for row in rows:
            sys_label, tg_label = _rdio_resolve_labels(row['system'], row['talkgroup'])
            rid = _extract_radio_id(row)
            dt = row['date_time']
            if dt is not None and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            results.append({
                'id': row['id'],
                'datetime': dt.isoformat() if dt else None,
                'system': row['system'],
                'system_label': sys_label,
                'talkgroup': row['talkgroup'],
                'talkgroup_label': tg_label,
                'transcript': row['transcript'],
                'radio_id': rid,
                'radio_label': _RDIO_UNIT_LABELS.get(rid) if rid else None,
                'call_url': f'https://radio.forcequit.xyz/?call={row["id"]}',
            })
        return jsonify({
            'total': total,
            'limit': limit,
            'offset': offset,
            'query': q,
            'call_id': call_id,
            'results': results,
        })
    except Exception as e:
        Log.error(f"/api/rdio/transcripts/search error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/rdio/calls/<int:call_id>')
def rdio_call_detail(call_id):
    """Return one rdio-scanner call with resolved labels."""
    if not _rdio_is_configured():
        return jsonify({'error': 'RDIO_DATABASE_URL not configured'}), 503
    try:
        with _RdioConn() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            try:
                cur.execute(
                    'SELECT "id", "dateTime" AS date_time, "system", "talkgroup", '
                    '"transcript", "source", "sources" FROM "rdioScannerCalls" '
                    'WHERE "id" = %s',
                    (call_id,),
                )
                row = cur.fetchone()
            finally:
                cur.close()
        if not row:
            return jsonify({'error': 'call not found'}), 404
        sys_label, tg_label = _rdio_resolve_labels(row['system'], row['talkgroup'])
        rid = _extract_radio_id(row)
        dt = row['date_time']
        if dt is not None and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return jsonify({
            'id': row['id'],
            'datetime': dt.isoformat() if dt else None,
            'system': row['system'],
            'system_label': sys_label,
            'talkgroup': row['talkgroup'],
            'talkgroup_label': tg_label,
            'transcript': row['transcript'],
            'radio_id': rid,
            'radio_label': _RDIO_UNIT_LABELS.get(rid) if rid else None,
            'call_url': f'https://radio.forcequit.xyz/?call={row["id"]}',
        })
    except Exception as e:
        Log.error(f"/api/rdio/calls/{call_id} error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/summaries/trigger', methods=['POST', 'GET'])
def summaries_trigger():
    """Manually trigger a summary generation.
    Body (JSON) or query params:
        type: 'hourly' | 'recent'
        hour_start: ISO8601 local time for hourly (optional, default previous hour)
        last_n: N most recent transcripts for 'recent' (optional, default 500, max 5000)
    """
    try:
        local_tz = _summary_local_tz()
        body = request.get_json(silent=True) or {}
        # Allow GET + query params too for easy curl/browser triggering
        args = request.args
        def p(key, default=None):
            return body.get(key, args.get(key, default))

        stype = p('type', 'hourly')
        if stype == 'hourly':
            hs = p('hour_start')
            if hs:
                hour_start = datetime.fromisoformat(hs)
                if hour_start.tzinfo is None:
                    hour_start = hour_start.replace(tzinfo=local_tz)
            else:
                hour_start = (datetime.now(local_tz) - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            result = generate_rdio_hourly_summary(hour_start, force=True)
        elif stype == 'recent':
            try:
                n = int(p('last_n', 500))
            except (TypeError, ValueError):
                return jsonify({'error': 'last_n must be an integer'}), 400
            if n < 1 or n > 5000:
                return jsonify({'error': 'last_n must be between 1 and 5000'}), 400
            # dry_run=1 returns the formatted prompt WITHOUT calling the LLM
            dry_run = str(p('dry_run', '') or '').lower() in ('1', 'true', 'yes')
            # sync=1 forces synchronous execution (small N only). By default
            # recent triggers run in a background thread so Cloudflare's 100s
            # edge timeout doesn't kill long Gemini calls.
            sync = str(p('sync', '') or '').lower() in ('1', 'true', 'yes')
            if not dry_run and not sync:
                def _run_recent_bg(_n):
                    try:
                        generate_rdio_recent_summary(_n, force=True)
                    except Exception as e:
                        _rdio_log(f"background recent error: {e}")
                threading.Thread(
                    target=_run_recent_bg, args=(n,), daemon=True,
                    name=f'rdio-recent-{n}',
                ).start()
                return jsonify({
                    'ok': True,
                    'queued': True,
                    'requested_n': n,
                    'message': (
                        'Summary is running in the background. '
                        'Poll /api/summaries/latest to see it when ready '
                        '(typically 30–120s for large N).'
                    ),
                })
            if dry_run:
                with _RdioConn() as conn:
                    cur = conn.cursor(cursor_factory=RealDictCursor)
                    try:
                        cur.execute(
                            '''
                            SELECT "dateTime" AS date_time, "system", "talkgroup", "transcript"
                            FROM "rdioScannerCalls"
                            WHERE "transcript" IS NOT NULL
                              AND length(btrim("transcript")) >= 2
                            ORDER BY "dateTime" DESC
                            LIMIT %s
                            ''',
                            (n,),
                        )
                        calls = list(reversed(cur.fetchall()))
                    finally:
                        cur.close()
                calls = _dedupe_calls(calls)
                formatted, total_chars = _format_rdio_prompt(
                    calls,
                    f"Last {len(calls)} transcripts (dry run)",
                )
                return jsonify({
                    'ok': True,
                    'dry_run': True,
                    'call_count': len(calls),
                    'transcript_chars': total_chars,
                    'user_prompt': formatted,
                    'system_prompt': _load_rdio_prompt('hourly', _HOURLY_PROMPT_FALLBACK),
                })
            result = generate_rdio_recent_summary(n, force=True)
        else:
            return jsonify({'error': "type must be 'hourly' or 'recent'"}), 400
        return jsonify({'ok': True, 'result': result})
    except Exception as e:
        Log.error(f"/api/summaries/trigger error: {e}")
        return jsonify({'error': str(e)}), 500


# ==================== DASHBOARD ====================
# Discord-OAuth-authenticated config dashboard for the NSW PSN Discord bot.
#
# Reads/writes the bot's Postgres DB (alert_presets + *_mute_state) via a
# separate connection pool (BOT_DATA_DATABASE_URL). Discord OAuth2 is used
# to identify the user and enumerate their guilds; Manage-Channels permission
# on the guild + bot-presence in that guild are both required for any guild
# scoped endpoint. Session state is a signed cookie — no server-side store.
#
# ---------------------------------------------------------------------------
# API CONTRACT
# ---------------------------------------------------------------------------
#
# Base:     /api/dashboard
# Auth:     Session cookie `nswpsn_dash_sess` (HttpOnly, SameSite=Lax, Secure)
# Errors:   JSON body `{"error": "<code>", "message": "<human>"}`
#           Codes: dashboard_disabled, missing_session_secret, invalid_session,
#                  session_expired, forbidden, bot_not_in_guild, guild_not_found,
#                  channel_not_found, bad_request, discord_error, rate_limited,
#                  upstream_error, db_error
#
# OAuth flow
#   GET  /api/dashboard/auth/login[?next=/path]
#        -> 302 redirect to Discord authorize URL (sets `nswpsn_dash_oauth` state cookie)
#   GET  /api/dashboard/auth/callback?code=...&state=...
#        -> exchanges code, writes `nswpsn_dash_sess`, 302 to {PUBLIC_BASE_URL}/dashboard
#           (or ?next=/path if preserved via state)
#   POST /api/dashboard/auth/logout
#        -> 204, clears session cookie
#
# Identity
#   GET  /api/dashboard/me  (auth required)
#        -> 200 {
#             "user":  {"id": str, "username": str, "avatar_url": str|null},
#             "guilds": [
#               {"id": str, "name": str, "icon_url": str|null,
#                "has_bot": bool, "manage_channels": bool}
#             ]
#           }
#
# Guild data (auth + MANAGE_CHANNELS + bot-in-guild required)
#   GET  /api/dashboard/guilds/<guild_id>/channels
#        -> 200 [{"id": str, "name": str, "position": int, "parent_id": str|null}]
#   GET  /api/dashboard/guilds/<guild_id>/roles
#        -> 200 [{"id": str, "name": str, "color": int, "position": int}]
#   GET    /api/dashboard/guilds/<guild_id>/presets   -> 200 {"presets": [Preset,...]}
#   POST   /api/dashboard/guilds/<guild_id>/presets   -> 201 {"preset": Preset}
#   PATCH  /api/dashboard/guilds/<guild_id>/presets/<preset_id>  -> 200 {"preset": Preset}
#   DELETE /api/dashboard/guilds/<guild_id>/presets/<preset_id>  -> 200 {"ok": true}
#   PUT    /api/dashboard/guilds/<guild_id>/presets/<preset_id>/type-overrides/<alert_type>
#          Body: {"enabled"?: bool, "enabled_ping"?: bool}  -> 200 {"preset": Preset}
#   DELETE /api/dashboard/guilds/<guild_id>/presets/<preset_id>/type-overrides/<alert_type>
#          -> 200 {"preset": Preset}
#   GET    /api/dashboard/guilds/<guild_id>/mute-state
#          -> 200 {"guild": {enabled, enabled_ping}, "channels": [{channel_id, enabled, enabled_ping},...]}
#   PUT    /api/dashboard/guilds/<guild_id>/mute-state/guild
#          Body: {"enabled"?: bool, "enabled_ping"?: bool}  -> 200 {"guild": {...}}
#   DELETE /api/dashboard/guilds/<guild_id>/mute-state/guild  -> 200 {"ok": true}
#   PUT    /api/dashboard/guilds/<guild_id>/mute-state/channels/<channel_id>
#          Body: {"enabled"?: bool, "enabled_ping"?: bool}  -> 200 {"channel": {...}}
#   DELETE /api/dashboard/guilds/<guild_id>/mute-state/channels/<channel_id> -> 200 {"ok": true}
#
# Preset shape:
#   {"id": int, "channel_id": str, "name": str, "alert_types": [str,...],
#    "pager_enabled": bool, "pager_capcodes": str|null, "role_ids": [str,...],
#    "enabled": bool, "enabled_ping": bool,
#    "type_overrides": {"<alert_type>": {"enabled": bool, "enabled_ping": bool}, ...},
#    "created_at": ISO, "updated_at": ISO}
#
# Session cookie
#   Name:    nswpsn_dash_sess
#   Value:   base64url(json_payload) + "." + base64url(hmac_sha256_sig)
#   Payload: {"uid": str, "username": str, "avatar": str|null,
#             "guilds": [{"id": str, "name": str, "icon": str|null,
#                         "permissions": str}],
#             "gfresh": int, "exp": int, "iat": int}
#   Signed with DASHBOARD_SESSION_SECRET, 24h expiry.
#   Guild list refreshed on /me when older than 10 minutes.

_BOT_DB_POOL = None
_BOT_DB_POOL_LOCK = threading.Lock()

_DISCORD_API_BASE = 'https://discord.com/api/v10'
_DASH_SESSION_COOKIE = 'nswpsn_dash_sess'
_DASH_OAUTH_COOKIE = 'nswpsn_dash_oauth'
_DASH_SESSION_TTL = 24 * 60 * 60          # 24 hours
_DASH_OAUTH_STATE_TTL = 10 * 60           # 10 minutes
_DASH_GUILD_REFRESH_INTERVAL = 10 * 60    # 10 minutes
_DISCORD_CHANNEL_CACHE_TTL = 60           # seconds
_DISCORD_ROLE_CACHE_TTL = 60              # seconds

_MANAGE_CHANNELS = 0x10
_ADMINISTRATOR = 0x8

def _dash_admin_ids():
    """Dashboard super-admin Discord user IDs, from DASHBOARD_ADMIN_IDS env
    (comma-separated). Re-read every call so edits to .env take effect on
    pm2 reload without a code redeploy."""
    raw = os.environ.get('DASHBOARD_ADMIN_IDS', '') or ''
    return {p.strip() for p in raw.split(',') if p.strip()}

def _dash_is_admin(session):
    uid = str(session.get('uid') or '')
    return bool(uid) and uid in _dash_admin_ids()

# Mirror of discord-bot/bot.py ALERT_TYPES — duplicated here because the
# dashboard must work without importing bot.py (different process).
_DASH_ALERT_TYPES = [
    'rfs',
    'bom_land', 'bom_marine',
    'traffic_incident', 'traffic_roadwork', 'traffic_flood',
    'traffic_fire', 'traffic_majorevent',
    'endeavour_current', 'endeavour_planned',
    'ausgrid',
    'essential_planned', 'essential_future',
    'waze_hazard', 'waze_jam', 'waze_police', 'waze_roadwork',
    'user_incident',
    'radio_summary',
]

# Per-guild in-memory caches for Discord REST lookups.
_dash_discord_cache = {}  # {(kind, guild_id): (ts, data)}
_dash_discord_cache_lock = threading.Lock()


def _dash_err(code, message, status):
    """Consistent JSON error body."""
    return jsonify({'error': code, 'message': message}), status


def _dash_b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def _dash_b64url_decode(s):
    pad = '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _dash_get_session_secret():
    """Fetch DASHBOARD_SESSION_SECRET — first-access fatal, not startup fatal."""
    secret = os.environ.get('DASHBOARD_SESSION_SECRET')
    if not secret:
        return None
    return secret.encode('utf-8') if isinstance(secret, str) else secret


def _dash_sign(payload_bytes, secret):
    import hmac as _hmac
    return _hmac.new(secret, payload_bytes, hashlib.sha256).digest()


def _dash_make_cookie(payload, secret):
    """Sign+encode a JSON payload → `<b64_payload>.<b64_sig>`."""
    import hmac as _hmac  # noqa: F401 (use stdlib; keeps `hmac` import local)
    raw = json.dumps(payload, separators=(',', ':'), sort_keys=True).encode('utf-8')
    p = _dash_b64url(raw)
    sig = _dash_sign(raw, secret)
    s = _dash_b64url(sig)
    return f"{p}.{s}"


def _dash_parse_cookie(cookie_value, secret):
    """Returns the payload dict or None on bad signature / malformed."""
    import hmac as _hmac
    if not cookie_value or '.' not in cookie_value:
        return None
    try:
        p_b64, s_b64 = cookie_value.split('.', 1)
        raw = _dash_b64url_decode(p_b64)
        sig = _dash_b64url_decode(s_b64)
    except Exception:
        return None
    expected = _dash_sign(raw, secret)
    if not _hmac.compare_digest(expected, sig):
        return None
    try:
        return json.loads(raw.decode('utf-8'))
    except Exception:
        return None


def _dash_is_https():
    """Whether to set Secure on cookies. Honours X-Forwarded-Proto (nginx)."""
    xf = request.headers.get('X-Forwarded-Proto', '').lower()
    if xf:
        return xf == 'https'
    return request.is_secure


# Cross-subdomain cookie: the dashboard UI is served from nswpsn.forcequit.xyz
# but the Flask backend (and OAuth callback) live at api.forcequit.xyz. To
# share the session cookie between both, scope it to the parent domain.
# Override via DASHBOARD_COOKIE_DOMAIN if running somewhere else.
_DASH_COOKIE_DOMAIN = (
    os.environ.get('DASHBOARD_COOKIE_DOMAIN', '.forcequit.xyz') or None
)


def _dash_set_cookie(response, name, value, max_age, secure=None):
    if secure is None:
        secure = _dash_is_https()
    # SameSite must be 'None' for credentialed cross-site requests. Browsers
    # require Secure=True when SameSite=None, which is fine over HTTPS.
    samesite = 'None' if secure else 'Lax'
    response.set_cookie(
        name, value,
        max_age=max_age,
        httponly=True,
        samesite=samesite,
        secure=secure,
        path='/',
        domain=_DASH_COOKIE_DOMAIN,
    )


def _dash_clear_cookie(response, name):
    secure = _dash_is_https()
    samesite = 'None' if secure else 'Lax'
    response.set_cookie(
        name, '', max_age=0, httponly=True,
        samesite=samesite, secure=secure, path='/',
        domain=_DASH_COOKIE_DOMAIN,
    )


# Server-side session store. We keep it small and in-process because:
#   • The cookie-side payload was blowing past the 4096-byte browser limit
#     once a user's guild list showed up in it — browsers silently drop
#     cookies that big, which manifested as a permanent 401 loop.
#   • A single-process Flask app behind Cloudflare is the deployment, so
#     in-memory is fine. Sessions don't survive restarts — users just
#     re-login, which is an acceptable UX trade for a management page.
_DASH_SESSIONS = {}  # sid -> session dict
_DASH_SESSIONS_DB_READY = False


def _dash_bot_db_indexes_ensure():
    """One-time index creation on bot-owned tables that the dashboard
    queries frequently. The bot creates these tables; we just make sure
    the read paths can use indexes. CONCURRENTLY so we don't block any
    writes happening on the bot side. Best-effort — silently no-ops if
    the table doesn't exist yet (first deploy with this feature) or if
    BOT_DATA_DATABASE_URL isn't set."""
    dsn = os.environ.get('BOT_DATA_DATABASE_URL', '')
    if not dsn:
        return
    try:
        import psycopg2
        conn = psycopg2.connect(dsn)
        try:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            for sql in (
                # preset-stats endpoint joins preset_fire_log on preset_id
                # with a fired_at time filter — index makes both the join
                # and the time-bucketed COUNT FILTERs fast.
                'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_preset_fire_log_preset_fired '
                '  ON preset_fire_log (preset_id, fired_at DESC)',
            ):
                parts = sql.split()
                try:
                    idx_name = parts[parts.index('EXISTS') + 1]
                except (ValueError, IndexError):
                    idx_name = '<unknown>'
                try:
                    Log.startup(f"Building bot-DB index {idx_name} CONCURRENTLY...")
                    cur.execute(sql)
                    Log.startup(f"✓ Bot-DB index {idx_name} ready")
                except Exception as e:
                    # Likely the table doesn't exist yet — bot hasn't created
                    # it. Not an error worth alarming on.
                    Log.warn(f"Bot-DB index {idx_name} skipped: {e}")
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        Log.warn(f"Bot-DB index migration error: {e}")


def _dash_sessions_db_ensure():
    """Create the dash_sessions table (if missing) and hydrate the in-memory
    dict from any rows that survived a restart. Runs at most once per process;
    subsequent put/drop just write through without re-reading the table."""
    global _DASH_SESSIONS_DB_READY
    if _DASH_SESSIONS_DB_READY:
        return
    conn = _bot_db_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS dash_sessions (
                sid TEXT PRIMARY KEY,
                data JSONB NOT NULL,
                exp INTEGER NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_dash_sessions_exp ON dash_sessions(exp)')
        # Persistent users table — survives session expiry. Lets the admin
        # panel show "all users that have ever logged in" not just current
        # active sessions.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS dashboard_users (
                uid TEXT PRIMARY KEY,
                username TEXT,
                avatar TEXT,
                first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
                login_count INTEGER NOT NULL DEFAULT 1
            )
        ''')
        now = int(time.time())
        cur.execute('DELETE FROM dash_sessions WHERE exp < %s', (now,))
        cur.execute('SELECT sid, data FROM dash_sessions')
        loaded = 0
        for row in cur.fetchall():
            sid = row['sid']
            if sid not in _DASH_SESSIONS:
                _DASH_SESSIONS[sid] = row['data']
                loaded += 1
        conn.commit()
        _DASH_SESSIONS_DB_READY = True
        if loaded:
            Log.startup(f"dashboard: restored {loaded} session(s) from DB")
    except Exception as e:
        Log.warn(f"dashboard session DB init failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


def _dash_session_persist(sid, data):
    """Write through to Postgres. Silent no-op if BOT_DATA_DATABASE_URL is
    unset or the write fails — in-memory dict is still authoritative."""
    conn = _bot_db_conn()
    if conn is None:
        return
    try:
        # Drop runtime-only keys (those prefixed with _) before persisting —
        # e.g. _sid is added by _dash_load_session and has no DB meaning.
        persisted = {k: v for k, v in data.items() if not k.startswith('_')}
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO dash_sessions (sid, data, exp) VALUES (%s, %s::jsonb, %s) '
            'ON CONFLICT (sid) DO UPDATE SET data = EXCLUDED.data, '
            'exp = EXCLUDED.exp, updated_at = now()',
            (sid, json.dumps(persisted), int(persisted.get('exp', 0) or 0)),
        )
        conn.commit()
    except Exception as e:
        Log.warn(f"dashboard session persist failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


def _dash_session_delete_db(sid):
    conn = _bot_db_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute('DELETE FROM dash_sessions WHERE sid = %s', (sid,))
        conn.commit()
    except Exception as e:
        Log.warn(f"dashboard session delete failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


def _dash_users_upsert(uid, username=None, avatar=None, increment_login=False):
    """Record a Discord user in the persistent dashboard_users table.
    Called on each login (increment_login=True) and on session load
    (increment_login=False — only updates last_seen). Best-effort; if
    BOT_DATA_DATABASE_URL isn't set the call is a no-op."""
    if not uid:
        return
    conn = _bot_db_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        if increment_login:
            cur.execute(
                'INSERT INTO dashboard_users (uid, username, avatar) '
                'VALUES (%s, %s, %s) '
                'ON CONFLICT (uid) DO UPDATE SET '
                '  username = COALESCE(EXCLUDED.username, dashboard_users.username), '
                '  avatar = COALESCE(EXCLUDED.avatar, dashboard_users.avatar), '
                '  last_seen = now(), '
                '  login_count = dashboard_users.login_count + 1',
                (str(uid), username or '', avatar or '')
            )
        else:
            # Touch last_seen only — don't bump login_count for ordinary
            # authenticated requests.
            cur.execute(
                'UPDATE dashboard_users SET last_seen = now() WHERE uid = %s',
                (str(uid),)
            )
        conn.commit()
    except Exception as e:
        Log.warn(f"dashboard_users upsert failed: {e}")
        try: conn.rollback()
        except Exception: pass
    finally:
        conn.close()


def _dash_session_put(sid, data):
    _dash_sessions_db_ensure()
    _DASH_SESSIONS[sid] = data
    _dash_session_persist(sid, data)
    # Record/refresh the persistent user row in a background thread so the
    # OAuth callback doesn't block on a second DB write — the user's
    # session is already valid in memory and persisted via session_persist.
    uid = data.get('uid')
    if uid:
        username = data.get('username')
        avatar = data.get('avatar')
        threading.Thread(
            target=lambda: _dash_users_upsert(
                uid, username=username, avatar=avatar, increment_login=True),
            daemon=True,
            name='dash-users-upsert',
        ).start()
    # Opportunistic GC when the store grows — drop anything already expired.
    if len(_DASH_SESSIONS) > 512:
        now = int(time.time())
        stale = [k for k, v in _DASH_SESSIONS.items() if v.get('exp', 0) < now]
        for k in stale:
            _DASH_SESSIONS.pop(k, None)
            _dash_session_delete_db(k)


def _dash_session_get(sid):
    _dash_sessions_db_ensure()
    sess = _DASH_SESSIONS.get(sid)
    if not sess:
        return None
    if sess.get('exp', 0) < int(time.time()):
        _DASH_SESSIONS.pop(sid, None)
        _dash_session_delete_db(sid)
        return None
    return sess


def _dash_session_drop(sid):
    _DASH_SESSIONS.pop(sid, None)
    _dash_session_delete_db(sid)


def _dash_load_session():
    """Return the server-side session dict for the current request, or None.

    Cookie now only carries a signed {sid, exp} payload — heavy data (guild
    list, access_token, cached fresh-at timestamps) lives in _DASH_SESSIONS
    keyed by that sid.
    """
    secret = _dash_get_session_secret()
    if not secret:
        return None
    cookie_val = request.cookies.get(_DASH_SESSION_COOKIE)
    payload = _dash_parse_cookie(cookie_val, secret)
    if not payload:
        return None
    exp = payload.get('exp', 0)
    if not isinstance(exp, (int, float)) or exp < time.time():
        return None
    sid = payload.get('sid')
    if not sid:
        # Legacy cookie payload (pre-server-side session) — treat as invalid so
        # the user is forced through a fresh OAuth round-trip. Backwards-compat
        # for cookies baked before this refactor.
        return None
    session = _dash_session_get(sid)
    if not session:
        return None
    # Expose sid on the session dict so downstream code can rotate/drop it.
    session['_sid'] = sid
    # Touch last_seen on every authenticated request — admin overview uses
    # this for the "Age" column instead of `iat` so it reflects recent
    # activity, not how long the cookie has existed. In-memory only — we
    # don't write through to Postgres on every request to keep this cheap.
    session['last_seen'] = int(time.time())
    return session


def _dash_require_session():
    """Decorator to guard session-only routes."""
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not _dash_enabled():
                return _dash_err('dashboard_disabled',
                                 'Dashboard is not configured on this server.', 503)
            if not _dash_get_session_secret():
                return _dash_err('missing_session_secret',
                                 'DASHBOARD_SESSION_SECRET is not configured.', 503)
            session = _dash_load_session()
            if not session:
                return _dash_err('invalid_session',
                                 'Sign in via /api/dashboard/auth/login.', 401)
            request._dash_session = session
            return func(*args, **kwargs)
        return wrapper
    return deco


def _dash_enabled():
    return bool(os.environ.get('BOT_DATA_DATABASE_URL'))


def _bot_db_conn():
    """Return a _PooledConn-style wrapper around a conn from the bot pool.

    Lazily creates a ThreadedConnectionPool(1, 10) on first call. Returns
    None when BOT_DATA_DATABASE_URL is unset (caller converts to 503).
    """
    global _BOT_DB_POOL
    url = os.environ.get('BOT_DATA_DATABASE_URL')
    if not url:
        return None
    if _BOT_DB_POOL is None:
        with _BOT_DB_POOL_LOCK:
            if _BOT_DB_POOL is None:
                from psycopg2 import pool as _pg_pool
                _BOT_DB_POOL = _pg_pool.ThreadedConnectionPool(1, 10, url)

    conn = _BOT_DB_POOL.getconn()
    conn.autocommit = False
    conn.cursor_factory = RealDictCursor

    # Minimal wrapper matching db.py's _PooledConn pattern: .close() returns
    # the conn to the pool instead of really closing it.
    class _BotPooledConn:
        __slots__ = ('_conn', '_closed')
        def __init__(self, c):
            object.__setattr__(self, '_conn', c)
            object.__setattr__(self, '_closed', False)
        def close(self):
            if self._closed:
                return
            object.__setattr__(self, '_closed', True)
            try:
                _BOT_DB_POOL.putconn(self._conn)
            except Exception:
                try:
                    self._conn.close()
                except Exception:
                    pass
        def __getattr__(self, name):
            return getattr(self._conn, name)
        def __setattr__(self, name, value):
            if name in _BotPooledConn.__slots__:
                object.__setattr__(self, name, value)
            else:
                setattr(self._conn, name, value)
        def __enter__(self):
            return self
        def __exit__(self, et, ev, tb):
            self.close()

    return _BotPooledConn(conn)


# 30s TTL cache for the bot-guild-ids DB lookup. Every dashboard request
# passes through `_dash_guild_guard` which calls this; without caching, every
# preset/mute/channel/role API hit added a Postgres round-trip. Membership in
# alert_presets only changes on /setup or /alert-remove, so a 30s staleness
# window is invisible to users.
_DASH_BOT_GUILD_IDS_CACHE = {'ts': 0.0, 'data': set()}
_DASH_BOT_GUILD_IDS_TTL = 30
_DASH_BOT_GUILD_IDS_LOCK = threading.Lock()


def _dash_bot_guild_ids():
    """DISTINCT guild_ids that have any preset subscription.
    Cached for 30 s to avoid hitting Postgres on every dashboard request.
    """
    now = time.time()
    with _DASH_BOT_GUILD_IDS_LOCK:
        if (now - _DASH_BOT_GUILD_IDS_CACHE['ts']) < _DASH_BOT_GUILD_IDS_TTL:
            return set(_DASH_BOT_GUILD_IDS_CACHE['data'])
    conn = _bot_db_conn()
    if conn is None:
        Log.warn("dashboard _dash_bot_guild_ids: BOT_DATA_DATABASE_URL not set")
        return set()
    try:
        cur = conn.cursor()
        cur.execute('SELECT DISTINCT guild_id FROM alert_presets')
        rows = cur.fetchall()
        ids = {str(r['guild_id']) for r in rows}
        with _DASH_BOT_GUILD_IDS_LOCK:
            _DASH_BOT_GUILD_IDS_CACHE['ts'] = time.time()
            _DASH_BOT_GUILD_IDS_CACHE['data'] = ids
        return ids
    except Exception as e:
        Log.error(f"dashboard _dash_bot_guild_ids error: {e}")
        return set()
    finally:
        conn.close()


def _dash_public_base_url():
    """Where the API lives — used to build the OAuth redirect_uri that
    Discord is registered to call back to."""
    return (os.environ.get('PUBLIC_BASE_URL') or '').rstrip('/')


def _dash_frontend_base_url():
    """Where the dashboard HTML page is served — can differ from the API's
    host when the dashboard lives on a sibling subdomain (our case:
    dashboard @ nswpsn.forcequit.xyz, API @ api.forcequit.xyz). Falls back
    to PUBLIC_BASE_URL if not set."""
    return (
        os.environ.get('DASHBOARD_FRONTEND_URL')
        or _dash_public_base_url()
    ).rstrip('/')


def _dash_redirect_uri():
    base = _dash_public_base_url() or request.host_url.rstrip('/')
    return f"{base}/api/dashboard/auth/callback"


def _dash_bot_token():
    # Prefer the shared DISCORD_BOT_TOKEN; fall back to env names some
    # deployments may already have.
    return (os.environ.get('DISCORD_BOT_TOKEN')
            or os.environ.get('BOT_TOKEN') or '')


def _dash_bot_api(path, params=None, timeout=10):
    """Call the Discord REST API with the bot token. Returns (status, json|text, headers)."""
    token = _dash_bot_token()
    if not token:
        Log.warn(f"dashboard bot_api {path}: DISCORD_BOT_TOKEN not set")
        return 503, {'error': 'no_bot_token'}, {}
    try:
        r = requests.get(
            f"{_DISCORD_API_BASE}{path}",
            headers={'Authorization': f'Bot {token}',
                     'User-Agent': 'NSWPSN-Dashboard (https://nswpsn.forcequit.xyz)'},
            params=params,
            timeout=timeout,
        )
    except Exception as e:
        Log.warn(f"dashboard bot_api {path}: network error — {e}")
        return 502, {'error': 'discord_unreachable', 'message': str(e)}, {}
    try:
        body = r.json()
    except Exception:
        body = r.text
    if r.status_code >= 400:
        # Short summary so the cause of a dashboard 502/503 is visible in logs
        # without leaking the bot token or full response bodies.
        msg = body.get('message') if isinstance(body, dict) else str(body)[:120]
        Log.warn(f"dashboard bot_api {path}: HTTP {r.status_code} — {msg}")
    return r.status_code, body, r.headers


def _dash_user_avatar_url(user_id, avatar_hash):
    if not avatar_hash:
        return None
    ext = 'gif' if str(avatar_hash).startswith('a_') else 'png'
    return f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.{ext}"


def _dash_guild_icon_url(guild_id, icon_hash):
    if not icon_hash:
        return None
    ext = 'gif' if str(icon_hash).startswith('a_') else 'png'
    return f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.{ext}"


def _dash_normalize_guilds(raw_guilds, bot_guild_ids):
    out = []
    for g in raw_guilds or []:
        try:
            perms = int(g.get('permissions', 0) or 0)
        except (TypeError, ValueError):
            perms = 0
        gid = str(g.get('id'))
        owner = bool(g.get('owner'))
        # Guild owners implicitly have every permission — Discord's
        # `permissions` bitmask may not include MANAGE_CHANNELS even for
        # owners of guilds created before certain permission migrations.
        # Treat `owner=true` as equivalent to Administrator.
        manage = owner or bool(perms & (_MANAGE_CHANNELS | _ADMINISTRATOR))
        out.append({
            'id': gid,
            'name': g.get('name', ''),
            'icon': g.get('icon'),
            'owner': owner,
            'permissions': str(perms),
            'has_bot': gid in bot_guild_ids,
            'manage_channels': manage,
        })
    return out


def _dash_session_guild_entry(session, guild_id):
    guild_id = str(guild_id)
    for g in session.get('guilds', []):
        if str(g.get('id')) == guild_id:
            return g
    return None


def _dash_guild_guard(guild_id):
    """Used inside @_dash_require_session endpoints. Returns (entry, error_response|None)."""
    session = request._dash_session
    entry = _dash_session_guild_entry(session, guild_id)
    if not entry:
        return None, _dash_err('guild_not_found',
                               'You are not a member of that guild.', 403)
    try:
        perms = int(entry.get('permissions', 0) or 0)
    except (TypeError, ValueError):
        perms = 0
    # Guild owners implicitly have every permission, even when Discord's
    # permission bitmask doesn't happen to include MANAGE_CHANNELS.
    is_owner = bool(entry.get('owner'))
    if not is_owner and not (perms & (_MANAGE_CHANNELS | _ADMINISTRATOR)):
        return None, _dash_err('forbidden',
                               'Manage Channels permission is required on this guild.', 403)
    if str(guild_id) not in _dash_bot_guild_ids():
        return None, _dash_err('bot_not_in_guild',
                               'The NSW PSN bot is not configured in this guild yet.', 403)
    return entry, None


# ---------- OAuth endpoints ----------

@app.route('/api/dashboard/auth/login', methods=['GET'])
def dashboard_auth_login():
    from flask import redirect, make_response
    from urllib.parse import urlencode
    if not _dash_enabled():
        return _dash_err('dashboard_disabled',
                         'Dashboard is not configured on this server.', 503)
    secret = _dash_get_session_secret()
    if not secret:
        return _dash_err('missing_session_secret',
                         'DASHBOARD_SESSION_SECRET is not configured.', 503)
    client_id = os.environ.get('DISCORD_CLIENT_ID', '')
    if not client_id:
        return _dash_err('dashboard_disabled',
                         'DISCORD_CLIENT_ID is not configured.', 503)

    # Short-lived signed state cookie (carries the `next` path + nonce).
    next_path = request.args.get('next') or '/dashboard.html'
    state_payload = {
        'nonce': _dash_b64url(os.urandom(16)),
        'next': next_path if next_path.startswith('/') else '/dashboard.html',
        'exp': int(time.time()) + _DASH_OAUTH_STATE_TTL,
    }
    state_cookie = _dash_make_cookie(state_payload, secret)

    params = {
        'client_id': client_id,
        'redirect_uri': _dash_redirect_uri(),
        'response_type': 'code',
        'scope': 'identify guilds',
        'state': state_payload['nonce'],
        'prompt': 'none',
    }
    url = f"{_DISCORD_API_BASE.replace('/api/v10', '')}/api/oauth2/authorize?" + urlencode(params)
    resp = make_response(redirect(url, code=302))
    _dash_set_cookie(resp, _DASH_OAUTH_COOKIE, state_cookie,
                     max_age=_DASH_OAUTH_STATE_TTL)
    return resp


@app.route('/api/dashboard/auth/callback', methods=['GET'])
def dashboard_auth_callback():
    from flask import redirect, make_response
    if not _dash_enabled():
        return _dash_err('dashboard_disabled',
                         'Dashboard is not configured on this server.', 503)
    secret = _dash_get_session_secret()
    if not secret:
        return _dash_err('missing_session_secret',
                         'DASHBOARD_SESSION_SECRET is not configured.', 503)

    code = request.args.get('code')
    state = request.args.get('state')
    if not code or not state:
        return _dash_err('bad_request', 'Missing code or state.', 400)

    # Validate state cookie
    state_cookie = request.cookies.get(_DASH_OAUTH_COOKIE)
    state_payload = _dash_parse_cookie(state_cookie, secret)
    if not state_payload:
        return _dash_err('bad_request', 'Invalid OAuth state.', 400)
    if state_payload.get('nonce') != state:
        return _dash_err('bad_request', 'OAuth state mismatch.', 400)
    if state_payload.get('exp', 0) < time.time():
        return _dash_err('bad_request', 'OAuth state expired.', 400)
    next_path = state_payload.get('next') or '/dashboard'

    client_id = os.environ.get('DISCORD_CLIENT_ID', '')
    client_secret = os.environ.get('DISCORD_CLIENT_SECRET', '')
    if not client_id or not client_secret:
        return _dash_err('dashboard_disabled',
                         'Discord OAuth is not configured.', 503)

    # Exchange code for access token
    try:
        tr = requests.post(
            f"{_DISCORD_API_BASE}/oauth2/token",
            data={
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': _dash_redirect_uri(),
            },
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=15,
        )
    except Exception as e:
        return _dash_err('discord_error', f'Token exchange failed: {e}', 502)
    if tr.status_code != 200:
        return _dash_err('discord_error',
                         f'Token exchange rejected: {tr.status_code} {tr.text[:300]}', 502)
    token_resp = tr.json() or {}
    access_token = token_resp.get('access_token')
    if not access_token:
        return _dash_err('discord_error', 'No access_token from Discord.', 502)

    def _with_token(path):
        return requests.get(
            f"{_DISCORD_API_BASE}{path}",
            headers={'Authorization': f'Bearer {access_token}',
                     'User-Agent': 'NSWPSN-Dashboard'},
            timeout=15,
        )
    # Parallelize the two Discord identity calls — they're independent and
    # each can take 500-1500ms. Sequential they added up to several
    # seconds of OAuth callback latency for the user.
    try:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_ur = ex.submit(_with_token, '/users/@me')
            f_gr = ex.submit(_with_token, '/users/@me/guilds')
            ur = f_ur.result()
            gr = f_gr.result()
    except Exception as e:
        return _dash_err('discord_error', f'Identity fetch failed: {e}', 502)
    if ur.status_code != 200 or gr.status_code != 200:
        return _dash_err('discord_error',
                         f'Discord identity/guild fetch failed (user={ur.status_code}, '
                         f'guilds={gr.status_code})', 502)

    user = ur.json()
    guilds_raw = gr.json() or []
    bot_guild_ids = _dash_bot_guild_ids()
    guild_entries = _dash_normalize_guilds(guilds_raw, bot_guild_ids)

    now = int(time.time())
    exp = now + _DASH_SESSION_TTL

    # Heavy session data lives server-side (see _DASH_SESSIONS). The cookie
    # only carries a signed {sid, exp} blob so it stays well under the 4 KB
    # browser limit. New sid per login so stolen cookies from a prior session
    # can't be revived.
    sid = _dash_b64url(os.urandom(24))
    _dash_session_put(sid, {
        'uid': str(user.get('id')),
        'username': user.get('username') or user.get('global_name') or '',
        'avatar': user.get('avatar'),
        # Store access_token so /me can refresh the guild list when the cache
        # goes stale without forcing another full OAuth round-trip.
        'access_token': token_resp.get('access_token'),
        'token_type': token_resp.get('token_type', 'Bearer'),
        'refresh_token': token_resp.get('refresh_token'),
        # Compact guild list — id/name/icon/permissions only, the rest is
        # noise from Discord's /users/@me/guilds response.
        'guilds': [
            {'id': g['id'], 'name': g['name'],
             'icon': g.get('icon'), 'permissions': g.get('permissions', '0'),
             'owner': bool(g.get('owner'))}
            for g in guild_entries
        ],
        'gfresh': now,
        'iat': now,
        'exp': exp,
    })
    session_cookie = _dash_make_cookie({'sid': sid, 'exp': exp}, secret)

    # Send the user back to the DASHBOARD FRONTEND (possibly a different
    # subdomain from the API), not to the API subdomain. If the caller
    # passed a `next` param we honour it — but only its path+query, never
    # its host, so we can't be tricked into redirecting off-site.
    base = _dash_frontend_base_url() or request.host_url.rstrip('/')
    safe_next = next_path if next_path.startswith('/') else '/dashboard.html'
    redirect_target = base + safe_next
    resp = make_response(redirect(redirect_target, code=302))
    _dash_set_cookie(resp, _DASH_SESSION_COOKIE, session_cookie,
                     max_age=_DASH_SESSION_TTL)
    _dash_clear_cookie(resp, _DASH_OAUTH_COOKIE)
    return resp


@app.route('/api/dashboard/auth/logout', methods=['POST'])
def dashboard_auth_logout():
    from flask import make_response
    # Also nuke the server-side session so the sid can't be replayed even if
    # the cookie somehow leaks.
    secret = _dash_get_session_secret()
    if secret:
        payload = _dash_parse_cookie(
            request.cookies.get(_DASH_SESSION_COOKIE), secret,
        ) or {}
        sid = payload.get('sid')
        if sid:
            _dash_session_drop(sid)
    resp = make_response('', 204)
    _dash_clear_cookie(resp, _DASH_SESSION_COOKIE)
    _dash_clear_cookie(resp, _DASH_OAUTH_COOKIE)
    return resp


# ---------- Identity ----------

@app.route('/api/dashboard/me', methods=['GET'])
@_dash_require_session()
def dashboard_me():
    session = request._dash_session
    bot_guild_ids = _dash_bot_guild_ids()
    now = int(time.time())

    # Refresh the guild list from Discord when ours is older than
    # _DASH_GUILD_REFRESH_INTERVAL. Access token lives server-side so we can
    # do this without a full OAuth round-trip.
    if now - int(session.get('gfresh', 0) or 0) > _DASH_GUILD_REFRESH_INTERVAL:
        access_token = session.get('access_token')
        token_type = session.get('token_type') or 'Bearer'
        if access_token:
            try:
                import requests as _rq
                gr = _rq.get(
                    f"{_DISCORD_API_BASE}/users/@me/guilds",
                    headers={'Authorization': f'{token_type} {access_token}'},
                    timeout=10,
                )
                if gr.status_code == 200:
                    fresh = gr.json() or []
                    session['guilds'] = [
                        {'id': g['id'], 'name': g['name'],
                         'icon': g.get('icon'),
                         'permissions': g.get('permissions', '0')}
                        for g in _dash_normalize_guilds(fresh, bot_guild_ids)
                    ]
                    session['gfresh'] = now
                    # Persist the refreshed guild list so a restart doesn't
                    # drop it and force another Discord round-trip.
                    sid = session.get('_sid')
                    if sid:
                        _dash_session_persist(sid, session)
                # Non-200 (401/403): leave the cached list, just don't bump
                # gfresh so we retry next request. The user can always
                # re-login if their token was revoked.
            except Exception as e:
                Log.warn(f"dashboard /me guild refresh failed: {e}")

    session_guilds = session.get('guilds', [])
    guilds_out = []
    for g in session_guilds:
        gid = str(g.get('id'))
        try:
            perms = int(g.get('permissions', 0) or 0)
        except (TypeError, ValueError):
            perms = 0
        is_owner = bool(g.get('owner'))
        guilds_out.append({
            'id': gid,
            'name': g.get('name', ''),
            'icon_url': _dash_guild_icon_url(gid, g.get('icon')),
            'has_bot': gid in bot_guild_ids,
            'owner': is_owner,
            # Owners have every permission implicitly, even when Discord's
            # permissions bitmask doesn't include MANAGE_CHANNELS.
            'manage_channels': is_owner or bool(perms & (_MANAGE_CHANNELS | _ADMINISTRATOR)),
        })

    # Bot install invite URL — OAuth2 bot + slash-commands scope. Admin grants
    # only what the bot actually uses (Send Messages, Manage Webhooks,
    # Read Message History, Embed Links, Attach Files, Use External Emojis).
    bot_invite_url = None
    client_id = os.environ.get('DISCORD_CLIENT_ID') or ''
    if client_id:
        bot_invite_url = (
            'https://discord.com/oauth2/authorize'
            f'?client_id={client_id}'
            '&permissions=378091407360'
            '&scope=bot+applications.commands'
        )

    body = {
        'user': {
            'id': session.get('uid'),
            'username': session.get('username'),
            'avatar_url': _dash_user_avatar_url(session.get('uid'), session.get('avatar')),
            'is_admin': _dash_is_admin(session),
        },
        'guilds': guilds_out,
        'bot_invite_url': bot_invite_url,
    }
    return jsonify(body)


# ---------- Discord proxy (cached) ----------

# Discord channel/role cache stale-entry threshold. Anything older than 10×
# the per-kind TTL is unlikely to be re-read (entries past TTL get re-fetched
# on next dashboard hit, populating a fresh row). Eviction runs inside the
# existing cache lock on every _dash_cache_set so there's no extra contention.
_DASH_DISCORD_CACHE_STALE_FACTOR = 10


def _dash_discord_cache_evict_locked():
    """Drop entries older than 10× their kind's TTL. Caller holds the lock."""
    now = time.time()
    ttl_for = {
        'channels': _DISCORD_CHANNEL_CACHE_TTL,
        'roles': _DISCORD_ROLE_CACHE_TTL,
        'bcast_channels': _DISCORD_CHANNEL_CACHE_TTL,
    }
    stale = []
    for key, (ts, _data) in _dash_discord_cache.items():
        kind = key[0] if isinstance(key, tuple) else None
        ttl = ttl_for.get(kind, _DISCORD_CHANNEL_CACHE_TTL)
        if (now - ts) > (ttl * _DASH_DISCORD_CACHE_STALE_FACTOR):
            stale.append(key)
    for k in stale:
        _dash_discord_cache.pop(k, None)


def _dash_cache_get(kind, guild_id, ttl):
    with _dash_discord_cache_lock:
        entry = _dash_discord_cache.get((kind, str(guild_id)))
        if entry and (time.time() - entry[0]) < ttl:
            return entry[1]
    return None


def _dash_cache_set(kind, guild_id, data):
    with _dash_discord_cache_lock:
        _dash_discord_cache[(kind, str(guild_id))] = (time.time(), data)
        # Sweep entries we'll never read again (e.g. for guilds the bot has
        # left) so this dict can't grow forever across long uptime.
        _dash_discord_cache_evict_locked()


@app.route('/api/dashboard/guilds/<guild_id>/channels', methods=['GET'])
@_dash_require_session()
def dashboard_guild_channels(guild_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    cached = _dash_cache_get('channels', guild_id, _DISCORD_CHANNEL_CACHE_TTL)
    if cached is not None:
        return jsonify(cached)

    status, body, headers = _dash_bot_api(f'/guilds/{guild_id}/channels')
    if status == 429:
        retry = headers.get('Retry-After', '1')
        try:
            time.sleep(min(5.0, float(retry)))
        except Exception:
            time.sleep(1.0)
        status, body, headers = _dash_bot_api(f'/guilds/{guild_id}/channels')
        if status == 429:
            return _dash_err('rate_limited',
                             'Discord rate limit hit; please retry shortly.', 503)
    if status != 200 or not isinstance(body, list):
        return _dash_err('discord_error',
                         f'Discord responded {status}.', 502 if status >= 500 else 503)

    # Type 0 = GUILD_TEXT, Type 5 = GUILD_ANNOUNCEMENT (both accept bot posts).
    channels = [
        {'id': str(c.get('id')), 'name': c.get('name', ''),
         'position': c.get('position', 0),
         'parent_id': str(c['parent_id']) if c.get('parent_id') else None}
        for c in body if c.get('type') in (0, 5)
    ]
    channels.sort(key=lambda c: (c['parent_id'] or '', c['position']))
    _dash_cache_set('channels', guild_id, channels)
    return jsonify(channels)


@app.route('/api/dashboard/guilds/<guild_id>/roles', methods=['GET'])
@_dash_require_session()
def dashboard_guild_roles(guild_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    cached = _dash_cache_get('roles', guild_id, _DISCORD_ROLE_CACHE_TTL)
    if cached is not None:
        return jsonify(cached)

    status, body, headers = _dash_bot_api(f'/guilds/{guild_id}/roles')
    if status == 429:
        retry = headers.get('Retry-After', '1')
        try:
            time.sleep(min(5.0, float(retry)))
        except Exception:
            time.sleep(1.0)
        status, body, headers = _dash_bot_api(f'/guilds/{guild_id}/roles')
        if status == 429:
            return _dash_err('rate_limited',
                             'Discord rate limit hit; please retry shortly.', 503)
    if status != 200 or not isinstance(body, list):
        return _dash_err('discord_error',
                         f'Discord responded {status}.', 502 if status >= 500 else 503)

    roles = []
    for r in body:
        # Skip @everyone (id == guild_id) and bot/integration-managed roles.
        if str(r.get('id')) == str(guild_id):
            continue
        if r.get('managed'):
            continue
        roles.append({
            'id': str(r.get('id')),
            'name': r.get('name', ''),
            'color': int(r.get('color', 0) or 0),
            'position': int(r.get('position', 0) or 0),
        })
    roles.sort(key=lambda x: -x['position'])
    _dash_cache_set('roles', guild_id, roles)
    return jsonify(roles)


# ---------- Presets & Mute State ----------

_PG_UNIQUE_VIOLATION = '23505'


def _dash_iso(ts):
    try:
        return ts.isoformat() if ts is not None else None
    except Exception:
        return str(ts) if ts is not None else None


def _dash_row_to_preset(row):
    role_ids = [str(int(r)) for r in (row.get('role_ids') or [])]
    alert_types = list(row.get('alert_types') or [])
    overrides_raw = row.get('type_overrides') or {}
    type_overrides = {}
    if isinstance(overrides_raw, dict):
        for k, v in overrides_raw.items():
            if not isinstance(v, dict):
                continue
            type_overrides[k] = {
                'enabled': bool(v.get('enabled', True)),
                'enabled_ping': bool(v.get('enabled_ping', True)),
            }
    filters_raw = row.get('filters') or {}
    filters = filters_raw if isinstance(filters_raw, dict) else {}
    return {
        'id': int(row['id']),
        'channel_id': str(int(row['channel_id'])),
        'name': row.get('name') or '',
        'alert_types': alert_types,
        'pager_enabled': bool(row.get('pager_enabled', False)),
        'pager_capcodes': row.get('pager_capcodes'),
        'role_ids': role_ids,
        'enabled': bool(row.get('enabled', True)),
        'enabled_ping': bool(row.get('enabled_ping', True)),
        'type_overrides': type_overrides,
        'filters': filters,
        'created_at': _dash_iso(row.get('created_at')),
        'updated_at': _dash_iso(row.get('updated_at')),
    }


def _dash_row_to_mute(row, channel_mode=False):
    out = {
        'enabled': bool(row.get('enabled', True)),
        'enabled_ping': bool(row.get('enabled_ping', True)),
    }
    if channel_mode:
        out = {'channel_id': str(int(row['channel_id'])), **out}
    return out


def _dash_parse_role_ids_array(value):
    """Body `role_ids` → list[int] deduped (order-preserving). None = invalid."""
    if value is None:
        return []
    if not isinstance(value, list):
        return None
    seen = set()
    out = []
    for p in value:
        s = str(p).strip() if p is not None else ''
        if not s:
            continue
        try:
            n = int(s)
        except ValueError:
            return None
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def _dash_parse_alert_types(value):
    """Body `alert_types` → deduped list[str] validated against _DASH_ALERT_TYPES."""
    if value is None:
        return []
    if not isinstance(value, list):
        return None
    seen = set()
    out = []
    for a in value:
        if not isinstance(a, str):
            return None
        s = a.strip()
        if not s:
            continue
        if s not in _DASH_ALERT_TYPES:
            return None
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# Per-alert-type severity scales. filters.severity_min is now a dict keyed
# by canonical alert_type → token; older presets stored a single string
# applied generically and that legacy shape is still accepted on input.
_DASH_SEVERITY_SCALES = {
    'rfs':                {'advice', 'watch_and_act', 'emergency'},
    'bom_land':           {'minor', 'moderate', 'major'},
    'bom_marine':         {'minor', 'moderate', 'major'},
    'traffic_majorevent': {'minor', 'moderate', 'major'},
}
# Flat union of every token in any per-type scale — used to validate the
# legacy string form, which doesn't carry type context.
_DASH_FILTER_SEVERITIES = set().union(*_DASH_SEVERITY_SCALES.values())

# Canonical alert_types whose alerts carry a meaningful sub-type field
# (RFS category, BOM warning category, traffic incidentType, Waze subtype).
# subtype_filters can only be set for these.
_DASH_SUBTYPE_AWARE_TYPES = {
    'rfs',
    'bom_land', 'bom_marine',
    'traffic_incident', 'traffic_roadwork', 'traffic_flood',
    'traffic_fire', 'traffic_majorevent',
    'waze_hazard', 'waze_jam', 'waze_police', 'waze_roadwork',
    'user_incident',
}

_DASH_FILTER_KNOWN_KEYS = {
    'keywords_include', 'keywords_exclude',
    'severity_min', 'subtype_filters',
    'geofilter', 'bbox',
}
_DASH_GEOFILTER_TYPES = {'bbox', 'ring', 'polygon'}
_DASH_RING_RADIUS_MIN_M = 1
_DASH_RING_RADIUS_MAX_M = 500_000  # 500 km — generous cap, NSW spans ~1100 km E-W
_DASH_POLYGON_MIN_POINTS = 3
_DASH_POLYGON_MAX_POINTS = 100


def _dash_validate_geofilter(g):
    """Validate + normalise a geofilter (discriminated union on `type`).

    Returns the normalised dict; raises ValueError on any structural / value
    issue with a short message safe to surface in the 400 body.
    """
    if not isinstance(g, dict):
        raise ValueError('geofilter must be an object.')
    t = g.get('type')
    if t not in _DASH_GEOFILTER_TYPES:
        raise ValueError(f'geofilter.type must be one of {sorted(_DASH_GEOFILTER_TYPES)}.')

    if t == 'bbox':
        try:
            lat_min = float(g['lat_min']); lat_max = float(g['lat_max'])
            lng_min = float(g['lng_min']); lng_max = float(g['lng_max'])
        except (KeyError, TypeError, ValueError):
            raise ValueError('bbox geofilter needs numeric lat_min/lat_max/lng_min/lng_max.')
        if not (-90.0 <= lat_min <= 90.0 and -90.0 <= lat_max <= 90.0):
            raise ValueError('bbox lat must be in [-90, 90].')
        if not (-180.0 <= lng_min <= 180.0 and -180.0 <= lng_max <= 180.0):
            raise ValueError('bbox lng must be in [-180, 180].')
        if lat_min > lat_max:
            raise ValueError('bbox lat_min must be <= lat_max.')
        if lng_min > lng_max:
            raise ValueError('bbox lng_min must be <= lng_max.')
        return {'type': 'bbox',
                'lat_min': lat_min, 'lat_max': lat_max,
                'lng_min': lng_min, 'lng_max': lng_max}

    if t == 'ring':
        try:
            lat = float(g['lat']); lng = float(g['lng'])
            radius_m = float(g['radius_m'])
        except (KeyError, TypeError, ValueError):
            raise ValueError('ring geofilter needs numeric lat/lng/radius_m.')
        if not (-90.0 <= lat <= 90.0):
            raise ValueError('ring lat must be in [-90, 90].')
        if not (-180.0 <= lng <= 180.0):
            raise ValueError('ring lng must be in [-180, 180].')
        if not (_DASH_RING_RADIUS_MIN_M <= radius_m <= _DASH_RING_RADIUS_MAX_M):
            raise ValueError(
                f'ring radius_m must be in [{_DASH_RING_RADIUS_MIN_M}, {_DASH_RING_RADIUS_MAX_M}].')
        return {'type': 'ring', 'lat': lat, 'lng': lng, 'radius_m': radius_m}

    # polygon
    pts = g.get('points')
    if not isinstance(pts, list):
        raise ValueError('polygon.points must be a list.')
    n = len(pts)
    if n < _DASH_POLYGON_MIN_POINTS:
        raise ValueError(f'polygon needs at least {_DASH_POLYGON_MIN_POINTS} points.')
    if n > _DASH_POLYGON_MAX_POINTS:
        raise ValueError(f'polygon: max {_DASH_POLYGON_MAX_POINTS} points.')
    norm_pts = []
    for p in pts:
        if not (isinstance(p, (list, tuple)) and len(p) == 2):
            raise ValueError('polygon points must be [lat, lng] pairs.')
        try:
            plat = float(p[0]); plng = float(p[1])
        except (TypeError, ValueError):
            raise ValueError('polygon points must be numeric.')
        if not (-90.0 <= plat <= 90.0 and -180.0 <= plng <= 180.0):
            raise ValueError('polygon point out of lat/lng bounds.')
        norm_pts.append([plat, plng])
    return {'type': 'polygon', 'points': norm_pts}


def _dash_validate_filters(v):
    """Validate + normalise a preset.filters dict.

    Returns a normalised dict (possibly empty). Raises ValueError on any
    structural / value issue with a short message safe to surface in the
    400 body.
    """
    if v is None:
        return {}
    if not isinstance(v, dict):
        raise ValueError('filters must be an object.')
    unknown = set(v.keys()) - _DASH_FILTER_KNOWN_KEYS
    if unknown:
        raise ValueError(f'unknown filter keys: {sorted(unknown)}')
    out = {}

    for key in ('keywords_include', 'keywords_exclude'):
        raw = v.get(key)
        if raw is None:
            continue
        if not isinstance(raw, list):
            raise ValueError(f'{key} must be a list.')
        if len(raw) > 20:
            raise ValueError(f'{key}: max 20 entries.')
        kws = []
        for k in raw:
            if not isinstance(k, str):
                raise ValueError(f'{key}: each entry must be a string.')
            s = k.strip()
            if not s:
                continue
            if len(s) > 100:
                raise ValueError(f'{key}: each entry must be <= 100 chars.')
            kws.append(s)
        if kws:
            out[key] = kws

    if 'severity_min' in v and v['severity_min'] is not None:
        sev = v['severity_min']
        if isinstance(sev, str):
            # Legacy single-token form. Stored as-is; the bot fans it out per
            # alert_type at evaluation time. New writes use the dict form.
            s = sev.strip().lower()
            if s and s not in _DASH_FILTER_SEVERITIES:
                raise ValueError(f'severity_min must be one of {sorted(_DASH_FILTER_SEVERITIES)}.')
            if s:
                out['severity_min'] = s
        elif isinstance(sev, dict):
            unknown = set(sev.keys()) - set(_DASH_SEVERITY_SCALES)
            if unknown:
                raise ValueError(f'severity_min: unknown alert types {sorted(unknown)}.')
            normalised = {}
            for at, val in sev.items():
                if val is None or val == '':
                    continue
                if not isinstance(val, str):
                    raise ValueError(f'severity_min[{at}] must be a string.')
                vv = val.strip().lower()
                if vv not in _DASH_SEVERITY_SCALES[at]:
                    raise ValueError(
                        f'severity_min[{at}] must be one of {sorted(_DASH_SEVERITY_SCALES[at])}.')
                normalised[at] = vv
            if normalised:
                out['severity_min'] = normalised
        else:
            raise ValueError('severity_min must be a string or object.')

    if 'subtype_filters' in v and v['subtype_filters'] is not None:
        sf = v['subtype_filters']
        if not isinstance(sf, dict):
            raise ValueError('subtype_filters must be an object.')
        unknown = set(sf.keys()) - _DASH_SUBTYPE_AWARE_TYPES
        if unknown:
            raise ValueError(
                f'subtype_filters: unsupported alert types {sorted(unknown)}.')
        normalised = {}
        for at, raw in sf.items():
            if raw is None:
                continue
            if not isinstance(raw, list):
                raise ValueError(f'subtype_filters[{at}] must be a list.')
            if len(raw) > 50:
                raise ValueError(f'subtype_filters[{at}]: max 50 entries.')
            items = []
            for s in raw:
                if not isinstance(s, str):
                    raise ValueError(f'subtype_filters[{at}]: each entry must be a string.')
                ss = s.strip()
                if not ss:
                    continue
                if len(ss) > 100:
                    raise ValueError(f'subtype_filters[{at}]: each entry must be <= 100 chars.')
                items.append(ss)
            if items:
                # Dedup while preserving insertion order.
                seen = set(); deduped = []
                for it in items:
                    if it not in seen:
                        seen.add(it); deduped.append(it)
                normalised[at] = deduped
        if normalised:
            out['subtype_filters'] = normalised

    if 'geofilter' in v and v['geofilter'] is not None:
        out['geofilter'] = _dash_validate_geofilter(v['geofilter'])

    # Legacy alias: a top-level `bbox` is auto-converted to geofilter type=bbox.
    # Older presets persist in the DB with this shape; the bot also still
    # reads it. Both shapes can't be set simultaneously.
    if 'bbox' in v and v['bbox'] is not None:
        if 'geofilter' in out:
            raise ValueError('use only one of bbox / geofilter.')
        legacy = v['bbox']
        if not isinstance(legacy, dict):
            raise ValueError('bbox must be an object.')
        out['geofilter'] = _dash_validate_geofilter({'type': 'bbox', **legacy})

    return out


_PRESET_COLS = (
    'id, guild_id, channel_id, name, alert_types, pager_enabled, pager_capcodes, '
    'role_ids, enabled, enabled_ping, type_overrides, filters, created_at, updated_at'
)


def _dash_fetch_preset(cur, gid, preset_id):
    cur.execute(
        f'SELECT {_PRESET_COLS} FROM alert_presets WHERE id=%s AND guild_id=%s',
        (preset_id, gid),
    )
    return cur.fetchone()


@app.route('/api/dashboard/guilds/<guild_id>/presets', methods=['GET'])
@_dash_require_session()
def dashboard_guild_presets_get(guild_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    try:
        gid = int(guild_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id must be numeric.', 400)
    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute(
            f'SELECT {_PRESET_COLS} FROM alert_presets '
            'WHERE guild_id=%s ORDER BY channel_id, name',
            (gid,),
        )
        rows = cur.fetchall()
        return jsonify({'presets': [_dash_row_to_preset(r) for r in rows]})
    except Exception as e:
        Log.error(f"dashboard presets_get error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


@app.route('/api/dashboard/guilds/<guild_id>/presets', methods=['POST'])
@_dash_require_session()
def dashboard_guild_presets_create(guild_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    body = request.get_json(silent=True) or {}
    try:
        gid = int(guild_id)
        channel_id = int(body.get('channel_id') or 0)
    except (TypeError, ValueError):
        return _dash_err('bad_request', 'channel_id must be numeric.', 400)
    if not channel_id:
        return _dash_err('bad_request', 'channel_id is required.', 400)

    name_raw = body.get('name')
    if not isinstance(name_raw, str):
        return _dash_err('bad_request', 'name is required.', 400)
    name = name_raw.strip()
    if not (1 <= len(name) <= 64):
        return _dash_err('bad_request', 'name must be 1-64 characters.', 400)

    alert_types = _dash_parse_alert_types(body.get('alert_types'))
    if alert_types is None:
        return _dash_err('bad_request',
                         f'alert_types must be strings in {_DASH_ALERT_TYPES}.', 400)

    role_ids = _dash_parse_role_ids_array(body.get('role_ids'))
    if role_ids is None:
        return _dash_err('bad_request', 'role_ids must be an array of numeric ids.', 400)

    pager_enabled = bool(body.get('pager_enabled', False))
    pager_capcodes = body.get('pager_capcodes')
    if pager_capcodes is not None and not isinstance(pager_capcodes, str):
        return _dash_err('bad_request', 'pager_capcodes must be a string.', 400)

    if not alert_types and not pager_enabled:
        return _dash_err('empty_preset',
                         'Preset must have at least one alert_type or pager_enabled=true.', 400)

    enabled = bool(body.get('enabled', True))
    enabled_ping = bool(body.get('enabled_ping', True))

    try:
        filters = _dash_validate_filters(body.get('filters'))
    except ValueError as fe:
        return _dash_err('bad_filter', str(fe), 400)

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            INSERT INTO alert_presets
                (guild_id, channel_id, name, alert_types, pager_enabled, pager_capcodes,
                 role_ids, enabled, enabled_ping, type_overrides, filters, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '{{}}'::jsonb, %s, now(), now())
            RETURNING {_PRESET_COLS}
            ''',
            (gid, channel_id, name, alert_types, pager_enabled, pager_capcodes,
             role_ids, enabled, enabled_ping, Json(filters)),
        )
        row = cur.fetchone()
        conn.commit()
        return jsonify({'preset': _dash_row_to_preset(row)}), 201
    except Exception as e:
        conn.rollback()
        if getattr(e, 'pgcode', None) == _PG_UNIQUE_VIOLATION:
            return _dash_err('name_conflict',
                             'A preset with this name already exists on this channel.', 409)
        Log.error(f"dashboard presets_create error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


@app.route('/api/dashboard/guilds/<guild_id>/presets/<int:preset_id>', methods=['PATCH'])
@_dash_require_session()
def dashboard_guild_presets_patch(guild_id, preset_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    body = request.get_json(silent=True) or {}
    try:
        gid = int(guild_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id must be numeric.', 400)

    sets = []
    params = []

    if 'channel_id' in body:
        try:
            cid = int(body['channel_id'])
        except (TypeError, ValueError):
            return _dash_err('bad_request', 'channel_id must be numeric.', 400)
        if not cid:
            return _dash_err('bad_request', 'channel_id must be non-zero.', 400)
        sets.append('channel_id=%s')
        params.append(cid)

    if 'name' in body:
        nm = body['name']
        if not isinstance(nm, str):
            return _dash_err('bad_request', 'name must be a string.', 400)
        nm = nm.strip()
        if not (1 <= len(nm) <= 64):
            return _dash_err('bad_request', 'name must be 1-64 characters.', 400)
        sets.append('name=%s')
        params.append(nm)

    new_alert_types = None
    if 'alert_types' in body:
        new_alert_types = _dash_parse_alert_types(body['alert_types'])
        if new_alert_types is None:
            return _dash_err('bad_request',
                             f'alert_types must be strings in {_DASH_ALERT_TYPES}.', 400)
        sets.append('alert_types=%s')
        params.append(new_alert_types)

    new_pager_enabled = None
    if 'pager_enabled' in body:
        new_pager_enabled = bool(body['pager_enabled'])
        sets.append('pager_enabled=%s')
        params.append(new_pager_enabled)

    if 'pager_capcodes' in body:
        pc = body['pager_capcodes']
        if pc is not None and not isinstance(pc, str):
            return _dash_err('bad_request', 'pager_capcodes must be a string.', 400)
        sets.append('pager_capcodes=%s')
        params.append(pc)

    if 'role_ids' in body:
        rids = _dash_parse_role_ids_array(body['role_ids'])
        if rids is None:
            return _dash_err('bad_request', 'role_ids must be an array of numeric ids.', 400)
        sets.append('role_ids=%s')
        params.append(rids)

    if 'enabled' in body:
        sets.append('enabled=%s')
        params.append(bool(body['enabled']))

    if 'enabled_ping' in body:
        sets.append('enabled_ping=%s')
        params.append(bool(body['enabled_ping']))

    if 'filters' in body:
        try:
            filters_norm = _dash_validate_filters(body['filters'])
        except ValueError as fe:
            return _dash_err('bad_filter', str(fe), 400)
        sets.append('filters=%s')
        params.append(Json(filters_norm))

    if not sets:
        return _dash_err('bad_request', 'No updatable fields supplied.', 400)

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        existing = _dash_fetch_preset(cur, gid, preset_id)
        if not existing:
            return _dash_err('not_found', 'Preset not found.', 404)

        # Enforce the CHECK constraint pre-emptively to return a cleaner error.
        final_alert_types = new_alert_types if new_alert_types is not None else list(existing.get('alert_types') or [])
        final_pager = new_pager_enabled if new_pager_enabled is not None else bool(existing.get('pager_enabled'))
        if not final_alert_types and not final_pager:
            return _dash_err('empty_preset',
                             'Preset must have at least one alert_type or pager_enabled=true.', 400)

        sets.append('updated_at=now()')
        sql = (
            f'UPDATE alert_presets SET {", ".join(sets)} '
            f'WHERE id=%s AND guild_id=%s RETURNING {_PRESET_COLS}'
        )
        cur.execute(sql, tuple(params) + (preset_id, gid))
        row = cur.fetchone()
        conn.commit()
        return jsonify({'preset': _dash_row_to_preset(row)})
    except Exception as e:
        conn.rollback()
        if getattr(e, 'pgcode', None) == _PG_UNIQUE_VIOLATION:
            return _dash_err('name_conflict',
                             'A preset with this name already exists on this channel.', 409)
        Log.error(f"dashboard presets_patch error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


@app.route('/api/dashboard/guilds/<guild_id>/presets/<int:preset_id>', methods=['DELETE'])
@_dash_require_session()
def dashboard_guild_presets_delete(guild_id, preset_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    try:
        gid = int(guild_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id must be numeric.', 400)
    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute(
            'DELETE FROM alert_presets WHERE id=%s AND guild_id=%s',
            (preset_id, gid),
        )
        if (cur.rowcount or 0) == 0:
            conn.rollback()
            return _dash_err('not_found', 'Preset not found.', 404)
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        conn.rollback()
        Log.error(f"dashboard presets_delete error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


@app.route(
    '/api/dashboard/guilds/<guild_id>/presets/<int:preset_id>/type-overrides/<alert_type>',
    methods=['PUT'],
)
@_dash_require_session()
def dashboard_guild_preset_override_put(guild_id, preset_id, alert_type):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    try:
        gid = int(guild_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id must be numeric.', 400)
    if alert_type not in _DASH_ALERT_TYPES:
        return _dash_err('bad_request',
                         f'alert_type must be one of {_DASH_ALERT_TYPES}.', 400)

    body = request.get_json(silent=True) or {}
    has_enabled = 'enabled' in body and body['enabled'] is not None
    has_ping = 'enabled_ping' in body and body['enabled_ping'] is not None
    if not has_enabled and not has_ping:
        return _dash_err('bad_request',
                         'Supply at least one of enabled/enabled_ping; use DELETE to clear.', 400)

    enabled = bool(body['enabled']) if has_enabled else True
    enabled_ping = bool(body['enabled_ping']) if has_ping else True
    override_value = json.dumps({'enabled': enabled, 'enabled_ping': enabled_ping})
    # alert_type already validated against allow-list, safe to inline in jsonb path.
    path_literal = '{' + alert_type + '}'

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        existing = _dash_fetch_preset(cur, gid, preset_id)
        if not existing:
            return _dash_err('not_found', 'Preset not found.', 404)
        cur.execute(
            f'''
            UPDATE alert_presets
               SET type_overrides = jsonb_set(
                        COALESCE(type_overrides, '{{}}'::jsonb),
                        %s::text[],
                        %s::jsonb,
                        true),
                   updated_at = now()
             WHERE id=%s AND guild_id=%s
             RETURNING {_PRESET_COLS}
            ''',
            (path_literal, override_value, preset_id, gid),
        )
        row = cur.fetchone()
        conn.commit()
        return jsonify({'preset': _dash_row_to_preset(row)})
    except Exception as e:
        conn.rollback()
        Log.error(f"dashboard preset_override_put error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


@app.route(
    '/api/dashboard/guilds/<guild_id>/presets/<int:preset_id>/type-overrides/<alert_type>',
    methods=['DELETE'],
)
@_dash_require_session()
def dashboard_guild_preset_override_delete(guild_id, preset_id, alert_type):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    try:
        gid = int(guild_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id must be numeric.', 400)
    if alert_type not in _DASH_ALERT_TYPES:
        return _dash_err('bad_request',
                         f'alert_type must be one of {_DASH_ALERT_TYPES}.', 400)

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        existing = _dash_fetch_preset(cur, gid, preset_id)
        if not existing:
            return _dash_err('not_found', 'Preset not found.', 404)
        # `#- text[]` removes a key; param-binding the path avoids SQL injection
        # even though alert_type is already allow-list validated.
        cur.execute(
            f'''
            UPDATE alert_presets
               SET type_overrides = COALESCE(type_overrides, '{{}}'::jsonb) #- %s::text[],
                   updated_at = now()
             WHERE id=%s AND guild_id=%s
             RETURNING {_PRESET_COLS}
            ''',
            ([alert_type], preset_id, gid),
        )
        row = cur.fetchone()
        conn.commit()
        return jsonify({'preset': _dash_row_to_preset(row)})
    except Exception as e:
        conn.rollback()
        Log.error(f"dashboard preset_override_delete error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


# ---------- Preset stats ----------

# Per-guild preset-stats cache. Underlying query joins alert_presets
# against preset_fire_log with two windowed COUNT FILTERs and a MAX —
# can take seconds on a busy guild before the index lands. Stale-while-
# revalidate so the dashboard never blocks waiting for it: serve the
# previous value (or an empty stub) instantly, refresh in background.
_DASH_PRESET_STATS_CACHE = {}      # gid -> (data, ts)
_DASH_PRESET_STATS_CACHE_LOCK = threading.Lock()
_DASH_PRESET_STATS_TTL = 60        # seconds — fresh window
_DASH_PRESET_STATS_INFLIGHT = set()  # coalesce duplicate refreshes
_DASH_PRESET_STATS_INFLIGHT_LOCK = threading.Lock()


def _refresh_preset_stats(gid):
    """Run the heavy preset-stats query and update the cache. Runs in a
    background thread when the cache is stale or absent — the request
    handler returns whatever was cached and never blocks on this."""
    with _DASH_PRESET_STATS_INFLIGHT_LOCK:
        if gid in _DASH_PRESET_STATS_INFLIGHT:
            return
        _DASH_PRESET_STATS_INFLIGHT.add(gid)
    try:
        conn = _bot_db_conn()
        if conn is None:
            return
        out = []
        try:
            cur = conn.cursor()
            cur.execute("SET LOCAL statement_timeout = '30s'")
            cur.execute('''
                SELECT p.id AS preset_id,
                       COUNT(*) FILTER (WHERE f.fired_at > NOW() - INTERVAL '7 days')  AS fires_7d,
                       COUNT(*) FILTER (WHERE f.fired_at > NOW() - INTERVAL '30 days') AS fires_30d,
                       MAX(f.fired_at) AS last_fire
                  FROM alert_presets p
             LEFT JOIN preset_fire_log f ON f.preset_id = p.id
                 WHERE p.guild_id = %s
              GROUP BY p.id
            ''', (gid,))
            for r in cur.fetchall():
                lf = r['last_fire']
                out.append({
                    'preset_id': str(r['preset_id']),
                    'fires_7d': int(r['fires_7d'] or 0),
                    'fires_30d': int(r['fires_30d'] or 0),
                    'last_fire': lf.isoformat() if lf else None,
                })
        except Exception as e:
            Log.warn(f"preset_stats refresh skipped (still slow): {e}")
            return
        finally:
            conn.close()
        with _DASH_PRESET_STATS_CACHE_LOCK:
            _DASH_PRESET_STATS_CACHE[gid] = (out, time.time())
    finally:
        with _DASH_PRESET_STATS_INFLIGHT_LOCK:
            _DASH_PRESET_STATS_INFLIGHT.discard(gid)


@app.route('/api/dashboard/guilds/<guild_id>/preset-stats', methods=['GET'])
@_dash_require_session()
def dashboard_guild_preset_stats(guild_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    try:
        gid = int(guild_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id must be numeric.', 400)

    now = time.time()
    with _DASH_PRESET_STATS_CACHE_LOCK:
        cached = _DASH_PRESET_STATS_CACHE.get(gid)

    if cached:
        data, ts = cached
        # Always return cached data immediately. If stale, kick off a
        # background refresh — the next call gets fresh data.
        if (now - ts) >= _DASH_PRESET_STATS_TTL:
            threading.Thread(
                target=_refresh_preset_stats, args=(gid,),
                daemon=True, name='preset-stats-refresh',
            ).start()
        return jsonify({'stats': data, 'cache_age_seconds': int(now - ts)})

    # First call ever for this guild — return an empty stub immediately
    # and start the refresh. The dashboard will see empty stats on the
    # first load (no historical data), and the refresh fills the cache
    # within a few seconds for the next call.
    threading.Thread(
        target=_refresh_preset_stats, args=(gid,),
        daemon=True, name='preset-stats-refresh',
    ).start()
    return jsonify({'stats': [], 'cache_age_seconds': None, 'warming': True})


# ---------- Mute state ----------

@app.route('/api/dashboard/guilds/<guild_id>/mute-state', methods=['GET'])
@_dash_require_session()
def dashboard_guild_mute_state_get(guild_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    try:
        gid = int(guild_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id must be numeric.', 400)
    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute(
            'SELECT enabled, enabled_ping FROM guild_mute_state WHERE guild_id=%s',
            (gid,),
        )
        g_row = cur.fetchone()
        guild_state = (_dash_row_to_mute(g_row) if g_row
                       else {'enabled': True, 'enabled_ping': True})
        cur.execute(
            'SELECT channel_id, enabled, enabled_ping FROM channel_mute_state '
            'WHERE guild_id=%s ORDER BY channel_id',
            (gid,),
        )
        ch_rows = cur.fetchall()
        channels = [_dash_row_to_mute(r, channel_mode=True) for r in ch_rows]
        return jsonify({'guild': guild_state, 'channels': channels})
    except Exception as e:
        Log.error(f"dashboard mute_state_get error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


def _dash_mute_partial_values(body):
    """Returns (enabled, enabled_ping) with None for omitted fields, or (None, None, err)."""
    has_enabled = 'enabled' in body and body['enabled'] is not None
    has_ping = 'enabled_ping' in body and body['enabled_ping'] is not None
    if not has_enabled and not has_ping:
        return None, None, _dash_err(
            'bad_request', 'Supply at least one of enabled/enabled_ping.', 400)
    return (
        bool(body['enabled']) if has_enabled else None,
        bool(body['enabled_ping']) if has_ping else None,
        None,
    )


@app.route('/api/dashboard/guilds/<guild_id>/mute-state/guild', methods=['PUT'])
@_dash_require_session()
def dashboard_guild_mute_state_guild_put(guild_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    try:
        gid = int(guild_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id must be numeric.', 400)
    body = request.get_json(silent=True) or {}
    enabled, enabled_ping, berr = _dash_mute_partial_values(body)
    if berr:
        return berr

    # Partial upsert: default missing fields to True on INSERT, keep existing on UPDATE.
    ins_enabled = True if enabled is None else enabled
    ins_ping = True if enabled_ping is None else enabled_ping

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute(
            '''
            INSERT INTO guild_mute_state (guild_id, enabled, enabled_ping, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (guild_id) DO UPDATE SET
                enabled = CASE WHEN %s THEN EXCLUDED.enabled ELSE guild_mute_state.enabled END,
                enabled_ping = CASE WHEN %s THEN EXCLUDED.enabled_ping ELSE guild_mute_state.enabled_ping END,
                updated_at = now()
            RETURNING enabled, enabled_ping
            ''',
            (gid, ins_enabled, ins_ping, enabled is not None, enabled_ping is not None),
        )
        row = cur.fetchone()
        conn.commit()
        return jsonify({'guild': _dash_row_to_mute(row)})
    except Exception as e:
        conn.rollback()
        Log.error(f"dashboard mute_state_guild_put error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


@app.route('/api/dashboard/guilds/<guild_id>/mute-state/guild', methods=['DELETE'])
@_dash_require_session()
def dashboard_guild_mute_state_guild_delete(guild_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    try:
        gid = int(guild_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id must be numeric.', 400)
    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute('DELETE FROM guild_mute_state WHERE guild_id=%s', (gid,))
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        conn.rollback()
        Log.error(f"dashboard mute_state_guild_delete error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


@app.route('/api/dashboard/guilds/<guild_id>/mute-state/channels/<channel_id>', methods=['PUT'])
@_dash_require_session()
def dashboard_guild_mute_state_channel_put(guild_id, channel_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    try:
        gid = int(guild_id)
        cid = int(channel_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id/channel_id must be numeric.', 400)
    if not cid:
        return _dash_err('bad_request', 'channel_id must be non-zero.', 400)

    body = request.get_json(silent=True) or {}
    enabled, enabled_ping, berr = _dash_mute_partial_values(body)
    if berr:
        return berr

    ins_enabled = True if enabled is None else enabled
    ins_ping = True if enabled_ping is None else enabled_ping

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute(
            '''
            INSERT INTO channel_mute_state (guild_id, channel_id, enabled, enabled_ping, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (guild_id, channel_id) DO UPDATE SET
                enabled = CASE WHEN %s THEN EXCLUDED.enabled ELSE channel_mute_state.enabled END,
                enabled_ping = CASE WHEN %s THEN EXCLUDED.enabled_ping ELSE channel_mute_state.enabled_ping END,
                updated_at = now()
            RETURNING channel_id, enabled, enabled_ping
            ''',
            (gid, cid, ins_enabled, ins_ping,
             enabled is not None, enabled_ping is not None),
        )
        row = cur.fetchone()
        conn.commit()
        return jsonify({'channel': _dash_row_to_mute(row, channel_mode=True)})
    except Exception as e:
        conn.rollback()
        Log.error(f"dashboard mute_state_channel_put error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


@app.route('/api/dashboard/guilds/<guild_id>/mute-state/channels/<channel_id>', methods=['DELETE'])
@_dash_require_session()
def dashboard_guild_mute_state_channel_delete(guild_id, channel_id):
    _, err = _dash_guild_guard(guild_id)
    if err:
        return err
    try:
        gid = int(guild_id)
        cid = int(channel_id)
    except ValueError:
        return _dash_err('bad_request', 'guild_id/channel_id must be numeric.', 400)
    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute(
            'DELETE FROM channel_mute_state WHERE guild_id=%s AND channel_id=%s',
            (gid, cid),
        )
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        conn.rollback()
        Log.error(f"dashboard mute_state_channel_delete error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()


# ---------- Admin (super-user) overview ----------

def _dash_require_admin():
    """Wraps _dash_require_session and additionally enforces admin membership."""
    base = _dash_require_session()
    def _wrap(fn):
        from functools import wraps
        session_wrapped = base(fn)
        @wraps(fn)
        def inner(*a, **kw):
            if not _dash_enabled():
                return _dash_err('dashboard_disabled',
                                 'BOT_DATA_DATABASE_URL is not configured.', 503)
            if not _dash_get_session_secret():
                return _dash_err('missing_session_secret',
                                 'DASHBOARD_SESSION_SECRET is not configured.', 503)
            session = _dash_load_session()
            if not session:
                return _dash_err('invalid_session',
                                 'Sign in via /api/dashboard/auth/login.', 401)
            if not _dash_is_admin(session):
                return _dash_err('not_admin',
                                 'Admin access required.', 403)
            request._dash_session = session
            return fn(*a, **kw)
        return inner
    return _wrap


def _dash_guild_names_from_sessions():
    """Best-effort map of guild_id -> name/icon, derived from any session that
    has that guild listed. Avoids an extra Discord API call per guild."""
    out = {}
    for sess in list(_DASH_SESSIONS.values()):
        for g in sess.get('guilds', []) or []:
            gid = str(g.get('id') or '')
            if gid and gid not in out:
                out[gid] = {
                    'name': g.get('name') or '',
                    'icon_url': _dash_guild_icon_url(gid, g.get('icon')),
                }
    return out


# Module-level cache for guild metadata fetched via the bot token, used as a
# fallback when no admin session covers the guild. 10-minute TTL — guild names
# don't churn often.
_DASH_GUILD_META_CACHE = {}  # gid -> (timestamp, {name, icon_url})
_DASH_GUILD_META_TTL = 10 * 60
_DASH_GUILD_META_CACHE_LOCK = threading.Lock()


def _dash_guild_meta_evict_locked():
    """Drop entries older than 2× TTL. Caller must hold the cache lock.
    Without this, the cache accumulates 'every guild_id ever queried'
    (including guilds the bot has long since left), since the TTL is only
    enforced on read."""
    cutoff = time.time() - (_DASH_GUILD_META_TTL * 2)
    stale = [k for k, (ts, _) in _DASH_GUILD_META_CACHE.items() if ts < cutoff]
    for k in stale:
        _DASH_GUILD_META_CACHE.pop(k, None)

# Cache for /applications/@me (bot guild count + user install count). 5-minute
# TTL — these are "approximate" counts from Discord and don't churn often.
_DASH_APP_INFO_CACHE = {'ts': 0, 'data': {}}
_DASH_APP_INFO_TTL = 5 * 60


def _dash_app_install_counts():
    """Return {servers_total, user_installs} from /applications/@me. Cached.
    Both fields default to None on failure so the UI can render a dash."""
    now = time.time()
    if _DASH_APP_INFO_CACHE['data'] and (now - _DASH_APP_INFO_CACHE['ts']) < _DASH_APP_INFO_TTL:
        return _DASH_APP_INFO_CACHE['data']
    out = {'servers_total': None, 'user_installs': None}
    if not _dash_bot_token():
        return out
    try:
        status, body, _hdrs = _dash_bot_api('/applications/@me')
        if status == 200 and isinstance(body, dict):
            gc = body.get('approximate_guild_count')
            uc = body.get('approximate_user_install_count')
            if isinstance(gc, int):
                out['servers_total'] = gc
            if isinstance(uc, int):
                out['user_installs'] = uc
            _DASH_APP_INFO_CACHE['data'] = out
            _DASH_APP_INFO_CACHE['ts'] = now
    except Exception:
        pass
    return out


def _dash_guild_meta_lookup(guild_ids):
    """Return {gid: {name, icon_url}} for the requested ids. Tries the
    session-derived map first; for anything still missing, fetches each
    via the Discord bot API in parallel (10-min cache per gid). Empty
    values stay empty if Discord returns an error — admin UI shows a dash."""
    out = dict(_dash_guild_names_from_sessions())
    now = time.time()
    needed = [str(gid) for gid in guild_ids if str(gid) not in out or not out[str(gid)].get('name')]
    if not needed:
        return out
    if not _dash_bot_token():
        return out

    # Split needed into cache-hit and cache-miss buckets in one pass.
    fresh_misses = []
    for gid in needed:
        with _DASH_GUILD_META_CACHE_LOCK:
            cached = _DASH_GUILD_META_CACHE.get(gid)
        if cached and (now - cached[0]) < _DASH_GUILD_META_TTL:
            out[gid] = cached[1]
        else:
            fresh_misses.append(gid)

    if not fresh_misses:
        return out

    # Parallelize the cache misses. Sequential per-gid Discord API calls
    # were the dominant cost on first dashboard load with N guilds —
    # 8-way concurrency cuts wall time by ~7-8x while staying well below
    # Discord's per-route rate limit.
    def _fetch_one(gid):
        try:
            status, body, _hdrs = _dash_bot_api(f'/guilds/{gid}')
            if status == 200 and isinstance(body, dict):
                return gid, {
                    'name': body.get('name') or '',
                    'icon_url': _dash_guild_icon_url(gid, body.get('icon')),
                }
        except Exception:
            pass
        return gid, None

    try:
        from concurrent.futures import ThreadPoolExecutor
        max_workers = min(8, len(fresh_misses))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for gid, meta in ex.map(_fetch_one, fresh_misses):
                if meta is not None:
                    with _DASH_GUILD_META_CACHE_LOCK:
                        _DASH_GUILD_META_CACHE[gid] = (now, meta)
                        _dash_guild_meta_evict_locked()
                    out[gid] = meta
    except Exception as e:
        Log.warn(f"guild meta parallel fetch failed: {e}")

    return out


# 30s cache for the admin overview response. The DB stats + guild meta +
# Discord install counts dominate the cost; session info is in-memory and
# cheap. 30s of staleness is acceptable for the speed win — admin users
# refreshing the page back-to-back hit cache instead of paying 3s+ each
# time.
_DASH_OVERVIEW_CACHE = {'data': None, 'ts': 0}
_DASH_OVERVIEW_CACHE_LOCK = threading.Lock()
_DASH_OVERVIEW_TTL = 30


@app.route('/api/dashboard/admin/overview', methods=['GET'])
@_dash_require_admin()
def dashboard_admin_overview():
    now = int(time.time())

    with _DASH_OVERVIEW_CACHE_LOCK:
        cached = _DASH_OVERVIEW_CACHE
        if cached['data'] is not None and (now - cached['ts']) < _DASH_OVERVIEW_TTL:
            return jsonify(cached['data'])

    # Session stats (from in-memory session store). Collapse to one row per
    # Discord user (freshest session wins) so the same person isn't listed
    # multiple times if they have more than one active cookie (mobile+desktop,
    # re-login after cookie clear, etc.).
    sessions_active = 0
    users_seen = set()
    per_user = {}   # uid -> session-row dict, newest by 'iat'
    for sid, sess in list(_DASH_SESSIONS.items()):
        exp = int(sess.get('exp', 0) or 0)
        if exp and exp < now:
            continue
        sessions_active += 1
        uid = str(sess.get('uid') or '')
        if not uid:
            continue
        users_seen.add(uid)
        iat = int(sess.get('iat', 0) or 0)
        # Prefer last_seen (touched on every authenticated request) for the
        # "age" display so it reflects recent activity, not how long the
        # cookie has existed. Falls back to iat for sessions that haven't
        # been touched yet (e.g., just loaded from DB after a restart).
        last_seen = int(sess.get('last_seen', 0) or 0) or iat
        row = {
            'uid': uid,
            'username': sess.get('username') or '',
            'avatar_url': _dash_user_avatar_url(uid, sess.get('avatar')),
            'guild_count': len(sess.get('guilds') or []),
            'age_seconds': max(0, now - last_seen),
            'session_age_seconds': max(0, now - iat),
            'is_admin': uid in _dash_admin_ids(),
            '_iat': iat,
        }
        prev = per_user.get(uid)
        if prev is None or iat > prev['_iat']:
            per_user[uid] = row
    sessions_out = sorted(per_user.values(), key=lambda s: s['age_seconds'])
    for r in sessions_out:
        r.pop('_iat', None)

    # Mark all currently-active rows so the frontend can style them.
    for r in sessions_out:
        r['is_active'] = True

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    historical_users = []
    try:
        cur = conn.cursor()

        # Pull historical users from dashboard_users so the panel shows
        # everyone who has ever logged in, not just current active sessions.
        # Reuses the same connection as the rest of the endpoint to avoid
        # a second round-trip to the pool.
        try:
            cur.execute(
                'SELECT uid, username, avatar, '
                '       EXTRACT(EPOCH FROM last_seen)::bigint AS last_seen, '
                '       login_count '
                '  FROM dashboard_users '
                'ORDER BY last_seen DESC'
            )
            for r in cur.fetchall():
                uid = str(r['uid'])
                if uid in per_user:
                    continue  # already covered by active session
                last_seen = int(r.get('last_seen') or 0)
                historical_users.append({
                    'uid': uid,
                    'username': r.get('username') or '',
                    'avatar_url': _dash_user_avatar_url(uid, r.get('avatar')),
                    'guild_count': 0,
                    'age_seconds': max(0, now - last_seen),
                    'session_age_seconds': max(0, now - last_seen),
                    'is_admin': uid in _dash_admin_ids(),
                    'is_active': False,
                    'login_count': int(r.get('login_count') or 0),
                })
        except Exception as e:
            Log.warn(f"dashboard_users query failed: {e}")

        # Lifetime unique-user count. Cheap COUNT — runs against the same
        # cursor we just used.
        try:
            cur.execute('SELECT COUNT(*) AS n FROM dashboard_users')
            users_lifetime = cur.fetchone()['n'] or 0
        except Exception:
            users_lifetime = len(users_seen)

        # All five alert_presets aggregates in one round-trip via FILTER
        # clauses. Was 5 separate COUNT queries adding up to several DB
        # round-trips per dashboard load.
        cur.execute('''
            SELECT
                COUNT(*)                                        AS total,
                COUNT(*) FILTER (WHERE pager_enabled)           AS pager,
                COUNT(*) FILTER (WHERE NOT enabled)             AS muted,
                COUNT(DISTINCT guild_id)                        AS guilds_with_presets,
                COUNT(DISTINCT (guild_id, channel_id))          AS channels_configured
            FROM alert_presets
        ''')
        ap_row = cur.fetchone() or {}
        presets_total = int(ap_row.get('total') or 0)
        presets_pager = int(ap_row.get('pager') or 0)
        presets_muted = int(ap_row.get('muted') or 0)
        guilds_with_presets = int(ap_row.get('guilds_with_presets') or 0)
        channels_configured = int(ap_row.get('channels_configured') or 0)

        # Combine the two mute-state COUNTs into a single round-trip.
        cur.execute('''
            SELECT
                (SELECT COUNT(*) FROM guild_mute_state WHERE NOT enabled)   AS guilds_muted,
                (SELECT COUNT(*) FROM channel_mute_state WHERE NOT enabled) AS channels_muted
        ''')
        ms_row = cur.fetchone() or {}
        guilds_muted = int(ms_row.get('guilds_muted') or 0)
        channels_muted = int(ms_row.get('channels_muted') or 0)

        # Per-alert-type subscriber count (number of presets subscribed)
        type_counts = {t: 0 for t in _DASH_ALERT_TYPES}
        cur.execute(
            'SELECT unnest(alert_types) AS t, COUNT(*) AS n '
            'FROM alert_presets GROUP BY t'
        )
        for r in cur.fetchall():
            t = r['t']
            if t in type_counts:
                type_counts[t] = r['n'] or 0

        # Per-guild breakdown
        cur.execute('''
            SELECT guild_id,
                   COUNT(*) AS preset_count,
                   COUNT(DISTINCT channel_id) AS channel_count,
                   SUM(CASE WHEN pager_enabled THEN 1 ELSE 0 END) AS pager_presets,
                   SUM(CASE WHEN NOT enabled THEN 1 ELSE 0 END) AS muted_presets,
                   MAX(updated_at) AS last_change
              FROM alert_presets
          GROUP BY guild_id
          ORDER BY preset_count DESC
        ''')
        guild_rows = cur.fetchall()
        name_map = _dash_guild_meta_lookup([str(r['guild_id']) for r in guild_rows])
        guilds_out = []
        for r in guild_rows:
            gid = str(r['guild_id'])
            meta = name_map.get(gid, {})
            lc = r['last_change']
            guilds_out.append({
                'guild_id': gid,
                'name': meta.get('name') or '',
                'icon_url': meta.get('icon_url') or '',
                'preset_count': int(r['preset_count'] or 0),
                'channel_count': int(r['channel_count'] or 0),
                'pager_presets': int(r['pager_presets'] or 0),
                'muted_presets': int(r['muted_presets'] or 0),
                'last_change': lc.isoformat() if lc else None,
            })
    except Exception as e:
        Log.error(f"dashboard admin_overview error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()

    # Merge historical users after the active list (active first, sorted
    # by recency).
    sessions_out = sessions_out + historical_users

    # Resolve once — was being called twice on the same response, doubling
    # the time spent in this code path even though the function itself is
    # cached internally.
    install_counts = _dash_app_install_counts()

    response_payload = {
        'stats': {
            'sessions_active': sessions_active,
            # 'dashboard_users' = lifetime unique users (from dashboard_users
            # table). 'users_active' is the count currently logged in.
            'dashboard_users': users_lifetime,
            'users_active': len(users_seen),
            # Backwards-compat alias for older frontend builds.
            'users_total': users_lifetime,
            'servers_total': install_counts.get('servers_total'),
            'user_installs': install_counts.get('user_installs'),
            'guilds_with_presets': guilds_with_presets,
            'channels_configured': channels_configured,
            'presets_total': presets_total,
            'presets_pager': presets_pager,
            'presets_muted': presets_muted,
            'guilds_muted': guilds_muted,
            'channels_muted': channels_muted,
            'alert_type_counts': type_counts,
        },
        'guilds': guilds_out,
        'sessions': sessions_out,
        'admin_ids': sorted(list(_dash_admin_ids())),
        'server_time': now,
    }
    with _DASH_OVERVIEW_CACHE_LOCK:
        _DASH_OVERVIEW_CACHE['data'] = response_payload
        _DASH_OVERVIEW_CACHE['ts'] = now
    return jsonify(response_payload)


# ---------- Admin broadcast (send an embed to many servers) ----------

_BCAST_CHANNEL_KEYWORDS = [
    'staff-alert', 'staff-alerts', 'bot-alerts', 'bot-log', 'bot-logs',
    'staff', 'admin', 'mods', 'mod', 'dev', 'ops', 'announce', 'team',
]


def _dash_bcast_fetch_channels(guild_id):
    """Return (list_of_text_channels, error_or_none). Cached."""
    cached = _dash_cache_get('bcast_channels', guild_id, _DISCORD_CHANNEL_CACHE_TTL)
    if cached is not None:
        return cached, None
    status, body, _headers = _dash_bot_api(f'/guilds/{guild_id}/channels')
    if status != 200 or not isinstance(body, list):
        return [], f'discord {status}'
    # Type 0 = GUILD_TEXT, 5 = GUILD_ANNOUNCEMENT
    out = [
        {'id': str(c.get('id')), 'name': c.get('name') or '',
         'type': c.get('type'), 'position': c.get('position', 0)}
        for c in body if c.get('type') in (0, 5)
    ]
    out.sort(key=lambda c: (c['position'], c['name']))
    _dash_cache_set('bcast_channels', guild_id, out)
    return out, None


def _dash_guess_broadcast_channel(guild_id, text_channels):
    """Pick a 'staff-ish' text channel for announcements. Heuristic:
    1. channel whose name contains a keyword, preferring longer/earlier matches
    2. channel with the most presets for this guild
    3. first text channel
    """
    if not text_channels:
        return None
    for kw in _BCAST_CHANNEL_KEYWORDS:
        for c in text_channels:
            if kw in (c['name'] or '').lower():
                return c
    conn = _bot_db_conn()
    if conn is not None:
        try:
            cur = conn.cursor()
            cur.execute(
                'SELECT channel_id, COUNT(*) AS n FROM alert_presets '
                'WHERE guild_id = %s GROUP BY channel_id ORDER BY n DESC LIMIT 1',
                (int(guild_id),),
            )
            row = cur.fetchone()
            if row:
                cid = str(row['channel_id'])
                match = next((c for c in text_channels if c['id'] == cid), None)
                if match:
                    return match
        except Exception:
            pass
        finally:
            conn.close()
    return text_channels[0]


# 60s TTL cache for broadcast targets — list of guilds and their channels
# rarely changes minute-to-minute, and the underlying Discord API calls
# are the dominant cost on a cold load.
_DASH_BCAST_TARGETS_CACHE = {'data': None, 'ts': 0}
_DASH_BCAST_TARGETS_CACHE_LOCK = threading.Lock()
_DASH_BCAST_TARGETS_TTL = 60


@app.route('/api/dashboard/admin/broadcast/targets', methods=['GET'])
@_dash_require_admin()
def dashboard_admin_broadcast_targets():
    now = time.time()
    with _DASH_BCAST_TARGETS_CACHE_LOCK:
        cached = _DASH_BCAST_TARGETS_CACHE
        if cached['data'] is not None and (now - cached['ts']) < _DASH_BCAST_TARGETS_TTL:
            return jsonify({'targets': cached['data']})

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute('SELECT DISTINCT guild_id FROM alert_presets ORDER BY guild_id')
        guild_ids = [str(r['guild_id']) for r in cur.fetchall()]
    finally:
        conn.close()

    name_map = _dash_guild_meta_lookup(guild_ids)

    # Parallelize the per-guild channel lookups. _dash_bcast_fetch_channels
    # makes a Discord API call per guild — sequentially that was N×~150ms
    # = several seconds with 30+ guilds. Same 8-way concurrency pattern as
    # _dash_guild_meta_lookup, well under Discord's rate-limit ceiling.
    from concurrent.futures import ThreadPoolExecutor
    channels_by_gid = {}
    if guild_ids:
        max_workers = min(8, len(guild_ids))
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                for gid, result in zip(
                    guild_ids,
                    ex.map(_dash_bcast_fetch_channels, guild_ids)
                ):
                    channels_by_gid[gid] = result  # (channels, err)
        except Exception as e:
            Log.warn(f"broadcast targets parallel fetch failed: {e}")

    out = []
    for gid in guild_ids:
        channels, err = channels_by_gid.get(gid, ([], 'fetch_failed'))
        guess = _dash_guess_broadcast_channel(gid, channels)
        meta = name_map.get(gid, {})
        out.append({
            'guild_id': gid,
            'guild_name': meta.get('name') or '',
            'guild_icon_url': meta.get('icon_url') or '',
            'detected_channel_id': guess['id'] if guess else None,
            'detected_channel_name': (guess or {}).get('name') or '',
            'channels': channels,
            'channels_error': err,
        })

    with _DASH_BCAST_TARGETS_CACHE_LOCK:
        _DASH_BCAST_TARGETS_CACHE['data'] = out
        _DASH_BCAST_TARGETS_CACHE['ts'] = now
    return jsonify({'targets': out})


def _hex_to_int(hex_str):
    if not hex_str:
        return None
    s = str(hex_str).strip().lstrip('#')
    if len(s) not in (3, 6):
        return None
    if len(s) == 3:
        s = ''.join(ch * 2 for ch in s)
    try:
        return int(s, 16)
    except ValueError:
        return None


@app.route('/api/dashboard/admin/broadcast', methods=['POST'])
@_dash_require_admin()
def dashboard_admin_broadcast():
    """Enqueue a broadcast for the bot worker to deliver. Previously this
    endpoint sent each Discord message synchronously, holding the request
    worker for up to N×10s. Now it just validates + queues; the bot's
    `drain_bot_actions` loop picks it up and uses channel.send() with
    discord.py's built-in rate-limit handling."""
    body = request.get_json(silent=True) or {}
    title = (body.get('title') or '').strip()
    description = (body.get('description') or '').strip()
    color_hex = (body.get('color') or '').strip()
    footer = (body.get('footer') or '').strip()
    url = (body.get('url') or '').strip()
    targets = body.get('targets') or []

    if not title and not description:
        return _dash_err('bad_request',
                         'title or description is required.', 400)
    if not isinstance(targets, list) or not targets:
        return _dash_err('bad_request', 'targets must be a non-empty list.', 400)

    # Normalise targets + dedupe by channel_id; reject malformed entries up
    # front so the bot worker doesn't have to.
    seen_channels = set()
    clean_targets = []
    for t in targets:
        if not isinstance(t, dict):
            continue
        gid = str(t.get('guild_id') or '').strip()
        cid = str(t.get('channel_id') or '').strip()
        if not (gid.isdigit() and cid.isdigit()):
            continue
        if cid in seen_channels:
            continue
        seen_channels.add(cid)
        clean_targets.append({'guild_id': gid, 'channel_id': cid})
    if not clean_targets:
        return _dash_err('bad_request',
                         'no valid targets after normalisation.', 400)

    params = {
        'title': title[:256],
        'description': description[:4000],
        'color': color_hex,
        'footer': footer[:2048],
        'url': url,
        'targets': clean_targets,
    }
    session = request._dash_session
    requested_by = str(session.get('uid') or '')

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO pending_bot_actions (action, params, requested_by) '
            'VALUES (%s, %s::jsonb, %s) RETURNING id',
            ('broadcast', json.dumps(params), requested_by),
        )
        new_id = cur.fetchone()['id']
        conn.commit()
    except Exception as e:
        conn.rollback()
        Log.error(f"dashboard broadcast enqueue error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()
    Log.info(f"dashboard broadcast queued: id={new_id} targets={len(clean_targets)} by={requested_by}")
    # 202 Accepted — the broadcast hasn't actually been delivered yet; the
    # client should poll /api/dashboard/admin/bot-actions to see status.
    return jsonify({
        'id': int(new_id),
        'queued': True,
        'total': len(clean_targets),
    }), 202


# ---------- Admin bot-action queue ----------

_ALLOWED_BOT_ACTIONS = {'sync', 'test', 'cleanup', 'broadcast'}


def _dash_bot_guild_ids_from_discord():
    """Authoritative 'which guilds is the bot in RIGHT NOW' — uses the bot
    token against Discord. Returns (set_of_guild_ids | None, error)."""
    status, body, _headers = _dash_bot_api('/users/@me/guilds')
    if status != 200 or not isinstance(body, list):
        return None, f'discord {status}'
    out = set()
    for g in body:
        try:
            out.add(str(g.get('id')))
        except Exception:
            continue
    return out, None


@app.route('/api/dashboard/admin/cleanup/candidates', methods=['GET'])
@_dash_require_admin()
def dashboard_admin_cleanup_candidates():
    live_ids, err = _dash_bot_guild_ids_from_discord()
    if live_ids is None:
        return _dash_err('discord_error', err or 'failed to list bot guilds', 502)

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute('''
            SELECT guild_id,
                   COUNT(*) AS preset_count,
                   COUNT(DISTINCT channel_id) AS channel_count,
                   SUM(CASE WHEN pager_enabled THEN 1 ELSE 0 END) AS pager_presets,
                   MAX(updated_at) AS last_change
              FROM alert_presets
          GROUP BY guild_id
        ''')
        rows = cur.fetchall()
    finally:
        conn.close()

    candidate_ids = [str(r['guild_id']) for r in rows if str(r['guild_id']) not in live_ids]
    name_map = _dash_guild_meta_lookup(candidate_ids)
    candidates = []
    for r in rows:
        gid = str(r['guild_id'])
        if gid in live_ids:
            continue
        meta = name_map.get(gid, {})
        lc = r['last_change']
        candidates.append({
            'guild_id': gid,
            'name': meta.get('name') or '',
            'icon_url': meta.get('icon_url') or '',
            'preset_count': int(r['preset_count'] or 0),
            'channel_count': int(r['channel_count'] or 0),
            'pager_presets': int(r['pager_presets'] or 0),
            'last_change': lc.isoformat() if lc else None,
        })
    return jsonify({'candidates': candidates, 'bot_guild_count': len(live_ids)})


@app.route('/api/dashboard/admin/bot-actions', methods=['GET'])
@_dash_require_admin()
def dashboard_admin_bot_actions_list():
    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute(
            'SELECT id, action, params, status, requested_by, '
            'requested_at, claimed_at, completed_at, result, error '
            'FROM pending_bot_actions ORDER BY requested_at DESC LIMIT 30'
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    out = []
    for r in rows:
        out.append({
            'id': int(r['id']),
            'action': r['action'],
            'params': r['params'] or {},
            'status': r['status'],
            'requested_by': r['requested_by'],
            'requested_at': r['requested_at'].isoformat() if r['requested_at'] else None,
            'claimed_at': r['claimed_at'].isoformat() if r['claimed_at'] else None,
            'completed_at': r['completed_at'].isoformat() if r['completed_at'] else None,
            'result': r['result'],
            'error': r['error'],
        })
    return jsonify({'actions': out})


@app.route('/api/dashboard/admin/bot-actions', methods=['POST'])
@_dash_require_admin()
def dashboard_admin_bot_actions_enqueue():
    body = request.get_json(silent=True) or {}
    action = (body.get('action') or '').strip().lower()
    if action not in _ALLOWED_BOT_ACTIONS:
        return _dash_err('bad_request',
                         f"action must be one of {sorted(_ALLOWED_BOT_ACTIONS)}.", 400)
    params = body.get('params') or {}
    if not isinstance(params, dict):
        return _dash_err('bad_request', 'params must be an object.', 400)

    # Per-action validation.
    if action == 'test':
        gid = params.get('guild_id')
        cid = params.get('channel_id')
        if not (gid and cid):
            return _dash_err('bad_request',
                             'test action needs params.guild_id and params.channel_id.', 400)
        at = params.get('alert_type') or 'all'
        params = {'guild_id': str(gid), 'channel_id': str(cid), 'alert_type': str(at)}
    elif action == 'cleanup':
        gids = params.get('guild_ids') or []
        if not isinstance(gids, list) or not gids:
            return _dash_err('bad_request',
                             'cleanup action needs params.guild_ids (non-empty array).', 400)
        params = {'guild_ids': [str(x) for x in gids]}
    else:  # sync
        params = {}

    session = request._dash_session
    requested_by = str(session.get('uid') or '')

    conn = _bot_db_conn()
    if conn is None:
        return _dash_err('dashboard_disabled',
                         'BOT_DATA_DATABASE_URL is not configured.', 503)
    try:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO pending_bot_actions (action, params, requested_by) '
            'VALUES (%s, %s::jsonb, %s) RETURNING id',
            (action, json.dumps(params), requested_by),
        )
        new_id = cur.fetchone()['id']
        conn.commit()
    except Exception as e:
        conn.rollback()
        Log.error(f"dashboard bot-action enqueue error: {e}")
        return _dash_err('db_error', str(e), 500)
    finally:
        conn.close()
    Log.info(f"dashboard bot-action queued: action={action} id={new_id} by={requested_by}")
    return jsonify({'id': int(new_id), 'action': action, 'params': params}), 201


@app.route('/api/dashboard/admin/sources', methods=['GET'])
@_dash_require_admin()
def dashboard_admin_sources():
    """Return upstream data-source health for the admin dashboard panel.

    Iterates _SOURCE_THRESHOLDS so every configured source shows up — sources
    that have never been called yet appear with state='unknown'. State buckets
    follow the soft/hard age thresholds defined per-source plus a consec-fails
    rule (>=5 consecutive failures → 'down').
    """
    now = int(time.time())
    out = []
    with _SOURCE_HEALTH_LOCK:
        snapshot = {k: dict(v) for k, v in _SOURCE_HEALTH.items()}

    for name, cfg in _SOURCE_THRESHOLDS.items():
        rec = snapshot.get(name) or {}
        last_success = rec.get('last_success')
        last_error = rec.get('last_error')
        consec_fails = int(rec.get('consec_fails') or 0)
        age = (now - last_success) if last_success else None

        soft = int(cfg.get('soft') or 300)
        hard = int(cfg.get('hard') or 900)

        if last_success is None:
            state = 'unknown'
        elif consec_fails >= 5 or (age is not None and age > hard):
            state = 'down'
        elif consec_fails > 0 or (age is not None and age > soft):
            state = 'degraded'
        else:
            state = 'ok'

        out.append({
            'name': name,
            'label': cfg.get('label') or name,
            'last_success': last_success,
            'last_error': last_error,
            'last_error_msg': rec.get('last_error_msg'),
            'consec_fails': consec_fails,
            'total_success': int(rec.get('total_success') or 0),
            'total_fail': int(rec.get('total_fail') or 0),
            'age_seconds': age,
            'state': state,
        })

    return jsonify({'sources': out, 'server_time': now})


@app.route('/api/dashboard/admin/sources', methods=['DELETE'])
@_dash_require_admin()
def dashboard_admin_sources_clear():
    """Reset every source's counters to zero. Used by the admin "Clear stats"
    button when an upstream incident has skewed the totals."""
    _source_health_clear_all()
    Log.info("source_health: cleared by admin")
    return jsonify({'ok': True})


# ==================== END DASHBOARD ====================


# ============== INITIALIZATION ==============
_initialized = False

def initialize():
    """Initialize database and start background threads"""
    global _initialized
    if _initialized:
        return  # Avoid double initialization in Flask debug mode
    _initialized = True
    
    # Register signal handlers for graceful shutdown (PM2 sends SIGTERM)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)
    
    init_archive_db()
    start_archive_thread()
    start_prewarm_thread()  # Start cache pre-warming
    start_cleanup_thread()  # Start data retention cleanup

    # Persistent source health: load whatever survived the last restart and
    # start the 60s flusher. Best-effort — silently no-ops without BOT_DATA_DATABASE_URL.
    try:
        _source_health_load_from_db()
        threading.Thread(
            target=_source_health_flusher_loop,
            daemon=True,
            name='source-health-flusher',
        ).start()
    except Exception as e:
        Log.warn(f"source_health init failed: {e}")

    # Start rdio-scanner transcript summary scheduler (hourly via Gemini)
    try:
        start_rdio_summary_thread()
    except Exception as e:
        Log.error(f"rdio summary scheduler failed to start: {e}")

    # Waze runs entirely off the Violentmonkey userscript ingest path now.
    if WAZE_INGEST_ENABLED:
        Log.startup("Waze: userscript ingest mode")
        if not WAZE_INGEST_KEY:
            Log.warn("Waze: WAZE_INGEST_KEY is empty — /api/waze/ingest will reject all POSTs")
        if WAZE_INGEST_STALE_SECS > 0:
            threading.Thread(
                target=_waze_staleness_watcher,
                daemon=True,
                name='waze-staleness-watcher',
            ).start()
            Log.startup(
                f"Waze: staleness watcher armed — threshold {WAZE_INGEST_STALE_SECS}s"
                + (f" (Discord ping enabled)" if WAZE_STALE_WEBHOOK else "")
            )

    mode_str = "DEV MODE" if DEV_MODE else "PRODUCTION"
    # Use lock to prevent prewarm thread logs from interleaving with banner
    with Log._lock:
        print("", flush=True)
        print("=" * 50, flush=True)
        print(f"  NSW PSN External API Proxy v2.4", flush=True)
        print(f"  Mode: {mode_str}", flush=True)
        print("=" * 50, flush=True)
    Log.startup("Database: PostgreSQL")
    Log.startup(f"Data retention: {DATA_RETENTION_DAYS} days")
    Log.startup("Cache pre-warming: ENABLED")
    if DEV_MODE:
        Log.info(f"Archive intervals: {IDLE_INTERVAL}s idle / {ACTIVE_INTERVAL}s active")
        Log.info("Verbose logging enabled - all requests will be logged")
    else:
        Log.info("Clean logging mode - only key events logged")
    with Log._lock:
        print("=" * 50, flush=True)
        print("", flush=True)


if __name__ == '__main__':
    # DEV_MODE controls verbose logging, NOT Flask's debug mode.
    # Flask debug mode enables the Werkzeug debugger (security risk, stores tracebacks
    # in memory) and the reloader (interferes with PM2). Never use in production.
    initialize()
    app.run(host=args.host, port=args.port, debug=False)
else:
    # When imported by WSGI server (gunicorn, etc.)
    initialize()