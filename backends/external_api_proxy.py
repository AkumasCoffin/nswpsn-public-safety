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
from psycopg2.extras import RealDictCursor
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
load_dotenv(os.path.join(_script_dir, '.env'))

from db import get_conn, get_conn_dict

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

# Try to import curl_cffi for better Cloudflare bypass
try:
    from curl_cffi import requests as curl_requests
    CURL_CFFI_AVAILABLE = True
except ImportError:
    CURL_CFFI_AVAILABLE = False
    curl_requests = None

app = Flask(__name__)

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

# Configure CORS to allow all origins explicitly
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Accept", "Authorization"],
        "supports_credentials": False
    }
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


# Ensure CORS headers are always set (backup in case reverse proxy interferes)
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Accept, Authorization'
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
    for page_id, session in active_page_sessions.items():
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
    skip_rate_limit = {'/api/heartbeat', '/api/health', '/api/config', '/api/cache/status', '/api/cache/stats'}
    if path in skip_rate_limit:
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
    
    if DEV_MODE:
        Log.prewarm(f"Endeavour: {len(areas)} incidents, {len(enrichment)} enriched")
    
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

# History database locks - one per source type
_db_lock_history_waze = threading.Lock()
_db_lock_history_traffic = threading.Lock()
_db_lock_history_rfs = threading.Lock()
_db_lock_history_power = threading.Lock()
_db_lock_history_pager = threading.Lock()
_db_lock_history_weather = threading.Lock()

# Map logical db keys to their locks
_DB_LOCKS = {
    DB_PATH_HISTORY_WAZE: _db_lock_history_waze,
    DB_PATH_HISTORY_TRAFFIC: _db_lock_history_traffic,
    DB_PATH_HISTORY_RFS: _db_lock_history_rfs,
    DB_PATH_HISTORY_POWER: _db_lock_history_power,
    DB_PATH_HISTORY_PAGER: _db_lock_history_pager,
    DB_PATH_HISTORY_WEATHER: _db_lock_history_weather,
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

# Track active page sessions with unique IDs and timestamps
# Format: {page_id: {'last_seen': timestamp, 'user_agent': str, 'ip': str, 'page_type': str, 'is_data_page': bool}}
active_page_sessions = {}
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

    # Run one-time migrations
    migrate_endeavour_categories()
    migrate_bom_sources()
    migrate_bom_subcategories()


# ==================== PERSISTENT DATA CACHE ====================
# SQLite-backed cache that survives restarts and is pre-warmed in background

def cache_set(endpoint, data, ttl=60, fetch_time_ms=0):
    """Store data in persistent PostgreSQL cache"""
    try:
        with _db_lock_cache:
            conn = get_conn()
            c = conn.cursor()
            c.execute('''
                INSERT INTO api_data_cache (endpoint, data, timestamp, ttl, fetch_time_ms)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (endpoint) DO UPDATE SET data = EXCLUDED.data, timestamp = EXCLUDED.timestamp, ttl = EXCLUDED.ttl, fetch_time_ms = EXCLUDED.fetch_time_ms
            ''', (endpoint, json.dumps(data), int(time.time()), ttl, fetch_time_ms))
            conn.commit()
            conn.close()
        return True
    except Exception as e:
        Log.cache(f"Set error for {endpoint}: {e}")
        return False

def cache_get(endpoint):
    """
    Get data from persistent cache.
    Returns: (data, age_seconds, is_expired)
    """
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('SELECT data, timestamp, ttl FROM api_data_cache WHERE endpoint = %s', (endpoint,))
        row = c.fetchone()
        conn.close()
        
        if row:
            data_str, timestamp, ttl = row
            age = int(time.time()) - timestamp
            is_expired = age >= ttl
            return json.loads(data_str), age, is_expired
        return None, 0, True
    except Exception as e:
        Log.cache(f"Get error for {endpoint}: {e}")
        return None, 0, True

def cache_get_any(endpoint):
    """
    Get data from cache even if expired.
    Returns: (data, age_seconds) or (None, 0)
    """
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('SELECT data, timestamp FROM api_data_cache WHERE endpoint = %s', (endpoint,))
        row = c.fetchone()
        conn.close()
        if row:
            return json.loads(row[0]), int(time.time()) - row[1]
        return None, 0
    except Exception as e:
        Log.cache(f"Get any error for {endpoint}: {e}")
        return None, 0

def cache_stats():
    """Get cache statistics for debug endpoint"""
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
        conn.close()
        
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


def store_incidents_batch(incidents, source_type=None):
    """
    Store multiple incidents in a single transaction with DEDUPLICATION.
    
    Only inserts a new row if:
    1. It's a new incident (never seen this source+source_id before), OR
    2. The data has changed since the last snapshot
    
    Also tracks is_live status:
    - All incidents in this batch are marked as is_live=1 (still in API)
    - Incidents from this source_type NOT in this batch are marked is_live=0
    
    Args:
        incidents: List of dicts with keys matching store_incident params
        source_type: The source type (e.g., 'rfs', 'traffic_incident') - used to mark
                     missing incidents as no longer live
    
    Returns:
        dict: {'total': int, 'new': int, 'changed': int, 'unchanged': int, 'ended': int}
    """
    if not incidents:
        return {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}
    
    # Determine source for logging
    src = source_type or (incidents[0].get('source') if incidents else 'unknown')
    
    # Friendly names for archive logging
    ARCHIVE_NAMES = {
        'rfs': 'RFS',
        'traffic_incident': 'Traffic',
        'traffic_roadwork': 'Roadwork',
        'traffic_flood': 'Floods',
        'traffic_fire': 'Traffic Fire',
        'traffic_majorevent': 'Major Events',
        'waze_hazard': 'Waze Hazards',
        'waze_police': 'Waze Police',
        'waze_roadwork': 'Waze Roadwork',
        'endeavour_current': 'Endeavour',
        'endeavour_planned': 'Endeavour Planned',
        'ausgrid': 'Ausgrid',
        'bom_warning': 'BOM Warnings',
    }
    name = ARCHIVE_NAMES.get(src, src)
    
    # Log archive start in dev mode (verbose)
    if DEV_MODE:
        Log.data(f"📥 {name} archiving {len(incidents)} incidents...")
    
    max_retries = 3
    retry_delay = 1.0
    start_time = time.time()
    
    for attempt in range(max_retries):
        try:
            result = _store_incidents_batch_inner(incidents, source_type)
            elapsed_ms = int((time.time() - start_time) * 1000)
            
            # Log archive results in dev (prewarm_single logs per-source in prod)
            if DEV_MODE:
                ended_str = f", Ended: {result['ended']}" if result['ended'] > 0 else ""
                Log.data(f"📦 {name}: New: {result['new']}, Old: {result['unchanged']}, Upd: {result['changed']}{ended_str} [{elapsed_ms}ms]")
            
            return result
        except psycopg2.OperationalError as e:
            if 'locked' in str(e).lower() and attempt < max_retries - 1:
                Log.warn(f"Database locked, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                Log.error(f"DataHistory batch store error: {e}")
                return {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}
        except Exception as e:
            Log.error(f"DataHistory batch store error: {e}")
            return {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}
    return {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}

def _store_incidents_batch_inner(incidents, source_type=None):
    """Inner function for store_incidents_batch - uses source-specific history DB"""
    gone_ids = set()  # Track incidents that are no longer live
    try:
        fetched_at = int(time.time())
        
        # Determine source type from incidents if not provided
        if source_type is None and incidents:
            source_type = incidents[0].get('source')
        
        # Get the right database and lock for this source type
        db_path = get_history_db_for_source(source_type)
        db_lock = get_history_lock_for_source(source_type)
        
        with db_lock:
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
                    # First, mark previous "latest" rows as not latest for source_ids we're inserting
                    source_ids_to_update = [(r[0], r[1]) for r in rows_to_insert if r[1]]  # (source, source_id)
                    if source_ids_to_update:
                        for src, sid in source_ids_to_update:
                            c.execute('''
                                UPDATE data_history SET is_latest = 0 
                                WHERE source = %s AND source_id = %s AND is_latest = 1
                            ''', (src, sid))
                    
                    # Insert new rows with is_latest = 1
                    c.executemany('''
                        INSERT INTO data_history 
                        (source, source_id, source_provider, source_type, fetched_at, source_timestamp, source_timestamp_unix,
                         latitude, longitude, location_text, title, category, subcategory, 
                         status, severity, data, is_active, is_live, last_seen, data_hash, is_latest)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                    ''', rows_to_insert)
                
                # Update last_seen for ALL incidents in this batch (even if data unchanged)
                if all_source_ids_in_batch:
                    for source, source_id in all_source_ids_in_batch:
                        c.execute('''
                            UPDATE data_history 
                            SET last_seen = %s, is_live = 1
                            WHERE source = %s AND source_id = %s
                        ''', (fetched_at, source, source_id))
                
                # Mark incidents NO LONGER in API response as is_live = 0
                # Only do this if we have a source_type and received at least some data
                if source_type and all_source_ids_in_batch:
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
    try:
        with _db_lock_history_waze:  # Use any history lock for coordination
            conn = get_conn()
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
            
            # Delete old data_history entries
            c.execute('DELETE FROM data_history WHERE fetched_at < %s', (cutoff,))
            deleted_history += c.rowcount
            
            # Delete records from deprecated sources
            for dep_src in DEPRECATED_SOURCES:
                c.execute('DELETE FROM data_history WHERE source = %s', (dep_src,))
                if c.rowcount > 0:
                    Log.cleanup(f"Removed {c.rowcount} deprecated {dep_src} records")
            
            conn.commit()
            conn.close()
    except Exception as e:
        Log.error(f"Cleanup history error: {e}")
    
    # Clean up stats.db
    try:
        with _db_lock_stats:
            conn = get_conn()
            c = conn.cursor()
            
            # Delete old stats_snapshots (convert ms to seconds for comparison)
            c.execute('DELETE FROM stats_snapshots WHERE timestamp < %s', (cutoff * 1000,))
            deleted_stats = c.rowcount
            
            conn.commit()
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
    try:
        with _db_lock_history_power:
            conn = get_conn()
            c = conn.cursor()
            
            # Check if migration is needed - look for any Endeavour records
            c.execute("SELECT COUNT(*) FROM data_history WHERE source LIKE 'endeavour%'")
            count = c.fetchone()[0]
            
            if count == 0:
                conn.close()
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
    try:
        with _db_lock_history_weather:
            conn = get_conn()
            c = conn.cursor()
            
            # Check if migration is needed
            c.execute("SELECT COUNT(*) FROM data_history WHERE source = 'bom_warning'")
            count = c.fetchone()[0]
            
            if count == 0:
                conn.close()
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
    try:
        with _db_lock_history_weather:
            conn = get_conn()
            c = conn.cursor()
            
            # Check if migration is needed - look for records with old-style subcategories
            c.execute("""
                SELECT COUNT(*) FROM data_history 
                WHERE (source = 'bom_land' OR source = 'bom_marine')
                AND (subcategory = 'land' OR subcategory = 'marine' OR subcategory IS NULL)
            """)
            count = c.fetchone()[0]
            
            if count == 0:
                conn.close()
                return 0
            
            Log.info(f"🔧 BOM subcategory migration: {count} records to update...")
            
            # Get all BOM records that need updating
            c.execute("""
                SELECT id, title FROM data_history 
                WHERE (source = 'bom_land' OR source = 'bom_marine')
                AND (subcategory = 'land' OR subcategory = 'marine' OR subcategory IS NULL)
            """)
            records = c.fetchall()
            
            updated = 0
            for record_id, title in records:
                warning_type = _extract_bom_warning_type(title)
                c.execute("UPDATE data_history SET subcategory = %s WHERE id = %s", (warning_type, record_id))
                updated += 1
            
            conn.commit()
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
                
                conn.close()
            except Exception as e:
                Log.error(f"Stats error for {os.path.basename(db_path)}: {e}")
        
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
    
    # Shut down Waze browser worker if running
    try:
        global _waze_browser_ready
        _waze_browser_ready = False
        _waze_request_queue.put(None)
    except Exception:
        pass
    
    # (Endeavour browser worker removed - now uses Supabase API directly)
    
    # Give threads a moment to exit their loops
    time.sleep(0.5)
    
    Log.info("✅ Shutdown complete")
    
    # Use os._exit() to forcibly terminate - sys.exit() won't work if threads
    # are blocked on locks or HTTP requests
    os._exit(0)

def cleanup_loop():
    """Background loop that periodically cleans up old data"""
    global _cleanup_running
    cleanup_counter = 0
    while _cleanup_running:
        cleanup_old_data()
        cleanup_stale_sessions()
        _cleanup_centralwatch_image_cache()
        cleanup_counter += 1
        # Clean up rate limit data every 5 cycles
        if cleanup_counter % 5 == 0:
            _cleanup_rate_limits()
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
            conn.close()
        
        if DEV_MODE:
            Log.data(f"Archived stats snapshot")
        return True
    except Exception as e:
        Log.error(f"Archive error: {e}")
        return False

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
    global active_page_sessions
    now = time.time()
    stale_sessions = [
        (page_id, session) for page_id, session in active_page_sessions.items()
        if now - session['last_seen'] > PAGE_SESSION_TIMEOUT
    ]
    for page_id, session in stale_sessions:
        short_id = page_id[-6:] if len(page_id) > 6 else page_id
        del active_page_sessions[page_id]
        Log.cleanup(f"Session expired: ...{short_id}")
    
    # Log summary if sessions were cleaned in production
    if stale_sessions and not DEV_MODE:
        total = len(active_page_sessions)
        data = sum(1 for s in active_page_sessions.values() if s.get('is_data_page', False))
        Log.cleanup(f"{len(stale_sessions)} session(s) expired (viewers: {total}, data: {data})")
    
    return len(stale_sessions)

def get_active_page_count():
    """Get count of all active page sessions after cleaning stale sessions"""
    cleanup_stale_sessions()
    return len(active_page_sessions)

def get_data_page_count():
    """Get count of active DATA pages (pages that fetch live data)"""
    cleanup_stale_sessions()
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
        # Archive immediately on startup
        archive_current_stats()
        # Start background loop
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


def _prewarm_fetch_rfs():
    """Fetch and parse RFS incidents for cache"""
    features = []
    try:
        r = requests.get('https://www.rfs.nsw.gov.au/feeds/majorIncidents.xml',
                        timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
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
    except Exception as e:
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
    except Exception as e:
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


def _prewarm_fetch_waze(category):
    """Fetch and parse Waze data for cache"""
    # Use existing fetch_waze_data which handles all regions
    alerts, jams = fetch_waze_data()
    features = []
    jam_features = []
    
    for alert in alerts:
        alert_type = alert.get('type', '').upper()
        subtype = alert.get('subtype', '') or ''
        subtype_upper = subtype.upper()
        
        # Filter based on category
        if category == 'hazards':
            is_police = (alert_type == 'POLICE' or 'POLICE' in subtype_upper)
            is_roadwork = (alert_type == 'CONSTRUCTION' or 'CONSTRUCTION' in subtype_upper)
            if is_police or is_roadwork:
                continue
            if alert_type in {'HAZARD', 'ACCIDENT', 'JAM', 'ROAD_CLOSED'}:
                feature = parse_waze_alert(alert, 'Hazard')
                if feature:
                    features.append(feature)
        elif category == 'police':
            is_police = (alert_type == 'POLICE' or 'POLICE' in subtype_upper)
            if is_police:
                feature = parse_waze_alert(alert, 'Police')
                if feature:
                    features.append(feature)
        elif category == 'roadwork':
            is_roadwork = (alert_type == 'CONSTRUCTION' or 'CONSTRUCTION' in subtype_upper)
            if is_roadwork:
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


def _prewarm_fetch_ausgrid(data_type):
    """Fetch and parse Ausgrid data for cache"""
    url_map = {
        'outages': 'https://www.ausgrid.com.au/webapi/OutageMapData/GetCurrentUnplannedOutageMarkersAndPolygons',
        'stats': 'https://www.ausgrid.com.au/webapi/outagemapdata/GetCurrentOutageStats',
    }
    try:
        r = requests.get(url_map[data_type], timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            return r.json()
    except Exception as e:
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
        
        if DEV_MODE:
            Log.info(f"Pagermon URL: {url}")
        
        resp = requests.get(url, headers=headers, timeout=15)
        if not resp.ok:
            Log.error(f"Pagermon API error: {resp.status_code}")
            if DEV_MODE:
                Log.error(f"Pagermon response: {resp.text[:200]}")
            return {'messages': [], 'count': 0}
        
        data = resp.json()
        messages = data.get('messages', [])
        
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
    
    try:
        # Individual fetch logging only in dev mode
        if DEV_MODE:
            Log.prewarm(f"{name} fetching...")
        
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
            # Don't cache empty Waze results — browser worker may not be ready yet
            if cache_key.startswith('waze_') and isinstance(data, dict) and len(data.get('features', [])) == 0:
                fetch_time_ms = int((time.time() - start_time) * 1000)
                # Still log but don't cache, so next request triggers a fresh fetch
            else:
                fetch_time_ms = int((time.time() - start_time) * 1000)
                cache_set(cache_key, data, ttl, fetch_time_ms)
            
            # Count items in data for logging
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
            
            # Store in historical data table and get stats
            history_records = _extract_history_records(cache_key, data)
            stats = {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}
            if history_records:
                stats = store_incidents_batch(history_records)
            
            # Log per-source archive stats (always in prod when we have history; verbose in dev)
            has_stats = stats['new'] > 0 or stats['unchanged'] > 0 or stats['changed'] > 0 or stats['ended'] > 0
            if history_records and has_stats:
                ended_str = f", Ended: {stats['ended']}" if stats['ended'] > 0 else ""
                Log.prewarm(f"{name}: New: {stats['new']}, Old: {stats['unchanged']}, Upd: {stats['changed']}{ended_str} [{fetch_time_ms}ms]")
            elif DEV_MODE:
                Log.prewarm(f"{name}: {item_count} items [{fetch_time_ms}ms]")
            
            # Return stats for aggregation
            return True, fetch_time_ms, stats
    except Exception as e:
        if DEV_MODE:
            Log.prewarm(f"❌ {name} failed: {e}")
        else:
            Log.error(f"{name} fetch failed: {e}")
    
    return False, 0, {'total': 0, 'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}


def prewarm_loop():
    """Background loop that keeps all caches warm
    
    Refresh intervals:
    - Active mode (map page open): Every 60 seconds (1 min)
    - Idle mode (no one on page): Every 120 seconds (2 min)
    """
    global _prewarm_running
    last_fetch = {key: 0 for key, _ in PREWARM_CONFIG}
    last_mode_log = 0
    
    Log.startup("Cache pre-warming started")
    
    # Initial prewarm of all endpoints (parallel)
    from concurrent.futures import ThreadPoolExecutor
    initial_start = time.time()
    
    if DEV_MODE:
        Log.prewarm("Fetching all sources...")
    
    # Aggregate stats
    total_stats = {'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}
    
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for cache_key, ttl in PREWARM_CONFIG:
            futures.append(executor.submit(prewarm_single, cache_key, ttl))
        for f in futures:
            try:
                success, fetch_ms, stats = f.result()
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
    ended_str = f", Ended: {total_stats['ended']}" if total_stats['ended'] > 0 else ""
    Log.prewarm(f"Fetch complete: New: {total_stats['new']}, Old: {total_stats['unchanged']}, Upd: {total_stats['changed']}{ended_str} [{elapsed_ms}ms]")
    
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
    while _prewarm_running:
        now = time.time()
        
        # Determine refresh interval based on whether any DATA page is open
        active = is_page_active()
        refresh_interval = PREWARM_ACTIVE_INTERVAL if active else PREWARM_IDLE_INTERVAL
        
        # Log mode changes (but not too often)
        if now - last_mode_log > 60:
            mode_str = "ACTIVE (1 min)" if active else "IDLE (2 min)"
            if DEV_MODE:
                Log.prewarm(f"Mode: {mode_str}")
            last_mode_log = now
        
        # Check each endpoint - refresh all at the same interval
        needs_refresh = []
        for cache_key, ttl in PREWARM_CONFIG:
            if now - last_fetch.get(cache_key, 0) >= refresh_interval:
                needs_refresh.append((cache_key, ttl))
        
        # Refresh all stale endpoints in parallel
        if needs_refresh:
            refresh_start = time.time()
            
            if DEV_MODE:
                Log.prewarm("Fetching all sources...")
            
            # Aggregate stats
            cycle_stats = {'new': 0, 'changed': 0, 'unchanged': 0, 'ended': 0}
            
            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(prewarm_single, key, ttl): key for key, ttl in needs_refresh}
                for f in futures:
                    try:
                        success, fetch_ms, stats = f.result()
                        last_fetch[futures[f]] = now
                        cycle_stats['new'] += stats.get('new', 0)
                        cycle_stats['changed'] += stats.get('changed', 0)
                        cycle_stats['unchanged'] += stats.get('unchanged', 0)
                        cycle_stats['ended'] += stats.get('ended', 0)
                    except Exception:
                        pass
            
            if not DEV_MODE:
                elapsed_ms = int((time.time() - refresh_start) * 1000)
                ended_str = f", Ended: {cycle_stats['ended']}" if cycle_stats['ended'] > 0 else ""
                Log.prewarm(f"Fetch complete: New: {cycle_stats['new']}, Old: {cycle_stats['unchanged']}, Upd: {cycle_stats['changed']}{ended_str} [{elapsed_ms}ms]")
            else:
                Log.prewarm(f"Refreshed {len(needs_refresh)} endpoints")
        
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
            cache[key] = {'data': result, 'time': now}
            return result
        return wrapper
    return decorator

# ============== API KEY AUTHENTICATION ==============
# Set your API key via environment variable or use the default
# Generate a secure key: python -c "import secrets; print(secrets.token_urlsafe(32))"
API_KEY = os.environ.get('NSWPSN_API_KEY', '')

# Public endpoints that don't require auth (health checks, etc)
PUBLIC_ENDPOINTS = {'/api/health', '/', '/api/config', '/api/heartbeat', '/api/debug/sessions', '/api/debug/heartbeat-test', '/api/editor-requests'}
# Endpoints that start with these prefixes are public (for dynamic routes like /api/check-editor/<user_id>)
PUBLIC_ENDPOINT_PREFIXES = ['/api/check-editor/', '/api/centralwatch/image/', '/api/centralwatch/cameras']

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
    
    # Fallback to live fetch if cache empty
    try:
        r = requests.get(
            'https://www.ausgrid.com.au/webapi/OutageMapData/GetCurrentUnplannedOutageMarkersAndPolygons',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        data = r.json()
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
    except Exception as e:
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


def _parse_rss_feed(url, source_name, source_icon):
    """Parse an RSS feed and return normalized items with auto-detected categories"""
    import xml.etree.ElementTree as ET
    
    items = []
    try:
        r = requests.get(url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*'
        })
        
        if r.status_code != 200:
            Log.warn(f"RSS feed {source_name} returned status {r.status_code}")
            return items
        
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
        
        conn.close()
        
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
            is_new = page_id not in active_page_sessions
            old_page_type = active_page_sessions.get(page_id, {}).get('page_type', None)
            was_data_page = active_page_sessions.get(page_id, {}).get('is_data_page', False)
            
            active_page_sessions[page_id] = {
                'last_seen': time.time(),
                'user_agent': user_agent,
                'ip': client_ip,
                'page_type': page_type,
                'is_data_page': is_data_page,
                'opened_at': active_page_sessions.get(page_id, {}).get('opened_at', time.time())
            }
            total_count = len(active_page_sessions)
            data_count = get_data_page_count()
            
            if is_new:
                # New viewer - always log
                Log.viewer(f"👤 Joined: ...{short_id} on {page_type} (viewers: {total_count}, data: {data_count})")
            elif old_page_type != page_type:
                # Navigation - always log page changes
                Log.viewer(f"📄 ...{short_id}: {old_page_type} → {page_type} (viewers: {total_count}, data: {data_count})")
            
            # Check for mode change (idle <-> active)
            _check_mode_change()
        else:
            Log.api(f"Page opened (no page_id) from {client_ip}")
    
    elif action == 'close':
        if page_id and page_id in active_page_sessions:
            # Only process close if it's from the CURRENT page
            # (ignore stale close signals from pages user navigated away from)
            session_current_page = active_page_sessions[page_id].get('page_type', 'unknown')
            if session_current_page == page_type:
                # User is actually leaving this page
                del active_page_sessions[page_id]
                total_count = len(active_page_sessions)
                data_count = get_data_page_count()
                Log.viewer(f"👋 Left: ...{short_id} (viewers: {total_count}, data: {data_count})")
                # Check for mode change (idle <-> active)
                _check_mode_change()
            else:
                # Stale close from old page after navigation - ignore
                Log.api(f"Ignoring stale close from {page_type} (session on {session_current_page})")
        elif not page_id:
            Log.api(f"Page close signal (no page_id) from {client_ip}")
    
    else:
        # Regular heartbeat ping - update session timestamp
        if page_id:
            if page_id in active_page_sessions:
                active_page_sessions[page_id]['last_seen'] = time.time()
            else:
                # New session via ping - register it
                active_page_sessions[page_id] = {
                    'last_seen': time.time(),
                    'user_agent': user_agent,
                    'ip': client_ip,
                    'page_type': page_type,
                    'is_data_page': is_data_page,
                    'opened_at': time.time()
                }
    
    # Stale session cleanup is now logged individually in cleanup_stale_sessions()
    
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
            for pid, session in active_page_sessions.items()
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

# playwright-stealth patches all Datadome/bot detection vectors
_stealth_available = False
_stealth_v2 = False  # Granitosaurus/playwright-stealth v2.x
try:
    from playwright_stealth import Stealth
    _stealth_obj = Stealth()
    _stealth_available = True
    _stealth_v2 = True
except ImportError:
    try:
        from playwright_stealth import stealth_sync
        _stealth_available = True
    except ImportError:
        pass

# Fallback: requests session for when playwright is not available
_centralwatch_session = None
_centralwatch_session_lock = threading.Lock()

# API endpoints to try (in order). The /api/v1/public/ endpoint is what the
# actual Central Watch website uses and may have higher/different rate limits.
_CENTRALWATCH_API_ENDPOINTS = [
    'https://centralwatch.watchtowers.io/api/v1/public/cameras',
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
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
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
    
    while True:
        try:
            # Wait for a request (with timeout so we can do periodic maintenance)
            try:
                task = _cw_request_queue.get(timeout=30)
            except _queue_mod.Empty:
                # Periodic: re-solve Vercel challenge every 15 min to keep cookies fresh
                if time.time() - last_challenge_refresh >= 900:
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
                            const reader = new FileReader();
                            return new Promise(resolve => {
                                reader.onloadend = () => resolve({
                                    ok: true, status: resp.status,
                                    contentType: blob.type, size: blob.size,
                                    data: reader.result
                                });
                                reader.readAsDataURL(blob);
                            });
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
                                const reader = new FileReader();
                                return new Promise(resolve => {
                                    reader.onloadend = () => resolve({
                                        id, ok: true,
                                        contentType: blob.type, size: blob.size,
                                        data: reader.result
                                    });
                                    reader.readAsDataURL(blob);
                                });
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
                                    resolve({ id, ok: false, error: 'timeout' });
                                }, TIMEOUT);
                                
                                const img = new Image();
                                
                                img.onload = () => {
                                    clearTimeout(timer);
                                    try {
                                        const c = document.createElement('canvas');
                                        c.width = img.naturalWidth;
                                        c.height = img.naturalHeight;
                                        c.getContext('2d').drawImage(img, 0, 0);
                                        const dataUrl = c.toDataURL('image/jpeg', 0.92);
                                        resolve({
                                            id, ok: true,
                                            contentType: 'image/jpeg',
                                            size: img.naturalWidth * img.naturalHeight,
                                            data: dataUrl
                                        });
                                    } catch (e) {
                                        resolve({ id, ok: false, error: 'canvas: ' + e.toString() });
                                    }
                                };
                                
                                img.onerror = () => {
                                    clearTimeout(timer);
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

    stale = [cid for cid, entry in _centralwatch_image_cache.items()
             if (now - entry.get('timestamp', 0)) > max_age or (active_ids and cid not in active_ids)]

    for cid in stale:
        del _centralwatch_image_cache[cid]

    if stale and DEV_MODE:
        Log.cleanup(f"CW image cache: evicted {len(stale)} stale images, {len(_centralwatch_image_cache)} remaining")


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
                if DEV_MODE:
                    Log.info(f"Central Watch images: Failed {cid[:8]}.. (HTTP {status})")
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

# NSW regions for Waze requests - split into smaller areas to get more data
# Waze API limits to ~200 alerts per request, so we split NSW into multiple regions
NSW_REGIONS = [
    # Sydney CBD & Inner suburbs (highest density)
    {'name': 'Sydney CBD', 'top': -33.75, 'bottom': -34.05, 'left': 151.0, 'right': 151.35},
    
    # Sydney - Eastern suburbs & North Shore
    {'name': 'Sydney East', 'top': -33.65, 'bottom': -34.0, 'left': 151.15, 'right': 151.55},
    
    # Sydney - Western suburbs
    {'name': 'Sydney West', 'top': -33.65, 'bottom': -34.1, 'left': 150.7, 'right': 151.1},
    
    # Sydney - South West
    {'name': 'Sydney South West', 'top': -33.85, 'bottom': -34.25, 'left': 150.6, 'right': 151.1},
    
    # Central Coast
    {'name': 'Central Coast', 'top': -33.15, 'bottom': -33.65, 'left': 151.1, 'right': 151.7},
    
    # Newcastle / Hunter
    {'name': 'Newcastle Hunter', 'top': -32.6, 'bottom': -33.2, 'left': 151.3, 'right': 152.0},
    
    # Wollongong / Illawarra
    {'name': 'Wollongong', 'top': -34.15, 'bottom': -34.75, 'left': 150.65, 'right': 151.15},
    
    # Shoalhaven / Jervis Bay (Nowra, Berry, Jervis Bay to Sussex Inlet)
    {'name': 'Shoalhaven', 'top': -34.65, 'bottom': -35.4, 'left': 150.15, 'right': 150.95},
    
    # Blue Mountains / Penrith
    {'name': 'Blue Mountains', 'top': -33.45, 'bottom': -33.95, 'left': 150.2, 'right': 150.85},
    
    # Northern NSW Coast (Port Macquarie to Tweed)
    {'name': 'Northern Coast', 'top': -28.15, 'bottom': -31.5, 'left': 151.5, 'right': 153.65},
    
    # Northern NSW Inland (Tamworth, Armidale)
    {'name': 'Northern Inland', 'top': -28.15, 'bottom': -32.0, 'left': 149.5, 'right': 152.0},
    
    # Southern NSW / ACT / Canberra region
    {'name': 'Southern ACT', 'top': -34.5, 'bottom': -36.3, 'left': 148.5, 'right': 150.5},
    
    # South Coast (Batemans Bay to Eden) - Eden is around -37.07
    {'name': 'South Coast', 'top': -35.3, 'bottom': -37.1, 'left': 149.5, 'right': 150.35},
    
    # Western NSW (Dubbo, Orange, Bathurst)
    {'name': 'Central West', 'top': -31.5, 'bottom': -34.5, 'left': 147.5, 'right': 150.5},
    
    # Far Western NSW (Broken Hill area)
    {'name': 'Far West', 'top': -28.15, 'bottom': -34.0, 'left': 140.99, 'right': 148.0},
    
    # Riverina / Murray region - Murray River is the NSW/VIC border (~-36.0)
    {'name': 'Riverina', 'top': -34.0, 'bottom': -36.1, 'left': 143.5, 'right': 148.5},
]

# Proxy for Waze requests (bypass datacenter IP blocks)
# Single proxy:  WAZE_PROXY_URL=http://host:port
# Proxy list:    WAZE_PROXY_LIST=host1:port,host2:port,host3:port  (rotates per-request)
# Protocol:      WAZE_PROXY_PROTO=http (default: http, also supports socks5)
_waze_proxy_single = os.environ.get('WAZE_PROXY_URL', '').strip() or None
_waze_proxy_list_raw = os.environ.get('WAZE_PROXY_LIST', '').strip()
_waze_proxy_proto = os.environ.get('WAZE_PROXY_PROTO', 'http').strip()
_waze_proxy_pool = []
if _waze_proxy_list_raw:
    for p in _waze_proxy_list_raw.split(','):
        p = p.strip()
        if p:
            # Add protocol if not present
            if '://' not in p:
                p = f"{_waze_proxy_proto}://{p}"
            _waze_proxy_pool.append(p)
    import random
    random.shuffle(_waze_proxy_pool)
    Log.info(f"Waze: Loaded {len(_waze_proxy_pool)} proxies (rotating)")
elif _waze_proxy_single:
    _waze_proxy_pool = [_waze_proxy_single]
    Log.info(f"Waze: Using proxy {_waze_proxy_single.split('@')[-1] if '@' in _waze_proxy_single else _waze_proxy_single}")

_waze_proxy_index = 0
_waze_proxy_lock = threading.Lock()

def _get_waze_proxy():
    """Get next proxy URL from pool (round-robin). Returns None if no proxies configured."""
    global _waze_proxy_index
    if not _waze_proxy_pool:
        return None
    with _waze_proxy_lock:
        proxy = _waze_proxy_pool[_waze_proxy_index % len(_waze_proxy_pool)]
        _waze_proxy_index += 1
    return proxy

# Legacy compat
WAZE_PROXY_URL = _waze_proxy_single

# Cache for Waze data (shared across all endpoints)
_waze_cache = {'alerts': [], 'jams': [], 'timestamp': 0}
WAZE_CACHE_TTL = 120  # Cache Waze data for 2 minutes

# Waze browser worker - bypasses 403 when Waze blocks server IPs
_waze_request_queue = _queue_mod.Queue()
_waze_browser_worker_thread = None
_waze_browser_ready = False
_waze_fetch_lock = threading.Lock()  # Serialize fetches to avoid rate limiting from parallel prewarm

def _waze_browser_worker():
    """Dedicated thread that uses Playwright to fetch Waze data.
    Uses playwright-stealth to bypass Datadome WAF detection, and a persistent
    browser profile so cookies/Datadome tokens survive across restarts.
    Strategy: non-headless Chrome (off-screen) with stealth patches."""
    global _waze_browser_ready
    pw = None
    browser = None
    page = None
    try:
        pw = sync_playwright().start()

        launch_args = [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-infobars',
            '--window-size=1920,1080',
            '--ozone-platform=x11',  # Force X11 backend (needed for Xvfb on modern Chrome)
        ]

        # Clean browser profile on each start (stale Datadome cookies cause instant 403)
        _waze_user_data_dir = os.path.join(_script_dir, 'data', '.waze-browser-profile')
        import shutil
        if os.path.exists(_waze_user_data_dir):
            try:
                shutil.rmtree(_waze_user_data_dir)
            except Exception:
                pass
        os.makedirs(_waze_user_data_dir, exist_ok=True)

        browser = None
        context = None
        channel_used = None

        # Start virtual display for non-headless Chrome (required on headless Linux)
        _xvfb_proc = None
        _xvfb_display = None
        if os.name != 'nt':
            existing_display = os.environ.get('DISPLAY', '')
            if DEV_MODE:
                Log.info(f"Waze: Current DISPLAY={existing_display or '(not set)'}")
            try:
                import subprocess
                # Kill any leftover Xvfb from previous runs
                subprocess.run(['pkill', '-f', 'Xvfb :9[0-9]'], capture_output=True)
                subprocess.run(['pkill', '-f', 'Xvfb :10[0-9]'], capture_output=True)
                time.sleep(0.3)
                for disp_num in range(99, 110):
                    _xvfb_proc = subprocess.Popen(
                        ['Xvfb', f':{disp_num}', '-screen', '0', '1920x1080x24', '-nolisten', 'tcp'],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                    )
                    time.sleep(0.5)
                    if _xvfb_proc.poll() is None:  # Still running = success
                        _xvfb_display = f':{disp_num}'
                        os.environ['DISPLAY'] = _xvfb_display
                        Log.info(f"Waze: Started Xvfb on {_xvfb_display}")
                        break
                    _xvfb_proc = None
                if not _xvfb_display:
                    Log.warn("Waze: Could not start Xvfb, falling back to headless")
            except FileNotFoundError:
                Log.warn("Waze: Xvfb not installed (apt install xvfb), falling back to headless")
            except Exception as e:
                Log.warn(f"Waze: Xvfb failed ({e}), falling back to headless")

        # Build env dict with DISPLAY for Xvfb
        _browser_env = dict(os.environ)
        if _xvfb_display:
            _browser_env['DISPLAY'] = _xvfb_display

        # Strategy 1: Non-headless system Chrome with persistent context (best for WAF bypass)
        # Non-headless avoids all headless detection; requires X server or Xvfb
        if _xvfb_display or os.environ.get('DISPLAY') or os.name == 'nt':
            for channel in ['chrome', 'msedge', None]:
                try:
                    context = pw.chromium.launch_persistent_context(
                        _waze_user_data_dir,
                        channel=channel,
                        headless=False,
                        args=launch_args + ['--window-position=-32000,-32000'],
                        viewport={'width': 1920, 'height': 1080},
                        locale='en-AU',
                        timezone_id='Australia/Sydney',
                        extra_http_headers={'Accept-Language': 'en-AU,en;q=0.9'},
                        env=_browser_env,
                    )
                    channel_used = channel or 'chromium-visible'
                    break
                except Exception as e:
                    label = channel or 'chromium-visible'
                    if DEV_MODE:
                        Log.info(f"Waze: {label} non-headless failed, trying next...")

        # Strategy 2: Headless with stealth (less reliable but works on headless servers)
        if not context:
            for channel in ['chrome', 'msedge', None]:
                try:
                    context = pw.chromium.launch_persistent_context(
                        _waze_user_data_dir,
                        channel=channel,
                        headless=True,
                        args=launch_args,
                        viewport={'width': 1920, 'height': 1080},
                        locale='en-AU',
                        timezone_id='Australia/Sydney',
                        extra_http_headers={'Accept-Language': 'en-AU,en;q=0.9'},
                    )
                    channel_used = f'{channel or "chromium"}-headless'
                    break
                except Exception:
                    pass

        if not context:
            Log.error("Waze: No browser could be launched")
            _waze_browser_ready = False
            return

        stealth_label = ' +stealth' if _stealth_available else ''
        Log.info(f"Waze: Browser started ({channel_used}{stealth_label})")

        # Apply playwright-stealth BEFORE creating page (v2 applies to context)
        if _stealth_available:
            try:
                if _stealth_v2:
                    _stealth_obj.apply_stealth_sync(context)
                else:
                    pass  # v1 applies to page, done after new_page() below
            except Exception as e:
                Log.warn(f"Waze: stealth failed ({e})")

        page = context.new_page()

        # v1 stealth applies to page
        if _stealth_available and not _stealth_v2:
            try:
                stealth_sync(page)
            except Exception as e:
                Log.warn(f"Waze: stealth v1 failed ({e})")

        if not _stealth_available:
            # Fallback: basic stealth (less effective against Datadome)
            Log.warn("Waze: playwright-stealth not installed, using basic patches (pip install playwright-stealth)")
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-AU', 'en']});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                window.chrome = {runtime: {}, loadTimes: function(){}, csi: function(){}};
                Object.defineProperty(navigator, 'maxTouchPoints', {get: () => 0});
            """)

        _georss_responses = []
        _georss_lock = threading.Lock()
        _api_status = []
        _georss_403_body = [None]

        def _on_response(response):
            """Capture georss responses and log API call status codes."""
            try:
                url = response.url
                if 'waze.com' in url and '/api/' in url:
                    endpoint = url.split('?')[0].split('/api/')[-1]
                    _api_status.append(f"{response.status}:{endpoint}")
                if '/georss' in url:
                    if response.status == 200:
                        body = response.json()
                        with _georss_lock:
                            _georss_responses.append(body)
                    elif response.status == 403 and _georss_403_body[0] is None:
                        try:
                            _georss_403_body[0] = response.text()[:500]
                        except Exception:
                            pass
            except Exception:
                pass

        page.on('response', _on_response)

        # Clear stale cookies/session to avoid Datadome blocking from previous runs
        context.clear_cookies()

        page.goto('https://www.waze.com/live-map', timeout=45000, wait_until='domcontentloaded')
        # Wait for georss API call (don't use networkidle — Waze never stops polling)
        page.wait_for_timeout(8000)

        import random
        for _ in range(5):
            page.mouse.move(random.randint(300, 900), random.randint(200, 700))
            page.wait_for_timeout(random.randint(200, 600))
        page.mouse.click(600, 400)
        page.wait_for_timeout(3000)

        if DEV_MODE:
            cookies = context.cookies()
            cookie_names = [c['name'] for c in cookies if 'waze' in c.get('domain', '')]
            Log.info(f"Waze: Cookies: {cookie_names}")
            if _api_status:
                Log.info(f"Waze: API calls during load: {_api_status[:15]}")
            if _georss_403_body[0]:
                body_preview = _georss_403_body[0][:200].replace('\n', ' ')
                Log.info(f"Waze: 403 response body: {body_preview}")

        use_interception = False
        use_evaluate = False

        # Test page.evaluate() — much faster than navigation+interception
        test = _waze_page_fetch(page, NSW_REGIONS[0])
        if test and (test.get('alerts') or test.get('jams')):
            use_evaluate = True
            Log.info(f"Waze: Ready — direct fetch ({len(test.get('alerts',[]))} alerts, {len(test.get('jams',[]))} jams from test region)")
        else:
            # Check if interception worked during page load
            with _georss_lock:
                initial_count = len(_georss_responses)
            if initial_count > 0:
                use_interception = True
                Log.info("Waze: Ready — using navigation interception (direct fetch blocked)")
            else:
                Log.warn(f"Waze: WAF still blocking — all methods failed")
                if not _stealth_available:
                    Log.warn("Waze: Try: pip install playwright-stealth")
                elif 'headless' in (channel_used or ''):
                    Log.warn("Waze: Try non-headless: apt install xvfb")
        _waze_browser_ready = True
    except Exception as e:
        Log.error(f"Waze browser worker failed: {e}")
        try:
            if context: context.close()
        except Exception: pass
        try:
            if browser: browser.close()
        except Exception: pass
        try:
            if pw: pw.stop()
        except Exception: pass
        if _xvfb_proc:
            try: _xvfb_proc.terminate()
            except Exception: pass
        _waze_browser_ready = False
        return

    while True:
        try:
            task = _waze_request_queue.get(timeout=60)
            if task is None:
                break
            regions, result_q = task
            try:
                result = []
                for i, region in enumerate(regions):
                    if i > 0:
                        page.wait_for_timeout(1500)
                    data = None

                    if use_interception:
                        try:
                            with _georss_lock:
                                _georss_responses.clear()
                            lat = (region['top'] + region['bottom']) / 2
                            lon = (region['left'] + region['right']) / 2
                            span = abs(region['top'] - region['bottom'])
                            zoom = 10 if span > 2 else (11 if span > 1 else 12)
                            page.goto(
                                f"https://www.waze.com/live-map?lat={lat}&lon={lon}&zoom={zoom}",
                                timeout=20000, wait_until='domcontentloaded'
                            )
                            # Wait for georss API response (don't use networkidle — Waze never stops polling)
                            for _ in range(20):  # Wait up to 10s in 0.5s increments
                                page.wait_for_timeout(500)
                                with _georss_lock:
                                    if _georss_responses:
                                        break
                            with _georss_lock:
                                captured = list(_georss_responses)
                            if captured:
                                data = {'alerts': [], 'jams': []}
                                for resp in captured:
                                    data['alerts'].extend(resp.get('alerts', []))
                                    data['jams'].extend(resp.get('jams', []))
                        except Exception as e:
                            if DEV_MODE:
                                Log.warn(f"Waze nav error [{region['name']}]: {e}")

                    if not data or (not data.get('alerts') and not data.get('jams')):
                        # Always try page.evaluate as fallback — faster than navigation
                        data = _waze_page_fetch(page, region)

                    if data and (data.get('alerts') or data.get('jams')):
                        result.append({'ok': True, 'data': data})
                    else:
                        result.append({'ok': False, 'status': 0})
                result_q.put(result)
            except Exception as e:
                Log.warn(f"Waze browser fetch error: {e}")
                result_q.put([])
        except _queue_mod.Empty:
            continue
        except Exception as e:
            if not _shutdown_event.is_set():
                Log.warn(f"Waze worker: {e}")

    _waze_browser_ready = False
    try:
        if context: context.close()
    except Exception: pass
    try:
        if browser: browser.close()
    except Exception: pass
    try:
        if pw: pw.stop()
    except Exception: pass
    if _xvfb_proc:
        try: _xvfb_proc.terminate()
        except Exception: pass


def _waze_page_fetch(page, region):
    """Use page.evaluate() to call fetch() from within the Waze page context.
    The request inherits the page's cookies, origin, and TLS session."""
    try:
        js_code = """
            async ({top, bottom, left, right}) => {
                const url = new URL('https://www.waze.com/live-map/api/georss');
                url.searchParams.set('top', top);
                url.searchParams.set('bottom', bottom);
                url.searchParams.set('left', left);
                url.searchParams.set('right', right);
                url.searchParams.set('env', 'row');
                url.searchParams.set('types', 'alerts,traffic');
                const resp = await fetch(url.toString(), {
                    credentials: 'include',
                    headers: {
                        'Accept': 'application/json, text/plain, */*',
                        'Referer': 'https://www.waze.com/live-map',
                    }
                });
                if (!resp.ok) return {_error: resp.status};
                return await resp.json();
            }
        """
        data = page.evaluate(js_code, {
            'top': region['top'],
            'bottom': region['bottom'],
            'left': region['left'],
            'right': region['right'],
        })
        if isinstance(data, dict) and '_error' in data:
            if DEV_MODE:
                Log.warn(f"Waze: page.evaluate [{region['name']}]: HTTP {data['_error']}")
            return {'alerts': [], 'jams': []}
        return data if isinstance(data, dict) else {'alerts': [], 'jams': []}
    except Exception as e:
        if DEV_MODE:
            Log.warn(f"Waze: page.evaluate error [{region['name']}]: {e}")
        return {'alerts': [], 'jams': []}

def _start_waze_browser_worker():
    global _waze_browser_worker_thread
    if not _playwright_available:
        return
    if _waze_browser_worker_thread and _waze_browser_worker_thread.is_alive():
        return
    _waze_browser_worker_thread = threading.Thread(target=_waze_browser_worker, daemon=True, name='waze-browser')
    _waze_browser_worker_thread.start()

def _waze_browser_fetch_regions(regions):
    """Fetch Waze data for multiple regions by navigating the map. Returns list of result dicts."""
    if not _waze_browser_ready:
        return None
    result_q = _queue_mod.Queue()
    _waze_request_queue.put((regions, result_q))
    try:
        timeout = 30 + len(regions) * 8
        results = result_q.get(timeout=timeout)
        return results
    except _queue_mod.Empty:
        Log.warn("Waze browser fetch timed out")
        return None

def _fetch_waze_region_curl_cffi(region):
    """Fetch Waze via curl_cffi (TLS fingerprint spoofing) - bypasses datacenter blocking."""
    if not CURL_CFFI_AVAILABLE or not curl_requests:
        return {'alerts': [], 'jams': []}, 0
    try:
        url = 'https://www.waze.com/live-map/api/georss'
        params = {
            'top': region['top'], 'bottom': region['bottom'],
            'left': region['left'], 'right': region['right'],
            'env': 'row', 'types': 'alerts,traffic'
        }
        proxy = _get_waze_proxy()
        kwargs = {
            'params': params, 'timeout': 15,
            'impersonate': 'chrome',
            'headers': {'Accept': 'application/json', 'Accept-Language': 'en-AU,en;q=0.9', 'Referer': 'https://www.waze.com/live-map'}
        }
        if proxy:
            kwargs['proxy'] = proxy
        r = curl_requests.get(url, **kwargs)
        if r.status_code == 200:
            data = r.json()
            return data, 200
        if DEV_MODE:
            Log.warn(f"Waze curl_cffi [{region['name']}]: HTTP {r.status_code}")
        return {'alerts': [], 'jams': []}, r.status_code
    except Exception as e:
        if DEV_MODE:
            Log.warn(f"Waze curl_cffi [{region['name']}]: {e}")
        return {'alerts': [], 'jams': []}, 0

def fetch_waze_region(region):
    """Fetch Waze data for a single region. Returns (data_dict, status_code)."""
    try:
        url = 'https://www.waze.com/live-map/api/georss'
        params = {
            'top': region['top'],
            'bottom': region['bottom'],
            'left': region['left'],
            'right': region['right'],
            'env': 'row',
            'types': 'alerts,traffic'
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-AU,en;q=0.9',
            'Referer': 'https://www.waze.com/live-map'
        }
        proxy = _get_waze_proxy()
        proxies = {'https': proxy, 'http': proxy} if proxy else None
        r = requests.get(url, params=params, timeout=15, headers=headers, proxies=proxies)
        if r.status_code == 200:
            data = r.json()
            alerts = data.get('alerts', [])
            jams = data.get('jams', [])
            if not alerts and not jams and len(NSW_REGIONS) > 0 and region['name'] == NSW_REGIONS[0]['name']:
                top_keys = list(data.keys())[:10] if isinstance(data, dict) else []
                Log.warn(f"Waze [{region['name']}]: 0 alerts, 0 jams. Response keys: {top_keys}")
            return data, 200
        return {'alerts': [], 'jams': []}, r.status_code
    except requests.exceptions.Timeout:
        Log.warn(f"Waze [{region['name']}]: Request timeout")
    except Exception as e:
        Log.error(f"Waze fetch error for {region['name']}: {e}")
    return {'alerts': [], 'jams': []}, 0

def fetch_waze_data():
    """Fetch Waze alerts and jams for all NSW regions.
    Uses browser worker (Playwright) when ready, falls back to curl_cffi/requests."""
    global _waze_cache

    now = time.time()
    if now - _waze_cache['timestamp'] < WAZE_CACHE_TTL and _waze_cache['alerts']:
        return _waze_cache['alerts'], _waze_cache['jams']

    with _waze_fetch_lock:
        # Re-check: another thread may have populated cache while we waited for lock
        now2 = time.time()
        if now2 - _waze_cache['timestamp'] < WAZE_CACHE_TTL and _waze_cache['alerts']:
            return _waze_cache['alerts'], _waze_cache['jams']

        all_alerts = {}
        all_jams = {}

        # Strategy 1: Use browser worker if ready (bypasses Datadome WAF)
        if _waze_browser_ready:
            results = _waze_browser_fetch_regions(NSW_REGIONS)
            if results:
                for r in results:
                    if r and r.get('ok') and r.get('data'):
                        data = r['data']
                        for alert in data.get('alerts', []):
                            uuid = alert.get('uuid')
                            if uuid and uuid not in all_alerts:
                                all_alerts[uuid] = alert
                        for jam in data.get('jams', []):
                            uuid = jam.get('uuid')
                            if uuid and uuid not in all_jams:
                                all_jams[uuid] = jam
                if all_alerts or all_jams:
                    Log.info(f"Waze: Browser fetch OK ({len(all_alerts)} alerts, {len(all_jams)} jams)")
                else:
                    first = next((r for r in results if r), None)
                    sample = f"ok={first.get('ok')}, status={first.get('status')}" if first else "no results"
                    Log.warn(f"Waze: Browser fetch returned no data ({sample})")
            else:
                Log.warn("Waze: Browser fetch timed out or returned None")
        else:
            # Strategy 2: Try curl_cffi (only when browser not yet ready, e.g. during startup)
            from concurrent.futures import ThreadPoolExecutor, as_completed
            got_blocked = False

            if CURL_CFFI_AVAILABLE:
                with ThreadPoolExecutor(max_workers=8) as executor:
                    future_to_region = {executor.submit(_fetch_waze_region_curl_cffi, region): region for region in NSW_REGIONS}
                    for future in as_completed(future_to_region):
                        try:
                            data, status = future.result()
                            if status == 403:
                                got_blocked = True
                            if status == 200:
                                for alert in data.get('alerts', []):
                                    uuid = alert.get('uuid')
                                    if uuid and uuid not in all_alerts:
                                        all_alerts[uuid] = alert
                                for jam in data.get('jams', []):
                                    uuid = jam.get('uuid')
                                    if uuid and uuid not in all_jams:
                                        all_jams[uuid] = jam
                        except Exception as e:
                            Log.error(f"Waze curl_cffi region error: {e}")
                if all_alerts or all_jams:
                    Log.info(f"Waze: curl_cffi OK ({len(all_alerts)} alerts, {len(all_jams)} jams)")

            # Start browser worker if we got blocked (it will be ready for next fetch)
            if not all_alerts and not all_jams:
                if _playwright_available:
                    Log.info("Waze: Starting browser worker...")
                    _start_waze_browser_worker()
                    # Wait for browser to become ready
                    for _ in range(120):
                        if _waze_browser_ready:
                            break
                        time.sleep(0.5)
                    # Try browser now if it's ready
                    if _waze_browser_ready:
                        results = _waze_browser_fetch_regions(NSW_REGIONS)
                        if results:
                            for r in results:
                                if r and r.get('ok') and r.get('data'):
                                    data = r['data']
                                    for alert in data.get('alerts', []):
                                        uuid = alert.get('uuid')
                                        if uuid and uuid not in all_alerts:
                                            all_alerts[uuid] = alert
                                    for jam in data.get('jams', []):
                                        uuid = jam.get('uuid')
                                        if uuid and uuid not in all_jams:
                                            all_jams[uuid] = jam
                            if all_alerts or all_jams:
                                Log.info(f"Waze: Browser fetch OK ({len(all_alerts)} alerts, {len(all_jams)} jams)")
                else:
                    Log.warn("Waze: No data fetched. Install playwright for browser bypass: pip install playwright && python -m playwright install chromium")

        alerts_list = list(all_alerts.values())
        jams_list = list(all_jams.values())
        _waze_cache = {'alerts': alerts_list, 'jams': jams_list, 'timestamp': now2}
        return alerts_list, jams_list


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
    # Check persistent cache first
    cached_data, age, expired = cache_get('waze_hazards')
    if cached_data and not expired:
        return jsonify(cached_data)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live
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
        'jamCount': len(jam_features)
    }
    if features or jam_features:
        cache_set('waze_hazards', result, 120)
    return jsonify(result)


@app.route('/api/waze/police')
def waze_police():
    """Waze police reports (uses persistent cache)"""
    # Check persistent cache first
    cached_data, age, expired = cache_get('waze_police')
    if cached_data and not expired:
        return jsonify(cached_data)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live
    alerts, _ = fetch_waze_data()
    features = []
    
    for alert in alerts:
        alert_type = alert.get('type', '').upper()
        subtype = alert.get('subtype', '') or ''
        
        # Check if it's a police alert - type POLICE (including with no subtype) or police-specific subtypes
        is_police = (alert_type == 'POLICE' or 'POLICE' in subtype.upper())
        
        if is_police:
            feature = parse_waze_alert(alert, 'Police')
            if feature:
                features.append(feature)
    
    result = {'type': 'FeatureCollection', 'features': features, 'count': len(features)}
    if features:
        cache_set('waze_police', result, 120)
    return jsonify(result)


@app.route('/api/waze/roadwork')
def waze_roadwork():
    """Waze construction and road closures (uses persistent cache)"""
    cached_data, age, expired = cache_get('waze_roadwork')
    if cached_data and not expired:
        return jsonify(cached_data)
    if cached_data:
        return jsonify(cached_data)
    
    # Fallback: fetch live
    alerts, _ = fetch_waze_data()
    features = []
    
    for alert in alerts:
        alert_type = alert.get('type', '').upper()
        subtype = alert.get('subtype', '') or ''
        subtype_upper = subtype.upper()
        
        # Include CONSTRUCTION type alerts and any construction-related subtypes
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
        # Try curl_cffi first (with proxy if configured), then plain requests
        method_used = 'requests'
        proxy = _get_waze_proxy()
        proxies = {'https': proxy, 'http': proxy} if proxy else None
        if CURL_CFFI_AVAILABLE and curl_requests:
            try:
                kwargs = {'params': params, 'timeout': 15, 'impersonate': 'chrome', 'headers': headers}
                if proxy:
                    kwargs['proxy'] = proxy
                r = curl_requests.get(url, **kwargs)
                method_used = 'curl_cffi' + (f' +proxy({proxy.split("//")[-1]})' if proxy else '')
            except Exception:
                r = requests.get(url, params=params, timeout=15, headers=headers, proxies=proxies)
                method_used = 'requests' + (' +proxy' if proxy else '')
        else:
            r = requests.get(url, params=params, timeout=15, headers=headers, proxies=proxies)
            method_used = 'requests' + (' +proxy' if proxy else '')
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
            'proxy_configured': len(_waze_proxy_pool) > 0,
            'proxy_count': len(_waze_proxy_pool),
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
    """Debug endpoint to view active page sessions"""
    active_count = get_active_page_count()  # This also cleans stale sessions
    data_count = get_data_page_count()
    
    sessions_detail = []
    now = time.time()
    for page_id, session in active_page_sessions.items():
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
        return jsonify({'error': f'Status {r.status_code}'})
    except Exception as e:
        return jsonify({'error': str(e)})


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
        active_only = request.args.get('active_only') == '1'
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
        
        for db_path in dbs_to_query:
            try:
                conn = get_conn()
                try:
                    c = conn.cursor()
                    
                    # Get total count for this DB
                    c.execute(f'SELECT COUNT(*) FROM data_history WHERE {actual_where}', params)
                    db_total = c.fetchone()[0]
                    total += db_total
                    
                    # Get live/ended counts (skip if filtering already)
                    if live_only:
                        live_count += db_total
                    elif historical_only:
                        ended_count += db_total
                    else:
                        c.execute(f'''
                            SELECT 
                                SUM(CASE 
                                    WHEN source = 'pager' THEN 
                                        CASE WHEN COALESCE(source_timestamp_unix, fetched_at) >= {pager_cutoff} THEN 1 ELSE 0 END
                                    ELSE 
                                        CASE WHEN is_live = 1 THEN 1 ELSE 0 END
                                END) as live_count,
                                SUM(CASE 
                                    WHEN source = 'pager' THEN 
                                        CASE WHEN COALESCE(source_timestamp_unix, fetched_at) < {pager_cutoff} THEN 1 ELSE 0 END
                                    ELSE 
                                        CASE WHEN is_live = 0 OR is_live IS NULL THEN 1 ELSE 0 END
                                END) as ended_count
                            FROM data_history
                            WHERE {actual_where}
                        ''', params)
                        le = c.fetchone()
                        live_count += le[0] or 0
                        ended_count += le[1] or 0
                finally:
                    conn.close()
            except Exception as e:
                Log.error(f"Count error: {e}")
        
        # For single-DB queries, use OFFSET directly. For multi-DB, need to fetch more and sort in memory.
        if len(dbs_to_query) == 1:
            # Single DB - simple query with proper pagination
            conn = get_conn()
            try:
                c = conn.cursor()
                c.execute(f'''
                    SELECT id, source, source_id, fetched_at, source_timestamp, source_timestamp_unix,
                           latitude, longitude, location_text, title, category, subcategory,
                           status, severity, data, is_active, is_live, last_seen
                    FROM data_history
                    WHERE {actual_where}
                    ORDER BY fetched_at {order_dir}
                    LIMIT %s OFFSET %s
                ''', params + [limit, offset])
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
                                   status, severity, data, is_active, is_live, last_seen
                            FROM data_history
                            WHERE {actual_where}
                            ORDER BY fetched_at {order_dir}
                            LIMIT %s
                        ''', params + [offset + limit])
                        all_rows.extend(c.fetchall())
                    finally:
                        conn.close()
                except Exception as e:
                    Log.error(f"Query error: {e}")
            
            # Sort merged results and apply pagination
            reverse = (order_dir == 'DESC')
            all_rows.sort(key=lambda r: r[3] or 0, reverse=reverse)
            all_rows = all_rows[offset:offset + limit]
        
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
                'data': json.loads(row[14]) if row[14] else {},
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
        
        return jsonify({
            'total': total,
            'live_count': live_count,
            'ended_count': ended_count,
            'limit': limit,
            'offset': offset,
            'count': len(records),
            'records': records,
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
                conn.close()
            except Exception as e:
                Log.error(f"Sources error: {e}")
        
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


@app.route('/api/data/history/filters')
def data_history_filters():
    """
    Get all available filter values for the history search UI.
    
    Query parameters:
        source: Optional - filter categories/subcategories/statuses by source
                e.g., ?source=waze_police will show only subcategories that exist for waze_police
        hours: Optional - only count records within the last N hours (matches search default)
    
    Returns:
        sources: All data sources with record counts
        categories: All categories (optionally filtered by source)
        subcategories: All subcategories (optionally filtered by source)
        statuses: All statuses (optionally filtered by source)
        severities: All severities (optionally filtered by source)
        date_range: Oldest and newest record timestamps
    
    Examples:
        /api/data/history/filters
        /api/data/history/filters?source=waze_police
        /api/data/history/filters?source=rfs&hours=24
    """
    try:
        source_filter = request.args.get('source')
        hours = request.args.get('hours', type=int)
        
        # Determine which databases to query
        dbs_to_query = get_history_dbs_for_sources([source_filter] if source_filter else None)
        
        # Aggregated results
        sources_data = {}  # value -> count
        categories_data = {}
        subcategories_data = {}
        statuses_data = {}
        severities_data = {}
        min_ts = None
        max_ts = None
        
        # Build time condition if hours filter specified
        time_condition = ""
        time_params = []
        if hours:
            cutoff = int(time.time()) - (hours * 3600)
            time_condition = "fetched_at >= %s"
            time_params = [cutoff]
        
        for db_path in dbs_to_query:
            try:
                conn = get_conn()
                c = conn.cursor()
                
                # Build base conditions for filtered queries
                conditions = []
                params = list(time_params)
                
                if time_condition:
                    conditions.append(time_condition.rstrip(' AND'))
                if source_filter:
                    conditions.append("source = %s")
                    params.append(source_filter)
                
                base_where = " AND ".join(conditions) if conditions else "1=1"
                
                # Get sources (skip deprecated)
                c.execute(f'''
                    SELECT source, COUNT(*) FROM data_history 
                    WHERE {base_where}
                    GROUP BY source
                ''', params)
                for r in c.fetchall():
                    if r[0] in DEPRECATED_SOURCES:
                        continue
                    sources_data[r[0]] = sources_data.get(r[0], 0) + r[1]
                
                # Get categories
                c.execute(f'''
                    SELECT category, COUNT(*) FROM data_history 
                    WHERE {base_where} AND category IS NOT NULL AND category != ''
                    GROUP BY category
                ''', params)
                for r in c.fetchall():
                    categories_data[r[0]] = categories_data.get(r[0], 0) + r[1]
                
                # Get subcategories
                c.execute(f'''
                    SELECT subcategory, COUNT(*) FROM data_history 
                    WHERE {base_where} AND subcategory IS NOT NULL AND subcategory != ''
                    GROUP BY subcategory
                ''', params)
                for r in c.fetchall():
                    subcategories_data[r[0]] = subcategories_data.get(r[0], 0) + r[1]
                
                # Get statuses
                c.execute(f'''
                    SELECT status, COUNT(*) FROM data_history 
                    WHERE {base_where} AND status IS NOT NULL AND status != ''
                    GROUP BY status
                ''', params)
                for r in c.fetchall():
                    statuses_data[r[0]] = statuses_data.get(r[0], 0) + r[1]
                
                # Get severities
                c.execute(f'''
                    SELECT severity, COUNT(*) FROM data_history 
                    WHERE {base_where} AND severity IS NOT NULL AND severity != ''
                    GROUP BY severity
                ''', params)
                for r in c.fetchall():
                    severities_data[r[0]] = severities_data.get(r[0], 0) + r[1]
                
                # Get date range
                c.execute('SELECT MIN(fetched_at), MAX(fetched_at) FROM data_history')
                db_min, db_max = c.fetchone()
                if db_min and (min_ts is None or db_min < min_ts):
                    min_ts = db_min
                if db_max and (max_ts is None or db_max > max_ts):
                    max_ts = db_max
                
                conn.close()
            except Exception as e:
                Log.error(f"Filters error: {e}")
        
        # Convert to sorted lists
        sources = [{'value': k, 'count': v} for k, v in sorted(sources_data.items(), key=lambda x: x[1], reverse=True)]
        categories = [{'value': k, 'count': v} for k, v in sorted(categories_data.items(), key=lambda x: x[1], reverse=True)]
        subcategories = [{'value': k, 'count': v} for k, v in sorted(subcategories_data.items(), key=lambda x: x[1], reverse=True)][:100]
        statuses = [{'value': k, 'count': v} for k, v in sorted(statuses_data.items(), key=lambda x: x[1], reverse=True)]
        severities = [{'value': k, 'count': v} for k, v in sorted(severities_data.items(), key=lambda x: x[1], reverse=True)]
        
        # Build hierarchical structure for UI
        providers_data = {}
        for source_name, count in sources_data.items():
            provider, source_type = get_source_hierarchy(source_name)
            if provider not in providers_data:
                providers_data[provider] = {'count': 0, 'types': {}}
            providers_data[provider]['count'] += count
            if source_type not in providers_data[provider]['types']:
                providers_data[provider]['types'][source_type] = {'count': 0, 'source': source_name}
            providers_data[provider]['types'][source_type]['count'] += count
        
        # Convert to list format
        providers = []
        for provider_name, pdata in sorted(providers_data.items(), key=lambda x: x[1]['count'], reverse=True):
            provider_info = SOURCE_PROVIDERS.get(provider_name, {})
            types_list = [
                {
                    'name': type_name,
                    'source': tdata['source'],
                    'count': tdata['count']
                }
                for type_name, tdata in sorted(pdata['types'].items(), key=lambda x: x[1]['count'], reverse=True)
            ]
            providers.append({
                'name': provider_name,
                'icon': provider_info.get('icon', '📊'),
                'color': provider_info.get('color', '#64748b'),
                'count': pdata['count'],
                'types': types_list
            })
        
        return jsonify({
            'sources': sources,
            'providers': providers,  # NEW: Hierarchical structure
            'categories': categories,
            'subcategories': subcategories,
            'statuses': statuses,
            'severities': severities,
            'date_range': {
                'oldest': datetime.fromtimestamp(min_ts).isoformat() if min_ts else None,
                'newest': datetime.fromtimestamp(max_ts).isoformat() if max_ts else None,
                'oldest_unix': min_ts,
                'newest_unix': max_ts
            },
            'filtered_by_source': source_filter,
            'filtered_by_hours': hours
        })
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
            c.execute('''
                SELECT id, fetched_at, source_timestamp, source_timestamp_unix,
                       latitude, longitude, location_text, title, category, subcategory,
                       status, severity, data, is_active, is_live, last_seen
                FROM data_history 
                WHERE source = %s AND source_id = %s
                ORDER BY fetched_at ASC
            ''', (source, source_id))
            
            rows = c.fetchall()
        finally:
            conn.close()
        
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
            conn.close()
        
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
        
        try:
            conn = get_conn()
            c = conn.cursor()
            
            c.execute('SELECT COUNT(*) FROM data_history')
            db_rows = c.fetchone()[0]
            total_rows = db_rows
            
            c.execute('SELECT COUNT(DISTINCT source || COALESCE(source_id, \'\')) FROM data_history')
            db_unique = c.fetchone()[0]
            unique_incidents = db_unique
            
            c.execute('SELECT source, COUNT(*) FROM data_history GROUP BY source')
            for r in c.fetchall():
                by_source[r[0]] = by_source.get(r[0], 0) + r[1]
            
            c.execute('SELECT COUNT(*) FROM data_history WHERE is_live = 1')
            db_live = c.fetchone()[0]
            live_count = db_live
            
            c.execute("SELECT pg_total_relation_size('data_history')")
            db_size = c.fetchone()[0]
            
            history_db_stats['data_history'] = {
                'rows': db_rows,
                'unique_incidents': db_unique,
                'live_count': db_live,
                'size_mb': round(db_size / (1024 * 1024), 2)
            }
            
            conn.close()
        except Exception as e:
            history_db_stats['data_history'] = {'error': str(e)}
        
        stats['data_history'] = {
            'total_rows': total_rows,
            'unique_incidents': unique_incidents,
            'by_source': by_source,
            'live_count': live_count,
            'databases': history_db_stats
        }
        
        # api_data_cache stats (from cache.db)
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
            conn.close()
        except Exception as e:
            stats['api_data_cache'] = {'error': str(e)}
        
        # stats_snapshots stats (from stats.db)
        try:
            conn = get_conn()
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM stats_snapshots')
            stats['stats_snapshots'] = {'total_rows': c.fetchone()[0]}
            conn.close()
        except Exception as e:
            stats['stats_snapshots'] = {'error': str(e)}
        
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
        cur = conn.cursor()
        for table in tables:
            try:
                cur.execute("SELECT pg_total_relation_size(%s)", (table,))
                size_before = cur.fetchone()[0] / (1024 * 1024)
                cur.execute(f'VACUUM "{table}"')
                conn.commit()
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
        conn.close()
        
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
                conn.close()
                return jsonify({'error': 'Request not found'}), 404
            
            if req['status'] != 'pending':
                conn.close()
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
                conn.close()
                return jsonify({'error': 'Request not found'}), 404
            
            if req['status'] != 'pending':
                conn.close()
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
    """Check user's admin access level (owner or team_member)"""
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
        is_admin = is_owner or is_team_member

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
        
        # Permission levels:
        # - Owner: Full access (approve, deny, any role, user management)
        # - Team Member: Approve/deny requests, can only assign map_editor/pager_contributor/radio_contributor
        result = {
            'user_id': user_id,
            'is_admin': is_admin,
            'is_owner': is_owner,
            'is_team_member': is_team_member,
            'can_manage_users': is_owner,  # Only owners can edit existing users
            'can_assign_privileged_roles': is_owner,  # Only owners can assign team_member/dev/owner
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

    # Pre-start Waze browser worker so it's ready for first fetch
    if _playwright_available:
        def _delayed_waze_browser_start():
            time.sleep(10)
            if not _shutdown_event.is_set():
                _start_waze_browser_worker()
                Log.info("Waze: Browser worker ready")
        threading.Thread(target=_delayed_waze_browser_start, daemon=True, name='waze-prestart').start()

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
    # In debug mode, Flask uses a reloader that spawns a child process
    # Only initialize in the reloader child process (indicated by WERKZEUG_RUN_MAIN)
    use_debug = DEV_MODE  # Flask debug mode matches our dev mode
    
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not use_debug:
        initialize()
    
    app.run(host=args.host, port=args.port, debug=use_debug)
else:
    # When imported by WSGI server (gunicorn, etc.)
    initialize()