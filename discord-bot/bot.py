#!/usr/bin/env python3
"""
NSW PSN Alert Discord Bot
Provides real-time alerts for emergency services, BOM warnings, traffic incidents, and pager messages.
"""

import os
import asyncio
import functools
import logging
import math
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

# Load .env BEFORE importing local modules — database.py reads
# BOT_DATABASE_URL at module-import time to decide SQLite vs Postgres.
# If load_dotenv() runs after that import, the env var is None and the
# bot silently falls back to SQLite even when .env has a valid URL.
load_dotenv()

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks

from database import Database
from alert_poller import AlertPoller
from embeds import EmbedBuilder

# Configure logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('nswpsn-bot')

# Set discord.py logging to WARNING to reduce noise (unless DEBUG)
if LOG_LEVEL != 'DEBUG':
    logging.getLogger('discord').setLevel(logging.WARNING)
    logging.getLogger('discord.http').setLevel(logging.WARNING)

# Bot configuration
DISCORD_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
API_BASE_URL = os.getenv('API_BASE_URL', 'http://localhost:8000').rstrip('/')
API_KEY = os.getenv('NSWPSN_API_KEY', '')
BOT_OWNER_ID = os.getenv('BOT_OWNER_ID', '')  # Discord User ID for admin commands


def is_bot_owner(user_id: int) -> bool:
    """Check if a user is the bot owner"""
    if not BOT_OWNER_ID:
        return False
    try:
        return int(BOT_OWNER_ID) == user_id
    except ValueError:
        return False

# Alert types available (canonical, singular, provider-prefixed where it
# helps — kept in sync with the dashboard PROVIDERS list and data_history).
ALERT_TYPES = {
    'rfs': 'RFS Major Incidents',
    'bom_land': 'BOM Land Warnings',
    'bom_marine': 'BOM Marine Warnings',
    'traffic_incident': 'Traffic Incidents',
    'traffic_roadwork': 'Traffic Roadwork',
    'traffic_flood': 'Flood Hazards',
    'traffic_fire': 'Traffic Fires',
    'traffic_majorevent': 'Major Events',
    'endeavour_current': 'Endeavour Current Outages',
    'endeavour_planned': 'Endeavour Planned Outages',
    'ausgrid': 'Ausgrid Outages',
    'essential_planned': 'Essential Energy Planned Outages',
    'essential_future': 'Essential Energy Future Outages',
    'waze_hazard': 'Waze Hazards',
    'waze_jam': 'Waze Traffic Jams',
    'waze_police': 'Waze Police',
    'waze_roadwork': 'Waze Roadwork',
    'user_incident': 'User Incidents',
    'radio_summary': 'Radio Summary',
}

# Alert types shown in the generic /setup alerts dropdown + "Enable All".
# Radio summary has its own dedicated setup flow (see SetupRadioSummarySubmenuView).
_GENERAL_ALERT_TYPES = {
    k: v for k, v in ALERT_TYPES.items() if k != 'radio_summary'
}

WEBSITE_URL = "https://nswpsn.forcequit.xyz/"


# ============================================================================
# Per-preset alert filters (keywords / severity floor / geofilter).
# Evaluated in the dispatchers after resolve_preset_effective_state passes —
# preset.filters JSONB shape:
#   {
#     "keywords_include": ["fire", ...],   # any-of, case-insensitive substring
#     "keywords_exclude": ["drill", ...],  # none-of
#     "severity_min": {"rfs": "watch_and_act", "bom_land": "major", ...}
#         # Dict keyed by alert_type → token. Legacy: a single string applied
#         # generically (whichever per-type scale matches) is still accepted.
#     "geofilter": { "type": "bbox" | "ring" | "polygon", ... }
#         bbox:    {"lat_min", "lng_min", "lat_max", "lng_max"}
#         ring:    {"lat", "lng", "radius_m"}
#         polygon: {"points": [[lat, lng], ...]}   # 3+ points
#     # Legacy: a top-level "bbox" without a geofilter wrapper is still
#     # accepted on read (older presets) and treated as type=bbox.
#   }
# Each key is optional; an empty/missing filters dict means "no filter".
# ============================================================================

# Per-source severity scales, lowest-to-highest. Severity floor passes when the
# alert's severity index is >= the floor's index. Sources without a scale here
# bypass the severity filter entirely.
_SEVERITY_SCALES = {
    'rfs': ['advice', 'watch_and_act', 'emergency'],
    # BOM real-world values are severe/warning/watch/advice/info but the
    # dashboard contract uses minor/moderate/major. We accept BOTH on input
    # via _SEVERITY_BOM_MAP and normalise to the canonical scale below.
    # Both bom_land and bom_marine share the same per-type scale.
    'bom_land': ['minor', 'moderate', 'major'],
    'bom_marine': ['minor', 'moderate', 'major'],
    'traffic_majorevent': ['minor', 'moderate', 'major'],
}

# RFS alertLevel raw → snake_case. Anything not matching is treated as "no
# severity available" and the filter passes (don't block on missing data).
_SEVERITY_RFS_MAP = {
    'advice': 'advice',
    'watch and act': 'watch_and_act',
    'emergency warning': 'emergency',
    'emergency': 'emergency',
}

# BOM data severity → canonical minor/moderate/major bucket.
_SEVERITY_BOM_MAP = {
    'severe': 'major',
    'warning': 'moderate',
    'watch': 'moderate',
    'advice': 'minor',
    'info': 'minor',
    # Pass through canonical values too, so filter-on-major works either way.
    'minor': 'minor',
    'moderate': 'moderate',
    'major': 'major',
}


def _alert_text_haystack(alert_type: str, alert_data: dict) -> str:
    """Concatenate searchable text fields per alert_type for keyword matching."""
    if not isinstance(alert_data, dict):
        return ''
    bits = []

    def _push(v):
        if v is None:
            return
        if isinstance(v, (str, int, float)):
            s = str(v).strip()
            if s:
                bits.append(s)

    if alert_type == 'rfs' or alert_type == 'user_incident':
        props = alert_data.get('properties') if alert_type == 'rfs' else None
        # RFS: properties.{title,description,location,councilArea,fireType,status}
        if isinstance(props, dict):
            for k in ('title', 'description', 'location', 'councilArea',
                      'fireType', 'status', 'alertLevel'):
                _push(props.get(k))
        else:
            for k in ('title', 'description', 'location'):
                _push(alert_data.get(k))
    elif alert_type and alert_type.startswith('bom_'):
        for k in ('title', 'description', 'area', 'category', 'severity'):
            _push(alert_data.get(k))
    elif alert_type and alert_type.startswith('traffic_'):
        props = alert_data.get('properties') or {}
        for k in ('title', 'headline', 'displayName', 'subtitle', 'roads',
                  'incidentType', 'otherAdvice', 'adviceA', 'adviceB',
                  'affectedDirection'):
            _push(props.get(k))
    elif alert_type and alert_type.startswith('waze_'):
        props = alert_data.get('properties') or {}
        for k in ('title', 'displayType', 'wazeSubtype', 'street',
                  'city', 'location'):
            _push(props.get(k))
    elif alert_type and (alert_type.startswith('endeavour_')
                         or alert_type == 'ausgrid'
                         or alert_type.startswith('essential_')):
        for k in ('suburb', 'Suburb', 'streets', 'cause', 'status',
                  'outageType'):
            _push(alert_data.get(k))
    elif alert_type == 'pager':
        # alert_data here is the pager msg dict.
        for k in ('capcode', 'type', 'category', 'alias', 'agency',
                  'address', 'suburb', 'council', 'incident_id', 'raw'):
            _push(alert_data.get(k))
    elif alert_type == 'radio_summary':
        # Radio summary content is multi-incident; flatten any string values.
        for k, v in (alert_data or {}).items():
            _push(v)
    else:
        # Generic fallback — flatten all top-level scalars.
        for v in (alert_data or {}).values():
            _push(v)
    return ' '.join(bits)


def _alert_lat_lng(alert_type: str, alert_data: dict):
    """Return (lat, lng) for an alert if available, else (None, None)."""
    if not isinstance(alert_data, dict):
        return (None, None)

    # GeoJSON-style geometry.coordinates = [lng, lat] for rfs, traffic_*, waze_*.
    if alert_type == 'rfs' or (alert_type or '').startswith('traffic_') \
            or (alert_type or '').startswith('waze_'):
        geom = alert_data.get('geometry') or {}
        coords = geom.get('coordinates') if isinstance(geom, dict) else None
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            try:
                return (float(coords[1]), float(coords[0]))
            except (TypeError, ValueError):
                return (None, None)
        return (None, None)

    # User incidents store lat/lng at top level.
    if alert_type == 'user_incident':
        try:
            lat = alert_data.get('lat')
            lng = alert_data.get('lng')
            if lat is not None and lng is not None:
                return (float(lat), float(lng))
        except (TypeError, ValueError):
            pass
        return (None, None)

    # Power outages use latitude/longitude (Endeavour/Essential) or
    # Latitude/Longitude (Ausgrid).
    if ((alert_type or '').startswith('endeavour_')
            or alert_type == 'ausgrid'
            or (alert_type or '').startswith('essential_')):
        lat = alert_data.get('latitude') or alert_data.get('Latitude')
        lng = alert_data.get('longitude') or alert_data.get('Longitude')
        try:
            if lat is not None and lng is not None:
                return (float(lat), float(lng))
        except (TypeError, ValueError):
            pass
        return (None, None)

    # Pager messages nest under coordinates.{lat,lon}.
    if alert_type == 'pager':
        coords = alert_data.get('coordinates') or {}
        if isinstance(coords, dict):
            try:
                lat = coords.get('lat')
                lng = coords.get('lon') if 'lon' in coords else coords.get('lng')
                if lat is not None and lng is not None:
                    return (float(lat), float(lng))
            except (TypeError, ValueError):
                pass
        return (None, None)

    return (None, None)


def _alert_subtype_token(alert_type: str, alert_data: dict):
    """Return the alert's per-type sub-classification, or None if not applicable.

    Each source uses a different field name:
      rfs              → properties.category   ("Bush Fire", "Grass Fire", "Hazard Reduction")
      bom_*            → properties.category or top-level category   ("Severe Weather", "Wind", ...)
      traffic_*        → properties.incidentType  ("CRASH", "BREAKDOWN", "BUSHFIRE", ...)
      waze_*           → subtype                  ("HAZARD_WEATHER_FOG", "POLICE_HIDING", ...)
      user_incident    → category
    For unsupported types, returns None — subtype_filters has no effect on them.
    """
    if not isinstance(alert_data, dict):
        return None
    if alert_type == 'rfs':
        props = alert_data.get('properties') or {}
        return (str(props.get('category') or '').strip()) or None
    if alert_type and alert_type.startswith('bom_'):
        props = alert_data.get('properties') or {}
        raw = props.get('category') or alert_data.get('category')
        return (str(raw or '').strip()) or None
    if alert_type and alert_type.startswith('traffic_'):
        props = alert_data.get('properties') or {}
        return (str(props.get('incidentType') or '').strip()) or None
    if alert_type and alert_type.startswith('waze_'):
        raw = alert_data.get('subtype') or alert_data.get('type')
        return (str(raw or '').strip()) or None
    if alert_type == 'user_incident':
        raw = alert_data.get('category') or alert_data.get('type')
        return (str(raw or '').strip()) or None
    return None


def _alert_severity_token(alert_type: str, alert_data: dict):
    """Map raw alert severity to a token in _SEVERITY_SCALES[alert_type]."""
    if not isinstance(alert_data, dict):
        return None
    if alert_type == 'rfs':
        props = alert_data.get('properties') or {}
        raw = (props.get('alertLevel') or '').strip().lower()
        return _SEVERITY_RFS_MAP.get(raw)
    if alert_type and alert_type.startswith('bom_'):
        raw = str(alert_data.get('severity') or '').strip().lower()
        return _SEVERITY_BOM_MAP.get(raw)
    if alert_type == 'traffic_majorevent':
        # No native severity field on Live Traffic — inspect props.severity if
        # ever present. Returning None means the floor filter passes through.
        props = alert_data.get('properties') or {}
        raw = str(props.get('severity') or '').strip().lower()
        return raw if raw in _SEVERITY_SCALES['traffic_majorevent'] else None
    return None


def alert_passes_severity(alert_type: str, alert_data: dict, severity_min) -> bool:
    """True if the alert meets the severity floor for its type (or filter N/A).

    severity_min may be:
      - dict keyed by alert_type → token (new shape)
      - str — legacy single-token form, applied if it appears in the type's scale
      - falsy → no filter
    """
    if not severity_min:
        return True
    scale = _SEVERITY_SCALES.get(alert_type)
    if not scale:
        return True  # no scale known for this type — don't filter
    if isinstance(severity_min, dict):
        floor_token = severity_min.get(alert_type)
    else:
        floor_token = severity_min
    if not floor_token:
        return True  # no per-type floor configured for this alert_type
    floor = str(floor_token).strip().lower()
    if floor not in scale:
        return True  # unknown floor for this scale — don't filter
    actual = _alert_severity_token(alert_type, alert_data)
    if actual is None or actual not in scale:
        return True  # alert has no severity field — don't filter
    return scale.index(actual) >= scale.index(floor)


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance between two lat/lng points, in metres."""
    R = 6371008.8  # mean Earth radius
    p1 = math.radians(lat1); p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1); dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _point_in_polygon(lat: float, lng: float, points: list) -> bool:
    """Ray-casting test. `points` is a list of [lat, lng] pairs."""
    n = len(points)
    if n < 3:
        return False
    inside = False
    j = n - 1
    for i in range(n):
        try:
            yi, xi = float(points[i][0]), float(points[i][1])
            yj, xj = float(points[j][0]), float(points[j][1])
        except (TypeError, ValueError, IndexError):
            return False
        denom = (yj - yi) or 1e-12
        if ((yi > lat) != (yj > lat)) and (lng < (xj - xi) * (lat - yi) / denom + xi):
            inside = not inside
        j = i
    return inside


def _geofilter_contains(gf: dict, lat: float, lng: float) -> bool:
    """True if (lat,lng) is inside the geofilter shape. False on malformed gf."""
    try:
        t = gf.get('type')
        if t == 'bbox':
            return (float(gf['lat_min']) <= lat <= float(gf['lat_max'])
                    and float(gf['lng_min']) <= lng <= float(gf['lng_max']))
        if t == 'ring':
            return _haversine_m(lat, lng, float(gf['lat']), float(gf['lng'])) <= float(gf['radius_m'])
        if t == 'polygon':
            return _point_in_polygon(lat, lng, gf.get('points') or [])
    except (KeyError, TypeError, ValueError):
        return False
    return False


def preset_alert_matches(preset: dict, alert_type: str, alert_data: dict) -> bool:
    """Return True if the alert passes this preset's filters (or no filters set)."""
    f = preset.get('filters') if isinstance(preset, dict) else None
    if not f:
        return True
    if not isinstance(f, dict):
        return True

    inc = f.get('keywords_include') or []
    exc = f.get('keywords_exclude') or []
    if inc or exc:
        haystack = _alert_text_haystack(alert_type, alert_data).lower()
        if inc and not any(str(k).lower() in haystack for k in inc):
            return False
        if exc and any(str(k).lower() in haystack for k in exc):
            return False

    if not alert_passes_severity(alert_type, alert_data, f.get('severity_min')):
        return False

    sf = f.get('subtype_filters')
    if isinstance(sf, dict):
        allowed = sf.get(alert_type)
        if allowed:  # non-empty list → whitelist gate
            token = _alert_subtype_token(alert_type, alert_data)
            # Pass-through when the alert has no subtype field — same policy
            # as severity, so a missing field never blocks legit traffic.
            if token and token not in allowed:
                return False

    gf = f.get('geofilter')
    # Legacy: a top-level `bbox` without a geofilter wrapper is treated as
    # type=bbox so older presets keep working.
    if not gf and isinstance(f.get('bbox'), dict):
        gf = {'type': 'bbox', **f['bbox']}
    if isinstance(gf, dict):
        lat, lng = _alert_lat_lng(alert_type, alert_data)
        if lat is None or lng is None:
            return False
        if not _geofilter_contains(gf, lat, lng):
            return False

    return True


# ============================================================================
# Default-preset helpers — bridge the per-(guild, channel) "Default" preset
# UX assumption that /setup, /alerts, /pager etc. embedded in legacy code.
# ============================================================================

DEFAULT_PRESET_NAME = "Default"


def _default_preset_for(db, guild_id: int, channel_id: int):
    """Return the channel's 'Default' preset, or None if it doesn't exist."""
    try:
        return db.get_preset_by_name(guild_id, channel_id, DEFAULT_PRESET_NAME)
    except Exception as e:
        logger.warning(f"_default_preset_for guild={guild_id} channel={channel_id} failed: {e}")
        return None


def _default_preset_upsert(db, guild_id: int, channel_id: int, *,
                           add_alert_type: Optional[str] = None,
                           pager_enabled: Optional[bool] = None,
                           pager_capcodes: Optional[str] = None,
                           role_ids: Optional[List[int]] = None) -> Optional[Dict[str, Any]]:
    """Create or update the channel's 'Default' preset.

    On first create the CHECK constraint requires alert_types[] non-empty OR
    pager_enabled=True — so callers must pass at least one of `add_alert_type`
    or `pager_enabled=True`. Returns the preset dict after the change."""
    existing = _default_preset_for(db, guild_id, channel_id)
    if existing is None:
        # First create — collect initial state.
        types = [add_alert_type] if add_alert_type else []
        pe = bool(pager_enabled)
        if not types and not pe:
            raise ValueError(
                "Default preset can't be created empty — pass add_alert_type or pager_enabled=True"
            )
        preset_id = db.create_preset(
            guild_id=guild_id,
            channel_id=channel_id,
            name=DEFAULT_PRESET_NAME,
            alert_types=types,
            pager_enabled=pe,
            pager_capcodes=pager_capcodes,
            role_ids=role_ids or [],
        )
        return db.get_preset(preset_id)

    # Update path.
    new_alert_types = list(existing.get('alert_types') or [])
    if add_alert_type and add_alert_type not in new_alert_types:
        new_alert_types.append(add_alert_type)

    update_kwargs: Dict[str, Any] = {}
    if add_alert_type:
        update_kwargs['alert_types'] = new_alert_types
    if pager_enabled is not None:
        update_kwargs['pager_enabled'] = bool(pager_enabled)
    if pager_capcodes is not None:
        update_kwargs['pager_capcodes'] = pager_capcodes
    if role_ids is not None:
        update_kwargs['role_ids'] = list(role_ids)

    if update_kwargs:
        db.update_preset(existing['id'], **update_kwargs)
    return db.get_preset(existing['id'])


def _default_preset_remove_alert_type(db, guild_id: int, channel_id: int,
                                      alert_type: str) -> bool:
    """Drop alert_type from Default preset. If preset becomes empty (no
    alert_types and pager disabled) the preset itself is deleted. Returns
    True if a change was made."""
    preset = _default_preset_for(db, guild_id, channel_id)
    if not preset:
        return False
    types = list(preset.get('alert_types') or [])
    if alert_type not in types:
        return False
    types.remove(alert_type)
    if not types and not preset.get('pager_enabled'):
        db.delete_preset(preset['id'])
        return True
    db.update_preset(preset['id'], alert_types=types)
    return True


def _default_preset_clear_pager(db, guild_id: int, channel_id: int) -> bool:
    """Disable pager on Default preset. If preset becomes empty (no
    alert_types and pager disabled) the preset itself is deleted. Returns
    True if a change was made."""
    preset = _default_preset_for(db, guild_id, channel_id)
    if not preset or not preset.get('pager_enabled'):
        return False
    types = list(preset.get('alert_types') or [])
    if not types:
        db.delete_preset(preset['id'])
        return True
    db.update_preset(preset['id'], pager_enabled=False, pager_capcodes=None)
    return True


def _preset_has_alert_type(preset: Optional[Dict[str, Any]], alert_type: str) -> bool:
    if not preset:
        return False
    return alert_type in (preset.get('alert_types') or [])


def _channel_has_subscriptions(db, guild_id: int, channel_id: int) -> bool:
    """Does this channel have any presets at all (Default or otherwise)?"""
    try:
        return bool(db.list_presets_in_channel(guild_id, channel_id))
    except Exception:
        return False


class NSWPSNBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        
        super().__init__(
            command_prefix='!',
            intents=intents,
            help_command=None
        )
        
        self.db = Database()
        self.poller = AlertPoller(API_BASE_URL, API_KEY, self.db)
        self.embed_builder = EmbedBuilder()
        
        # Rate limiting for Discord API
        self.message_queue = asyncio.Queue(maxsize=500)
        self.rate_limit_delay = 0.5  # 500ms between messages
        self.max_messages_per_batch = 10  # Max messages to send per poll cycle
        
        # Track startup time - don't remove guild configs within first 60 seconds
        # This prevents race conditions during startup where guilds may briefly appear disconnected
        self._startup_time = datetime.now()
        self._min_uptime_for_guild_remove = 60  # seconds
        
        # Track permission errors to suppress repeated warnings (debounce).
        # Entries are swept by `_record_send_error` whenever the dict is
        # touched, so this can't grow unbounded across long bot uptime even
        # as channels get deleted/re-added.
        self._permission_error_channels: Dict[int, datetime] = {}
        self._permission_error_debounce_seconds = 300  # 5 minutes

    def _record_send_error(self, channel_id: int) -> bool:
        """Mark `channel_id` as having just errored. Returns True if this is
        the first error inside the debounce window (caller should log) or
        False if we should stay quiet. Also evicts entries older than
        4× the debounce window so the dict can't grow forever."""
        now = datetime.now()
        cutoff_secs = self._permission_error_debounce_seconds * 4
        # Sweep stale entries — cheap, runs at most once per send error
        # which is rare under steady state.
        stale = [
            cid for cid, ts in self._permission_error_channels.items()
            if (now - ts).total_seconds() > cutoff_secs
        ]
        for cid in stale:
            self._permission_error_channels.pop(cid, None)
        last_error = self._permission_error_channels.get(channel_id)
        should_log = (
            last_error is None
            or (now - last_error).total_seconds() > self._permission_error_debounce_seconds
        )
        self._permission_error_channels[channel_id] = now
        return should_log

    async def setup_hook(self):
        """Called when the bot is starting up"""
        logger.info("Setting up bot...")
        logger.info(f"Database path: {self.db.db_path}")
        self.db.init_db()
        
        # Start background tasks
        self.poll_alerts.start()
        self.poll_pager.start()
        self.process_message_queue.start()
        self.drain_bot_actions.start()
        
        # Sync commands globally
        synced = await self.tree.sync()
        logger.info(f"Synced {len(synced)} global commands!")
    
    async def on_ready(self):
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')
        logger.info(f'Connected to {len(self.guilds)} guilds')
        
        # Log subscription counts with guild details
        logger.info("=== Subscription Summary ===")
        all_guild_ids = set()
        # Pull every preset once and bucket by alert_type + pager. Run the
        # sync DB read in the executor so a slow Postgres connect can't
        # delay the gateway heartbeat during reconnect storms.
        loop = asyncio.get_event_loop()
        try:
            all_presets = await loop.run_in_executor(None, self.db.list_all_presets)
        except Exception as e:
            logger.warning(f"on_ready: list_all_presets failed: {e}")
            all_presets = []
        per_type_presets: Dict[str, List[Dict[str, Any]]] = {a: [] for a in ALERT_TYPES}
        pager_presets: List[Dict[str, Any]] = []
        for p in all_presets:
            for at in (p.get('alert_types') or []):
                if at in per_type_presets:
                    per_type_presets[at].append(p)
            if p.get('pager_enabled'):
                pager_presets.append(p)
        for alert_type in ALERT_TYPES:
            presets = per_type_presets.get(alert_type) or []
            if presets:
                logger.info(f"  {alert_type}: {len(presets)} presets")
                for p in presets:
                    all_guild_ids.add(p['guild_id'])
        if pager_presets:
            logger.info(f"  pager: {len(pager_presets)} presets")
            for p in pager_presets:
                all_guild_ids.add(p['guild_id'])
        
        # Show which guilds have configs vs which we're connected to
        logger.info(f"Guilds with configs in DB: {len(all_guild_ids)}")
        for gid in all_guild_ids:
            guild = self.get_guild(gid)
            if guild:
                logger.info(f"  ✅ {guild.name} ({gid})")
            else:
                logger.warning(f"  ❌ Unknown guild ({gid}) - not connected!")
        
        connected_ids = {g.id for g in self.guilds}
        missing = connected_ids - all_guild_ids
        if missing:
            logger.info(f"Guilds connected but NO configs: {missing}")
            logger.info(f"  (Use /setup in those servers to configure alerts)")
        logger.info(f"⛔ Guild removal BLOCKED for next {self._min_uptime_for_guild_remove}s (startup protection)")
        logger.info("============================")
        
        # Note: Commands are synced globally in setup_hook()
        # Guild-specific sync removed to prevent duplicate commands
        
        # Set bot status
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="NSW Emergency Alerts"
            )
        )
    
    async def on_guild_remove(self, guild: discord.Guild):
        """Called when the bot is removed from a guild - but we DON'T auto-delete configs anymore.
        
        Auto-deletion was causing config loss due to Discord's unreliable guild_remove events
        during network issues/reconnects. Configs should be manually cleaned up using /dev-cleanup.
        """
        # Log for awareness but DO NOT delete anything automatically
        logger.warning(f"⚠️ on_guild_remove triggered for: {guild.name} (ID: {guild.id})")
        logger.warning(f"  → ⛔ AUTO-DELETION DISABLED - configs preserved")
        logger.warning(f"  → Use /dev-cleanup to manually remove stale guild configs if needed")
    
    @tasks.loop(seconds=60)
    async def poll_alerts(self):
        """Poll for new alerts every 60 seconds"""
        try:
            new_alerts = await self.poller.check_alerts()

            if new_alerts:
                logger.info(f"📢 Found {len(new_alerts)} new alerts")
                for alert in new_alerts:
                    logger.debug(f"  → {alert['type']}: {alert['id']}")
                # Batched dispatch: multiple alerts destined for the same
                # channel in this poll cycle are coalesced into a single
                # LayoutView message (chunked by char budget if needed)
                # instead of one message per (alert × config).
                await self._dispatch_alerts_batched(new_alerts)
            else:
                logger.debug("No new alerts")

            # Heartbeat: emit an INFO line every 5 minutes (5 × 60s ticks) so
            # quiet stretches are visible without switching to DEBUG logging.
            tick = getattr(self, '_poll_tick', 0) + 1
            self._poll_tick = tick
            if tick % 5 == 0:
                q = self.message_queue.qsize() if hasattr(self, 'message_queue') else 0
                logger.info(
                    f"💓 Alert poll heartbeat — tick {tick}, "
                    f"queue={q}, last batch={len(new_alerts)} new"
                )

        except Exception as e:
            logger.error(f"Error polling alerts: {e}", exc_info=True)
    
    @tasks.loop(seconds=30)
    async def poll_pager(self):
        """Poll for new pager messages every 30 seconds"""
        try:
            new_messages = await self.poller.check_pager()
            
            if new_messages:
                logger.info(f"📟 Found {len(new_messages)} new pager messages")
                for msg in new_messages:
                    logger.debug(f"  → {msg.get('capcode', 'UNKNOWN')}: {msg.get('type', 'Unknown')}")
                # Batched dispatch — same batching pattern as alert polling.
                # Pager messages typically arrive one at a time so this is
                # usually a no-op, but the V2 container path still applies.
                await self._dispatch_pager_batched(new_messages)
            else:
                logger.debug("No new pager messages")
                
        except Exception as e:
            logger.error(f"Error polling pager: {e}", exc_info=True)
    
    @poll_alerts.before_loop
    async def before_poll_alerts(self):
        await self.wait_until_ready()
    
    @poll_pager.before_loop
    async def before_poll_pager(self):
        await self.wait_until_ready()
    
    @tasks.loop(seconds=1)
    async def process_message_queue(self):
        """Process queued messages with rate limiting"""
        messages_sent = 0
        
        while not self.message_queue.empty() and messages_sent < self.max_messages_per_batch:
            try:
                item = self.message_queue.get_nowait()
                channel_id = item['channel_id']
                # `embeds` is the new canonical key — list of up to 10 embeds.
                # Fall back to the legacy single-embed `embed` key so existing
                # call sites keep working without touching them.
                # Components V2 path wins if a view is queued — it replaces
                # both content embeds AND the legacy `embeds[]` list.
                view = item.get('view')
                embeds = None
                if not view:
                    embeds = item.get('embeds') or ([item['embed']] if item.get('embed') else [])
                    embeds = embeds[:10]  # Discord hard limit
                content = item.get('content')
                config_id = item.get('config_id')
                config_type = item.get('config_type', 'alert')

                try:
                    channel = self.get_channel(channel_id)
                    if not channel:
                        channel = await self.fetch_channel(channel_id)

                    if channel:
                        if view is not None:
                            # Components V2 (LayoutView) forbids the `content`
                            # field — role mentions must live inside a
                            # TextDisplay in the view instead. Discord returns
                            # 400 Invalid Form Body if content is sent.
                            message = await channel.send(view=view)
                        else:
                            message = await channel.send(content=content, embeds=embeds)
                        messages_sent += 1
                        
                        # Debug logging (alerts are already marked as seen in poller)
                        alert_type = item.get('alert_type')
                        if alert_type:
                            logger.debug(f"✅ Sent {alert_type} to #{channel.name}")
                        
                        # Save message URL for incident tracking (RFS alerts).
                        # Run the DB commit in a thread so a slow Postgres
                        # commit doesn't block the gateway heartbeat — this
                        # was the cause of "Shard ID None heartbeat blocked"
                        # warnings under queue load.
                        incident_guid = item.get('incident_guid')
                        if incident_guid and message:
                            status = item.get('incident_status')
                            message_url = message.jump_url
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(
                                None,
                                lambda: self.db.save_incident_message(
                                    incident_guid=incident_guid,
                                    channel_id=channel_id,
                                    message_url=message_url,
                                    status=status,
                                ),
                            )
                        
                        # Rate limit delay
                        if not self.message_queue.empty():
                            await asyncio.sleep(self.rate_limit_delay)
                            
                except discord.NotFound:
                    # 404 is ambiguous: the channel may genuinely be deleted,
                    # OR it's a transient permissions race / rename / cache
                    # miss. We used to auto-remove the legacy per-alert-type
                    # config row, but in the preset world `config_id` points
                    # at the WHOLE preset (potentially 10+ alert subscriptions).
                    # Auto-wiping on a single 404 silently blackholed channels,
                    # so we now just debounce-log and let the user clean up
                    # via the dashboard. On-guild-remove / on-channel-delete
                    # events still run their own cleanup paths.
                    if self._record_send_error(channel_id):
                        logger.warning(
                            f"Channel {channel_id} returned 404 (preset {config_id}) — "
                            f"channel may be deleted or inaccessible. Not auto-removing; "
                            f"delete the preset from the dashboard if permanent."
                        )
                except discord.Forbidden:
                    # Debounce permission error logging to reduce spam
                    if self._record_send_error(channel_id):
                        logger.warning(f"No permission to send to channel {channel_id}")
                except discord.HTTPException as e:
                    if e.status == 429:  # Rate limited
                        logger.warning(f"Rate limited, re-queuing message")
                        await self.message_queue.put(item)  # Re-queue
                        await asyncio.sleep(5)  # Wait 5 seconds
                        break
                    else:
                        logger.error(f"HTTP error sending message: {e}")
                        
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                logger.error(f"Error processing message queue: {e}")
        
        if messages_sent > 0:
            logger.info(f"Sent {messages_sent} messages (queue size: {self.message_queue.qsize()})")
    
    @process_message_queue.before_loop
    async def before_process_queue(self):
        await self.wait_until_ready()

    # ------------------------------------------------------------------
    # Admin action queue drain — runs every 10s, picks up rows written by
    # the dashboard (sync / test / cleanup) and executes them.
    # ------------------------------------------------------------------
    @tasks.loop(seconds=10)
    async def drain_bot_actions(self):
        loop = asyncio.get_event_loop()
        try:
            action = await loop.run_in_executor(None, self.db.claim_next_bot_action)
        except Exception as e:
            logger.warning(f"drain_bot_actions claim failed: {e}")
            return
        if not action:
            return

        action_id = int(action['id'])
        kind = action['action']
        params = action.get('params') or {}
        logger.info(f"bot-action drain: executing id={action_id} action={kind}")
        try:
            if kind == 'sync':
                result = await self._exec_action_sync()
            elif kind == 'test':
                result = await self._exec_action_test(params)
            elif kind == 'cleanup':
                result = await self._exec_action_cleanup(params)
            elif kind == 'broadcast':
                result = await self._exec_action_broadcast(params)
            else:
                raise ValueError(f"unknown action: {kind}")
            await loop.run_in_executor(
                None, lambda: self.db.complete_bot_action(action_id, result=result),
            )
            logger.info(f"bot-action drain: id={action_id} ok")
        except Exception as e:
            logger.exception(f"bot-action drain: id={action_id} failed")
            await loop.run_in_executor(
                None, lambda: self.db.complete_bot_action(action_id, error=str(e)[:500]),
            )

    @drain_bot_actions.before_loop
    async def before_drain_bot_actions(self):
        await self.wait_until_ready()

    async def _exec_action_sync(self):
        synced = await self.tree.sync()
        return {'synced_global': len(synced)}

    async def _exec_action_broadcast(self, params):
        """Send an embed to a list of channels. Uses channel.send() so
        discord.py's built-in rate-limit handling kicks in — backend used
        to do this with raw requests.post and could trip 429s under load."""
        title = (params.get('title') or '').strip()
        description = (params.get('description') or '').strip()
        color_hex = (params.get('color') or '').strip()
        footer = (params.get('footer') or '').strip()
        url = (params.get('url') or '').strip()
        targets = params.get('targets') or []
        if not (title or description):
            raise ValueError('title or description is required')
        if not isinstance(targets, list) or not targets:
            raise ValueError('targets must be a non-empty list')

        # Build the embed once, reuse for every target.
        try:
            color_int = int(color_hex.lstrip('#'), 16) if color_hex else None
        except ValueError:
            color_int = None
        embed = discord.Embed(
            title=title[:256] if title else None,
            description=description[:4000] if description else None,
            color=color_int if color_int is not None else 0xf97316,
            url=url or None,
            timestamp=datetime.now(timezone.utc),
        )
        if footer:
            embed.set_footer(text=footer[:2048])

        results = []
        sent_to = set()
        for t in targets:
            try:
                gid = int((t or {}).get('guild_id') or 0)
                cid = int((t or {}).get('channel_id') or 0)
            except (TypeError, ValueError):
                results.append({'guild_id': str((t or {}).get('guild_id', '')),
                                'channel_id': str((t or {}).get('channel_id', '')),
                                'ok': False, 'error': 'bad_ids'})
                continue
            if cid in sent_to:
                results.append({'guild_id': str(gid), 'channel_id': str(cid),
                                'ok': False, 'error': 'duplicate_channel'})
                continue
            guild = self.get_guild(gid)
            channel = guild.get_channel(cid) if guild else None
            if channel is None:
                results.append({'guild_id': str(gid), 'channel_id': str(cid),
                                'ok': False, 'error': 'channel_not_found'})
                continue
            try:
                msg = await channel.send(embed=embed)
                sent_to.add(cid)
                results.append({'guild_id': str(gid), 'channel_id': str(cid),
                                'ok': True, 'message_id': str(msg.id)})
                # Short pacing pause; discord.py handles 429s but a small
                # gap keeps us well under the per-channel write rate.
                await asyncio.sleep(0.4)
            except discord.Forbidden:
                results.append({'guild_id': str(gid), 'channel_id': str(cid),
                                'ok': False, 'error': 'forbidden'})
            except discord.NotFound:
                results.append({'guild_id': str(gid), 'channel_id': str(cid),
                                'ok': False, 'error': 'channel_gone'})
            except discord.HTTPException as e:
                results.append({'guild_id': str(gid), 'channel_id': str(cid),
                                'ok': False, 'error': f'HTTP {getattr(e, "status", "?")}: {str(e)[:120]}'})
            except Exception as e:
                results.append({'guild_id': str(gid), 'channel_id': str(cid),
                                'ok': False, 'error': str(e)[:160]})
        sent = sum(1 for r in results if r.get('ok'))
        logger.info(f"broadcast action: {sent}/{len(results)} delivered")
        return {'sent': sent, 'total': len(results), 'results': results}

    async def _exec_action_cleanup(self, params):
        loop = asyncio.get_event_loop()
        ids = [int(x) for x in (params.get('guild_ids') or []) if str(x).isdigit()]
        connected = {g.id for g in self.guilds}
        results = []
        for gid in ids:
            if gid in connected:
                results.append({'guild_id': str(gid), 'ok': False, 'reason': 'bot_still_connected'})
                continue
            def _sync_cleanup(g=gid):
                presets = self.db.list_presets_in_guild(g)
                alert_count = sum(len(p.get('alert_types') or []) for p in presets)
                pager_count = sum(1 for p in presets if p.get('pager_enabled'))
                for p in presets:
                    self.db.delete_preset(p['id'])
                self.db.clear_guild_mute(g)
                for cm in self.db.list_channel_mutes(g):
                    self.db.clear_channel_mute(g, cm['channel_id'])
                return {'presets': len(presets), 'alert_subs': alert_count, 'pager': pager_count}
            counts = await loop.run_in_executor(None, _sync_cleanup)
            results.append({'guild_id': str(gid), 'ok': True, **counts})
        return {'results': results}

    async def _exec_action_test(self, params):
        """Render and post test alerts (current live data) to the specified
        channel. Bypasses mute + filters — purely for visual verification."""
        try:
            gid = int(params.get('guild_id') or 0)
            cid = int(params.get('channel_id') or 0)
        except (TypeError, ValueError):
            raise ValueError('guild_id and channel_id must be numeric')
        atype = (params.get('alert_type') or 'all')

        guild = self.get_guild(gid)
        if not guild:
            raise ValueError(f'bot not in guild {gid}')
        channel = guild.get_channel(cid)
        if not channel:
            raise ValueError(f'channel {cid} not found in guild {gid}')

        import aiohttp
        sent = 0
        errors = []
        if atype == 'all':
            types_to_fetch = list(self.poller.endpoints.keys()) + ['pager', 'user_incident', 'radio_summary']
        else:
            types_to_fetch = [atype]

        for t in types_to_fetch:
            try:
                if t == 'radio_summary':
                    headers = {'Authorization': f'Bearer {API_KEY}', 'User-Agent': 'NSWPSNBot/1.0', 'X-Client-Type': 'discord-bot'}
                    async with aiohttp.ClientSession() as session:
                        async with session.get(f"{API_BASE_URL}/api/summaries/latest", headers=headers, timeout=20) as resp:
                            if resp.status != 200:
                                errors.append(f"{t}: backend returned {resp.status}")
                                continue
                            payload = await resp.json()
                    row = (payload or {}).get('hourly')
                    if not row:
                        errors.append(f"{t}: no summary row yet")
                        continue
                    from embeds import chunk_containers_for_message
                    containers = self.embed_builder.build_radio_summary_components(row)
                    for group in chunk_containers_for_message(containers):
                        view = discord.ui.LayoutView(timeout=None)
                        for container in group:
                            view.add_item(container)
                        await channel.send(view=view)
                        sent += 1
                        await asyncio.sleep(0.5)
                elif t == 'pager':
                    messages = await self.poller._fetch_pager_from_api()
                    for msg in messages[:3]:
                        parsed = self.poller._format_api_pager(msg)
                        if parsed:
                            embed = self.embed_builder.build_pager_embed(parsed)
                            await channel.send(embed=embed)
                            sent += 1
                            await asyncio.sleep(0.5)
                    if not messages:
                        errors.append(f"{t}: no messages available")
                elif t == 'user_incident':
                    incidents = await self.poller._fetch_user_incidents()
                    for inc in incidents[:3]:
                        alert = {'type': 'user_incident', 'data': inc}
                        embed = self.embed_builder.build_alert_embed(alert)
                        await channel.send(embed=embed)
                        sent += 1
                        await asyncio.sleep(0.5)
                    if not incidents:
                        errors.append(f"{t}: no active incidents")
                else:
                    endpoint = self.poller.endpoints.get(t)
                    if not endpoint:
                        errors.append(f"{t}: unknown alert type")
                        continue
                    data = await self.poller._fetch(endpoint)
                    if not data:
                        errors.append(f"{t}: no data")
                        continue
                    items = self.poller._extract_items(t, data)
                    for item in items[:2]:
                        alert = {'type': t, 'data': item}
                        embed = self.embed_builder.build_alert_embed(alert)
                        await channel.send(embed=embed)
                        sent += 1
                        await asyncio.sleep(0.5)
                    if not items:
                        errors.append(f"{t}: no active alerts")
            except Exception as e:
                errors.append(f"{t}: {type(e).__name__}: {str(e)[:80]}")
                logger.error(f"test-action error for {t}: {e}")

        return {'sent': sent, 'errors': errors, 'channel_id': str(cid), 'guild_id': str(gid)}

    def queue_message(self, channel_id: int, embed: discord.Embed = None,
                      content: str = None,
                      config_id: int = None, config_type: str = 'alert',
                      incident_guid: str = None, incident_status: str = None,
                      alert_type: str = None, alert_id: str = None,
                      embeds: List[discord.Embed] = None,
                      view: Optional[discord.ui.View] = None):
        """Add a message to the queue for rate-limited sending.

        Pass one of: `embed` (single), `embeds` (list of up to 10), or `view`
        (Components V2 LayoutView). Precedence: view > embeds > embed.
        """
        item = {
            'channel_id': channel_id,
            'embed': embed,
            'embeds': embeds,
            'view': view,
            'content': content,
            'config_id': config_id,
            'config_type': config_type,
            'incident_guid': incident_guid,
            'incident_status': incident_status,
            'alert_type': alert_type,
            'alert_id': alert_id
        }
        if self.message_queue.full():
            logger.warning(f"Message queue full ({self.message_queue.maxsize}), dropping oldest message")
            try:
                self.message_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        self.message_queue.put_nowait(item)
    
    # ------------------------------------------------------------
    # Alert dispatch — batched Components V2 path.
    #
    # The old flow was: for each alert, iterate every subscribing config
    # and queue one message per (alert × config). For 10 alerts across
    # 7 channels that's 70 separate queue items + 70 Discord sends.
    #
    # The new flow groups alerts by destination channel and emits ONE
    # LayoutView per channel (chunked if it exceeds the char budget).
    # 10 alerts across 7 channels → 7 messages (or 7..N with chunking
    # on very busy incidents).
    # ------------------------------------------------------------

    @staticmethod
    def _alert_incident_keys(alert_type: str, alert_data: dict):
        """Return (incident_guid, incident_status) for RFS / user_incident
        alerts, else (None, None). Centralises the two shapes so the batched
        dispatcher doesn't duplicate the logic."""
        if alert_type == 'rfs':
            props = alert_data.get('properties', {}) if isinstance(alert_data, dict) else {}
            return (
                props.get('guid') or props.get('link') or props.get('title'),
                props.get('status'),
            )
        if alert_type == 'user_incident':
            data = alert_data or {}
            return (f"user_{data.get('id', '')}", data.get('status', 'Active'))
        return (None, None)

    def _resolve_ping_role_ids(self, config: dict) -> list:
        """Return a list of role ids to ping for `config`, honouring the
        /mute gate and filtering out roles that no longer exist on the guild.
        Accepts both legacy configs (`role_ids` CSV + legacy `role_id`) and
        preset rows (`role_ids` as a list of ints). Returns [] when no ping
        should fire.
        """
        # Legacy `enabled_ping == 0` gate; new presets use Python bools.
        ping_flag = config.get('enabled_ping')
        if ping_flag is False or ping_flag == 0:
            return []
        raw = config.get('role_ids')
        if isinstance(raw, list):
            # Preset shape — already a list of ints (BIGINT[] from psycopg2).
            role_ids = [int(r) for r in raw if r is not None]
        else:
            role_ids = self.db.parse_role_ids(raw, config.get('role_id'))
        if not role_ids:
            return []
        guild = self.get_guild(config['guild_id'])
        if guild is not None:
            role_ids = [rid for rid in role_ids if guild.get_role(rid) is not None]
        return role_ids

    async def _dispatch_alerts_batched(self, new_alerts: list):
        """Fan `new_alerts` out to their configured channels, batching
        multiple alerts into a single LayoutView per channel.

        Ordering preserved: alerts keep their poll-result order within each
        channel bucket, so the first alert sent in the cycle renders first
        in the combined message.
        """
        if not new_alerts:
            return
        loop = asyncio.get_event_loop()

        # Per-dispatch mute caches so each guild / channel is read from the
        # DB at most once per call.
        guild_mute_cache: Dict[int, Dict[str, bool]] = {}
        channel_mute_cache: Dict[tuple, Dict[str, bool]] = {}

        async def _get_guild_mute(gid: int) -> Dict[str, bool]:
            if gid not in guild_mute_cache:
                guild_mute_cache[gid] = await loop.run_in_executor(
                    None, self.db.get_guild_mute, gid
                )
            return guild_mute_cache[gid]

        async def _get_channel_mute(gid: int, cid: int) -> Dict[str, bool]:
            key = (gid, cid)
            if key not in channel_mute_cache:
                channel_mute_cache[key] = await loop.run_in_executor(
                    None, self.db.get_channel_mute, gid, cid
                )
            return channel_mute_cache[key]

        # Per-channel bucket: channel_id -> {
        #   'alerts': [{alert, preset, previous_message, ...}, ...],
        #   'any_preset_id': int,
        #   'role_ids_set': set(int),
        #   'guild_id': int,
        # }
        buckets: Dict[int, Dict[str, Any]] = {}
        fires: List[tuple] = []  # (preset_id, alert_type) — for preset_fire_log

        for alert in new_alerts:
            alert_type = alert.get('type')
            alert_data = alert.get('data', {})

            # radio_summary keeps its own multi-container path — it's already
            # emitted as a single big message per channel, and Components V2
            # rendering is incident-aware in ways that don't compose with
            # other alert types in the same view.
            if alert_type == 'radio_summary':
                await self._dispatch_radio_summary(alert)
                continue

            presets = await loop.run_in_executor(
                None, self.db.get_presets_for_alert_type, alert_type
            )
            incident_guid, incident_status = self._alert_incident_keys(alert_type, alert_data)

            for preset in presets:
                guild_id = preset['guild_id']
                channel_id = preset['channel_id']
                guild_mute = await _get_guild_mute(guild_id)
                channel_mute = await _get_channel_mute(guild_id, channel_id)
                effective = Database.resolve_preset_effective_state(
                    preset, alert_type, channel_mute, guild_mute
                )
                if not effective['enabled']:
                    continue
                # Per-preset filter gate (keywords / severity floor / bbox).
                if not preset_alert_matches(preset, alert_type, alert_data):
                    continue
                fires.append((preset['id'], alert_type))

                # Previous-message lookup for incident tracking (RFS / user_incident).
                # Use get_previous_incident_message so the link points to the
                # most recent prior update of THIS incident, not the very first
                # one — when an incident has 4-5 updates the "first" message
                # has scrolled far up the channel and looks random.
                previous_message = None
                if incident_guid:
                    previous_message = await loop.run_in_executor(
                        None,
                        self.db.get_previous_incident_message,
                        incident_guid,
                        channel_id,
                    )

                bucket = buckets.setdefault(channel_id, {
                    'alerts': [],
                    'any_preset_id': preset['id'],
                    'role_ids_set': set(),
                    'guild_id': guild_id,
                })
                bucket['alerts'].append({
                    'alert': alert,
                    'preset': preset,
                    'previous_message': previous_message,
                    'incident_guid': incident_guid,
                    'incident_status': incident_status,
                })
                # Union role ids only when this preset's effective_ping is on;
                # _resolve_ping_role_ids also filters deleted roles.
                if effective['enabled_ping']:
                    ping_view = dict(preset)
                    ping_view['enabled_ping'] = True
                    for rid in self._resolve_ping_role_ids(ping_view):
                        bucket['role_ids_set'].add(rid)

        # Build + queue one LayoutView per channel (chunked if oversized).
        from embeds import chunk_containers_for_message

        for channel_id, bucket in buckets.items():
            items = bucket['alerts']
            if not items:
                continue

            containers = []
            # Track incident info so we can still call save_incident_message
            # for the first incident-bearing alert in this message — older
            # messages keep their existing jump_urls as "previous" links.
            primary_incident_guid = None
            primary_incident_status = None
            for it in items:
                container = self.embed_builder.build_alert_container(
                    it['alert'], previous_message=it['previous_message'],
                )
                containers.append(container)
                if primary_incident_guid is None and it['incident_guid']:
                    primary_incident_guid = it['incident_guid']
                    primary_incident_status = it['incident_status']

            # Build a TextDisplay for role pings once — Discord still parses
            # `<@&id>` from TextDisplay content to produce a real mention.
            role_ids = sorted(bucket['role_ids_set'])
            ping_text = ' '.join(f'<@&{rid}>' for rid in role_ids) if role_ids else None

            groups = chunk_containers_for_message(containers)
            for i, group in enumerate(groups):
                view = discord.ui.LayoutView(timeout=None)
                if i == 0 and ping_text:
                    view.add_item(discord.ui.TextDisplay(content=ping_text))
                for container in group:
                    view.add_item(container)

                # Only attach incident-tracking metadata to the first chunk.
                # The DB row records which jump_url was sent first — if we
                # wrote it for chunk N we'd link updates to that one instead
                # of the top of the batch.
                queue_kwargs = {
                    'channel_id': channel_id,
                    'view': view,
                    'config_id': bucket['any_preset_id'],
                    'config_type': 'alert',
                    'alert_type': 'batch',  # batched; logging-only
                }
                if i == 0 and primary_incident_guid:
                    queue_kwargs['incident_guid'] = primary_incident_guid
                    queue_kwargs['incident_status'] = primary_incident_status
                self.queue_message(**queue_kwargs)

        if fires:
            await loop.run_in_executor(None, self.db.log_preset_fires, fires)

    async def _dispatch_radio_summary(self, alert: dict):
        """Dispatch a radio_summary alert (already Components V2 via its own
        incident-aware builder). Kept as a separate path so radio-summary
        messages don't get interleaved with generic alerts in the same view.
        """
        loop = asyncio.get_event_loop()
        presets = await loop.run_in_executor(
            None, self.db.get_presets_for_alert_type, 'radio_summary'
        )
        containers_list = self.embed_builder.build_radio_summary_components(
            alert.get('data') or {}
        )
        from embeds import chunk_containers_for_message
        groups = chunk_containers_for_message(containers_list)

        # Per-dispatch mute caches.
        guild_mute_cache: Dict[int, Dict[str, bool]] = {}
        channel_mute_cache: Dict[tuple, Dict[str, bool]] = {}

        # Collapse multiple presets in the same channel (matches alerts_batched).
        # channel_id -> {'guild_id', 'any_preset_id', 'role_ids_set'}
        buckets: Dict[int, Dict[str, Any]] = {}
        fires: List[tuple] = []

        for preset in presets:
            guild_id = preset['guild_id']
            channel_id = preset['channel_id']
            if guild_id not in guild_mute_cache:
                guild_mute_cache[guild_id] = await loop.run_in_executor(
                    None, self.db.get_guild_mute, guild_id
                )
            key = (guild_id, channel_id)
            if key not in channel_mute_cache:
                channel_mute_cache[key] = await loop.run_in_executor(
                    None, self.db.get_channel_mute, guild_id, channel_id
                )
            effective = Database.resolve_preset_effective_state(
                preset, 'radio_summary',
                channel_mute_cache[key], guild_mute_cache[guild_id],
            )
            if not effective['enabled']:
                continue
            # Per-preset filter gate. Radio summary has no severity/geo so the
            # filter is effectively keyword-only over the summary data dict.
            if not preset_alert_matches(preset, 'radio_summary', alert.get('data') or {}):
                continue
            fires.append((preset['id'], 'radio_summary'))
            bucket = buckets.setdefault(channel_id, {
                'guild_id': guild_id,
                'any_preset_id': preset['id'],
                'role_ids_set': set(),
            })
            if effective['enabled_ping']:
                ping_view = dict(preset)
                ping_view['enabled_ping'] = True
                for rid in self._resolve_ping_role_ids(ping_view):
                    bucket['role_ids_set'].add(rid)

        for channel_id, bucket in buckets.items():
            role_ids = sorted(bucket['role_ids_set'])
            ping_text = ' '.join(f'<@&{rid}>' for rid in role_ids) if role_ids else None

            for i, group in enumerate(groups):
                view = discord.ui.LayoutView(timeout=None)
                if i == 0 and ping_text:
                    view.add_item(discord.ui.TextDisplay(content=ping_text))
                for container in group:
                    view.add_item(container)
                self.queue_message(
                    channel_id=channel_id,
                    view=view,
                    config_id=bucket['any_preset_id'],
                    config_type='alert',
                    alert_type='radio_summary',
                    alert_id=f"{alert.get('id')}#{i}" if i > 0 else alert.get('id'),
                )

        if fires:
            await loop.run_in_executor(None, self.db.log_preset_fires, fires)

    async def _dispatch_pager_batched(self, new_messages: list):
        """Batch pager messages per channel (like alerts). Pager traffic is
        typically one message per poll so batching collapses to a no-op, but
        the V2 container path still applies.
        """
        if not new_messages:
            return
        loop = asyncio.get_event_loop()
        presets = await loop.run_in_executor(None, self.db.get_presets_for_pager)

        # Per-dispatch mute caches.
        guild_mute_cache: Dict[int, Dict[str, bool]] = {}
        channel_mute_cache: Dict[tuple, Dict[str, bool]] = {}

        async def _get_guild_mute(gid: int) -> Dict[str, bool]:
            if gid not in guild_mute_cache:
                guild_mute_cache[gid] = await loop.run_in_executor(
                    None, self.db.get_guild_mute, gid
                )
            return guild_mute_cache[gid]

        async def _get_channel_mute(gid: int, cid: int) -> Dict[str, bool]:
            key = (gid, cid)
            if key not in channel_mute_cache:
                channel_mute_cache[key] = await loop.run_in_executor(
                    None, self.db.get_channel_mute, gid, cid
                )
            return channel_mute_cache[key]

        # Pre-resolve effective state + normalised capcode list per preset so
        # we only pay the cost once across the whole pager batch.
        preset_plans = []
        for preset in presets:
            guild_id = preset['guild_id']
            channel_id = preset['channel_id']
            guild_mute = await _get_guild_mute(guild_id)
            channel_mute = await _get_channel_mute(guild_id, channel_id)
            # alert_type=None — per-type overrides don't apply to pager.
            effective = Database.resolve_preset_effective_state(
                preset, None, channel_mute, guild_mute
            )
            if not effective['enabled']:
                continue
            raw_capcodes = preset.get('pager_capcodes') or ''
            capcode_list = [c.strip().upper() for c in raw_capcodes.split(',') if c.strip()]
            preset_plans.append((preset, effective, capcode_list))

        # channel_id -> {'containers': [...], 'role_ids_set': set(), 'any_preset_id': ..., 'msg_hashes': [...]}
        buckets: Dict[int, Dict[str, Any]] = {}
        fires: List[tuple] = []

        for msg in new_messages:
            capcode = str(msg.get('capcode', '')).strip().upper()

            for preset, effective, capcode_list in preset_plans:
                # Empty list = match all capcodes (preserves existing semantics).
                if capcode_list and capcode not in capcode_list:
                    continue
                # Per-preset filter gate (keyword-only for pager — geo/severity
                # are typically absent so those filters short-circuit cleanly).
                if not preset_alert_matches(preset, 'pager', msg):
                    continue

                fires.append((preset['id'], 'pager'))
                channel_id = preset['channel_id']
                bucket = buckets.setdefault(channel_id, {
                    'containers': [],
                    'role_ids_set': set(),
                    'any_preset_id': preset['id'],
                    'msg_hashes': [],
                })
                bucket['containers'].append(
                    self.embed_builder.build_pager_container(msg)
                )
                bucket['msg_hashes'].append(msg.get('_msg_hash'))
                if effective['enabled_ping']:
                    ping_view = dict(preset)
                    ping_view['enabled_ping'] = True
                    for rid in self._resolve_ping_role_ids(ping_view):
                        bucket['role_ids_set'].add(rid)

        from embeds import chunk_containers_for_message

        for channel_id, bucket in buckets.items():
            if not bucket['containers']:
                continue
            role_ids = sorted(bucket['role_ids_set'])
            ping_text = ' '.join(f'<@&{rid}>' for rid in role_ids) if role_ids else None
            groups = chunk_containers_for_message(bucket['containers'])
            for i, group in enumerate(groups):
                view = discord.ui.LayoutView(timeout=None)
                if i == 0 and ping_text:
                    view.add_item(discord.ui.TextDisplay(content=ping_text))
                for container in group:
                    view.add_item(container)
                # alert_id for the queue entry is the first msg_hash in the
                # chunk — used only for logging (alerts are already marked
                # seen in the poller before we get here).
                first_hash = bucket['msg_hashes'][0] if bucket['msg_hashes'] else None
                self.queue_message(
                    channel_id=channel_id,
                    view=view,
                    config_id=bucket['any_preset_id'],
                    config_type='pager',
                    alert_type='pager',
                    alert_id=first_hash,
                )

        if fires:
            await loop.run_in_executor(None, self.db.log_preset_fires, fires)

    # ------------------------------------------------------------
    # Backward-compat wrappers. Internal call sites in poll_alerts /
    # poll_pager now use `_dispatch_alerts_batched` /
    # `_dispatch_pager_batched` directly, but these single-item
    # entry points are kept so /dev-test and any external callers keep
    # working without a caller-side change.
    # ------------------------------------------------------------

    async def send_alert(self, alert: dict):
        """Dispatch a single alert via the batched path (one-item batch)."""
        await self._dispatch_alerts_batched([alert])

    async def send_pager_message(self, msg: dict):
        """Dispatch a single pager message via the batched path."""
        await self._dispatch_pager_batched([msg])


# Create bot instance
bot = NSWPSNBot()


# ==================== SLASH COMMANDS ====================

@bot.tree.command(name="alert", description="Set up alerts for a channel")
@app_commands.describe(
    channel="The channel to send alerts to",
    alert_type="The type of alert to receive (leave empty for ALL alerts)",
    role="Optional role to ping when alerts are sent"
)
@app_commands.choices(alert_type=[
    app_commands.Choice(name=name, value=key) 
    for key, name in ALERT_TYPES.items()
])
@app_commands.default_permissions(manage_channels=True)
async def alert_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    alert_type: str = None,
    role: discord.Role = None
):
    """Set up an alert subscription for a channel"""
    try:
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id
        role_id = role.id if role else None
        loop = asyncio.get_event_loop()

        # If no alert type specified, subscribe to ALL
        if alert_type is None:
            def _sync_all():
                # Add every alert_type to the channel's Default preset, and
                # apply role_id (if provided) preset-wide.
                roles_arg = [role_id] if role_id else None
                for atype in ALERT_TYPES.keys():
                    _default_preset_upsert(
                        bot.db, guild_id, channel.id,
                        add_alert_type=atype,
                        role_ids=roles_arg,
                    )

            await loop.run_in_executor(None, _sync_all)

            embed = discord.Embed(
                title="✅ All Alerts Configured",
                description=f"Now sending **ALL alert types** to {channel.mention}",
                color=0x00ff00
            )
            embed.add_field(
                name="Alert Types",
                value=f"{len(ALERT_TYPES)} types configured",
                inline=True
            )
            if role:
                embed.add_field(name="Ping Role", value=role.mention, inline=True)

            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Single alert type
        def _sync_single():
            preset = _default_preset_for(bot.db, guild_id, channel.id)
            already_subscribed = _preset_has_alert_type(preset, alert_type)
            roles_arg = [role_id] if role_id else None
            _default_preset_upsert(
                bot.db, guild_id, channel.id,
                add_alert_type=alert_type,
                role_ids=roles_arg,
            )
            return already_subscribed

        was_existing = await loop.run_in_executor(None, _sync_single)

        if was_existing:
            embed = discord.Embed(
                title="✅ Alert Updated",
                description=f"Updated **{ALERT_TYPES[alert_type]}** alerts for {channel.mention}",
                color=0x00ff00
            )
            if role:
                embed.add_field(name="Ping Role", value=role.mention, inline=True)
            else:
                embed.add_field(name="Ping Role", value="None", inline=True)
        else:
            embed = discord.Embed(
                title="✅ Alert Configured",
                description=f"Now sending **{ALERT_TYPES[alert_type]}** alerts to {channel.mention}",
                color=0x00ff00
            )
            if role:
                embed.add_field(name="Ping Role", value=role.mention, inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        logger.error(f"Error in alert command: {e}")
        try:
            if interaction.response.is_done():
                await interaction.followup.send(
                    "❌ An error occurred while setting up the alert.",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "❌ An error occurred while setting up the alert.",
                    ephemeral=True
                )
        except Exception:
            pass


# ---- Autocomplete helpers for alert_type on scoped commands ----
#
# These replace static @app_commands.choices(...) lists on /alert-remove,
# /mute, /smute, /unmute so users only see alert types actually configured
# in the relevant scope. Discord fires autocomplete on every keystroke, so
# each call issues a single DB read (list_presets_in_guild) and filters in
# memory — never per-type lookups.


def _format_alert_type_choice(key: str) -> app_commands.Choice:
    """Map an alert_type key (or 'pager') to a Choice with its display label."""
    if key == 'pager':
        label = 'Pager Messages'
    else:
        label = ALERT_TYPES.get(key, key)
    return app_commands.Choice(name=label, value=key)


def _filter_and_sort_alert_types(
    configured_types: set,
    current: str,
) -> List[app_commands.Choice]:
    """Filter configured_types by `current` substring (case-insensitive,
    matching against key or human label) and return up to 25 sorted choices.
    """
    cur_lower = (current or '').lower()
    # Sort 'pager' last for stable UX, otherwise alphabetically by key.
    keys_sorted = sorted(
        configured_types,
        key=lambda k: (1 if k == 'pager' else 0, k),
    )
    choices: List[app_commands.Choice] = []
    for key in keys_sorted:
        label = 'Pager Messages' if key == 'pager' else ALERT_TYPES.get(key, key)
        if cur_lower and cur_lower not in key.lower() and cur_lower not in label.lower():
            continue
        choices.append(app_commands.Choice(name=label, value=key))
        if len(choices) >= 25:
            break
    return choices


async def alert_type_autocomplete_channel(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for /alert-remove — scoped to the channel already chosen
    in this interaction's namespace. Returns alert types configured in that
    specific channel only (plus 'pager' if a pager_config exists there).
    If no channel has been picked yet, fall back to guild scope so the user
    still sees something useful.
    """
    guild_id = interaction.guild_id
    if guild_id is None:
        return []

    namespace = interaction.namespace
    channel = getattr(namespace, 'channel', None)
    channel_id = channel.id if channel is not None else None

    loop = asyncio.get_event_loop()
    try:
        presets = await loop.run_in_executor(
            None, bot.db.list_presets_in_guild, guild_id,
        )
    except Exception as e:
        logger.warning(f"alert_type_autocomplete_channel DB read failed: {e}")
        return []

    if channel_id is not None:
        presets = [p for p in presets if p.get('channel_id') == channel_id]

    configured_types = set()
    for p in presets:
        for at in (p.get('alert_types') or []):
            configured_types.add(at)
    # Note: /alert-remove doesn't manage pager subscriptions (that's /pager-remove),
    # so we intentionally exclude 'pager' here.
    return _filter_and_sort_alert_types(configured_types, current)


async def alert_type_autocomplete_mute(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for /mute /smute /unmute. Scope follows the optional
    `channel` param: if the user has already picked a channel, only show
    types configured in that channel; otherwise show types configured
    anywhere in the guild. Includes 'pager' when a matching pager_config
    exists, since these commands accept pager as a scope value.
    """
    guild_id = interaction.guild_id
    if guild_id is None:
        return []

    namespace = interaction.namespace
    channel = getattr(namespace, 'channel', None)
    channel_id = channel.id if channel is not None else None

    loop = asyncio.get_event_loop()
    try:
        presets = await loop.run_in_executor(
            None, bot.db.list_presets_in_guild, guild_id,
        )
    except Exception as e:
        logger.warning(f"alert_type_autocomplete_mute DB read failed: {e}")
        return []

    if channel_id is not None:
        presets = [p for p in presets if p.get('channel_id') == channel_id]

    configured_types = set()
    has_pager = False
    for p in presets:
        for at in (p.get('alert_types') or []):
            configured_types.add(at)
        if p.get('pager_enabled'):
            has_pager = True
    if has_pager:
        configured_types.add('pager')

    return _filter_and_sort_alert_types(configured_types, current)


@bot.tree.command(name="alert-remove", description="Remove alert subscriptions from a channel")
@app_commands.describe(
    channel="The channel to remove alerts from",
    alert_type="The type of alert to remove (leave empty to remove ALL)"
)
@app_commands.autocomplete(alert_type=alert_type_autocomplete_channel)
@app_commands.default_permissions(manage_channels=True)
async def alert_remove_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    alert_type: str = None
):
    """Remove an alert subscription"""
    try:
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id
        loop = asyncio.get_event_loop()

        # If no alert type specified, remove ALL
        if alert_type is None:
            def _sync_remove_all():
                preset = _default_preset_for(bot.db, guild_id, channel.id)
                if not preset:
                    return 0
                removed = len(preset.get('alert_types') or [])
                if not preset.get('pager_enabled'):
                    bot.db.delete_preset(preset['id'])
                else:
                    bot.db.update_preset(preset['id'], alert_types=[])
                return removed

            removed = await loop.run_in_executor(None, _sync_remove_all)

            if removed > 0:
                embed = discord.Embed(
                    title="✅ All Alerts Removed",
                    description=f"Removed **{removed} alert types** from {channel.mention}",
                    color=0xff6600
                )
            else:
                embed = discord.Embed(
                    title="❌ Not Found",
                    description=f"No alerts configured for {channel.mention}",
                    color=0xff0000
                )

            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Single alert type
        def _sync_remove_one():
            return _default_preset_remove_alert_type(
                bot.db, guild_id, channel.id, alert_type,
            )

        removed = await loop.run_in_executor(None, _sync_remove_one)

        if removed:
            embed = discord.Embed(
                title="✅ Alert Removed",
                description=f"Removed **{ALERT_TYPES[alert_type]}** alerts from {channel.mention}",
                color=0xff6600
            )
        else:
            embed = discord.Embed(
                title="❌ Not Found",
                description=f"No **{ALERT_TYPES[alert_type]}** alert configured for {channel.mention}",
                color=0xff0000
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        logger.error(f"Error in alert-remove command: {e}")
        try:
            if interaction.response.is_done():
                await interaction.followup.send(
                    "❌ An error occurred while removing the alert.",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "❌ An error occurred while removing the alert.",
                    ephemeral=True
                )
        except Exception:
            pass


@bot.tree.command(name="alert-list", description="List all alert subscriptions for this server")
@app_commands.default_permissions(manage_channels=True)
async def alert_list_command(interaction: discord.Interaction):
    """List all alert subscriptions for the server.

    Components V2 path — one Container per channel, rendered via a LayoutView
    and split across messages when the char budget overflows. Stays ephemeral
    because server configuration is sensitive-ish; the user invoked the
    command so the message is for them.
    """
    try:
        await interaction.response.defer(ephemeral=True, thinking=True)

        guild_id = interaction.guild_id
        loop = asyncio.get_event_loop()

        # DB reads are still sync; offload to the default executor so we don't
        # stall the event loop on a slow Postgres commit.
        presets = await loop.run_in_executor(
            None, bot.db.list_presets_in_guild, guild_id,
        )

        if not presets:
            await interaction.followup.send(
                "📋 **No alerts configured** — use `/alert` to set up alerts "
                "or `/pager` for pager messages.",
                ephemeral=True,
            )
            return

        containers = bot.embed_builder.build_alert_list_components(
            presets, interaction.guild,
        )

        if not containers:
            # build_alert_list_components dropped everything (e.g. rows with
            # NULL channel_id). Treat as "no configs" so the user sees a
            # coherent message instead of an empty view.
            await interaction.followup.send(
                "📋 **No alerts configured** — use `/alert` to set up alerts "
                "or `/pager` for pager messages.",
                ephemeral=True,
            )
            return

        from embeds import chunk_containers_for_message
        groups = chunk_containers_for_message(containers)
        for group in groups:
            view = discord.ui.LayoutView(timeout=None)
            for container in group:
                view.add_item(container)
            await interaction.followup.send(view=view, ephemeral=True)

    except Exception as e:
        logger.error(f"Error in alert-list command: {e}", exc_info=True)
        msg = "❌ An error occurred while listing alerts."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="pager", description="Set up pager message alerts for a channel")
@app_commands.describe(
    channel="The channel to send pager messages to",
    capcodes="Comma-separated list of capcodes to filter (leave empty for all messages)",
    role="Optional role to ping when messages are received"
)
@app_commands.default_permissions(manage_channels=True)
async def pager_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    capcodes: str = None,
    role: discord.Role = None
):
    """Set up pager message alerts for a channel"""
    try:
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id
        role_id = role.id if role else None

        # Normalize capcodes
        capcode_list = None
        if capcodes:
            capcode_list = ','.join([c.strip().upper() for c in capcodes.split(',') if c.strip()])

        def _sync_work():
            preset = _default_preset_for(bot.db, guild_id, channel.id)
            already_pager = bool(preset and preset.get('pager_enabled'))
            roles_arg = [role_id] if role_id else None
            _default_preset_upsert(
                bot.db, guild_id, channel.id,
                pager_enabled=True,
                pager_capcodes=capcode_list,
                role_ids=roles_arg,
            )
            return already_pager

        loop = asyncio.get_event_loop()
        was_existing = await loop.run_in_executor(None, _sync_work)

        if was_existing:
            embed = discord.Embed(
                title="✅ Pager Config Updated",
                description=f"Updated pager alerts for {channel.mention}",
                color=0x00ff00
            )
        else:
            embed = discord.Embed(
                title="✅ Pager Configured",
                description=f"Now sending pager messages to {channel.mention}",
                color=0x00ff00
            )

        if capcode_list:
            codes = capcode_list.split(',')
            embed.add_field(
                name="Capcodes",
                value=f"`{', '.join(codes[:10])}`" + (f" and {len(codes)-10} more" if len(codes) > 10 else ""),
                inline=True
            )
        else:
            embed.add_field(name="Filter", value="All messages", inline=True)

        if role:
            embed.add_field(name="Ping Role", value=role.mention, inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        logger.error(f"Error in pager command: {e}")
        try:
            if interaction.response.is_done():
                await interaction.followup.send(
                    "❌ An error occurred while setting up pager alerts.",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "❌ An error occurred while setting up pager alerts.",
                    ephemeral=True
                )
        except Exception:
            pass


@bot.tree.command(name="pager-remove", description="Remove pager message alerts from a channel")
@app_commands.describe(
    channel="The channel to remove pager alerts from"
)
@app_commands.default_permissions(manage_channels=True)
async def pager_remove_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel
):
    """Remove pager message alerts"""
    try:
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id

        def _sync_work():
            return _default_preset_clear_pager(bot.db, guild_id, channel.id)

        loop = asyncio.get_event_loop()
        removed = await loop.run_in_executor(None, _sync_work)

        if removed:
            embed = discord.Embed(
                title="✅ Pager Removed",
                description=f"Removed pager alerts from {channel.mention}",
                color=0xff6600
            )
        else:
            embed = discord.Embed(
                title="❌ Not Found",
                description=f"No pager alerts configured for {channel.mention}",
                color=0xff0000
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        logger.error(f"Error in pager-remove command: {e}")
        try:
            if interaction.response.is_done():
                await interaction.followup.send(
                    "❌ An error occurred while removing pager alerts.",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "❌ An error occurred while removing pager alerts.",
                    ephemeral=True
                )
        except Exception:
            pass


# Note: /mute /smute /unmute previously used a static `_MUTE_SCOPE_CHOICES`
# list; they now use `alert_type_autocomplete_mute` (defined near the top of
# the command section) to surface only types actually configured in the
# guild (or in the optionally-selected channel).


async def _apply_mute_toggle(
    interaction: discord.Interaction,
    channel: Optional[discord.TextChannel],
    alert_type: Optional[str],
    *,
    enabled: Optional[bool],
    enabled_ping: Optional[bool],
    action_label: str,
    action_emoji: str,
    action_color: int,
):
    """Shared implementation for /mute /smute /unmute.

    `enabled` and `enabled_ping` are tri-state: True/False sets the flag, None
    leaves it untouched. Scope is (guild, optional channel, optional alert_type)
    and maps to the preset/channel/guild mute-state hierarchy:
      * guild only          → guild_mute_state
      * guild + channel     → channel_mute_state
      * guild + channel + alert_type → Default preset's per-type override
      * guild + alert_type (no channel) → set per-type override on every
        Default preset that subscribes to that type in the guild
      * alert_type == 'pager' is treated as the pager subscription scope.
    Unmute (enabled=True / enabled_ping=True) clears the corresponding row /
    override.
    """
    await interaction.response.defer(ephemeral=True)

    guild_id = interaction.guild_id
    channel_id = channel.id if channel else None
    loop = asyncio.get_event_loop()
    is_unmute = enabled is True or enabled_ping is True

    def _apply_sync():
        affected = 0

        # ------ Guild-wide scope (no channel, no alert_type) ------
        if channel_id is None and not alert_type:
            if is_unmute:
                bot.db.clear_guild_mute(guild_id)
                affected = 1
            else:
                bot.db.set_guild_mute(
                    guild_id,
                    enabled=enabled, enabled_ping=enabled_ping,
                )
                affected = 1
            return affected

        # ------ Channel-wide scope (channel given, no alert_type) ------
        if channel_id is not None and not alert_type:
            if is_unmute:
                bot.db.clear_channel_mute(guild_id, channel_id)
                affected = 1 if _channel_has_subscriptions(bot.db, guild_id, channel_id) else 0
                return affected
            # Mute/silence whole channel.
            if not _channel_has_subscriptions(bot.db, guild_id, channel_id):
                return 0
            bot.db.set_channel_mute(
                guild_id, channel_id,
                enabled=enabled, enabled_ping=enabled_ping,
            )
            return 1

        # ------ Per-alert-type scope ------
        # alert_type can be a real type or 'pager'. We apply per-type overrides
        # on Default preset(s) — either in the picked channel or every Default
        # in the guild that subscribes.
        atype_key = alert_type  # 'pager' is also a valid override key here
        targets: List[Dict[str, Any]] = []
        if channel_id is not None:
            preset = _default_preset_for(bot.db, guild_id, channel_id)
            if preset:
                # Only target presets that actually carry this alert_type
                # (or pager_enabled when alert_type == 'pager').
                if atype_key == 'pager':
                    if preset.get('pager_enabled'):
                        targets.append(preset)
                elif atype_key in (preset.get('alert_types') or []):
                    targets.append(preset)
        else:
            for preset in bot.db.list_presets_in_guild(guild_id):
                if preset.get('name') != DEFAULT_PRESET_NAME:
                    continue
                if atype_key == 'pager':
                    if preset.get('pager_enabled'):
                        targets.append(preset)
                elif atype_key in (preset.get('alert_types') or []):
                    targets.append(preset)

        for preset in targets:
            if is_unmute:
                bot.db.clear_preset_type_override(preset['id'], atype_key)
            else:
                bot.db.set_preset_type_override(
                    preset['id'], atype_key,
                    enabled=enabled, enabled_ping=enabled_ping,
                )
            affected += 1

        return affected

    try:
        affected = await loop.run_in_executor(None, _apply_sync)
    except Exception as e:
        logger.error(f"Error applying mute toggle: {e}", exc_info=True)
        await interaction.followup.send(
            "❌ An error occurred while updating subscriptions.",
            ephemeral=True,
        )
        return

    total = affected
    if total == 0:
        await interaction.followup.send(
            "❌ No matching subscriptions found for that scope.",
            ephemeral=True,
        )
        return

    scope_bits = []
    if channel is not None:
        scope_bits.append(f"channel {channel.mention}")
    if alert_type == 'pager':
        scope_bits.append("pager only")
    elif alert_type:
        scope_bits.append(ALERT_TYPES.get(alert_type, alert_type))
    scope = ' / '.join(scope_bits) if scope_bits else "whole server"

    embed = discord.Embed(
        title=f"{action_emoji} {action_label}",
        description=f"Updated **{total}** target(s) — scope: {scope}",
        color=action_color,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="mute", description="Stop role pings but keep receiving alert embeds")
@app_commands.describe(
    channel="Optional: limit to this channel (default: whole server)",
    alert_type="Optional: limit to this alert type (default: all including pager)",
)
@app_commands.autocomplete(alert_type=alert_type_autocomplete_mute)
@app_commands.default_permissions(manage_channels=True)
async def mute_command(
    interaction: discord.Interaction,
    channel: Optional[discord.TextChannel] = None,
    alert_type: Optional[str] = None,
):
    await _apply_mute_toggle(
        interaction, channel, alert_type,
        enabled=None, enabled_ping=False,
        action_label="Pings Muted",
        action_emoji="🔕",
        action_color=0xffa500,
    )


@bot.tree.command(name="smute", description="Silence alerts entirely (no embed, no ping) without removing subscription")
@app_commands.describe(
    channel="Optional: limit to this channel (default: whole server)",
    alert_type="Optional: limit to this alert type (default: all including pager)",
)
@app_commands.autocomplete(alert_type=alert_type_autocomplete_mute)
@app_commands.default_permissions(manage_channels=True)
async def smute_command(
    interaction: discord.Interaction,
    channel: Optional[discord.TextChannel] = None,
    alert_type: Optional[str] = None,
):
    await _apply_mute_toggle(
        interaction, channel, alert_type,
        enabled=False, enabled_ping=None,
        action_label="Alerts Silenced",
        action_emoji="🔇",
        action_color=0x808080,
    )


@bot.tree.command(name="unmute", description="Re-enable previously muted/silenced subscriptions")
@app_commands.describe(
    channel="Optional: limit to this channel (default: whole server)",
    alert_type="Optional: limit to this alert type (default: all including pager)",
)
@app_commands.autocomplete(alert_type=alert_type_autocomplete_mute)
@app_commands.default_permissions(manage_channels=True)
async def unmute_command(
    interaction: discord.Interaction,
    channel: Optional[discord.TextChannel] = None,
    alert_type: Optional[str] = None,
):
    await _apply_mute_toggle(
        interaction, channel, alert_type,
        enabled=True, enabled_ping=True,
        action_label="Alerts Re-enabled",
        action_emoji="🔔",
        action_color=0x00cc66,
    )


@bot.tree.command(name="status", description="Check the bot's status and connection")
async def status_command(interaction: discord.Interaction):
    """Show bot status"""
    embed = discord.Embed(
        title="🤖 NSW PSN Alert Bot Status",
        color=0x3498db
    )
    
    embed.add_field(
        name="Status",
        value="🟢 Online",
        inline=True
    )
    embed.add_field(
        name="Latency",
        value=f"{round(bot.latency * 1000)}ms",
        inline=True
    )
    embed.add_field(
        name="Servers",
        value=str(len(bot.guilds)),
        inline=True
    )
    
    # Get stats — sum subscription counts over all presets.
    # Offload the sync DB read to the executor so a slow commit can't block
    # the gateway heartbeat.
    loop = asyncio.get_event_loop()
    try:
        all_presets = await loop.run_in_executor(None, bot.db.list_all_presets)
    except Exception as e:
        logger.warning(f"/status: list_all_presets failed: {e}")
        all_presets = []
    total_alerts = sum(len(p.get('alert_types') or []) for p in all_presets)
    total_pager = sum(1 for p in all_presets if p.get('pager_enabled'))

    embed.add_field(
        name="Alert Subscriptions",
        value=str(total_alerts),
        inline=True
    )
    embed.add_field(
        name="Pager Subscriptions",
        value=str(total_pager),
        inline=True
    )
    
    embed.add_field(
        name="🌐 Website",
        value=f"[nswpsn.forcequit.xyz]({WEBSITE_URL})",
        inline=False
    )
    
    embed.set_footer(text=f"NSW PSN • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    await interaction.response.send_message(embed=embed)


def _remove_all_alert_configs_for_channel(guild_id: int, channel_id: int) -> int:
    """Remove all alert subscriptions for a given guild+channel.

    Wipes alert_types[] on the Default preset (preserving pager if set), and
    deletes the preset entirely if pager wasn't set."""
    preset = _default_preset_for(bot.db, guild_id, channel_id)
    if not preset:
        return 0
    removed = len(preset.get('alert_types') or [])
    if not preset.get('pager_enabled'):
        bot.db.delete_preset(preset['id'])
    else:
        bot.db.update_preset(preset['id'], alert_types=[])
    return removed


class PagerCapcodesModal(discord.ui.Modal, title="Pager Filter (Optional)"):
    capcodes = discord.ui.TextInput(
        label="Capcodes (comma-separated, blank = all)",
        required=False,
        max_length=500,
        placeholder="e.g. 1160008,1160056,1440136"
    )

    def __init__(self, on_submit_cb):
        super().__init__()
        self._on_submit_cb = on_submit_cb

    async def on_submit(self, interaction: discord.Interaction):
        raw = (self.capcodes.value or "").strip()
        # Normalize: remove spaces around commas
        normalized = ",".join([p.strip() for p in raw.split(",") if p.strip()]) or None
        await self._on_submit_cb(interaction, normalized)


def _get_alert_configs_for_channel(guild_id: int, channel_id: int) -> List[Dict[str, Any]]:
    """Return a list of legacy-shaped 'cfg' rows synthesised from the channel's
    presets — one row per (preset, alert_type). Each row has an `_preset_id`
    and `_preset_name` plus the legacy fields (alert_type, role_ids, enabled,
    enabled_ping) so the existing display + edit helpers keep working."""
    rows: List[Dict[str, Any]] = []
    try:
        presets = bot.db.list_presets_in_channel(guild_id, channel_id)
    except Exception:
        return rows
    # Resolve channel + guild mute state once per call.
    try:
        channel_mute = bot.db.get_channel_mute(guild_id, channel_id)
        guild_mute = bot.db.get_guild_mute(guild_id)
    except Exception:
        channel_mute = None
        guild_mute = None
    for preset in presets:
        for atype in (preset.get('alert_types') or []):
            eff = bot.db.resolve_preset_effective_state(
                preset, atype, channel_mute, guild_mute,
            )
            rows.append({
                '_preset_id': preset['id'],
                '_preset_name': preset.get('name'),
                'guild_id': guild_id,
                'channel_id': channel_id,
                'alert_type': atype,
                'role_ids': preset.get('role_ids') or [],
                'enabled': 1 if eff['enabled'] else 0,
                'enabled_ping': 1 if eff['enabled_ping'] else 0,
            })
    return rows


def _format_role_mentions(cfg: Dict[str, Any]) -> str:
    """Render config/preset's role list as mentions. Empty string if none.
    Accepts both list-shaped (preset) and CSV-shaped (legacy) role data."""
    raw = cfg.get('role_ids')
    if isinstance(raw, list):
        role_ids = [int(r) for r in raw if r is not None]
    else:
        role_ids = bot.db.parse_role_ids(raw, cfg.get('role_id'))
    return ' '.join(f'<@&{r}>' for r in role_ids) if role_ids else ''


def _format_mute_state(cfg: Dict[str, Any]) -> str:
    """Annotate a config row with mute/silence markers."""
    enabled = cfg.get('enabled')
    enabled_ping = cfg.get('enabled_ping')
    # Accept both legacy ints (0/1) and modern bools.
    if enabled == 0 or enabled is False:
        return ' 🔇'
    if enabled_ping == 0 or enabled_ping is False:
        return ' 🔕'
    return ''


def _status_icon(cfg: Dict[str, Any]) -> str:
    """Return a status glyph for a config row.

    🟢 active (enabled + pings)
    🔕 muted (enabled, no ping)
    🔇 silenced (disabled entirely)
    """
    enabled = cfg.get('enabled')
    enabled_ping = cfg.get('enabled_ping')
    if enabled == 0 or enabled is False:
        return '🔇'
    if enabled_ping == 0 or enabled_ping is False:
        return '🔕'
    return '🟢'


def _format_alert_configs_for_channel(guild_id: int, channel_id: int) -> str:
    cfgs = _get_alert_configs_for_channel(guild_id, channel_id)
    if not cfgs:
        return "**Off**"

    parts: List[str] = []
    for cfg in sorted(cfgs, key=lambda c: c.get("alert_type", "")):
        atype = cfg.get("alert_type", "")
        name = ALERT_TYPES.get(atype, atype)
        roles = _format_role_mentions(cfg)
        role_txt = f" · {roles}" if roles else ""
        icon = _status_icon(cfg)
        parts.append(f"{icon} `{atype}` {name}{role_txt}")
    return "\n".join(parts)[:1000]


def _format_pager_config_for_channel(guild_id: int, channel_id: int) -> str:
    preset = _default_preset_for(bot.db, guild_id, channel_id)
    if not preset or not preset.get('pager_enabled'):
        return "**Off**"

    # Resolve effective mute state for the pager-row equivalent.
    try:
        channel_mute = bot.db.get_channel_mute(guild_id, channel_id)
        guild_mute = bot.db.get_guild_mute(guild_id)
    except Exception:
        channel_mute = None
        guild_mute = None
    eff = bot.db.resolve_preset_effective_state(preset, 'pager', channel_mute, guild_mute)
    cfg_view = {
        'role_ids': preset.get('role_ids') or [],
        'enabled': 1 if eff['enabled'] else 0,
        'enabled_ping': 1 if eff['enabled_ping'] else 0,
    }

    capcodes = preset.get('pager_capcodes')
    if isinstance(capcodes, list):
        capcodes_txt = ", ".join(capcodes)
    elif isinstance(capcodes, str) and capcodes.strip():
        capcodes_txt = capcodes.strip()
    else:
        capcodes_txt = "All capcodes"

    roles = _format_role_mentions(cfg_view)
    role_txt = f" · {roles}" if roles else ""
    icon = _status_icon(cfg_view)
    return f"{icon} {capcodes_txt}{role_txt}"[:1000]


def _format_radio_summary_state(guild_id: int, channel_id: int) -> str:
    """Render the radio_summary subscription state for a channel."""
    preset = _default_preset_for(bot.db, guild_id, channel_id)
    if not preset or 'radio_summary' not in (preset.get('alert_types') or []):
        return "**Off**"

    try:
        channel_mute = bot.db.get_channel_mute(guild_id, channel_id)
        guild_mute = bot.db.get_guild_mute(guild_id)
    except Exception:
        channel_mute = None
        guild_mute = None
    eff = bot.db.resolve_preset_effective_state(
        preset, 'radio_summary', channel_mute, guild_mute,
    )
    cfg_view = {
        'role_ids': preset.get('role_ids') or [],
        'enabled': 1 if eff['enabled'] else 0,
        'enabled_ping': 1 if eff['enabled_ping'] else 0,
    }
    roles = _format_role_mentions(cfg_view)
    role_txt = f"\nPing Role(s): {roles}" if roles else ""
    mute = _format_mute_state(cfg_view)
    return f"**On**{mute}{role_txt}"[:1000]


def _build_setup_home_embed(channel: discord.TextChannel, guild_id: int) -> discord.Embed:
    embed = discord.Embed(
        title="⚙️ Setup",
        description=f"Channel: {channel.mention}\n\nChoose what you want to edit:",
        color=0x3498db
    )
    embed.add_field(name="📢 Alerts", value=_format_alert_configs_for_channel(guild_id, channel.id), inline=False)
    embed.add_field(name="📻 Radio Summary", value=_format_radio_summary_state(guild_id, channel.id), inline=False)
    embed.add_field(name="📟 Pager", value=_format_pager_config_for_channel(guild_id, channel.id), inline=False)
    embed.add_field(name="🌐 Website", value=f"[nswpsn.forcequit.xyz]({WEBSITE_URL})", inline=False)
    return embed


async def _edit_or_send(interaction: discord.Interaction, *, embed: discord.Embed, view: Optional[discord.ui.View]):
    try:
        await interaction.response.edit_message(embed=embed, view=view)
    except Exception:
        try:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        except Exception:
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)


# (removed 2026-04: _build_setup_*_container + _edit_or_send_v2 were part of a
# partial V2 /setup migration that was reverted. Submenus are still V1.)


class SetupHomeView(discord.ui.View):
    """V1 home menu for /setup. The submenu views are all V1, so this view
    stays V1 too — mixing V1 submenu + V2 home produces a 400 when Discord
    rejects Button (type 2) components on a V2 message.
    """

    def __init__(self, invoker_id: int, channel: discord.TextChannel):
        super().__init__(timeout=180)
        self.invoker_id = invoker_id
        self.channel = channel

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    @discord.ui.button(label="Setup Alerts", style=discord.ButtonStyle.primary)
    async def setup_alerts(self, interaction: discord.Interaction, button: discord.ui.Button):
        loop = asyncio.get_event_loop()
        current = await loop.run_in_executor(
            None, _format_alert_configs_for_channel,
            interaction.guild_id, self.channel.id,
        )
        embed = discord.Embed(
            title="📢 Setup Alerts",
            description=(
                f"Editing alert subscriptions for {self.channel.mention}\n\n"
                f"*Select alert types from the dropdown, then click off the dropdown "
                f"and press **Save**.*"
            ),
            color=0x3498db,
        )
        embed.add_field(name="Current", value=current, inline=False)
        embed.add_field(
            name="🌐 Website",
            value=f"[nswpsn.forcequit.xyz]({WEBSITE_URL})",
            inline=False,
        )
        await _edit_or_send(
            interaction, embed=embed,
            view=SetupAlertsSubmenuView(self.invoker_id, self.channel),
        )

    @discord.ui.button(label="Setup Pager", style=discord.ButtonStyle.primary)
    async def setup_pager(self, interaction: discord.Interaction, button: discord.ui.Button):
        loop = asyncio.get_event_loop()
        current = await loop.run_in_executor(
            None, _format_pager_config_for_channel,
            interaction.guild_id, self.channel.id,
        )
        embed = discord.Embed(
            title="📟 Setup Pager",
            description=f"Editing pager hits for {self.channel.mention}",
            color=0x3498db,
        )
        embed.add_field(name="Current", value=current, inline=False)
        embed.add_field(
            name="🌐 Website",
            value=f"[nswpsn.forcequit.xyz]({WEBSITE_URL})",
            inline=False,
        )
        await _edit_or_send(
            interaction, embed=embed,
            view=SetupPagerSubmenuView(self.invoker_id, self.channel),
        )

    @discord.ui.button(label="Radio Summary", style=discord.ButtonStyle.primary, emoji="📻")
    async def setup_radio_summary(self, interaction: discord.Interaction, button: discord.ui.Button):
        loop = asyncio.get_event_loop()
        current = await loop.run_in_executor(
            None, _format_radio_summary_state,
            interaction.guild_id, self.channel.id,
        )
        embed = discord.Embed(
            title="📻 Radio Scanner Hourly Summary",
            description=(
                f"Editing the hourly rdio-scanner summary feed for {self.channel.mention}.\n\n"
                f"*Posts one message per hour (~1 min past the hour) summarising FRNSW / "
                f"RFS radio activity. Use `/mute` or `/smute` afterwards if you want to "
                f"silence it without un-subscribing.*"
            ),
            color=0x8b5cf6,
        )
        embed.add_field(name="Current", value=current, inline=False)
        await _edit_or_send(
            interaction, embed=embed,
            view=SetupRadioSummarySubmenuView(self.invoker_id, self.channel),
        )

    @discord.ui.button(label="Setup Roles", style=discord.ButtonStyle.secondary, emoji="🎭")
    async def setup_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        loop = asyncio.get_event_loop()
        # Build the embed + read both current-state strings off the gateway thread.
        def _build():
            e = _build_roles_embed(self.channel, interaction.guild_id, selected_types=[])
            e.add_field(
                name="Current Alerts",
                value=_format_alert_configs_for_channel(interaction.guild_id, self.channel.id),
                inline=False,
            )
            e.add_field(
                name="Current Pager",
                value=_format_pager_config_for_channel(interaction.guild_id, self.channel.id),
                inline=False,
            )
            return e
        embed = await loop.run_in_executor(None, _build)
        await _edit_or_send(
            interaction, embed=embed,
            view=SetupRolesSubmenuView(self.invoker_id, self.channel),
        )


def _load_roles_for_type(guild_id: int, channel_id: int, alert_type: str) -> List[int]:
    """Read current role_ids for a given alert_type or 'pager' in a channel.

    Roles now live on the Default preset and apply to ALL alert_types in that
    preset — there's no per-alert-type role anymore. We still expose this
    per-type signature so the existing roles-edit UI keeps working."""
    preset = _default_preset_for(bot.db, guild_id, channel_id)
    if not preset:
        return []
    if alert_type == 'pager' and not preset.get('pager_enabled'):
        return []
    if alert_type != 'pager' and alert_type not in (preset.get('alert_types') or []):
        return []
    raw = preset.get('role_ids') or []
    return [int(r) for r in raw if r is not None]


def _type_label(atype: str) -> str:
    if atype == 'pager':
        return 'Pager Messages'
    return ALERT_TYPES.get(atype, atype)


def _build_roles_embed(channel: discord.TextChannel, guild_id: int,
                       selected_types: List[str]) -> discord.Embed:
    description = (
        f"Pick which roles get pinged for {channel.mention}.\n\n"
        "1. Pick **one or more** alert types in the first dropdown.\n"
        "2. Pick up to 5 roles in the second dropdown.\n"
        "3. Press **Save** — those roles apply to **every alert type "
        "subscribed in this channel** (not just the picked ones).\n\n"
        "*Note: roles in the preset model are channel-wide. To get "
        "per-alert-type role pings, create extra presets in the dashboard.*"
    )
    embed = discord.Embed(
        title="🎭 Setup Roles",
        description=description,
        color=0x3498db,
    )
    if selected_types:
        lines = []
        for atype in selected_types:
            current_ids = _load_roles_for_type(guild_id, channel.id, atype)
            roles_txt = ' '.join(f'<@&{r}>' for r in current_ids) if current_ids else '*(none)*'
            lines.append(f"**{_type_label(atype)}** → {roles_txt}")
        embed.add_field(
            name=f"Current roles — {len(selected_types)} type(s) selected",
            value='\n'.join(lines)[:1024],
            inline=False,
        )
    else:
        embed.add_field(
            name="Tip",
            value="Only rows that already exist can have roles set — enable the "
                  "alert type from `Setup Alerts` / `Setup Pager` first if it's off.",
            inline=False,
        )
    return embed


class SetupRolesSubmenuView(discord.ui.View):
    """Pick roles for one or more alert types at once."""

    def __init__(self, invoker_id: int, channel: discord.TextChannel,
                 selected_types: Optional[List[str]] = None):
        super().__init__(timeout=180)
        self.invoker_id = invoker_id
        self.channel = channel
        self.selected_types: List[str] = list(selected_types or [])
        self.selected_role_ids: List[int] = []
        self._roles_touched = False

        # Row 0 — multi-select alert-type picker (+ pager).
        selected_set = set(self.selected_types)
        type_options = [
            discord.SelectOption(
                label=name, value=key,
                default=(key in selected_set),
            )
            for key, name in ALERT_TYPES.items()
        ]
        type_options.append(discord.SelectOption(
            label='Pager Messages', value='pager', emoji='📟',
            default=('pager' in selected_set),
        ))
        max_types = len(type_options)
        type_select = discord.ui.Select(
            placeholder="1. Pick one or more alert types",
            min_values=1, max_values=max_types,
            options=type_options,
            row=0,
        )
        type_select.callback = self._on_types_picked
        self.add_item(type_select)
        self._type_select = type_select

        # Row 1 — role picker. Always shown but only meaningful once types are chosen.
        role_select = discord.ui.RoleSelect(
            placeholder="2. Pick up to 5 roles (empty + Save = no change)",
            min_values=0, max_values=5,
            row=1,
        )
        role_select.callback = self._on_roles_picked
        self.add_item(role_select)
        self._role_select = role_select

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    async def _on_types_picked(self, interaction: discord.Interaction):
        picked = list(self._type_select.values)
        # Rebuild so the embed reflects current roles for each selected type.
        new_view = SetupRolesSubmenuView(self.invoker_id, self.channel, selected_types=picked)
        embed = _build_roles_embed(self.channel, interaction.guild_id, picked)
        await interaction.response.edit_message(embed=embed, view=new_view)

    async def _on_roles_picked(self, interaction: discord.Interaction):
        self.selected_role_ids = [r.id for r in self._role_select.values]
        self._roles_touched = True
        await interaction.response.defer(ephemeral=True)

    def _apply_roles_to_types(self, guild_id: int, channel_id: int,
                              atypes: List[str], role_ids: List[int]) -> int:
        """Apply role_ids to the channel's Default preset, when at least one
        of the picked alert types is actually subscribed.

        NOTE: roles in the preset model apply to the WHOLE preset (every
        alert_type subscribed in it), not per-type. So this writes role_ids
        once and returns 1 if any picked atype matched, otherwise 0."""
        preset = _default_preset_for(bot.db, guild_id, channel_id)
        if not preset:
            return 0
        present_types = set(preset.get('alert_types') or [])
        has_pager = bool(preset.get('pager_enabled'))
        any_match = any(
            (a == 'pager' and has_pager) or (a != 'pager' and a in present_types)
            for a in atypes
        )
        if not any_match:
            return 0
        bot.db.update_preset(preset['id'], role_ids=role_ids)
        return 1

    @discord.ui.button(label="Save", style=discord.ButtonStyle.green, row=2)
    async def save(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_types:
            await interaction.response.send_message(
                "❌ Pick at least one alert type first.", ephemeral=True,
            )
            return
        if not self._roles_touched:
            await interaction.response.send_message(
                "❌ Pick roles in the second dropdown first, or use **Clear** "
                "if you want to remove them.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        guild_id = interaction.guild_id
        channel_id = self.channel.id
        atypes = list(self.selected_types)
        role_ids = list(self.selected_role_ids)

        loop = asyncio.get_event_loop()
        try:
            applied = await loop.run_in_executor(
                None, self._apply_roles_to_types,
                guild_id, channel_id, atypes, role_ids,
            )
        except Exception as e:
            logger.error(f"Error saving roles: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ Failed to save roles.", ephemeral=True,
            )
            return

        if applied == 0:
            await interaction.followup.send(
                "⚠️ None of the selected types are enabled in this channel yet — "
                "nothing to update. Enable them in Setup Alerts / Setup Pager first.",
                ephemeral=True,
            )
            return

        home_embed = await loop.run_in_executor(
            None, _build_setup_home_embed, self.channel, guild_id,
        )
        await _edit_or_send(
            interaction, embed=home_embed,
            view=SetupHomeView(self.invoker_id, self.channel),
        )

    @discord.ui.button(label="Clear Roles", style=discord.ButtonStyle.danger, row=2)
    async def clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_types:
            await interaction.response.send_message(
                "❌ Pick at least one alert type first.", ephemeral=True,
            )
            return

        guild_id = interaction.guild_id
        channel_id = self.channel.id
        atypes = list(self.selected_types)

        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None, self._apply_roles_to_types,
                guild_id, channel_id, atypes, [],
            )
        except Exception as e:
            logger.error(f"Error clearing roles: {e}", exc_info=True)

        # Keep the user on the roles view with the same selection so they can
        # see the cleared state and adjust again.
        new_view = SetupRolesSubmenuView(self.invoker_id, self.channel, selected_types=atypes)
        embed = _build_roles_embed(self.channel, guild_id, atypes)
        await interaction.response.edit_message(embed=embed, view=new_view)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=2)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        loop = asyncio.get_event_loop()
        home_embed = await loop.run_in_executor(
            None, _build_setup_home_embed, self.channel, interaction.guild_id,
        )
        await _edit_or_send(
            interaction, embed=home_embed,
            view=SetupHomeView(self.invoker_id, self.channel),
        )


class SetupRadioSummarySubmenuView(discord.ui.View):
    """Toggle the radio_summary subscription for a channel (enable/disable)."""

    def __init__(self, invoker_id: int, channel: discord.TextChannel):
        super().__init__(timeout=180)
        self.invoker_id = invoker_id
        self.channel = channel

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    @discord.ui.button(label="Enable", style=discord.ButtonStyle.green, emoji="📻")
    async def enable(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id
        channel_id = self.channel.id

        def _enable_sync():
            preset = _default_preset_for(bot.db, guild_id, channel_id)
            if preset and 'radio_summary' in (preset.get('alert_types') or []):
                # Already subscribed — clear any per-type override that may
                # have come from /mute or /smute on this alert type.
                bot.db.clear_preset_type_override(preset['id'], 'radio_summary')
            else:
                _default_preset_upsert(
                    bot.db, guild_id, channel_id,
                    add_alert_type='radio_summary',
                )

        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, _enable_sync)
        except Exception as e:
            logger.error(f"Error enabling radio_summary: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ Failed to enable radio summary.", ephemeral=True,
            )
            return

        home_embed = await loop.run_in_executor(
            None, _build_setup_home_embed, self.channel, guild_id,
        )
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="Disable", style=discord.ButtonStyle.danger, emoji="🚫")
    async def disable(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id
        channel_id = self.channel.id

        def _disable_sync():
            _default_preset_remove_alert_type(
                bot.db, guild_id, channel_id, 'radio_summary',
            )

        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, _disable_sync)
        except Exception as e:
            logger.error(f"Error disabling radio_summary: {e}", exc_info=True)

        home_embed = await loop.run_in_executor(
            None, _build_setup_home_embed, self.channel, guild_id,
        )
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        loop = asyncio.get_event_loop()
        home_embed = await loop.run_in_executor(
            None, _build_setup_home_embed, self.channel, interaction.guild_id,
        )
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))


class SetupAlertsSubmenuView(discord.ui.View):
    def __init__(self, invoker_id: int, channel: discord.TextChannel):
        super().__init__(timeout=180)
        self.invoker_id = invoker_id
        self.channel = channel

        existing_cfgs = _get_alert_configs_for_channel(channel.guild.id, channel.id)
        # Only general alert types — radio_summary is managed in its own submenu
        existing_types = sorted([
            c.get("alert_type") for c in existing_cfgs
            if c.get("alert_type") and c.get("alert_type") != 'radio_summary'
        ])
        self.selected_alert_types: List[str] = existing_types[:]

        self.alert_select.options = [
            discord.SelectOption(label=name, value=key, default=(key in set(existing_types)))
            for key, name in _GENERAL_ALERT_TYPES.items()
        ]

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    @discord.ui.select(
        placeholder="Select alert types to enable",
        min_values=0,
        max_values=len(_GENERAL_ALERT_TYPES)
    )
    async def alert_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.selected_alert_types = list(select.values)
        await interaction.response.defer(ephemeral=True)

    @discord.ui.button(label="Save", style=discord.ButtonStyle.green)
    async def save(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id
        channel_id = self.channel.id
        selected = set(self.selected_alert_types)

        def _sync_work():
            preset = _default_preset_for(bot.db, guild_id, channel_id)
            current_types = list((preset or {}).get('alert_types') or [])
            # Preserve radio_summary (managed in its own submenu).
            keep_radio = 'radio_summary' if 'radio_summary' in current_types else None
            new_types: List[str] = list(selected)
            if keep_radio and keep_radio not in new_types:
                new_types.append(keep_radio)

            if not preset:
                if new_types:
                    bot.db.create_preset(
                        guild_id=guild_id, channel_id=channel_id,
                        name=DEFAULT_PRESET_NAME,
                        alert_types=new_types,
                        pager_enabled=False,
                    )
            else:
                if not new_types and not preset.get('pager_enabled'):
                    bot.db.delete_preset(preset['id'])
                else:
                    bot.db.update_preset(preset['id'], alert_types=new_types)
            return _build_setup_home_embed(self.channel, guild_id)

        loop = asyncio.get_event_loop()
        home_embed = await loop.run_in_executor(None, _sync_work)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="Enable All Alerts", style=discord.ButtonStyle.green)
    async def enable_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id
        channel_id = self.channel.id

        def _sync_work():
            # Enable all *general* alert types. radio_summary is opt-in through its
            # own button so we don't drag it in with the broad "enable all".
            preset = _default_preset_for(bot.db, guild_id, channel_id)
            current_types = list((preset or {}).get('alert_types') or [])
            new_types = list(current_types)
            for atype in _GENERAL_ALERT_TYPES.keys():
                if atype not in new_types:
                    new_types.append(atype)
            if not preset:
                bot.db.create_preset(
                    guild_id=guild_id, channel_id=channel_id,
                    name=DEFAULT_PRESET_NAME,
                    alert_types=new_types,
                    pager_enabled=False,
                )
            else:
                bot.db.update_preset(preset['id'], alert_types=new_types)
            return _build_setup_home_embed(self.channel, guild_id)

        loop = asyncio.get_event_loop()
        home_embed = await loop.run_in_executor(None, _sync_work)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="Turn Alerts Off", style=discord.ButtonStyle.danger)
    async def turn_off(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id
        channel_id = self.channel.id

        def _sync_work():
            # Only wipe general alert types — leave radio_summary alone.
            preset = _default_preset_for(bot.db, guild_id, channel_id)
            if preset:
                kept = [a for a in (preset.get('alert_types') or []) if a == 'radio_summary']
                if not kept and not preset.get('pager_enabled'):
                    bot.db.delete_preset(preset['id'])
                else:
                    bot.db.update_preset(preset['id'], alert_types=kept)
            return _build_setup_home_embed(self.channel, guild_id)

        loop = asyncio.get_event_loop()
        home_embed = await loop.run_in_executor(None, _sync_work)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        loop = asyncio.get_event_loop()
        home_embed = await loop.run_in_executor(
            None, _build_setup_home_embed, self.channel, interaction.guild_id,
        )
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))


class SetupPagerSubmenuView(discord.ui.View):
    def __init__(self, invoker_id: int, channel: discord.TextChannel):
        super().__init__(timeout=180)
        self.invoker_id = invoker_id
        self.channel = channel

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    @discord.ui.button(label="Turn Pager Off", style=discord.ButtonStyle.danger)
    async def turn_off(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id
        channel_id = self.channel.id

        def _sync_work():
            _default_preset_clear_pager(bot.db, guild_id, channel_id)
            return _build_setup_home_embed(self.channel, guild_id)

        loop = asyncio.get_event_loop()
        home_embed = await loop.run_in_executor(None, _sync_work)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="All Pager Hits", style=discord.ButtonStyle.green)
    async def all_hits(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild_id
        channel_id = self.channel.id

        def _sync_work():
            _default_preset_upsert(
                bot.db, guild_id, channel_id,
                pager_enabled=True,
                pager_capcodes=None,
            )
            return _build_setup_home_embed(self.channel, guild_id)

        loop = asyncio.get_event_loop()
        home_embed = await loop.run_in_executor(None, _sync_work)
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

    @discord.ui.button(label="Filter Capcodes…", style=discord.ButtonStyle.primary)
    async def filter_capcodes(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def _after_modal(modal_interaction: discord.Interaction, capcodes: Optional[str]):
            await modal_interaction.response.defer(ephemeral=True)
            guild_id = modal_interaction.guild_id
            channel_id = self.channel.id

            def _sync_work():
                _default_preset_upsert(
                    bot.db, guild_id, channel_id,
                    pager_enabled=True,
                    pager_capcodes=capcodes,
                )
                return _build_setup_home_embed(self.channel, guild_id)

            loop = asyncio.get_event_loop()
            home_embed = await loop.run_in_executor(None, _sync_work)
            await _edit_or_send(modal_interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))

        await interaction.response.send_modal(PagerCapcodesModal(_after_modal))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        loop = asyncio.get_event_loop()
        home_embed = await loop.run_in_executor(
            None, _build_setup_home_embed, self.channel, interaction.guild_id,
        )
        await _edit_or_send(interaction, embed=home_embed, view=SetupHomeView(self.invoker_id, self.channel))


@bot.tree.command(name="setup", description="Interactive setup wizard for alerts and/or pager hits")
@app_commands.describe(
    channel="Channel to configure (defaults to current channel)"
)
@app_commands.default_permissions(manage_channels=True)
async def setup_command(
    interaction: discord.Interaction,
    channel: Optional[discord.TextChannel] = None
):
    # NOTE: /setup must not change configs by default. It only shows current config and allows editing.
    if channel is None:
        if isinstance(interaction.channel, discord.TextChannel):
            channel = interaction.channel
        else:
            await interaction.response.send_message("❌ Please specify a channel for `/setup`.", ephemeral=True)
            return

    await interaction.response.defer(ephemeral=True)
    loop = asyncio.get_event_loop()
    embed = await loop.run_in_executor(
        None, _build_setup_home_embed, channel, interaction.guild_id,
    )
    await interaction.followup.send(
        embed=embed, view=SetupHomeView(interaction.user.id, channel), ephemeral=True,
    )


@bot.tree.command(name="dashboard", description="Open the web dashboard to manage alerts")
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=True)
async def dashboard_command(interaction: discord.Interaction):
    base = os.environ.get('PUBLIC_BASE_URL', 'https://nswpsn.forcequit.xyz').rstrip('/')
    url = f"{base}/dashboard"
    params = []
    if interaction.guild_id:
        params.append(f"guild={interaction.guild_id}")
    if interaction.channel_id:
        params.append(f"channel={interaction.channel_id}")
    if params:
        url = f"{url}?{'&'.join(params)}"
    # Send an ephemeral reply with a link button (V1 button — not V2 — because
    # the message is simple). No embed needed; just content with the URL and a
    # button for one-click open.
    view = discord.ui.View(timeout=300)
    view.add_item(discord.ui.Button(
        style=discord.ButtonStyle.link,
        label="Open Dashboard",
        url=url,
        emoji="🎛️",
    ))
    await interaction.response.send_message(
        content=(
            "Click below to manage your alert subscriptions, pager config, and role pings "
            "in the web UI. The link auto-selects this server and channel."
        ),
        view=view,
        ephemeral=True,
    )


# ==================== /help CATEGORY BROWSER ====================

_HELP_COLOR = 0x3498db

# Category key -> title + intro lines / command entries. Each command entry is
# (command, description). Kept concise — one line per command where possible.
_HELP_CATEGORIES: Dict[str, Dict[str, Any]] = {
    "intro": {
        "title": "📚 NSW PSN Alert Bot",
        "lines": [
            "Real-time alerts for NSW emergencies, traffic, weather warnings, "
            "and pager messages — sourced from RFS, BOM, TfNSW, Ausgrid, "
            "Endeavour, and Waze.",
            "",
            f"🌐 Website: [nswpsn.forcequit.xyz]({WEBSITE_URL})",
            "",
            "**Pick a category below to see commands.**",
        ],
    },
    "alerts": {
        "title": "📢 Alerts",
        "commands": [
            ("/dashboard",
             "Same editing surface as /setup, but in a browser. Link auto-scopes to "
             "this server + channel."),
            ("/setup",
             "**Start here.** Interactive wizard — pick a channel, choose "
             "alert types (including pager + radio summary), configure "
             "roles, and mute/unmute, all from one menu."),
        ],
        "footer": (
            "Power users can manage subscriptions manually — see the 🔧 Manual "
            "category. Everyone else should stick with /setup."
        ),
    },
    "mute": {
        "title": "🔕 Mute",
        "commands": [
            ("/mute",
             "Stop role pings but keep the alert embeds. Scope by channel "
             "and/or alert type (default: whole server, all types)."),
            ("/smute",
             "Silence alerts entirely — no embed, no ping — without deleting "
             "the subscription."),
            ("/unmute",
             "Re-enable previously muted or silenced subscriptions."),
        ],
    },
    "radio": {
        "title": "📻 Radio",
        "commands": [
            ("/summary",
             "View the latest hourly radio summary, with arrows to walk back "
             "up to 24 hours. Pass `date` (YYYY-MM-DD) to step through every "
             "summary on that day instead. Works in DMs and via user-install."),
            ("/ts",
             "Search rdio-scanner radio transcripts. One phrase, or "
             "comma-separated for OR (e.g. `fire,crash,police`). "
             "Optional `date` (YYYY-MM-DD). Works in DMs and via user-install."),
        ],
        "footer": (
            "Live radio-summary pushes are available as an alert type — "
            "subscribe via `/setup` → Radio Summary."
        ),
    },
    "info": {
        "title": "📊 Info",
        "commands": [
            ("/overview",
             "Dashboard of current incidents across NSW (formerly /summary)."),
            ("/dashboard",
             "Open the web dashboard to manage alerts, pager config, and roles in a GUI."),
            ("/status",
             "Bot latency, server count, and subscription totals."),
            ("/help",
             "Show this help browser."),
        ],
    },
    "manual": {
        "title": "🔧 Manual (Power Users)",
        "commands": [
            ("/alert",
             "Subscribe a channel to a single alert type (or all types if "
             "omitted). Optional role to ping. **/setup is usually easier.**"),
            ("/alert-remove",
             "Remove one or all alert subscriptions from a channel."),
            ("/alert-list",
             "List every alert + pager subscription in this server."),
            ("/pager",
             "Subscribe a channel to NSW pager messages with an optional "
             "comma-separated capcode filter + role to ping."),
            ("/pager-remove",
             "Remove the pager subscription from a channel."),
        ],
        "footer": (
            "Alert types: rfs · bom_land · bom_marine · traffic_incident · "
            "traffic_roadwork · traffic_flood · traffic_fire · "
            "traffic_majorevent · endeavour_current · endeavour_planned · "
            "ausgrid · essential_planned · essential_future · "
            "waze_hazard · waze_jam · waze_police · waze_roadwork · "
            "user_incident · radio_summary"
        ),
    },
}


def build_help_embed(category: str) -> discord.Embed:
    """Build the embed for a given help category key.

    Falls back to the intro embed for unknown keys.
    """
    data = _HELP_CATEGORIES.get(category) or _HELP_CATEGORIES["intro"]

    if "lines" in data:
        embed = discord.Embed(
            title=data["title"],
            description="\n".join(data["lines"]),
            color=_HELP_COLOR,
        )
        embed.set_footer(
            text="NSW PSN • Pick a category from the menu below",
        )
        return embed

    lines = [f"`{cmd}` — {desc}" for cmd, desc in data["commands"]]
    embed = discord.Embed(
        title=data["title"],
        description="\n\n".join(lines),
        color=_HELP_COLOR,
    )
    if data.get("footer"):
        embed.set_footer(text=data["footer"])
    else:
        embed.set_footer(text="NSW PSN • /help to browse other categories")
    return embed


class HelpView(discord.ui.View):
    """Category-select dropdown for /help. Only the invoker can change pages."""

    def __init__(self, invoker_id: int):
        super().__init__(timeout=300)
        self.invoker_id = invoker_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "Run `/help` yourself to browse commands.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self) -> None:
        # Disable children once the view expires so stale menus don't look
        # interactive. Best-effort — we don't hold a message handle here to
        # push the disabled state back to Discord.
        for child in self.children:
            child.disabled = True

    @discord.ui.select(
        placeholder="Pick a category…",
        min_values=1,
        max_values=1,
        options=[
            discord.SelectOption(label="Alerts", value="alerts", emoji="📢",
                                 description="Start here — the /setup wizard"),
            discord.SelectOption(label="Mute", value="mute", emoji="🔕",
                                 description="Silence pings or whole alerts"),
            discord.SelectOption(label="Radio", value="radio", emoji="📻",
                                 description="Search radio transcripts"),
            discord.SelectOption(label="Info", value="info", emoji="📊",
                                 description="Summary, status, help"),
            discord.SelectOption(label="Manual", value="manual", emoji="🔧",
                                 description="Power-user commands"),
        ],
    )
    async def category_select(
        self,
        interaction: discord.Interaction,
        select: discord.ui.Select,
    ):
        embed = build_help_embed(select.values[0])
        await interaction.response.edit_message(embed=embed, view=self)


@bot.tree.command(name="help", description="Show available commands and how to use the bot")
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=True)
async def help_command(interaction: discord.Interaction):
    """Interactive category-select help browser."""
    embed = build_help_embed("intro")
    view = HelpView(invoker_id=interaction.user.id)
    # Public reply (so others can see it if invoked in a channel), but
    # suppress all mentions so this message can never ping anyone.
    await interaction.response.send_message(
        embed=embed,
        view=view,
        allowed_mentions=discord.AllowedMentions.none(),
    )


# ==================== /ts RADIO TRANSCRIPT SEARCH ====================

_TS_PAGE_SIZE = 10
_TS_MAX_TRANSCRIPT_CHARS = 240  # trim long transmissions in the embed
_TS_LOCAL_TZ_NAME = os.getenv('SUMMARY_TZ', 'Australia/Sydney')


async def _ts_fetch_page(query: str, date: Optional[str], offset: int) -> Optional[dict]:
    """Hit /api/rdio/transcripts/search. Returns None on failure."""
    params = {
        'q': query,
        'limit': _TS_PAGE_SIZE,
        'offset': offset,
        'order': 'desc',
    }
    if date:
        params['date'] = date
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'User-Agent': 'NSWPSNBot/1.0',
        'X-Client-Type': 'discord-bot',
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{API_BASE_URL}/api/rdio/transcripts/search",
                headers=headers, params=params, timeout=60,
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"/ts got {resp.status} from backend")
                    return None
                return await resp.json()
    except Exception as e:
        logger.error(f"/ts fetch error: {type(e).__name__}: {e!r}", exc_info=True)
        return None


def _ts_format_local_time(iso_str: Optional[str]) -> str:
    if not iso_str:
        return '??:??'
    try:
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        try:
            from zoneinfo import ZoneInfo
            dt = dt.astimezone(ZoneInfo(_TS_LOCAL_TZ_NAME))
        except Exception:
            pass
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return iso_str[:19].replace('T', ' ')


def _ts_build_embed(data: dict, query: str, date: Optional[str]) -> discord.Embed:
    total = int(data.get('total') or 0)
    offset = int(data.get('offset') or 0)
    limit = int(data.get('limit') or _TS_PAGE_SIZE) or _TS_PAGE_SIZE
    results = data.get('results') or []

    title = f"🔎 Transcripts · \"{query}\""
    if total == 0:
        embed = discord.Embed(
            title=title,
            description="No matching transmissions found.",
            color=0x94a3b8,
        )
        if date:
            embed.set_footer(text=f"date: {date}")
        return embed

    page = (offset // limit) + 1
    total_pages = max(1, (total + limit - 1) // limit)

    lines = []
    for r in results:
        when = _ts_format_local_time(r.get('datetime'))
        tg = r.get('talkgroup_label') or f"TG {r.get('talkgroup') or '?'}"
        rid_label = r.get('radio_label')
        rid = r.get('radio_id')
        who = rid_label or (f"RID {rid}" if rid else None)
        transcript = (r.get('transcript') or '').strip().replace('\n', ' ')
        if len(transcript) > _TS_MAX_TRANSCRIPT_CHARS:
            transcript = transcript[:_TS_MAX_TRANSCRIPT_CHARS - 1].rstrip() + '…'
        url = r.get('call_url') or f"https://radio.forcequit.xyz/?call={r.get('id')}"
        header = f"🕐 `{when}` · **{tg}**"
        if who:
            header += f" · {who}"
        header += f" · [#{r.get('id')}]({url})"
        lines.append(f"{header}\n> {transcript}")

    description = '\n\n'.join(lines)
    # Hard cap — Discord rejects descriptions > 4096 chars
    if len(description) > 4000:
        description = description[:3997] + '…'

    embed = discord.Embed(
        title=title,
        description=description,
        color=0x3498db,
    )
    footer = f"Page {page}/{total_pages} · {total} total"
    if date:
        footer += f" · {date}"
    embed.set_footer(text=footer)
    return embed


class TsPager(discord.ui.View):
    def __init__(self, invoker_id: int, query: str, date: Optional[str], total: int):
        super().__init__(timeout=300)
        self.invoker_id = invoker_id
        self.query = query
        self.date = date
        self.offset = 0
        self.total = total
        self._refresh_buttons()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "This pager belongs to someone else — run `/ts` yourself to search.",
                ephemeral=True,
            )
            return False
        return True

    def _refresh_buttons(self):
        has_prev = self.offset > 0
        has_next = (self.offset + _TS_PAGE_SIZE) < self.total
        # Reference buttons by attribute name (not child index) so adding
        # First/Last doesn't silently break disable logic.
        self.first_button.disabled = not has_prev
        self.prev_button.disabled = not has_prev
        self.next_button.disabled = not has_next
        self.last_button.disabled = not has_next

    async def _update(self, interaction: discord.Interaction):
        data = await _ts_fetch_page(self.query, self.date, self.offset)
        if data is None:
            await interaction.response.send_message(
                "❌ Backend error fetching transcripts.", ephemeral=True,
            )
            return
        self.total = int(data.get('total') or 0)
        self._refresh_buttons()
        embed = _ts_build_embed(data, self.query, self.date)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="|◀ First", style=discord.ButtonStyle.secondary)
    async def first_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.offset = 0
        await self._update(interaction)

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.offset = max(0, self.offset - _TS_PAGE_SIZE)
        await self._update(interaction)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.offset += _TS_PAGE_SIZE
        await self._update(interaction)

    @discord.ui.button(label="Last ▶|", style=discord.ButtonStyle.secondary)
    async def last_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Jump to the offset that starts the final page.
        if self.total <= 0:
            self.offset = 0
        else:
            total_pages = max(1, (self.total + _TS_PAGE_SIZE - 1) // _TS_PAGE_SIZE)
            self.offset = (total_pages - 1) * _TS_PAGE_SIZE
        await self._update(interaction)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger)
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        # Ephemeral messages from followup.send() can't always be edited
        # via interaction.response.edit_message — try that first, then
        # fall back to editing the original response directly.
        try:
            await interaction.response.edit_message(view=self)
        except (discord.InteractionResponded, discord.HTTPException):
            try:
                await interaction.edit_original_response(view=self)
            except Exception as e:
                logger.warning(f"/ts close fallback failed: {type(e).__name__}: {e}")
        self.stop()


@bot.tree.command(name="ts", description="Search rdio-scanner radio transcripts")
@app_commands.describe(
    query="Keyword(s) — one phrase, or comma-separated for OR (e.g. 'fire,crash,police')",
    date="Optional date YYYY-MM-DD (local time)",
)
# User-install support: /ts is available in guilds, DMs, group DMs, and any
# channel the invoking user is in (via user-install). Output is public so
# anyone in the channel can see the result; the pager buttons are still
# owner-locked via TsPager.interaction_check.
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=True)
async def ts_command(
    interaction: discord.Interaction,
    query: str,
    date: Optional[str] = None,
):
    query = (query or '').strip()
    # Comma mode: at least one segment must be >= 2 chars after stripping.
    valid_terms = [t.strip() for t in query.split(',') if len(t.strip()) >= 2]
    if not valid_terms:
        await interaction.response.send_message(
            "❌ Query must contain at least one term of 2+ characters "
            "(comma-separate for multiple: `fire,crash,police`).",
            ephemeral=True,
        )
        return

    # Public defer — result is visible to the whole channel. Validation errors
    # stay ephemeral (above) so we don't spam the channel with bad-query noise.
    await interaction.response.defer(ephemeral=False, thinking=True)

    data = await _ts_fetch_page(query, date, offset=0)
    if data is None:
        # Backend error is still ephemeral — noise to show publicly.
        await interaction.followup.send(
            "❌ Backend error fetching transcripts. Try again shortly.",
            ephemeral=True,
        )
        return

    total = int(data.get('total') or 0)
    embed = _ts_build_embed(data, query, date)

    if total <= _TS_PAGE_SIZE:
        # No pagination needed — send a plain embed
        await interaction.followup.send(embed=embed, ephemeral=False)
        return

    view = TsPager(invoker_id=interaction.user.id, query=query, date=date, total=total)
    await interaction.followup.send(embed=embed, view=view, ephemeral=False)


# ---------------------------------------------------------------------------
# /summary — Components V2 paged navigator over hourly radio summaries.
# ---------------------------------------------------------------------------
# In default mode (no `date`) the navigator loads the most recent
# _SUMMARY_HISTORY_LIMIT hourly summaries; with `date` it loads every
# summary on that day. List is held in-memory and indexed (newest = 0)
# so navigation is local — no per-click backend round-trip.

_SUMMARY_HISTORY_LIMIT = 24


async def _summary_fetch(date: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """Fetch hourly summaries from /api/summaries. Returns newest-first list,
    or None on backend error, or empty list if nothing matches."""
    import aiohttp
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'User-Agent': 'NSWPSNBot/1.0',
        'X-Client-Type': 'discord-bot',
    }
    params = {'type': 'hourly', 'limit': '50' if date else str(_SUMMARY_HISTORY_LIMIT)}
    if date:
        params['date'] = date
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{API_BASE_URL}/api/summaries",
                headers=headers, params=params, timeout=15,
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"/api/summaries returned {resp.status}")
                    return None
                payload = await resp.json()
        return list(payload.get('results') or [])
    except Exception as e:
        logger.error(f"_summary_fetch error: {type(e).__name__}: {e}")
        return None


class SummaryPager(discord.ui.LayoutView):
    """Paged Components V2 view that navigates a list of hourly summaries.

    A summary often exceeds Discord's 4000-char-per-message text cap, so the
    navigator dispatches across several messages:

      • Heading + chunk[0] is the FIRST message in the thread.
      • Middle chunks are plain follow-up messages.
      • LAST chunk + navigation buttons lives in THIS LayoutView (the
        "anchor"), which is always the newest message in the thread so the
        user lands on the buttons after reading top-to-bottom.

    On a navigation click we delete the entire current thread (including the
    clicked anchor) and send a fresh thread for the new summary. The trade-off
    is that the new thread re-anchors at the channel bottom — that's the
    point: nav follows the user's eye, not the original send position.

    Newest-first list. index 0 is the most recent summary.
    Owner-locked via interaction_check, mirroring TsPager."""

    def __init__(self, invoker_id: int, summaries: List[Dict[str, Any]],
                 date: Optional[str] = None):
        super().__init__(timeout=600)
        self.invoker_id = invoker_id
        self.summaries = summaries
        self.date = date
        self.index = 0
        # Every message ID in the current thread, oldest-first. Anchor is at
        # the tail. On navigation we delete all of these and start fresh.
        self._thread_ids: List[int] = []
        self._webhook: Optional[discord.Webhook] = None
        self._rebuild_anchor()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "This pager belongs to someone else — run `/summary` yourself.",
                ephemeral=True,
            )
            return False
        return True

    def _header_text(self, page: int = 1, total_pages: int = 1) -> str:
        cur = self.summaries[self.index]
        n = len(self.summaries)
        period_start = cur.get('period_start') or ''
        period_end = cur.get('period_end') or ''
        when = ''
        try:
            ds = datetime.fromisoformat(period_start.replace('Z', '+00:00'))
            de = None
            if period_end:
                try:
                    de = datetime.fromisoformat(period_end.replace('Z', '+00:00'))
                except Exception:
                    de = None
            # `<t:N:D>` = "26 April 2026"; `:t` = "13:00". Discord renders these
            # in each viewer's local timezone automatically.
            date_part = f"<t:{int(ds.timestamp())}:D>"
            if de:
                when = f"{date_part} · <t:{int(ds.timestamp())}:t>–<t:{int(de.timestamp())}:t>"
            else:
                when = f"{date_part} · <t:{int(ds.timestamp())}:t>"
        except Exception:
            # Last-resort fallback if the timestamps don't parse.
            slot = cur.get('hour_slot')
            day = cur.get('day_date') or ''
            if day and slot is not None:
                when = f"{day} · hour {slot}"
            else:
                when = period_start or "(unknown time)"

        scope = f"date {self.date}" if self.date else f"last {n} hour{'s' if n != 1 else ''}"
        # Footer line: which summary (out of N), how many pages this summary
        # spans, and the navigation scope. Three counters are shown so users
        # can tell at a glance "I'm on summary 3 of 24, and this one runs
        # across page 1 of 4 messages".
        footer_parts = [f"Summary {self.index + 1} of {n}", scope]
        if total_pages > 1:
            footer_parts.append(f"page {page} of {total_pages}")
        return (
            f"### 📻 Hourly Radio Summary\n"
            f"{when}\n"
            f"-# {' · '.join(footer_parts)}"
        )

    def _page_text(self, page: int, total_pages: int) -> str:
        """Tiny per-message page indicator for middle / last messages
        in a multi-page summary thread. The first page already carries the
        full heading."""
        return f"-# 📻 Hourly Radio Summary · page {page} of {total_pages}"

    def _make_button(self, label: str, *, style=discord.ButtonStyle.secondary,
                     disabled=False, target_idx=None,
                     close=False) -> discord.ui.Button:
        btn = discord.ui.Button(label=label, style=style, disabled=disabled)
        if close:
            async def cb(interaction: discord.Interaction, _btn=btn):
                try:
                    await interaction.response.defer()
                except Exception as e:
                    logger.debug(f"/summary close: defer failed: {type(e).__name__}: {e}")
                wh = self._webhook or interaction.followup
                for mid in list(self._thread_ids):
                    try:
                        await wh.delete_message(mid)
                    except Exception as e:
                        logger.debug(f"/summary close: couldn't delete {mid}: {e}")
                self._thread_ids.clear()
                self.stop()
        else:
            async def cb(interaction: discord.Interaction, _idx=target_idx):
                self.index = max(0, min(len(self.summaries) - 1, int(_idx)))
                # Defer first since we're not editing the clicked message —
                # we delete it and re-send a fresh thread at the channel
                # bottom so the user keeps the buttons in sight.
                try:
                    await interaction.response.defer()
                except Exception as e:
                    logger.warning(f"/summary nav: defer failed: {type(e).__name__}: {e}")
                    return
                wh = self._webhook or interaction.followup
                # Tear down the existing thread (anchor + every follow-up).
                old_ids = list(self._thread_ids)
                self._thread_ids.clear()
                for mid in old_ids:
                    try:
                        await wh.delete_message(mid)
                    except Exception as e:
                        logger.debug(f"/summary nav: delete {mid} failed: {e}")
                # Rebuild the anchor for the new index and send the fresh
                # thread. send_initial appends to self._thread_ids.
                self._rebuild_anchor()
                await self._send_thread(wh)
        btn.callback = cb
        return btn

    def _current_groups(self) -> list:
        """Return the chunked container groups for the current summary."""
        if not self.summaries:
            return []
        cur = self.summaries[self.index]
        from embeds import chunk_containers_for_message
        containers = bot.embed_builder.build_radio_summary_components(cur) or []
        return chunk_containers_for_message(containers, max_chars=3200, max_per_message=2)

    def _build_lead_views(self) -> List[discord.ui.LayoutView]:
        """LayoutViews for the messages that go BEFORE the anchor — i.e.
        chunks 0 .. N-2. The first lead carries the heading at the top;
        middle leads get a small page indicator instead."""
        groups = self._current_groups()
        if len(groups) <= 1:
            return []
        total_pages = len(groups)
        out: List[discord.ui.LayoutView] = []
        for i, group in enumerate(groups[:-1]):
            page = i + 1
            v = discord.ui.LayoutView(timeout=None)
            if i == 0:
                v.add_item(discord.ui.TextDisplay(
                    content=self._header_text(page=page, total_pages=total_pages)))
            else:
                v.add_item(discord.ui.TextDisplay(
                    content=self._page_text(page, total_pages)))
            for c in group:
                v.add_item(c)
            out.append(v)
        return out

    def _rebuild_anchor(self):
        """(Re)populate `self` (the anchor view) with the LAST chunk + nav.
        When the summary is small enough to fit in one message, the anchor
        also carries the heading at the top."""
        self.clear_items()
        if not self.summaries:
            self.add_item(discord.ui.TextDisplay(content="*(no summaries available)*"))
            self._add_nav_row(only_close=True)
            return

        groups = self._current_groups()
        if not groups:
            self.add_item(discord.ui.TextDisplay(
                content=self._header_text(page=1, total_pages=1)))
            self.add_item(discord.ui.TextDisplay(content="*(empty summary)*"))
            self._add_nav_row()
            return

        total_pages = len(groups)
        if total_pages == 1:
            # Single-message thread — heading + body + nav all on the anchor.
            self.add_item(discord.ui.TextDisplay(
                content=self._header_text(page=1, total_pages=1)))
            for c in groups[0]:
                self.add_item(c)
        else:
            # Multi-message thread — anchor only carries the LAST chunk +
            # nav. Show a page indicator at top so readers landing on the
            # bottom message know it's the tail of a longer thread.
            self.add_item(discord.ui.TextDisplay(
                content=self._page_text(total_pages, total_pages)))
            for c in groups[-1]:
                self.add_item(c)

        self._add_nav_row()

    def _add_nav_row(self, only_close: bool = False):
        """Append the navigation ActionRow. Components V2 requires Buttons
        to live inside an ActionRow — V2 LayoutView rejects bare Buttons
        (type 2) at the top level."""
        n = len(self.summaries)
        at_newest = self.index <= 0
        at_oldest = self.index >= n - 1
        nav = discord.ui.ActionRow()
        if not only_close:
            nav.add_item(self._make_button(
                "|◀ Latest", target_idx=0, disabled=at_newest))
            nav.add_item(self._make_button(
                "◀ Newer", target_idx=self.index - 1, disabled=at_newest))
            nav.add_item(self._make_button(
                "Older ▶", target_idx=self.index + 1, disabled=at_oldest))
            nav.add_item(self._make_button(
                "Oldest ▶|", target_idx=n - 1, disabled=at_oldest))
        nav.add_item(self._make_button(
            "Close", style=discord.ButtonStyle.danger, close=True))
        self.add_item(nav)

    async def _send_thread(self, webhook: discord.Webhook) -> None:
        """Push lead messages first (older end of the thread), then the
        anchor (newest, with buttons). All IDs collected into _thread_ids."""
        for v in self._build_lead_views():
            try:
                msg = await webhook.send(view=v, ephemeral=False, wait=True)
                self._thread_ids.append(msg.id)
            except Exception as e:
                logger.warning(f"/summary thread: lead send failed: {type(e).__name__}: {e}")
        try:
            anchor_msg = await webhook.send(view=self, ephemeral=False, wait=True)
            self._thread_ids.append(anchor_msg.id)
        except Exception as e:
            logger.error(f"/summary thread: anchor send failed: {type(e).__name__}: {e}")

    async def send_initial(self, interaction: discord.Interaction) -> None:
        """First-time dispatch from the slash command. Stashes the webhook
        for later navigation/close calls."""
        self._webhook = interaction.followup
        await self._send_thread(interaction.followup)


@bot.tree.command(
    name="summary",
    description="Latest hourly radio summary, with arrows to walk back up to 24 h",
)
@app_commands.describe(
    date="Optional date YYYY-MM-DD — view every hourly summary on that day instead.",
)
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=True)
async def summary_command(
    interaction: discord.Interaction,
    date: Optional[str] = None,
):
    date = (date or '').strip() or None
    if date:
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            await interaction.response.send_message(
                "❌ Date must be `YYYY-MM-DD` (e.g. `2026-04-26`).",
                ephemeral=True,
            )
            return

    await interaction.response.defer(ephemeral=False)

    summaries = await _summary_fetch(date=date)
    if summaries is None:
        await interaction.followup.send(
            "❌ Backend error fetching summaries. Try again shortly.",
            ephemeral=True,
        )
        return
    if not summaries:
        scope = f"on {date}" if date else "in the last 24 hours"
        await interaction.followup.send(
            f"No hourly summaries available {scope}.",
            ephemeral=True,
        )
        return

    pager = SummaryPager(interaction.user.id, summaries, date=date)
    await pager.send_initial(interaction)


@bot.tree.command(name="overview", description="Dashboard of current incidents across NSW")
# User-install support: /overview works in guilds, DMs, group DMs, and any
# channel the invoking user has access to (via user-install). Output is
# public — it's a broadcast snapshot, no reason to hide it.
# Renamed from /summary so /summary can be the radio-summary navigator.
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=True)
async def overview_command(interaction: discord.Interaction):
    """Show a dashboard of current incidents and stats, rendered as
    Components V2 containers (one colour-accented section per category)
    rather than a single packed embed."""
    await interaction.response.defer()

    import aiohttp

    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'User-Agent': 'NSWPSNBot/1.0',
        'X-Client-Type': 'discord-bot'
    }

    # Fetch the aggregated counts in one shot from /api/stats/summary.
    # Payload shape (keys consumed by build_summary_components):
    #   power.endeavour.{current,future}
    #   power.ausgrid.{outages,customersAffected}
    #   traffic.{crashes,hazards,breakdowns,incidents,roadwork,fires,floods,major_events}
    #   emergency.{rfs_incidents, bom_warnings.{land,marine}}
    stats = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/api/stats/summary", headers=headers, timeout=15) as resp:
                if resp.status == 200:
                    stats = await resp.json()
                else:
                    logger.warning(f"/api/stats/summary returned {resp.status}")
    except Exception as e:
        logger.error(f"Error fetching stats summary: {e}")
        await interaction.followup.send(
            f"❌ Couldn't fetch the incident summary: `{type(e).__name__}`",
            ephemeral=True,
        )
        return

    if not stats:
        await interaction.followup.send(
            "❌ Couldn't fetch the incident summary — the stats API returned no data.",
            ephemeral=True,
        )
        return

    # Pager rolling counts — four calls to /api/pager/hits, one per window.
    # Any failure here is non-fatal; we just skip the pager section rather
    # than aborting the whole command.
    pager_counts: Dict[str, int] = {'1h': 0, '6h': 0, '12h': 0, '24h': 0}
    pager_ok = False
    try:
        async with aiohttp.ClientSession() as session:
            for hours_key, hours_val in [('24h', 24), ('12h', 12), ('6h', 6), ('1h', 1)]:
                url = f"{API_BASE_URL}/api/pager/hits"
                params = {'hours': str(hours_val), 'limit': '2000'}
                async with session.get(url, headers=headers, params=params, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if isinstance(data, dict):
                            pager_counts[hours_key] = int(data.get('count') or 0)
                            pager_ok = True
                    else:
                        logger.warning(f"API pager query returned {resp.status} for {hours_key}")
    except Exception as e:
        logger.error(f"Error fetching pager counts: {e}")

    # Build Components V2 containers and split across messages if the
    # aggregate char budget overflows — same pattern /dev-test radio_summary
    # uses. followup.send (interaction webhook) rather than channel.send so
    # the command still works in user-install / DM contexts where the bot
    # has no member presence.
    from embeds import chunk_containers_for_message
    containers = bot.embed_builder.build_summary_components(
        stats, pager_counts=pager_counts if pager_ok else None,
    )
    groups = chunk_containers_for_message(containers)
    for group in groups:
        view = discord.ui.LayoutView(timeout=None)
        for container in group:
            view.add_item(container)
        await interaction.followup.send(view=view, ephemeral=False)


# ==================== MAIN ====================

def main():
    if not DISCORD_TOKEN:
        logger.error("DISCORD_BOT_TOKEN environment variable not set!")
        logger.error("Please create a .env file with DISCORD_BOT_TOKEN=your_token_here")
        return
    
    logger.info("Starting NSW PSN Alert Bot...")
    bot.run(DISCORD_TOKEN)


if __name__ == '__main__':
    main()

