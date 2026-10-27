#!/usr/bin/env python3
"""
AusAware Alert Discord Bot
Provides real-time alerts for emergency services, BOM warnings, traffic incidents, and pager messages.
"""

import os
import io
import re
import time
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

import database
from database import Database
from alert_poller import AlertPoller
from embeds import (
    EmbedBuilder, build_staff_notify_embed, STAFF_NOTIFY_KINDS,
    FEEDBACK_KINDS, build_feedback_notice_embed, build_feedback_notice_view,
    build_feedback_reply_embed, build_feedback_board_embed,
    set_feedback_board_status, build_feedback_status_embed,
)
import alert_catalog

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


def safe_add_containers(view, group) -> int:
    """Add each container of a chunked group to a LayoutView, skipping any
    that discord.py rejects (ValueError('maximum number of children
    exceeded') when a container slipped past the chunker's component
    budget — e.g. truncation couldn't shrink it enough). One over-budget
    container must degrade to "that container missing", never to an
    exception that aborts the whole dispatch cycle: by that point the
    poller has already marked every alert in the batch as seen, so an
    abort silently loses them all. Returns how many containers were added.
    """
    added = 0
    for container in group:
        try:
            view.add_item(container)
            added += 1
        except Exception:
            logger.exception("Container rejected by LayoutView — skipping it")
    return added

# Alert types — from shared/alert-catalog.json via alert_catalog.py.
# This was a hand-written dict kept "in sync" with the dashboard and the
# backend whitelist by hand; it drifted, and the backend started rejecting
# keys the dashboard offered. All three now read the one file.
ALERT_TYPES = alert_catalog.LABELS

# Alert types a catch-all selection must never sweep in — they only arrive by
# being ticked deliberately. FIRMS is a satellite hotspot feed, not an incident
# feed: on 2026-10-12 one poll queued 2,577 hotspots for a single channel
# through a "select everything" preset, filled the send queue and left every
# other channel's alerts up to 75 minutes behind. The type stays in the
# per-source picker, so it can still be chosen on purpose; what changes is that
# "Enable All" and a bare /alert no longer choose it for anyone. The other half
# of the rule — an unscoped hotspot subscription is not honoured at all — lives
# in Database.get_presets_for_alert_type.
# Shark reports join it for the same reason at a smaller scale: roughly 25 a
# day nationally, 56% of which are a tagged shark swimming past a listening
# station. That is a feed somebody chooses, not one a "select everything"
# preset should pour into a channel that wanted fires and outages.
_OPT_IN_ONLY_TYPES = {'firms', 'shark_report'}

# Alert types shown in the generic /setup alerts dropdown + "Enable All".
# Radio summary opts out in the catalog (generalPicker:false) because it has
# its own dedicated setup flow (see SetupRadioSummarySubmenuView).
_GENERAL_ALERT_TYPES = {
    k: v for k, v in alert_catalog.general_picker_types().items()
    if k not in _OPT_IN_ONLY_TYPES
}

# Discord hard-caps a slash-command choice list AND a select menu at 25
# options (and max_values at 25). ALERT_TYPES passed that when the
# interstate sources were added, which failed command sync and crash-looped
# the bot. Anything built from ALERT_TYPES must chunk or autocomplete.
DISCORD_SELECT_MAX = 25

# Pager is not an alert type — it runs off the pager_enabled column — but the
# roles submenu assigns roles to it alongside them, so it gets a pseudo-provider.
PAGER_PROVIDER_KEY = '__pager__'


def _chunk_options(options):
    """Split into runs of at most DISCORD_SELECT_MAX, never an empty list."""
    if not options:
        return [[]]
    return [options[i:i + DISCORD_SELECT_MAX]
            for i in range(0, len(options), DISCORD_SELECT_MAX)]


def _merge_select_values(selects, interaction):
    """Union the picks across chunked selects.

    Discord only reports values for the menu that actually fired; the others
    arrive empty. Reading those back from their `default` flags is what stops
    the view forgetting a selection made in a different chunk - while still
    honouring a deliberate clear-out of the menu that did fire.
    """
    fired = (interaction.data or {}).get('custom_id')
    picked = []
    for sel in selects:
        if sel.custom_id == fired:
            picked.extend(sel.values)
        else:
            picked.extend(o.value for o in sel.options if o.default)
    return list(dict.fromkeys(picked))

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
# Per-alert-type severity scales, from shared/alert-catalog.json. These were
# three hand-written dicts (the scales plus an RFS and a BOM raw-value map)
# that listed only the original NSW types, so every interstate agency carried
# an alertLevel the floor filter could not read.
_SEVERITY_SCALES = {
    k: alert_catalog.severity_tokens(k)
    for k in alert_catalog.LABELS
    if alert_catalog.severity_tokens(k)
}

# Every alert type whose records arrive as a normalised emergency-service
# GeoJSON Feature — title/status/location under `properties`, a point or
# polygon under `geometry`. Derived from the catalog's feedShape, because the
# hand-written tuples this replaces predated vicses, emv, esta and
# nt_bushfires and the qld_warning→qfd_warning / wa_warning→dfes_warning
# renames: those six types fell through to the generic branches, so a
# keywords_include preset dropped every one of their alerts, keywords_exclude
# excluded nothing, and a geofilter passed them whatever their location.
_FEATURE_ALERT_TYPES = alert_catalog.keys_with_shape(
    'geo_fire', 'geo_fire_warning', 'geo_act')

# The above plus the traffic feeds, which are the same GeoJSON shape but carry
# no severity or agency fields and so read differently for keywords.
#
# geo_point (shark reports) MUST be in here, and it is not cosmetic:
# preset_has_geoscope only checks that a geofilter key exists, so a
# scoped-only type whose coordinates cannot be read would satisfy the gate
# and then pass every geofilter unconditionally — the opposite of what
# requiring a region is for.
_GEO_POINT_ALERT_TYPES = _FEATURE_ALERT_TYPES | alert_catalog.keys_with_shape(
    'geo_traffic', 'geo_point')


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

    # A preset saved before a rename still holds the retired spelling, so fold
    # to the canonical key before testing membership.
    canon = alert_catalog.canonical(alert_type) or alert_type

    if canon in _FEATURE_ALERT_TYPES or canon == 'user_incident':
        props = alert_data.get('properties') if canon != 'user_incident' else None
        # RFS: properties.{title,description,location,councilArea,fireType,status}
        if isinstance(props, dict):
            for k in ('title', 'description', 'location', 'location_text',
                      'councilArea', 'fireType', 'status', 'alertLevel'):
                _push(props.get(k))
        else:
            for k in ('title', 'description', 'location'):
                _push(alert_data.get(k))
    elif canon and canon.startswith('bom_'):
        for k in ('title', 'description', 'area', 'category', 'severity'):
            _push(alert_data.get(k))
    elif canon and canon.startswith('traffic_'):
        props = alert_data.get('properties') or {}
        for k in ('title', 'headline', 'displayName', 'subtitle', 'roads',
                  'incidentType', 'otherAdvice', 'adviceA', 'adviceB',
                  'affectedDirection'):
            _push(props.get(k))
    elif canon == 'shark_report':
        # GeoJSON like the feature types, but its own field names — falling
        # through to the generic branch would flatten the geometry dict and
        # match nothing a keyword filter could usefully name.
        props = alert_data.get('properties') or {}
        for k in ('title', 'species', 'encounter', 'category', 'location',
                  'zone', 'state', 'provenance', 'comment'):
            _push(props.get(k))
    elif canon in ('wire_article', 'wire_fleet'):
        for k in ('title', 'excerpt', 'callsign', 'agency', 'station',
                  'state', 'lga', 'suburb'):
            _push(alert_data.get(k))
        for a in (alert_data.get('agencies') or []):
            _push(a)
    elif canon and (canon.startswith('endeavour_')
                    or canon == 'ausgrid'
                    or canon.startswith('essential_')):
        for k in ('suburb', 'Suburb', 'streets', 'cause', 'status',
                  'outageType'):
            _push(alert_data.get(k))
    elif canon == 'pager':
        # alert_data here is the pager msg dict.
        for k in ('capcode', 'type', 'category', 'alias', 'agency',
                  'address', 'suburb', 'council', 'incident_id', 'raw'):
            _push(alert_data.get(k))
    elif canon == 'radio_summary':
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

    # Fold a retired spelling to its canonical key before branching — see
    # _alert_text_haystack.
    canon = alert_catalog.canonical(alert_type) or alert_type or ''

    # GeoJSON-style geometry.coordinates = [lng, lat] for rfs, traffic_*
    # and the interstate fire feeds. The prefix test stays as the safety net
    # for a traffic key that is live before it reaches the catalog.
    if canon in _GEO_POINT_ALERT_TYPES or canon.startswith('traffic_'):
        geom = alert_data.get('geometry') or {}
        coords = geom.get('coordinates') if isinstance(geom, dict) else None
        if isinstance(coords, (list, tuple)) and len(coords) >= 1:
            # LineString: list of [lng,lat] pairs — take the midpoint so the
            # geofilter has a real point to test. Polygon (NT warnings):
            # rings nest one level deeper — keep unwrapping until we hold a
            # bare [lng, lat] pair.
            while (isinstance(coords, (list, tuple)) and coords
                   and isinstance(coords[0], (list, tuple))):
                coords = coords[len(coords) // 2]
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                try:
                    return (float(coords[1]), float(coords[0]))
                except (TypeError, ValueError):
                    return (None, None)
        return (None, None)

    # FIRMS hotspots: coords live in properties.latitude/longitude, with a
    # snapped cluster_glat/glon fallback set by the clusterer.
    if canon == 'firms':
        props = alert_data.get('properties') or {}
        lat = props.get('latitude')
        lng = props.get('longitude')
        if lat is None:
            lat = props.get('cluster_glat')
        if lng is None:
            lng = props.get('cluster_glon')
        try:
            if lat is not None and lng is not None:
                return (float(lat), float(lng))
        except (TypeError, ValueError):
            pass
        return (None, None)

    # User incidents store lat/lng at top level.
    if canon == 'user_incident':
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
    if (canon.startswith('endeavour_')
            or canon == 'ausgrid'
            or canon.startswith('essential_')):
        lat = alert_data.get('latitude') or alert_data.get('Latitude')
        lng = alert_data.get('longitude') or alert_data.get('Longitude')
        try:
            if lat is not None and lng is not None:
                return (float(lat), float(lng))
        except (TypeError, ValueError):
            pass
        return (None, None)

    # Pager messages nest under coordinates.{lat,lon}.
    if canon == 'pager':
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

    Each source uses a different field name. These MUST line up with the
    catalogue dim the dashboard draws its subtype chips from (see
    dashboard.html `_SUBTYPE_FIELDS_BY_TYPE`) and with what the backend
    archives — otherwise a chosen chip never matches a token and silently
    filters out every alert.
      rfs              → properties.fireType   ("Bush Fire", "Grass Fire", "MVA/Transport", ...)
      bom_*            → warningType           ("Flood", "Severe Weather", "Wind", ...)
      traffic_*        → properties.incidentType  ("CRASH", "BREAKDOWN", "BUSHFIRE", ...)
      waze_*           → properties.wazeSubtype, else properties.wazeType
                         ("HAZARD_WEATHER_FOG", "POLICE_HIDING", "ACCIDENT_MAJOR",
                         or the bare type "ACCIDENT" when there's no subtype)
      user_incident    → category
    For unsupported types, returns None — subtype_filters has no effect on them.
    """
    if not isinstance(alert_data, dict):
        return None
    if alert_type == 'rfs':
        # fireType is the incident sub-classification; the RFS feed has no
        # `category`. (alertLevel — Advice/Watch & Act/Emergency — is the
        # separate severity dimension, handled by alert_passes_severity.)
        props = alert_data.get('properties') or {}
        return (str(props.get('fireType') or '').strip()) or None
    if alert_type and alert_type.startswith('bom_'):
        # warningType is the title-derived classification the backend now
        # exposes on each warning. `category` is only "land"/"marine"
        # (already split into bom_land/bom_marine) so it's no use here.
        props = alert_data.get('properties') or {}
        raw = (alert_data.get('warningType') or props.get('warningType'))
        return (str(raw or '').strip()) or None
    if alert_type and alert_type.startswith('traffic_'):
        props = alert_data.get('properties') or {}
        return (str(props.get('incidentType') or '').strip()) or None
    if alert_type and alert_type.startswith('waze_'):
        # The poller passes the GeoJSON feature, whose raw Waze type/subtype
        # live under properties.wazeType / properties.wazeSubtype — the
        # feature's top-level `type` is the literal "Feature" and there is no
        # top-level `subtype`. Mirror the ingest's
        # `subcategory = rawSubtype || rawType` (api/waze-ingest.ts) so the
        # token matches the catalogue values the dashboard whitelists
        # (ACCIDENT, ACCIDENT_MAJOR, POLICE_VISIBLE, HAZARD_ON_ROAD_POT_HOLE…).
        props = alert_data.get('properties') or {}
        raw = props.get('wazeSubtype') or props.get('wazeType')
        if not raw:
            # Fallback for a raw-alert shape (top-level type/subtype); never
            # emit the GeoJSON literal "Feature" as a token.
            t = alert_data.get('type')
            raw = alert_data.get('subtype') or (t if t != 'Feature' else None)
        return (str(raw or '').strip()) or None
    if alert_type == 'user_incident':
        raw = alert_data.get('category') or alert_data.get('type')
        return (str(raw or '').strip()) or None
    if alert_type == 'shark_report':
        # The normalised report category — sighting / detection / incident /
        # closure / advisory / tagging / carcass. This is the dimension worth
        # filtering on: species is interesting, but "only tell me about bites
        # and closures" is the subscription people actually want.
        props = alert_data.get('properties') or {}
        return (str(props.get('category') or '').strip()) or None
    return None


def _alert_severity_token(alert_type: str, alert_data: dict):
    """Map a record's raw severity onto a token in its type's scale.

    Where to read it and how to map it both come from the catalog, so a newly
    added agency inherits its scale instead of needing a branch here. None
    means "no severity available" and the floor passes through.
    """
    return alert_catalog.severity_token(alert_type, alert_data)


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
    if actual == '__below__':
        # The catalog maps this raw value BELOW the whole scale (e.g. RFS
        # "Not Applicable") — any configured floor filters it out. Distinct
        # from None: absent severity passes, sub-scale severity does not.
        return False
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
        # Pass-through when the alert has no coordinates — same policy as
        # the severity/subtype gates above. A geofilter can't meaningfully
        # test a location-less alert (e.g. state-wide BOM warnings, which
        # have no point geometry), so don't silently drop every such alert
        # just because a region was set on the preset.
        if lat is not None and lng is not None and not _geofilter_contains(gf, lat, lng):
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


_FEEDBACK_TITLE_RE = re.compile(r'^#{1,3}\s+(\S.*)$')


def split_feedback_message(text: str):
    """A bug report or suggestion posted in Discord: a '#' title line, then
    the details. Returns (title, details); title is None when the first
    non-blank line is not a heading, or the heading is too short to be a
    subject (the backend wants three characters)."""
    lines = [ln.rstrip() for ln in (text or '').strip().splitlines()]
    while lines and not lines[0].strip():
        lines.pop(0)
    if not lines:
        return None, ''
    m = _FEEDBACK_TITLE_RE.match(lines[0].strip())
    if not m:
        return None, (text or '').strip()
    title = m.group(1).strip().strip('#').strip()
    if len(title) < 3:
        return None, (text or '').strip()
    details = '\n'.join(lines[1:]).strip()
    return title, details


class NSWPSNBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        
        super().__init__(
            command_prefix='!',
            intents=intents,
            help_command=None,
            # Nothing the bot relays may ping on its own. Alert text is
            # third-party copy — a feed title, an editor's incident
            # description, a relayed pager address — and it lands in a
            # TextDisplay, which Discord parses for mentions, so an
            # "@everyone" typed into an incident would have mass-pinged every
            # subscribed guild the bot can do that in. The bot's OWN role
            # pings are re-granted per send, naming only the roles the
            # matching presets asked for (see _send_one_message).
            allowed_mentions=discord.AllowedMentions(
                everyone=False, users=False, roles=False, replied_user=False),
        )

        self.db = Database()
        self.poller = AlertPoller(API_BASE_URL, API_KEY, self.db)
        self.embed_builder = EmbedBuilder()
        
        # Rate limiting for Discord API. The queue is a large burst buffer so
        # a busy Waze cycle (hundreds of messages across many channels) is
        # never dropped — it drains over subsequent ticks. Per-tick we drain a
        # big batch and fan it out per channel concurrently; discord.py
        # enforces the actual per-channel + global rate limits, so this can't
        # exceed Discord's caps.
        self.message_queue = asyncio.Queue(maxsize=5000)
        self.rate_limit_delay = 0.5  # legacy; pacing is now handled by discord.py
        self.max_messages_per_batch = 100  # messages drained per tick (fanned out per channel)
        # How much of one batch a single alert_type may claim while another
        # type is waiting. A FIRMS burst used to own every slot in the batch
        # because the drain was pure FIFO; see _drain_fair_share.
        self.max_batch_share_per_type = 25
        # Rotates the round-robin's starting channel, so the tail of the
        # channel list can't starve if there are ever more channels than one
        # batch covers.
        self._fair_share_cursor = 0

        # Track startup time - don't remove guild configs within first 60 seconds
        # This prevents race conditions during startup where guilds may briefly appear disconnected
        self._startup_time = datetime.now()
        self._min_uptime_for_guild_remove = 60  # seconds

        # Bug reports and suggestions (see _file_feedback): which channels each
        # kind uses, from feedback_channels at startup and the feedback_channels
        # action after that. _feedback_intake is the reverse map on_message
        # needs: intake channel id -> kind.
        self._feedback_channels: Dict[str, Dict[str, Any]] = {}
        self._feedback_intake: Dict[int, str] = {}
        # Per-user: when they last filed, for a short cooldown. The real cap
        # (five open reports) is the backend's.
        self._feedback_last_post: Dict[int, float] = {}
        self._feedback_unsigned_notice = False
        self._feedback_notices_checked = False
        
        # Track permission errors to suppress repeated warnings (debounce).
        # Entries are swept by `_record_send_error` whenever the dict is
        # touched, so this can't grow unbounded across long bot uptime even
        # as channels get deleted/re-added.
        self._permission_error_channels: Dict[int, datetime] = {}
        self._permission_error_debounce_seconds = 300  # 5 minutes

        # Set once the "no signing secret, refusing admin actions" explainer
        # has been logged, so a misconfigured deployment says so loudly once
        # instead of on every queued action.
        self._unsigned_action_notice = False

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

    def _describe_channel(self, channel, channel_id: int,
                          config_id: Optional[int] = None) -> str:
        """Human-readable '#channel in "Guild" (owner: …, channel id)' for
        log messages.

        Falls back progressively when names aren't available (e.g. a
        deleted channel on a 404): channel arg → cache lookup → preset's
        guild (via config_id — a deleted channel is gone from the cache,
        but the guild it lived in usually is not) → bare id. The owner
        name needs the member cached; otherwise we show the owner id,
        which the guild always carries.
        """
        ch = channel if channel is not None else self.get_channel(channel_id)
        if ch is not None:
            ch_name = getattr(ch, 'name', None)
            guild = getattr(ch, 'guild', None)
            guild_name = getattr(guild, 'name', None)
            if ch_name and guild_name:
                owner = getattr(guild, 'owner', None)
                owner_id = getattr(guild, 'owner_id', None)
                owner_name = getattr(owner, 'name', None) or getattr(owner, 'display_name', None)
                if owner_name and owner_id:
                    owner_str = f'{owner_name} ({owner_id})'
                elif owner_name:
                    owner_str = owner_name
                elif owner_id:
                    owner_str = f'id {owner_id}'
                else:
                    owner_str = 'unknown'
                return f'#{ch_name} in "{guild_name}" (owner: {owner_str}, channel {channel_id})'
            if ch_name:
                return f'#{ch_name} ({channel_id})'
        # Channel gone from the cache — name the SERVER via the preset so the
        # operator knows where to look without pasting ids into Discord.
        if config_id is not None:
            try:
                preset = self.db.get_preset(int(config_id))
                gid = int(preset.get('guild_id') or 0) if preset else 0
                if gid:
                    gname = getattr(self.get_guild(gid), 'name', None)
                    if gname:
                        return f'channel {channel_id} in "{gname}" ({gid})'
                    return f'channel {channel_id} in guild {gid}'
            except Exception:
                pass
        return f'channel {channel_id}'

    async def setup_hook(self):
        """Called when the bot is starting up"""
        logger.info("Setting up bot...")
        logger.info(f"Database path: {self.db.db_path}")
        self.db.init_db()
        try:
            self._load_feedback_channels()
        except Exception as e:
            # Without the map nothing is filed from Discord; the next
            # feedback_channels action (any picker change) rebuilds it.
            logger.warning(f"feedback channels: load failed: {e}")

        # Start background tasks
        self.poll_alerts.start()
        self.poll_pager.start()
        self.process_message_queue.start()
        self.drain_bot_actions.start()
        self.run_retention.start()
        
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
                name="Australian Emergency Alerts"
            )
        )

        # The how-tos in the intake channels: bring them up to date
        # with this build's wording (once per process — on_ready fires again
        # on every reconnect).
        if not self._feedback_notices_checked:
            self._feedback_notices_checked = True
            try:
                await self._refresh_feedback_notices()
            except Exception as e:
                logger.warning(f"feedback notices: refresh failed: {e}")
    
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
    
    def _requeue_message(self, item):
        """Re-queue an item without blocking. The send loop is the queue's
        only consumer, so `await put()` on a full queue would deadlock; use
        put_nowait and drop the oldest to make room rather than lose this one."""
        try:
            self.message_queue.put_nowait(item)
        except asyncio.QueueFull:
            try:
                self.message_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self.message_queue.put_nowait(item)
            except asyncio.QueueFull:
                pass

    async def _send_one_message(self, item):
        """Send a single queued message. Returns 'sent', 'skip', or
        'ratelimited' (the channel should re-queue this item and pause)."""
        channel_id = item['channel_id']
        view = item.get('view')
        embeds = None
        if not view:
            # `embeds` is canonical; fall back to the legacy single `embed`.
            embeds = item.get('embeds') or ([item['embed']] if item.get('embed') else [])
            embeds = embeds[:10]  # Discord hard limit
        content = item.get('content')
        config_id = item.get('config_id')
        channel = None
        try:
            channel = self.get_channel(channel_id)
            if not channel:
                channel = await self.fetch_channel(channel_id)
            if not channel:
                return 'skip'
            # An update replies to the message that first reported the
            # incident. fail_if_not_exists=False is not optional here: the
            # original is routinely gone — deleted by a moderator, or purged
            # past the 14-day incident_messages retention — and without it
            # Discord rejects the whole send and the update is simply lost.
            reference = None
            reply_to = item.get('reply_to_message_id')
            if reply_to:
                try:
                    reference = discord.MessageReference(
                        message_id=int(reply_to),
                        channel_id=int(channel_id),
                        fail_if_not_exists=False,
                    )
                except (TypeError, ValueError):
                    reference = None

            # The bot defaults to mentioning nothing (see NSWPSNBot.__init__),
            # so a message that is MEANT to ping has to name its roles here.
            # Only the roles the matching presets asked for are listed, which
            # is also what keeps an "@everyone" in relayed text inert.
            ping_ids = item.get('ping_role_ids') or []
            if ping_ids:
                mentions = discord.AllowedMentions(
                    everyone=False, users=False, replied_user=False,
                    roles=[discord.Object(id=int(r)) for r in ping_ids],
                )
            else:
                mentions = discord.AllowedMentions(
                    everyone=False, users=False, roles=False,
                    replied_user=False)

            if view is not None:
                # Components V2 (LayoutView) forbids `content` — role mentions
                # live inside a TextDisplay in the view instead.
                message = await channel.send(view=view, reference=reference,
                                             allowed_mentions=mentions)
            else:
                message = await channel.send(content=content, embeds=embeds,
                                             reference=reference,
                                             allowed_mentions=mentions)

            alert_type = item.get('alert_type')
            if alert_type:
                logger.debug(f"✅ Sent {alert_type} to #{getattr(channel, 'name', '?')}")

            # Save message URL for incident tracking (RFS / user_incident).
            # Run the DB commit off-thread so a slow commit doesn't block
            # the gateway heartbeat.
            incident_guid = item.get('incident_guid')
            if incident_guid and message:
                status = item.get('incident_status')
                message_url = message.jump_url
                message_id = message.id
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self.db.save_incident_message(
                        incident_guid=incident_guid,
                        channel_id=channel_id,
                        message_url=message_url,
                        status=status,
                        message_id=message_id,
                    ),
                )
            return 'sent'
        except discord.NotFound:
            # Channel deleted / inaccessible. Debounce-log; don't auto-remove
            # (the preset may cover many alert types). Dashboard cleanup path.
            if self._record_send_error(channel_id):
                logger.warning(
                    f"{self._describe_channel(channel, channel_id, config_id)} returned 404 "
                    f"(preset {config_id}) — channel may be deleted or inaccessible. "
                    f"Not auto-removing; delete the preset from the dashboard if permanent."
                )
            return 'skip'
        except discord.Forbidden:
            if self._record_send_error(channel_id):
                logger.warning(
                    f"No permission to send to {self._describe_channel(channel, channel_id, config_id)}"
                )
            return 'skip'
        except discord.HTTPException as e:
            if getattr(e, 'status', None) == 429:
                # Backstop — discord.py normally waits out rate limits itself.
                # Honour Discord's retry_after, then have the channel re-queue.
                retry_after = getattr(e, 'retry_after', None)
                wait = retry_after if isinstance(retry_after, (int, float)) and retry_after > 0 else 5
                logger.warning(f"Rate limited (429) on channel {channel_id}; backing off {wait:.1f}s")
                await asyncio.sleep(wait)
                return 'ratelimited'
            logger.error(f"HTTP error sending message: {e}")
            return 'skip'
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return 'skip'

    async def _send_channel_batch(self, channel_id, items):
        """Send one channel's queued items in order. discord.py paces this
        channel's bucket; on a 429 we re-queue the remainder and stop so the
        channel resumes next tick. Returns the count actually sent."""
        sent = 0
        for idx, item in enumerate(items):
            status = await self._send_one_message(item)
            if status == 'sent':
                sent += 1
            elif status == 'ratelimited':
                for remaining in items[idx:]:
                    self._requeue_message(remaining)
                break
            # 'skip' — drop this one, keep going with the channel
        return sent

    async def close(self):
        """Tidy shutdown: close the poller's shared HTTP session before the
        gateway tears down so aiohttp doesn't warn about a leaked session /
        connector, then defer to the base class."""
        try:
            await self.poller.close()
        except Exception as e:
            logger.error(f"Error closing poller session: {e}")
        await super().close()

    def _drain_fair_share(self):
        """Build this tick's batch ROUND-ROBIN across channels.

        The queue is FIFO and the batch used to be its first 100 items, so
        whichever channel sat at the front owned the whole batch. On
        2026-10-12 one poll queued 2,577 FIRMS hotspots for a single channel:
        every other channel's alerts — RFS, CFA, ambulance, 47 channels of
        them — waited behind that backlog for up to 75 minutes, and 1,996
        messages were eventually dropped as the queue overflowed.

        So the queue is drained in full, bucketed per channel, and the batch
        filled one item per channel per pass. A single alert_type may take
        only `max_batch_share_per_type` of a batch while another type still
        has items waiting; once nothing else can use the slots, the rest of
        the batch is topped up from that type anyway, because a cap that
        idles the sender buys nobody anything.

        Whatever doesn't fit goes straight back on the queue, in per-channel
        order: the queue stays the single backlog, so its size still reads
        true in the log lines and `queue_message`'s drop-oldest valve still
        applies. There is no await in here, so the drain and the re-queue are
        atomic against the poll loops.
        """
        pending: Dict[int, List[Dict[str, Any]]] = {}
        order: List[int] = []
        while not self.message_queue.empty():
            try:
                item = self.message_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            cid = item['channel_id']
            if cid not in pending:
                pending[cid] = []
                order.append(cid)
            pending[cid].append(item)
        if not order:
            return []

        start = self._fair_share_cursor % len(order)
        self._fair_share_cursor += 1
        order = order[start:] + order[:start]

        limit = self.max_messages_per_batch
        type_cap = max(1, int(self.max_batch_share_per_type))
        cursor = {cid: 0 for cid in order}
        per_type: Dict[str, int] = {}
        batch: List[Dict[str, Any]] = []
        held_by_cap = False

        def _fill(respect_cap: bool):
            nonlocal held_by_cap
            progressed = True
            while len(batch) < limit and progressed:
                progressed = False
                for cid in order:
                    if len(batch) >= limit:
                        break
                    idx = cursor[cid]
                    items = pending[cid]
                    if idx >= len(items):
                        continue
                    item = items[idx]
                    atype = item.get('alert_type') or 'alert'
                    if respect_cap and per_type.get(atype, 0) >= type_cap:
                        # Leave the cursor where it is — a channel's items
                        # keep their poll order, so this type's next item
                        # blocks the ones behind it rather than being skipped.
                        held_by_cap = True
                        continue
                    cursor[cid] = idx + 1
                    per_type[atype] = per_type.get(atype, 0) + 1
                    batch.append(item)
                    progressed = True

        _fill(respect_cap=True)
        if held_by_cap and len(batch) < limit:
            _fill(respect_cap=False)

        for cid in order:
            for item in pending[cid][cursor[cid]:]:
                self._requeue_message(item)
        return batch

    @tasks.loop(seconds=1)
    async def process_message_queue(self):
        """Drain a batch from the queue and send it, fanning out per channel
        CONCURRENTLY. A single slow/rate-limited channel no longer blocks the
        others, which is what let a Waze burst overflow the queue. discord.py
        still enforces per-channel + global rate limits, so concurrency can't
        exceed Discord's caps — it just keeps every channel making progress."""
        # Outer guard: a @tasks.loop that raises an unhandled exception STOPS
        # and is not auto-restarted. The send fan-out is already shielded by
        # return_exceptions=True, but the drain / by_channel setup is not —
        # wrap the whole body so a transient error can't silently kill the
        # queue drainer (mirrors poll_alerts / poll_pager / drain_bot_actions).
        try:
            batch = self._drain_fair_share()
            if not batch:
                return

            by_channel = {}
            for item in batch:
                by_channel.setdefault(item['channel_id'], []).append(item)

            results = await asyncio.gather(
                *[self._send_channel_batch(cid, items) for cid, items in by_channel.items()],
                return_exceptions=True,
            )
            sent = 0
            for r in results:
                if isinstance(r, int):
                    sent += r
                elif isinstance(r, Exception):
                    logger.error(f"Channel batch error: {r}")
            if sent:
                logger.info(f"Sent {sent} messages across {len(by_channel)} channel(s) "
                            f"(queue size: {self.message_queue.qsize()})")
        except Exception as e:
            logger.error(f"process_message_queue cycle error: {e}", exc_info=True)
    
    @process_message_queue.before_loop
    async def before_process_queue(self):
        await self.wait_until_ready()

    @tasks.loop(hours=24)
    async def run_retention(self):
        """Apply the documented retention windows to the bot's own database.

        Every cleanup_* method below existed and none of them was called from
        anywhere: the only historical caller was the /dev clear-seen command,
        removed when the dashboard took over. So seen_alerts (every FIRMS
        cluster of every pass, every traffic and outage id), seen_pager,
        incident_messages, preset_fire_log, pending_bot_actions and
        staff_notify_messages had grown since the bot was first deployed,
        while the code comments and the backend's dashboard queries assumed a
        7/14/30-day purge was happening.

        The windows are each method's own default. Every step is guarded
        individually: a failure on one table must not skip the rest, and must
        not stop the loop — nothing restarts it.
        """
        loop = asyncio.get_event_loop()
        steps = (
            ('seen_alerts + seen_pager', self.db.cleanup_old_seen),
            ('incident_messages', self.db.cleanup_old_incident_messages),
            ('preset_fire_log', self.db.cleanup_preset_fire_log),
            ('pending_bot_actions', self.db.cleanup_old_bot_actions),
            ('staff_notify_messages', self.db.cleanup_old_staff_messages),
        )
        done = []
        for label, fn in steps:
            try:
                await loop.run_in_executor(None, fn)
                done.append(label)
            except Exception as e:
                logger.warning(f"retention: {label} cleanup failed: {e}")
        logger.info(f"🧹 Retention pass complete — purged {', '.join(done) or 'nothing'}")

    @run_retention.before_loop
    async def before_run_retention(self):
        await self.wait_until_ready()

    # ------------------------------------------------------------------
    # Admin action queue drain — runs every 10s, picks up rows written by
    # the dashboard (sync / test / cleanup) and executes them.
    # ------------------------------------------------------------------
    def _reclaim_stale_bot_actions(self):
        """Return actions stuck in 'running' (claimed but never completed —
        e.g. the bot crashed or threw mid-action) back to 'pending' so they
        can be re-claimed. claim_next_bot_action() only selects 'pending', so
        without this a stranded row never drains again.

        Implemented at the call site because database.py has no reclaim method
        and must not be edited; uses its existing connection helper + flag.
        """
        if not database.USE_POSTGRES:
            return 0
        conn = self.db._connect()
        try:
            c = conn.cursor()
            c.execute('''
                UPDATE pending_bot_actions
                   SET status = 'pending', claimed_at = NULL
                 WHERE status = 'running'
                   AND claimed_at < now() - interval '5 minutes'
            ''')
            n = c.rowcount or 0
            conn.commit()
            return n
        finally:
            conn.close()

    async def _complete_bot_action_safely(self, action_id: int, *,
                                          result: Optional[Dict[str, Any]] = None,
                                          error: Optional[str] = None):
        """Mark a drained action finished, swallowing DB errors.

        drain_bot_actions is a @tasks.loop, and discord.ext.tasks only retries
        a handful of network errors — a psycopg2 error or a PoolError raised
        out of here would END the loop, and nothing restarts it, so staff
        notifications, broadcasts, syncs and tests would queue up in silence.
        A row we fail to complete stays 'running' and is reclaimed five
        minutes later instead.
        """
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                functools.partial(self.db.complete_bot_action, action_id,
                                  result=result, error=error),
            )
        except Exception as e:
            logger.error(
                f"bot-action drain: could not mark id={action_id} complete: {e}"
            )

    @tasks.loop(seconds=10)
    async def drain_bot_actions(self):
        # Outer guard, same reasoning as _complete_bot_action_safely: the
        # claim, the signature gate and the completion are all DB work, and an
        # unhandled error out of any of them would stop this loop for good.
        # The other three loops have carried this guard for a while; this one
        # did not, so a single Postgres blip could silence the admin queue
        # until someone noticed the dashboard had gone quiet.
        try:
            await self._drain_bot_actions_once()
        except Exception as e:
            logger.error(f"drain_bot_actions cycle error: {e}", exc_info=True)

    async def _drain_bot_actions_once(self):
        loop = asyncio.get_event_loop()
        try:
            reclaimed = await loop.run_in_executor(None, self._reclaim_stale_bot_actions)
            if reclaimed:
                logger.warning(f"drain_bot_actions: reclaimed {reclaimed} stale running action(s)")
        except Exception as e:
            logger.warning(f"drain_bot_actions reclaim failed: {e}")
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

        # --- HMAC signature gate -------------------------------------------
        # The backend signs each queue row with BOT_ACTION_SIGNING_SECRET.
        # Reject rows that didn't come from the backend so an attacker who
        # can merely INSERT into pending_bot_actions can't get unattended
        # Discord actions. With no secret configured we now fail CLOSED: a
        # queued action executes with no further authentication, so without
        # the shared secret there is nothing to tell a backend insert from
        # anyone else's. This used to fail open as a rollout accommodation;
        # the secret is deployed on both sides, so an unsigned queue is a
        # misconfiguration, not a migration state.
        signing_secret = os.getenv('BOT_ACTION_SIGNING_SECRET')
        if not signing_secret:
            if not self._unsigned_action_notice:
                self._unsigned_action_notice = True
                logger.error(
                    "BOT_ACTION_SIGNING_SECRET is not set — REFUSING every "
                    "queued bot action. Set the same value in the backend's "
                    ".env and this bot's .env (openssl rand -hex 32) and "
                    "restart both; until then the dashboard's sync, test, "
                    "broadcast and staff notifications will not run."
                )
            logger.warning(
                f"bot-action drain: REFUSED id={action_id} action={kind} "
                "— no signing secret configured"
            )
            await self._complete_bot_action_safely(
                action_id,
                error='refused: BOT_ACTION_SIGNING_SECRET is not set on the bot',
            )
            return

        ok = self.db.verify_bot_action_sig(
            signing_secret,
            action['action'],
            action.get('requested_by') or '',
            action.get('params') or {},
            action.get('sig'),
        )
        if not ok:
            logger.warning(
                f"bot-action drain: REJECTED id={action_id} action={kind} "
                "— signature verification failed (missing or bad sig)"
            )
            await self._complete_bot_action_safely(
                action_id, error='signature verification failed')
            return

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
            elif kind == 'staff_notify':
                result = await self._exec_action_staff_notify(params)
            elif kind == 'feedback_channels':
                result = await self._exec_action_feedback_channels(params)
            elif kind == 'feedback_board':
                result = await self._exec_action_feedback_board(params)
            else:
                raise ValueError(f"unknown action: {kind}")
            await self._complete_bot_action_safely(action_id, result=result)
            logger.info(f"bot-action drain: id={action_id} ok")
        except Exception as e:
            logger.exception(f"bot-action drain: id={action_id} failed")
            await self._complete_bot_action_safely(
                action_id, error=str(e)[:500])

    @drain_bot_actions.before_loop
    async def before_drain_bot_actions(self):
        await self.wait_until_ready()

    async def _exec_action_sync(self):
        synced = await self.tree.sync()
        return {'synced_global': len(synced)}

    async def _exec_action_staff_notify(self, params):
        """Post - or update - a staff moderation notification.

        Pushed by the backend when a signup request, Wire submission or
        takedown notice arrives or is actioned. On resolution we EDIT the
        message the arrival posted rather than adding a second one, which
        keeps the channel a clean worklist.

        No slash command configures this: the target channel is chosen on
        the staff page and arrives in params.
        """
        kind = (params.get('kind') or '').strip()
        if kind not in STAFF_NOTIFY_KINDS:
            raise ValueError(f'unknown staff notify kind: {kind}')
        event = (params.get('event') or 'new').strip()
        ref = (params.get('ref') or '').strip()
        try:
            gid = int(params.get('guild_id') or 0)
            cid = int(params.get('channel_id') or 0)
        except (TypeError, ValueError):
            raise ValueError('bad guild_id/channel_id')
        if not cid:
            raise ValueError('channel_id required')

        loop = asyncio.get_event_loop()
        embed = build_staff_notify_embed(kind, params)

        # Cache first, then REST. _exec_action_broadcast is cache-only,
        # which silently reports 'channel_not_found' for a channel the bot
        # hasn't touched since boot - not acceptable for moderation traffic.
        channel = self.get_channel(cid)
        if channel is None:
            try:
                channel = await self.fetch_channel(cid)
            except discord.NotFound:
                raise ValueError(f'channel_not_found (guild {gid}, channel {cid})')
            except discord.Forbidden:
                raise ValueError(f'forbidden (guild {gid}, channel {cid})')

        # 'update' edits like 'resolved' does (a status that is not the end
        # of the matter); the embed builder tells the two apart.
        if event in ('resolved', 'update') and ref:
            prior = None
            try:
                prior = await loop.run_in_executor(
                    None, lambda: self.db.get_staff_message(kind, ref))
            except Exception as e:
                logger.warning(f"staff_notify: message lookup failed for {kind}/{ref}: {e}")
            if prior:
                try:
                    msg = await channel.fetch_message(int(prior['message_id']))
                    await msg.edit(embed=embed)
                    return {'kind': kind, 'ref': ref, 'edited': True,
                            'message_id': str(msg.id)}
                except discord.NotFound:
                    # Message deleted - fall through and post a fresh one so
                    # the resolution isn't lost entirely.
                    logger.info(
                        f"staff_notify: original message for {kind}/{ref} is gone; posting anew")
                except discord.Forbidden:
                    raise ValueError(f'forbidden editing in channel {cid}')

        msg = await channel.send(embed=embed)
        if ref:
            try:
                await loop.run_in_executor(
                    None,
                    lambda: self.db.record_staff_message(kind, ref, cid, msg.id))
            except Exception as e:
                # The post succeeded; only the ability to edit it later is
                # lost, so this must not fail the action.
                logger.warning(f"staff_notify: could not record message id for {kind}/{ref}: {e}")
        logger.info(f"staff_notify: {kind}/{ref} -> channel {cid} ({event})")
        return {'kind': kind, 'ref': ref, 'edited': False, 'message_id': str(msg.id)}

    # ------------------------------------------------------------------
    # Bug reports and suggestions
    #
    # Two public channels per kind, chosen on the staff page and pushed here
    # by the feedback_channels action: an INTAKE channel, where a member's
    # message is filed as a report (on_message -> _file_feedback) and a
    # how-to lives (the only message left standing there — every member's
    # message is removed once filed, so it needs no pin); and a BOARD
    # channel, where every report is posted
    # with its status and each status change is relayed under it
    # (feedback_board action). Filing is a signed POST to the backend
    # (_feedback_post): the static key plus an HMAC of the body, the same
    # signing the outbox uses, run the other way.
    # ------------------------------------------------------------------

    def _load_feedback_channels(self):
        rows = self.db.get_feedback_channels()
        self._feedback_channels = {}
        self._feedback_intake = {}
        for row in rows:
            self._apply_feedback_row(row)
        if self._feedback_intake:
            logger.info(f"feedback channels: intake {dict(self._feedback_intake)}")

    def _apply_feedback_row(self, row: Dict[str, Any]):
        kind = row.get('kind')
        if not kind:
            return
        # Drop whatever this kind mapped to before, then map the new intake.
        for cid, k in list(self._feedback_intake.items()):
            if k == kind:
                del self._feedback_intake[cid]
        self._feedback_channels[kind] = dict(row)
        intake = int(row.get('intake_channel_id') or 0)
        if intake:
            self._feedback_intake[intake] = kind

    async def _fetch_text_channel(self, cid: int):
        """Cache first, then REST — as _exec_action_staff_notify does."""
        channel = self.get_channel(cid)
        if channel is None:
            try:
                channel = await self.fetch_channel(cid)
            except discord.NotFound:
                raise ValueError(f'channel_not_found ({cid})')
            except discord.Forbidden:
                raise ValueError(f'forbidden ({cid})')
        return channel

    def _site_url(self) -> str:
        """The site's origin for links in Discord — the bot's PUBLIC_BASE_URL
        is where the site is served (see env.sample), unlike the backend's."""
        return os.getenv('PUBLIC_BASE_URL', 'https://nswpsn.forcequit.xyz').rstrip('/')

    async def _refresh_feedback_notices(self):
        """Edit each how-to in place when its wording has changed since it
        was posted (a new build, a corrected link); repost one that was
        deleted. Content is compared, so an unchanged notice is left alone.
        One pinned by an earlier build is unpinned: it is the only message
        in its channel, so a pin adds nothing but a banner."""
        loop = asyncio.get_event_loop()
        for kind, row in list(self._feedback_channels.items()):
            intake = int(row.get('intake_channel_id') or 0)
            notice_id = int(row.get('notice_message_id') or 0)
            if not intake:
                continue
            wanted = build_feedback_notice_embed(kind, self._site_url())
            wanted_view = build_feedback_notice_view(kind, self._site_url())
            try:
                channel = await self._fetch_text_channel(intake)
            except ValueError as e:
                logger.warning(f"feedback notices: {kind} intake channel unavailable: {e}")
                continue
            msg = None
            if notice_id:
                try:
                    msg = await channel.fetch_message(notice_id)
                except discord.NotFound:
                    msg = None
                except discord.HTTPException as e:
                    logger.warning(f"feedback notices: {kind} notice not fetched: {e}")
                    continue
            if msg is not None:
                if msg.pinned:
                    try:
                        await msg.unpin()
                    except discord.HTTPException as e:
                        logger.info(f"feedback notices: {kind} unpin failed: {e}")
                have = msg.embeds[0] if msg.embeds else None
                # Same words and already carrying the button: leave it alone.
                if (have and have.title == wanted.title
                        and (have.description or '') == (wanted.description or '')
                        and bool(msg.components) == bool(wanted_view)):
                    continue
                try:
                    await msg.edit(embed=wanted, view=wanted_view)
                    logger.info(f"feedback notices: {kind} how-to updated in place")
                except discord.HTTPException as e:
                    logger.warning(f"feedback notices: {kind} edit failed: {e}")
                continue
            # Gone (or never recorded): post a fresh one.
            msg = await channel.send(embed=wanted, view=wanted_view)
            row = dict(row)
            row['notice_message_id'] = msg.id
            await loop.run_in_executor(
                None, lambda: self.db.set_feedback_channels(
                    kind, int(row.get('guild_id') or 0), intake,
                    int(row.get('board_channel_id') or 0), msg.id))
            self._apply_feedback_row(row)
            logger.info(f"feedback notices: {kind} how-to reposted")

    async def _remove_feedback_notice(self, channel_id: int, message_id: int):
        """Take a how-to down (unpinning first, for one an earlier build
        pinned). Best-effort: it may already be gone."""
        if not channel_id or not message_id:
            return
        try:
            channel = await self._fetch_text_channel(channel_id)
            msg = await channel.fetch_message(message_id)
        except Exception as e:
            logger.info(f"feedback notice: old notice {message_id} not fetched: {e}")
            return
        for op in (msg.unpin, msg.delete):
            try:
                await op()
            except Exception as e:
                logger.info(f"feedback notice: {op.__name__} failed: {e}")

    async def _exec_action_feedback_channels(self, params):
        """One kind's channels changed on the staff page."""
        kind = (params.get('kind') or '').strip()
        if kind not in FEEDBACK_KINDS:
            raise ValueError(f'unknown feedback kind: {kind}')
        try:
            gid = int(params.get('guild_id') or 0)
            intake = int(params.get('intake_channel_id') or 0)
            board = int(params.get('board_channel_id') or 0)
        except (TypeError, ValueError):
            raise ValueError('bad guild_id/channel ids')
        site_url = (params.get('site_url') or '').strip() or self._site_url()
        loop = asyncio.get_event_loop()

        prior = self._feedback_channels.get(kind) or {}
        prior_intake = int(prior.get('intake_channel_id') or 0)
        notice_id = int(prior.get('notice_message_id') or 0)

        # The how-to belongs to the intake channel: when that changes or goes,
        # the old one comes down; a new intake channel gets a fresh one. No
        # pin: every other message there is removed once filed, so the
        # how-to is the only one standing.
        if notice_id and intake != prior_intake:
            await self._remove_feedback_notice(prior_intake, notice_id)
            notice_id = 0
        if intake and (intake != prior_intake or not notice_id):
            channel = await self._fetch_text_channel(intake)
            msg = await channel.send(embed=build_feedback_notice_embed(kind, site_url),
                                     view=build_feedback_notice_view(kind, site_url))
            notice_id = msg.id

        await loop.run_in_executor(
            None, lambda: self.db.set_feedback_channels(kind, gid, intake, board, notice_id))
        self._apply_feedback_row({
            'kind': kind, 'guild_id': gid, 'intake_channel_id': intake,
            'board_channel_id': board, 'notice_message_id': notice_id,
        })
        logger.info(f"feedback channels: {kind} intake={intake or '-'} board={board or '-'} notice={notice_id or '-'}")
        return {'kind': kind, 'intake_channel_id': str(intake), 'board_channel_id': str(board),
                'notice_message_id': str(notice_id)}

    async def _post_feedback_board(self, kind: str, feedback_id: int, subject: str, body: str,
                                   reporter: str, source: str, status: str, status_label: str,
                                   site_url: str, files=None):
        """Post a report to the kind's board, remember the message. None when
        the kind has no board channel."""
        row = self._feedback_channels.get(kind) or {}
        cid = int(row.get('board_channel_id') or 0)
        if not cid:
            return None
        channel = await self._fetch_text_channel(cid)
        embed = build_feedback_board_embed(kind, str(feedback_id), subject, body, reporter, source,
                                           status, status_label, site_url)
        msg = await channel.send(embed=embed, files=files or [])
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None, lambda: self.db.record_feedback_board_post(feedback_id, kind, cid, msg.id))
        except Exception as e:
            # The post stands; only the ability to reply to it later is lost.
            logger.warning(f"feedback board: could not record post for #{feedback_id}: {e}")
        return msg

    async def _exec_action_feedback_board(self, params):
        """The backend's word on a report: post it, relay a status change, or take it down."""
        event = (params.get('event') or '').strip()
        kind = (params.get('kind') or '').strip()
        if kind not in FEEDBACK_KINDS:
            raise ValueError(f'unknown feedback kind: {kind}')
        try:
            fid = int(params.get('id') or 0)
        except (TypeError, ValueError):
            raise ValueError('bad id')
        if not fid:
            raise ValueError('id required')
        status = (params.get('status') or 'open').strip()
        status_label = (params.get('status_label') or '').strip()
        site_url = (params.get('site_url') or '').strip()
        loop = asyncio.get_event_loop()

        if event == 'new':
            msg = await self._post_feedback_board(
                kind, fid, params.get('subject') or '', params.get('body') or '',
                params.get('reporter') or '', params.get('source') or 'site',
                status, status_label, site_url)
            if msg is None:
                return {'skipped': 'no_board_channel'}
            # The backend's "Open in Discord" link for a site report.
            await self._feedback_post('feedback_board_post', f'/api/discord/feedback/{fid}/board', {
                'id': str(fid), 'guild_id': str(msg.guild.id if msg.guild else 0),
                'channel_id': str(msg.channel.id), 'message_id': str(msg.id),
            })
            return {'posted': True, 'message_id': str(msg.id)}

        prior = await loop.run_in_executor(None, lambda: self.db.get_feedback_board_post(fid))
        if not prior:
            return {'skipped': 'no_board_post'}
        channel = await self._fetch_text_channel(int(prior['channel_id']))
        try:
            msg = await channel.fetch_message(int(prior['message_id']))
        except discord.NotFound:
            await loop.run_in_executor(None, lambda: self.db.delete_feedback_board_post(fid))
            return {'skipped': 'board_post_gone'}

        if event == 'delete':
            # The replies under the post go first: the ones recorded as they
            # were posted, plus a sweep of the channel after the post for any
            # of the bot's messages that reply to it (replies from before they
            # were recorded). Then the post itself.
            gone = 0
            seen: set = set()
            try:
                for r in await loop.run_in_executor(None, lambda: self.db.get_feedback_board_replies(fid)):
                    mid = int(r['message_id'])
                    seen.add(mid)
                    try:
                        await (await channel.fetch_message(mid)).delete()
                        gone += 1
                    except discord.NotFound:
                        pass
                    except discord.HTTPException as e:
                        logger.info(f"feedback board: reply {mid} not deleted: {e}")
            except Exception as e:
                logger.warning(f"feedback board: reply lookup failed for #{fid}: {e}")
            try:
                async for m in channel.history(after=discord.Object(id=msg.id), limit=300):
                    ref = m.reference
                    if (m.author.id == self.user.id and ref and ref.message_id == msg.id
                            and m.id not in seen):
                        try:
                            await m.delete()
                            gone += 1
                        except discord.HTTPException as e:
                            logger.info(f"feedback board: reply {m.id} not deleted: {e}")
            except discord.HTTPException as e:
                logger.info(f"feedback board: history sweep failed for #{fid}: {e}")
            try:
                await msg.delete()
            except discord.Forbidden:
                raise ValueError('forbidden deleting the board post')
            await loop.run_in_executor(None, lambda: self.db.delete_feedback_board_post(fid))
            return {'deleted': True, 'replies_deleted': gone}

        if event == 'status':
            # Reply, so followers of the post see it, AND edit, so the post
            # itself always reads current.
            if msg.embeds:
                try:
                    await msg.edit(embed=set_feedback_board_status(msg.embeds[0], status, status_label))
                except discord.HTTPException as e:
                    logger.warning(f"feedback board: edit failed for #{fid}: {e}")
            reply = await msg.reply(
                embed=build_feedback_status_embed(kind, status, status_label,
                                                  params.get('note') or '', params.get('actor') or ''),
                mention_author=False)
            try:
                await loop.run_in_executor(
                    None, lambda: self.db.record_feedback_board_reply(fid, channel.id, reply.id))
            except Exception as e:
                # The reply stands; the sweep on delete still finds it.
                logger.warning(f"feedback board: could not record reply for #{fid}: {e}")
            return {'replied': True, 'message_id': str(reply.id)}

        raise ValueError(f'unknown feedback_board event: {event}')

    async def _feedback_post(self, action: str, path: str, params: Dict[str, str]):
        """A signed POST to the backend. Returns (status, json or {})."""
        secret = os.getenv('BOT_ACTION_SIGNING_SECRET') or ''
        sig = database.sign_bot_action(secret, action, 'bot', params) if secret else ''
        headers = {
            'Authorization': f'Bearer {API_KEY}',
            'User-Agent': 'AusAwareBot/1.0',
            'X-Client-Type': 'discord-bot',
            'X-Bot-Signature': sig,
        }
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{API_BASE_URL}{path}", json=params, headers=headers) as resp:
                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        data = {}
                    return resp.status, data if isinstance(data, dict) else {}
        except Exception as e:
            logger.warning(f"feedback post {path}: {e}")
            return 0, {}

    async def _answer_member(self, message: discord.Message, embed: discord.Embed, after: int,
                             delete_original: bool, view=None):
        """Answer a member who posted in an intake channel, and keep the
        channel clean: an embed in the channel, mentioning them (re-allowed
        for this one send — the bot's default allows none), that self-deletes
        after `after` seconds. Discord has no "only you can see this" for a
        reply to an ordinary message (ephemeral exists for interactions
        alone), so brief is the nearest thing. The member's own message goes
        too once the report is filed."""
        try:
            await message.channel.send(
                content=message.author.mention, embed=embed, view=view, delete_after=after,
                allowed_mentions=discord.AllowedMentions(users=[message.author]))
        except Exception as e:
            logger.warning(f"feedback intake: reply failed in {message.channel.id}: {e}")
        if delete_original:
            try:
                await message.delete()
            except discord.Forbidden:
                logger.warning(f"feedback intake: cannot delete messages in {message.channel.id} (needs Manage Messages)")
            except discord.NotFound:
                pass
            except Exception as e:
                logger.warning(f"feedback intake: delete failed: {e}")

    async def on_message(self, message: discord.Message):
        # The only thing on_message does. No prefix commands exist (the '!'
        # prefix is a discord.py default nothing uses), so there is no
        # process_commands to hand the message on to.
        if message.author.bot or message.guild is None:
            return
        kind = self._feedback_intake.get(message.channel.id)
        if not kind:
            return
        try:
            await self._file_feedback(kind, message)
        except Exception as e:
            logger.error(f"feedback intake: {e}", exc_info=True)

    async def _file_feedback(self, kind: str, message: discord.Message):
        """A member's message in an intake channel becomes a report."""
        label, in_sentence, _, _ = FEEDBACK_KINDS[kind]
        text = (message.content or '').strip()
        answer = lambda tone, title, body, after, delete_original, view=None: self._answer_member(  # noqa: E731
            message, build_feedback_reply_embed(kind, tone, title, body), after, delete_original, view)

        # The required shape: a '#' title line, the details under it. That
        # is what gives the report a title and a description of its own
        # rather than the same first line twice. Anything else is told the
        # format and not filed.
        title, details = split_feedback_message(text)
        example = '# A dark mode for the map' if kind == 'suggestion' else '# Map tiles go blank past zoom 14'
        if title is None:
            await answer(
                'hint', f"Not filed — start with a title line",
                f"Begin your message with a title line, then put the details on the lines under it:\n"
                f"```\n{example}\nWhat it is, and the details…\n```\n"
                f"The how-to at the top of the channel has a full example.",
                15, True)
            return
        if len(details) < 10 and not message.attachments:
            await answer(
                'hint', f"Not filed — add the details under the title",
                ("What it would do, and why it would help." if kind == 'suggestion'
                 else "What you did, what happened, what you expected, and where.")
                + " The how-to at the top of the channel has an example.",
                15, True)
            return

        now = time.monotonic()
        if now - self._feedback_last_post.get(message.author.id, 0.0) < 20:
            await answer('hint', "One at a time", f"Give it a few seconds before the next {in_sentence}.", 10, True)
            return
        self._feedback_last_post[message.author.id] = now
        if len(self._feedback_last_post) > 500:
            cutoff = now - 60
            self._feedback_last_post = {u: t for u, t in self._feedback_last_post.items() if t > cutoff}

        if not os.getenv('BOT_ACTION_SIGNING_SECRET'):
            if not self._feedback_unsigned_notice:
                self._feedback_unsigned_notice = True
                logger.error("feedback intake: BOT_ACTION_SIGNING_SECRET is not set — nothing can be filed")
            await answer('info', f"{label}s aren't set up yet", "Please use the site for now.", 15, False)
            return

        # Attachments are read NOW: the message is deleted once filed, and
        # Discord's attachment links expire anyway, so the files are re-sent
        # on the board post, which is the lasting copy.
        files: List[discord.File] = []
        attach_names: List[str] = []
        for a in message.attachments[:8]:
            if a.size and a.size > 8 * 1024 * 1024:
                attach_names.append(f"{a.filename} (too large to keep)")
                continue
            try:
                data = await a.read()
                files.append(discord.File(io.BytesIO(data), filename=a.filename))
                attach_names.append(a.filename)
            except Exception as e:
                logger.warning(f"feedback intake: attachment {a.filename} not read: {e}")
                attach_names.append(f"{a.filename} (could not be fetched)")

        reporter = (message.author.display_name or message.author.name or 'a member')[:100]
        subject = title[:140]
        body = details or '(see the attachments on the board post)'
        if attach_names:
            body += "\n\nAttachments: " + ", ".join(attach_names)
        params = {
            'kind': kind,
            'discord_user_id': str(message.author.id),
            'discord_user_name': reporter,
            'subject': subject,
            'body': body[:5000],
        }
        status, data = await self._feedback_post('feedback', '/api/discord/feedback', params)

        if status == 200 and data.get('ok'):
            fb = data.get('feedback') or {}
            fid = int(fb.get('id') or 0)
            site_url = self._site_url()
            board_msg = None
            try:
                board_msg = await self._post_feedback_board(
                    kind, fid, subject, body, reporter, 'discord', 'open',
                    fb.get('statusLabel') or 'Open', site_url, files=files)
            except Exception as e:
                logger.warning(f"feedback intake: board post failed for #{fid}: {e}")
            if board_msg is not None:
                await self._feedback_post('feedback_board_post', f'/api/discord/feedback/{fid}/board', {
                    'id': str(fid), 'guild_id': str(message.guild.id),
                    'channel_id': str(board_msg.channel.id), 'message_id': str(board_msg.id),
                })
            lines = [f"**{subject}**"]
            if board_msg is not None:
                lines.append(f"It's on the board in <#{board_msg.channel.id}> with its status, and the post "
                             f"is replied to whenever that changes.")
            # Linked or not, say how it reaches their contact page: the one
            # we could not match may well have an account they never linked.
            if data.get('linked'):
                lines.append("It's on your contact page on the site too — your Discord is linked to your account.")
            else:
                lines.append("If you have a site account and your Discord is linked to it, it shows on your "
                             "contact page there too.")
            view = None
            if board_msg is not None:
                view = discord.ui.View(timeout=None)
                view.add_item(discord.ui.Button(style=discord.ButtonStyle.link, label='Open on the board',
                                                url=board_msg.jump_url))
            await answer('ok', f"✅ Logged as {in_sentence} #{fid}", "\n".join(lines), 10, True, view)
            logger.info(f"feedback intake: {kind} #{fid} from {message.author.id}")
            return

        if status == 429:
            await answer('fail', "Not filed — too many open",
                         (data.get('error') or 'You have too many open reports already') + '.', 15, False)
            return
        logger.warning(f"feedback intake: backend answered {status}: {data}")
        await answer('fail', "Couldn't file that right now", "Try again in a minute.", 15, False)

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
        # Discord caps the COMBINED title+description+footer at 6000 chars per
        # embed (separate from the per-field caps of 256/4096/2048). Clamp the
        # title and footer to their field limits first, then give the
        # description whatever budget is left so the total never trips a 400.
        title = title[:256]
        footer = footer[:2048]
        desc_budget = max(0, 6000 - len(title) - len(footer))
        description = description[:desc_budget]
        embed = discord.Embed(
            title=title if title else None,
            description=description if description else None,
            color=color_int if color_int is not None else 0xf97316,
            url=url or None,
            timestamp=datetime.now(timezone.utc),
        )
        if footer:
            embed.set_footer(text=footer)

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

        async def _send_container(cont):
            # Send via a Components-V2 LayoutView (same path as live alerts)
            # so the map link renders as a Link Button, not markdown.
            view = discord.ui.LayoutView(timeout=None)
            view.add_item(cont)
            await channel.send(view=view)

        if atype == 'all':
            types_to_fetch = list(self.poller.endpoints.keys()) + ['pager', 'user_incident', 'radio_summary']
        else:
            types_to_fetch = [atype]

        for t in types_to_fetch:
            try:
                if t == 'radio_summary':
                    headers = {'Authorization': f'Bearer {API_KEY}', 'User-Agent': 'AusAwareBot/1.0', 'X-Client-Type': 'discord-bot'}
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
                        if not safe_add_containers(view, group):
                            continue  # every container rejected — nothing to send
                        await channel.send(view=view)
                        sent += 1
                        await asyncio.sleep(0.5)
                elif t == 'pager':
                    messages = await self.poller._fetch_pager_from_api()
                    for msg in messages[:3]:
                        parsed = self.poller._format_api_pager(msg)
                        if parsed:
                            await _send_container(self.embed_builder.build_pager_container(parsed))
                            sent += 1
                            await asyncio.sleep(0.5)
                    if not messages:
                        errors.append(f"{t}: no messages available")
                elif t == 'user_incident':
                    incidents = await self.poller._fetch_user_incidents()
                    for inc in incidents[:3]:
                        alert = {'type': 'user_incident', 'data': inc}
                        await _send_container(self.embed_builder.build_alert_container(alert))
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
                        await _send_container(self.embed_builder.build_alert_container(alert))
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
                      view: Optional[discord.ui.View] = None,
                      reply_to_message_id: int = None,
                      ping_role_ids: Optional[List[int]] = None):
        """Add a message to the queue for rate-limited sending.

        Pass one of: `embed` (single), `embeds` (list of up to 10), or `view`
        (Components V2 LayoutView). Precedence: view > embeds > embed.

        `reply_to_message_id` makes this message a REPLY to an earlier one —
        used so an incident's updates hang off the message that first reported
        it instead of scattering down the channel.

        `ping_role_ids` are the roles this message is allowed to mention. The
        bot mentions nothing by default, so a `<@&id>` rendered into the view
        only actually pings when its id is listed here.
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
            'alert_id': alert_id,
            'reply_to_message_id': reply_to_message_id,
            'ping_role_ids': list(ping_role_ids) if ping_role_ids else None,
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
        """Fan `new_alerts` out to their configured channels, ONE MESSAGE PER
        ALERT.

        This used to pack a poll's alerts for a channel into a single
        LayoutView. That saved messages when the Waze firehose was running,
        but it cost two things that matter more now the volume is lower:

          - Incident tracking. Only the FIRST incident-bearing alert in a
            batch could have its message recorded, because the row stores one
            jump_url per message. Every other incident in that batch was never
            written down, so it could never be linked to — or, now, replied to
            — when it updated.
          - Legibility. Five unrelated incidents shared one timestamp and one
            block, and an update could only ever reply to the whole batch.

        Alerts still keep their poll-result order within a channel; they are
        simply queued one at a time, and the send queue paces them.

        FIRMS is the one exception, for the opposite reason: a satellite
        hotspot is not an incident — it has no status to update and nothing to
        reply to — and one pass over a fire ground lights up hundreds of them.
        A poll's hotspots for a channel are collapsed into a single digest,
        built after the per-alert loop.

        Each alert is queued as soon as its presets resolve, and every DB read
        on the way is guarded. By the time we are called the poller has
        already marked this batch as seen, so an alert dropped here is lost
        for good: a failure must cost that one alert, or that one preset, and
        never the remainder of the cycle.
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

        # Subscriptions are read once per alert_type for the same reason the
        # mute state is read once per guild: a poll carrying 2,577 hotspots
        # asked the DB the same question 2,577 times.
        preset_cache: Dict[str, List[Dict[str, Any]]] = {}

        fires: List[tuple] = []  # (preset_id, alert_type) — for preset_fire_log
        # The FIRMS accumulator: channel_id -> {
        #   'hotspots': [alert_data, ...], 'preset_id': int,
        #   'ping_ids': set(int),
        # }
        firms_buckets: Dict[int, Dict[str, Any]] = {}

        for alert in new_alerts:
            alert_type = alert.get('type')
            alert_data = alert.get('data', {})

            # radio_summary keeps its own multi-container path — it's already
            # emitted as a single big message per channel, and Components V2
            # rendering is incident-aware in ways that don't compose with
            # other alert types in the same view.
            if alert_type == 'radio_summary':
                # Guarded like the per-alert container builds below: the
                # summary builder chews LLM-structured `details` (agencies
                # / codes lists can carry non-string junk) and a raised
                # exception here would drop every remaining alert in this
                # cycle — all already marked seen, so lost for good.
                try:
                    await self._dispatch_radio_summary(alert)
                except Exception:
                    logger.exception(
                        "radio_summary dispatch failed for %s — skipping",
                        alert.get('id'),
                    )
                continue

            if alert_type in preset_cache:
                presets = preset_cache[alert_type]
            else:
                try:
                    presets = await loop.run_in_executor(
                        None, self.db.get_presets_for_alert_type, alert_type
                    )
                except Exception:
                    # One failed lookup costs this alert, not the cycle: the
                    # rest of the batch is already marked seen and will never
                    # come round again. Not cached, so the next alert of this
                    # type gets its own attempt.
                    logger.exception(
                        "Preset lookup failed for %s alert %s — skipping it",
                        alert_type, alert.get('id'),
                    )
                    continue
                preset_cache[alert_type] = presets
            incident_guid, incident_status = self._alert_incident_keys(alert_type, alert_data)

            # channel_id -> {'preset_id', 'ping_ids'}, for the presets that
            # matched THIS alert. Keying by channel renders the alert once
            # when two presets in the same channel both subscribe to its type
            # — a supported dashboard configuration — while fire-logging
            # still runs per preset. The ping set is per alert for the same
            # reason: a set unioned across the whole poll pinged the traffic
            # role for a fire and the fire role for a crash, and bypassed a
            # per-type ping mute whenever the preset matched anything else.
            targets: Dict[int, Dict[str, Any]] = {}

            for preset in presets:
                # The whole evaluation sits in one guard — the mute reads,
                # the filter gate and the role resolution — so a bad row or a
                # dropped connection costs that preset alone.
                try:
                    channel_id = preset['channel_id']
                    guild_mute = await _get_guild_mute(preset['guild_id'])
                    channel_mute = await _get_channel_mute(
                        preset['guild_id'], channel_id)
                    effective = Database.resolve_preset_effective_state(
                        preset, alert_type, channel_mute, guild_mute
                    )
                    if not effective['enabled']:
                        continue
                    # Per-preset filter gate (keywords / severity floor / bbox).
                    if not preset_alert_matches(preset, alert_type, alert_data):
                        continue
                    # Role ids only when this preset's effective_ping is on;
                    # _resolve_ping_role_ids also filters deleted roles.
                    ping_ids = []
                    if effective['enabled_ping']:
                        ping_view = dict(preset)
                        ping_view['enabled_ping'] = True
                        ping_ids = self._resolve_ping_role_ids(ping_view)
                except Exception:
                    logger.exception(
                        "Preset %s failed on %s alert %s — skipping that preset",
                        preset.get('id'), alert_type, alert.get('id'),
                    )
                    continue

                fires.append((preset['id'], alert_type))
                target = targets.setdefault(channel_id, {
                    'preset_id': preset['id'],
                    'ping_ids': set(),
                })
                target['ping_ids'].update(ping_ids)

            if not targets:
                continue

            # Hotspots accumulate instead of sending — one digest per channel
            # goes out below, however many arrived in this poll.
            if alert_type == 'firms':
                for channel_id, target in targets.items():
                    bucket = firms_buckets.setdefault(channel_id, {
                        'hotspots': [],
                        'preset_id': target['preset_id'],
                        'ping_ids': set(),
                    })
                    bucket['hotspots'].append(alert_data)
                    bucket['ping_ids'] |= target['ping_ids']
                continue

            for channel_id, target in targets.items():
                # Previous-message lookup for incident tracking (RFS /
                # user_incident). Use get_previous_incident_message so the
                # link points to the most recent prior update of THIS
                # incident, not the very first one — when an incident has 4-5
                # updates the "first" message has scrolled far up the channel
                # and looks random. Replies, by contrast, hang off the FIRST
                # report: chaining reply-to-reply would build a ladder that
                # Discord renders one rung deep anyway, and the first message
                # is the one people recognise as "that fire".
                previous_message = None
                reply_to_message_id = None
                if incident_guid:
                    try:
                        previous_message = await loop.run_in_executor(
                            None,
                            self.db.get_previous_incident_message,
                            incident_guid,
                            channel_id,
                        )
                        reply_to_message_id = await loop.run_in_executor(
                            None,
                            self.db.get_incident_reply_target,
                            incident_guid,
                            channel_id,
                        )
                    except Exception:
                        # Degrade to a standalone post. The alert itself still
                        # has to go out; losing the update link is the lesser
                        # failure by a long way.
                        logger.exception(
                            "Incident-message lookup failed for %s in channel %s — "
                            "posting it without the update link",
                            incident_guid, channel_id,
                        )
                        previous_message = None
                        reply_to_message_id = None

                # Built inside a guard: one malformed alert must never raise
                # out of the loop and drop every other alert in the cycle.
                try:
                    container = self.embed_builder.build_alert_container(
                        alert, previous_message=previous_message,
                    )
                except Exception:
                    logger.exception(
                        "Failed to build container for %s alert %s — skipping",
                        alert_type, alert.get('id'),
                    )
                    continue

                self._queue_alert_message(
                    channel_id, [container],
                    ping_ids=target['ping_ids'],
                    preset_id=target['preset_id'],
                    alert_type=alert_type or 'alert',
                    alert_id=alert.get('id'),
                    incident_guid=incident_guid,
                    incident_status=incident_status,
                    reply_to_message_id=reply_to_message_id,
                )

        for channel_id, bucket in firms_buckets.items():
            hotspots = bucket['hotspots']
            try:
                if len(hotspots) == 1:
                    container = self.embed_builder.build_firms_container(hotspots[0])
                else:
                    container = self.embed_builder.build_firms_digest_container(hotspots)
            except Exception:
                logger.exception(
                    "Failed to build the FIRMS digest for channel %s (%d hotspot(s)) "
                    "— skipping", channel_id, len(hotspots),
                )
                continue
            if len(hotspots) > 1:
                logger.info(
                    f"🛰️ Collapsed {len(hotspots)} FIRMS hotspots into one "
                    f"digest for channel {channel_id}"
                )
            self._queue_alert_message(
                channel_id, [container],
                ping_ids=bucket['ping_ids'],
                preset_id=bucket['preset_id'],
                alert_type='firms',
                alert_id=f"firms_digest_{len(hotspots)}",
            )

        if fires:
            await loop.run_in_executor(None, self.db.log_preset_fires, fires)

    def _queue_alert_message(self, channel_id: int, containers: list, *,
                             ping_ids=None, preset_id: Optional[int] = None,
                             alert_type: str = 'alert',
                             alert_id: Optional[str] = None,
                             incident_guid: Optional[str] = None,
                             incident_status: Optional[str] = None,
                             reply_to_message_id: Optional[int] = None):
        """Chunk `containers` into messages and queue them for one channel.

        Role pings ride on the FIRST chunk — as mention text inside the view,
        and as the matching allowed_mentions grant on the send, since the bot
        itself is configured to mention nothing. The incident metadata rides
        there too: the first chunk is the message an update should reply to.
        """
        from embeds import chunk_containers_for_message

        role_ids = sorted(ping_ids or ())
        ping_text = ' '.join(f'<@&{rid}>' for rid in role_ids) if role_ids else None
        # Still chunked, for the rare single alert that overflows the
        # character budget on its own.
        for i, group in enumerate(chunk_containers_for_message(containers)):
            view = discord.ui.LayoutView(timeout=None)
            if i == 0 and ping_text:
                view.add_item(discord.ui.TextDisplay(content=ping_text))
            if not safe_add_containers(view, group) and not (i == 0 and ping_text):
                continue  # container rejected — nothing to send
            queue_kwargs = {
                'channel_id': channel_id,
                'view': view,
                'config_id': preset_id,
                'config_type': 'alert',
                'alert_type': alert_type,
                'alert_id': f"{alert_id}#{i}" if (alert_id and i) else alert_id,
            }
            if i == 0:
                queue_kwargs['ping_role_ids'] = role_ids or None
                # Every incident-bearing alert records its own message, where
                # the old batch could only ever record its first.
                if incident_guid:
                    queue_kwargs['incident_guid'] = incident_guid
                    queue_kwargs['incident_status'] = incident_status
                    queue_kwargs['reply_to_message_id'] = reply_to_message_id
            self.queue_message(**queue_kwargs)

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
                if not safe_add_containers(view, group) and not (i == 0 and ping_text):
                    continue  # every container rejected — nothing to send
                self.queue_message(
                    channel_id=channel_id,
                    view=view,
                    config_id=bucket['any_preset_id'],
                    config_type='alert',
                    alert_type='radio_summary',
                    alert_id=f"{alert.get('id')}#{i}" if i > 0 else alert.get('id'),
                    # The bot mentions nothing unless the send says otherwise.
                    ping_role_ids=role_ids if i == 0 else None,
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
            # 'pager' is the per-type override key /mute and /smute write for
            # the pager scope, and the key /setup reads back when it shows the
            # pager as silenced. Resolving with alert_type=None skipped the
            # per-type tier entirely, so `/smute alert_type:pager` reported
            # success, displayed as silenced, and kept posting and pinging.
            effective = Database.resolve_preset_effective_state(
                preset, 'pager', channel_mute, guild_mute
            )
            if not effective['enabled']:
                continue
            raw_capcodes = preset.get('pager_capcodes')
            if isinstance(raw_capcodes, list):
                capcode_list = [str(c).strip().upper() for c in raw_capcodes if str(c).strip()]
            elif raw_capcodes:
                capcode_list = [c.strip().upper() for c in str(raw_capcodes).split(',') if c.strip()]
            else:
                capcode_list = []
            preset_plans.append((preset, effective, capcode_list))

        # channel_id -> {'containers': [...], 'role_ids_set': set(), 'any_preset_id': ..., 'msg_hashes': [...]}
        buckets: Dict[int, Dict[str, Any]] = {}
        fires: List[tuple] = []
        # (channel_id, msg_hash) pairs already bucketed — two presets in the
        # same channel with overlapping capcode filters must not render the
        # same pager message twice in one send (same dedupe as the alerts
        # dispatcher). Fire-logging and ping unions still run per preset.
        bucketed: set = set()

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
                dedupe_key = (channel_id, msg.get('_msg_hash'))
                if dedupe_key not in bucketed:
                    bucketed.add(dedupe_key)
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
                if not safe_add_containers(view, group) and not (i == 0 and ping_text):
                    continue  # every container rejected — nothing to send
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
                    # The bot mentions nothing unless the send says otherwise.
                    ping_role_ids=role_ids if i == 0 else None,
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

async def _alert_type_autocomplete(interaction: discord.Interaction, current: str):
    """Suggest alert types as the user types.

    Replaces a fixed `choices` list, which Discord caps at 25 - exceeded once
    the interstate sources landed, and a hard failure at command sync rather
    than a degraded picker. Autocomplete caps only the SUGGESTIONS, so the
    underlying set can keep growing.
    """
    cur = (current or '').lower().strip()
    out = []
    for key, name in ALERT_TYPES.items():
        if not cur or cur in key.lower() or cur in name.lower():
            out.append(app_commands.Choice(name=name, value=key))
            if len(out) >= DISCORD_SELECT_MAX:
                break
    return out


@bot.tree.command(name="alert", description="Set up alerts for a channel")
@app_commands.describe(
    channel="The channel to send alerts to",
    alert_type="The type of alert to receive (leave empty for ALL alerts)",
    role="Optional role to ping when alerts are sent"
)
@app_commands.autocomplete(alert_type=_alert_type_autocomplete)
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
            # …except the types that only ever arrive by being asked for by
            # name. This bare /alert is a catch-all, and a catch-all is
            # exactly how two presets came to be subscribed to the FIRMS
            # hotspot firehose without anyone choosing it.
            all_types = [a for a in ALERT_TYPES if a not in _OPT_IN_ONLY_TYPES]

            def _sync_all():
                # Add every alert_type to the channel's Default preset, and
                # apply role_id (if provided) preset-wide.
                roles_arg = [role_id] if role_id else None
                for atype in all_types:
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
                value=f"{len(all_types)} types configured",
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
            if not safe_add_containers(view, group):
                continue  # every container rejected — nothing to send
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
        title="🤖 AusAware Alert Bot Status",
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
    
    embed.set_footer(text=f"AusAware • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
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
                # A subscription the dispatcher refuses — today only a
                # hotspot type on a preset with no region — must not display
                # as a healthy row.
                '_unscoped': not Database.subscription_is_honoured(preset, atype),
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
        # Name the owning preset when it isn't Default: the /setup picker only
        # edits Default, so a row from a dashboard preset has to be
        # distinguishable from one the picker will overwrite.
        preset_name = cfg.get("_preset_name")
        owner_txt = (f" · *{preset_name}*"
                     if preset_name and preset_name != DEFAULT_PRESET_NAME else "")
        # Subscribed but not delivered: say so here rather than leave the
        # operator waiting for alerts that the dispatcher is refusing.
        warn_txt = " · ⚠️ needs a region" if cfg.get("_unscoped") else ""
        parts.append(f"{icon} `{atype}` {name}{role_txt}{owner_txt}{warn_txt}")
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
                f"Editing the **{DEFAULT_PRESET_NAME}** subscriptions for "
                f"{self.channel.mention}\n\n"
                f"*Select alert types from the dropdown, then click off the dropdown "
                f"and press **Save**.*\n"
                f"*Any other preset in this channel is listed below for "
                f"reference only — it is managed on the dashboard and Save "
                f"does not touch it.*"
            ),
            color=0x3498db,
        )
        embed.add_field(name="Current", value=current, inline=False)
        embed.add_field(
            name="🌐 Website",
            value=f"[nswpsn.forcequit.xyz]({WEBSITE_URL})",
            inline=False,
        )

        # Seed the picker from the DEFAULT preset alone, off-loop — the view
        # constructor must not touch the DB (it runs on the gateway loop).
        #
        # It used to be seeded from every preset in the channel while Save
        # wrote the whole selection back to Default, so pressing Save without
        # touching anything copied a filtered dashboard preset's types into
        # the unfiltered Default — which then got every RFS/CFA alert
        # statewide, with Default's role pings, and nothing the dashboard
        # preset's geofilter could do about it.
        #
        # get_preset_by_name rather than _default_preset_for: a DB error here
        # must fail the button, not quietly seed an empty picker that a later
        # Save would write over the real subscriptions.
        def _existing_alert_types() -> List[str]:
            preset = bot.db.get_preset_by_name(
                self.channel.guild.id, self.channel.id, DEFAULT_PRESET_NAME)
            # Only general alert types — radio_summary is managed in its
            # own submenu.
            return sorted([
                a for a in ((preset or {}).get('alert_types') or [])
                if a and a != 'radio_summary'
            ])
        try:
            existing_types = await loop.run_in_executor(None, _existing_alert_types)
        except Exception as e:
            logger.error(
                f"Setup Alerts: could not read the {DEFAULT_PRESET_NAME} preset "
                f"for channel {self.channel.id}: {e}", exc_info=True,
            )
            await interaction.response.send_message(
                "❌ Couldn't read this channel's current subscriptions, so the "
                "editor would have shown the wrong ones. Nothing was changed — "
                "try again in a moment.",
                ephemeral=True,
            )
            return

        await _edit_or_send(
            interaction, embed=embed,
            view=SetupAlertsSubmenuView(self.invoker_id, self.channel, existing_types),
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
                 selected_types: Optional[List[str]] = None,
                 active_provider: Optional[str] = None):
        super().__init__(timeout=180)
        self.invoker_id = invoker_id
        self.channel = channel
        self.selected_types: List[str] = list(selected_types or [])
        self.selected_role_ids: List[int] = []
        self._roles_touched = False

        # Provider first, then that provider's types — the same shape as the
        # alerts submenu, and the same reason: the flat list is past Discord's
        # 25-option ceiling and chunking it gave pages of unrelated agencies.
        # A "Pager" pseudo-provider carries the pager row, which is not an
        # alert type (it runs off the pager_enabled column).
        # Every type, including radio summary and the not-yet-live Wire:
        # assigning a ping role is not the same as subscribing, and roles were
        # assignable for those before this menu was grouped by provider.
        self._providers = alert_catalog.providers_with_types(
            include_soon=True, general_only=False)
        # Carried across rebuilds so picking a type does not bounce the user
        # back to the first source in the list.
        self._active_provider = active_provider or (
            self._providers[0]['key'] if self._providers else PAGER_PROVIDER_KEY)
        self._provider_select = None
        self._type_selects: List[discord.ui.Select] = []
        self._role_select = None
        self._rebuild()

    def _rebuild(self):
        for item in [self._provider_select, *self._type_selects, self._role_select]:
            if item is not None:
                self.remove_item(item)
        self._type_selects = []

        selected = set(self.selected_types)

        prov_options = []
        for p in self._providers:
            keys = [t['key'] for t in p['types']]
            on = sum(1 for k in keys if k in selected)
            prov_options.append(discord.SelectOption(
                label=p['label'][:100], value=p['key'],
                description=(f"{on} of {len(keys)} picked" if on else f"{len(keys)} available"),
                default=(p['key'] == self._active_provider),
            ))
        prov_options.append(discord.SelectOption(
            label='Pager Messages', value=PAGER_PROVIDER_KEY, emoji='📟',
            description=('picked' if 'pager' in selected else 'Pager alerts'),
            default=(self._active_provider == PAGER_PROVIDER_KEY),
        ))
        self._provider_select = discord.ui.Select(
            placeholder="1. Pick a source", options=prov_options,
            min_values=1, max_values=1, row=0,
        )
        self._provider_select.callback = self._on_provider_picked
        self.add_item(self._provider_select)

        if self._active_provider == PAGER_PROVIDER_KEY:
            type_options = [discord.SelectOption(
                label='Pager Messages', value='pager', emoji='📟',
                default=('pager' in selected))]
            active_label = 'Pager'
        else:
            active = next((p for p in self._providers
                           if p['key'] == self._active_provider), None)
            active_label = active['label'] if active else ''
            type_options = [
                discord.SelectOption(
                    label=t['label'][:100], value=t['key'],
                    description=(t.get('agencyLabel') or '')[:100] or None,
                    default=(t['key'] in selected),
                )
                for t in (active['types'] if active else [])
            ]

        next_row = 1
        for chunk in _chunk_options(type_options):
            if not chunk:
                continue
            sel = discord.ui.Select(
                placeholder=f"2. {active_label} — pick types",
                min_values=0, max_values=len(chunk),
                options=chunk, row=next_row,
            )
            sel.callback = self._on_types_picked
            self.add_item(sel)
            self._type_selects.append(sel)
            next_row += 1

        role_select = discord.ui.RoleSelect(
            placeholder="3. Pick up to 5 roles (empty + Save = no change)",
            min_values=0, max_values=5,
            row=next_row,
        )
        role_select.callback = self._on_roles_picked
        self.add_item(role_select)
        self._role_select = role_select

    async def _on_provider_picked(self, interaction: discord.Interaction):
        self._active_provider = self._provider_select.values[0]
        self._rebuild()
        await interaction.response.edit_message(view=self)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    async def _on_types_picked(self, interaction: discord.Interaction):
        # Only the ACTIVE provider's slice is replaced — the select says
        # nothing about types belonging to any other provider.
        if self._active_provider == PAGER_PROVIDER_KEY:
            owned = {'pager'}
        else:
            active = next((p for p in self._providers
                           if p['key'] == self._active_provider), None)
            owned = {t['key'] for t in (active['types'] if active else [])}
        chosen = [k for k in _merge_select_values(self._type_selects, interaction)
                  if k in owned]
        picked = [k for k in self.selected_types if k not in owned] + chosen
        # Rebuild so the embed reflects current roles for each selected type.
        new_view = SetupRolesSubmenuView(
            self.invoker_id, self.channel, selected_types=picked,
            active_provider=self._active_provider)
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

    @discord.ui.button(label="Save", style=discord.ButtonStyle.green, row=4)
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

    @discord.ui.button(label="Clear Roles", style=discord.ButtonStyle.danger, row=4)
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

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=4)
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
    def __init__(self, invoker_id: int, channel: discord.TextChannel,
                 existing_types: List[str]):
        # `existing_types` is fetched by the CALLER via run_in_executor —
        # this constructor runs on the gateway event loop, and it used to
        # make three blocking Postgres round-trips itself (the only DB
        # call path in this file not offloaded), stalling the heartbeat
        # for up to ~10s under DB latency whenever someone clicked
        # "Setup Alerts".
        super().__init__(timeout=180)
        self.invoker_id = invoker_id
        self.channel = channel
        self.selected_alert_types: List[str] = existing_types[:]

        self._providers = alert_catalog.providers_with_types()
        self._active_provider = self._providers[0]['key'] if self._providers else None
        self._provider_select = None
        self._type_selects: List[discord.ui.Select] = []
        self._rebuild()

    def _rebuild(self):
        """Provider first, then that provider's types.

        The alert list is past Discord's 25-option select ceiling, and chunking
        it gave two pages of alphabetically unrelated agencies. Grouping by
        provider matches how the dashboard and the logs page already present
        these, and the cap stops mattering however many agencies are added.
        """
        for item in [self._provider_select, *self._type_selects]:
            if item is not None:
                self.remove_item(item)
        self._type_selects = []

        selected = set(self.selected_alert_types)

        prov_options = []
        for p in self._providers:
            keys = [t['key'] for t in p['types']]
            on = sum(1 for k in keys if k in selected)
            prov_options.append(discord.SelectOption(
                label=p['label'][:100],
                value=p['key'],
                description=(f"{on} of {len(keys)} on" if on else f"{len(keys)} available"),
                default=(p['key'] == self._active_provider),
            ))
        self._provider_select = discord.ui.Select(
            placeholder="1. Pick a source",
            options=prov_options or [discord.SelectOption(label="None", value="none")],
            min_values=1, max_values=1, row=0,
        )
        self._provider_select.callback = self._on_provider_picked
        self.add_item(self._provider_select)

        active = next((p for p in self._providers
                       if p['key'] == self._active_provider), None)
        types = active['types'] if active else []
        type_options = [
            discord.SelectOption(
                label=t['label'][:100],
                value=t['key'],
                description=(t.get('agencyLabel') or '')[:100] or None,
                default=(t['key'] in selected),
            )
            for t in types
        ]
        if not type_options:
            return

        # _chunk_options stays as the safety net for a provider that ever
        # grows past 25 types on its own; today the largest has eight.
        for idx, chunk in enumerate(_chunk_options(type_options)):
            sel = discord.ui.Select(
                placeholder=f"2. {active['label']} — pick types",
                min_values=0, max_values=len(chunk),
                options=chunk,
                row=1 + idx,
            )
            sel.callback = self._on_alerts_picked
            self.add_item(sel)
            self._type_selects.append(sel)

    async def _on_provider_picked(self, interaction: discord.Interaction):
        self._active_provider = self._provider_select.values[0]
        self._rebuild()
        await interaction.response.edit_message(view=self)

    async def _on_alerts_picked(self, interaction: discord.Interaction):
        # Replace only the ACTIVE provider's slice of the selection — a select
        # reports nothing about types belonging to any other provider, so
        # anything picked elsewhere has to be carried over untouched.
        active = next((p for p in self._providers
                       if p['key'] == self._active_provider), None)
        owned = {t['key'] for t in (active['types'] if active else [])}
        picked = [k for k in _merge_select_values(self._type_selects, interaction)
                  if k in owned]
        kept = [k for k in self.selected_alert_types if k not in owned]
        self.selected_alert_types = kept + picked
        self._rebuild()
        await interaction.response.edit_message(view=self)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker_id

    @discord.ui.button(label="Save", style=discord.ButtonStyle.green, row=4)
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

    @discord.ui.button(label="Enable All Alerts", style=discord.ButtonStyle.green, row=4)
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

    @discord.ui.button(label="Turn Alerts Off", style=discord.ButtonStyle.danger, row=4)
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

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=4)
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
        "title": "📚 AusAware Alert Bot",
        "lines": [
            "Real-time alerts for emergencies, traffic, weather warnings, "
            "pager messages and The Wire — sourced from NSW RFS, CFA, DEECA, "
            "QFD, DFES, SA CFS/MFS, NT Fire & Rescue, NASA FIRMS, BOM, TfNSW, "
            "Ausgrid, Endeavour, Essential Energy, our radio scanner and "
            "AusAware's own reporting.",
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
             "Dashboard of current incidents across Australia (formerly /summary)."),
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
             "Subscribe a channel to NSW paging-network messages with an "
             "optional comma-separated capcode filter + role to ping."),
            ("/pager-remove",
             "Remove the pager subscription from a channel."),
        ],
        # Derived from ALERT_TYPES so the list can never drift again
        # (the old hand-written copy still advertised retired Waze types
        # and omitted firms).
        "footer": "Alert types: " + " · ".join(ALERT_TYPES.keys()),
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
            text="AusAware • Pick a category from the menu below",
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
        embed.set_footer(text="AusAware • /help to browse other categories")
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
                                 description="Hourly radio summaries"),
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
        'User-Agent': 'AusAwareBot/1.0',
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
    Owner-locked via interaction_check."""

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


@bot.tree.command(name="overview", description="Dashboard of current incidents across Australia")
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
        'User-Agent': 'AusAwareBot/1.0',
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
        if not safe_add_containers(view, group):
            continue  # every container rejected — nothing to send
        await interaction.followup.send(view=view, ephemeral=False)


# ==================== MAIN ====================

def main():
    if not DISCORD_TOKEN:
        logger.error("DISCORD_BOT_TOKEN environment variable not set!")
        logger.error("Please create a .env file with DISCORD_BOT_TOKEN=your_token_here")
        return
    
    logger.info("Starting AusAware Alert Bot...")
    bot.run(DISCORD_TOKEN)


if __name__ == '__main__':
    main()

