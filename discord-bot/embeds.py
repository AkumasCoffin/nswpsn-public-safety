"""
Embed Builder - Creates beautiful Discord embeds for various alert types.
"""

import os
import json
import re
import html
import discord
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from urllib.parse import quote
import alert_catalog

# Base URL for the map
MAP_BASE_URL = "https://nswpsn.forcequit.xyz"

# --- Staff moderation notifications ---------------------------------------
# Label + accent per kind, and the accent a resolved item switches to.
STAFF_NOTIFY_KINDS = {
    'signup_request': ('Signup request', '\U0001F4E5', 0x38BDF8),
    'wire_approval': ('Wire approval', '\U0001F4F0', 0xF97316),
    'wire_takedown': ('Wire takedown', '\u2696\uFE0F', 0xEF4444),
    'new_user': ('New account', '\U0001F464', 0xA855F7),
    'new_node': ('New node', '\U0001F4E1', 0x22C55E),
}
_STAFF_STATUS_COLOR = {
    'approved': 0x22C55E,
    'upheld': 0x22C55E,
    'rejected': 0x64748B,
}


def _parse_notify_fields(raw: Any) -> List[Dict[str, Any]]:
    """Decode the JSON-encoded detail rows.

    Every pending_bot_actions param is a string so the canonical signing form
    matches byte-for-byte across the language boundary, so the field list
    arrives as JSON text. Malformed input yields no rows rather than losing
    the whole notification.
    """
    if not raw:
        return []
    if isinstance(raw, list):
        return [f for f in raw if isinstance(f, dict)]
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return []
    return [f for f in parsed if isinstance(f, dict)] if isinstance(parsed, list) else []


def build_staff_notify_embed(kind: str, params: Dict[str, Any]) -> discord.Embed:
    """Embed for a staff moderation notification.

    These go to a PRIVATE staff channel, so they carry the detail needed to
    triage without opening the site. The backend decides what to include and
    has already dropped empties, truncated long values and capped the count
    (packFields in backends/node/src/services/staffNotify.ts) - this only
    renders whatever survived.

    A plain Embed rather than a Components V2 container: resolution edits
    this message in place, and embeds edit cleanly.
    """
    label, icon, base_color = STAFF_NOTIFY_KINDS.get(
        kind, ('Staff notification', '\U0001F514', 0x94A3B8))
    title = (params.get('title') or '').strip() or label
    subtitle = (params.get('subtitle') or '').strip()
    status = (params.get('status') or '').strip().lower()
    actor = (params.get('actor') or '').strip()
    url = (params.get('url') or '').strip()

    resolved = bool(status)
    color = _STAFF_STATUS_COLOR.get(status, base_color) if resolved else base_color

    heading = f"{icon} {label}"
    if resolved:
        heading = f"{heading} \u00b7 {status.title()}"

    embed = discord.Embed(
        title=heading[:256],
        description=title[:4096],
        color=color,
        url=url or None,
        timestamp=datetime.now(timezone.utc),
    )
    if subtitle:
        embed.add_field(name='\u200b', value=subtitle[:1024], inline=False)
    for f in _parse_notify_fields(params.get('fields')):
        embed.add_field(
            name=str(f.get('name') or '-')[:256],
            value=str(f.get('value') or '')[:1024],
            inline=bool(f.get('inline', True)),
        )

    if resolved:
        embed.add_field(
            name='Handled by',
            value=(actor or 'a staff member')[:1024],
            inline=True,
        )
    embed.set_footer(text='AusAware staff' if not resolved else 'AusAware staff \u00b7 resolved')
    return embed


def strip_html(text: str) -> str:
    """Remove HTML tags and decode HTML entities from text"""
    if not text:
        return ""
    # Remove HTML tags
    clean = re.sub(r'<[^>]+>', '', str(text))
    # Decode HTML entities (&#39; -> ', &amp; -> &, etc.)
    clean = html.unescape(clean)
    # Clean up whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean


def is_valid_value(value: Any) -> bool:
    """Check if a value is valid for display"""
    if value is None:
        return False
    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() in ['unknown', 'n/a', 'none', '-1', '']:
            return False
    if isinstance(value, (int, float)):
        if value == -1 or value == 0:
            return False
    return True


def parse_timestamp_to_datetime(ts_value: Any) -> Optional[datetime]:
    """Parse various timestamp formats into a datetime object.
    
    Handles:
    - ISO strings (2026-01-07T14:30:00Z)
    - Unix timestamps as int/float (seconds or milliseconds)
    - Unix timestamps as string
    
    Returns datetime or None if parsing fails.
    """
    if not ts_value:
        return None
    
    try:
        if isinstance(ts_value, datetime):
            return ts_value
        
        if isinstance(ts_value, (int, float)):
            # Unix timestamp - could be seconds or milliseconds
            ts = float(ts_value)
            if ts > 1e12:  # Milliseconds
                ts = ts / 1000
            return datetime.fromtimestamp(ts)
        
        if isinstance(ts_value, str):
            ts_str = ts_value.strip()
            
            # Check if it's a numeric string (Unix timestamp)
            if ts_str.replace('.', '').isdigit():
                ts = float(ts_str)
                if ts > 1e12:  # Milliseconds
                    ts = ts / 1000
                return datetime.fromtimestamp(ts)
            
            # ISO format
            if 'T' in ts_str:
                return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        
        return None
    except (ValueError, TypeError, OSError):
        return None


def format_timestamp(ts: str, use_discord_format: bool = True) -> Optional[str]:
    """Format a timestamp string nicely.
    
    Args:
        ts: Timestamp string (ISO format or other)
        use_discord_format: If True, returns Discord dynamic timestamp <t:UNIX:f>
                           which automatically shows in user's local timezone
    """
    if not ts:
        return None
    try:
        # Check for obviously bad dates
        if ts.startswith('3408') or ts.startswith('9999'):
            return None
        
        dt = None
        
        # Try ISO format (2026-01-07T14:30:00Z)
        if 'T' in str(ts):
            dt = datetime.fromisoformat(str(ts).replace('Z', '+00:00'))
        
        # Try RSS format (Wed, 07 Jan 2026 03:56:18 GMT)
        elif ',' in str(ts) and 'GMT' in str(ts):
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(ts)
        
        if dt and use_discord_format:
            # Convert to Unix timestamp for Discord format
            unix_ts = int(dt.timestamp())
            return f"<t:{unix_ts}:f>"  # Full date/time in user's local timezone
        elif dt:
            return dt.strftime('%H:%M %d/%m/%Y')
        
        return str(ts)
    except (ValueError, TypeError, OSError, AttributeError):
        return str(ts) if ts else None


def build_map_url(lat: float, lon: float, label: str = "", layer: str = "incidents", zoom: int = 15) -> str:
    """Build a URL to the AusAware map"""
    label_encoded = quote(label, safe='') if label else ''
    return f"{MAP_BASE_URL}/map.html?lat={lat}&lng={lon}&zoom={zoom}&label={label_encoded}&layer={layer}"


# Only build URLs for paths that match exactly what the uploader generates.
# A malformed/hostile `file` value (legacy row, DB tampering) yields None
# rather than an arbitrary URL — same defence the map/logs pages apply.
_INCIDENT_IMAGE_PATH_RE = re.compile(
    r'^/uploads/incident-images/[A-Za-z0-9._-]{1,64}/[A-Za-z0-9-]{1,64}\.(?:jpe?g|png|webp|gif)$'
)


def build_incident_image_url(file_path: str, width: int = 1024) -> Optional[str]:
    """Full public URL for an incident photo, resized by Cloudflare.

    `file_path` is the stored root-relative path
    (/uploads/incident-images/<id>/<uuid>.<ext>). Resizing keeps the payload
    small enough for Discord's media proxy (originals can be up to 50MB).
    Returns None for anything that doesn't match the expected shape.
    """
    if not isinstance(file_path, str) or not _INCIDENT_IMAGE_PATH_RE.match(file_path):
        return None
    return f"{MAP_BASE_URL}/cdn-cgi/image/width={width},quality=80,format=auto{file_path}"


def representative_lonlat(coords: Any):
    """Reduce a GeoJSON `coordinates` array to one representative
    (lon, lat) numeric pair, regardless of geometry type.

    Handles Point `[lon, lat]`, LineString `[[lon, lat], ...]`, Polygon
    `[[[lon, lat], ...]]`, and MultiPolygon (deeper) by descending through
    the nesting and picking the middle element at each level until it
    reaches a numeric pair. Returns `(lon, lat)` floats, or `(None, None)`
    when the structure isn't usable — callers must guard for that so a
    non-Point geometry never produces a `lat=[...]` garbage map link.
    """
    node = coords
    # Bounded descent: Point=0, LineString=1, Polygon=2, MultiPolygon=3
    # levels of list nesting above the coordinate pair; 6 is ample headroom.
    for _ in range(6):
        if not isinstance(node, (list, tuple)) or not node:
            return None, None
        first = node[0]
        if isinstance(first, (int, float)):
            # `node` is itself a coordinate pair [lon, lat, ...].
            if len(node) >= 2 and isinstance(node[1], (int, float)):
                return float(node[0]), float(node[1])
            return None, None
        # `node` is a list of sub-arrays — descend into the middle one.
        node = node[len(node) // 2]
    return None, None


def _embed_char_size(embed: discord.Embed) -> int:
    """Approximate the char count Discord charges against the 6000 per-message
    embed budget. Covers title, description, footer.text, author.name, and
    each field's name + value."""
    size = 0
    if embed.title:
        size += len(embed.title)
    if embed.description:
        size += len(embed.description)
    footer = getattr(embed, 'footer', None)
    if footer is not None and getattr(footer, 'text', None):
        size += len(footer.text)
    author = getattr(embed, 'author', None)
    if author is not None and getattr(author, 'name', None):
        size += len(author.name)
    for field in embed.fields:
        size += len(field.name or '') + len(field.value or '')
    return size


def _container_char_size(container) -> int:
    """Approximate char count of all TextDisplay content inside a Components-V2
    Container. Used for message-budget splitting."""
    size = 0
    try:
        for child in getattr(container, 'children', []) or []:
            content = getattr(child, 'content', None)
            if content:
                size += len(content)
    except Exception:
        pass
    return size


def _truncate_container_inplace(container, max_chars: int) -> int:
    """Clamp the TextDisplay text inside a Container so the total stays
    under `max_chars`. Returns the resulting size. Last overflowing
    TextDisplay is trimmed with a "(truncated)" marker; subsequent ones are
    blanked. Used as a safety net before sending so a single chunk can
    never exceed Discord's 4000-char hard cap on V2 message text."""
    cumulative = 0
    marker = "\n\n*… truncated to fit Discord limit.*"
    for child in list(getattr(container, 'children', []) or []):
        content = getattr(child, 'content', None)
        if not content:
            continue
        room = max_chars - cumulative
        if room <= 0:
            # No headroom left — collapse to a tiny marker so the child
            # still renders but contributes ~nothing.
            try:
                child.content = "*…*"
            except Exception:
                pass
            cumulative += 3
            continue
        if len(content) > room:
            keep = max(0, room - len(marker))
            try:
                child.content = content[:keep].rstrip() + marker
            except Exception:
                pass
            cumulative = max_chars
        else:
            cumulative += len(content)
    return cumulative


def _container_component_count(container) -> int:
    """Approximate the number of Components-V2 components a Container uses,
    including nested children (ActionRow buttons, section accessories).
    Discord caps a single message at 40 components, so message packing has
    to budget on this in addition to the char limit."""
    n = 1  # the container itself
    try:
        for child in getattr(container, 'children', []) or []:
            n += 1
            for sub in getattr(child, 'children', []) or []:
                n += 1
    except Exception:
        pass
    return n


def _truncate_container_components_inplace(container, max_components: int) -> int:
    """Drop trailing children from a Container until its component count
    fits `max_components`, then append a marker so the clip is visible.

    Discord HARD-rejects (400) a message whose component count exceeds 40,
    so a single container that's over budget on components — independent of
    its char size — must be trimmed before it's packed, or the whole chunk
    fails to send. We reserve one slot for the marker we add at the end."""
    if not isinstance(getattr(container, 'children', None), list):
        return _container_component_count(container)
    target = max(1, max_components - 1)  # leave room for the marker below
    remove = getattr(container, 'remove_item', None)
    guard = 0
    # IMPORTANT: discord.py's Container.children property returns a COPY
    # of the internal list, so it must be re-read every iteration. The
    # previous version snapshotted it once: after the first removal the
    # stale copy kept handing back the same already-removed child, which
    # Container.remove_item silently ignores (it swallows ValueError), so
    # the loop spun to the guard cap having removed exactly ONE child —
    # and the still-over-budget container later made view.add_item raise,
    # killing the whole dispatch cycle.
    while guard < 500:
        guard += 1
        live = getattr(container, 'children', None)
        if not isinstance(live, list) or len(live) <= 1:
            break
        if _container_component_count(container) <= target:
            break
        before = len(live)
        removed = False
        if callable(remove):
            try:
                remove(live[-1])
                after = getattr(container, 'children', None)
                removed = isinstance(after, list) and len(after) < before
            except Exception:
                removed = False
        if not removed:
            # Public API made no progress — fall back to the underlying
            # list (private but stable in discord.py 2.x); bail rather
            # than loop forever if even that isn't there.
            raw = getattr(container, '_children', None)
            if isinstance(raw, list) and raw:
                raw.pop()
            else:
                break
    try:
        container.add_item(
            discord.ui.TextDisplay(content="*… truncated to fit Discord limit.*")
        )
    except Exception:
        pass
    return _container_component_count(container)


def chunk_containers_for_message(containers: list, max_chars: int = 3600,
                                 max_components: int = 36,
                                 max_per_message: int = 25) -> list:
    """Split Components-V2 Container objects into groups that each fit under
    Discord's per-message budget, packing as MANY as fit per message.

    Discord enforces two hard per-message ceilings: ~4000 chars of total
    displayable text, and 40 total components. We pack up to both budgets
    (max_chars=3600 leaves headroom for button labels / role-ping mentions;
    max_components=36 leaves room for the role-ping TextDisplay added to the
    first chunk). Packing tightly is what keeps a Waze burst (hundreds of
    small alerts) from exploding into hundreds of one- or two-alert messages
    that overflow the send queue. `max_per_message` is just a soft backstop;
    the char/component budgets normally bind first.

    A *single* container can exceed the char limit on its own (e.g. a busy
    radio summary) — when detected, truncate it in-place rather than letting
    Discord 400 us."""
    groups = []
    current = []
    current_size = 0
    current_components = 0
    for c in containers:
        sz = _container_char_size(c)
        # Defensive: if a single container is over budget, trim it before
        # we even try to pack. Without this, a 5000-char container ends up
        # in its own (size > max_chars) group and Discord rejects the send.
        if sz > max_chars:
            sz = _truncate_container_inplace(c, max_chars)
        cc = _container_component_count(c)
        # Defensive: a single container can also blow the component ceiling
        # on its own (e.g. a busy radio summary with many sections). Trim
        # its components too — otherwise it lands in a solo group that, with
        # the chunk-0 role-ping TextDisplay, can exceed Discord's hard 40
        # and the whole message 400s.
        if cc > max_components:
            cc = _truncate_container_components_inplace(c, max_components)
        would_exceed = current and (
            current_size + sz > max_chars
            or current_components + cc > max_components
            or len(current) >= max_per_message
        )
        if would_exceed:
            groups.append(current)
            current = []
            current_size = 0
            current_components = 0
        current.append(c)
        current_size += sz
        current_components += cc
    if current:
        groups.append(current)
    return groups


# ---------------------------------------------------------------------------
# The shared card
# ---------------------------------------------------------------------------
#
# Every alert renders through one layout. Before this each of the thirteen
# builders invented its own, so no two sources looked alike and the same facts
# were printed twice — the location line repeating the title, the incident type
# named in the heading and again in the status line.
#
# The body carries NO emoji. One icon in the heading is an identity; six in a
# row is noise, and at a glance it was impossible to tell which glyph meant
# severity and which meant "this is a fire". Field labels do that job in plain
# words that also survive being read aloud by a screen reader.
#
#     [icon] Phillip St, St Marys
#     **Advice** - Being controlled
#     Type   Grass Fire · 0 ha
#     Area   Penrith · Fire and Rescue NSW
#
#     NSW RFS · 2 hours ago
#     [Map] [Details]

# Words a title-caser must not touch. Upstream feeds shout their titles
# (QLD sends "FIRE VEGETATION - LAMMERMOOR"), and naive .title() would turn
# every one of these into "Rfs" or "Mva".
_KEEP_UPPER = {
    'RFS', 'CFA', 'SES', 'MFS', 'CFS', 'DFES', 'QFD', 'QFES', 'EMV', 'ESTA',
    'NSW', 'VIC', 'QLD', 'WA', 'SA', 'NT', 'ACT', 'TAS', 'AFP', 'BOM',
    'MVA', 'CBD', 'LPG', 'ATV', 'SUV', 'EV', 'ETA', 'GPS', 'UHF', 'VHF',
    'NE', 'NW', 'SE', 'SW',
}
# Deliberately NOT here: ST, RD, HWY, AVE, PDE, CRES. They look like acronyms
# but they are street suffixes, and "Illawong ST" reads worse than
# "Illawong St" — the point of calming a shouted address is to calm all of it.



def _detitle_word(word: str) -> str:
    bare = word.strip('.,;:()[]-/')
    if not bare or bare in _KEEP_UPPER:
        return word
    return word.replace(bare, bare.capitalize(), 1)


def smart_title(text: str) -> str:
    """Calm down a SHOUTED title, leaving mixed-case text alone.

    Works PER WORD, not per string: feeds shout inconsistently, and
    "Oceana Cres, LAMMERMOOR" needs the suburb fixed while the rest is already
    fine. A word that is not entirely upper-case is left exactly as it is,
    which is what protects names like "McKinnon" and "St Marys" from being
    re-cased into nonsense.
    """
    t = (text or '').strip()
    if not t:
        return ''
    out = []
    for word in t.split(' '):
        letters = [c for c in word if c.isalpha()]
        # Untouched unless the word is ALL capitals and long enough to be a
        # word rather than an initial.
        if len(letters) >= 2 and all(c.isupper() for c in letters):
            out.append(_detitle_word(word))
        else:
            out.append(word)
    return ' '.join(out)


def _norm_compare(text: str) -> str:
    """Letters and digits only, lowered — for deciding whether two strings say
    the same thing. Punctuation, case and spacing differ constantly between a
    feed's title and its location field, and none of it matters here."""
    return re.sub(r'[^a-z0-9]', '', (text or '').lower())


def says_the_same(a: str, b: str) -> bool:
    """True when `b` adds nothing to `a`.

    The St Marys case: a title of "Phillip St, St Marys" followed by a location
    of "Phillip St, St Marys NSW 2760" is the same fact with a postcode glued
    on. Containment either way counts, since feeds disagree about which field
    gets the fuller version.
    """
    na, nb = _norm_compare(a), _norm_compare(b)
    if not na or not nb:
        return False
    return na in nb or nb in na


# Endpoints whose payload is a GeoJSON Feature the interstate container reads.
# At module level, not on the class: a comprehension inside a class body gets
# its own scope and cannot see sibling class attributes, so deriving the set
# in there raises NameError at import — which takes the whole bot down.
_INTERSTATE_ENDPOINTS = (
    '/api/vic-emergency/',
    '/api/qld-fire/',
    '/api/wa-emergency/',
    '/api/nt-fire/',
    '/api/sa-fire/',
    '/api/act-ambulance/',
)
_INCIDENT_TYPES = frozenset(
    ['rfs'] + [
        t['key'] for t in alert_catalog.TYPE_DEFS
        if str(t.get('endpoint') or '').startswith(_INTERSTATE_ENDPOINTS)
    ]
)


class EmbedBuilder:
    """Builds beautiful Discord embeds for different alert types"""
    
    # Color scheme for different alert types
    # Embed accents, from shared/alert-catalog.json — the same hexes the map
    # layers and incident cards use, so an agency is one colour everywhere.
    # pager/pager_stop are not alert types (pager runs off the pager_enabled
    # column), so they stay hand-listed here.
    COLORS = {
        **{k: v for k, v in (
            (t['key'], alert_catalog.color(t['key']))
            for t in alert_catalog.TYPE_DEFS) if v is not None},
        'pager': 0x32CD32,
        'pager_stop': 0x228B22,
    }
    
    # Colors for specific incident types extracted from title
    INCIDENT_TYPE_COLORS = {
        'crash': 0xEF4444,              # Red
        'hazard': 0xEAB308,             # Yellow
        'breakdown': 0x6366F1,          # Indigo
        'changed traffic conditions': 0xF97316,  # Orange
        'traffic lights blacked out': 0xF59E0B,  # Amber
        'grass fire': 0xDC2626,         # Dark red
        'building fire': 0xDC2626,      # Dark red
        'bush fire': 0xDC2626,          # Dark red
        'fire': 0xDC2626,               # Dark red
        'smoke': 0x9CA3AF,              # Gray
        'adverse weather': 0x0EA5E9,    # Sky blue
        'flood': 0x0EA5E9,              # Sky blue
        'roadwork': 0xA855F7,           # Purple
        'road closure': 0xA855F7,       # Purple
        'clearways': 0x3B82F6,          # Blue
        'special event clearways': 0x3B82F6,  # Blue (clearways takes priority)
        'major event clearways': 0x3B82F6,    # Blue (clearways takes priority)
        'holiday traffic expected': 0xA855F7, # Purple
        'special event': 0x8B5CF6,      # Violet
        'major event': 0xFF8C00,        # Dark orange
    }
    
    # Icons for specific incident types
    INCIDENT_TYPE_ICONS = {
        'crash': '💥',
        'hazard': '⚠️',
        'breakdown': '🚗',
        'changed traffic conditions': '🚧',
        'traffic lights blacked out': '🚦',
        'grass fire': '🔥',
        'building fire': '🏠🔥',
        'bush fire': '🔥',
        'fire': '🔥',
        'smoke': '💨',
        'adverse weather': '🌧️',
        'flood': '🌊',
        'roadwork': '🚧',
        'road closure': '🚫',
        'clearways': '🅿️',
        'special event clearways': '🅿️',
        'major event clearways': '🅿️',
        'holiday traffic expected': '🚗',
        'special event': '🎉',
        'major event': '🎪',
    }
    
    # BOM severity colors
    BOM_SEVERITY_COLORS = {
        'severe': 0xFF0000,    # Red for severe/emergency
        'warning': 0xFFA500,   # Orange for warnings
        'watch': 0xFFFF00,     # Yellow for watch
        'advice': 0x00BFFF,    # Light blue for advice
        'info': 0x6495ED,      # Default blue
    }
    
    # BOM category colors
    BOM_CATEGORY_COLORS = {
        'land': 0x1E90FF,
        'marine': 0x4169E1,
        'general': 0x6495ED,
    }
    
    ICONS = {
        **{t['key']: t['icon'] for t in alert_catalog.TYPE_DEFS if t.get('icon')},
        'pager': '📟',
    }
    
    # BOM category icons
    BOM_CATEGORY_ICONS = {
        'land': '🌍',
        'marine': '🌊',
        'general': '📢',
    }
    
    def _parse_rfs_description(self, description: str) -> Dict[str, str]:
        """Parse RFS description text into structured fields
        
        Format: ALERT LEVEL: xxx <br />LOCATION: xxx <br />COUNCIL AREA: xxx <br />...
        Or after cleaning: ALERT LEVEL: xxx LOCATION: xxx COUNCIL AREA: xxx ...
        """
        fields = {}
        if not description:
            return fields
        
        # Clean HTML tags but preserve structure for parsing
        # Replace <br /> and similar with a delimiter
        clean_desc = re.sub(r'<br\s*/?>', ' | ', description)
        clean_desc = re.sub(r'<[^>]+>', '', clean_desc)
        clean_desc = re.sub(r'\s+', ' ', clean_desc).strip()
        
        # Extract alert level from the start
        alert_match = re.match(r'^(Advice|Watch and Act|Emergency Warning|Emergency)\s*[:|]?\s*', clean_desc, re.IGNORECASE)
        if alert_match:
            fields['alert_level'] = alert_match.group(1).strip()
        
        # Also check for ALERT LEVEL: prefix format
        alert_level_match = re.search(r'ALERT\s*LEVEL:\s*([^|]+?)(?=\s*\||$)', clean_desc, re.IGNORECASE)
        if alert_level_match:
            fields['alert_level'] = alert_level_match.group(1).strip()
        
        # Extract fields - look for FIELD: value patterns
        field_patterns = [
            ('location', r'LOCATION:\s*([^|]+?)(?=\s*\||COUNCIL|STATUS|TYPE|FIRE|SIZE|RESPONSIBLE|UPDATED|$)'),
            ('council_area', r'COUNCIL\s*AREA:\s*([^|]+?)(?=\s*\||STATUS|TYPE|FIRE|SIZE|RESPONSIBLE|UPDATED|$)'),
            ('status', r'STATUS:\s*([^|]+?)(?=\s*\||TYPE|FIRE|SIZE|RESPONSIBLE|UPDATED|$)'),
            ('type', r'TYPE:\s*([^|]+?)(?=\s*\||FIRE:|SIZE|RESPONSIBLE|UPDATED|$)'),
            ('size', r'SIZE:\s*([^|]+?)(?=\s*\||RESPONSIBLE|UPDATED|$)'),
            ('responsible_agency', r'RESPONSIBLE\s*AGENCY:\s*([^|]+?)(?=\s*\||UPDATED|$)'),
            ('updated', r'UPDATED:\s*([^|]+?)(?=\s*\||$)'),
        ]
        
        for field_name, pattern in field_patterns:
            match = re.search(pattern, clean_desc, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                # Clean up any trailing pipes or whitespace
                value = re.sub(r'\s*\|\s*$', '', value)
                if value and value.lower() not in ['yes', 'no']:
                    fields[field_name] = value
        
        return fields
    
    # Severity → embed colour mapping for incident embeds
    _RADIO_SEV_COLORS = {
        'critical': 0xdc2626,
        'high':     0xef4444,
        'emergency':0xef4444,
        'medium':   0xf97316,
        'moderate': 0xf97316,
        'low':      0x3b82f6,
        'info':     0x94a3b8,
        'routine':  0x94a3b8,
    }

    _RADIO_MAIN_COLOR = 0x8b5cf6  # purple — distinguishes from other alert types

    @staticmethod
    def _radio_incidents_from_details(details: Dict[str, Any]) -> list:
        """Pull the incidents list out of a summary row's details dict.

        The LLM output lands under `details.structured.incidents` (validated
        structured output). Legacy rows sometimes wrote directly to
        `details.incidents`. Support both so we don't miss cards depending
        on which code path wrote the row.
        """
        if not isinstance(details, dict):
            return []
        structured = details.get('structured')
        if isinstance(structured, dict):
            inc = structured.get('incidents')
            if isinstance(inc, list) and inc:
                return inc
        inc = details.get('incidents')
        return inc if isinstance(inc, list) else []

    # ============================================================
    # Components V2 path — wider, less cluttered alternative to embeds.
    # Requires discord.py 2.6+. When enabled (default on radio_summary),
    # we build a list of discord.ui.Container objects instead of embeds
    # and send them via a LayoutView. Char budget is managed separately
    # from the embeds path.
    # ============================================================

    @staticmethod
    def _radio_hour_range(data: Dict[str, Any], details: Dict[str, Any]) -> str:
        """Shared hour-range formatter — '5 AM – 6 AM' style."""
        from datetime import datetime as _dt
        start_iso = data.get('period_start')
        end_iso = data.get('period_end')
        tz_name = details.get('tz') or 'Australia/Sydney'
        try:
            start = _dt.fromisoformat(start_iso.replace('Z', '+00:00')) if start_iso else None
            end = _dt.fromisoformat(end_iso.replace('Z', '+00:00')) if end_iso else None
            if start is None or end is None:
                return ''
            try:
                from zoneinfo import ZoneInfo
                tz = ZoneInfo(tz_name)
                start = start.astimezone(tz)
                end = end.astimezone(tz)
            except Exception:
                pass
            fmt = '%-I %p' if os.name != 'nt' else '%#I %p'
            try:
                return f"{start.strftime(fmt)} – {end.strftime(fmt)}"
            except Exception:
                return f"{start.strftime('%I %p').lstrip('0')} – {end.strftime('%I %p').lstrip('0')}"
        except Exception:
            return ''

    # Human-readable labels for alert_type values. Mirrored from bot.ALERT_TYPES
    # — duplicated here to avoid a circular import. Keep in sync when new
    # alert types are added.
    # Labels from shared/alert-catalog.json — a duplicate of bot.py
    # ALERT_TYPES until both were pointed at the catalog.
    _ALERT_TYPE_LABELS = dict(alert_catalog.LABELS)

    _ALERT_LIST_COLOR = 0x3498db  # blue accent for /alert-list containers

    @staticmethod
    def _alert_list_status_glyph(cfg: Dict[str, Any]) -> str:
        """Status glyph for an alert/pager config row.

        🟢 active  (enabled != 0, enabled_ping != 0 — NULL counts as active)
        🔕 muted   (enabled != 0, enabled_ping == 0 — pings stripped)
        🔇 silenced(enabled == 0 — entirely off)
        """
        enabled = cfg.get('enabled')
        enabled_ping = cfg.get('enabled_ping')
        if enabled == 0:
            return '🔇'
        if enabled_ping == 0:
            return '🔕'
        return '🟢'

    @staticmethod
    def _alert_list_role_chips(role_ids) -> str:
        """Render a list of role ids as ' <@&id1> <@&id2>' (with leading space)
        or empty string when there are none."""
        if not role_ids:
            return ''
        return ' ' + ' '.join(f'<@&{int(r)}>' for r in role_ids)

    def build_alert_list_components(
        self,
        presets: list,
        guild,
    ) -> list:
        """Return a list of `discord.ui.Container` objects for /alert-list.

        One container per channel that has at least one preset for `guild`.
        Each preset becomes a sub-block within its channel's container; when
        a channel has multiple presets (dashboard users), the preset name is
        shown as a sub-header. `presets` is the list returned by
        `db.list_presets_in_guild`. `guild` is the `discord.Guild` used for
        channel lookups — when a channel has been deleted we fall back to the
        raw `<#id>` mention (Discord renders it as a greyed-out
        "deleted-channel" chip).
        """
        def _roles(preset):
            raw = preset.get('role_ids') or []
            if isinstance(raw, list):
                return [int(r) for r in raw if r is not None]
            return []

        def _preset_status_glyph(preset):
            """Top-level glyph for a preset row — based on its own enabled /
            enabled_ping flags. Per-type / channel / guild mute aren't
            resolved here because /alert-list shows the preset's stored state."""
            if not preset.get('enabled', True):
                return '🔇'
            if not preset.get('enabled_ping', True):
                return '🔕'
            return '🟢'

        # Group presets by channel_id, preserving list order within a channel.
        by_channel: Dict[int, list] = {}
        for preset in presets or []:
            ch_id = preset.get('channel_id')
            if ch_id is None:
                continue
            by_channel.setdefault(ch_id, []).append(preset)

        # Preserve a stable order: by guild channel position if available,
        # tiebroken by channel_id. Deleted channels sort to the bottom.
        def _sort_key(ch_id: int):
            ch = guild.get_channel(ch_id) if guild is not None else None
            pos = getattr(ch, 'position', None)
            return (pos if pos is not None else 1 << 31, ch_id)

        containers = []
        for ch_id in sorted(by_channel.keys(), key=_sort_key):
            channel_presets = by_channel[ch_id]
            if not channel_presets:
                continue

            channel = guild.get_channel(ch_id) if guild is not None else None
            if channel is not None:
                title_line = f"### {channel.mention}"
            else:
                title_line = (
                    f"### <#{ch_id}>\n"
                    f"-# _channel unavailable · id {ch_id}_"
                )

            container = discord.ui.Container(accent_colour=self._ALERT_LIST_COLOR)
            container.add_item(discord.ui.TextDisplay(content=title_line))

            multi = len(channel_presets) > 1
            for idx, preset in enumerate(channel_presets):
                if idx > 0:
                    container.add_item(discord.ui.Separator())

                lines: list = []
                if multi:
                    pname = preset.get('name') or f"preset {preset.get('id')}"
                    lines.append(f"**Preset:** `{pname}`")

                preset_glyph = _preset_status_glyph(preset)
                role_ids = _roles(preset)
                roles_text = self._alert_list_role_chips(role_ids)

                alert_types = preset.get('alert_types') or []
                if alert_types:
                    lines.append(f"**Alerts** {preset_glyph}{roles_text}")
                    overrides = preset.get('type_overrides') or {}
                    for atype in sorted(alert_types):
                        label = self._ALERT_TYPE_LABELS.get(atype, atype)
                        ov = overrides.get(atype) if isinstance(overrides, dict) else None
                        # Per-type override view — fall back to preset glyph.
                        if isinstance(ov, dict):
                            cfg_view = {
                                'enabled': 0 if ov.get('enabled') is False else 1,
                                'enabled_ping': 0 if ov.get('enabled_ping') is False else 1,
                            }
                            t_glyph = self._alert_list_status_glyph(cfg_view)
                        else:
                            t_glyph = '🟢'
                        lines.append(f"- {t_glyph} **{label}**")

                if preset.get('pager_enabled'):
                    if alert_types:
                        lines.append("")  # spacer line before pager block
                    raw_capcodes = preset.get('pager_capcodes')
                    if isinstance(raw_capcodes, list):
                        capcode_list = [str(c).strip() for c in raw_capcodes if str(c).strip()]
                    elif raw_capcodes:
                        capcode_list = [c.strip() for c in str(raw_capcodes).split(',') if c.strip()]
                    else:
                        capcode_list = []
                    if capcode_list:
                        shown = ', '.join(f'`{c}`' for c in capcode_list[:6])
                        if len(capcode_list) > 6:
                            shown += f' _+{len(capcode_list) - 6} more_'
                        capcode_desc = f"capcodes: {shown}"
                    else:
                        capcode_desc = "all messages"
                    overrides = preset.get('type_overrides') or {}
                    pg_ov = overrides.get('__pager__') if isinstance(overrides, dict) else None
                    if isinstance(pg_ov, dict):
                        cfg_view = {
                            'enabled': 0 if pg_ov.get('enabled') is False else 1,
                            'enabled_ping': 0 if pg_ov.get('enabled_ping') is False else 1,
                        }
                        pg_glyph = self._alert_list_status_glyph(cfg_view)
                    else:
                        pg_glyph = preset_glyph
                    lines.append("**Pager**")
                    lines.append(f"- {pg_glyph} {capcode_desc}{roles_text}")

                if not lines:
                    continue
                block_text = '\n'.join(lines)
                if len(block_text) > 3800:
                    block_text = block_text[:3797] + '…'
                container.add_item(discord.ui.TextDisplay(content=block_text))

            containers.append(container)

        return containers

    def build_radio_summary_components(self, data: Dict[str, Any]) -> list:
        """Return a list of `discord.ui.Container` objects for Components V2.

        [0] Main summary container (purple accent).
        [1..N] One per incident, colour-coded by severity.
        """
        details = data.get('details') or {}
        if isinstance(details, str):
            import json as _json
            try:
                details = _json.loads(details)
            except Exception:
                details = {}

        containers = [self._build_radio_main_container(data, details)]
        incidents = self._radio_incidents_from_details(details)
        for inc in incidents[:9]:
            containers.append(self._build_radio_incident_container(inc))
        return containers

    def _build_radio_main_container(self, data: Dict[str, Any], details: Dict[str, Any]):
        """Main summary container — heading + body + footer-style metadata."""
        summary_type = (data.get('type') or 'hourly').lower()
        day = data.get('day_date') or ''
        hour_range = self._radio_hour_range(data, details)

        if summary_type == 'hourly' and hour_range and day:
            title_line = f"## 📻 Radio Summary — {day} · {hour_range}"
        elif summary_type == 'hourly' and hour_range:
            title_line = f"## 📻 Radio Summary — {hour_range}"
        elif summary_type == 'adhoc':
            title_line = "## 📻 Radio Summary — Recent Activity"
        else:
            title_line = "## 📻 Radio Summary"

        summary_text = (data.get('summary') or '').strip() or '_(no summary)_'
        # Hard clamp each TextDisplay at ~3800 chars (4000 Discord limit)
        if len(summary_text) > 3800:
            summary_text = summary_text[:3797] + '…'

        footer_bits = ["📡 rdio-scanner (self-hosted)"]
        call_count = data.get('call_count')
        if call_count is not None:
            footer_bits.append(f"{call_count} calls")
        model = data.get('model')
        if model and model != 'none':
            footer_bits.append(model)
        footer_line = ' · '.join(footer_bits)

        container = discord.ui.Container(accent_colour=self._RADIO_MAIN_COLOR)
        container.add_item(discord.ui.TextDisplay(content=title_line))
        container.add_item(discord.ui.Separator())
        container.add_item(discord.ui.TextDisplay(content=summary_text))
        if footer_line:
            container.add_item(discord.ui.Separator(visible=False))
            container.add_item(discord.ui.TextDisplay(content=f"-# {footer_line}"))
        return container

    def _build_radio_incident_container(self, inc: Dict[str, Any]):
        """One Container per incident — cleaner analogue to _build_radio_incident_embed."""
        sev = (inc.get('severity') or '').strip().lower()
        color = self._RADIO_SEV_COLORS.get(sev, self._RADIO_MAIN_COLOR)

        title_part = strip_html(str(inc.get('title') or inc.get('type') or 'Incident'))
        sev_label = f"[{sev.upper()}] " if sev else ""
        title_line = f"### 🚨 {sev_label}{title_part}"

        # Body: summary, then location line, then compact meta line.
        body_bits = []
        summary = strip_html(str(inc.get('summary') or inc.get('description') or ''))
        if summary:
            if len(summary) > 1500:
                summary = summary[:1497] + '…'
            body_bits.append(summary)

        locations = inc.get('locations') or []
        if isinstance(locations, list) and locations:
            loc_txt = ' · '.join(strip_html(str(l)) for l in locations if l)
            if loc_txt:
                body_bits.append(f"📍 {loc_txt}")
        elif inc.get('location') or inc.get('suburb'):
            body_bits.append(f"📍 {strip_html(str(inc.get('location') or inc.get('suburb')))}")

        meta_parts = []
        window = inc.get('window') or {}
        if isinstance(window, dict) and window.get('start'):
            end = window.get('end') or window.get('start')
            meta_parts.append(f"🕐 {window['start']}–{end}")
        status = inc.get('status')
        if status:
            meta_parts.append(f"**{status}**")
        agencies = inc.get('agencies') or []
        if agencies:
            meta_parts.append(', '.join(agencies))
        codes = inc.get('codes') or []
        if codes:
            meta_parts.append(', '.join(codes))
        units_raw = inc.get('units') or []
        units = []
        for u in units_raw:
            if isinstance(u, str):
                label = u
            elif isinstance(u, dict):
                label = u.get('id') or u.get('callsign')
            else:
                label = None
            if not label:
                continue
            # Drop bare "UID:<n>" tokens (unresolved unit ids) — noise.
            if str(label).strip().upper().startswith('UID:'):
                continue
            units.append(str(label))
        if units:
            meta_parts.append(f"Units: {', '.join(units)}")
        if meta_parts:
            body_bits.append(' · '.join(meta_parts))

        # Transcripts — compact, one line per call. Show up to 30 per incident
        # (matches the LLM's emit cap). If the LLM's structured output was
        # truncated to 30, append a footer noting that more existed.
        RADIO_CALL_URL = "https://radio.forcequit.xyz/?call={id}"
        MAX_TRANSCRIPTS_PER_INCIDENT = 30
        transcripts_lines = []
        transcripts = inc.get('transcripts') or []
        if isinstance(transcripts, list) and transcripts:
            shown = transcripts[:MAX_TRANSCRIPTS_PER_INCIDENT]
            header = f"**📻 Transcripts ({len(shown)})**"
            transcripts_lines.append(header)
            for t in shown:
                if not isinstance(t, dict):
                    continue
                time_s = t.get('time', '??:??')
                cid = t.get('call_id')
                text = strip_html(str(t.get('text') or ''))
                if len(text) > 220:
                    text = text[:217] + '…'
                try:
                    cid_int = int(cid) if cid is not None else None
                except (TypeError, ValueError):
                    cid_int = None
                link = f"[#{cid_int}]({RADIO_CALL_URL.format(id=cid_int)})" if cid_int else ''
                transcripts_lines.append(f"`{time_s}` {link} {text}".strip())
            if inc.get('transcripts_truncated'):
                transcripts_lines.append(
                    f"-# _Showing top {len(shown)} transcripts — more existed in the source hour_"
                )

        container = discord.ui.Container(accent_colour=color)
        container.add_item(discord.ui.TextDisplay(content=title_line))
        if body_bits:
            body_text = '\n'.join(body_bits)
            if len(body_text) > 3800:
                body_text = body_text[:3797] + '…'
            container.add_item(discord.ui.TextDisplay(content=body_text))
        if transcripts_lines:
            container.add_item(discord.ui.Separator())
            # Each TextDisplay is capped at ~4000 chars. With up to 30 transcripts
            # per incident the block can exceed that, so pack lines into multiple
            # TextDisplays within this same container instead of truncating.
            TD_SOFT_CAP = 3500
            buf = []
            buf_len = 0
            for line in transcripts_lines:
                line_len = len(line) + 1  # + newline
                if buf and buf_len + line_len > TD_SOFT_CAP:
                    container.add_item(discord.ui.TextDisplay(content='\n'.join(buf)))
                    buf = []
                    buf_len = 0
                buf.append(line)
                buf_len += line_len
            if buf:
                container.add_item(discord.ui.TextDisplay(content='\n'.join(buf)))
        return container

    # ============================================================
    # /overview command — Components V2 dashboard of incident totals.
    # Backed by GET /api/stats/summary. Each section becomes its own
    # colour-accented Container so the feed reads top-to-bottom rather
    # than squeezing 10+ inline fields into a single embed.
    # ============================================================

    # Per-section accent palette. Kept distinct from _RADIO_* so the
    # summary view doesn't look like a radio alert at a glance.
    _SUMMARY_HEADER_COLOR    = 0x3498db  # blue
    _SUMMARY_POWER_COLOR     = 0xf59e0b  # amber
    _SUMMARY_TRAFFIC_COLOR   = 0xf97316  # orange
    _SUMMARY_EMERGENCY_COLOR = 0xdc2626  # red
    _SUMMARY_PAGER_COLOR     = 0x32cd32  # green (matches pager embed colour)
    _SUMMARY_FOOTER_COLOR    = 0x64748b  # slate / muted

    @staticmethod
    def _clip_text(text: str, limit: int = 3800) -> str:
        """Hard clamp a TextDisplay body to Discord's 4000-char ceiling."""
        if len(text) > limit:
            return text[: limit - 1] + '…'
        return text

    _MD_LINK_RE = re.compile(r'\[([^\]]+)\]\((https?://[^)]+)\)')

    def _append_container_footer(self, container, footer_bits, source=None):
        """Emit the footer for a V2 Container.

        Components V2 TextDisplay DOES NOT render `[label](url)` markdown as
        clickable links — the entire token renders as literal text. To get
        real clickable affordances we extract any `[label](url)` bits from
        the footer and emit them as **Link Buttons** in an ActionRow below
        the subtle-text line instead.

        `source` (optional) is the human-readable data-source label, e.g.
        "NSW RFS" or "Bureau of Meteorology". Prepended to the subtle
        footer line so every alert advertises where it came from.
        """
        if source:
            # No glyph: the time beside it is bare, and one lone emoji on an
            # otherwise plain muted line reads as a leftover.
            footer_bits = [str(source), *(footer_bits or [])]
        if not footer_bits:
            return
        subtle_parts = []
        link_buttons = []  # list of (label, url) tuples
        for bit in footer_bits:
            text = str(bit)
            m = self._MD_LINK_RE.search(text)
            if m:
                label = m.group(1).strip()
                url = m.group(2).strip()
                # strip any wrapping text from the same bit and drop it — the
                # button will carry the whole affordance. If the bit is just
                # the link (common case) this is a no-op.
                link_buttons.append((label, url))
            else:
                subtle_parts.append(text)
        if subtle_parts:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text('-# ' + ' · '.join(subtle_parts))
            ))
        if link_buttons:
            # ActionRow maxes out at 5 buttons; beyond that we'd need a
            # second row — rare for our alerts so cap defensively.
            row = discord.ui.ActionRow()
            for label, url in link_buttons[:5]:
                # Discord-safe: label max 80 chars, URL max 512 chars.
                safe_label = label[:80] if label else 'Open'
                try:
                    row.add_item(discord.ui.Button(
                        style=discord.ButtonStyle.link,
                        label=safe_label,
                        url=url,
                    ))
                except Exception:
                    # If the URL is malformed, Button() can raise — skip this
                    # one rather than blowing up the whole alert.
                    continue
            if row.children:
                container.add_item(row)

    def build_summary_components(self, stats: Dict[str, Any],
                                 pager_counts: Optional[Dict[str, int]] = None) -> list:
        """Return a list of `discord.ui.Container` objects for `/summary`.

        Sections (in order):
          [0] Header   — title + timestamp (blue)
          [1] Power    — Ausgrid + Endeavour outage totals (amber)
          [2] Traffic  — crashes / hazards / breakdowns / roadwork / fires /
                         floods / major events (orange)
          [3] Emergency — RFS major incidents + BOM warnings (red)
          [4] Pager    — rolling 1h/6h/12h/24h pager hit counts (green)
                         Only rendered when `pager_counts` is supplied.
          [5] Footer   — last-updated line + link to the live dashboard (muted)
        """
        from datetime import datetime as _dt

        stats = stats or {}
        power     = stats.get('power') or {}
        traffic   = stats.get('traffic') or {}
        emergency = stats.get('emergency') or {}

        containers = []

        # --- Header -------------------------------------------------------
        now_local = _dt.now().astimezone()
        header = discord.ui.Container(accent_colour=self._SUMMARY_HEADER_COLOR)
        header.add_item(discord.ui.TextDisplay(
            content="# 📊 Incident Summary"
        ))
        header.add_item(discord.ui.Separator(visible=False))
        header.add_item(discord.ui.TextDisplay(
            content=f"-# Current status across all monitored services · {now_local.strftime('%a %d %b · %I:%M %p %Z').lstrip('0')}"
        ))
        containers.append(header)

        # --- Power --------------------------------------------------------
        endeavour = power.get('endeavour') or {}
        ausgrid   = power.get('ausgrid') or {}
        end_current = int(endeavour.get('current') or 0)
        end_planned = int(endeavour.get('future') or 0)
        aus_outages = int(ausgrid.get('outages') or 0)
        aus_customers = int(ausgrid.get('customersAffected') or 0)

        power_lines = [
            "## ⚡ Power",
            f"**🔌 Endeavour** — ⚡ Current: **{end_current}** · 🔧 Planned: **{end_planned}**",
            f"**🔌 Ausgrid** — ⚡ Outages: **{aus_outages}**"
            + (f" · 👥 Affected: **{aus_customers:,}**" if aus_customers > 0 else ""),
        ]
        power_c = discord.ui.Container(accent_colour=self._SUMMARY_POWER_COLOR)
        power_c.add_item(discord.ui.TextDisplay(content=power_lines[0]))
        power_c.add_item(discord.ui.Separator())
        power_c.add_item(discord.ui.TextDisplay(
            content=self._clip_text('\n'.join(power_lines[1:]))
        ))
        containers.append(power_c)

        # --- Traffic ------------------------------------------------------
        crashes    = int(traffic.get('crashes') or 0)
        hazards    = int(traffic.get('hazards') or 0)
        breakdowns = int(traffic.get('breakdowns') or 0)
        total_inc  = int(traffic.get('incidents') or 0)
        roadwork   = int(traffic.get('roadwork') or 0)
        fires      = int(traffic.get('fires') or 0)
        floods     = int(traffic.get('floods') or 0)
        majors     = int(traffic.get('major_events') or traffic.get('majorEvents') or 0)
        changed    = max(0, total_inc - crashes - hazards - breakdowns)

        incident_bits = [
            f"💥 Crashes: **{crashes}**",
            f"⚠️ Hazards: **{hazards}**",
            f"🚗 Breakdowns: **{breakdowns}**",
        ]
        if changed > 0:
            incident_bits.append(f"🚧 Changed Conditions: **{changed}**")

        other_bits = [
            f"🚧 Active Roadwork: **{roadwork}**",
            f"🔥 Road Fire Hazards: **{fires}**",
            f"🌊 Flood Hazards: **{floods}**",
        ]
        if majors > 0:
            other_bits.append(f"🎪 Major Events: **{majors}**")

        traffic_c = discord.ui.Container(accent_colour=self._SUMMARY_TRAFFIC_COLOR)
        traffic_c.add_item(discord.ui.TextDisplay(content="## 🚗 Traffic"))
        traffic_c.add_item(discord.ui.Separator())
        traffic_c.add_item(discord.ui.TextDisplay(
            content=self._clip_text('\n'.join(f"• {b}" for b in incident_bits))
        ))
        traffic_c.add_item(discord.ui.Separator(visible=False))
        traffic_c.add_item(discord.ui.TextDisplay(
            content=self._clip_text('\n'.join(f"• {b}" for b in other_bits))
        ))
        containers.append(traffic_c)

        # --- Emergency ----------------------------------------------------
        rfs_count = int(emergency.get('rfs_incidents') or 0)
        bom = emergency.get('bom_warnings') or {}
        bom_land   = int(bom.get('land') or 0)
        bom_marine = int(bom.get('marine') or 0)

        emer_c = discord.ui.Container(accent_colour=self._SUMMARY_EMERGENCY_COLOR)
        emer_c.add_item(discord.ui.TextDisplay(content="## 🚨 Emergency Services"))
        emer_c.add_item(discord.ui.Separator())
        emer_c.add_item(discord.ui.TextDisplay(
            content=f"• 🔥 **RFS Major Incidents:** {rfs_count}"
        ))
        emer_c.add_item(discord.ui.Separator(visible=False))
        emer_c.add_item(discord.ui.TextDisplay(
            content=(
                "**⛈️ BOM Warnings**\n"
                f"• 🌍 Land: **{bom_land}**\n"
                f"• 🌊 Marine: **{bom_marine}**"
            )
        ))
        containers.append(emer_c)

        # --- Pager (optional) --------------------------------------------
        if pager_counts:
            pc = pager_counts
            pager_c = discord.ui.Container(accent_colour=self._SUMMARY_PAGER_COLOR)
            pager_c.add_item(discord.ui.TextDisplay(content="## 📟 Pager Hits"))
            pager_c.add_item(discord.ui.Separator())
            pager_c.add_item(discord.ui.TextDisplay(
                content=(
                    f"• Last 1h: **{int(pc.get('1h') or 0)}**\n"
                    f"• Last 6h: **{int(pc.get('6h') or 0)}**\n"
                    f"• Last 12h: **{int(pc.get('12h') or 0)}**\n"
                    f"• Last 24h: **{int(pc.get('24h') or 0)}**"
                )
            ))
            containers.append(pager_c)

        # --- Footer -------------------------------------------------------
        footer_c = discord.ui.Container(accent_colour=self._SUMMARY_FOOTER_COLOR)
        footer_c.add_item(discord.ui.TextDisplay(
            content=(
                f"-# 🌐 [View the full live dashboard on AusAware]({MAP_BASE_URL}/) · "
                f"data refreshed {now_local.strftime('%I:%M %p').lstrip('0')}"
            )
        ))
        containers.append(footer_c)

        return containers

    # ============================================================
    # /dev status — Components V2 diagnostics view for the bot owner.
    # One container per logical section so the diagnostics read
    # top-to-bottom instead of packing 10+ inline fields into a
    # single embed. Mirrors the layout used by `build_summary_components`.
    # ============================================================

    _DEV_STATUS_HEADER_COLOR   = 0x3498db  # blue — identity line
    _DEV_STATUS_DB_COLOR       = 0x22c55e  # green — database health
    _DEV_STATUS_SUBS_COLOR     = 0x14b8a6  # teal — subscriptions
    _DEV_STATUS_GUILDS_COLOR   = 0x94a3b8  # muted grey — per-guild breakdown
    _DEV_STATUS_TASKS_COLOR    = 0xf59e0b  # amber — task loops / queue
    _DEV_STATUS_FOOTER_COLOR   = 0x64748b  # slate — footer/version

    # ============================================================
    # Per-alert-type Components V2 container builders.
    #
    # These mirror the `_build_*_embed` methods but emit a single
    # `discord.ui.Container` (TextDisplay-based) instead of a
    # `discord.Embed`. They share the same COLORS / INCIDENT_TYPE_COLORS
    # / ICONS palette so users see the same colour cues they already
    # recognise from the embed path.
    #
    # Each builder returns exactly ONE Container so the caller can
    # stuff many of them into a single LayoutView for per-channel
    # batching (multiple alerts -> one message).
    # ============================================================

    def _card(self, *, icon: str, title: str, color: int,
              headline: str = '', fields=None, body: str = '',
              source: str = '', footer_bits=None):
        """Render the shared card.

        `headline`  the severity/status line, already formatted.
        `fields`    [(label, value)] — label is one short word, rendered in
                    the muted style; a None or empty value drops the row.
        `body`      free prose (advice text), after the fields.
        """
        container = discord.ui.Container(accent_colour=color)
        heading = smart_title(strip_html(title or '')) or 'Alert'
        container.add_item(discord.ui.TextDisplay(
            content=self._clip_text(f"### {icon} {heading}".strip())
        ))

        lines = []
        if headline:
            lines.append(headline)
        for label, value in (fields or []):
            if value is None:
                continue
            v = str(value).strip()
            if not v or not is_valid_value(v):
                continue
            # `-#` is Discord's small-text marker; it makes the label recede
            # without an emoji doing the work.
            lines.append(f"**{label}** {v}")
        if lines:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text('\n'.join(lines))
            ))
        if body:
            clean = strip_html(body).strip()
            if clean and is_valid_value(clean):
                container.add_item(discord.ui.TextDisplay(
                    content=self._clip_text(clean[:1200])
                ))

        self._append_container_footer(container, footer_bits or [], source=source)
        return container

    @staticmethod
    def _join(parts, sep=' · '):
        """Join the non-empty bits of a line, or return '' so the row drops."""
        got = [str(p).strip() for p in parts if p is not None and str(p).strip()]
        return sep.join(got) if got else ''

    def _update_line(self, previous_message, status: str, alert_level: str) -> str:
        """State what changed, for an update that replies to the first report.

        Written to stand alone. A reply is very often read without its parent
        in view, so "Update · now Under control" has to make sense by itself —
        which is why it names the new state rather than only the old one.
        """
        if not previous_message:
            return ''
        was = (previous_message.get('status') or '').strip()
        now = (status or alert_level or '').strip()
        if was and now and _norm_compare(was) != _norm_compare(now):
            return f"**Update** {was} → {now}"
        if now:
            return f"**Update** now {now}"
        return "**Update**"

    def build_alert_container(self, alert: Dict[str, Any],
                              previous_message: Dict[str, Any] = None):
        """Route an alert dict to the right container builder.

        Mirrors `build_alert_embed` — returns a single `discord.ui.Container`
        for all non-radio alert types. radio_summary keeps its dedicated
        multi-container builder (`build_radio_summary_components`).
        """
        alert_type = alert.get('type', 'unknown')
        data = alert.get('data', {})

        if alert_type in self._INCIDENT_TYPES:
            return self.build_incident_container(
                data, alert_type, previous_message=previous_message)
        elif alert_type == 'firms':
            return self.build_firms_container(data)
        elif alert_type.startswith('bom_'):
            return self.build_bom_container(data)
        elif alert_type.startswith('traffic_'):
            return self.build_traffic_container(data, alert_type)
        elif (alert_type.startswith('endeavour_')
              or alert_type == 'ausgrid'
              or alert_type.startswith('essential_')):
            return self.build_power_container(data, alert_type)
        elif alert_type == 'wire_article':
            return self.build_wire_article_container(data)
        elif alert_type == 'wire_fleet':
            return self.build_wire_fleet_container(data)
        elif alert_type == 'user_incident':
            return self.build_user_incident_container(data, previous_message=previous_message)
        else:
            return self.build_generic_container(data, alert_type)

    # ---- Emergency incidents (one builder, ten sources) --------
    #
    # RFS, CFA, DEECA, VICSES, EMV, ESTA, QFD, DFES, SA CFS/MFS, NT and ACT
    # all render here. That is possible because the BACKEND already normalises
    # them: every one of those feeds arrives as a GeoJSON Feature whose
    # properties are `title, status, location, alertLevel, fireType,
    # responsibleAgency, updatedISO, guid, polygons`. There was never a data
    # reason for ten different layouts.

    _LEVEL_STYLE = (
        # (match, colour, rank) — rank orders severity for the accent colour.
        ('emergency', 0xEF4444, 3),
        ('watch',     0xF97316, 2),
        ('advice',    0xFACC15, 1),
    )

    def _level_color(self, alert_level: str, default: int) -> int:
        low = (alert_level or '').lower()
        for word, color, _rank in self._LEVEL_STYLE:
            if word in low:
                return color
        return default

    def build_incident_container(self, data: Dict[str, Any], alert_type: str,
                                 previous_message: Dict[str, Any] = None):
        """The shared card for every emergency-service incident feed."""
        data = data or {}
        props = data.get('properties') or {}

        title = strip_html(props.get('title') or '')
        status = strip_html(props.get('status') or '')
        # ACT Ambulance is the one source the backend did not fully normalise:
        # it uses location_text, category/subcategory, and an epoch timestamp.
        location = strip_html(props.get('location') or props.get('location_text') or '')
        alert_level = strip_html(props.get('alertLevel') or '')
        fire_type = strip_html(
            props.get('fireType') or props.get('subcategory') or props.get('category') or ''
        )
        agency = strip_html(props.get('responsibleAgency') or '')
        council = strip_html(props.get('councilArea') or props.get('district')
                             or props.get('region') or '')
        size = strip_html(props.get('size') or props.get('sizeFmt') or '')

        # An alertLevel that arrived as a whole sentence — RFS sometimes puts
        # the full advice line in the field. Keep the leading classification.
        if alert_level and len(alert_level) > 30:
            m = re.match(r'^(Advice|Watch and Act|Emergency Warning|Emergency)',
                         alert_level, re.IGNORECASE)
            alert_level = m.group(1) if m else ''

        # RFS omits alertLevel on older records but its status implies one.
        if not alert_level and status:
            low = status.lower()
            if 'out of control' in low:
                alert_level = 'Emergency Warning'
            elif 'being controlled' in low:
                alert_level = 'Watch and Act'
            elif 'under control' in low:
                alert_level = 'Advice'

        color = self._level_color(alert_level, self.COLORS.get(alert_type, 0xEF4444))
        icon = self.ICONS.get(alert_type, '🔥')

        # The severity line. Level is the loud part; status qualifies it.
        headline = self._join(
            [f"**{alert_level}**" if alert_level else '', status], sep=' — ',
        )

        fields = []
        upd = self._update_line(previous_message, status, alert_level)
        if upd:
            fields = []          # the update line leads; it is not a field
            headline = self._join([upd, headline], sep='\n')

        # The type is dropped when the heading already says it — QLD titles are
        # literally "FIRE VEGETATION - LAMMERMOOR" beside a fireType of
        # "FIRE VEGETATION", which is what made those cards read as stutter.
        type_bits = []
        if fire_type and not says_the_same(title, fire_type):
            type_bits.append(smart_title(fire_type))
        if size:
            type_bits.append(size)
        fields.append(('Type', self._join(type_bits)))

        # The location is dropped when it merely restates the title, which for
        # most of these feeds it does — the title IS the place.
        area_bits = []
        if location and not says_the_same(title, location):
            area_bits.append(smart_title(location))
        if council:
            area_bits.append(council)
        if agency:
            area_bits.append(agency)
        fields.append(('Area', self._join(area_bits)))

        # Public advice, where the feed carries it.
        body = ''
        for key in ('action', 'whatToDo', 'adviceToPublic', 'warningText',
                    'headline', 'text', 'currentSituation'):
            v = props.get(key)
            if isinstance(v, str) and v.strip() and not says_the_same(title, v):
                body = v
                break

        footer_bits = []
        dt = parse_timestamp_to_datetime(
            props.get('updatedISO') or props.get('timestamp') or props.get('updated') or '')
        if dt:
            footer_bits.append(f"<t:{int(dt.timestamp())}:R>")

        geom = data.get('geometry') or {}
        if isinstance(geom, dict) and geom.get('coordinates') is not None:
            lon, lat = representative_lonlat(geom['coordinates'])
            if lat is not None and lon is not None:
                layer = 'rfs' if alert_type == 'rfs' else 'incidents'
                footer_bits.append(
                    f"[Map]({build_map_url(lat, lon, label=title, layer=layer)})")
        url = (props.get('url') or props.get('link') or '').strip()
        if url:
            footer_bits.append(f"[Details]({url})")

        return self._card(
            icon=icon, title=title, color=color,
            headline=headline, fields=fields, body=body,
            source=self._INCIDENT_SOURCES.get(alert_type, 'Emergency services'),
            footer_bits=footer_bits,
        )

    def build_firms_container(self, data: Dict[str, Any]):
        """Components V2 container for a NASA FIRMS satellite fire-hotspot cluster."""
        props = data.get('properties', {})
        lat = props.get('latitude')
        lon = props.get('longitude')
        confidence = str(props.get('confidence', '')).strip()
        frp = props.get('cluster_max_frp', props.get('frp'))
        count = props.get('cluster_count', 1)
        satellite = props.get('satellite_tag') or props.get('satellite') or ''
        instrument = props.get('instrument', '')
        daynight = props.get('daynight', '')
        acq = props.get('acq_datetime', '')

        color = self.COLORS.get('firms', 0xF97316)
        container = discord.ui.Container(accent_colour=color)
        container.add_item(discord.ui.TextDisplay(content="### 🛰️ Fire Hotspot Detected"))

        # FIRMS only gives coordinates — no place name.
        if lat is not None and lon is not None:
            try:
                coord_str = f"{float(lat):.4f}, {float(lon):.4f}"
            except (TypeError, ValueError):
                coord_str = None
            if coord_str:
                container.add_item(discord.ui.TextDisplay(
                    content=self._clip_text(f"📍 `{coord_str}`")
                ))

        meta_bits = []
        conf_badges = {'high': '🔴 High', 'nominal': '🟠 Nominal', 'low': '🟡 Low'}
        cb = conf_badges.get(confidence.lower())
        if cb:
            meta_bits.append(f"Confidence: {cb}")
        if is_valid_value(frp):
            try:
                meta_bits.append(f"🔥 {float(frp):.0f} MW FRP")
            except (TypeError, ValueError):
                pass
        if isinstance(count, int) and count > 1:
            meta_bits.append(f"{count} detections")
        if meta_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(meta_bits))
            ))

        sat_bits = []
        if satellite:
            sat_bits.append(f"🛰️ {instrument} {satellite}".strip()
                            if instrument else f"🛰️ {satellite}")
        if daynight:
            sat_bits.append('☀️ Day' if str(daynight).upper().startswith('D') else '🌙 Night')
        if sat_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(sat_bits))
            ))

        footer_bits = []
        dt_acq = parse_timestamp_to_datetime(acq)
        if dt_acq:
            footer_bits.append(f"🕐 <t:{int(dt_acq.timestamp())}:R>")
        if lat is not None and lon is not None:
            map_url = build_map_url(lat, lon, label="Fire Hotspot", layer="firms")
            footer_bits.append(f"[🗺️ Map]({map_url})")

        self._append_container_footer(container, footer_bits, source="NASA FIRMS")
        return container

    # ---- BOM ---------------------------------------------------
    def build_bom_container(self, data: Dict[str, Any]):
        """Components V2 container for BOM warnings."""
        title = strip_html(data.get('title', 'Weather Warning'))
        description = strip_html(data.get('description', ''))
        area = strip_html(data.get('area', ''))
        issued = data.get('issued', '')
        expiry = data.get('expiry', '')
        link = data.get('link', '')
        category = data.get('category', 'general')
        severity = data.get('severity', 'info')

        category_icon = self.BOM_CATEGORY_ICONS.get(category, '⚠️')
        if severity == 'severe':
            color = self.BOM_SEVERITY_COLORS['severe']
        elif severity == 'warning':
            color = self.BOM_SEVERITY_COLORS['warning']
        else:
            color = self.BOM_CATEGORY_COLORS.get(category, 0x1E90FF)

        severity_badges = {
            'severe': '🔴 SEVERE',
            'warning': '🟠 WARNING',
            'watch': '🟡 WATCH',
            'advice': '🔵 ADVICE',
            'info': '⚪ INFO',
        }
        severity_badge = severity_badges.get(severity, '')

        container = discord.ui.Container(accent_colour=color)
        container.add_item(discord.ui.TextDisplay(content=f"### {category_icon} {title}"))

        meta_bits = []
        if severity_badge:
            meta_bits.append(severity_badge)
        meta_bits.append(category.title())
        if meta_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(meta_bits))
            ))

        if description:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(description[:2000])
            ))

        if is_valid_value(area):
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"📍 {area}")
            ))

        footer_bits = []
        if is_valid_value(issued):
            footer_bits.append(f"issued {issued}")
        if is_valid_value(expiry):
            footer_bits.append(f"expires {expiry}")
        if is_valid_value(link):
            footer_bits.append(f"[BOM]({link})")
        self._append_container_footer(container, footer_bits, source="Bureau of Meteorology")
        return container

    # ---- Traffic -----------------------------------------------
    def build_traffic_container(self, data: Dict[str, Any], alert_type: str):
        """Components V2 container for Live Traffic NSW incidents."""
        props = data.get('properties', {})
        incident_type = props.get('incidentType', '')
        title = props.get('title') or props.get('headline') or props.get('displayName', 'Traffic Alert')
        title = strip_html(title)
        subtitle = strip_html(props.get('subtitle', ''))
        roads = strip_html(str(props.get('roads', '')))
        advice = strip_html(props.get('otherAdvice', '') or props.get('adviceA', ''))
        advice_b = strip_html(props.get('adviceB', ''))
        delay = props.get('expectedDelay', '') or props.get('delay', '')
        direction = strip_html(props.get('affectedDirection', ''))

        incident_type_lower = incident_type.lower() if incident_type else ''
        icon = self.INCIDENT_TYPE_ICONS.get(incident_type_lower, self.ICONS.get(alert_type, '🚗'))
        color = self.INCIDENT_TYPE_COLORS.get(incident_type_lower)
        if not color:
            for type_key, type_color in self.INCIDENT_TYPE_COLORS.items():
                if type_key in incident_type_lower or incident_type_lower in type_key:
                    color = type_color
                    break
        if not color:
            color = self.COLORS.get(alert_type, 0xFFA500)

        heading = f"### {icon} {incident_type}" if incident_type else f"### {icon} Traffic Alert"

        container = discord.ui.Container(accent_colour=color)
        container.add_item(discord.ui.TextDisplay(content=heading))

        body_bits = []
        if is_valid_value(title):
            body_bits.append(f"**{title}**")
        if is_valid_value(subtitle) and subtitle != title:
            body_bits.append(subtitle)
        if body_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text('\n'.join(body_bits))
            ))

        if is_valid_value(roads):
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"📍 {roads[:1500]}")
            ))

        meta_bits = []
        if is_valid_value(direction):
            meta_bits.append(f"➡️ {direction}")
        if is_valid_value(delay) and str(delay) != '-1':
            meta_bits.append(f"⏱️ delay: {delay}")
        if meta_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(meta_bits))
            ))

        if is_valid_value(advice):
            if len(advice) > 500:
                advice = advice[:497] + '...'
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"ℹ️ {advice}")
            ))
        if is_valid_value(advice_b):
            if len(advice_b) > 300:
                advice_b = advice_b[:297] + '...'
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(advice_b)
            ))

        footer_bits = []
        created_val = props.get('created', '') or props.get('lastUpdated', '')
        dt_created = parse_timestamp_to_datetime(created_val)
        if dt_created:
            footer_bits.append(f"🕐 <t:{int(dt_created.timestamp())}:R>")

        geometry = data.get('geometry', {})
        if isinstance(geometry, dict) and geometry.get('coordinates') is not None:
            # Traffic closures/roadwork are often LineString, not Point —
            # reduce to a representative point instead of assuming a flat
            # [lon, lat] pair.
            lon, lat = representative_lonlat(geometry['coordinates'])
            if lat is not None and lon is not None:
                map_url = build_map_url(lat, lon, label=title, layer="incidents")
                footer_bits.append(f"[🗺️ Map]({map_url})")

        self._append_container_footer(container, footer_bits, source="Live Traffic NSW")
        return container

    # ---- Interstate fire services -------------------------------
    # All of these serve RFS-shaped GeoJSON properties (title / status /
    # alertLevel / location / fireType / updatedISO / url), so one builder
    # covers every agency — only the accent colour and source label differ.
    _INCIDENT_SOURCES = {
        'rfs': 'NSW RFS',
        'cfa': 'CFA (Vic)',
        'deeca': 'DEECA (Vic)',
        'vicses': 'VICSES',
        'emv': 'Emergency Management Vic',
        'esta': 'Triple Zero (Vic)',
        'qfd': 'QLD Fire Dept',
        'qfd_warning': 'QLD Fire Dept',
        'dfes': 'DFES (WA)',
        'dfes_warning': 'DFES (WA)',
        'sa_cfs': 'SA CFS',
        'sa_mfs': 'SA MFS',
        'nt_fire': 'NT Fire & Rescue',
        'nt_bushfires': 'Bushfires NT',
        'act_ambulance': 'ACT Ambulance',
    }

    # Every alert type served by a state emergency feed — that is, every type
    # whose payload is a GeoJSON Feature the interstate container knows how to
    # read.
    #
    # DERIVED FROM THE CATALOG, not hand-listed. The hand-list this replaces
    # had drifted from alert_catalog in both directions: it still routed
    # `qld_warning` and `wa_warning`, which had been renamed to `qfd_warning`
    # and `dfes_warning`, and it had never gained `vicses`, `emv`, `esta` or
    # `nt_bushfires`. All six fell through to the generic fallback, which is
    # why they posted as bare "Alert" cards with no body at all.
    #
    # Keyed on the endpoint because that is what actually determines the
    # payload shape: two types served by the same route cannot need different
    # renderers, and a new type on an existing route now routes itself.
    _INCIDENT_TYPES = _INCIDENT_TYPES

    # ---- The Wire ----------------------------------------------
    def build_wire_article_container(self, data: Dict[str, Any]):
        """Components V2 container for a newly published Wire article."""
        data = data or {}
        color = self.COLORS.get('wire_article', 0xF59E0B)
        container = discord.ui.Container(accent_colour=color)
        title = data.get('title') or 'New article'
        container.add_item(discord.ui.TextDisplay(
            content=self._clip_text(f"### \U0001F4F0 {title}")
        ))
        excerpt = (data.get('excerpt') or '').strip()
        if excerpt:
            container.add_item(discord.ui.TextDisplay(content=self._clip_text(excerpt)))

        meta_bits = []
        author = (data.get('author') or {}).get('name')
        if author:
            meta_bits.append(f"\u270D\uFE0F {author}")
        agencies = data.get('agencies') or []
        if agencies:
            meta_bits.append(' / '.join(str(a) for a in agencies[:3]))
        region = (data.get('location') or {}).get('region')
        if region:
            meta_bits.append(f"\U0001F4CD {region}")
        if meta_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' \u00b7 '.join(meta_bits))
            ))

        # Cover photo (public R2/CF URL). Degrades silently — a photo must
        # never break the card.
        cover = data.get('cover')
        cover_url = cover.get('url') if isinstance(cover, dict) else None
        if cover_url:
            try:
                container.add_item(discord.ui.MediaGallery(
                    discord.MediaGalleryItem(cover_url, description="Cover photo")
                ))
            except Exception:
                pass

        footer_bits = []
        dt_pub = parse_timestamp_to_datetime(
            data.get('published_at') or data.get('created_at') or ''
        )
        if dt_pub:
            footer_bits.append(f"\U0001F550 <t:{int(dt_pub.timestamp())}:R>")
        slug = data.get('slug') or data.get('id')
        if slug:
            footer_bits.append(
                f"[\U0001F4F0 Read on The Wire]({MAP_BASE_URL}/wire?article={slug})"
            )
        self._append_container_footer(container, footer_bits, source="The Wire")
        return container

    def build_wire_fleet_container(self, data: Dict[str, Any]):
        """Components V2 container for a new fleet vehicle added to The Wire."""
        data = data or {}
        color = self.COLORS.get('wire_fleet', 0x14B8A6)
        container = discord.ui.Container(accent_colour=color)
        callsign = data.get('callsign') or data.get('title') or 'New vehicle'
        container.add_item(discord.ui.TextDisplay(
            content=self._clip_text(f"### \U0001F692 {callsign}")
        ))

        line_bits = []
        agency = data.get('agency')
        if agency:
            line_bits.append(str(agency))
        station = data.get('station')
        if station:
            line_bits.append(f"\U0001F3E0 {station}")
        place = ' '.join(str(v) for v in (data.get('lga'), data.get('state')) if v)
        if place:
            line_bits.append(f"\U0001F4CD {place}")
        if line_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' \u00b7 '.join(line_bits))
            ))

        vehicle_bits = []
        vtype = data.get('vehicle_type')
        if vtype:
            vehicle_bits.append(str(vtype))
        mm = ' '.join(str(v) for v in (data.get('make'), data.get('model')) if v)
        if mm:
            vehicle_bits.append(mm)
        year = data.get('production_year')
        if year:
            vehicle_bits.append(str(year))
        if vehicle_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' \u00b7 '.join(vehicle_bits))
            ))

        photo = data.get('image_url')
        if photo:
            try:
                container.add_item(discord.ui.MediaGallery(
                    discord.MediaGalleryItem(photo, description="Fleet photo")
                ))
            except Exception:
                pass

        footer_bits = []
        dt_added = parse_timestamp_to_datetime(data.get('created_at') or '')
        if dt_added:
            footer_bits.append(f"\U0001F550 <t:{int(dt_added.timestamp())}:R>")
        vid = data.get('id')
        if vid:
            footer_bits.append(
                f"[\U0001F692 View on The Wire]({MAP_BASE_URL}/wire?tab=fleet&vehicle={vid})"
            )
        self._append_container_footer(container, footer_bits, source="The Wire")
        return container

    def build_power_container(self, data: Dict[str, Any], alert_type: str):
        if alert_type == 'ausgrid':
            return self.build_ausgrid_container(data)
        if alert_type.startswith('essential_'):
            return self.build_essential_container(data, alert_type)
        return self.build_endeavour_container(data, alert_type)

    def build_endeavour_container(self, data: Dict[str, Any],
                                  alert_type: str = 'endeavour_current'):
        """Components V2 container for Endeavour power outages (current + planned)."""
        suburb = data.get('suburb', 'Unknown')
        streets = strip_html(data.get('streets', ''))
        customers = data.get('customersAffected', 0)
        status = data.get('status', '')
        cause = data.get('cause', '')
        outage_type = data.get('outageType', 'Unplanned')
        restoration = data.get('estimatedRestoration', '')
        start_time = data.get('startTime', '')
        last_updated = data.get('lastUpdated', '')

        # The backend emits 'Unplanned' | 'Current Maintenance' | 'Future
        # Maintenance' — it never says 'Planned', so the old equality check
        # rendered every planned/maintenance outage with the unplanned ⚡.
        title_prefix = "⚡" if outage_type == 'Unplanned' else "🔧"

        container = discord.ui.Container(
            accent_colour=self.COLORS.get(alert_type, self.COLORS['endeavour_current'])
        )
        container.add_item(discord.ui.TextDisplay(
            content=f"### {title_prefix} {outage_type} Outage — {suburb}"
        ))

        if is_valid_value(streets):
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"📍 {streets[:1500]}")
            ))

        meta_bits = []
        if is_valid_value(customers) and customers > 0:
            meta_bits.append(f"👥 **{customers:,}**")
        if is_valid_value(status):
            meta_bits.append(status)
        if meta_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(meta_bits))
            ))

        if is_valid_value(cause):
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"Cause: {cause}")
            ))

        footer_bits = []
        source_time = start_time or last_updated
        dt_start = parse_timestamp_to_datetime(source_time)
        if dt_start:
            footer_bits.append(f"🕐 <t:{int(dt_start.timestamp())}:R>")
        formatted_restoration = format_timestamp(restoration)
        if formatted_restoration:
            footer_bits.append(f"ETA: {formatted_restoration}")
        lat = data.get('latitude')
        lon = data.get('longitude')
        if lat and lon:
            map_url = build_map_url(lat, lon, label=f"{outage_type} Outage - {suburb}", layer="outages")
            footer_bits.append(f"[🗺️ Map]({map_url})")
        self._append_container_footer(container, footer_bits, source="Endeavour Energy")
        return container

    def build_ausgrid_container(self, data: Dict[str, Any]):
        """Components V2 container for Ausgrid power outages."""
        suburb = data.get('Suburb') or data.get('suburb', 'Unknown')
        street = strip_html(data.get('StreetName') or data.get('streetName', ''))
        customers = data.get('CustomersAffected') or data.get('customersAffected', 0)
        postcode = data.get('Postcode') or data.get('postcode', '')
        outage_type = data.get('OutageType') or data.get('outageType', '')
        cause = data.get('Cause') or data.get('cause', '')
        start_time = data.get('StartTime') or data.get('startTime', '')
        est_restore = data.get('EstRestoration') or data.get('estRestoration', '')

        is_planned = outage_type in ('P', 'Planned', 'planned')
        type_text = 'Planned' if is_planned else 'Unplanned'
        title_prefix = "🔧" if is_planned else "⚡"

        container = discord.ui.Container(accent_colour=self.COLORS['ausgrid'])
        container.add_item(discord.ui.TextDisplay(
            content=f"### {title_prefix} {type_text} Outage — {suburb}"
        ))

        loc_bits = []
        if is_valid_value(street):
            loc_bits.append(f"📍 {street[:1500]}")
        if is_valid_value(postcode):
            loc_bits.append(f"Postcode: {postcode}")
        if loc_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text('\n'.join(loc_bits))
            ))

        meta_bits = []
        if is_valid_value(customers) and customers > 0:
            meta_bits.append(f"👥 **{customers:,}**")
        if meta_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(meta_bits))
            ))

        if is_valid_value(cause):
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"Cause: {cause}")
            ))

        footer_bits = []
        formatted_start = format_timestamp(start_time)
        if formatted_start:
            footer_bits.append(f"started {formatted_start}")
        formatted_restoration = format_timestamp(est_restore)
        if formatted_restoration:
            footer_bits.append(f"ETA: {formatted_restoration}")
        lat = data.get('Latitude') or data.get('latitude')
        lon = data.get('Longitude') or data.get('longitude')
        if lat and lon:
            map_url = build_map_url(lat, lon, label=f"{type_text} Outage - {suburb}", layer="outages")
            footer_bits.append(f"[🗺️ Map]({map_url})")
        self._append_container_footer(container, footer_bits, source="Ausgrid")
        return container

    # ---- Essential Energy --------------------------------------
    def build_essential_container(self, data: Dict[str, Any],
                                  alert_type: str = 'essential_unplanned'):
        """Components V2 container for Essential Energy outages
        (unplanned + future). Best-effort field reads — Essential's API
        contract isn't pinned yet; we accept several common spellings and
        gracefully omit anything missing."""
        suburb = (data.get('suburb') or data.get('Suburb')
                  or data.get('locality') or 'Unknown')
        streets = strip_html(str(
            data.get('streets') or data.get('streetName')
            or data.get('StreetName') or data.get('street') or ''
        ))
        customers = (data.get('customersAffected')
                     or data.get('CustomersAffected') or 0)
        status = data.get('status') or data.get('Status') or ''
        cause = data.get('cause') or data.get('Cause') or ''
        start_time = (data.get('startTime') or data.get('StartTime')
                      or data.get('plannedStart') or '')
        restoration = (data.get('estimatedRestoration')
                       or data.get('EstRestoration')
                       or data.get('expectedRestore') or '')

        if alert_type == 'essential_future':
            title_prefix, type_text = "📅", 'Future'
        elif alert_type == 'essential_unplanned':
            title_prefix, type_text = "⚡", 'Unplanned'
        else:
            # Retired essential_planned key from an unmigrated preset.
            title_prefix, type_text = "🔧", 'Planned'

        container = discord.ui.Container(
            accent_colour=self.COLORS.get(alert_type, 0x06B6D4)
        )
        container.add_item(discord.ui.TextDisplay(
            content=f"### {title_prefix} {type_text} Outage — {suburb}"
        ))

        if is_valid_value(streets):
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"📍 {streets[:1500]}")
            ))

        meta_bits = []
        if is_valid_value(customers) and customers and int(customers) > 0:
            meta_bits.append(f"👥 **{int(customers):,}**")
        if is_valid_value(status):
            meta_bits.append(str(status))
        if meta_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(meta_bits))
            ))

        if is_valid_value(cause):
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"Cause: {cause}")
            ))

        footer_bits = []
        formatted_start = format_timestamp(start_time)
        if formatted_start:
            footer_bits.append(f"start {formatted_start}")
        formatted_restoration = format_timestamp(restoration)
        if formatted_restoration:
            footer_bits.append(f"ETA: {formatted_restoration}")
        lat = data.get('latitude') or data.get('Latitude')
        lon = data.get('longitude') or data.get('Longitude')
        if lat and lon:
            map_url = build_map_url(lat, lon, label=f"{type_text} Outage - {suburb}", layer="outages")
            footer_bits.append(f"[🗺️ Map]({map_url})")
        self._append_container_footer(container, footer_bits, source="Essential Energy")
        return container

    # ---- User incidents ----------------------------------------
    def build_user_incident_container(self, data: Dict[str, Any],
                                      previous_message: Dict[str, Any] = None):
        """Components V2 container for user-submitted incidents."""
        title = strip_html(data.get('title', 'User Incident'))
        location = strip_html(data.get('location', ''))
        description = strip_html(data.get('description', ''))
        status = data.get('status', 'Active')
        size = data.get('size', '')

        inc_type = data.get('type', '')
        if isinstance(inc_type, list):
            type_display = ', '.join(inc_type)
        else:
            type_display = str(inc_type) if inc_type else ''

        agencies = data.get('responding_agencies', [])
        if isinstance(agencies, str):
            agencies = [agencies]

        lat = data.get('lat')
        lng = data.get('lng')
        created_at = data.get('created_at', '')
        logs = data.get('logs', [])

        color = self.COLORS['user_incident']
        icon = '📢'
        type_lower = type_display.lower() if type_display else ''
        if 'fire' in type_lower or 'bush' in type_lower:
            color = 0xFF4500
            icon = '🔥'
        elif 'flood' in type_lower or 'water' in type_lower:
            color = 0x00CED1
            icon = '🌊'
        elif 'crash' in type_lower or 'accident' in type_lower or 'mva' in type_lower:
            color = 0xEF4444
            icon = '💥'
        elif 'rescue' in type_lower:
            color = 0x3B82F6
            icon = '🚑'
        elif 'hazmat' in type_lower:
            color = 0xA855F7
            icon = '☣️'
        elif 'police' in type_lower or 'pursuit' in type_lower:
            color = 0x1E40AF
            icon = '🚔'
        elif 'storm' in type_lower or 'weather' in type_lower:
            color = 0x6B7280
            icon = '⛈️'

        container = discord.ui.Container(accent_colour=color)
        container.add_item(discord.ui.TextDisplay(content=f"### {icon} {title}"))

        desc_bits = []
        if is_valid_value(location):
            desc_bits.append(f"📍 **{location}**")
        if is_valid_value(description):
            desc_bits.append(description[:500])
        if desc_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text('\n'.join(desc_bits))
            ))

        meta_bits = []
        if is_valid_value(type_display):
            meta_bits.append(f"Type: **{type_display}**")
        if is_valid_value(status):
            meta_bits.append(f"Status: **{status}**")
        if is_valid_value(size):
            meta_bits.append(f"Size: **{size}**")
        if meta_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(meta_bits))
            ))

        if agencies and len(agencies) > 0:
            agency_icons = {
                'FRNSW': '🚒', 'Fire': '🚒', 'RFS': '🔥',
                'Ambulance': '🚑', 'NSW Ambulance': '🚑', 'NSWA': '🚑',
                'Police': '🚔', 'NSWPF': '🚔',
                'SES': '🦺', 'VRA': '🦺'
            }
            agency_list = []
            for agency in agencies:
                icon_for_agency = ''
                for key, emoji in agency_icons.items():
                    if key.lower() in str(agency).lower():
                        icon_for_agency = emoji + ' '
                        break
                agency_list.append(f"{icon_for_agency}{agency}")
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"Responding: {' • '.join(agency_list)}")
            ))

        # Attached units (editor-entered callsigns).
        units = data.get('units', [])
        if isinstance(units, str):
            units = [units]
        if isinstance(units, list):
            unit_labels = [strip_html(str(u)) for u in units if u and str(u).strip()]
            if unit_labels:
                container.add_item(discord.ui.TextDisplay(
                    content=self._clip_text(f"🚒 **Units:** {', '.join(unit_labels)}")
                ))

        # Attached photos → a media gallery (max 4, matching the uploader
        # cap). Degrades to a text link list if the gallery component is
        # unavailable so a photo never breaks the whole card.
        images = data.get('images', [])
        if isinstance(images, list) and images:
            photo_urls = []
            for img in images:
                if isinstance(img, dict):
                    url = build_incident_image_url(img.get('file'))
                    if url:
                        photo_urls.append(url)
                        if len(photo_urls) >= 4:
                            break
            if photo_urls:
                try:
                    gallery_items = [
                        discord.MediaGalleryItem(u, description="Incident photo")
                        for u in photo_urls
                    ]
                    container.add_item(discord.ui.MediaGallery(*gallery_items))
                except Exception:
                    links = ' • '.join(
                        f"[Photo {i + 1}]({u})" for i, u in enumerate(photo_urls)
                    )
                    container.add_item(discord.ui.TextDisplay(
                        content=self._clip_text(f"📷 {links}")
                    ))

        if logs and len(logs) > 0:
            dict_logs = [x for x in logs if isinstance(x, dict)]
            sorted_logs = sorted(dict_logs, key=lambda x: x.get('created_at', ''), reverse=True)
            log_lines = ["**📋 Incident Log**"]
            for log in sorted_logs[:5]:
                log_ts = log.get('created_at', '')
                log_msg = strip_html(log.get('message', ''))[:200]
                if log_msg:
                    dt = parse_timestamp_to_datetime(log_ts)
                    time_str = f"<t:{int(dt.timestamp())}:t>" if dt else "Unknown time"
                    log_lines.append(f"**{time_str}** — {log_msg}")
            if len(logs) > 5:
                log_lines.append(f"_+{len(logs) - 5} more..._")
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text('\n'.join(log_lines))
            ))

        footer_bits = []
        dt_created = parse_timestamp_to_datetime(created_at)
        if dt_created:
            footer_bits.append(f"🕐 <t:{int(dt_created.timestamp())}:R>")
        if lat and lng:
            map_url = build_map_url(lat, lng, label=title, layer="user")
            footer_bits.append(f"[🗺️ Map]({map_url})")
        if previous_message and previous_message.get('message_url'):
            prev_status = previous_message.get('status', 'initial')
            footer_bits.append(
                f"[📜 Previous ({prev_status})]({previous_message['message_url']})"
            )
        self._append_container_footer(container, footer_bits, source="User-submitted (AusAware)")
        return container

    # ---- Pager -------------------------------------------------
    def build_pager_container(self, msg: Dict[str, Any]):
        """Components V2 container for pager messages."""
        capcode = msg.get('capcode', 'UNKNOWN')
        incident_id = msg.get('incident_id', '')
        msg_type = msg.get('type', '')
        category = msg.get('category', '')
        alias = msg.get('alias', '')
        agency = msg.get('agency', '')
        address = msg.get('address', '')
        suburb = msg.get('suburb', '')
        council = msg.get('council', '')
        postcode = msg.get('postcode', '')
        coordinates = msg.get('coordinates')
        timestamp = msg.get('timestamp', '')
        raw = msg.get('raw', '')

        is_stop = 'stop' in msg_type.lower() if msg_type else False
        color = self.COLORS['pager_stop'] if is_stop else self.COLORS['pager']
        icon = '🛑' if is_stop else '📟'

        if is_stop:
            title = "STOP MESSAGE"
        elif msg_type and msg_type != 'Pager Alert':
            title = msg_type
        elif alias:
            title = alias
        else:
            title = "Pager Alert"

        container = discord.ui.Container(accent_colour=color)
        container.add_item(discord.ui.TextDisplay(content=f"### {icon} {title}"))

        if is_valid_value(category) and category != msg_type:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"🚨 **{category}**")
            ))

        location_parts = []
        if is_valid_value(address):
            location_parts.append(f"**{address}**")
        if is_valid_value(suburb):
            suburb_text = suburb
            if is_valid_value(postcode):
                suburb_text += f" {postcode}"
            location_parts.append(suburb_text)
        if is_valid_value(council):
            location_parts.append(council)
        if location_parts:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text('📍 ' + ' · '.join(location_parts))
            ))

        ref_bits = []
        if is_valid_value(capcode):
            ref_bits.append(f"Capcode `{capcode}`")
        if is_valid_value(incident_id):
            ref_bits.append(f"Incident `{incident_id}`")
        if is_valid_value(agency):
            ref_bits.append(f"Area: {agency}")
        if ref_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(ref_bits))
            ))

        if is_valid_value(raw) and len(raw) > 10:
            raw_display = raw[:600] + "..." if len(raw) > 600 else raw
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"```{raw_display}```")
            ))

        footer_bits = []
        dt_ts = parse_timestamp_to_datetime(timestamp)
        if dt_ts:
            footer_bits.append(f"🕐 <t:{int(dt_ts.timestamp())}:R>")
        if coordinates:
            lat = coordinates.get('lat')
            lon = coordinates.get('lon')
            if lat and lon:
                label = f"{msg_type} - {incident_id}" if msg_type and incident_id else (msg_type or incident_id or capcode)
                map_url = build_map_url(lat, lon, label=label, layer="pager")
                footer_bits.append(f"[🗺️ Map]({map_url})")
        self._append_container_footer(container, footer_bits, source="Pagermon (self-hosted)")
        return container

    # ---- Generic fallback --------------------------------------
    def build_generic_container(self, data: Dict[str, Any], alert_type: str):
        """Fallback Components V2 container for unknown alert types.

        Reached only by a type with no dedicated builder. It still has to
        produce something a reader can act on: a card with a title and nothing
        else is worse than an ugly card, because it tells the channel an
        incident happened and then refuses to say what.

        The GeoJSON unwrap below is why this used to fail completely. Most of
        our feeds are FeatureCollections, so a Feature's only top-level keys
        are `type`, `geometry` and `properties` — and the loop skipped all
        three. Everything worth printing was inside `properties`, which was on
        the skip list.
        """
        data = data or {}
        icon = self.ICONS.get(alert_type, '📢')
        color = self.COLORS.get(alert_type, 0x5865F2)
        container = discord.ui.Container(accent_colour=color)

        # A GeoJSON Feature keeps its content in `properties`; anything else is
        # already flat. Both are read, so a hybrid shape loses nothing.
        props = data.get('properties')
        fields = dict(data)
        if isinstance(props, dict):
            fields.update(props)

        # Give the card a real heading when the payload offers one, rather than
        # the bare word "Alert" that made these unreadable.
        title = ''
        for key in ('title', 'headline', 'name', 'displayName', 'event',
                    'capEvent', 'description'):
            v = fields.get(key)
            if isinstance(v, str) and is_valid_value(strip_html(v)):
                title = strip_html(v)
                break
        label = self._INCIDENT_SOURCES.get(alert_type) or alert_type.replace('_', ' ').title()
        heading = title or f"{label} alert"
        container.add_item(discord.ui.TextDisplay(
            content=self._clip_text(f"### {icon} {heading}")
        ))

        lines = []
        for key, value in fields.items():
            if key in ('geometry', 'properties', 'type'):
                continue
            if not value or isinstance(value, (dict, list)):
                continue
            clean_value = strip_html(str(value))
            if is_valid_value(clean_value):
                lines.append(f"**{key.replace('_', ' ').title()}:** {clean_value[:500]}")
        if lines:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text('\n'.join(lines))
            ))
        elif not title:
            # Nothing printable at all. Say so, rather than posting a card that
            # looks like a rendering bug — which is exactly how the blank ones
            # read in the channel.
            container.add_item(discord.ui.TextDisplay(
                content='_No details were included with this alert._'
            ))
        return container
