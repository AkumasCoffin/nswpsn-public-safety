"""
Embed Builder - Creates beautiful Discord embeds for various alert types.
"""

import os
import re
import html
import discord
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import quote

# Base URL for the map
MAP_BASE_URL = "https://nswpsn.forcequit.xyz"


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


def format_timestamp_relative(ts: str) -> Optional[str]:
    """Format timestamp as relative time (e.g., '2 hours ago')"""
    if not ts:
        return None
    try:
        dt = None
        if 'T' in str(ts):
            dt = datetime.fromisoformat(str(ts).replace('Z', '+00:00'))
        elif ',' in str(ts) and 'GMT' in str(ts):
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(ts)

        if dt:
            unix_ts = int(dt.timestamp())
            return f"<t:{unix_ts}:R>"  # Relative time
        return str(ts)
    except (ValueError, TypeError, OSError, AttributeError):
        return str(ts) if ts else None


def build_map_url(lat: float, lon: float, label: str = "", layer: str = "incidents", zoom: int = 15) -> str:
    """Build a URL to the NSW PSN map"""
    label_encoded = quote(label, safe='') if label else ''
    return f"{MAP_BASE_URL}/map.html?lat={lat}&lng={lon}&zoom={zoom}&label={label_encoded}&layer={layer}"


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


def chunk_containers_for_message(containers: list, max_chars: int = 3200,
                                 max_per_message: int = 2) -> list:
    """Split Components-V2 Container objects into groups that each fit under
    Discord's per-message budget.

    Discord enforces a hard 4000-char limit on the total *displayable text*
    across all components in one message. That sum includes TextDisplay
    bodies, button labels, role-ping mentions, and section spacing overhead
    — `_container_char_size` only counts TextDisplay content, so we leave
    ~800 chars of headroom (max_chars=3200) to absorb everything else.

    A *single* container can also exceed the limit on its own (e.g. a busy
    radio summary incident with 30 transcripts) — when we detect that, we
    truncate the container in-place rather than letting Discord 400 us.
    Pack at most 2 containers per message so one busy incident still gets
    its own room without starving its neighbour."""
    groups = []
    current = []
    current_size = 0
    for c in containers:
        sz = _container_char_size(c)
        # Defensive: if a single container is over budget, trim it before
        # we even try to pack. Without this, a 5000-char container ends up
        # in its own (size > max_chars) group and Discord rejects the send.
        if sz > max_chars:
            sz = _truncate_container_inplace(c, max_chars)
        would_exceed = current and (
            current_size + sz > max_chars
            or len(current) >= max_per_message
        )
        if would_exceed:
            groups.append(current)
            current = []
            current_size = 0
        current.append(c)
        current_size += sz
    if current:
        groups.append(current)
    return groups


def chunk_embeds_for_message(embeds: list, max_chars: int = 5500,
                             max_per_message: int = 10) -> list:
    """Split a list of embeds into groups that each fit under Discord's per-
    message budget (10 embeds + 6000 char total). Default limits leave a bit
    of headroom so we don't tip over on rounding.
    Returns a list of lists; every group is send-safe in one call.
    """
    groups = []
    current = []
    current_size = 0
    for e in embeds:
        sz = _embed_char_size(e)
        would_exceed = current and (
            current_size + sz > max_chars
            or len(current) >= max_per_message
        )
        if would_exceed:
            groups.append(current)
            current = []
            current_size = 0
        current.append(e)
        current_size += sz
    if current:
        groups.append(current)
    return groups


class EmbedBuilder:
    """Builds beautiful Discord embeds for different alert types"""
    
    # Color scheme for different alert types
    COLORS = {
        'rfs': 0xFF4500,
        'traffic_fire': 0xFF6347,
        'bom': 0x1E90FF,
        'traffic_incidents': 0xFFA500,
        'traffic_roadwork': 0xFFD700,
        'traffic_flood': 0x00CED1,
        'traffic_major': 0xFF8C00,
        'power_endeavour': 0x8B008B,
        'pager': 0x32CD32,
        'pager_stop': 0x228B22,
        'power_ausgrid': 0xE67E22,  # Orange for Ausgrid
        'user_incidents': 0x9333EA,  # Purple for user incidents
        'waze_hazards': 0xEAB308,    # Yellow for hazards
        'waze_police': 0x3B82F6,     # Blue for police
        'waze_roadwork': 0xA855F7,   # Purple for roadwork
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
        'rfs': '🔥',
        'bom': '⛈️',
        'traffic_incidents': '🚗',
        'traffic_roadwork': '🚧',
        'traffic_flood': '🌊',
        'traffic_fire': '🔥',
        'traffic_major': '🎉',
        'power_endeavour': '⚡',
        'pager': '📟',
        'user_incidents': '📢',
        'waze_hazards': '⚠️',
        'waze_police': '👮',
        'waze_roadwork': '🚧',
    }
    
    # BOM category icons
    BOM_CATEGORY_ICONS = {
        'land': '🌍',
        'marine': '🌊',
        'general': '📢',
    }
    
    def build_alert_embed(self, alert: Dict[str, Any], previous_message: Dict[str, Any] = None) -> discord.Embed:
        """Build an embed for any alert type
        
        Args:
            alert: The alert data
            previous_message: Optional previous message info for linking (has 'message_url' and 'status')
        """
        alert_type = alert.get('type', 'unknown')
        data = alert.get('data', {})
        
        if alert_type == 'rfs':
            return self._build_rfs_embed(data, previous_message=previous_message)
        elif alert_type == 'bom':
            return self._build_bom_embed(data, alert_type)
        elif alert_type.startswith('traffic_'):
            return self._build_traffic_embed(data, alert_type)
        elif alert_type.startswith('power_'):
            return self._build_power_embed(data, alert_type)
        elif alert_type.startswith('waze_'):
            return self._build_waze_embed(data, alert_type)
        elif alert_type == 'user_incidents':
            return self._build_user_incident_embed(data, previous_message=previous_message)
        elif alert_type == 'radio_summary':
            return self._build_radio_summary_embed(data)
        else:
            return self._build_generic_embed(data, alert_type)
    
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
    
    def _build_rfs_embed(self, data: Dict[str, Any], previous_message: Dict[str, Any] = None) -> discord.Embed:
        """Build embed for RFS incidents
        
        Args:
            data: The incident data
            previous_message: Optional previous message info for linking updates
        """
        props = data.get('properties', {})
        
        title = strip_html(props.get('title', 'Unknown Incident'))
        link = props.get('link', '')
        
        # Get values directly from properties (API already parses them)
        # Also check for raw description if API didn't parse
        raw_desc = props.get('description', '') or ''
        
        # Use API-parsed values first, fall back to parsing description
        status = props.get('status', '')
        location = props.get('location', '')
        size = props.get('size', '')
        alert_level = props.get('alertLevel', '')
        council = props.get('councilArea', '')  # New API field
        fire_type = props.get('fireType', '')   # New API field
        updated = props.get('updated', '')      # Display format: "7 Jan 2026 13:35"
        updated_iso = props.get('updatedISO', '')  # ISO format with timezone
        responsible_agency = props.get('responsibleAgency', '')  # New API field
        
        # If API didn't parse (fields empty) but we have raw description, parse it
        if raw_desc and not status and not location:
            parsed = self._parse_rfs_description(raw_desc)
            alert_level = parsed.get('alert_level', '') or alert_level
            status = parsed.get('status', '') or status
            location = parsed.get('location', '') or location
            size = parsed.get('size', '') or size
            council = parsed.get('council_area', '') or council
            fire_type = parsed.get('type', '') or fire_type
            updated = parsed.get('updated', '') or updated
        
        # Clean any values that might have extra text
        if alert_level and len(alert_level) > 30:
            # alertLevel is too long - probably contains raw description, extract just the level
            match = re.match(r'^(Advice|Watch and Act|Emergency Warning|Emergency)', alert_level, re.IGNORECASE)
            if match:
                alert_level = match.group(1)
            else:
                alert_level = ''
        
        # Color based on alert level or status
        color = self.COLORS['rfs']
        level_emoji = '🟡'
        
        # Determine alert level from status if not set
        if not alert_level and status:
            status_lower = status.lower()
            if 'out of control' in status_lower:
                alert_level = 'Emergency Warning'
            elif 'being controlled' in status_lower:
                alert_level = 'Watch and Act'
            elif 'under control' in status_lower:
                alert_level = 'Advice'
        
        if alert_level:
            level_lower = alert_level.lower()
            if 'emergency' in level_lower:
                color = 0xFF0000
                level_emoji = '🔴'
            elif 'watch' in level_lower:
                color = 0xFF8C00
                level_emoji = '🟠'
            elif 'advice' in level_lower:
                color = 0xFFD700
                level_emoji = '🟡'
        
        # Use source timestamp for embed (the "updated" time from RFS)
        embed_timestamp = parse_timestamp_to_datetime(updated_iso) or datetime.now()
        
        embed = discord.Embed(
            title=f"🔥 {title}",
            color=color,
            timestamp=embed_timestamp
        )
        
        # Row 1: Alert Level, Status (all inline)
        if alert_level:
            embed.add_field(name="⚠️ Alert Level", value=f"{level_emoji} {alert_level}", inline=True)
        
        if status:
            embed.add_field(name="📊 Status", value=status, inline=True)
        
        if fire_type:
            embed.add_field(name="🔥 Type", value=fire_type, inline=True)
        
        # Row 2: Size
        if size:
            embed.add_field(name="📏 Size", value=size, inline=True)
        
        # Location on its own row (not inline)
        if location:
            embed.add_field(name="📍 Location", value=location, inline=False)
        
        # Council area
        if council:
            embed.add_field(name="🏛️ Council Area", value=council, inline=True)
        
        # Responsible agency
        if responsible_agency:
            embed.add_field(name="🚒 Agency", value=responsible_agency, inline=True)
        
        # Map link
        geometry = data.get('geometry', {})
        if geometry.get('coordinates'):
            coords = geometry['coordinates']
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                map_url = build_map_url(lat, lon, label=title, layer="rfs")
                embed.add_field(name="🗺️ Map", value=f"[View on Map]({map_url})", inline=True)
        
        if link:
            embed.add_field(name="ℹ️ More Info", value=f"[View Details]({link})", inline=True)
        
        # Link to previous message for this incident (for tracking updates)
        if previous_message and previous_message.get('message_url'):
            prev_status = previous_message.get('status', 'initial')
            embed.add_field(
                name="📜 Previous Update",
                value=f"[View original alert ({prev_status})]({previous_message['message_url']})",
                inline=False
            )
        
        embed.set_footer(text="NSW RFS • Rural Fire Service")
        return embed
    
    def _build_bom_embed(self, data: Dict[str, Any], alert_type: str) -> discord.Embed:
        """Build embed for BOM warnings"""
        title = strip_html(data.get('title', 'Weather Warning'))
        description = strip_html(data.get('description', ''))
        area = strip_html(data.get('area', ''))
        issued = data.get('issued', '')
        expiry = data.get('expiry', '')
        link = data.get('link', '')
        category = data.get('category', 'general')  # land, marine, or general
        severity = data.get('severity', 'info')     # severe, warning, watch, advice, info
        
        # Get icon based on category
        category_icon = self.BOM_CATEGORY_ICONS.get(category, '⚠️')
        
        # Get color based on severity (severe overrides category color)
        if severity == 'severe':
            color = self.BOM_SEVERITY_COLORS['severe']
        elif severity == 'warning':
            color = self.BOM_SEVERITY_COLORS['warning']
        else:
            # Use category-based color for lower severity
            color = self.BOM_CATEGORY_COLORS.get(category, 0x1E90FF)
        
        # Build severity badge
        severity_badges = {
            'severe': '🔴 SEVERE',
            'warning': '🟠 WARNING',
            'watch': '🟡 WATCH',
            'advice': '🔵 ADVICE',
            'info': '⚪ INFO',
        }
        severity_badge = severity_badges.get(severity, '')
        
        # Use source timestamp (issued time from BOM)
        embed_timestamp = parse_timestamp_to_datetime(issued) or datetime.now()
        
        # Build title with category icon
        embed = discord.Embed(
            title=f"{category_icon} {title}",
            description=description[:2000] if description else None,
            color=color,
            timestamp=embed_timestamp
        )
        
        # Add severity and category as inline fields
        if severity_badge:
            embed.add_field(name="Severity", value=severity_badge, inline=True)
        
        embed.add_field(name="Type", value=category.title(), inline=True)
        
        if is_valid_value(area):
            embed.add_field(name="📍 Area", value=area[:1024], inline=False)
        
        if is_valid_value(issued):
            embed.add_field(name="Issued", value=issued, inline=True)
        
        if is_valid_value(expiry):
            embed.add_field(name="Expires", value=expiry, inline=True)
        
        if is_valid_value(link):
            embed.add_field(name="More Info", value=f"[View on BOM]({link})", inline=False)
        
        embed.set_footer(text="Bureau of Meteorology")
        return embed
    
    def _build_traffic_embed(self, data: Dict[str, Any], alert_type: str) -> discord.Embed:
        """Build embed for traffic incidents"""
        props = data.get('properties', {})
        
        # Get incident type extracted from title (e.g., HAZARD, CRASH, CHANGED TRAFFIC CONDITIONS)
        incident_type = props.get('incidentType', '')
        
        # Get and clean title (now contains description after type prefix removed)
        title = props.get('title') or props.get('headline') or props.get('displayName', 'Traffic Alert')
        title = strip_html(title)
        
        subtitle = strip_html(props.get('subtitle', ''))
        roads = strip_html(str(props.get('roads', '')))
        advice = strip_html(props.get('otherAdvice', '') or props.get('adviceA', ''))
        advice_b = strip_html(props.get('adviceB', ''))
        delay = props.get('expectedDelay', '') or props.get('delay', '')
        direction = strip_html(props.get('affectedDirection', ''))
        
        # Use incident type for icon and color if available
        incident_type_lower = incident_type.lower() if incident_type else ''
        
        # Get icon - check incident type first, then fall back to alert type
        icon = self.INCIDENT_TYPE_ICONS.get(incident_type_lower, self.ICONS.get(alert_type, '🚗'))
        
        # Get color - check incident type first, then fall back to alert type
        color = self.INCIDENT_TYPE_COLORS.get(incident_type_lower)
        if not color:
            # Try partial match for incident type color
            for type_key, type_color in self.INCIDENT_TYPE_COLORS.items():
                if type_key in incident_type_lower or incident_type_lower in type_key:
                    color = type_color
                    break
        if not color:
            color = self.COLORS.get(alert_type, 0xFFA500)
        
        # Build embed title with incident type badge if available
        if incident_type:
            embed_title = f"{icon} {incident_type}"
        else:
            embed_title = f"{icon} Traffic Alert"
        
        # Use source timestamp (created or lastUpdated)
        created_val = props.get('created', '') or props.get('lastUpdated', '')
        embed_timestamp = parse_timestamp_to_datetime(created_val) or datetime.now()
        
        embed = discord.Embed(
            title=embed_title,
            color=color,
            timestamp=embed_timestamp
        )
        
        # Show the title/description as the main content
        if is_valid_value(title):
            embed.description = f"**{title}**"
        
        if is_valid_value(subtitle) and subtitle != title:
            if embed.description:
                embed.description += f"\n{subtitle}"
            else:
                embed.description = subtitle
        
        if is_valid_value(roads):
            embed.add_field(name="📍 Location", value=roads[:1024], inline=False)
        
        if is_valid_value(direction):
            embed.add_field(name="Direction", value=direction, inline=True)
        
        # Only show delay if it's valid (not -1 or empty)
        if is_valid_value(delay) and str(delay) != '-1':
            embed.add_field(name="Expected Delay", value=str(delay), inline=True)
        
        # Truncate long advice text nicely
        if is_valid_value(advice):
            if len(advice) > 500:
                advice = advice[:497] + "..."
            embed.add_field(name="ℹ️ Advice", value=advice, inline=False)
        
        if is_valid_value(advice_b):
            if len(advice_b) > 300:
                advice_b = advice_b[:297] + "..."
            embed.add_field(name="Additional Info", value=advice_b, inline=False)
        
        # Map link
        geometry = data.get('geometry', {})
        if geometry.get('coordinates'):
            coords = geometry['coordinates']
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                map_url = build_map_url(lat, lon, label=title, layer="incidents")
                embed.add_field(name="🗺️ Map", value=f"[View on Map]({map_url})", inline=True)
        
        embed.set_footer(text="Live Traffic NSW")
        return embed
    
    def _build_waze_embed(self, data: Dict[str, Any], alert_type: str) -> discord.Embed:
        """Build embed for Waze alerts (hazards, police, roadwork)"""
        props = data.get('properties', {})
        
        # Get type info
        waze_type = props.get('wazeType', '')
        waze_subtype = props.get('wazeSubtype', '')
        display_type = props.get('displayType', '')
        title = strip_html(props.get('title', ''))
        
        # Location
        street = strip_html(props.get('street', ''))
        city = strip_html(props.get('city', ''))
        location = strip_html(props.get('location', ''))
        
        # Reliability/thumbs up
        thumbs_up = props.get('thumbsUp', 0)
        reliability = props.get('reliability', 0)
        
        # Time
        created = props.get('created', '')
        
        # Get icon and color for this alert type
        icon = self.ICONS.get(alert_type, '⚠️')
        color = self.COLORS.get(alert_type, 0xEAB308)
        
        # Build title based on alert type
        type_labels = {
            'waze_hazards': 'Waze Hazard',
            'waze_police': 'Waze Police Report',
            'waze_roadwork': 'Waze Roadwork',
        }
        type_label = type_labels.get(alert_type, 'Waze Alert')
        
        # Use display type or subtype for more specific title
        if display_type and display_type != 'Unknown':
            embed_title = f"{icon} {display_type}"
        else:
            embed_title = f"{icon} {type_label}"
        
        # Use source timestamp (Waze created time)
        embed_timestamp = parse_timestamp_to_datetime(created) or datetime.now()
        
        embed = discord.Embed(
            title=embed_title,
            color=color,
            timestamp=embed_timestamp
        )
        
        # Description/title from Waze
        if is_valid_value(title) and title != display_type:
            embed.description = title[:2000]
        
        # Location
        if is_valid_value(location):
            embed.add_field(name="📍 Location", value=location[:1024], inline=False)
        elif is_valid_value(street) or is_valid_value(city):
            loc_parts = [p for p in [street, city] if is_valid_value(p)]
            if loc_parts:
                embed.add_field(name="📍 Location", value=", ".join(loc_parts)[:1024], inline=False)
        
        # Alert type details
        if is_valid_value(waze_subtype) and waze_subtype.lower() != display_type.lower():
            embed.add_field(name="Type", value=waze_subtype.replace('_', ' ').title(), inline=True)
        
        # Reliability score
        if reliability and reliability > 0:
            reliability_pct = min(100, reliability)
            embed.add_field(name="Reliability", value=f"{reliability_pct}%", inline=True)
        
        # Thumbs up (confirmations)
        if thumbs_up and thumbs_up > 0:
            embed.add_field(name="👍 Confirmations", value=str(thumbs_up), inline=True)
        
        # Time reported
        formatted_time = format_timestamp(created)
        if formatted_time:
            embed.add_field(name="🕐 Reported", value=formatted_time, inline=True)
        
        # Map link
        geometry = data.get('geometry', {})
        if geometry.get('coordinates'):
            coords = geometry['coordinates']
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                layer_map = {
                    'waze_hazards': 'waze-hazards',
                    'waze_police': 'waze-police',
                    'waze_roadwork': 'waze-roadwork',
                }
                layer = layer_map.get(alert_type, 'incidents')
                map_url = build_map_url(lat, lon, label=title or display_type, layer=layer)
                embed.add_field(name="🗺️ Map", value=f"[View on Map]({map_url})", inline=True)
        
        embed.set_footer(text="Waze Community Reports")
        return embed
    
    def _build_power_embed(self, data: Dict[str, Any], alert_type: str) -> discord.Embed:
        """Build embed for power outages"""
        if alert_type == 'power_ausgrid':
            return self._build_ausgrid_embed(data)
        return self._build_endeavour_embed(data)
    
    def _build_endeavour_embed(self, data: Dict[str, Any]) -> discord.Embed:
        """Build embed for Endeavour outages"""
        suburb = data.get('suburb', 'Unknown')
        streets = strip_html(data.get('streets', ''))
        customers = data.get('customersAffected', 0)
        status = data.get('status', '')
        cause = data.get('cause', '')
        outage_type = data.get('outageType', 'Unplanned')
        restoration = data.get('estimatedRestoration', '')
        start_time = data.get('startTime', '')
        last_updated = data.get('lastUpdated', '')
        
        title_prefix = "🔧" if outage_type == 'Planned' else "⚡"
        
        # Use source timestamp (start time or last updated)
        source_time = start_time or last_updated
        embed_timestamp = parse_timestamp_to_datetime(source_time) or datetime.now()
        
        embed = discord.Embed(
            title=f"{title_prefix} {outage_type} Outage - {suburb}",
            color=self.COLORS['power_endeavour'],
            timestamp=embed_timestamp
        )
        
        if is_valid_value(streets):
            embed.add_field(name="Streets Affected", value=streets[:1024], inline=False)
        
        if is_valid_value(customers) and customers > 0:
            embed.add_field(name="Customers Affected", value=f"**{customers:,}**", inline=True)
        
        if is_valid_value(status):
            embed.add_field(name="Status", value=status, inline=True)
        
        if is_valid_value(cause):
            embed.add_field(name="Cause", value=cause, inline=False)
        
        # Format and validate restoration time
        formatted_restoration = format_timestamp(restoration)
        if formatted_restoration:
            embed.add_field(name="Est. Restoration", value=formatted_restoration, inline=True)
        
        # Map link
        lat = data.get('latitude')
        lon = data.get('longitude')
        if lat and lon:
            map_url = build_map_url(lat, lon, label=f"{outage_type} Outage - {suburb}", layer="outages")
            embed.add_field(name="🗺️ Map", value=f"[View on Map]({map_url})", inline=True)
        
        embed.set_footer(text="Endeavour Energy")
        return embed
    
    def _build_ausgrid_embed(self, data: Dict[str, Any]) -> discord.Embed:
        """Build embed for Ausgrid outages"""
        # Ausgrid fields - handle both camelCase and PascalCase
        suburb = data.get('Suburb') or data.get('suburb', 'Unknown')
        street = strip_html(data.get('StreetName') or data.get('streetName', ''))
        customers = data.get('CustomersAffected') or data.get('customersAffected', 0)
        postcode = data.get('Postcode') or data.get('postcode', '')
        outage_type = data.get('OutageType') or data.get('outageType', '')
        cause = data.get('Cause') or data.get('cause', '')
        start_time = data.get('StartTime') or data.get('startTime', '')
        est_restore = data.get('EstRestoration') or data.get('estRestoration', '')
        
        # P = Planned, U = Unplanned (or full text)
        is_planned = outage_type in ('P', 'Planned', 'planned')
        type_text = 'Planned' if is_planned else 'Unplanned'
        title_prefix = "🔧" if is_planned else "⚡"
        
        # Use source timestamp (start time from Ausgrid)
        embed_timestamp = parse_timestamp_to_datetime(start_time) or datetime.now()
        
        embed = discord.Embed(
            title=f"{title_prefix} {type_text} Outage - {suburb}",
            color=self.COLORS['power_ausgrid'],
            timestamp=embed_timestamp
        )
        
        if is_valid_value(street):
            embed.add_field(name="📍 Street", value=street[:1024], inline=False)
        
        if is_valid_value(postcode):
            embed.add_field(name="Postcode", value=postcode, inline=True)
        
        if is_valid_value(customers) and customers > 0:
            embed.add_field(name="Customers Affected", value=f"**{customers:,}**", inline=True)
        
        if is_valid_value(cause):
            embed.add_field(name="Cause", value=cause, inline=False)
        
        # Format and validate start time
        formatted_start = format_timestamp(start_time)
        if formatted_start:
            embed.add_field(name="Started", value=formatted_start, inline=True)
        
        # Format and validate restoration time
        formatted_restoration = format_timestamp(est_restore)
        if formatted_restoration:
            embed.add_field(name="Est. Restoration", value=formatted_restoration, inline=True)
        
        # Map link
        lat = data.get('Latitude') or data.get('latitude')
        lon = data.get('Longitude') or data.get('longitude')
        if lat and lon:
            map_url = build_map_url(lat, lon, label=f"{type_text} Outage - {suburb}", layer="outages")
            embed.add_field(name="🗺️ Map", value=f"[View on Map]({map_url})", inline=True)
        
        embed.set_footer(text="Ausgrid")
        return embed
    
    def _build_generic_embed(self, data: Dict[str, Any], alert_type: str) -> discord.Embed:
        """Build a generic embed for unknown alert types"""
        icon = self.ICONS.get(alert_type, '📢')
        color = self.COLORS.get(alert_type, 0x5865F2)
        
        embed = discord.Embed(
            title=f"{icon} Alert",
            color=color,
            timestamp=datetime.now()
        )
        
        for key, value in data.items():
            if value and key not in ['geometry', 'properties', 'type']:
                if isinstance(value, (dict, list)):
                    continue
                clean_value = strip_html(str(value))
                if is_valid_value(clean_value):
                    embed.add_field(
                        name=key.replace('_', ' ').title(),
                        value=clean_value[:1024],
                        inline=True
                    )
        
        return embed
    
    def _build_user_incident_embed(self, data: Dict[str, Any], previous_message: Dict[str, Any] = None) -> discord.Embed:
        """Build embed for user-submitted incidents from Supabase"""
        title = strip_html(data.get('title', 'User Incident'))
        location = strip_html(data.get('location', ''))
        description = strip_html(data.get('description', ''))
        status = data.get('status', 'Active')
        size = data.get('size', '')
        
        # Type can be an array or string
        inc_type = data.get('type', '')
        if isinstance(inc_type, list):
            type_display = ', '.join(inc_type)
        else:
            type_display = str(inc_type) if inc_type else ''
        
        # Responding agencies
        agencies = data.get('responding_agencies', [])
        if isinstance(agencies, str):
            agencies = [agencies]
        
        # Coordinates
        lat = data.get('lat')
        lng = data.get('lng')
        
        # Created time
        created_at = data.get('created_at', '')
        
        # Incident logs
        logs = data.get('logs', [])
        
        # Determine color based on type or status
        color = self.COLORS['user_incidents']
        icon = '📢'
        
        # Check for fire-related types
        type_lower = type_display.lower() if type_display else ''
        if 'fire' in type_lower or 'bush' in type_lower:
            color = 0xFF4500  # Orange-red for fires
            icon = '🔥'
        elif 'flood' in type_lower or 'water' in type_lower:
            color = 0x00CED1  # Cyan for water
            icon = '🌊'
        elif 'crash' in type_lower or 'accident' in type_lower or 'mva' in type_lower:
            color = 0xEF4444  # Red for crashes
            icon = '💥'
        elif 'rescue' in type_lower:
            color = 0x3B82F6  # Blue for rescue
            icon = '🚑'
        elif 'hazmat' in type_lower:
            color = 0xA855F7  # Purple for hazmat
            icon = '☣️'
        elif 'police' in type_lower or 'pursuit' in type_lower:
            color = 0x1E40AF  # Dark blue for police
            icon = '🚔'
        elif 'storm' in type_lower or 'weather' in type_lower:
            color = 0x6B7280  # Gray for weather
            icon = '⛈️'
        
        # Use source timestamp (created_at from Supabase)
        embed_timestamp = parse_timestamp_to_datetime(created_at) or datetime.now()
        
        embed = discord.Embed(
            title=f"{icon} {title}",
            color=color,
            timestamp=embed_timestamp
        )
        
        # Build description with location
        desc_parts = []
        if is_valid_value(location):
            desc_parts.append(f"📍 **{location}**")
        if is_valid_value(description):
            desc_parts.append(description[:500])
        if desc_parts:
            embed.description = "\n\n".join(desc_parts)
        
        # Info row - compact inline fields
        info_parts = []
        if is_valid_value(type_display):
            info_parts.append(("Type", type_display))
        if is_valid_value(status):
            info_parts.append(("Status", status))
        if is_valid_value(size):
            info_parts.append(("Size", size))
        
        for name, value in info_parts:
            embed.add_field(name=name, value=value, inline=True)
        
        # Responding agencies (if any)
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
                    if key.lower() in agency.lower():
                        icon_for_agency = emoji + ' '
                        break
                agency_list.append(f"{icon_for_agency}{agency}")
            embed.add_field(name="Responding", value=" • ".join(agency_list), inline=False)
        
        # Incident logs - cleaner format with Discord timestamps
        if logs and len(logs) > 0:
            # Sort logs by created_at descending (newest first)
            sorted_logs = sorted(logs, key=lambda x: x.get('created_at', ''), reverse=True)
            log_lines = []
            for log in sorted_logs[:5]:  # Show max 5 logs
                log_ts = log.get('created_at', '')
                log_msg = strip_html(log.get('message', ''))[:200]
                if log_msg:
                    # Use Discord timestamp for local time display
                    dt = parse_timestamp_to_datetime(log_ts)
                    if dt:
                        unix_ts = int(dt.timestamp())
                        time_str = f"<t:{unix_ts}:t>"  # Short time format
                    else:
                        time_str = "Unknown time"
                    log_lines.append(f"**{time_str}** — {log_msg}")
            
            if log_lines:
                logs_text = "\n".join(log_lines)
                if len(logs) > 5:
                    logs_text += f"\n*+{len(logs) - 5} more...*"
                embed.add_field(name="📋 Incident Log", value=logs_text[:1024], inline=False)
        
        # Footer row with reported time and map link
        footer_parts = []
        
        # Reported time (Discord timestamp)
        dt_created = parse_timestamp_to_datetime(created_at)
        if dt_created:
            unix_ts = int(dt_created.timestamp())
            footer_parts.append(f"🕐 Reported <t:{unix_ts}:R>")  # Relative time
        
        # Map link
        if lat and lng:
            map_url = build_map_url(lat, lng, label=title, layer="incidents")
            footer_parts.append(f"[🗺️ View on Map]({map_url})")
        
        if footer_parts:
            embed.add_field(name="\u200b", value=" • ".join(footer_parts), inline=False)
        
        # Link to previous message for this incident (for tracking updates)
        if previous_message and previous_message.get('message_url'):
            prev_status = previous_message.get('status', 'initial')
            embed.add_field(
                name="📜 Previous Update",
                value=f"[View original alert ({prev_status})]({previous_message['message_url']})",
                inline=False
            )
        
        embed.set_footer(text="NSW PSN • User Submitted Incident")
        return embed
    
    def build_pager_embed(self, msg: Dict[str, Any]) -> discord.Embed:
        """Build embed for pager messages"""
        capcode = msg.get('capcode', 'UNKNOWN')
        station_code = msg.get('station_code', '')
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
        
        # Determine if it's a stop message
        is_stop = 'stop' in msg_type.lower() if msg_type else False
        
        color = self.COLORS['pager_stop'] if is_stop else self.COLORS['pager']
        icon = '🛑' if is_stop else '📟'
        
        # Build title - prefer type, then alias, then generic
        if is_stop:
            title = f"{icon} STOP MESSAGE"
        elif msg_type and msg_type != 'Pager Alert':
            title = f"{icon} {msg_type}"
        elif alias:
            title = f"{icon} {alias}"
        else:
            title = f"{icon} Pager Alert"
        
        # Use source timestamp from pager message
        embed_timestamp = parse_timestamp_to_datetime(timestamp) or datetime.now()
        
        embed = discord.Embed(
            title=title,
            color=color,
            timestamp=embed_timestamp
        )
        
        # Category/dispatch type (FIRECALL, VEHICLE FIRE, etc.)
        if is_valid_value(category) and category != msg_type:
            embed.add_field(name="🚨 Dispatch", value=f"**{category}**", inline=True)
        
        # Location info - address, suburb, council
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
            embed.add_field(name="📍 Location", value="\n".join(location_parts), inline=False)
        
        # Reference info - capcode and incident
        ref_parts = []
        if is_valid_value(capcode):
            ref_parts.append(f"**Capcode:** `{capcode}`")
        if is_valid_value(incident_id):
            ref_parts.append(f"**Incident:** `{incident_id}`")
        if is_valid_value(agency):
            ref_parts.append(f"**Area:** {agency}")
        if ref_parts:
            embed.add_field(name="📌 Reference", value="\n".join(ref_parts), inline=True)
        
        # Map link
        if coordinates:
            lat = coordinates.get('lat')
            lon = coordinates.get('lon')
            if lat and lon:
                label = f"{msg_type} - {incident_id}" if msg_type and incident_id else (msg_type or incident_id or capcode)
                map_url = build_map_url(lat, lon, label=label, layer="pager")
                embed.add_field(name="🗺️ Map", value=f"[View on Map]({map_url})", inline=True)
        
        # Raw message - collapsed at bottom
        if is_valid_value(raw) and len(raw) > 10:
            raw_display = raw[:600] + "..." if len(raw) > 600 else raw
            embed.add_field(
                name="📝 Raw Message",
                value=f"```{raw_display}```",
                inline=False
            )
        
        embed.set_footer(text="NSW PSN Pager Feed")
        return embed

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

    # 800x1 transparent PNG hosted alongside the frontend assets. Discord sizes
    # an embed's container to the embedded image width, so attaching a very
    # wide transparent spacer forces the card to render wide instead of the
    # default narrow "mobile-friendly" layout.
    _RADIO_WIDENER_IMAGE = f"{MAP_BASE_URL}/assets/embed-widener.png"

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
    _ALERT_TYPE_LABELS = {
        'rfs': 'RFS Major Incidents',
        'bom': 'BOM Warnings',
        'traffic_incidents': 'Traffic Incidents',
        'traffic_roadwork': 'Traffic Roadwork',
        'traffic_flood': 'Flood Hazards',
        'traffic_fire': 'Traffic Fires',
        'traffic_major': 'Major Events',
        'power_endeavour': 'Endeavour Outages',
        'power_ausgrid': 'Ausgrid Outages',
        'waze_hazards': 'Waze Hazards',
        'waze_police': 'Waze Police',
        'waze_roadwork': 'Waze Roadwork',
        'user_incidents': 'User Incidents',
        'radio_summary': 'Radio Summary',
    }

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

        footer_bits = []
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
                units.append(u)
            elif isinstance(u, dict):
                uid = u.get('id') or u.get('callsign')
                if uid:
                    units.append(uid)
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
    # /summary command — Components V2 dashboard for NSW incident totals.
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

    def _append_container_footer(self, container, footer_bits):
        """Emit the footer for a V2 Container.

        Components V2 TextDisplay DOES NOT render `[label](url)` markdown as
        clickable links — the entire token renders as literal text. To get
        real clickable affordances we extract any `[label](url)` bits from
        the footer and emit them as **Link Buttons** in an ActionRow below
        the subtle-text line instead.
        """
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
            content="# 📊 NSW Incident Summary"
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
                f"-# 🌐 [View the full live dashboard on nswpsn.forcequit.xyz]({MAP_BASE_URL}/) · "
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

    def build_radio_summary_embeds(self, data: Dict[str, Any]) -> list:
        """Return a list of embeds (≤10) for a radio summary alert.

        [0] Main summary embed (title, plain-text summary, footer).
        [1..N] One embed per incident, colour-coded by severity.

        Kept for backward compatibility — prefer `build_radio_summary_components`
        when the target client is discord.py 2.6+.
        """
        details = data.get('details') or {}
        if isinstance(details, str):
            import json as _json
            try:
                details = _json.loads(details)
            except Exception:
                details = {}

        main = self._build_radio_summary_embed(data, details=details, include_incidents=False)
        embeds = [main]

        # Discord caps at 10 embeds per message. Reserve [0] for the summary,
        # which leaves 9 slots for incidents.
        incidents = self._radio_incidents_from_details(details)
        for inc in incidents[:9]:
            embeds.append(self._build_radio_incident_embed(inc))
        return embeds

    def _build_radio_incident_embed(self, inc: Dict[str, Any]) -> discord.Embed:
        """One embed per incident inside a radio-summary message.

        Renders the new LLM incident schema (summary / locations[] / agencies[]
        / codes[] / units[] / window / transcripts[]). Falls back to the legacy
        timeline[] shape when transcripts[] is absent.
        """
        sev = (inc.get('severity') or '').strip().lower()
        color = self._RADIO_SEV_COLORS.get(sev, self._RADIO_MAIN_COLOR)

        title_part = strip_html(str(inc.get('title') or inc.get('type') or 'Incident'))
        sev_label = f"[{sev.upper()}] " if sev else ""
        title = f"🚨 {sev_label}{title_part}"[:256]

        # --- Description: summary + location line + time/status line ---------
        desc_bits = []
        body = inc.get('summary') or inc.get('description') or ''
        if body:
            desc_bits.append(strip_html(str(body)))

        # locations[] (new) — fall back to legacy singular location/suburb
        locations = inc.get('locations') or []
        if isinstance(locations, str):
            locations = [locations]
        loc_line = ''
        if locations:
            loc_parts = [strip_html(str(l)) for l in locations if l]
            loc_parts = [p for p in loc_parts if p]
            if loc_parts:
                loc_line = f"📍 {', '.join(loc_parts)}"
        else:
            legacy_loc = inc.get('location') or inc.get('suburb')
            if legacy_loc:
                loc_line = f"📍 {strip_html(str(legacy_loc))}"

        # Time window: prefer window.start/end, else min/max of transcripts[].time
        window = inc.get('window') or {}
        win_start = (window.get('start') if isinstance(window, dict) else None) or ''
        win_end = (window.get('end') if isinstance(window, dict) else None) or ''
        transcripts = inc.get('transcripts') or []
        if (not win_start or not win_end) and transcripts:
            times = [str(t.get('time') or '').strip() for t in transcripts if isinstance(t, dict)]
            times = [t for t in times if t]
            if times:
                if not win_start:
                    win_start = min(times)
                if not win_end:
                    win_end = max(times)

        status = inc.get('status') or ''
        time_line = ''
        if win_start and win_end:
            time_line = f"🕐 {win_start} – {win_end}"
        elif win_start:
            time_line = f"🕐 {win_start}"
        if status:
            time_line = f"{time_line} · {strip_html(str(status))}" if time_line else f"🕐 {strip_html(str(status))}"

        # Stitch together: summary paragraph, blank line, then loc + time lines
        tail_lines = [x for x in (loc_line, time_line) if x]
        if tail_lines:
            if desc_bits:
                # Blank line between summary and the metadata block
                desc_bits.append('\n'.join(tail_lines))
            else:
                desc_bits.append('\n'.join(tail_lines))
        description = '\n\n'.join(desc_bits)[:4000] or '—'

        embed = discord.Embed(
            title=title,
            description=description,
            color=color,
        )

        # --- Units ----------------------------------------------------------
        units = inc.get('units') or []
        if units:
            clean_units = [strip_html(str(u)) for u in units if u]
            clean_units = [u for u in clean_units if u]
            if clean_units:
                units_txt = ', '.join(clean_units)[:1024]
                embed.add_field(
                    name="🚒 Units",
                    value=units_txt,
                    inline=len(clean_units) <= 3,
                )

        # --- Codes ----------------------------------------------------------
        codes = inc.get('codes') or []
        if codes:
            clean_codes = [strip_html(str(c)) for c in codes if c]
            clean_codes = [c for c in clean_codes if c]
            if clean_codes:
                embed.add_field(
                    name="🎯 Codes",
                    value=', '.join(clean_codes)[:1024],
                    inline=True,
                )

        # --- Agencies -------------------------------------------------------
        agencies = inc.get('agencies') or []
        if agencies:
            clean_agencies = [strip_html(str(a)) for a in agencies if a]
            clean_agencies = [a for a in clean_agencies if a]
            if clean_agencies:
                embed.add_field(
                    name="🛰️ Agencies",
                    value=', '.join(clean_agencies)[:1024],
                    inline=True,
                )

        # --- Transcripts (new schema) or Timeline (legacy) ------------------
        RADIO_CALL_URL = "https://radio.forcequit.xyz/?call={id}"
        FIELD_CAP = 1024

        def _build_transcript_lines(raw_lines):
            """Shrink individual line texts (with trailing '…') until the
            joined block fits under Discord's 1024-char field cap. Keeps all
            rows — trims text bodies, never drops whole lines.

            Each element of raw_lines is a tuple (prefix, text) where prefix
            is the non-truncatable lead (e.g. `\\`00:09\\` [#123](url) `) and
            text is the truncatable transcript body.
            """
            # Start with generous per-line text budget, shrink until it fits.
            lines = [f"{p}{t}" for p, t in raw_lines]
            joined = '\n'.join(lines)
            if len(joined) <= FIELD_CAP:
                return joined

            # Iteratively clip text bodies. Allocate a per-line text budget.
            n = len(raw_lines)
            # Reserve newlines between lines
            overhead_per_line = sum(len(p) for p, _ in raw_lines) + (n - 1)
            budget = FIELD_CAP - overhead_per_line
            if budget < n:  # extreme fallback — prefixes alone already huge
                return joined[:FIELD_CAP]
            per_text = max(1, budget // n)
            trimmed = []
            for p, t in raw_lines:
                if len(t) > per_text:
                    t = t[: max(1, per_text - 1)].rstrip() + '…'
                trimmed.append(f"{p}{t}")
            out = '\n'.join(trimmed)
            # Final safety clamp
            return out[:FIELD_CAP]

        if transcripts:
            # New schema: list of {time, call_id, text}
            TOP_N = 10
            original_count = inc.get('transcripts_count') or len(transcripts)
            shown = transcripts[:TOP_N]
            raw_lines = []
            for t in shown:
                if not isinstance(t, dict):
                    continue
                ttime = str(t.get('time') or '').strip()
                text = strip_html(str(t.get('text') or ''))
                cid = t.get('call_id')
                try:
                    cid_int = int(cid)
                    call_link = f"[#{cid_int}]({RADIO_CALL_URL.format(id=cid_int)})"
                except (TypeError, ValueError):
                    call_link = ''
                time_tag = f"`{ttime}` " if ttime else ''
                prefix = f"{time_tag}{call_link}{' ' if call_link else ''}"
                raw_lines.append((prefix, text))

            if raw_lines:
                body_text = _build_transcript_lines(raw_lines)

                # Append a truncation marker if applicable
                if inc.get('transcripts_truncated'):
                    if original_count and original_count > TOP_N:
                        marker = f"\n-# …Showing top {TOP_N} of {original_count}"
                    else:
                        marker = "\n-# …some transcripts omitted"
                    # Only append if there is room; otherwise trim body to fit
                    if len(body_text) + len(marker) <= FIELD_CAP:
                        body_text = body_text + marker
                    else:
                        allowed = FIELD_CAP - len(marker)
                        body_text = body_text[: max(0, allowed)].rstrip() + marker

                embed.add_field(
                    name=f"📻 Transcripts (top {min(len(shown), TOP_N)})",
                    value=body_text[:FIELD_CAP] or '—',
                    inline=False,
                )
        else:
            # Legacy schema: timeline[] with {time, event, call_ids: [...]}
            timeline = inc.get('timeline') or []
            if timeline:
                legacy_lines = []
                for entry in timeline:
                    if not isinstance(entry, dict):
                        continue
                    etime = str(entry.get('time') or '').strip()
                    event = strip_html(str(entry.get('event') or ''))
                    eids = entry.get('call_ids') or []
                    if not isinstance(eids, (list, tuple)):
                        eids = [eids]
                    # One line per call_id (matches old behaviour)
                    emitted_any = False
                    for cid in eids:
                        try:
                            cid_int = int(cid)
                        except (TypeError, ValueError):
                            continue
                        time_tag = f"`{etime}` " if etime else ''
                        link = f"[#{cid_int}]({RADIO_CALL_URL.format(id=cid_int)}) "
                        legacy_lines.append((f"{time_tag}{link}", event))
                        emitted_any = True
                    # If an entry has no call_ids, still emit one line
                    if not emitted_any:
                        time_tag = f"`{etime}` " if etime else ''
                        legacy_lines.append((time_tag, event))

                if legacy_lines:
                    body_text = _build_transcript_lines(legacy_lines[:20])
                    embed.add_field(
                        name="📻 Transcripts",
                        value=body_text[:FIELD_CAP] or '—',
                        inline=False,
                    )
            else:
                # Final legacy fallback: flat call_ids list
                call_ids = inc.get('call_ids') or inc.get('calls') or []
                if call_ids:
                    links = []
                    for cid in call_ids[:10]:
                        try:
                            cid_int = int(cid)
                        except (TypeError, ValueError):
                            continue
                        links.append(f"[#{cid_int}]({RADIO_CALL_URL.format(id=cid_int)})")
                    if links:
                        embed.add_field(
                            name="📻 Calls",
                            value=' · '.join(links)[:FIELD_CAP],
                            inline=False,
                        )

        return embed

    def _build_radio_summary_embed(self, data: Dict[str, Any],
                                   details: Dict[str, Any] = None,
                                   include_incidents: bool = True) -> discord.Embed:
        """Build embed for rdio-scanner hourly summary alerts.

        `data` is the summary row from /api/summaries/latest (hourly field):
            { id, type, period_start, period_end, day_date, hour_slot,
              summary, call_count, details: { incidents: [...] }, ... }

        When `include_incidents=False` the per-incident fields are skipped —
        used by `build_radio_summary_embeds` which renders them as separate
        embeds instead.
        """
        summary_type = (data.get('type') or 'hourly').lower()
        day = data.get('day_date') or ''
        hour_slot = data.get('hour_slot')
        call_count = data.get('call_count')
        if details is None:
            details = data.get('details') or {}
            if isinstance(details, str):
                import json as _json
                try:
                    details = _json.loads(details)
                except Exception:
                    details = {}

        # Build a human-friendly hour range ("11 PM – 12 AM") from the
        # period_start / period_end ISO strings. Falls back to the hour_slot
        # number if parsing fails or the row doesn't have period bounds.
        def _fmt_hour_range(start_iso, end_iso, tz_name):
            from datetime import datetime as _dt
            try:
                start = _dt.fromisoformat(start_iso.replace('Z', '+00:00')) if start_iso else None
                end = _dt.fromisoformat(end_iso.replace('Z', '+00:00')) if end_iso else None
                if start is None or end is None:
                    return None
                if tz_name:
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
                    # %-I / %#I not supported — fall back to padded 12-hour.
                    return f"{start.strftime('%I %p').lstrip('0')} – {end.strftime('%I %p').lstrip('0')}"
            except Exception:
                return None

        hour_range = _fmt_hour_range(
            data.get('period_start'),
            data.get('period_end'),
            details.get('tz') or 'Australia/Sydney',
        )

        if summary_type == 'hourly':
            if hour_range and day:
                title = f"📻 Radio Summary — {day} · {hour_range}"
            elif hour_range:
                title = f"📻 Radio Summary — {hour_range}"
            elif hour_slot is not None:
                # Keep old fallback but use "XX:00 – YY:00" rather than a bare
                # hour_slot which could render as "24:00".
                end_h = int(hour_slot) % 24
                start_h = (end_h - 1) % 24
                title = f"📻 Radio Summary — {day} · {start_h:02d}:00 – {end_h:02d}:00"
            else:
                title = "📻 Radio Summary"
        elif summary_type == 'adhoc':
            title = "📻 Radio Summary — Recent Activity"
        else:
            title = "📻 Radio Summary"

        # Description: prefer the plain-text summary if present; otherwise
        # build one from the top few incidents.
        description = (data.get('summary') or '').strip()
        if not description:
            incidents = self._radio_incidents_from_details(details)
            lines = []
            for inc in incidents[:5]:
                t = strip_html(str(inc.get('title') or inc.get('type') or 'Incident'))
                sev = inc.get('severity')
                if sev:
                    t = f"**[{sev}]** {t}"
                lines.append(f"• {t}")
            description = '\n'.join(lines) if lines else "No notable activity."
        if len(description) > 4000:
            description = description[:3997] + '…'

        embed = discord.Embed(
            title=title,
            description=description,
            color=self._RADIO_MAIN_COLOR,
            timestamp=datetime.now(),
            url=f"{MAP_BASE_URL}/live.html#radio-summary",
        )

        # Legacy path: inline up to 3 incidents as fields. Skipped when the
        # multi-embed builder is rendering — it makes a dedicated embed per
        # incident instead.
        if include_incidents:
            incidents = self._radio_incidents_from_details(details)
            for inc in incidents[:3]:
                name_bits = []
                sev = inc.get('severity')
                if sev:
                    name_bits.append(f"[{sev}]")
                title_part = strip_html(str(inc.get('title') or inc.get('type') or 'Incident'))
                name_bits.append(title_part)
                field_name = ' '.join(name_bits)[:256] or 'Incident'

                desc_bits = []
                loc = inc.get('location') or inc.get('suburb')
                if loc:
                    desc_bits.append(f"📍 {strip_html(str(loc))}")
                body = inc.get('summary') or inc.get('description') or ''
                if body:
                    desc_bits.append(strip_html(str(body)))
                field_val = '\n'.join(desc_bits).strip()[:1024] or '—'
                embed.add_field(name=field_name, value=field_val, inline=False)

        footer_bits = []
        if call_count:
            footer_bits.append(f"{call_count} calls")
        model = data.get('model')
        if model:
            footer_bits.append(model)
        if footer_bits:
            embed.set_footer(text=' · '.join(footer_bits))

        return embed

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

    def build_alert_container(self, alert: Dict[str, Any],
                              previous_message: Dict[str, Any] = None):
        """Route an alert dict to the right container builder.

        Mirrors `build_alert_embed` — returns a single `discord.ui.Container`
        for all non-radio alert types. radio_summary keeps its dedicated
        multi-container builder (`build_radio_summary_components`).
        """
        alert_type = alert.get('type', 'unknown')
        data = alert.get('data', {})

        if alert_type == 'rfs':
            return self.build_rfs_container(data, previous_message=previous_message)
        elif alert_type == 'bom':
            return self.build_bom_container(data)
        elif alert_type.startswith('traffic_'):
            return self.build_traffic_container(data, alert_type)
        elif alert_type.startswith('power_'):
            return self.build_power_container(data, alert_type)
        elif alert_type.startswith('waze_'):
            return self.build_waze_container(data, alert_type)
        elif alert_type == 'user_incidents':
            return self.build_user_incident_container(data, previous_message=previous_message)
        else:
            return self.build_generic_container(data, alert_type)

    # ---- RFS ---------------------------------------------------
    def build_rfs_container(self, data: Dict[str, Any],
                            previous_message: Dict[str, Any] = None):
        """Components V2 container for NSW RFS major incidents."""
        props = data.get('properties', {})
        title = strip_html(props.get('title', 'Unknown Incident'))
        link = props.get('link', '')
        raw_desc = props.get('description', '') or ''

        status = props.get('status', '')
        location = props.get('location', '')
        size = props.get('size', '')
        alert_level = props.get('alertLevel', '')
        council = props.get('councilArea', '')
        fire_type = props.get('fireType', '')
        updated = props.get('updated', '')
        updated_iso = props.get('updatedISO', '')
        responsible_agency = props.get('responsibleAgency', '')

        if raw_desc and not status and not location:
            parsed = self._parse_rfs_description(raw_desc)
            alert_level = parsed.get('alert_level', '') or alert_level
            status = parsed.get('status', '') or status
            location = parsed.get('location', '') or location
            size = parsed.get('size', '') or size
            council = parsed.get('council_area', '') or council
            fire_type = parsed.get('type', '') or fire_type
            updated = parsed.get('updated', '') or updated

        if alert_level and len(alert_level) > 30:
            match = re.match(r'^(Advice|Watch and Act|Emergency Warning|Emergency)',
                             alert_level, re.IGNORECASE)
            alert_level = match.group(1) if match else ''

        color = self.COLORS['rfs']
        level_emoji = '🟡'
        if not alert_level and status:
            status_lower = status.lower()
            if 'out of control' in status_lower:
                alert_level = 'Emergency Warning'
            elif 'being controlled' in status_lower:
                alert_level = 'Watch and Act'
            elif 'under control' in status_lower:
                alert_level = 'Advice'

        if alert_level:
            level_lower = alert_level.lower()
            if 'emergency' in level_lower:
                color = 0xFF0000
                level_emoji = '🔴'
            elif 'watch' in level_lower:
                color = 0xFF8C00
                level_emoji = '🟠'
            elif 'advice' in level_lower:
                color = 0xFFD700
                level_emoji = '🟡'

        container = discord.ui.Container(accent_colour=color)
        container.add_item(discord.ui.TextDisplay(content=f"### 🔥 {title}"))

        meta_bits = []
        if alert_level:
            meta_bits.append(f"{level_emoji} **{alert_level}**")
        if status:
            meta_bits.append(f"📊 {status}")
        if fire_type:
            meta_bits.append(f"🔥 {fire_type}")
        if size:
            meta_bits.append(f"📏 {size}")
        if meta_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(meta_bits))
            ))

        loc_bits = []
        if location:
            loc_bits.append(f"📍 {location}")
        agency_bits = []
        if council:
            agency_bits.append(f"🏛️ {council}")
        if responsible_agency:
            agency_bits.append(f"🚒 {responsible_agency}")
        if agency_bits:
            loc_bits.append(' · '.join(agency_bits))
        if loc_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text('\n'.join(loc_bits))
            ))

        dt_updated = parse_timestamp_to_datetime(updated_iso)
        footer_bits = []
        if dt_updated:
            footer_bits.append(f"🕐 <t:{int(dt_updated.timestamp())}:R>")

        geometry = data.get('geometry', {})
        if geometry.get('coordinates'):
            coords = geometry['coordinates']
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                map_url = build_map_url(lat, lon, label=title, layer="rfs")
                footer_bits.append(f"[🗺️ Map]({map_url})")

        if link:
            footer_bits.append(f"[ℹ️ Details]({link})")

        if previous_message and previous_message.get('message_url'):
            prev_status = previous_message.get('status', 'initial')
            footer_bits.append(
                f"[📜 Previous ({prev_status})]({previous_message['message_url']})"
            )

        self._append_container_footer(container, footer_bits)
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
        self._append_container_footer(container, footer_bits)
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
        if geometry.get('coordinates'):
            coords = geometry['coordinates']
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                map_url = build_map_url(lat, lon, label=title, layer="incidents")
                footer_bits.append(f"[🗺️ Map]({map_url})")

        self._append_container_footer(container, footer_bits)
        return container

    # ---- Waze --------------------------------------------------
    def build_waze_container(self, data: Dict[str, Any], alert_type: str):
        """Components V2 container for Waze hazards / police / roadwork."""
        props = data.get('properties', {})
        waze_subtype = props.get('wazeSubtype', '')
        display_type = props.get('displayType', '')
        title = strip_html(props.get('title', ''))
        street = strip_html(props.get('street', ''))
        city = strip_html(props.get('city', ''))
        location = strip_html(props.get('location', ''))
        thumbs_up = props.get('thumbsUp', 0)
        reliability = props.get('reliability', 0)
        created = props.get('created', '')

        icon = self.ICONS.get(alert_type, '⚠️')
        color = self.COLORS.get(alert_type, 0xEAB308)

        type_labels = {
            'waze_hazards': 'Waze Hazard',
            'waze_police': 'Waze Police Report',
            'waze_roadwork': 'Waze Roadwork',
        }
        type_label = type_labels.get(alert_type, 'Waze Alert')
        heading = (f"### {icon} {display_type}" if display_type and display_type != 'Unknown'
                   else f"### {icon} {type_label}")

        container = discord.ui.Container(accent_colour=color)
        container.add_item(discord.ui.TextDisplay(content=heading))

        if is_valid_value(title) and title != display_type:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(title[:2000])
            ))

        if is_valid_value(location):
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"📍 {location[:1500]}")
            ))
        elif is_valid_value(street) or is_valid_value(city):
            loc_parts = [p for p in [street, city] if is_valid_value(p)]
            if loc_parts:
                container.add_item(discord.ui.TextDisplay(
                    content=self._clip_text(f"📍 {', '.join(loc_parts)}")
                ))

        meta_bits = []
        if is_valid_value(waze_subtype) and waze_subtype.lower() != display_type.lower():
            meta_bits.append(f"Type: {waze_subtype.replace('_', ' ').title()}")
        if reliability and reliability > 0:
            meta_bits.append(f"Reliability: {min(100, reliability)}%")
        if thumbs_up and thumbs_up > 0:
            meta_bits.append(f"👍 {thumbs_up}")
        if meta_bits:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(' · '.join(meta_bits))
            ))

        footer_bits = []
        dt_created = parse_timestamp_to_datetime(created)
        if dt_created:
            footer_bits.append(f"🕐 <t:{int(dt_created.timestamp())}:R>")

        geometry = data.get('geometry', {})
        if geometry.get('coordinates'):
            coords = geometry['coordinates']
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                layer_map = {
                    'waze_hazards': 'waze-hazards',
                    'waze_police': 'waze-police',
                    'waze_roadwork': 'waze-roadwork',
                }
                layer = layer_map.get(alert_type, 'incidents')
                map_url = build_map_url(lat, lon, label=title or display_type, layer=layer)
                footer_bits.append(f"[🗺️ Map]({map_url})")

        self._append_container_footer(container, footer_bits)
        return container

    # ---- Power (dispatcher) ------------------------------------
    def build_power_container(self, data: Dict[str, Any], alert_type: str):
        if alert_type == 'power_ausgrid':
            return self.build_ausgrid_container(data)
        return self.build_endeavour_container(data)

    def build_endeavour_container(self, data: Dict[str, Any]):
        """Components V2 container for Endeavour power outages."""
        suburb = data.get('suburb', 'Unknown')
        streets = strip_html(data.get('streets', ''))
        customers = data.get('customersAffected', 0)
        status = data.get('status', '')
        cause = data.get('cause', '')
        outage_type = data.get('outageType', 'Unplanned')
        restoration = data.get('estimatedRestoration', '')
        start_time = data.get('startTime', '')
        last_updated = data.get('lastUpdated', '')

        title_prefix = "🔧" if outage_type == 'Planned' else "⚡"

        container = discord.ui.Container(accent_colour=self.COLORS['power_endeavour'])
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
        self._append_container_footer(container, footer_bits)
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

        container = discord.ui.Container(accent_colour=self.COLORS['power_ausgrid'])
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
        self._append_container_footer(container, footer_bits)
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

        color = self.COLORS['user_incidents']
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
                    if key.lower() in agency.lower():
                        icon_for_agency = emoji + ' '
                        break
                agency_list.append(f"{icon_for_agency}{agency}")
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text(f"Responding: {' • '.join(agency_list)}")
            ))

        if logs and len(logs) > 0:
            sorted_logs = sorted(logs, key=lambda x: x.get('created_at', ''), reverse=True)
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
            map_url = build_map_url(lat, lng, label=title, layer="incidents")
            footer_bits.append(f"[🗺️ Map]({map_url})")
        if previous_message and previous_message.get('message_url'):
            prev_status = previous_message.get('status', 'initial')
            footer_bits.append(
                f"[📜 Previous ({prev_status})]({previous_message['message_url']})"
            )
        self._append_container_footer(container, footer_bits)
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
        self._append_container_footer(container, footer_bits)
        return container

    # ---- Generic fallback --------------------------------------
    def build_generic_container(self, data: Dict[str, Any], alert_type: str):
        """Fallback Components V2 container for unknown alert types."""
        icon = self.ICONS.get(alert_type, '📢')
        color = self.COLORS.get(alert_type, 0x5865F2)
        container = discord.ui.Container(accent_colour=color)
        container.add_item(discord.ui.TextDisplay(content=f"### {icon} Alert"))
        lines = []
        for key, value in (data or {}).items():
            if value and key not in ['geometry', 'properties', 'type']:
                if isinstance(value, (dict, list)):
                    continue
                clean_value = strip_html(str(value))
                if is_valid_value(clean_value):
                    lines.append(f"**{key.replace('_', ' ').title()}:** {clean_value[:500]}")
        if lines:
            container.add_item(discord.ui.TextDisplay(
                content=self._clip_text('\n'.join(lines))
            ))
        return container
