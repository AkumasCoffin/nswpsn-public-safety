"""
Embed Builder - Creates beautiful Discord embeds for various alert types.
"""

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
