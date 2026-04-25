"""
Alert Poller - Fetches alerts from the NSW PSN API and tracks new incidents.
"""

import os
import aiohttp
import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger('nswpsn-bot.poller')


class AlertPoller:
    def __init__(self, api_base_url: str, api_key: str, database):
        self.api_base_url = api_base_url.rstrip('/')
        self.api_key = api_key
        self.db = database
        
        # API endpoints mapping
        self.endpoints = {
            'rfs': '/api/rfs/incidents',
            'bom': '/api/bom/warnings',
            'traffic_incidents': '/api/traffic/incidents',
            'traffic_roadwork': '/api/traffic/roadwork',
            'traffic_flood': '/api/traffic/flood',
            'traffic_fire': '/api/traffic/fire',
            'traffic_major': '/api/traffic/majorevent',
            'power_endeavour': '/api/endeavour/current',
            'power_ausgrid': '/api/ausgrid/outages',
            'waze_hazards': '/api/waze/hazards',
            'waze_police': '/api/waze/police',
            'waze_roadwork': '/api/waze/roadwork',
        }
        
        # Track last seen pager message ID
        self.last_pager_id = 0
        
        # Track first poll cycle (skip high-volume sources on first run)
        self._first_poll = True
    
    async def _fetch(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """Fetch data from an API endpoint"""
        url = f"{self.api_base_url}{endpoint}"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'User-Agent': 'NSWPSNBot/1.0',
            'X-Client-Type': 'discord-bot'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.warning(f"API returned {response.status} for {endpoint}")
                        return None
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching {endpoint}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {endpoint}: {e}")
            return None
    
    def _get_alert_id(self, alert_type: str, item: Dict[str, Any]) -> str:
        """Generate a unique ID for an alert"""
        if alert_type == 'rfs':
            # Use guid (unique incident ID from RFS) + status for re-alerting on status changes
            props = item.get('properties', {})
            guid = props.get('guid', '') or props.get('link', '') or props.get('title', '')
            return f"rfs_{guid}_{props.get('status', '')}"
        
        elif alert_type == 'bom':
            # Use title + issued date
            return f"bom_{item.get('title', '')}_{item.get('issued', '')}"
        
        elif alert_type.startswith('traffic_'):
            # Use the ID from properties
            props = item.get('properties', {})
            return str(props.get('id', '')) or hashlib.md5(str(item).encode()).hexdigest()[:16]
        
        elif alert_type == 'power_endeavour':
            # Use incident ID + suburb + streets to make unique 
            # (same incident can have multiple street entries per suburb)
            incident_id = str(item.get('id', '')) or str(item.get('incidentId', ''))
            suburb = item.get('suburb', '') or item.get('location', '')
            streets = item.get('streets', '') or item.get('streetName', '')
            # Create hash of streets to keep ID shorter
            street_hash = hashlib.md5(str(streets).encode()).hexdigest()[:8] if streets else ''
            return f"endeavour_{incident_id}_{suburb}_{street_hash}" if suburb else f"endeavour_{incident_id}"
        
        elif alert_type == 'power_ausgrid':
            # Ausgrid outages - use OutageId or combine Suburb+StreetName
            outage_id = item.get('OutageId') or item.get('outageId', '')
            if outage_id:
                return f"ausgrid_{outage_id}"
            suburb = item.get('Suburb', '') or item.get('suburb', '')
            street = item.get('StreetName', '') or item.get('streetName', '')
            return f"ausgrid_{suburb}_{street}" if suburb else hashlib.md5(str(item).encode()).hexdigest()[:16]
        
        elif alert_type.startswith('waze_'):
            # Waze alerts - use UUID from properties
            props = item.get('properties', {})
            waze_id = props.get('id', '')
            return f"{alert_type}_{waze_id}" if waze_id else hashlib.md5(str(item).encode()).hexdigest()[:16]
        
        elif alert_type == 'user_incidents':
            # User incidents from Supabase - use incident ID + latest log ID for update tracking
            incident_id = item.get('id', '')
            logs = item.get('logs', [])
            latest_log_id = logs[0].get('id', '') if logs else ''
            return f"user_{incident_id}_{latest_log_id}"
        
        # Fallback to hash
        return hashlib.md5(str(item).encode()).hexdigest()[:16]
    
    def _get_alert_timestamp(self, alert_type: str, item: Dict[str, Any]) -> datetime:
        """Extract the source timestamp from an alert item for sorting.

        Always returns a tz-AWARE datetime so mixed-source batches sort cleanly
        (a single naive value mixed with aware ones causes
        `can't compare offset-naive and offset-aware datetimes`).
        Naive parses are normalised to UTC; falls back to current UTC time
        if no timestamp is found.
        """
        def _aware(dt):
            if dt is None:
                return None
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt

        try:
            if alert_type == 'rfs':
                # RFS uses updatedISO (ISO format with timezone)
                props = item.get('properties', {})
                ts = props.get('updatedISO') or props.get('updated') or ''
                if ts:
                    return _aware(datetime.fromisoformat(ts.replace('Z', '+00:00')))

            elif alert_type == 'bom':
                # BOM uses pubDate (RFC 2822 format)
                pub_date = item.get('pubDate', '')
                if pub_date:
                    from email.utils import parsedate_to_datetime
                    return _aware(parsedate_to_datetime(pub_date))

            elif alert_type.startswith('traffic_'):
                # Traffic uses Created (Unix timestamp in milliseconds)
                created = item.get('Created')
                if created:
                    if isinstance(created, (int, float)):
                        # Convert ms to seconds if needed
                        if created > 1e12:
                            return datetime.fromtimestamp(created / 1000, tz=timezone.utc)
                        return datetime.fromtimestamp(created, tz=timezone.utc)

            elif alert_type.startswith('waze_'):
                # Waze uses properties.created (ISO format)
                props = item.get('properties', {})
                ts = props.get('created', '')
                if ts:
                    return _aware(datetime.fromisoformat(ts.replace('Z', '+00:00')))

            elif alert_type == 'power_endeavour':
                # Endeavour uses estimatedRestoreTime or just current time
                restore_time = item.get('estimatedRestoreTime', '')
                if restore_time:
                    return _aware(datetime.fromisoformat(restore_time.replace('Z', '+00:00')))

            elif alert_type == 'power_ausgrid':
                # Ausgrid uses StartTime (ISO format)
                start_time = item.get('StartTime') or item.get('startTime', '')
                if start_time:
                    return _aware(datetime.fromisoformat(start_time.replace('Z', '+00:00')))

            elif alert_type == 'user_incidents':
                # User incidents use created_at (ISO format)
                ts = item.get('created_at', '')
                if ts:
                    return _aware(datetime.fromisoformat(ts.replace('Z', '+00:00')))

            elif alert_type == 'radio_summary':
                ts = item.get('created_at') or item.get('period_start', '')
                if ts:
                    return _aware(datetime.fromisoformat(ts.replace('Z', '+00:00')))

        except (ValueError, TypeError, KeyError) as e:
            logger.debug(f"Could not parse timestamp for {alert_type}: {e}")

        # Fallback to current time
        return datetime.now(timezone.utc)
    
    def _filter_recent_waze(self, items: List[Dict[str, Any]], max_age_minutes: int = 15, max_items: int = 5) -> List[Dict[str, Any]]:
        """Filter Waze items to only include very recent reports.
        
        Waze has thousands of active reports at any time. This filter ensures
        we only alert on truly new reports (last 15 minutes) and limits the
        number per poll cycle to prevent spam.
        
        Args:
            items: List of Waze GeoJSON features
            max_age_minutes: Maximum age in minutes to include (default 15)
            max_items: Maximum items to return per cycle (default 5)
        
        Returns:
            Filtered list of recent items only (limited), sorted oldest-first
        """
        recent_items = []
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
        
        for item in items:
            props = item.get('properties', {})
            created_str = props.get('created', '')
            
            if not created_str:
                # No timestamp - skip to avoid old data
                continue
            
            try:
                # Parse ISO timestamp (2025-01-07T14:30:00Z format)
                created_dt = datetime.fromisoformat(created_str.replace('Z', '+00:00'))
                if created_dt >= cutoff:
                    recent_items.append((created_dt, item))
            except (ValueError, TypeError):
                # Can't parse timestamp - skip
                continue
        
        # Sort by timestamp (oldest first) for chronological posting
        recent_items.sort(key=lambda x: x[0])
        
        # Limit to max_items to prevent flooding, take oldest first
        if len(recent_items) > max_items:
            logger.debug(f"  → Limiting Waze from {len(recent_items)} to {max_items} items")
            recent_items = recent_items[:max_items]
        
        # Extract just the items (discard timestamps used for sorting)
        return [item for _, item in recent_items]
    
    async def check_alerts(self) -> List[Dict[str, Any]]:
        """Check all alert types for new alerts"""
        new_alerts = []

        # Single event-loop handle reused for all run_in_executor calls below,
        # so every sync DB call is offloaded off the gateway heartbeat path.
        loop = asyncio.get_event_loop()

        for alert_type, endpoint in self.endpoints.items():
            # Check if anyone is subscribed to this alert type
            presets = await loop.run_in_executor(
                None, self.db.get_presets_for_alert_type, alert_type
            )
            if not presets:
                logger.debug(f"Skipping {alert_type} - no subscribers")
                continue  # Skip if no one is subscribed

            logger.debug(f"Checking {alert_type} ({len(presets)} subscribers)...")
            data = await self._fetch(endpoint)
            if not data:
                logger.debug(f"  → No data returned for {alert_type}")
                continue

            items = self._extract_items(alert_type, data)
            original_count = len(items)

            # Filter Waze items to only very recent ones (prevents flooding)
            # Waze has thousands of reports - only alert on last 15 min, max 5 per cycle
            if alert_type.startswith('waze_'):
                # On first poll, mark all Waze items as seen without alerting (bootstrap)
                if self._first_poll:
                    logger.info(f"  → {alert_type}: Bootstrapping {original_count} items (marking as seen, no alerts)")
                    # Use batch operation in executor to avoid blocking the event loop
                    batch = [(alert_type, self._get_alert_id(alert_type, item)) for item in items]
                    await loop.run_in_executor(None, self.db.mark_alerts_seen_batch, batch)
                    items = []  # Don't alert on any
                else:
                    items = self._filter_recent_waze(items, max_age_minutes=15, max_items=5)
                    logger.debug(f"  → {alert_type}: {original_count} total, {len(items)} recent (last 15min, max 5)")
            else:
                logger.debug(f"  → {alert_type}: {len(items)} items")

            # Build candidate list and check all at once (single DB query instead of N)
            candidates = [(alert_type, self._get_alert_id(alert_type, item)) for item in items]
            item_by_id = {self._get_alert_id(alert_type, item): item for item in items}

            # Run synchronous DB calls in executor to avoid blocking the event loop
            unseen = await loop.run_in_executor(None, self.db.filter_unseen_alerts, candidates)

            if unseen:
                # Mark all unseen as seen in one batch commit
                await loop.run_in_executor(None, self.db.mark_alerts_seen_batch, unseen)

                for _, alert_id in unseen:
                    item = item_by_id[alert_id]
                    source_ts = self._get_alert_timestamp(alert_type, item)
                    alert = {
                        'type': alert_type,
                        'id': alert_id,
                        'data': item,
                        'timestamp': source_ts.isoformat()
                    }
                    new_alerts.append(alert)
                    logger.info(f"New alert: {alert_type} - {alert_id}")

            if not unseen:
                logger.debug(f"  → No new {alert_type} alerts")
        
        # Check user incidents from Supabase
        user_incident_alerts = await self._check_user_incidents()
        if user_incident_alerts:
            logger.info(f"Found {len(user_incident_alerts)} new user incidents")
        new_alerts.extend(user_incident_alerts)

        # Check for new rdio-scanner hourly summaries
        radio_summary_alerts = await self._check_radio_summary()
        if radio_summary_alerts:
            logger.info(f"Found {len(radio_summary_alerts)} new radio summaries")
        new_alerts.extend(radio_summary_alerts)

        # R2 fix: mark radio_summary alerts as seen only AFTER they have been
        # appended to new_alerts. This mirrors the filter_unseen / mark_seen
        # batching used above and narrows the window in which a crash could
        # lose a summary (previously mark-seen happened inside
        # _check_radio_summary before the alert was even returned).
        if radio_summary_alerts:
            for rs_alert in radio_summary_alerts:
                await loop.run_in_executor(
                    None, self.db.mark_alert_seen, 'radio_summary', rs_alert['id']
                )
        
        # Mark first poll as complete
        if self._first_poll:
            self._first_poll = False
            logger.info("First poll cycle complete - Waze alerts now active")
        
        # Sort all alerts by source timestamp (oldest first) for chronological posting
        if new_alerts:
            new_alerts.sort(key=lambda a: self._get_alert_timestamp(a['type'], a['data']))
            logger.debug(f"Sorted {len(new_alerts)} alerts chronologically")
        
        return new_alerts
    
    async def _check_user_incidents(self) -> List[Dict[str, Any]]:
        """Check for new user-submitted incidents from Supabase"""
        new_alerts = []

        # Run synchronous DB calls in executor to avoid blocking the Discord
        # gateway heartbeat under Postgres latency (mirrors _check_radio_summary).
        loop = asyncio.get_event_loop()

        # Check if anyone is subscribed to user_incidents
        presets = await loop.run_in_executor(
            None, self.db.get_presets_for_alert_type, 'user_incidents'
        )
        if not presets:
            return new_alerts

        # Fetch user incidents from backend API
        incidents = await self._fetch_user_incidents()

        for incident in incidents:
            # Skip incidents that don't have minimum required data
            # (prevents posting when marker just created but not filled in)
            if not self._is_incident_ready(incident):
                continue

            alert_id = self._get_alert_id('user_incidents', incident)

            seen = await loop.run_in_executor(
                None, self.db.is_alert_seen, 'user_incidents', alert_id
            )
            if not seen:
                # Mark as seen IMMEDIATELY to prevent duplicates on next poll cycle
                await loop.run_in_executor(
                    None, self.db.mark_alert_seen, 'user_incidents', alert_id
                )

                # Use actual source timestamp for proper ordering
                source_ts = self._get_alert_timestamp('user_incidents', incident)
                alert = {
                    'type': 'user_incidents',
                    'id': alert_id,
                    'data': incident,
                    'timestamp': source_ts.isoformat()
                }
                new_alerts.append(alert)
                logger.info(f"New user incident: {incident.get('title', 'Unknown')}")

        return new_alerts

    async def _check_radio_summary(self) -> List[Dict[str, Any]]:
        """Poll /api/summaries/latest and emit an alert when a new summary id
        appears. The backend releases scheduled hourly rows just before the
        top-of-hour (release_at ~= hour-top - 30s) so this naturally fires
        within one poll tick of a new summary going live.

        NOTE: This function DOES NOT call mark_alert_seen. The mark-seen is
        deferred to check_alerts() after the alert is appended to new_alerts,
        so a crash between the is_alert_seen check and returning here does not
        permanently lose the summary. The is_alert_seen guard stays to avoid
        re-emitting within a single poll; the mark commits only once the
        alert has been handed back to the caller.
        """
        # Skip the heavy bits if nobody's subscribed
        loop = asyncio.get_event_loop()
        presets = await loop.run_in_executor(
            None, self.db.get_presets_for_alert_type, 'radio_summary'
        )
        if not presets:
            return []

        payload = await self._fetch('/api/summaries/latest')
        if not payload:
            return []

        row = payload.get('hourly')
        if not row or not row.get('id'):
            return []

        alert_id = f"radio_summary_{row['id']}"
        seen = await loop.run_in_executor(
            None, self.db.is_alert_seen, 'radio_summary', alert_id
        )
        if seen:
            return []

        # Prefer created_at for chronological ordering alongside other alerts
        created_at = row.get('created_at')
        try:
            source_ts = (
                datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                if created_at else datetime.now(timezone.utc)
            )
        except Exception:
            source_ts = datetime.now(timezone.utc)

        return [{
            'type': 'radio_summary',
            'id': alert_id,
            'data': row,
            'timestamp': source_ts.isoformat(),
        }]

    def _is_incident_ready(self, incident: Dict[str, Any]) -> bool:
        """Check if incident has minimum required data to be posted.
        
        Returns False for incidents that were just created but not filled in yet.
        Requires at least one of:
        - A type selected (not empty)
        - A log entry added
        - A responding agency selected
        """
        # Check if type is set (can be array or string)
        inc_type = incident.get('type')
        has_type = False
        if isinstance(inc_type, list) and len(inc_type) > 0:
            has_type = True
        elif isinstance(inc_type, str) and inc_type.strip():
            has_type = True
        
        # Check if has log entries
        logs = incident.get('logs', [])
        has_logs = len(logs) > 0
        
        # Check if has responding agencies
        agencies = incident.get('responding_agencies', [])
        has_agencies = isinstance(agencies, list) and len(agencies) > 0
        
        # Incident is ready if it has type, logs, or agencies
        return has_type or has_logs or has_agencies
    
    async def _fetch_user_incidents(self) -> List[Dict[str, Any]]:
        """Fetch active user incidents from backend API"""
        url = f"{self.api_base_url}/api/incidents?active=true"
        headers = {
            'Authorization': f"Bearer {self.api_key}",
            'Content-Type': 'application/json'
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        incidents = await response.json()
                        if not isinstance(incidents, list):
                            return []

                        # Filter out RFS stubs client-side
                        incidents = [i for i in incidents if not i.get('is_rfs_stub')]

                        # Fetch incident logs for each incident
                        for incident in incidents:
                            incident_id = incident.get('id')
                            if incident_id:
                                logs = await self._fetch_incident_logs(incident_id)
                                incident['logs'] = logs

                        return incidents
                    else:
                        logger.warning(f"Backend returned {response.status} for user incidents")
                        return []
        except asyncio.TimeoutError:
            logger.error("Timeout fetching user incidents from backend")
            return []
        except Exception as e:
            logger.error(f"Error fetching user incidents: {e}")
            return []

    async def _fetch_incident_logs(self, incident_id: str) -> List[Dict[str, Any]]:
        """Fetch incident logs/updates from backend API"""
        url = f"{self.api_base_url}/api/incidents/{incident_id}/updates"
        headers = {
            'Authorization': f"Bearer {self.api_key}",
            'Content-Type': 'application/json'
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data if isinstance(data, list) else []
                    return []
        except Exception as e:
            logger.debug(f"Error fetching incident logs: {e}")
            return []
    
    def _extract_items(self, alert_type: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract individual items from API response"""
        if alert_type == 'rfs':
            # GeoJSON format
            return data.get('features', [])
        
        elif alert_type == 'bom':
            # BOM warnings format
            return data.get('warnings', [])
        
        elif alert_type.startswith('traffic_'):
            # GeoJSON format
            return data.get('features', [])
        
        elif alert_type == 'power_endeavour':
            # Endeavour format - array of outages
            return data if isinstance(data, list) else []
        
        elif alert_type == 'power_ausgrid':
            # Ausgrid format - {'Markers': [...], 'Polygons': [...]}
            if isinstance(data, list):
                return data
            # API returns PascalCase 'Markers' key
            return data.get('Markers', []) or data.get('markers', []) or []
        
        elif alert_type.startswith('waze_'):
            # Waze alerts - GeoJSON format
            return data.get('features', [])
        
        elif alert_type == 'user_incidents':
            # User incidents from Supabase - already a list
            return data if isinstance(data, list) else []
        
        return []
    
    async def check_pager(self) -> List[Dict[str, Any]]:
        """Check for new pager messages from API"""
        new_messages = []

        # Run sync DB calls in the default executor so a slow Postgres
        # commit can't block the gateway heartbeat.
        loop = asyncio.get_event_loop()

        # Check if anyone has pager subscriptions
        presets = await loop.run_in_executor(None, self.db.get_presets_for_pager)
        if not presets:
            logger.debug("Skipping pager - no subscribers")
            return new_messages

        logger.debug(f"Checking pager ({len(presets)} subscribers)...")

        # Fetch recent pager messages from API
        messages = await self._fetch_pager_from_api()
        logger.debug(f"  → Fetched {len(messages)} pager messages from API")

        for msg in messages:
            # Use pager_msg_id as unique identifier
            pager_msg_id = str(msg.get('pager_msg_id', ''))
            if not pager_msg_id:
                continue

            msg_hash = f"pager_{pager_msg_id}"

            seen = await loop.run_in_executor(None, self.db.is_pager_seen, msg_hash)
            if not seen:
                # Parse and format the message
                # Returns None for test messages that should be filtered
                parsed = self._format_api_pager(msg)
                if parsed:
                    # Mark as seen IMMEDIATELY to prevent duplicates on next poll cycle
                    # (poll runs every 30s, queue processing can take longer due to rate limits)
                    await loop.run_in_executor(None, self.db.mark_pager_seen, msg_hash)
                    parsed['_msg_hash'] = msg_hash
                    new_messages.append(parsed)
                    logger.info(f"New pager message: {parsed.get('capcode', 'UNKNOWN')} - {parsed.get('incident_id', '')} - {parsed.get('type', '')}")
                else:
                    # Mark filtered/test messages as seen immediately (they won't be sent)
                    await loop.run_in_executor(None, self.db.mark_pager_seen, msg_hash)
                    logger.debug(f"Filtered pager message (test/invalid): {pager_msg_id}")

        return new_messages
    
    async def _fetch_pager_from_api(self) -> List[Dict[str, Any]]:
        """Fetch recent pager messages from the NSW PSN API"""
        url = f"{self.api_base_url}/api/pager/hits"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'User-Agent': 'NSWPSNBot/1.0',
            'X-Client-Type': 'discord-bot'
        }
        
        # Get messages from the last 1 hour (for near-realtime alerts)
        params = {
            'hours': '1',
            'limit': '100'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        # API returns GeoJSON FeatureCollection, extract features
                        if isinstance(data, dict) and 'features' in data:
                            # Convert features to flat format for processing
                            messages = []
                            for feature in data.get('features', []):
                                props = feature.get('properties', {})
                                messages.append(props)
                            return messages
                        return []
                    else:
                        logger.warning(f"API returned {response.status} for pager/hits")
                        return []
        except asyncio.TimeoutError:
            logger.error("Timeout fetching pager messages from API")
            return []
        except Exception as e:
            logger.error(f"Error fetching pager from API: {e}")
            return []
    
    def _format_api_pager(self, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Format an API pager_hits response into our standard format"""
        message_text = msg.get('message', '')
        
        # Skip test pages
        if self._is_test_message(message_text):
            return None
        
        # Check if it's a stop message
        is_stop = 'stop message' in message_text.lower()
        
        # Parse type and category from message
        msg_type, category = self._extract_incident_type_and_category(message_text)
        if is_stop:
            msg_type = 'STOP MESSAGE'
            category = 'STOP'
        
        # Extract station code from raw message (e.g., "SNOPS12" from "SNOPS12 - 25-142476 - ...")
        station_code = self._extract_station_code(message_text)
        
        # Parse address details from message
        address_info = self._parse_address_from_message(message_text)
        
        return {
            'raw': message_text,
            'capcode': msg.get('capcode', ''),
            'station_code': station_code,
            'incident_id': msg.get('incident_id', ''),
            'type': msg_type,
            'category': category,
            'description': f"{msg_type} - {category}" if category and category != msg_type else msg_type,
            'alias': msg.get('alias', ''),
            'agency': msg.get('agency', ''),
            'address': address_info.get('address', ''),
            'suburb': address_info.get('suburb', ''),
            'council': address_info.get('council', ''),
            'postcode': address_info.get('postcode', ''),
            'coordinates': {
                'lat': msg.get('lat'),
                'lon': msg.get('lon')
            } if msg.get('lat') and msg.get('lon') else None,
            'timestamp': msg.get('incident_time', ''),
            'pager_msg_id': msg.get('pager_msg_id')
        }
    
    def _is_test_message(self, message: str) -> bool:
        """Check if a message is a test page that should be filtered"""
        if not message:
            return False
        upper = message.upper()
        test_patterns = [
            'DAILY TEST',
            'TEST PAGE',
            'WEEKLY TEST',
            'SUNDAY TEST',
            'DO NOT RESPOND',
            'NO ACTION REQUIRED'
        ]
        return any(pattern in upper for pattern in test_patterns)
    
    def _extract_station_code(self, message: str) -> str:
        """Extract station code from the start of a pager message"""
        if not message:
            return ''
        
        # Remove leading timestamp if present (HH:MM:SS)
        clean_msg = message.strip()
        time_match = re.match(r'^\d{2}:\d{2}:\d{2}\s+', clean_msg)
        if time_match:
            clean_msg = clean_msg[time_match.end():]
        
        # Station code is at the start, before the first " - "
        # Format: SNOPS12, SHBERRI7, ISBULLI1A, etc.
        match = re.match(r'^([A-Z]{2}[A-Z0-9]+)', clean_msg)
        if match:
            return match.group(1)
        
        return ''
    
    def _parse_address_from_message(self, message: str) -> Dict[str, str]:
        """Parse address, suburb, council, postcode from pager message"""
        result = {'address': '', 'suburb': '', 'council': '', 'postcode': ''}
        
        if not message:
            return result
        
        # Remove coordinates from end - handle various formats
        clean_msg = re.sub(r'\s*-?\s*\[[-\d.,\s]+\]\s*$', '', message)
        clean_msg = re.sub(r'\s*\([-\d.,\s]+\)\s*$', '', clean_msg)
        
        # Split by " - " to get parts
        parts = [p.strip() for p in clean_msg.split(' - ') if p.strip()]
        
        # Find the part that looks like an address (contains commas, ends with postcode)
        # Format: ADDRESS,SUBURB,COUNCIL (NSW),POSTCODE
        address_part = None
        
        # Work backwards to find address-like part
        for i in range(len(parts) - 1, -1, -1):
            part = parts[i]
            # Check if it contains comma-separated values with postcode
            if ',' in part:
                # Check for postcode at end
                postcode_match = re.search(r'(\d{4})\s*$', part)
                if postcode_match:
                    address_part = part
                    break
                # Also check if it contains NSW or council indicator
                if '(NSW)' in part.upper() or 'COUNCIL' in part.upper():
                    address_part = part
                    break
        
        # Fallback: use last part if it has commas
        if not address_part and len(parts) >= 1 and ',' in parts[-1]:
            address_part = parts[-1]
        
        if address_part:
            addr_parts = [a.strip() for a in address_part.split(',')]
            
            if len(addr_parts) >= 1:
                result['address'] = addr_parts[0]
            if len(addr_parts) >= 2:
                result['suburb'] = addr_parts[1]
            if len(addr_parts) >= 3:
                # Council might have (NSW) suffix
                council = addr_parts[2]
                result['council'] = council
            if len(addr_parts) >= 4:
                # Extract postcode from last part
                last_part = addr_parts[-1]
                postcode_match = re.search(r'(\d{4})', last_part)
                if postcode_match:
                    result['postcode'] = postcode_match.group(1)
            elif len(addr_parts) >= 3:
                # Postcode might be in the council part
                postcode_match = re.search(r'(\d{4})', addr_parts[-1])
                if postcode_match:
                    result['postcode'] = postcode_match.group(1)
        
        return result
    
    def _extract_incident_type_and_category(self, message: str) -> tuple:
        """Extract incident type and category from pager message
        
        Returns:
            tuple: (type, category) e.g. ('Bush Fire', 'FIRECALL')
        """
        if not message:
            return ('Pager Alert', '')
        
        # Categories (dispatch classifications) - expanded list
        categories = [
            'FIRECALL', 'STRUCTURE FIRE', 'VEHICLE FIRE', 'MVA', 
            'INCIDENT CALL', 'ASSIST AMBOS', 'ASSIST POLICE', 'ASSIST AMBULANCE',
            'HAZMAT', 'RESCUE', 'WATER RESCUE', 'ROAD RESCUE',
            'GRASS AND SCRUB', 'BUSH FIRE', 'GRASS FIRE', 'CAR FIRE',
            'RUBBISH FIRE', 'ILLEGAL BURN', 'SMOKE SIGHTING',
            'ALARM', 'AFA', 'AUTOMATIC FIRE ALARM', 'INVESTIGATION'
        ]
        category_pattern = '|'.join(re.escape(c) for c in categories)
        
        # Pattern 1: " - Type - CATEGORY - " (most common format)
        # e.g., " - Bush Fire - FIRECALL - "
        pattern1 = rf' - ([^-]+?) - ({category_pattern})\s*-'
        match = re.search(pattern1, message, re.IGNORECASE)
        if match:
            return (match.group(1).strip(), match.group(2).strip().upper())
        
        # Pattern 2: Category at end without trailing dash
        pattern2 = rf' - ([^-]+?) - ({category_pattern})\s*$'
        match = re.search(pattern2, message, re.IGNORECASE)
        if match:
            return (match.group(1).strip(), match.group(2).strip().upper())
        
        # Pattern 3: Just look for category anywhere after station code
        # Format: STATIONCODE - ID - Type - CATEGORY - address
        pattern3 = rf'^\w+ - [\d-]+ - ([^-]+?) - ({category_pattern})'
        match = re.search(pattern3, message, re.IGNORECASE)
        if match:
            return (match.group(1).strip(), match.group(2).strip().upper())
        
        # Pattern for known incident types (case insensitive)
        known_types = [
            'Car fire', 'Bush Fire', 'Grass', 'Grass Fire', 'Tree alight', 
            'Tree down', 'Tree down on power lines', 'Tree on fire',
            'MVA', 'MVA persons trapped', 'Motor vehicle accident',
            'AFA', 'Explosion', 'Structure/building/house fire', 'Shed fire',
            'Backyard fire', 'Assist Ambulance', 'Assist Police', 'Assist public',
            'Electrical appliance fire', 'Tip/rubbish fire', 'Unknown fire',
            'House fire', 'Building fire', 'Smoke in area', 'Smoke investigation',
            'Hazmat', 'Rescue', 'Water rescue', 'Search and rescue',
            'LPG leak', 'Gas leak', 'Fuel spill', 'Power lines down',
            'Illegal burn', 'Rubbish fire', 'Pile burn', 'Controlled burn',
            'Alarm', 'Automatic fire alarm', 'Investigation'
        ]
        
        for known_type in known_types:
            # Look for " - known_type - " pattern
            pattern = rf' - ({re.escape(known_type)}) - '
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                # Try to find category after
                category_match = re.search(rf'{re.escape(known_type)}\s*-\s*({category_pattern})', message, re.IGNORECASE)
                category = category_match.group(1).upper() if category_match else ''
                return (match.group(1).strip(), category)
        
        # Fallback: look for category alone and use it
        category_match = re.search(rf' - ({category_pattern}) - ', message, re.IGNORECASE)
        if category_match:
            return (category_match.group(1).strip(), category_match.group(1).strip().upper())
        
        # Last fallback: check for category without dashes
        category_match = re.search(rf'\b({category_pattern})\b', message, re.IGNORECASE)
        if category_match:
            return (category_match.group(1).strip(), category_match.group(1).strip().upper())
        
        return ('Pager Alert', '')
    
    def _hash_pager_message(self, message: str) -> str:
        """Create a hash for a pager message to detect duplicates"""
        if isinstance(message, dict):
            message = str(message)
        return hashlib.md5(message.encode()).hexdigest()

