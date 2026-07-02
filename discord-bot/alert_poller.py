"""
Alert Poller - Fetches alerts from the NSW PSN API and tracks new incidents.
"""

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

# Freshness window for Waze alerts. Anything whose upstream `created`
# timestamp is older than this is treated as stale backlog and NOT posted
# as "new" — this is what stops a region's week-old flood closures from
# flooding in when the scrape bbox shifts. Jams carry no pubMillis so they
# are undated → treated as "now" → always pass. Tune to taste: lower = only
# very fresh alerts; higher = lets older-but-active roadwork/closures through.
WAZE_MAX_AGE_MINUTES = 360  # 6 hours

# Police dedup window. Waze police are crowd-sourced and churn uuids: the
# same speed trap is re-reported under many distinct ids within minutes, so
# keying on the uuid posts the same spot repeatedly. Instead we key police on
# subtype + a coarse location grid + a time bucket of this length, so repeat
# reports of one spot within the window collapse to a single alert while the
# spot can re-alert in a later window if police are reported there again.
WAZE_POLICE_DEDUP_MINUTES = 60
# Location grid for police dedup, in degrees. ~50m (1° lat ≈ 111.3km, so
# 50m ≈ 0.00045°). Reports snapped to the same cell are treated as the same
# spot.
WAZE_POLICE_GRID_DEG = 0.00045


class AlertPoller:
    def __init__(self, api_base_url: str, api_key: str, database):
        self.api_base_url = api_base_url.rstrip('/')
        self.api_key = api_key
        self.db = database
        
        # API endpoints mapping.
        # NOTE: Some canonical alert_types share a single backend fetch and are
        # split out by `_extract_items` / dispatcher inspection:
        #   - 'bom_land' uses /api/bom/warnings (the warnings response is split
        #     into bom_land / bom_marine by category at extraction time).
        #   - 'endeavour_planned' uses /api/endeavour/planned (current outages
        #     come back as 'endeavour_current').
        #   - 'essential_planned'/'essential_future' point at the Essential
        #     Energy backend endpoints (wired so they're ready when the proxy
        #     exposes them; safe to call before the routes exist — _fetch
        #     returns None on non-200).
        #   - 'waze_jam' shares the hazards feed (waze backend returns hazards
        #     and jams together; we route per-feature in _extract_items).
        #   - 'firms' returns thousands of satellite fire pixels; _extract_items
        #     collapses them into ~100m clusters per pass (see _cluster_firms).
        self.endpoints = {
            'rfs': '/api/rfs/incidents',
            'firms': '/api/firms/hotspots',
            'bom_land': '/api/bom/warnings',
            'bom_marine': '/api/bom/warnings',
            'traffic_incident': '/api/traffic/incidents',
            'traffic_roadwork': '/api/traffic/roadwork',
            'traffic_flood': '/api/traffic/flood',
            'traffic_fire': '/api/traffic/fire',
            'traffic_majorevent': '/api/traffic/majorevent',
            'endeavour_current': '/api/endeavour/current',
            'endeavour_planned': '/api/endeavour/planned',
            'ausgrid': '/api/ausgrid/outages',
            'essential_planned': '/api/essential/planned',
            'essential_future': '/api/essential/future',
            'waze_hazard': '/api/waze/hazards',
            'waze_jam': '/api/waze/hazards',
            'waze_police': '/api/waze/police',
            'waze_roadwork': '/api/waze/roadwork',
        }
        
        # Track last seen pager message ID
        self.last_pager_id = 0

        # Per-source bootstrap tracking. A source is bootstrapped (all its
        # currently-active items marked seen, no alerts fired) on its first
        # SUCCESSFUL fetch after startup. This used to be one global
        # _first_poll flag flipped at the end of cycle 1 — but a source
        # whose first fetch failed (backend still booting after a shared
        # host restart, or a 30s timeout) was never bootstrapped, and on
        # cycle 2 every active item for it fired as "new": exactly the
        # restart flood the bootstrap exists to prevent.
        self._bootstrapped: set = set()

        # Shared HTTP session — created lazily on first use so it binds to
        # the running event loop. ~18 endpoints are fetched per 60s cycle;
        # reusing one session keeps TCP/TLS connections alive instead of
        # paying a fresh handshake per request. Closed via close().
        self._session: Optional[aiohttp.ClientSession] = None

    def _get_session(self) -> aiohttp.ClientSession:
        """Return the shared aiohttp session, (re)creating it if absent or
        closed. Must be called from within the running event loop."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """Close the shared HTTP session. Call on bot shutdown so aiohttp
        doesn't warn about an unclosed session / leaked connector."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _fetch(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """Fetch data from an API endpoint"""
        url = f"{self.api_base_url}{endpoint}"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'User-Agent': 'NSWPSNBot/1.0',
            'X-Client-Type': 'discord-bot'
        }

        try:
            session = self._get_session()
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
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

        elif alert_type == 'firms':
            # One alert per ~100m cluster per satellite pass. cluster_glat/glon
            # are the snapped grid cell (set in _cluster_firms); acq_date+
            # acq_time identify the pass, so a later pass over the same cell
            # produces a new id and re-alerts (once per new pass).
            props = item.get('properties', {})
            glat = props.get('cluster_glat')
            glon = props.get('cluster_glon')
            sat = props.get('satellite_tag') or props.get('satellite') or ''
            acq = f"{props.get('acq_date', '')}{props.get('acq_time', '')}"
            return f"firms_{glat}_{glon}_{sat}_{acq}"

        elif alert_type.startswith('bom_'):
            # Use title + issued date (shared shape between bom_land/bom_marine)
            return f"{alert_type}_{item.get('title', '')}_{item.get('issued', '')}"

        elif alert_type.startswith('traffic_'):
            # Use the ID from properties
            props = item.get('properties', {})
            return str(props.get('id', '')) or hashlib.md5(str(item).encode(), usedforsecurity=False).hexdigest()[:16]

        elif alert_type.startswith('endeavour_'):
            # Use incident ID + suburb + streets to make unique
            # (same incident can have multiple street entries per suburb).
            # Same key shape for endeavour_current and endeavour_planned.
            incident_id = str(item.get('id', '')) or str(item.get('incidentId', ''))
            suburb = item.get('suburb', '') or item.get('location', '')
            streets = item.get('streets', '') or item.get('streetName', '')
            # Create hash of streets to keep ID shorter
            street_hash = hashlib.md5(str(streets).encode(), usedforsecurity=False).hexdigest()[:8] if streets else ''
            prefix = alert_type  # endeavour_current / endeavour_planned
            return f"{prefix}_{incident_id}_{suburb}_{street_hash}" if suburb else f"{prefix}_{incident_id}"

        elif alert_type == 'ausgrid':
            # Ausgrid outages - use OutageId or combine Suburb+StreetName
            outage_id = item.get('OutageId') or item.get('outageId', '')
            if outage_id:
                return f"ausgrid_{outage_id}"
            suburb = item.get('Suburb', '') or item.get('suburb', '')
            street = item.get('StreetName', '') or item.get('streetName', '')
            return f"ausgrid_{suburb}_{street}" if suburb else hashlib.md5(str(item).encode(), usedforsecurity=False).hexdigest()[:16]

        elif alert_type.startswith('essential_'):
            # Essential Energy outages — best-effort key off id-ish fields
            outage_id = (item.get('id') or item.get('outageId')
                         or item.get('OutageId') or item.get('reference'))
            if outage_id:
                return f"{alert_type}_{outage_id}"
            suburb = item.get('suburb') or item.get('Suburb') or ''
            street = item.get('street') or item.get('streetName') or item.get('StreetName') or ''
            if suburb or street:
                return f"{alert_type}_{suburb}_{street}"
            return hashlib.md5(str(item).encode(), usedforsecurity=False).hexdigest()[:16]

        elif alert_type.startswith('waze_'):
            # Waze alerts - use the Waze UUID from properties when present.
            props = item.get('properties', {})

            if alert_type == 'waze_police':
                # Police churn uuids — the same speed trap is re-reported
                # under many distinct ids within minutes, so uuid-keyed dedup
                # posts the same spot repeatedly. Key on subtype + a coarse
                # ~50m grid + a time bucket so repeat reports of one spot
                # collapse to one alert. The bucket is taken from the report
                # time (fallback: now) so the SAME report doesn't re-alert
                # poll-to-poll, but a genuinely new report in a later window
                # at the same spot can. Unlike road closures (linear, many
                # distinct segments) police are point sightings, so coarse
                # gridding is safe here and applied to police only.
                geom = (item.get('geometry') or {}).get('coordinates') or []
                pt = geom[0] if (geom and isinstance(geom[0], (list, tuple))) else geom
                try:
                    glat = round(float(pt[1]) / WAZE_POLICE_GRID_DEG)
                    glon = round(float(pt[0]) / WAZE_POLICE_GRID_DEG)
                    loc = f"{glat},{glon}"
                except (TypeError, ValueError, IndexError):
                    loc = str(props.get('street', ''))
                created = props.get('created', '')
                bucket_dt = None
                if created:
                    try:
                        bucket_dt = datetime.fromisoformat(str(created).replace('Z', '+00:00'))
                        if bucket_dt.tzinfo is None:
                            bucket_dt = bucket_dt.replace(tzinfo=timezone.utc)
                    except (ValueError, TypeError):
                        bucket_dt = None
                if bucket_dt is None:
                    bucket_dt = datetime.now(timezone.utc)
                bucket = int(bucket_dt.timestamp() // (WAZE_POLICE_DEDUP_MINUTES * 60))
                stable = '|'.join([str(props.get('wazeSubtype', '')), loc, str(bucket)])
                return "waze_police_" + hashlib.md5(stable.encode(), usedforsecurity=False).hexdigest()[:16]

            waze_id = props.get('id', '')
            if waze_id:
                return f"{alert_type}_{waze_id}"
            # No Waze id — derive a STABLE key from fields that don't change
            # poll-to-poll. The old md5(str(item)) hashed the WHOLE feature,
            # whose volatile fields (speed/level/length/thumbs/coord
            # precision) changed every cycle, so such an alert got a new id
            # each poll: it was re-sent (duplicates) and slipped past the
            # bootstrap-seen mark (marked under a now-stale id) -> persistent
            # "old" alerts kept firing once the per-cycle cap was removed.
            geom = (item.get('geometry') or {}).get('coordinates') or []
            pt = geom[0] if (geom and isinstance(geom[0], (list, tuple))) else geom
            try:
                loc = f"{round(float(pt[1]), 4)},{round(float(pt[0]), 4)}"
            except (TypeError, ValueError, IndexError):
                loc = ''
            stable = '|'.join([
                str(props.get('wazeType', '')),
                str(props.get('wazeSubtype', '')),
                str(props.get('street', '')),
                loc,
            ])
            return f"{alert_type}_" + hashlib.md5(stable.encode(), usedforsecurity=False).hexdigest()[:16]

        elif alert_type == 'user_incident':
            # User incidents from Supabase - use incident ID + latest log ID for update tracking
            incident_id = item.get('id', '')
            logs = item.get('logs', [])
            # Backend log order isn't guaranteed (embeds.py re-sorts for
            # display), so pick the newest by created_at/id rather than
            # trusting logs[0] — otherwise updates can fail to re-fire.
            dict_logs = [row for row in logs if isinstance(row, dict)]
            latest_log = max(
                dict_logs,
                key=lambda row: (str(row.get('created_at', '')), str(row.get('id', ''))),
                default=None,
            ) if dict_logs else None
            latest_log_id = latest_log.get('id', '') if latest_log else ''
            return f"user_{incident_id}_{latest_log_id}"
        
        # Fallback to hash
        return hashlib.md5(str(item).encode(), usedforsecurity=False).hexdigest()[:16]
    
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

            elif alert_type == 'firms':
                # acq_datetime is the satellite acquisition time (ISO UTC).
                ts = item.get('properties', {}).get('acq_datetime', '')
                if ts:
                    return _aware(datetime.fromisoformat(ts.replace('Z', '+00:00')))

            elif alert_type.startswith('bom_'):
                # The backend's BomWarning shape carries `issued` — ISO-ish
                # local time for land warnings, RFC 2822 (mapped from the
                # feed's pubDate) for marine. The old top-level `pubDate`
                # read never matched the API payload, so every BOM alert
                # fell back to "now" and sorted arbitrarily. Try ISO first,
                # then RFC 2822; keep pubDate as a legacy fallback.
                ts = item.get('issued') or item.get('pubDate') or ''
                if ts:
                    try:
                        return _aware(datetime.fromisoformat(str(ts).replace('Z', '+00:00')))
                    except ValueError:
                        from email.utils import parsedate_to_datetime
                        try:
                            return _aware(parsedate_to_datetime(str(ts)))
                        except Exception:
                            pass

            elif alert_type.startswith('traffic_'):
                # The backend GeoJSON carries properties.created (ISO); the
                # old top-level `Created` read never matched, so traffic
                # alerts always sorted as "now". Keep Created as a legacy
                # fallback for raw-feed payload shapes.
                created = item.get('properties', {}).get('created') or item.get('Created')
                if created:
                    if isinstance(created, (int, float)):
                        # Convert ms to seconds if needed
                        if created > 1e12:
                            return datetime.fromtimestamp(created / 1000, tz=timezone.utc)
                        return datetime.fromtimestamp(created, tz=timezone.utc)
                    # Created can also arrive as a string — try numeric epoch
                    # first, then ISO, before falling through to "now".
                    if isinstance(created, str):
                        s = created.strip()
                        try:
                            num = float(s)
                            if num > 1e12:
                                return datetime.fromtimestamp(num / 1000, tz=timezone.utc)
                            return datetime.fromtimestamp(num, tz=timezone.utc)
                        except ValueError:
                            try:
                                return _aware(datetime.fromisoformat(s.replace('Z', '+00:00')))
                            except ValueError:
                                pass

            elif alert_type.startswith('waze_'):
                # Waze uses properties.created (ISO format)
                props = item.get('properties', {})
                ts = props.get('created', '')
                if ts:
                    return _aware(datetime.fromisoformat(ts.replace('Z', '+00:00')))

            elif alert_type.startswith('endeavour_'):
                # The backend emits startTime + estimatedRestoration (the
                # old `estimatedRestoreTime` field name never matched, so
                # endeavour alerts always sorted as "now"). startTime =
                # when the outage began — the right chronological key;
                # restoration is a future estimate, kept only as fallback.
                ts = item.get('startTime') or item.get('estimatedRestoration') or ''
                if ts:
                    return _aware(datetime.fromisoformat(str(ts).replace('Z', '+00:00')))

            elif alert_type == 'ausgrid':
                # Ausgrid uses StartTime (ISO format)
                start_time = item.get('StartTime') or item.get('startTime', '')
                if start_time:
                    return _aware(datetime.fromisoformat(start_time.replace('Z', '+00:00')))

            elif alert_type.startswith('essential_'):
                # Essential Energy — try common ISO fields
                ts = (item.get('startTime') or item.get('StartTime')
                      or item.get('start_time') or item.get('plannedStart')
                      or item.get('scheduledStart') or '')
                if ts:
                    try:
                        return _aware(datetime.fromisoformat(str(ts).replace('Z', '+00:00')))
                    except Exception:
                        pass

            elif alert_type == 'user_incident':
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
    
    def _filter_recent_waze(self, items: List[Dict[str, Any]], max_age_minutes: int = 15, max_items: int = 0) -> List[Dict[str, Any]]:
        """Order Waze items for posting (and optionally bound them).

        Novelty is handled by seen-dedup + the first-poll bootstrap, so by
        default this emits EVERY item, just sorted oldest-first for
        chronological posting (and treating undated items — e.g. jams with
        no pubMillis — as "now" so they aren't lost).

        Args:
            items: List of Waze GeoJSON features
            max_age_minutes: Drop items whose `created` is older than this
                (WAZE_MAX_AGE_MINUTES). Undated items (jams have no pubMillis)
                are treated as "now" so they always pass.
            max_items: Optional per-cycle cap. 0 (default) = no cap, emit
                everything. When >0, keep the NEWEST max_items so new alerts
                aren't starved by stale ones.

        Returns:
            Items sorted oldest-first.
        """
        recent_items = []
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=max_age_minutes)

        for item in items:
            props = item.get('properties', {}) or {}
            created_str = props.get('created', '')
            created_dt = None
            if created_str:
                try:
                    # Parse ISO timestamp (2026-01-07T14:30:00Z format).
                    created_dt = datetime.fromisoformat(str(created_str).replace('Z', '+00:00'))
                    if created_dt.tzinfo is None:
                        created_dt = created_dt.replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    created_dt = None

            if created_dt is None:
                # No usable timestamp. Waze JAMS routinely lack pubMillis,
                # so `created` is empty — the old "skip if no timestamp"
                # rule silently dropped every jam (and any roadwork without
                # a date). Don't drop them: treat as "now" so they still
                # post. The first-poll bootstrap + seen-dedup prevent
                # floods, so an undated alert still only fires once.
                recent_items.append((now, item))
            elif created_dt >= cutoff:
                recent_items.append((created_dt, item))
            # else: older than the age window — drop (hazard/police path).

        # Optional per-cycle cap. 0 = emit everything (default). When set,
        # keep the NEWEST max_items so new alerts aren't starved by stale
        # ones (the cap runs before the unseen check).
        if max_items and len(recent_items) > max_items:
            logger.debug(f"  → Limiting Waze from {len(recent_items)} to newest {max_items}")
            recent_items.sort(key=lambda x: x[0], reverse=True)
            recent_items = recent_items[:max_items]
        recent_items.sort(key=lambda x: x[0])  # oldest-first for posting

        return [item for _, item in recent_items]

    def _cluster_firms(self, features: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Collapse FIRMS pixel detections into ~100m clusters per satellite pass.

        A single satellite pass over an active fire lights up many adjacent
        375m pixels; alerting on each would flood channels. We snap every
        detection to a ~100m grid (3 decimal places ≈ 110m) and group by
        (grid cell, satellite, acquisition time) so all pixels from one pass
        over one spot collapse to a single alert. The representative pixel is
        the one with the highest fire-radiative-power (FRP); cluster_count and
        cluster_max_frp summarise the rest. A later pass over the same cell has
        a different acquisition time, so it re-alerts (once per new pass).
        """
        GRID = 3  # decimal places ≈ 110m
        clusters: Dict[tuple, Dict[str, Any]] = {}
        for f in features:
            props = f.get('properties') or {}
            lat = props.get('latitude')
            lon = props.get('longitude')
            if lat is None or lon is None:
                continue
            try:
                glat = round(float(lat), GRID)
                glon = round(float(lon), GRID)
            except (TypeError, ValueError):
                continue
            sat = props.get('satellite_tag') or props.get('satellite') or ''
            key = (glat, glon, sat,
                   props.get('acq_date', ''), props.get('acq_time', ''))
            try:
                frp = float(props.get('frp') or 0)
            except (TypeError, ValueError):
                frp = 0.0
            c = clusters.get(key)
            if c is None:
                clusters[key] = {'glat': glat, 'glon': glon,
                                 'rep': f, 'count': 1, 'max_frp': frp}
            else:
                c['count'] += 1
                if frp > c['max_frp']:
                    c['max_frp'] = frp
                    c['rep'] = f  # keep the most intense pixel as representative

        out: List[Dict[str, Any]] = []
        for c in clusters.values():
            rep = dict(c['rep'])
            props = dict(rep.get('properties') or {})
            props['cluster_count'] = c['count']
            props['cluster_max_frp'] = c['max_frp']
            props['cluster_glat'] = c['glat']
            props['cluster_glon'] = c['glon']
            rep['properties'] = props
            out.append(rep)
        return out

    async def check_alerts(self) -> List[Dict[str, Any]]:
        """Check all alert types for new alerts"""
        new_alerts = []

        # Single event-loop handle reused for all run_in_executor calls below,
        # so every sync DB call is offloaded off the gateway heartbeat path.
        loop = asyncio.get_event_loop()

        # Fetch all subscribed endpoints CONCURRENTLY. This loop used to fetch
        # each of the ~18 endpoints sequentially, every one with a 30s
        # timeout — so a single slow or unreachable source could stall the
        # whole cycle for minutes and delay EVERY source's alerts (and, with
        # @tasks.loop, push back the next cycle too). Now we first resolve
        # which alert types have subscribers (cheap local DB), then fire all
        # their HTTP fetches at once, so a cycle is bounded by the single
        # slowest fetch instead of the sum of all of them.
        subscribed = []  # [(alert_type, endpoint)]
        for alert_type, endpoint in self.endpoints.items():
            presets = await loop.run_in_executor(
                None, self.db.get_presets_for_alert_type, alert_type
            )
            if not presets:
                logger.debug(f"Skipping {alert_type} - no subscribers")
                continue  # Skip if no one is subscribed
            subscribed.append((alert_type, endpoint))

        fetched = await asyncio.gather(
            *[self._fetch(ep) for (_, ep) in subscribed],
            return_exceptions=True,
        )

        for idx, (alert_type, _endpoint) in enumerate(subscribed):
            data = fetched[idx]
            # _fetch swallows its own errors and returns None, but gather can
            # still surface an unexpected exception — treat it like no data.
            if isinstance(data, Exception):
                logger.warning(f"Fetch error for {alert_type}: {data}")
                continue
            if not data:
                logger.debug(f"  → No data returned for {alert_type}")
                continue

            items = self._extract_items(alert_type, data)
            original_count = len(items)

            # On a source's first SUCCESSFUL fetch after startup, bootstrap
            # it: mark every currently-active item as seen without firing
            # an alert. Without this, a restart after downtime surfaces
            # every active outage / incident / pager hit as "new" — the
            # bot then tries to send thousands of messages at once,
            # saturates the 500-message queue, and drops a bunch of them
            # as "Message queue full". Production saw 2126 alerts on a
            # single restart cycle.
            #
            # Per-source (not a global first-cycle flag) so a source whose
            # first fetch failed still gets bootstrapped when it recovers,
            # instead of flooding on cycle 2.
            if alert_type not in self._bootstrapped:
                self._bootstrapped.add(alert_type)
                logger.info(
                    f"  → {alert_type}: Bootstrapping {original_count} items "
                    "(marking as seen, no alerts)"
                )
                batch = [(alert_type, self._get_alert_id(alert_type, item)) for item in items]
                await loop.run_in_executor(None, self.db.mark_alerts_seen_batch, batch)
                items = []
            elif alert_type.startswith('waze_'):
                # All Waze types: NO per-cycle cap (emit everything), but DO
                # drop anything older than the freshness window. A region's
                # stale backlog (e.g. week-old flood road-closures) enters the
                # feed when the scrape bbox shifts; without this they'd post as
                # "new". Jams have no pubMillis → undated → treated as "now" →
                # always pass. The first-poll bootstrap + seen-dedup still
                # prevent restart floods and repeats.
                items = self._filter_recent_waze(items, max_age_minutes=WAZE_MAX_AGE_MINUTES)
                logger.debug(f"  → {alert_type}: {original_count} total, {len(items)} within freshness window")
            else:
                logger.debug(f"  → {alert_type}: {len(items)} items")

            # Build candidate list and check all at once (single DB query
            # instead of N). De-dupe by id WITHIN this cycle: two distinct
            # items can resolve to the same alert id (the stable-fields
            # fallback for Waze alerts with no upstream uuid collides when
            # type/subtype/street/rounded-location match — e.g. several flood
            # road-closures on the same street). Without de-duping,
            # filter_unseen_alerts returns that id once per copy and we post
            # the identical alert several times in one batch.
            item_by_id = {}
            candidates = []
            for item in items:
                aid = self._get_alert_id(alert_type, item)
                if aid in item_by_id:
                    continue
                item_by_id[aid] = item
                candidates.append((alert_type, aid))

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
            None, self.db.get_presets_for_alert_type, 'user_incident'
        )
        if not presets:
            return new_alerts

        # Fetch user incidents from backend API. None = fetch FAILED (as
        # opposed to a successful fetch with zero incidents) — skip the
        # cycle without consuming the bootstrap, so a backend that's still
        # booting doesn't cause a cycle-2 flood.
        incidents = await self._fetch_user_incidents()
        if incidents is None:
            return new_alerts

        # Bootstrap on the first successful fetch — mark currently-ready
        # incidents as seen without firing alerts. Same anti-flood
        # reasoning as check_alerts: restart after downtime would
        # otherwise re-emit every active user incident as new.
        if 'user_incident' not in self._bootstrapped:
            self._bootstrapped.add('user_incident')
            ready = [i for i in incidents if self._is_incident_ready(i)]
            for incident in ready:
                alert_id = self._get_alert_id('user_incident', incident)
                await loop.run_in_executor(
                    None, self.db.mark_alert_seen, 'user_incident', alert_id
                )
            if ready:
                logger.info(
                    f"  → user_incident: Bootstrapping {len(ready)} items "
                    "(marking as seen, no alerts)"
                )
            return new_alerts

        pending_seen = []
        for incident in incidents:
            # Skip incidents that don't have minimum required data
            # (prevents posting when marker just created but not filled in)
            if not self._is_incident_ready(incident):
                continue

            alert_id = self._get_alert_id('user_incident', incident)

            seen = await loop.run_in_executor(
                None, self.db.is_alert_seen, 'user_incident', alert_id
            )
            if not seen:
                # Use actual source timestamp for proper ordering
                source_ts = self._get_alert_timestamp('user_incident', incident)
                alert = {
                    'type': 'user_incident',
                    'id': alert_id,
                    'data': incident,
                    'timestamp': source_ts.isoformat()
                }
                new_alerts.append(alert)
                pending_seen.append(alert_id)
                logger.info(f"New user incident: {incident.get('title', 'Unknown')}")

        # Defer mark-seen until AFTER the alerts are appended (mirrors the
        # radio_summary path). Cross-cycle dedup is preserved because this
        # still completes before the function returns, but the window in
        # which a crash could drop an alert shrinks to these milliseconds.
        for alert_id in pending_seen:
            await loop.run_in_executor(
                None, self.db.mark_alert_seen, 'user_incident', alert_id
            )

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

        # Consume the bootstrap on the first SUCCESSFUL fetch — checked
        # BEFORE the seen guard, so a restart whose inherited summary is
        # already seen doesn't leave the bootstrap pending to swallow the
        # next genuinely-new summary.
        first_success = 'radio_summary' not in self._bootstrapped
        self._bootstrapped.add('radio_summary')

        alert_id = f"radio_summary_{row['id']}"
        seen = await loop.run_in_executor(
            None, self.db.is_alert_seen, 'radio_summary', alert_id
        )
        if seen:
            return []

        # Bootstrap — the latest summary the bot inherits at startup may
        # already be hours old; firing it as "new" once the bot reconnects
        # is misleading (and noisy if downtime spanned multiple summary
        # boundaries; a quiet hour summary then a backlog of new alerts is
        # the typical stack).
        if first_success:
            await loop.run_in_executor(
                None, self.db.mark_alert_seen, 'radio_summary', alert_id
            )
            logger.info(
                f"  → radio_summary: Bootstrapping {alert_id} (marking as seen, no alert)"
            )
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
    
    async def _fetch_user_incidents(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch active user incidents from backend API.

        Returns None on a FAILED fetch (HTTP error / timeout / exception)
        so the caller can distinguish "backend unreachable" from "no
        incidents" — the bootstrap must only be consumed by a successful
        fetch, and a malformed-but-200 payload counts as failure too.
        """
        url = f"{self.api_base_url}/api/incidents?active=true"
        headers = {
            'Authorization': f"Bearer {self.api_key}",
            'Content-Type': 'application/json'
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        incidents = await response.json()
                        if not isinstance(incidents, list):
                            return None

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
                        return None
        except asyncio.TimeoutError:
            logger.error("Timeout fetching user incidents from backend")
            return None
        except Exception as e:
            logger.error(f"Error fetching user incidents: {e}")
            return None

    async def _fetch_incident_logs(self, incident_id: str) -> List[Dict[str, Any]]:
        """Fetch incident logs/updates from backend API"""
        url = f"{self.api_base_url}/api/incidents/{incident_id}/updates"
        headers = {
            'Authorization': f"Bearer {self.api_key}",
            'Content-Type': 'application/json'
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data if isinstance(data, list) else []
                    return []
        except Exception as e:
            logger.debug(f"Error fetching incident logs: {e}")
            return []
    
    def _extract_items(self, alert_type: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract individual items from API response.

        Some endpoints return a mixed feed that the dispatcher splits across
        canonical alert_types — handled inline here by inspecting each item:
          - bom_land vs bom_marine: single /api/bom/warnings response, split
            on `category` ('land' | 'marine').
          - waze_hazard vs waze_jam: single /api/waze/hazards response, split
            on the `wazeType` / `displayType` of each feature ('JAM' → jam,
            anything else → hazard).
        """
        if alert_type == 'rfs':
            # GeoJSON format
            return data.get('features', [])

        elif alert_type == 'firms':
            # NASA FIRMS hotspots — GeoJSON FeatureCollection of satellite fire
            # pixels. Cluster to ~100m so one fire = one alert (see
            # _cluster_firms); each returned item is a cluster representative.
            return self._cluster_firms(data.get('features', []) or [])

        elif alert_type == 'bom_land':
            # Single BOM endpoint returns both land + marine; route per-item.
            warnings = data.get('warnings', []) or []
            return [w for w in warnings
                    if str(w.get('category', '')).lower() != 'marine']

        elif alert_type == 'bom_marine':
            warnings = data.get('warnings', []) or []
            return [w for w in warnings
                    if str(w.get('category', '')).lower() == 'marine']

        elif alert_type.startswith('traffic_'):
            # GeoJSON format
            return data.get('features', [])

        elif alert_type.startswith('endeavour_'):
            # Endeavour format - array of outages (current + planned share shape)
            return data if isinstance(data, list) else []

        elif alert_type == 'ausgrid':
            # Ausgrid format - {'Markers': [...], 'Polygons': [...]}
            if isinstance(data, list):
                return data
            # API returns PascalCase 'Markers' key
            return data.get('Markers', []) or data.get('markers', []) or []

        elif alert_type.startswith('essential_'):
            # Essential Energy — accept either bare list or dict with common keys
            if isinstance(data, list):
                return data
            for key in ('outages', 'Outages', 'results', 'data',
                        'plannedOutages', 'futureOutages'):
                v = data.get(key) if isinstance(data, dict) else None
                if isinstance(v, list):
                    return v
            return []

        elif alert_type == 'waze_hazard':
            # Hazards feed mixes hazards + jams; jams handled separately below.
            features = data.get('features', []) or []
            out = []
            for f in features:
                props = f.get('properties') or {}
                wtype = (props.get('wazeType')
                         or props.get('displayType')
                         or props.get('type') or '').upper()
                if 'JAM' in wtype:
                    continue
                out.append(f)
            return out

        elif alert_type == 'waze_jam':
            # The /api/waze/hazards response carries jam polylines under a
            # separate `jams` key (parseWazeJam features); `features` holds
            # only hazards since the 2026-05-28 JAM→waze_jam ingest split.
            # Reading `features` here returned nothing, so jam alerts never
            # fired — pull from `jams` instead.
            out = list(data.get('jams', []) or [])
            # Defensive: also catch any JAM-typed alert that lands in
            # `features` (pre-split / edge data).
            for f in (data.get('features', []) or []):
                props = f.get('properties') or {}
                wtype = (props.get('wazeType')
                         or props.get('displayType')
                         or props.get('type') or '').upper()
                if 'JAM' in wtype:
                    out.append(f)
            return out

        elif alert_type.startswith('waze_'):
            # Waze police / roadwork - GeoJSON format
            return data.get('features', [])

        elif alert_type == 'user_incident':
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

        pending_seen = []
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
                    parsed['_msg_hash'] = msg_hash
                    new_messages.append(parsed)
                    # Defer mark-seen for REAL messages until after append
                    # (see below) so a crash can't drop an unsent pager alert.
                    pending_seen.append(msg_hash)
                    logger.info(f"New pager message: {parsed.get('capcode', 'UNKNOWN')} - {parsed.get('incident_id', '')} - {parsed.get('type', '')}")
                else:
                    # Filtered/test messages are never sent, so mark them seen
                    # immediately — there's nothing to lose and we want them
                    # suppressed on the next cycle.
                    await loop.run_in_executor(None, self.db.mark_pager_seen, msg_hash)
                    logger.debug(f"Filtered pager message (test/invalid): {pager_msg_id}")

        # Mark real messages seen AFTER they're appended to new_messages.
        # This still runs before check_pager returns, so the next poll cycle
        # (~30s later) sees them as seen and won't re-queue — cross-cycle
        # dedup is intact — while the crash-loss window shrinks to here.
        for msg_hash in pending_seen:
            await loop.run_in_executor(None, self.db.mark_pager_seen, msg_hash)

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
            session = self._get_session()
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
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
            } if msg.get('lat') is not None and msg.get('lon') is not None else None,
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
        return hashlib.md5(message.encode(), usedforsecurity=False).hexdigest()

