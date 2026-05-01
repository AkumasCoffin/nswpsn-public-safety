// ==UserScript==
// @name         NSWPSN Waze Forwarder
// @namespace    nswpsn.forcequit.xyz
// @version      1.21
// @description  Intercept Waze live-map georss responses (via fetch + XHR hooks) in a real user's browser and forward them to the NSWPSN backend. Rotates through NSW regions by finding Waze's map instance and calling its pan/setView API. Does NOT use URL navigation as a fallback because Waze interprets ?ll= URLs as "drop a pin" destinations.
// @match        https://www.waze.com/*
// @match        https://*.waze.com/*
// @grant        GM_xmlhttpRequest
// @grant        GM.xmlHttpRequest
// @grant        GM_cookie
// @grant        GM.cookie
// @grant        unsafeWindow
// @connect      *
// @run-at       document-start
// @updateURL    https://raw.githubusercontent.com/AkumasCoffin/nswpsn-public-safety/main/docs/waze-userscript.user.js
// @downloadURL  https://raw.githubusercontent.com/AkumasCoffin/nswpsn-public-safety/main/docs/waze-userscript.user.js
// ==/UserScript==

(function () {
    'use strict';

    // ====== CONFIG ======
    const BACKEND_URL = 'https://api.forcequit.xyz/api/waze/ingest';
    const INGEST_KEY  = 'REPLACE_WITH_YOUR_WAZE_INGEST_KEY';

    // Regions rotated through. v1.21: switched from a dense ~190-tile
    // zoom-14 grid (street level, ~1-2 km per tile) to a sparse 6-tile
    // zoom-8 grid (~100-150 km per tile) so the script does one full
    // NSW pass in 6 snapshots instead of 190. Trades fine-grained
    // detail in low-density areas for far less browser activity, fewer
    // pan operations, and dramatically lower risk of Waze 403'ing the
    // georss endpoint for over-querying. The trade-off works because
    // Waze's georss endpoint returns up to ~200 alerts per viewport
    // and most regional NSW tiles have well under that.
    //
    // The six tiles are placed to cover the populated parts of NSW:
    //   1. Sydney metro + Central Coast + Blue Mountains
    //   2. Hunter / Newcastle / Mid North Coast
    //   3. Northern Rivers / Far North Coast
    //   4. Illawarra / South Coast / Far South
    //   5. Central West / Western Plains (Bathurst, Dubbo, Orange)
    //   6. Riverina / South West (Wagga, Albury, Griffith)
    // Far-west / outback (Broken Hill, Tibooburra) is intentionally
    // not covered — minimal traffic, high cost-per-alert.
    const REGIONS = [
        // 6-tile NSW pass at zoom 8. Each tile's viewport is ~100-150 km
        // wide. Centres are placed so the union of all six covers the
        // NSW coastal corridor, Hunter, Riverina and Central West —
        // i.e. everywhere with non-trivial traffic.
        { name: 'Sydney + Blue Mountains',     lat: -33.700, lon: 150.800, zoom: 8 },
        { name: 'Hunter / Mid North Coast',    lat: -32.000, lon: 152.200, zoom: 8 },
        { name: 'Northern Rivers',             lat: -29.500, lon: 152.800, zoom: 8 },
        { name: 'Illawarra / South Coast',     lat: -35.300, lon: 150.300, zoom: 8 },
        { name: 'Central West Plains',         lat: -33.000, lon: 148.300, zoom: 8 },
        { name: 'Riverina / South West',       lat: -35.300, lon: 146.500, zoom: 8 },
    ];
    // After panning, wait up to this long for Waze to fire georss for the
    // new viewport. As soon as a georss response arrives we accelerate the
    // next pan — no point waiting if the data is already in.
    //
    // Cadence kept at 7s + 0-2s jitter from v1.18. With v1.21's 6-tile
    // pass, this is ~50s per full sweep and ~7-9 requests/min from one
    // IP/cookie pair — trivially under Waze's bot heuristics. Could go
    // faster but slower-than-needed is friendlier; the backend cache
    // doesn't benefit from sub-minute repolls of the same incidents.
    const PAN_INTERVAL_MS        = 7_000;  // fallback max wait per region (dead zones)
    const PAN_INTERVAL_JITTER_MS = 2_000;  // random extra 0..N per pan
    const PAN_AFTER_INGEST_MS    = 1_500;  // how long to wait after georss before panning
    // Reload the page every 30 min as a recovery for stuck states. Waze
    // occasionally stops emitting georss responses after long sessions —
    // backend warns "Waze ingest stale: no POST in 15m" when this happens.
    // Reloading restarts the SPA, the WebSocket, and our hooks. Cheap.
    // Absolute backstop reload. Must be MORE than 2× full-rotation time
    // (190 regions × 5s ≈ 16 min). Reloading sooner means a region visited
    // just before the reload may not get re-visited within the backend's
    // WAZE_INGEST_MAX_AGE window, so its pins get pruned and the map
    // shows fewer markers. The 4-min stuck-watchdog still catches real
    // hangs — this timer only exists for slow drift the watchdog misses.
    const RELOAD_INTERVAL_MS     = 30 * 60 * 1000;
    // Watchdog: if no successful ingest in this long, force a reload
    // even before the absolute timer fires. The backend's staleness
    // threshold is 15 min — set the watchdog tighter so we recover
    // before the backend pages anyone.
    const STUCK_RELOAD_AFTER_MS  = 4 * 60 * 1000;
    const STUCK_CHECK_INTERVAL_MS = 30 * 1000;     // check more often so we
                                                    // catch stalls quickly
                                                    // even when timers are
                                                    // mildly throttled.

    const log = (...args) => console.log('[NSWPSN]', ...args);

    // ====== CROSS-WORLD ACCESS ======
    const pageWin = (typeof unsafeWindow !== 'undefined') ? unsafeWindow : window;
    const _exportFn = (typeof exportFunction === 'function') ? exportFunction : null;
    const exportedOrDirect = (fn, target) => {
        try { return _exportFn ? _exportFn(fn, target) : fn; }
        catch (e) { return fn; }
    };

    // ====== INTERCEPT + FORWARD ======
    function handleGeorss(urlStr, data) {
        try {
            const u = new URL(urlStr, location.origin);
            forward({
                bbox: {
                    top:    parseFloat(u.searchParams.get('top')),
                    bottom: parseFloat(u.searchParams.get('bottom')),
                    left:   parseFloat(u.searchParams.get('left')),
                    right:  parseFloat(u.searchParams.get('right')),
                },
                alerts: data.alerts || [],
                jams:   data.jams   || [],
                users:  data.users  || [],
            });
        } catch (e) { log('parse err', e); }
    }

    // Watchdog state: last time we successfully POSTed an ingest. If
    // this stalls (Waze SPA wedged, fetch-hook detached, etc.) the
    // backend goes blind to whichever region we were on. Reload
    // proactively before the 30-min absolute timer fires.
    let _lastIngestSuccess = Date.now();

    // Anti-bot state — when Waze 403s/429s the georss endpoint, we go
    // into a cooldown that PERSISTS across page reloads (in localStorage).
    // We deliberately do NOT reload on 403 unless we successfully cleared
    // Waze's session cookies (which requires GM_cookie permission, since
    // Waze's session cookies are HttpOnly and document.cookie can't touch
    // them). Reloading without clearing cookies just hits another 403 and
    // wastes the cooldown.
    const LS_BLOCK_UNTIL = 'nswpsn_waze_block_until';
    const LS_BLOCK_COUNT = 'nswpsn_waze_block_count';
    const COOLDOWN_BASE_MS = 5 * 60 * 1000;   // 5 min for first 403
    const COOLDOWN_MAX_MS  = 30 * 60 * 1000;  // cap at 30 min after repeats

    function loadCooldown() {
        try {
            const until = parseInt(pageWin.localStorage.getItem(LS_BLOCK_UNTIL) || '0', 10);
            const count = parseInt(pageWin.localStorage.getItem(LS_BLOCK_COUNT) || '0', 10);
            // Reset counter once the cooldown has fully elapsed AND we've
            // had a quiet hour — if we've been clean for an hour the
            // backoff history is no longer relevant.
            if (Number.isFinite(until) && Date.now() > until + 60 * 60 * 1000) {
                pageWin.localStorage.setItem(LS_BLOCK_COUNT, '0');
                return { until: 0, count: 0 };
            }
            return {
                until: Number.isFinite(until) ? until : 0,
                count: Number.isFinite(count) ? count : 0,
            };
        } catch (e) { return { until: 0, count: 0 }; }
    }

    function saveCooldown(until, count) {
        try {
            pageWin.localStorage.setItem(LS_BLOCK_UNTIL, String(until));
            pageWin.localStorage.setItem(LS_BLOCK_COUNT, String(count));
        } catch (e) {}
    }

    function isInCooldown() {
        return Date.now() < loadCooldown().until;
    }

    function cooldownRemainingMs() {
        return Math.max(0, loadCooldown().until - Date.now());
    }

    function noteWazeBlock(status) {
        const { count: prevCount } = loadCooldown();
        const newCount = prevCount + 1;
        // Exponential backoff: 5min, 10min, 20min, 30min (capped).
        const delay = Math.min(
            COOLDOWN_BASE_MS * Math.pow(2, newCount - 1),
            COOLDOWN_MAX_MS,
        );
        const until = Date.now() + delay;
        saveCooldown(until, newCount);
        log(`Waze ${status} — pausing rotation for ${Math.round(delay / 60000)}m (count ${newCount})`);
        // No reload — reloading was the bug. Waze's block is IP/session
        // based and reloading just re-triggers the watchdog/scheduled-
        // reload loop. Instead: pause rotation in place. The page stays
        // loaded, Waze keeps doing whatever it does, and after the
        // cooldown elapses we silently resume panning. If the block was
        // tied to our cookie, time alone usually clears it.
    }

    function forward(payload) {
        const xhr = (typeof GM_xmlhttpRequest !== 'undefined') ? GM_xmlhttpRequest
                  : (typeof GM !== 'undefined' && GM.xmlHttpRequest) ? GM.xmlHttpRequest
                  : null;
        if (!xhr) { log('no GM_xmlhttpRequest'); return; }
        xhr({
            method: 'POST',
            url: BACKEND_URL,
            headers: { 'Content-Type': 'application/json', 'X-Ingest-Key': INGEST_KEY },
            data: JSON.stringify(payload),
            timeout: 15000,
            onload: (r) => {
                if (r.status >= 200 && r.status < 300) {
                    _lastIngestSuccess = Date.now();
                    log('ingest OK', payload.alerts.length + 'a', payload.jams.length + 'j');
                    try { onIngestArrived(); } catch (e) {}
                } else {
                    log('ingest HTTP', r.status, (r.responseText || '').slice(0, 120));
                }
            },
            onerror: (e) => log('ingest err', e),
        });
    }

    // ====== FETCH HOOK ======
    try {
        const origFetch = pageWin.fetch.bind(pageWin);
        pageWin.fetch = exportedOrDirect(function (input, init) {
            return origFetch(input, init).then(function (response) {
                try {
                    let urlStr = '';
                    if (typeof input === 'string') urlStr = input;
                    else if (input && input.href) urlStr = input.href;
                    else if (input && input.url) urlStr = input.url;
                    if (urlStr && urlStr.indexOf('/api/georss') !== -1) {
                        if (response.ok) {
                            response.clone().json().then(d => handleGeorss(urlStr, d)).catch(() => {});
                        } else if (response.status === 403 || response.status === 429) {
                            // Waze rate-limited / blocked us. Trigger cooldown.
                            noteWazeBlock(response.status);
                        }
                    }
                } catch (e) {}
                return response;
            });
        }, pageWin);
        log('fetch hook installed');
    } catch (e) { log('fetch hook fail:', e); }

    // ====== XHR HOOK (Waze uses Axios → XHR) ======
    try {
        const XHR = pageWin.XMLHttpRequest;
        const origOpen = XHR.prototype.open;
        const origSend = XHR.prototype.send;
        XHR.prototype.open = exportedOrDirect(function (method, url) {
            try { this._nswpsnUrl = url; } catch (e) {}
            return origOpen.apply(this, arguments);
        }, pageWin);
        XHR.prototype.send = exportedOrDirect(function (body) {
            try {
                const url = this._nswpsnUrl;
                if (url && String(url).indexOf('/api/georss') !== -1) {
                    const self = this;
                    this.addEventListener('load', function () {
                        try {
                            if (self.readyState !== 4) return;
                            if (self.status === 200 && self.responseText) {
                                handleGeorss(String(url), JSON.parse(self.responseText));
                            } else if (self.status === 403 || self.status === 429) {
                                noteWazeBlock(self.status);
                            }
                        } catch (e) {}
                    });
                }
            } catch (e) {}
            return origSend.apply(this, arguments);
        }, pageWin);
        log('XHR hook installed');
    } catch (e) { log('XHR hook fail:', e); }

    // ====== CAPTURE LEAFLET MAP INSTANCE ======
    // Waze's Leaflet map isn't exposed on any DOM element or global —
    // it lives in module scope. We can't reach it directly. But we CAN
    // monkey-patch L.Map.prototype so that whenever Waze's map fires
    // any event (tile load, mouse move, resize — happens constantly),
    // we capture `this` into pageWin.__nswpsnMap. findLeafletMap() then
    // returns that.

    function tryInstallMapCapture() {
        if (pageWin.__nswpsnMapCaptureInstalled) return true;
        if (!pageWin.L || !pageWin.L.Map || !pageWin.L.Map.prototype) return false;
        try {
            const MapProto = pageWin.L.Map.prototype;
            const origFire = MapProto.fire;
            const hookedFire = function (type, data, propagate) {
                try { pageWin.__nswpsnMap = this; } catch (e) {}
                return origFire.apply(this, arguments);
            };
            MapProto.fire = exportedOrDirect(hookedFire, pageWin);
            pageWin.__nswpsnMapCaptureInstalled = true;
            log('L.Map.prototype.fire hook installed — waiting for first map event');
            return true;
        } catch (e) { log('L.Map hook failed:', e); return false; }
    }

    // Install the capture hook ASAP. Try immediately; if window.L isn't
    // loaded yet, poll for up to 30s.
    if (!tryInstallMapCapture()) {
        const poll = setInterval(() => {
            if (tryInstallMapCapture()) clearInterval(poll);
        }, 250);
        setTimeout(() => clearInterval(poll), 30_000);
    }

    // ====== MAP-PAN ROTATION ======
    // Tries to find Waze's Leaflet map instance (via the capture hook above
    // or DOM probing) and call setView() directly. No URL navigation fallback
    // because Waze treats ?ll= URLs as dropped-pin destinations.

    function findLeafletMap() {
        // Waze's map library isn't public. Probe a bunch of common patterns
        // and return anything with setView() or panTo() + setZoom().

        // Wrap a map-ish object so rotateToNextRegion can call setView uniformly
        const NOANIM = { animate: false, duration: 0 };
        const wrap = (m) => {
            if (typeof m.setView === 'function') {
                // Expose both setView (Leaflet native) AND setZoom/getZoom
                // pass-through so rotateToNextRegion can force the zoom.
                return m;
            }
            if (typeof m.panTo === 'function') {
                return {
                    setView: (ll, z) => {
                        try { m.panTo(ll, NOANIM); } catch (e) {}
                        if (z != null && typeof m.setZoom === 'function') {
                            try { m.setZoom(z, NOANIM); } catch (e) {}
                        }
                    },
                    setZoom: (z) => {
                        if (typeof m.setZoom === 'function') {
                            try { m.setZoom(z, NOANIM); } catch (e) {}
                        }
                    },
                    getZoom: () => typeof m.getZoom === 'function' ? m.getZoom() : undefined,
                };
            }
            if (typeof m.setCenter === 'function') {
                return {
                    setView: (ll, z) => {
                        try { m.setCenter(ll); } catch (e) {}
                        if (z != null && typeof m.setZoom === 'function') {
                            try { m.setZoom(z); } catch (e) {}
                        }
                    },
                    setZoom: (z) => {
                        if (typeof m.setZoom === 'function') {
                            try { m.setZoom(z); } catch (e) {}
                        }
                    },
                    getZoom: () => typeof m.getZoom === 'function' ? m.getZoom() : undefined,
                };
            }
            return null;
        };

        try {
            // 0. Did our prototype hook capture the Map instance? (Best path —
            // works regardless of how Waze exposes it, as long as the map has
            // fired any event since the hook was installed.)
            if (pageWin.__nswpsnMap && typeof pageWin.__nswpsnMap.setView === 'function') {
                return wrap(pageWin.__nswpsnMap);
            }

            // 1. Leaflet — Waze uses custom class names (wz-livemap, wm-map)
            // not the default .leaflet-container. Probe Waze-specific selectors
            // first, then fall back to generic.
            const leafletContainers = pageWin.document.querySelectorAll(
                '.wz-livemap, .wm-map, .wz-map-placeholder__container, .leaflet-container, .wm-map__leaflet'
            );
            for (const c of leafletContainers) {
                if (c._leaflet_map) {
                    const w = wrap(c._leaflet_map);
                    if (w) { log(`map found at <${c.tagName.toLowerCase()}.${c.className}>._leaflet_map`); return w; }
                }
                // Scan ALL expando props (including _-prefixed ones).
                // Leaflet uses several: _leaflet_map, _leaflet_id, etc.
                // Previous version skipped '_' keys — that was the bug.
                for (const k of Object.keys(c)) {
                    const v = c[k];
                    if (v && typeof v === 'object' && (typeof v.setView === 'function' || typeof v.panTo === 'function')) {
                        const w = wrap(v);
                        if (w) { log(`map found at <${c.tagName.toLowerCase()}.${c.className}>.${k}`); return w; }
                    }
                }
            }

            // 2. Waze-specific globals (legacy)
            if (pageWin.W && pageWin.W.map) {
                const w = wrap(pageWin.W.map);
                if (w) return w;
            }

            // 3. Mapbox GL / similar — expose as global or attached to canvas
            if (pageWin.mapboxgl && pageWin.map) {
                const w = wrap(pageWin.map);
                if (w) return w;
            }

            // 4. OpenLayers — has getView().setCenter
            if (pageWin.ol && pageWin.map) {
                const m = pageWin.map;
                if (m.getView && typeof m.getView().setCenter === 'function') {
                    return {
                        setView: (ll, z) => {
                            try { m.getView().setCenter(ll); } catch (e) {}
                            if (z != null) { try { m.getView().setZoom(z); } catch (e) {} }
                        }
                    };
                }
            }

            // 5. Scan window for any global that quacks like a map
            const MAP_HINTS = /^(map|waze|livemap|_map|__map|__W|W)$/i;
            for (const k of Object.keys(pageWin)) {
                if (!MAP_HINTS.test(k)) continue;
                const v = pageWin[k];
                if (v && typeof v === 'object') {
                    const w = wrap(v);
                    if (w) { log(`map found at window.${k}`); return w; }
                    // Nested: e.g. window.W.map
                    for (const k2 of Object.keys(v)) {
                        const v2 = v[k2];
                        if (v2 && typeof v2 === 'object') {
                            const w2 = wrap(v2);
                            if (w2) { log(`map found at window.${k}.${k2}`); return w2; }
                        }
                    }
                }
            }

            // 6. Scan every visible canvas / map-ish div's expando properties
            // (Don't skip _-prefixed ones — Leaflet uses _leaflet_map.)
            const candidates = pageWin.document.querySelectorAll(
                'canvas, [class*="map" i], [class*="Map" i], [id*="map" i]'
            );
            for (const el of candidates) {
                for (const k of Object.keys(el)) {
                    if (k.startsWith('__')) continue;  // skip React internals only
                    const v = el[k];
                    if (v && typeof v === 'object' && (typeof v.setView === 'function' || typeof v.panTo === 'function' || typeof v.setCenter === 'function')) {
                        const w = wrap(v);
                        if (w) { log(`map found on element <${el.tagName.toLowerCase()}>.${k}`); return w; }
                    }
                }
            }

            // 7. Last resort: full DOM walk looking for _leaflet_map.
            // Slow (scans every element) but guaranteed to find it if Leaflet
            // initialised on any element.
            const all = pageWin.document.querySelectorAll('*');
            for (const el of all) {
                if (el._leaflet_map && typeof el._leaflet_map.setView === 'function') {
                    log(`map found via full DOM walk on <${el.tagName.toLowerCase()}.${el.className}>`);
                    return wrap(el._leaflet_map);
                }
            }
        } catch (e) { log('findLeafletMap err', e); }
        return null;
    }

    // Persist the region index in localStorage so if the page reloads
    // for any reason, we pick up where we left off instead of looping on
    // the first region forever.
    const LS_KEY = 'nswpsn_region_idx';
    function loadRegionIdx() {
        try {
            const v = parseInt(pageWin.localStorage.getItem(LS_KEY), 10);
            return Number.isFinite(v) ? (v % REGIONS.length) : 0;
        } catch (e) { return 0; }
    }
    function saveRegionIdx(i) {
        try { pageWin.localStorage.setItem(LS_KEY, String(i)); } catch (e) {}
    }

    function rotateToNextRegion() {
        let idx = loadRegionIdx();
        const r = REGIONS[idx];
        saveRegionIdx((idx + 1) % REGIONS.length);

        const map = findLeafletMap();
        if (map) {
            try {
                // setView + setZoom — both called with animate:false because
                // Waze's own map listeners can intercept animated transitions
                // mid-flight and snap zoom back to their default. Teleporting
                // with animate:false + a followup setZoom forces the target.
                map.setView([r.lat, r.lon], r.zoom, { animate: false, duration: 0 });
                if (typeof map.setZoom === 'function') {
                    map.setZoom(r.zoom, { animate: false });
                }
                // Waze sometimes reasserts the zoom 100-300ms after pan.
                // Re-apply once more just to make sure ours sticks.
                setTimeout(() => {
                    try {
                        if (typeof map.setZoom === 'function' &&
                            typeof map.getZoom === 'function' &&
                            map.getZoom() !== r.zoom) {
                            map.setZoom(r.zoom, { animate: false });
                        }
                    } catch (e) {}
                }, 400);
                log(`panned → ${r.name} (${r.lat}, ${r.lon}) z=${r.zoom}`);
                return;
            } catch (e) { log('setView err', e); }
        }
        // No map API found — do NOT fall back to URL navigation. Waze
        // interprets `?ll=X,Y` as a dropped-pin destination, which reloads
        // the page and burns the reCAPTCHA trust score. Instead, log
        // diagnostics so we can add detection paths for whatever Waze is
        // using under the hood.
        log('WARN: no map instance found — skipping pan. Current region stays.');
        log('Map debugging — attach this to an issue if rotation never works:');
        try {
            const keys = Object.keys(pageWin).filter(k => /map|waze|leaflet|ol\b/i.test(k)).slice(0, 20);
            log('  window keys with map-ish names:', keys);
            const containers = pageWin.document.querySelectorAll('.leaflet-container, canvas, [class*="map" i]');
            log('  potential map containers:', containers.length, Array.from(containers).slice(0, 3).map(c => c.className || c.tagName));
        } catch (e) {}
    }

    // Event-driven rotation: after panning to a region, arm two timers:
    //   1. A short "ingest arrived" timer that pans as soon as the first
    //      georss response lands for this viewport (usually 1-3s after pan).
    //   2. A longer fallback that pans anyway if nothing arrives (dead zone).
    // Whichever fires first wins. We re-arm on every pan.
    let rotationTimer = null;
    let ingestArmedForCurrent = false;
    let ingestWaitTimer = null;

    function scheduleNextPan() {
        clearTimeout(rotationTimer);
        clearTimeout(ingestWaitTimer);
        ingestArmedForCurrent = true;
        // Add 0..PAN_INTERVAL_JITTER_MS so the timing isn't a perfect
        // 7s metronome — Waze's bot heuristics can fingerprint regular
        // intervals.
        const jitter = Math.floor(Math.random() * PAN_INTERVAL_JITTER_MS);
        rotationTimer = setTimeout(() => {
            ingestArmedForCurrent = false;
            // Skip the pan during cooldown — every request just earns
            // another 403 and resets the cookie clock.
            if (isInCooldown()) {
                log(`cooldown active, skipping pan (${Math.round((_wazeBlockedUntil - Date.now()) / 60000)}m left)`);
                scheduleNextPan();
                return;
            }
            rotateToNextRegion();
            scheduleNextPan();
        }, PAN_INTERVAL_MS + jitter);
    }

    // Called by handleGeorss on successful ingest — bumps up the pan schedule
    function onIngestArrived() {
        if (!ingestArmedForCurrent) return;
        ingestArmedForCurrent = false;
        clearTimeout(rotationTimer);
        clearTimeout(ingestWaitTimer);
        ingestWaitTimer = setTimeout(() => {
            rotateToNextRegion();
            scheduleNextPan();
        }, PAN_AFTER_INGEST_MS);
    }

    function startRotation() {
        // Honour persisted cooldown across page reloads — if we just
        // came back from a 403 reload, the cooldown is still ticking
        // and panning immediately would just earn us another 403.
        const remainingMs = cooldownRemainingMs();
        if (remainingMs > 0) {
            log(`startRotation: cooldown active, waiting ${Math.round(remainingMs / 60000)}m before first pan`);
            setTimeout(startRotation, remainingMs + 1000);
            return;
        }
        // Wait ~15s for Waze's map JS to initialise before first pan attempt.
        setTimeout(() => {
            if (isInCooldown()) {
                // Race: a 403 hit in the 15s warm-up. Re-arm.
                startRotation();
                return;
            }
            rotateToNextRegion();
            scheduleNextPan();
        }, 15_000);
    }

    // Only rotate if we're on a live-map URL (don't rotate on /directions etc)
    function onLiveMap() {
        return /live-map/.test(pageWin.location.pathname);
    }

    if (onLiveMap()) {
        startRotation();
    } else {
        log('not on live-map — rotation skipped');
    }

    // Try every reload mechanism we can — pageWin.location.reload() can
    // fail silently when the tab is heavily backgrounded or when Waze's
    // SPA has wedged the JS context. Fall back to assigning to href and
    // top-level reload.
    let _reloadInProgress = false;
    function forceReload(reason) {
        if (_reloadInProgress) return;
        _reloadInProgress = true;
        log(`reload (${reason})`);
        const tries = [
            () => pageWin.location.reload(),
            () => { pageWin.location.href = pageWin.location.href; },
            () => window.location.reload(),
            () => { window.location.href = window.location.href; },
            () => { if (window.top) window.top.location.reload(); },
        ];
        for (const fn of tries) {
            try { fn(); return; } catch (e) {}
        }
        log('all reload paths failed — manual refresh needed');
    }

    // Scheduled reload — fires once per page load. After reload, the script
    // re-runs and schedules a fresh timer, so this naturally repeats.
    // Region index is persisted in localStorage (LS_KEY) so we resume mid-rotation.
    // Skipped during cooldown — reloading mid-block earns more 403s.
    setTimeout(() => {
        if (isInCooldown()) {
            log(`scheduled ${RELOAD_INTERVAL_MS / 60000}m reload — skipped (cooldown active)`);
            return;
        }
        forceReload(`scheduled ${RELOAD_INTERVAL_MS / 60000}m`);
    }, RELOAD_INTERVAL_MS);

    // Watchdog — runs on every check tick AND every visibility change.
    // The visibility hook is the important one: backgrounded tabs get
    // setInterval throttled to 1/min and sometimes stalled entirely, so
    // when the user comes back to check on the tab we re-evaluate state
    // immediately instead of waiting for the next throttled tick.
    function checkStuck() {
        // During cooldown, the cooldown handler already manages the reload.
        // Don't fight it — that would just rapid-fire reloads and prevent
        // the cooldown from elapsing.
        if (isInCooldown()) return;
        const idleMs = Date.now() - _lastIngestSuccess;
        if (idleMs > STUCK_RELOAD_AFTER_MS) {
            forceReload(`watchdog ${Math.round(idleMs / 1000)}s idle`);
        }
    }

    // Self-rescheduling setTimeout chain. Slightly more robust under
    // background-throttling than setInterval — each tick reschedules
    // from the actual fire time, so drift can't accumulate quietly.
    function scheduleNextStuckCheck() {
        setTimeout(() => {
            try { checkStuck(); } catch (e) {}
            scheduleNextStuckCheck();
        }, STUCK_CHECK_INTERVAL_MS);
    }
    scheduleNextStuckCheck();

    // Backup setInterval — belt-and-suspenders in case the chained
    // setTimeout drops a beat.
    setInterval(() => { try { checkStuck(); } catch (e) {} }, STUCK_CHECK_INTERVAL_MS);

    // Visibility change handler — fires the moment the tab is foregrounded.
    // Catches the common case where the tab was hidden long enough for
    // browser throttling to stall the timers entirely.
    try {
        document.addEventListener('visibilitychange', () => {
            if (!document.hidden) {
                log('tab visible — running stuck check');
                checkStuck();
            }
        });
        pageWin.addEventListener('focus', checkStuck);
    } catch (e) { log('visibility hook fail:', e); }

    {
        const _cd = loadCooldown();
        const _remMin = Math.round(Math.max(0, _cd.until - Date.now()) / 60000);
        log('NSWPSN Waze Forwarder v1.20 loaded — backend:', BACKEND_URL,
            `· pan ${PAN_INTERVAL_MS / 1000}s+jitter`,
            `· auto-reload ${RELOAD_INTERVAL_MS / 60000}m`,
            `· watchdog ${STUCK_RELOAD_AFTER_MS / 60000}m`,
            `· 403/429 cooldown ${COOLDOWN_BASE_MS / 60000}-${COOLDOWN_MAX_MS / 60000}m`,
            _cd.until > 0
                ? `· COOLDOWN ACTIVE ${_remMin}m left (count ${_cd.count})`
                : '· no active cooldown');
    }
})();
