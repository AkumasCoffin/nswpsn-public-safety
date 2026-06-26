// ==UserScript==
// @name         NSWPSN Waze Forwarder
// @namespace    nswpsn.forcequit.xyz
// @version      1.25
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

    // Regions rotated through. v1.25: expanded to a ~94-point NSW grid —
    // four zoom-13 "Wide Zone" catch-alls plus ~90 zoom-14 street-level
    // tiles over the populated corridors (Sydney, Illawarra/South Coast,
    // Hunter, Mid/North Coast, Riverina, Canberra region, Central West).
    // The region index is persisted in localStorage and 403/429 blocks
    // now pause-and-continue (see noteWazeBlock) rather than reloading, so
    // a long list completes across blocks. One full sweep is ~5-12 min
    // depending on how fast georss returns per tile.
    //
    // NOTE: a few entries are still labelled "Riverina / South West" but
    // sit near Sydney/Illawarra — the coordinates are authoritative, the
    // names are cosmetic.
    const REGIONS = [
        { name: 'Wide Zone 1', lat: -36.778458, lon: 149.950325, zoom: 13 },
        { name: 'Wide Zone 2', lat: -35.639371, lon: 149.883138, zoom: 13 },
        { name: 'Wide Zone 3', lat: -30.059437, lon: 152.449813, zoom: 13 },
        { name: 'Wide Zone 4', lat: -34.015957, lon: 146.195516, zoom: 13 },
        { name: 'Sydney + Blue Mountains', lat: -33.700, lon: 150.800, zoom: 14 },
        { name: 'Northen Sydney', lat: -33.833, lon: 151.207, zoom: 14 },
        { name: 'Sydney CBD', lat: -33.897, lon: 151.213, zoom: 14 },
        { name: 'Sydney South', lat: -34.032, lon: 151.101, zoom: 14 },
        { name: 'Hunter / Mid North Coast', lat: -32.000, lon: 152.200, zoom: 14 },
        { name: 'Northern Rivers', lat: -29.500, lon: 152.800, zoom: 14 },
        { name: 'Illawarra / South Coast', lat: -35.300, lon: 150.300, zoom: 14 },
        { name: 'Central West Plains', lat: -33.000, lon: 148.300, zoom: 14 },
        { name: 'Riverina / South West', lat: -35.300, lon: 146.500, zoom: 14 },
        { name: 'nowra', lat: -34.889, lon: 150.606, zoom: 14 },
        { name: 'falls creek', lat: -35.154, lon: 150.575, zoom: 14 },
        { name: 'gong north', lat: -34.317, lon: 150.924, zoom: 14 },
        { name: 'Riverina / South West', lat: -34.551, lon: 150.814, zoom: 14 },
        { name: 'Riverina / South West', lat: -33.779, lon: 150.911, zoom: 14 },
        { name: 'Penrith / Cranebrook', lat: -33.766572, lon: 150.802924, zoom: 14 },
        { name: 'Prospect / Pemulwuy', lat: -33.801956, lon: 151.027964, zoom: 14 },
        { name: 'Lane Cove / Chatswood', lat: -33.833902, lon: 151.254376, zoom: 14 },
        { name: 'Wallacia / Silverdale', lat: -33.939924, lon: 150.723396, zoom: 14 },
        { name: 'Bankstown / Padstow', lat: -33.966124, lon: 150.951181, zoom: 14 },
        { name: 'Sutherland / Menai', lat: -33.996871, lon: 151.114472, zoom: 14 },
        { name: 'Camden / Mount Hunter', lat: -34.167482, lon: 150.765934, zoom: 14 },
        { name: 'Picton / Wilton', lat: -34.371761, lon: 150.901745, zoom: 14 },
        { name: 'Cataract / Appin', lat: -34.516723, lon: 150.837252, zoom: 14 },
        { name: 'Dapto / Avondale', lat: -34.674988, lon: 150.855091, zoom: 14 },
        { name: 'Kangaroo Valley', lat: -34.867896, lon: 150.602194, zoom: 14 },
        { name: 'Berry / Bomaderry', lat: -35.024356, lon: 150.673724, zoom: 14 },
        { name: 'Canberra North / Belconnen', lat: -35.209152, lon: 149.115020, zoom: 14 },
        { name: 'Canberra Central', lat: -35.321840, lon: 149.123253, zoom: 14 },
        { name: 'Canberra / Woden', lat: -35.403035, lon: 149.119823, zoom: 14 },
        { name: 'Canberra / Fyshwick', lat: -35.380084, lon: 149.226042, zoom: 14 },
        { name: 'Batemans Bay', lat: -35.703581, lon: 150.187950, zoom: 14 },
        { name: 'Nerriga / Endrick', lat: -35.340326, lon: 150.417245, zoom: 14 },
        { name: 'Lake Macquarie / Morisset', lat: -33.098427, lon: 151.581014, zoom: 14 },
        { name: 'Newcastle / Lake Macquarie', lat: -32.914162, lon: 151.729211, zoom: 14 },
        { name: 'Port Stephens / Nelson Bay', lat: -32.738754, lon: 152.113872, zoom: 14 },
        { name: 'Port Macquarie', lat: -31.450907, lon: 152.894893, zoom: 14 },
        { name: 'Jerilderie', lat: -35.362159, lon: 145.725113, zoom: 14 },
        { name: 'Wagga Wagga', lat: -35.389015, lon: 146.383053, zoom: 14 },
        { name: 'Swan Hill / Tooleybuc', lat: -34.962427, lon: 143.483915, zoom: 14 },
        { name: 'Mildura / Merbein', lat: -35.229898, lon: 141.248196, zoom: 14 },
        { name: 'Narooma / Tilba', lat: -36.223218, lon: 150.128371, zoom: 14 },
        { name: 'Cann River / Bombala', lat: -37.313366, lon: 149.932245, zoom: 14 },
        { name: 'Eden / Pambula', lat: -36.986083, lon: 149.777034, zoom: 14 },
        { name: 'Merimbula / Tathra', lat: -36.678315, lon: 149.949950, zoom: 14 },
        { name: 'Jindabyne / Berridale', lat: -36.146677, lon: 148.065160, zoom: 14 },
        { name: 'Albury / Wodonga', lat: -36.047971, lon: 146.951954, zoom: 14 },
        { name: 'Lockhart / The Rock', lat: -35.382297, lon: 146.160268, zoom: 14 },
        { name: 'Yamba / Maclean', lat: -29.056095, lon: 153.413975, zoom: 14 },
        { name: 'Broken Hill', lat: -30.087959, lon: 142.771761, zoom: 14 },
        { name: 'Bourke / Brewarrina', lat: -29.783300, lon: 145.692195, zoom: 14 },
        { name: 'Moree', lat: -29.916703, lon: 150.151179, zoom: 14 },
        { name: 'Tamworth / Gunnedah', lat: -31.146712, lon: 150.302402, zoom: 14 },
        { name: 'Coonabarabran / Gilgandra', lat: -31.071462, lon: 148.809455, zoom: 14 },
        { name: 'Nyngan / Cobar', lat: -30.836067, lon: 146.070713, zoom: 14 },
        { name: 'Central Coast / Gosford', lat: -33.387844, lon: 151.456784, zoom: 14 },
        { name: 'Dapto / Albion Park', lat: -34.673309, lon: 150.849426, zoom: 14 },
        { name: 'Forster / Tuncurry', lat: -32.435577, lon: 152.144535, zoom: 14 },
        { name: 'Wagga Wagga East', lat: -35.134492, lon: 147.677250, zoom: 14 },
        { name: 'Coolamon / Ganmain', lat: -35.096312, lon: 147.364209, zoom: 14 },
        { name: 'Junee / Marrar', lat: -35.185590, lon: 147.356662, zoom: 14 },
        { name: 'Griffith', lat: -34.746682, lon: 146.559805, zoom: 14 },
        { name: 'Leeton / Yanco', lat: -34.741039, lon: 146.779363, zoom: 14 },
        { name: 'Muswellbrook / Denman', lat: -32.335875, lon: 150.850695, zoom: 14 },
        { name: 'Singleton', lat: -32.463996, lon: 151.035395, zoom: 14 },
        { name: 'Branxton / Greta', lat: -32.600617, lon: 151.060780, zoom: 14 },
        { name: 'Cessnock / Kurri Kurri', lat: -32.709880, lon: 151.101269, zoom: 14 },
        { name: 'Maitland', lat: -32.826483, lon: 151.339017, zoom: 14 },
        { name: 'Raymond Terrace', lat: -32.787238, lon: 151.575034, zoom: 14 },
        { name: 'Coffs Harbour', lat: -30.075018, lon: 153.188499, zoom: 14 },
        { name: 'Coffs Harbour South', lat: -30.219897, lon: 153.146965, zoom: 14 },
        { name: 'Sawtell / Bonville', lat: -30.324267, lon: 153.119521, zoom: 14 },
        { name: 'Urunga / Mylestom', lat: -30.484147, lon: 153.029340, zoom: 14 },
        { name: 'Nambucca Heads', lat: -30.616041, lon: 152.971131, zoom: 14 },
        { name: 'Cobar / Nyngan West', lat: -30.599798, lon: 145.009396, zoom: 14 },
        { name: 'Lightning Ridge / Walgett', lat: -29.151861, lon: 148.695047, zoom: 14 },
        { name: 'Walgett / Come By Chance', lat: -30.149803, lon: 148.663142, zoom: 14 },
        { name: 'Narrabri / Wee Waa', lat: -30.116473, lon: 150.261235, zoom: 14 },
        { name: 'Goondiwindi / Boggabilla', lat: -28.439563, lon: 150.568606, zoom: 14 },
        { name: 'Narrandera / Grong Grong', lat: -34.292954, lon: 146.055804, zoom: 14 },
        { name: 'Narrandera East', lat: -34.211767, lon: 146.713946, zoom: 14 },
        { name: 'Junee South', lat: -35.173804, lon: 147.359512, zoom: 14 },
        { name: 'Coolamon South', lat: -35.096874, lon: 147.365344, zoom: 14 },
        { name: 'Wagga / Lake Albert', lat: -35.124678, lon: 147.279239, zoom: 14 },
        { name: 'Wagga / Gumly Gumly', lat: -35.148262, lon: 147.458996, zoom: 14 },
        { name: 'Goulburn West', lat: -34.774891, lon: 149.687394, zoom: 14 },
        { name: 'Goulburn', lat: -34.722140, lon: 149.745713, zoom: 14 },
        { name: 'Tarago / Lake Bathurst', lat: -34.710430, lon: 149.996833, zoom: 14 },
        { name: 'Bungonia / Marulan', lat: -34.709019, lon: 150.096852, zoom: 14 },
        { name: 'Nowra / Bomaderry', lat: -34.847901, lon: 150.604764, zoom: 14 },
        { name: 'Ulladulla / Milton', lat: -35.257670, lon: 150.513800, zoom: 14 },
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

    // After every full sweep through REGIONS, sleep an extra random
    // 30-60s before starting the next round. Disrupts perfectly-
    // periodic sweep timing (which Waze's heuristics could fingerprint
    // as a 50s metronome) and gives our backend cache a moment of
    // quiet between batches.
    const ROUND_PAUSE_MIN_MS     = 30_000;
    const ROUND_PAUSE_RANGE_MS   = 30_000;  // 30s..60s
    const PAN_AFTER_INGEST_MS    = 1_500;  // how long to wait after georss before panning
    // Reload the page every 30 min as a recovery for stuck states. Waze
    // occasionally stops emitting georss responses after long sessions —
    // backend warns "Waze ingest stale: no POST in 15m" when this happens.
    // Reloading restarts the SPA, the WebSocket, and our hooks. Cheap.
    // Absolute backstop reload. Must be MORE than 2× full-rotation time
    // (~94 regions × ~3-7s ≈ 5-12 min). Reloading sooner means a region
    // visited just before the reload may not get re-visited within the
    // backend's WAZE_INGEST_MAX_AGE window, so its pins get pruned and the
    // map shows fewer markers. The 4-min stuck-watchdog still catches real
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

    // Anti-bot state — when Waze 403s/429s the georss endpoint we
    // try to clear waze.com cookies via GM_cookie (HttpOnly cookies
    // are reachable through that API where document.cookie can't see
    // them) and force a page reload, which gives us a fresh session
    // and usually clears the block immediately.
    //
    // Circuit breaker: if we hit too many 403s in a row across reloads
    // (tracked via LS_BLOCK_COUNT, which persists), the IP itself is
    // probably banned and reloading just earns more 403s. After
    // RELOAD_BACKOFF_THRESHOLD consecutive 403s we fall back to the
    // exponential cooldown without reloading.
    const LS_BLOCK_UNTIL = 'nswpsn_waze_block_until';
    const LS_BLOCK_COUNT = 'nswpsn_waze_block_count';
    const COOLDOWN_BASE_MS = 5 * 60 * 1000;   // 5 min for first 403 past threshold
    const COOLDOWN_MAX_MS  = 30 * 60 * 1000;  // cap at 30 min after repeats
    const RELOAD_BACKOFF_THRESHOLD = 3;       // reload attempts before long backoff
    // Block handling is tiered so a long region list still completes across
    // 403/429s. The old code full-reloaded on EVERY block, and the 15s
    // warm-up per reload meant a big list never finished a single sweep:
    //   tier 1 (≤ SOFT_BLOCK_LIMIT):  short in-place cooldown, NO reload —
    //          the rotation loop pauses then resumes from the same index.
    //   tier 2 (next RELOAD_BACKOFF_THRESHOLD): clear cookies + reload once
    //          for a fresh waze.com session.
    //   tier 3: long exponential backoff, no reload (IP likely throttled).
    const SOFT_BLOCK_LIMIT        = 4;
    const SOFT_COOLDOWN_MS        = 60_000;   // pause on a soft block, then resume
    const SOFT_COOLDOWN_JITTER_MS = 30_000;

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

    // GM_cookie is the only way to delete HttpOnly waze.com cookies
    // (where document.cookie is blocked). Both the legacy GM_cookie
    // (Tampermonkey) and the modern GM.cookie (Violentmonkey, GM4)
    // are supported.
    function getCookieApi() {
        if (typeof GM_cookie !== 'undefined' && GM_cookie) return GM_cookie;
        if (typeof GM !== 'undefined' && GM && GM.cookie) return GM.cookie;
        return null;
    }

    // forceReload() is defined once, lower in this file (the robust
    // multi-fallback version guarded by _reloadInProgress). A duplicate
    // definition used to live here and shadowed that one as dead code —
    // removed to avoid confusion about which reload path actually runs.

    // Best-effort: enumerate every waze.com cookie and delete them all.
    // Calls cb() once when done (or after a 3s safety timeout if the
    // userscript manager's API never invokes our callbacks).
    function clearWazeCookies(cb) {
        const api = getCookieApi();
        if (!api) {
            log('GM_cookie API not available — cannot clear cookies');
            cb(false);
            return;
        }
        let done = false;
        const finish = (ok) => {
            if (done) return;
            done = true;
            cb(ok);
        };
        // Safety timeout — some managers silently drop the callback.
        setTimeout(() => finish(false), 3000);
        try {
            // GM_cookie.list takes a filter (domain) + callback. Some
            // managers also expose Promise-based GM.cookie.list().
            const listRes = api.list({ domain: 'waze.com' }, (cookies) => {
                handleList(cookies);
            });
            if (listRes && typeof listRes.then === 'function') {
                listRes.then(handleList).catch((err) => {
                    log('cookie list failed', err);
                    finish(false);
                });
            }
        } catch (e) {
            log('cookie list threw', e);
            finish(false);
        }
        function handleList(cookies) {
            const all = Array.isArray(cookies) ? cookies : [];
            if (all.length === 0) {
                log('no waze cookies to clear');
                finish(true);
                return;
            }
            let pending = all.length;
            const tick = () => {
                pending--;
                if (pending <= 0) {
                    log(`cleared ${all.length} waze cookie(s)`);
                    finish(true);
                }
            };
            for (const c of all) {
                try {
                    const dRes = api.delete({
                        url: 'https://www.waze.com/',
                        domain: c.domain,
                        name: c.name,
                    }, tick);
                    if (dRes && typeof dRes.then === 'function') {
                        dRes.then(tick).catch(tick);
                    }
                } catch (e) { tick(); }
            }
        }
    }

    function noteWazeBlock(status) {
        const { count: prevCount } = loadCooldown();
        const newCount = prevCount + 1;

        // Tier 1 — soft block: short in-place cooldown and KEEP rotating.
        // scheduleNextPan() honours the cooldown (skips panning) and then
        // resumes from the persisted region index, so the list keeps
        // advancing across blocks instead of restarting on every 403. A
        // clean ingest resets the counter, so this only escalates on a
        // genuine sustained block, not on isolated 403s.
        if (newCount <= SOFT_BLOCK_LIMIT) {
            const delay = SOFT_COOLDOWN_MS + Math.floor(Math.random() * SOFT_COOLDOWN_JITTER_MS);
            saveCooldown(Date.now() + delay, newCount);
            log(`Waze ${status} #${newCount} — soft cooldown ${Math.round(delay / 1000)}s, continuing rotation (no reload)`);
            return;
        }

        // Tier 2 — sustained blocking: the waze.com session looks stale.
        // Clear cookies + reload ONCE for a fresh session, bounded by the
        // reload budget. LS_BLOCK_COUNT persists across the reload so we
        // keep counting; LS_BLOCK_UNTIL gets a tiny 5s window so
        // startRotation has a breather before the first post-reload pan.
        if (newCount <= SOFT_BLOCK_LIMIT + RELOAD_BACKOFF_THRESHOLD) {
            saveCooldown(Date.now() + 5_000, newCount);
            log(`Waze ${status} #${newCount} — clearing cookies + reloading (fresh session)`);
            clearWazeCookies((ok) => {
                forceReload(ok ? `${status} (cookies cleared)` : `${status} (cookie clear failed)`);
            });
            return;
        }

        // Tier 3 — still blocked after the reload attempts: the IP is
        // probably throttled, so reloading just earns more 403s. Long
        // exponential backoff (no reload), capped at COOLDOWN_MAX_MS.
        const over = newCount - SOFT_BLOCK_LIMIT - RELOAD_BACKOFF_THRESHOLD - 1;
        const delay = Math.min(COOLDOWN_BASE_MS * Math.pow(2, Math.max(0, over)), COOLDOWN_MAX_MS);
        saveCooldown(Date.now() + delay, newCount);
        log(`Waze ${status} #${newCount} — extended backoff ${Math.round(delay / 60000)}m`);
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
                    // Successful ingest → reset the consecutive-403 counter
                    // so the reload budget is fresh for the next bad streak.
                    try {
                        const { count } = loadCooldown();
                        if (count > 0) {
                            saveCooldown(0, 0);
                            log('clean ingest — reset block count');
                        }
                    } catch (e) {}
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

    // Earliest wallclock time we're allowed to pan again. Set when we
    // pan to the last region of a sweep — both timer paths (the normal
    // cadence and the ingest-arrived fast path) check this and stretch
    // their delay so neither fires before this timestamp. A flag-based
    // version (consumed-on-read) was racy: ingest arrival after the
    // last pan would consume the flag and bypass the pause.
    let earliestPanAt = 0;

    function rotateToNextRegion() {
        let idx = loadRegionIdx();
        const r = REGIONS[idx];
        const nextIdx = (idx + 1) % REGIONS.length;
        saveRegionIdx(nextIdx);
        // We just panned to REGIONS[idx]; if the next index has wrapped
        // back to 0 it means the region we just processed was the last
        // one in the sweep — set an absolute wait-until timestamp so
        // BOTH the cadence timer AND the ingest-arrived fast path
        // honour the pause, regardless of which one fires next.
        if (nextIdx === 0) {
            const pauseMs = ROUND_PAUSE_MIN_MS + Math.floor(Math.random() * ROUND_PAUSE_RANGE_MS);
            earliestPanAt = Date.now() + pauseMs;
            log(`end of round — pausing ${Math.round(pauseMs / 1000)}s before next sweep`);
        }

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

    function nextDelayMs(baseMs) {
        const jitter = Math.floor(Math.random() * PAN_INTERVAL_JITTER_MS);
        // Stretch the delay so we don't fire before earliestPanAt.
        // Survives any path that schedules the next pan: cadence timer,
        // ingest-arrived fast path, or post-ingest timer all converge
        // here. earliestPanAt = 0 (default) is in the past, so this
        // adds nothing during normal sweeps.
        const earliestExtra = Math.max(0, earliestPanAt - Date.now() - baseMs);
        return baseMs + jitter + earliestExtra;
    }

    function scheduleNextPan() {
        clearTimeout(rotationTimer);
        clearTimeout(ingestWaitTimer);
        ingestArmedForCurrent = true;
        rotationTimer = setTimeout(() => {
            ingestArmedForCurrent = false;
            // Skip the pan during cooldown — every request just earns
            // another 403 and resets the cookie clock.
            if (isInCooldown()) {
                log(`cooldown active, skipping pan (${Math.round(cooldownRemainingMs() / 60000)}m left)`);
                scheduleNextPan();
                return;
            }
            rotateToNextRegion();
            scheduleNextPan();
        }, nextDelayMs(PAN_INTERVAL_MS));
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
        }, nextDelayMs(PAN_AFTER_INGEST_MS));
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
        log('NSWPSN Waze Forwarder v1.25 loaded — backend:', BACKEND_URL,
            `· pan ${PAN_INTERVAL_MS / 1000}s+jitter`,
            `· auto-reload ${RELOAD_INTERVAL_MS / 60000}m`,
            `· watchdog ${STUCK_RELOAD_AFTER_MS / 60000}m`,
            `· 403/429 cooldown ${COOLDOWN_BASE_MS / 60000}-${COOLDOWN_MAX_MS / 60000}m`,
            _cd.until > 0
                ? `· COOLDOWN ACTIVE ${_remMin}m left (count ${_cd.count})`
                : '· no active cooldown');
    }
})();
