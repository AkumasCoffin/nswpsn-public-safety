// ==UserScript==
// @name         NSWPSN Waze Forwarder (Manual)
// @namespace    nswpsn.forcequit.xyz
// @version      1.0
// @description  Passive companion to the auto-rotating NSWPSN Waze Forwarder. Hooks Waze's georss fetch/XHR responses and forwards them to the NSWPSN backend, but does NOT rotate the map — the operator pans around themselves. Use this in your normal browser; it contributes whatever you happen to be viewing on Waze to the NSWPSN data pool.
// @match        https://www.waze.com/*
// @match        https://*.waze.com/*
// @grant        GM_xmlhttpRequest
// @grant        GM.xmlHttpRequest
// @grant        unsafeWindow
// @connect      *
// @run-at       document-start
// @updateURL    https://raw.githubusercontent.com/AkumasCoffin/nswpsn-public-safety/main/docs/waze-userscript-manual.user.js
// @downloadURL  https://raw.githubusercontent.com/AkumasCoffin/nswpsn-public-safety/main/docs/waze-userscript-manual.user.js
// ==/UserScript==

(function () {
    'use strict';

    // ====== CONFIG ======
    const BACKEND_URL = 'https://api.forcequit.xyz/api/waze/ingest';
    const INGEST_KEY  = 'REPLACE_WITH_YOUR_WAZE_INGEST_KEY';

    // Optional NSW geofence — drop ingests whose bbox center sits outside
    // this rectangle. Saves the backend from processing data the user
    // browsed in QLD/VIC/etc. Set to null to disable the geofence and
    // forward whatever you're viewing.
    const NSW_BBOX = {
        // Roughly all of NSW + ACT, with a generous border.
        lat_min: -37.6, lat_max: -28.0,
        lng_min: 140.9, lng_max: 154.0,
    };

    const log = (...args) => console.log('[NSWPSN-manual]', ...args);

    // ====== CROSS-WORLD ACCESS ======
    const pageWin = (typeof unsafeWindow !== 'undefined') ? unsafeWindow : window;
    const _exportFn = (typeof exportFunction === 'function') ? exportFunction : null;
    const exportedOrDirect = (fn, target) => {
        try { return _exportFn ? _exportFn(fn, target) : fn; }
        catch (e) { return fn; }
    };

    // ====== INTERCEPT + FORWARD ======
    function inGeofence(bbox) {
        if (!NSW_BBOX) return true;
        const cLat = (bbox.top + bbox.bottom) / 2;
        const cLng = (bbox.left + bbox.right) / 2;
        return cLat >= NSW_BBOX.lat_min && cLat <= NSW_BBOX.lat_max
            && cLng >= NSW_BBOX.lng_min && cLng <= NSW_BBOX.lng_max;
    }

    function handleGeorss(urlStr, data) {
        try {
            const u = new URL(urlStr, location.origin);
            const bbox = {
                top:    parseFloat(u.searchParams.get('top')),
                bottom: parseFloat(u.searchParams.get('bottom')),
                left:   parseFloat(u.searchParams.get('left')),
                right:  parseFloat(u.searchParams.get('right')),
            };
            if ([bbox.top, bbox.bottom, bbox.left, bbox.right].some(isNaN)) return;
            if (!inGeofence(bbox)) {
                log('skip (outside NSW)', bbox);
                return;
            }
            forward({
                bbox,
                alerts: data.alerts || [],
                jams:   data.jams   || [],
                users:  data.users  || [],
            });
        } catch (e) { log('parse err', e); }
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
                    log('ingest OK', payload.alerts.length + 'a', payload.jams.length + 'j');
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
                    if (urlStr && urlStr.indexOf('/api/georss') !== -1 && response.ok) {
                        response.clone().json().then(d => handleGeorss(urlStr, d)).catch(() => {});
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
                            if (self.readyState === 4 && self.status === 200 && self.responseText) {
                                handleGeorss(String(url), JSON.parse(self.responseText));
                            }
                        } catch (e) {}
                    });
                }
            } catch (e) {}
            return origSend.apply(this, arguments);
        }, pageWin);
        log('XHR hook installed');
    } catch (e) { log('XHR hook fail:', e); }

    log('NSWPSN Waze Forwarder (Manual) v1.0 loaded — backend:', BACKEND_URL);
    log('No auto-rotation. Pan around the live-map yourself; whatever you view gets forwarded.');
})();
