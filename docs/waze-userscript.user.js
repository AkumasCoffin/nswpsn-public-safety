// ==UserScript==
// @name         NSWPSN Waze Forwarder
// @namespace    nswpsn.forcequit.xyz
// @version      1.17
// @description  Intercept Waze live-map georss responses (via fetch + XHR hooks) in a real user's browser and forward them to the NSWPSN backend. Rotates through NSW regions by finding Waze's map instance and calling its pan/setView API. Does NOT use URL navigation as a fallback because Waze interprets ?ll= URLs as "drop a pin" destinations.
// @match        https://www.waze.com/*
// @match        https://*.waze.com/*
// @grant        GM_xmlhttpRequest
// @grant        GM.xmlHttpRequest
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

    // Regions rotated through. All at zoom 14 (street-level detail) — small
    // bbox per tile (~1.5 × 1 km), ensures Waze returns all alerts in view
    // without hitting the ~200 alert cap. Dense coverage of Sydney metro +
    // all major NSW regional centres. ~70 tiles total.
    const REGIONS = [
        // ---- Sydney CBD + Inner ring ----
        { name: 'Sydney CBD',              lat: -33.870, lon: 151.210, zoom: 14 },
        { name: 'Sydney Harbour',          lat: -33.855, lon: 151.225, zoom: 14 },
        { name: 'Pyrmont / Glebe',         lat: -33.870, lon: 151.190, zoom: 14 },
        { name: 'Surry Hills / Redfern',   lat: -33.895, lon: 151.210, zoom: 14 },
        { name: 'Newtown / Erskineville',  lat: -33.900, lon: 151.175, zoom: 14 },
        { name: 'Ultimo / Haymarket',      lat: -33.881, lon: 151.200, zoom: 14 },
        // ---- Sydney Eastern Suburbs ----
        { name: 'Eastern Suburbs',         lat: -33.895, lon: 151.260, zoom: 14 },
        { name: 'Bondi / Randwick',        lat: -33.925, lon: 151.250, zoom: 14 },
        { name: 'Bondi Junction / Double Bay', lat: -33.890, lon: 151.248, zoom: 14 },
        { name: 'Rose Bay / Vaucluse',     lat: -33.870, lon: 151.270, zoom: 14 },
        { name: 'Maroubra / Malabar',      lat: -33.950, lon: 151.245, zoom: 14 },
        // ---- Sydney North Shore ----
        { name: 'North Sydney',            lat: -33.840, lon: 151.210, zoom: 14 },
        { name: 'Mosman / Neutral Bay',    lat: -33.830, lon: 151.240, zoom: 14 },
        { name: 'Chatswood / St Leonards', lat: -33.800, lon: 151.185, zoom: 14 },
        { name: 'Hornsby / Waitara',       lat: -33.700, lon: 151.100, zoom: 14 },
        { name: 'Lane Cove / Artarmon',    lat: -33.815, lon: 151.165, zoom: 14 },
        { name: 'Gordon / Pymble',         lat: -33.755, lon: 151.155, zoom: 14 },
        // ---- Sydney Northern Beaches ----
        { name: 'Manly / Balgowlah',       lat: -33.800, lon: 151.285, zoom: 14 },
        { name: 'Dee Why / Brookvale',     lat: -33.750, lon: 151.290, zoom: 14 },
        { name: 'Mona Vale / Narrabeen',   lat: -33.685, lon: 151.305, zoom: 14 },
        { name: 'Avalon / Palm Beach',     lat: -33.630, lon: 151.325, zoom: 14 },
        // ---- Sydney Inner West ----
        { name: 'Inner West',              lat: -33.890, lon: 151.140, zoom: 14 },
        { name: 'Marrickville / Dulwich Hill', lat: -33.915, lon: 151.150, zoom: 14 },
        { name: 'Ashfield / Strathfield',  lat: -33.875, lon: 151.095, zoom: 14 },
        { name: 'Olympic Park / Homebush', lat: -33.845, lon: 151.065, zoom: 14 },
        // ---- Sydney South ----
        { name: 'Canterbury / Hurstville', lat: -33.960, lon: 151.100, zoom: 14 },
        { name: 'Rockdale / Kogarah',      lat: -33.955, lon: 151.140, zoom: 14 },
        { name: 'Sutherland Shire',        lat: -34.030, lon: 151.080, zoom: 14 },
        { name: 'Cronulla / Miranda',      lat: -34.050, lon: 151.150, zoom: 14 },
        { name: 'Airport / Mascot',        lat: -33.935, lon: 151.180, zoom: 14 },
        // ---- Sydney West ----
        { name: 'Parramatta',              lat: -33.815, lon: 151.000, zoom: 14 },
        { name: 'Blacktown',               lat: -33.770, lon: 150.910, zoom: 14 },
        { name: 'Mount Druitt / St Marys', lat: -33.770, lon: 150.810, zoom: 14 },
        { name: 'Ryde / Epping',           lat: -33.805, lon: 151.105, zoom: 14 },
        { name: 'Carlingford / North Rocks', lat: -33.780, lon: 151.050, zoom: 14 },
        { name: 'Liverpool / Bankstown',   lat: -33.920, lon: 150.930, zoom: 14 },
        { name: 'Cabramatta / Fairfield',  lat: -33.895, lon: 150.955, zoom: 14 },
        { name: 'Penrith',                 lat: -33.751, lon: 150.695, zoom: 14 },
        { name: 'Campbelltown',            lat: -34.070, lon: 150.830, zoom: 14 },
        { name: 'Camden / Narellan',       lat: -34.050, lon: 150.700, zoom: 14 },
        // ---- Sydney Hills District ----
        { name: 'Castle Hill / Baulkham Hills', lat: -33.735, lon: 150.985, zoom: 14 },
        { name: 'Rouse Hill / Kellyville', lat: -33.685, lon: 150.920, zoom: 14 },
        { name: 'Windsor / Richmond',      lat: -33.610, lon: 150.755, zoom: 14 },
        // ---- Blue Mountains + Hawkesbury ----
        { name: 'Katoomba / Leura',        lat: -33.715, lon: 150.310, zoom: 14 },
        { name: 'Springwood / Glenbrook',  lat: -33.700, lon: 150.575, zoom: 14 },
        // ---- Central Coast ----
        { name: 'Woy Woy / Ettalong',      lat: -33.488, lon: 151.330, zoom: 14 },
        { name: 'Gosford',                 lat: -33.425, lon: 151.345, zoom: 14 },
        { name: 'Erina / Wamberal',        lat: -33.430, lon: 151.395, zoom: 14 },
        { name: 'Avoca Beach',             lat: -33.470, lon: 151.435, zoom: 14 },
        { name: 'Terrigal / The Entrance', lat: -33.355, lon: 151.480, zoom: 14 },
        { name: 'Wyong / Tuggerah',        lat: -33.280, lon: 151.425, zoom: 14 },
        { name: 'Toukley / Budgewoi',      lat: -33.260, lon: 151.540, zoom: 14 },
        // ---- Newcastle / Hunter ----
        { name: 'Newcastle CBD',           lat: -32.925, lon: 151.780, zoom: 14 },
        { name: 'Newcastle West / Wallsend', lat: -32.900, lon: 151.720, zoom: 14 },
        { name: 'Charlestown',             lat: -32.965, lon: 151.690, zoom: 14 },
        { name: 'Belmont / Swansea',       lat: -33.030, lon: 151.665, zoom: 14 },
        { name: 'Toronto / Lake Macquarie West', lat: -33.020, lon: 151.595, zoom: 14 },
        { name: 'Lake Macquarie',          lat: -33.050, lon: 151.630, zoom: 14 },
        { name: 'Maitland',                lat: -32.733, lon: 151.555, zoom: 14 },
        { name: 'East Maitland / Thornton', lat: -32.745, lon: 151.605, zoom: 14 },
        { name: 'Kurri Kurri',             lat: -32.815, lon: 151.480, zoom: 14 },
        { name: 'Cessnock',                lat: -32.833, lon: 151.355, zoom: 14 },
        { name: 'Singleton',               lat: -32.568, lon: 151.170, zoom: 14 },
        { name: 'Muswellbrook',            lat: -32.265, lon: 150.890, zoom: 14 },
        { name: 'Scone',                   lat: -32.058, lon: 150.865, zoom: 14 },
        // ---- Illawarra / South Coast ----
        // Densified: each regional centre split into CBD + surrounding suburbs.
        { name: 'Helensburgh / Stanwell',  lat: -34.180, lon: 150.985, zoom: 14 },
        { name: 'Bulli / Thirroul',        lat: -34.330, lon: 150.920, zoom: 14 },
        { name: 'Corrimal / Fairy Meadow', lat: -34.395, lon: 150.890, zoom: 14 },
        { name: 'Wollongong',              lat: -34.430, lon: 150.880, zoom: 14 },
        { name: 'Figtree / Mount Keira',   lat: -34.435, lon: 150.860, zoom: 14 },
        { name: 'Port Kembla',             lat: -34.475, lon: 150.905, zoom: 14 },
        { name: 'Lake Heights / Warrawong', lat: -34.490, lon: 150.880, zoom: 14 },
        { name: 'Dapto',                   lat: -34.495, lon: 150.795, zoom: 14 },
        { name: 'Albion Park',             lat: -34.575, lon: 150.785, zoom: 14 },
        { name: 'Shellharbour / Kiama',    lat: -34.630, lon: 150.830, zoom: 14 },
        { name: 'Shellharbour Square',     lat: -34.580, lon: 150.860, zoom: 14 },
        { name: 'Berry / Gerringong',      lat: -34.760, lon: 150.700, zoom: 14 },
        { name: 'Bomaderry / North Nowra', lat: -34.857, lon: 150.610, zoom: 14 },
        { name: 'Nowra / Bomaderry',       lat: -34.880, lon: 150.605, zoom: 14 },
        { name: 'South Nowra',             lat: -34.910, lon: 150.595, zoom: 14 },
        { name: 'Worrigee / Falls Creek',  lat: -34.930, lon: 150.625, zoom: 14 },
        { name: 'Vincentia / Huskisson',   lat: -35.040, lon: 150.685, zoom: 14 },
        { name: 'Sussex Inlet / St Georges Basin', lat: -35.155, lon: 150.605, zoom: 14 },
        { name: 'Ulladulla',               lat: -35.360, lon: 150.475, zoom: 14 },
        { name: 'Mollymook / Milton',      lat: -35.330, lon: 150.470, zoom: 14 },
        { name: 'Batemans Bay',            lat: -35.710, lon: 150.180, zoom: 14 },
        { name: 'Moruya',                  lat: -35.910, lon: 150.080, zoom: 14 },
        { name: 'Narooma',                 lat: -36.215, lon: 150.135, zoom: 14 },
        { name: 'Merimbula / Bega',        lat: -36.680, lon: 149.830, zoom: 14 },
        { name: 'Eden',                    lat: -37.062, lon: 149.901, zoom: 14 },
        // ---- Southern Highlands ----
        { name: 'Bowral / Mittagong',      lat: -34.485, lon: 150.415, zoom: 14 },
        { name: 'Moss Vale / Goulburn',    lat: -34.755, lon: 149.720, zoom: 14 },
        // ---- ACT region ----
        { name: 'Canberra / ACT',          lat: -35.280, lon: 149.130, zoom: 14 },
        { name: 'Canberra North',          lat: -35.240, lon: 149.130, zoom: 14 },
        { name: 'Canberra South',          lat: -35.345, lon: 149.095, zoom: 14 },
        { name: 'Belconnen',               lat: -35.235, lon: 149.067, zoom: 14 },
        { name: 'Tuggeranong',             lat: -35.420, lon: 149.075, zoom: 14 },
        { name: 'Gungahlin',               lat: -35.185, lon: 149.130, zoom: 14 },
        { name: 'Queanbeyan',              lat: -35.355, lon: 149.235, zoom: 14 },
        { name: 'Yass',                    lat: -34.845, lon: 148.915, zoom: 14 },
        { name: 'Murrumbateman',           lat: -34.972, lon: 149.030, zoom: 14 },
        // ---- Snowy Mountains ----
        { name: 'Cooma',                   lat: -36.235, lon: 149.130, zoom: 14 },
        { name: 'Jindabyne',               lat: -36.415, lon: 148.625, zoom: 14 },
        // ---- Central West ----
        { name: 'Bathurst',                lat: -33.420, lon: 149.580, zoom: 14 },
        { name: 'Kelso / Eglinton',        lat: -33.405, lon: 149.620, zoom: 14 },
        { name: 'Orange',                  lat: -33.285, lon: 149.100, zoom: 14 },
        { name: 'Orange North',            lat: -33.265, lon: 149.090, zoom: 14 },
        { name: 'Lithgow',                 lat: -33.485, lon: 150.155, zoom: 14 },
        { name: 'Portland / Wallerawang',  lat: -33.380, lon: 150.050, zoom: 14 },
        { name: 'Mudgee',                  lat: -32.593, lon: 149.585, zoom: 14 },
        { name: 'Parkes',                  lat: -33.135, lon: 148.175, zoom: 14 },
        { name: 'Forbes',                  lat: -33.385, lon: 148.010, zoom: 14 },
        { name: 'Cowra',                   lat: -33.835, lon: 148.685, zoom: 14 },
        { name: 'Young',                   lat: -34.315, lon: 148.300, zoom: 14 },
        { name: 'Dubbo',                   lat: -32.250, lon: 148.600, zoom: 14 },
        { name: 'Dubbo West',              lat: -32.245, lon: 148.580, zoom: 14 },
        // ---- Riverina ----
        { name: 'Wagga Wagga',             lat: -35.115, lon: 147.370, zoom: 14 },
        { name: 'Wagga South / Glenfield Park', lat: -35.140, lon: 147.355, zoom: 14 },
        { name: 'Wagga North / Estella',   lat: -35.080, lon: 147.358, zoom: 14 },
        { name: 'Junee',                   lat: -34.870, lon: 147.585, zoom: 14 },
        { name: 'Albury',                  lat: -36.080, lon: 146.915, zoom: 14 },
        { name: 'Lavington',               lat: -36.040, lon: 146.945, zoom: 14 },
        { name: 'Griffith',                lat: -34.290, lon: 146.045, zoom: 14 },
        { name: 'Leeton',                  lat: -34.555, lon: 146.405, zoom: 14 },
        { name: 'Deniliquin',              lat: -35.535, lon: 144.960, zoom: 14 },
        { name: 'Finley',                  lat: -35.640, lon: 145.575, zoom: 14 },
        { name: 'Tumut',                   lat: -35.305, lon: 148.225, zoom: 14 },
        { name: 'Tumbarumba',              lat: -35.778, lon: 148.011, zoom: 14 },
        { name: 'Cootamundra',             lat: -34.640, lon: 148.030, zoom: 14 },
        { name: 'Temora',                  lat: -34.450, lon: 147.535, zoom: 14 },
        { name: 'Gundagai',                lat: -35.065, lon: 148.100, zoom: 14 },
        // ---- North Coast ----
        { name: 'Bulahdelah',              lat: -32.404, lon: 152.215, zoom: 14 },
        { name: 'Taree',                   lat: -31.905, lon: 152.460, zoom: 14 },
        { name: 'Forster / Tuncurry',      lat: -32.180, lon: 152.510, zoom: 14 },
        { name: 'Wingham',                 lat: -31.870, lon: 152.367, zoom: 14 },
        { name: 'Port Macquarie',          lat: -31.430, lon: 152.900, zoom: 14 },
        { name: 'Port Macquarie West',     lat: -31.450, lon: 152.860, zoom: 14 },
        { name: 'Wauchope',                lat: -31.460, lon: 152.730, zoom: 14 },
        { name: 'Laurieton / Camden Haven', lat: -31.640, lon: 152.815, zoom: 14 },
        { name: 'Kempsey',                 lat: -31.080, lon: 152.835, zoom: 14 },
        { name: 'South West Rocks',        lat: -30.895, lon: 153.040, zoom: 14 },
        { name: 'Nambucca Heads',          lat: -30.640, lon: 152.985, zoom: 14 },
        { name: 'Macksville',              lat: -30.710, lon: 152.918, zoom: 14 },
        { name: 'Coffs Harbour',           lat: -30.300, lon: 153.120, zoom: 14 },
        { name: 'Coffs Harbour South / Sawtell', lat: -30.365, lon: 153.100, zoom: 14 },
        { name: 'Woolgoolga',              lat: -30.110, lon: 153.200, zoom: 14 },
        { name: 'Grafton',                 lat: -29.695, lon: 152.935, zoom: 14 },
        { name: 'Yamba / Maclean',         lat: -29.435, lon: 153.355, zoom: 14 },
        { name: 'Casino',                  lat: -28.865, lon: 153.050, zoom: 14 },
        { name: 'Lismore',                 lat: -28.810, lon: 153.280, zoom: 14 },
        { name: 'Goonellabah',             lat: -28.815, lon: 153.305, zoom: 14 },
        { name: 'Ballina',                 lat: -28.865, lon: 153.565, zoom: 14 },
        { name: 'Lennox Head',             lat: -28.787, lon: 153.585, zoom: 14 },
        { name: 'Byron Bay',               lat: -28.645, lon: 153.615, zoom: 14 },
        { name: 'Mullumbimby',             lat: -28.555, lon: 153.500, zoom: 14 },
        { name: 'Tweed Heads',             lat: -28.180, lon: 153.545, zoom: 14 },
        { name: 'Kingscliff / Pottsville', lat: -28.255, lon: 153.572, zoom: 14 },
        { name: 'Murwillumbah',            lat: -28.330, lon: 153.390, zoom: 14 },
        // ---- New England / North West ----
        { name: 'Tamworth',                lat: -31.090, lon: 150.930, zoom: 14 },
        { name: 'Tamworth South / Hillvue', lat: -31.115, lon: 150.910, zoom: 14 },
        { name: 'Tamworth West',           lat: -31.090, lon: 150.890, zoom: 14 },
        { name: 'Armidale',                lat: -30.510, lon: 151.665, zoom: 14 },
        { name: 'Uralla',                  lat: -30.638, lon: 151.495, zoom: 14 },
        { name: 'Gunnedah',                lat: -30.980, lon: 150.250, zoom: 14 },
        { name: 'Narrabri',                lat: -30.325, lon: 149.785, zoom: 14 },
        { name: 'Moree',                   lat: -29.465, lon: 149.840, zoom: 14 },
        { name: 'Inverell',                lat: -29.775, lon: 151.110, zoom: 14 },
        { name: 'Glen Innes',              lat: -29.735, lon: 151.740, zoom: 14 },
        { name: 'Tenterfield',             lat: -29.050, lon: 152.020, zoom: 14 },
        // ---- Western NSW (Central West → Outback) ----
        { name: 'Narromine',               lat: -32.230, lon: 148.245, zoom: 14 },
        { name: 'Warren',                  lat: -31.700, lon: 147.835, zoom: 14 },
        { name: 'Coonamble',               lat: -30.955, lon: 148.390, zoom: 14 },
        { name: 'Gilgandra',               lat: -31.710, lon: 148.665, zoom: 14 },
        { name: 'Condobolin',              lat: -33.085, lon: 147.150, zoom: 14 },
        { name: 'West Wyalong',            lat: -33.925, lon: 147.225, zoom: 14 },
        { name: 'Lake Cargelligo',         lat: -33.305, lon: 146.375, zoom: 14 },
        // ---- Far West / Outback ----
        { name: 'Cobar',                   lat: -31.498, lon: 145.835, zoom: 14 },
        { name: 'Nyngan',                  lat: -31.559, lon: 147.195, zoom: 14 },
        { name: 'Bourke',                  lat: -30.091, lon: 145.935, zoom: 14 },
        { name: 'Walgett',                 lat: -30.025, lon: 148.120, zoom: 14 },
        { name: 'Lightning Ridge',         lat: -29.430, lon: 147.975, zoom: 14 },
        { name: 'Brewarrina',              lat: -29.965, lon: 146.870, zoom: 14 },
        { name: 'Hillston',                lat: -33.485, lon: 145.535, zoom: 14 },
        { name: 'Hay',                     lat: -34.510, lon: 144.840, zoom: 14 },
        { name: 'Balranald',               lat: -34.640, lon: 143.560, zoom: 14 },
        { name: 'Wentworth',               lat: -34.110, lon: 141.915, zoom: 14 },
        { name: 'Wilcannia',               lat: -31.560, lon: 143.380, zoom: 14 },
        { name: 'Menindee',                lat: -32.395, lon: 142.420, zoom: 14 },
        { name: 'Broken Hill',             lat: -31.955, lon: 141.465, zoom: 14 },
        { name: 'Tibooburra',              lat: -29.435, lon: 142.015, zoom: 14 },
    ];
    // After panning, wait up to this long for Waze to fire georss for the
    // new viewport. As soon as a georss response arrives we accelerate the
    // next pan — no point waiting if the data is already in.
    const PAN_INTERVAL_MS        = 5_000;  // fallback max wait per region (dead zones)
    const PAN_AFTER_INGEST_MS    = 1_000;  // how long to wait after georss before panning
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
        rotationTimer = setTimeout(() => {
            ingestArmedForCurrent = false;
            rotateToNextRegion();
            scheduleNextPan();
        }, PAN_INTERVAL_MS);
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
        // Wait ~15s for Waze's map JS to initialise before first pan attempt
        setTimeout(() => {
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
    setTimeout(() => forceReload(`scheduled ${RELOAD_INTERVAL_MS / 60000}m`),
        RELOAD_INTERVAL_MS);

    // Watchdog — runs on every check tick AND every visibility change.
    // The visibility hook is the important one: backgrounded tabs get
    // setInterval throttled to 1/min and sometimes stalled entirely, so
    // when the user comes back to check on the tab we re-evaluate state
    // immediately instead of waiting for the next throttled tick.
    function checkStuck() {
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

    log('NSWPSN Waze Forwarder v1.17 loaded — backend:', BACKEND_URL,
        `· auto-reload ${RELOAD_INTERVAL_MS / 60000}m`,
        `· watchdog ${STUCK_RELOAD_AFTER_MS / 60000}m`,
        `· check every ${STUCK_CHECK_INTERVAL_MS / 1000}s + on visibility`);
})();
