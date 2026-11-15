/* Live dashboard (live.html).
 *
 * One fetch per feed into one store; the tiles, lists and trends all draw from
 * it, so two numbers for the same thing cannot disagree. Every source carries
 * a state (loading, live, stale, unavailable) and a failed source is shown as
 * failed, never as zero or as an all-clear.
 *
 * Nothing here uses inline handlers: one delegated listener reads data-act.
 */
(function () {
  'use strict';

  const API = 'https://api.forcequit.xyz';
  const STATES = ['ALL', 'NSW', 'VIC', 'QLD', 'SA', 'WA', 'ACT', 'NT'];
  // Only for reading the few feeds that still send a Sydney wall-clock time
  // with no zone. Times are SHOWN in the reader's own zone: the page covers
  // five time zones, and "6:12 pm" in Sydney time is wrong on a phone in Perth.
  const TZ = 'Australia/Sydney';

  // ---------------------------------------------------------------- helpers

  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const num = (n) => (Number(n) || 0).toLocaleString('en-AU');
  const plural = (n, one, many) => `${num(n)} ${n === 1 ? one : (many || one + 's')}`;

  // Feed text sometimes arrives with HTML entities already in it. A textarea
  // decodes them without parsing markup; the result is escaped again on output.
  const _ta = document.createElement('textarea');
  function plain(s) {
    if (s == null) return '';
    _ta.innerHTML = String(s).replace(/<[^>]*>/g, ' ');
    return _ta.value.replace(/\s+/g, ' ').trim();
  }

  function httpUrl(u) {
    try {
      const p = new URL(String(u || ''), location.href);
      return (p.protocol === 'http:' || p.protocol === 'https:') ? p.href : '';
    } catch (_e) { return ''; }
  }

  /**
   * Milliseconds for any time the feeds send, or null.
   * - numbers and digit strings: unix seconds or milliseconds (Live Traffic
   *   sends milliseconds as a string);
   * - "DD/MM/YYYY HH:mm[:ss]" (Essential Energy), read as Sydney wall time;
   * - anything Date can read (ISO, RFC 822).
   */
  function toMs(v) {
    if (v == null || v === '') return null;
    if (typeof v === 'number' || /^\d{9,14}$/.test(String(v).trim())) {
      const n = Number(v);
      if (!Number.isFinite(n) || n <= 0) return null;
      return n < 1e11 ? n * 1000 : n;
    }
    const s = String(v).trim();
    const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})[ ,T]+(\d{1,2}):(\d{2})(?::(\d{2}))?/.exec(s);
    if (m) return sydneyWall(+m[3], +m[2], +m[1], +m[4], +m[5], +(m[6] || 0));
    const t = Date.parse(s);
    return Number.isNaN(t) ? null : t;
  }

  // A Sydney wall-clock time as an instant: guess UTC, see what Sydney calls
  // that instant, and correct by the difference (handles daylight saving).
  function sydneyWall(y, mo, d, h, mi, s) {
    const guess = Date.UTC(y, mo - 1, d, h, mi, s);
    const parts = {};
    for (const p of new Intl.DateTimeFormat('en-AU', {
      timeZone: TZ, hourCycle: 'h23', year: 'numeric', month: 'numeric', day: 'numeric',
      hour: 'numeric', minute: 'numeric', second: 'numeric',
    }).formatToParts(new Date(guess))) parts[p.type] = Number(p.value);
    const seen = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
    return guess - (seen - guess);
  }

  const fmtClock = new Intl.DateTimeFormat('en-AU', { hour: 'numeric', minute: '2-digit' });
  const fmtDay = new Intl.DateTimeFormat('en-AU', { weekday: 'short', day: 'numeric', month: 'short' });
  const fmtDayKey = new Intl.DateTimeFormat('en-CA', { year: 'numeric', month: '2-digit', day: '2-digit' });
  const clock = (ms) => fmtClock.format(new Date(ms)).replace(/\s/g, ' ').toLowerCase();

  /** "6:12 pm" today, "Fri 13 Nov, 6:12 pm" otherwise. The reader's own time zone. */
  function when(ms) {
    if (ms == null) return '';
    const sameDay = fmtDayKey.format(new Date(ms)) === fmtDayKey.format(new Date());
    return sameDay ? clock(ms) : `${fmtDay.format(new Date(ms))}, ${clock(ms)}`;
  }

  /** "5 min ago", "3 h ago", or the date once it is more than a day old. */
  function ago(ms) {
    if (ms == null) return '';
    const d = Date.now() - ms;
    if (d < 0) return when(ms);
    if (d < 90_000) return 'just now';
    if (d < 3_600_000) return `${Math.round(d / 60_000)} min ago`;
    if (d < 86_400_000) return `${Math.round(d / 3_600_000)} h ago`;
    return when(ms);
  }

  const timeTag = (ms, text) => (ms == null ? '' : `<time datetime="${new Date(ms).toISOString()}">${esc(text)}</time>`);

  // "SCHEDULED ROADWORK" and "scheduled_roadwork" both read "Scheduled roadwork".
  function tidy(s) {
    const t = plain(s).replace(/_/g, ' ');
    if (!t || t.toLowerCase() === 'null' || t.toLowerCase() === 'none' || t.toLowerCase() === 'unknown') return '';
    // All-capitals words of up to three letters are acronyms (MVC, AFA) and stay as they are.
    if (t === t.toUpperCase() && !/\s/.test(t) && t.length <= 3) return t;
    return (t === t.toUpperCase() || t === t.toLowerCase()) ? t.charAt(0).toUpperCase() + t.slice(1).toLowerCase() : t;
  }

  function pointOf(f) {
    const g = f && f.geometry;
    if (g && g.type === 'Point' && Array.isArray(g.coordinates)) {
      const [lon, lat] = g.coordinates;
      if (Number.isFinite(lat) && Number.isFinite(lon) && (lat || lon)) return { lat, lon };
    }
    return null;
  }
  const mapLink = (pt) => (pt ? `<a href="map?lat=${pt.lat.toFixed(5)}&amp;lng=${pt.lon.toFixed(5)}&amp;zoom=13">Map</a>` : '');
  const pill = (text, cls) => (text ? `<span class="lv-pill ${cls || ''}">${esc(text)}</span>` : '');
  const statePill = (st) => (st ? `<span class="lv-pill state">${esc(st)}</span>` : '');

  // ------------------------------------------------------------------ store

  /**
   * Every feed the page reads. `every` is seconds between refreshes; `states`
   * is where the feed has anything to say (null = everywhere).
   */
  const SOURCES = {
    fires: { label: 'Fire services (all states)', url: '/api/v1/fires', every: 120 },
    outages: { label: 'Power outages (all distributors)', url: '/api/v1/outages', every: 120 },
    roadIncidents: { label: 'Road incidents', url: '/api/v1/traffic/incidents', every: 120 },
    roadFloods: { label: 'Flooded roads', url: '/api/v1/traffic/flood', every: 180 },
    roadFires: { label: 'Roads closed by fire (NSW)', url: '/api/v1/traffic/fire', every: 180 },
    roadEvents: { label: 'Major road events', url: '/api/v1/traffic/majorevent', every: 300 },
    roadwork: { label: 'Roadwork', url: '/api/v1/traffic/roadwork', every: 300 },
    bom: { label: 'Weather warnings (NSW)', url: '/api/v1/bom/warnings', every: 300 },
    fdr: { label: 'Fire danger ratings (NSW)', url: '/api/v1/rfs/fdr', every: 900 },
    summary: { label: 'Radio summary', url: '/api/v1/summaries/latest', every: 300 },
    aircraft: { label: 'Aircraft', url: '/api/v1/adsb/aircraft', every: 120 },
    pager: { label: 'Pager messages (NSW)', url: '/api/v1/pager/hits?hours=3&limit=60', every: 120 },
    transport: { label: 'Transport alerts (NSW)', url: '/api/v1/transport/alerts', every: 300 },
    beachwatch: { label: 'Beach water quality (NSW)', url: '/api/v1/beachwatch', every: 900 },
    beachsafe: { label: 'Beach patrols (NSW)', url: '/api/v1/beachsafe', every: 900 },
    news: { label: 'News', url: '/api/news/rss?limit=10', every: 300 },
    act: { label: 'ACT Ambulance', url: '/api/v1/act-ambulance/incidents', every: 300, optIn: true },
    history: { label: 'Trends', url: null, every: 300 },
  };

  const store = {};
  for (const k of Object.keys(SOURCES)) store[k] = { status: 'idle', data: null, at: 0, tried: 0, busy: false, error: '' };

  const ui = {
    state: 'ALL',
    open: {},          // list id -> expanded
    tab: { fires: 'fires', news: 'all' },
    trend: { metric: 'fires', period: '6h' },
    actOn: false,
    sourcesOpen: false,
  };

  async function getJson(url) {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 25_000);
    try {
      const res = await window.AusApi.fetch(API + url, { signal: ctl.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const j = await res.json();
      // /api/v1/ wraps everything that is not GeoJSON in `data`.
      return (j && j.data !== undefined && !j.features) ? j.data : j;
    } finally { clearTimeout(timer); }
  }

  const PERIODS = {
    '1h': { hours: 1, step: 0, label: 'Last hour' },
    '6h': { hours: 6, step: 0, label: 'Last 6 hours' },
    '24h': { hours: 24, step: 15, label: 'Last 24 hours' },
    '7d': { hours: 168, step: 60, label: 'Last 7 days' },
  };

  async function load(key) {
    const src = SOURCES[key], s = store[key];
    if (s.busy || (src.optIn && !ui.actOn)) return;
    s.busy = true; s.tried = Date.now();
    if (s.status === 'idle') s.status = 'loading';
    try {
      let url = src.url;
      if (key === 'history') {
        const p = PERIODS[ui.trend.period];
        url = `/api/stats/history?hours=${p.hours}${p.step ? `&step=${p.step}` : ''}`;
      }
      const data = await getJson(url);
      if (key === 'history' && !Array.isArray(data)) throw new Error('no history');
      s.data = data; s.at = Date.now(); s.status = 'ok'; s.error = '';
    } catch (e) {
      s.status = 'error';
      s.error = (e && e.name === 'AbortError') ? 'timed out' : String((e && e.message) || e);
    } finally {
      s.busy = false;
      paint(key);
    }
  }

  /** 'loading' | 'ok' | 'stale' (have data, could not refresh or it is old) | 'error' (nothing to show). */
  function stateOf(key) {
    const s = store[key];
    if (s.status === 'idle' || (s.status === 'loading' && !s.data)) return 'loading';
    if (!s.data) return 'error';
    if (s.status === 'error') return 'stale';
    return (Date.now() - s.at > SOURCES[key].every * 3000 + 60_000) ? 'stale' : 'ok';
  }
  const worst = (keys) => {
    const st = keys.map(stateOf);
    return st.includes('error') ? 'error' : st.includes('stale') ? 'stale' : st.includes('loading') ? 'loading' : 'ok';
  };
  const oldest = (keys) => Math.min(...keys.map((k) => store[k].at || Infinity));

  function chip(keys) {
    const st = worst(keys);
    const at = oldest(keys);
    const text = st === 'ok' ? `Live · ${clock(at)}`
      : st === 'stale' ? `Not refreshed since ${Number.isFinite(at) ? when(at) : 'load'}`
      : st === 'error' ? 'Unavailable' : 'Loading';
    return `<span class="lv-dot ${st === 'loading' ? '' : st}"></span>${esc(text)}`;
  }

  /** What to show above (stale) or instead of (error, loading) a section's body. '' when live. */
  function problem(keys, what) {
    const st = worst(keys);
    if (st === 'loading') return { block: true, html: `<p class="lv-empty">Loading ${esc(what)}…</p>` };
    if (st === 'error') {
      return { block: keys.every((k) => !store[k].data), html: `<p class="lv-problem">${esc(what.charAt(0).toUpperCase() + what.slice(1))} could not be loaded. This is a fault with the feed or with AusAware, not an all-clear. It will be tried again shortly.</p>` };
    }
    if (st === 'stale') {
      const at = oldest(keys);
      return { block: false, html: `<p class="lv-problem stale">Could not refresh ${esc(what)}. Showing what was last received${Number.isFinite(at) ? ` at ${esc(when(at))}` : ''}.</p>` };
    }
    return { block: false, html: '' };
  }

  // ------------------------------------------------------------- normalisers

  const inState = (st) => ui.state === 'ALL' || st === ui.state;
  const feats = (key) => (store[key].data && Array.isArray(store[key].data.features) ? store[key].data.features : []);

  const LEVEL_RANK = { emergency: 3, watch: 2, advice: 1, '': 0 };
  function levelOf(text) {
    const t = String(text || '').toLowerCase();
    if (t.includes('emergency')) return 'emergency';
    if (t.includes('watch')) return 'watch';
    if (t.includes('advice')) return 'advice';
    return '';
  }
  const LEVEL_LABEL = { emergency: 'Emergency Warning', watch: 'Watch and Act', advice: 'Advice' };

  function fires() {
    return feats('fires').map((f) => {
      const p = f.properties || {}, d = p.details || {};
      return {
        id: p.id, state: p.state || '', kind: p.kind, layer: p.layer,
        title: plain(p.title) || plain(p.location) || 'Unnamed incident',
        level: levelOf(p.alert_level), status: tidy(p.status), category: tidy(p.category),
        where: plain(d.councilArea) || (plain(p.location) !== plain(p.title) ? plain(p.location) : ''),
        size: plain(d.size), agency: plain(p.agency), updated: toMs(p.updated_at),
        link: httpUrl(d.link), pt: pointOf(f), active: p.is_active !== false,
      };
    }).filter((x) => x.active && inState(x.state));
  }

  // The distributors the feed lists, with the state each is in. Before the
  // feed carried a state they were all in NSW.
  const distributors = () => ((store.outages.data && store.outages.data.sources) || [])
    .map((s) => ({ id: s.distributor, name: s.name || s.distributor, state: s.state || 'NSW', read: !!s.updated_at }));
  const distributorName = (id) => (distributors().find((d) => d.id === id) || {}).name || tidy(id);
  // States with a distributor feed. Until the feed has answered, NSW (what it has always had).
  const powerStates = () => { const st = [...new Set(distributors().map((d) => d.state))]; return st.length ? st : ['NSW']; };

  function outages() {
    const seen = new Set();
    const out = [];
    for (const f of feats('outages')) {
      const p = f.properties || {}, d = p.details || {};
      const key = `${p.distributor}:${p.id}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        id: p.id, distributor: p.distributor, state: p.state || 'NSW', name: distributorName(p.distributor),
        planned: p.kind === 'planned', active: p.status === 'active',
        area: plain(p.area), streets: plain(d.streets), postcode: plain(d.postcode),
        customers: Number(p.customers_affected) || 0, cause: tidy(p.cause), crew: tidy(d.status),
        start: toMs(p.start_at), restore: toMs(p.restore_at), pt: pointOf(f),
      });
    }
    return out.filter((x) => inState(x.state));
  }

  const ROAD_KINDS = {
    incidents: ['roadIncidents'], closures: ['roadFloods', 'roadFires'], events: ['roadEvents'], roadwork: ['roadwork'],
  };
  function roads(kind) {
    const out = [];
    for (const key of ROAD_KINDS[kind]) {
      for (const f of feats(key)) {
        const p = f.properties || {};
        if (p.isEnded === true) continue;
        const cat = tidy(p.mainCategory) || tidy(p.incidentType) || tidy(p.type);
        out.push({
          id: p.id, state: p.state || 'NSW',
          title: plain(p.title) || plain(p.headline) || plain(p.displayName) || cat || 'Road incident',
          cat, roads: typeof p.roads === 'string' ? plain(p.roads) : '',
          advice: tidy(p.otherAdvice), planned: String(p.incidentKind || '').toLowerCase() === 'planned',
          major: p.isMajor === true, time: toMs(p.lastUpdated) || toMs(p.created), pt: pointOf(f),
        });
      }
    }
    return out.filter((x) => inState(x.state))
      .sort((a, b) => (b.major - a.major) || (a.planned - b.planned) || ((b.time || 0) - (a.time || 0)));
  }

  // ----------------------------------------------------------------- pieces

  function row({ cls, title, href, side, meta, text }) {
    const t = href ? `<a href="${esc(href)}" target="_blank" rel="noopener noreferrer">${esc(title)}</a>` : esc(title);
    const m = (meta || []).filter(Boolean);
    return `<li class="lv-row ${cls || ''}">
      <div class="lv-row-title">${t}</div>
      ${side && side.filter(Boolean).length ? `<div class="lv-row-side">${side.filter(Boolean).join('')}</div>` : ''}
      ${m.length ? `<div class="lv-row-meta">${m.map((x) => `<span>${x}</span>`).join('')}</div>` : ''}
      ${text ? `<div class="lv-row-text">${esc(text)}</div>` : ''}
    </li>`;
  }

  /** A list that shows `first` rows, then all (to `cap`) once opened. The open state survives refreshes. */
  function list(id, items, render, { first = 6, cap = 150, empty, noun = 'item', more } = {}) {
    if (!items.length) return `<p class="lv-empty">${esc(empty || 'Nothing to show.')}</p>`;
    const open = !!ui.open[id];
    const shown = items.slice(0, open ? cap : first);
    let html = `<ul class="lv-list">${shown.map(render).join('')}</ul>`;
    if (items.length > first) {
      html += `<button type="button" class="lv-more" data-act="more" data-arg="${esc(id)}" aria-expanded="${open}">${open ? 'Show fewer' : `Show all ${num(Math.min(items.length, cap))}`}</button>`;
    }
    if (open && items.length > cap) {
      html += `<p class="lv-note">Showing the first ${num(cap)} of ${plural(items.length, noun)}. ${more || 'The rest are on the <a href="map">map</a> and in the <a href="logs">logs</a>.'}</p>`;
    }
    return html;
  }

  const tabs = (group, defs) => `<div class="lv-tabs" role="group">${defs.map(([id, label, n]) =>
    `<button type="button" data-act="tab" data-arg="${group}:${id}" aria-pressed="${ui.tab[group] === id}">${esc(label)}${n == null ? '' : `<span>${num(n)}</span>`}</button>`).join('')}</div>`;
  const figs = (pairs) => `<p class="lv-figs">${pairs.filter(Boolean).map(([n, label]) => `<span><b>${typeof n === 'number' ? num(n) : esc(n)}</b>${esc(label)}</span>`).join('')}</p>`;

  function set(id, keys, html) {
    const body = $(`${id}-body`), st = $(`${id}-state`);
    if (st) st.innerHTML = chip(keys);
    if (body) body.innerHTML = html;
  }

  // --------------------------------------------------------------- sections

  function renderFires() {
    const pr = problem(['fires'], 'fire incidents');
    if (pr.block) return set('fires', ['fires'], pr.html);
    const all = fires();
    const byRank = (a, b) => (LEVEL_RANK[b.level] - LEVEL_RANK[a.level]) || ((b.updated || 0) - (a.updated || 0));
    const groups = {
      fires: all.filter((x) => x.kind !== 'warning' && x.layer === 'fire').sort(byRank),
      warnings: all.filter((x) => x.kind === 'warning').sort(byRank),
      other: all.filter((x) => x.kind !== 'warning' && x.layer !== 'fire').sort(byRank),
    };
    const lv = (l) => all.filter((x) => x.level === l).length;
    const cur = groups[ui.tab.fires] ? ui.tab.fires : 'fires';
    const html = pr.html
      + figs([[lv('emergency'), 'Emergency Warning'], [lv('watch'), 'Watch and Act'], [lv('advice'), 'Advice']])
      + tabs('fires', [['fires', 'Fires', groups.fires.length], ['warnings', 'Warnings', groups.warnings.length], ['other', 'Other incidents', groups.other.length]])
      + list(`fires-${cur}`, groups[cur], (x) => row({
        cls: x.level, title: x.title, href: x.link,
        side: [pill(LEVEL_LABEL[x.level], x.level), statePill(x.state)],
        meta: [esc(x.category), esc(x.status), esc(x.size), esc(x.where), timeTag(x.updated, `Updated ${ago(x.updated)}`), mapLink(x.pt)],
      }), { first: 8, noun: 'incident', empty: ui.state === 'ALL' ? 'None listed right now.' : `None listed for ${ui.state} right now.` });
    set('fires', ['fires'], html);
  }

  const FDR_RANK = { CATASTROPHIC: 4, EXTREME: 3, HIGH: 2, MODERATE: 1 };
  function renderDanger() {
    const pr = problem(['fdr'], 'fire danger ratings');
    if (pr.block) return set('danger', ['fdr'], pr.html);
    const rs = ((store.fdr.data && store.fdr.data.ratings) || []).map((r) => ({
      district: plain(r.district || r.title), today: String(r.danger_today || '').toUpperCase(), tomorrow: String(r.danger_tomorrow || '').toUpperCase(),
      banToday: r.fire_ban_today === true, banTomorrow: r.fire_ban_tomorrow === true,
    })).sort((a, b) => (b.banToday - a.banToday) || ((FDR_RANK[b.today] || 0) - (FDR_RANK[a.today] || 0)) || a.district.localeCompare(b.district));
    if (!rs.length) return set('danger', ['fdr'], pr.html + '<p class="lv-empty">The RFS has not published ratings.</p>');
    const fdr = (v) => `<span class="lv-fdr ${FDR_RANK[v] ? v.toLowerCase() : 'none'}">${esc(FDR_RANK[v] ? v.charAt(0) + v.slice(1).toLowerCase() : 'No rating')}</span>`;
    const ban = (b) => (b ? pill('Total fire ban', 'emergency') : '<span class="lv-muted">No ban</span>');
    const open = !!ui.open.danger;
    const shown = open ? rs : rs.slice(0, 7);
    const html = pr.html
      + figs([[rs.filter((r) => r.banToday).length, 'total fire bans today'], [rs.filter((r) => r.banTomorrow).length, 'tomorrow'], [rs.filter((r) => (FDR_RANK[r.today] || 0) >= 3).length, 'districts Extreme or above']])
      + `<div class="lv-table-wrap"><table class="lv-table"><thead><tr><th scope="col">District</th><th scope="col">Today</th><th scope="col">Ban today</th><th scope="col">Tomorrow</th><th scope="col">Ban tomorrow</th></tr></thead><tbody>${
        shown.map((r) => `<tr><th scope="row" style="text-transform:none;letter-spacing:0;font-size:13px;color:inherit;border-bottom:1px solid var(--lv-line)">${esc(r.district)}</th><td>${fdr(r.today)}</td><td>${ban(r.banToday)}</td><td>${fdr(r.tomorrow)}</td><td>${ban(r.banTomorrow)}</td></tr>`).join('')
      }</tbody></table></div>`
      + (rs.length > 7 ? `<button type="button" class="lv-more" data-act="more" data-arg="danger" aria-expanded="${open}">${open ? 'Show fewer' : `Show all ${rs.length} districts`}</button>` : '');
    set('danger', ['fdr'], html);
  }

  function renderWeather() {
    const pr = problem(['bom'], 'weather warnings');
    if (pr.block) return set('weather', ['bom'], pr.html);
    const ws = ((store.bom.data && store.bom.data.warnings) || []).map((w) => ({
      // The Bureau prefixes titles with its own issue stamp ("13/18:03 EDT "); the time is shown properly below.
      title: plain(w.title).replace(/^\d{1,2}\/\d{1,2}:\d{2}\s+[A-Z]{2,5}\s+/, ''),
      severe: /severe|major|emergency/i.test(String(w.severity || '') + ' ' + String(w.title || '')),
      marine: w.category === 'marine', issued: toMs(w.issued), link: httpUrl(w.link), area: plain(w.area),
    })).sort((a, b) => (b.severe - a.severe) || ((b.issued || 0) - (a.issued || 0)));
    const html = pr.html
      + figs([[ws.filter((w) => !w.marine).length, 'land'], [ws.filter((w) => w.marine).length, 'marine']])
      + list('weather', ws, (w) => row({
        cls: w.severe ? 'watch' : '', title: w.title, href: w.link,
        side: [w.severe ? pill('Severe', 'watch') : '', pill(w.marine ? 'Marine' : 'Land')],
        meta: [esc(w.area), timeTag(w.issued, `Issued ${when(w.issued)}`)],
      }), { first: 6, noun: 'warning', empty: 'No current Bureau warnings for NSW.' });
    set('weather', ['bom'], html);
  }

  // ---- status blocks: these cards show counts and proportions, not records.
  // The individual outages, incidents, beaches and messages are on the map
  // and in the logs; this page is for the state of things.

  /** Horizontal bars, longest first as given. rows: [{ label, value, note?, cls? }]. */
  function bars(title, rows, { unit = '', empty = 'Nothing to show.' } = {}) {
    const head = `<h3 class="lv-sub">${esc(title)}</h3>`;
    const live = rows.filter((r) => r.value > 0);
    if (!live.length) return head + `<p class="lv-empty">${esc(empty)}</p>`;
    const max = Math.max(...live.map((r) => r.value));
    return head + `<ul class="lv-bars">${live.map((r) => `<li>
      <span class="lv-bars-label">${r.html || esc(r.label)}</span>
      <span class="lv-bars-track" aria-hidden="true"><span class="lv-bars-fill ${r.cls || ''}" style="width:${Math.max(1.5, (r.value / max) * 100).toFixed(1)}%"></span></span>
      <span class="lv-bars-value lv-num">${num(r.value)}${unit ? ` ${esc(unit)}` : ''}${r.note ? ` <span class="lv-soft">${esc(r.note)}</span>` : ''}</span>
    </li>`).join('')}</ul>`;
  }

  /** One bar split into shares, with a legend that carries the numbers. parts: [{ label, value, cls }]. */
  function stack(title, parts) {
    const total = parts.reduce((n, x) => n + x.value, 0);
    if (!total) return '';
    const live = parts.filter((x) => x.value > 0);
    return `<h3 class="lv-sub">${esc(title)}</h3>
      <div class="lv-stack" role="img" aria-label="${esc(live.map((x) => `${x.label} ${x.value}`).join(', '))}">${live.map((x) => `<span class="${x.cls}" style="flex:${x.value} 1 0"></span>`).join('')}</div>
      <p class="lv-legend">${live.map((x) => `<span><i class="${x.cls}"></i>${esc(x.label)} <b class="lv-num">${num(x.value)}</b> <span class="lv-soft">${Math.round((x.value / total) * 100)}%</span></span>`).join('')}</p>`;
  }

  const seeMore = (what) => `<p class="lv-note">${esc(what)} are on the <a href="map">map</a> and in the <a href="logs">logs</a>.</p>`;

  function renderPower() {
    const pr = problem(['outages'], 'power outages');
    if (pr.block) return set('power', ['outages'], pr.html);
    const all = outages();
    // One row per distributor in the chosen state(s), in the state filter's order,
    // and any other with an outage there: Essential Energy is a NSW distributor
    // whose network reaches into southern Queensland.
    const srcs = distributors().filter((d) => inState(d.state) || all.some((o) => o.distributor === d.id))
      .sort((a, b) => (STATES.indexOf(a.state) - STATES.indexOf(b.state)) || a.name.localeCompare(b.name));
    if (!srcs.length) return set('power', ['outages'], pr.html + `<p class="lv-empty">AusAware has no distributor feed for ${esc(ui.state)}.</p>`);
    const many = ui.state === 'ALL';
    const rows = srcs.map((d) => {
      const mine = all.filter((o) => o.distributor === d.id);
      const act = mine.filter((o) => o.active);
      return { d, unplanned: act.filter((o) => !o.planned).length, planned: act.filter((o) => o.planned).length, scheduled: mine.length - act.length, customers: act.reduce((n, o) => n + o.customers, 0) };
    });
    const read = rows.filter((r) => r.d.read);
    const sum = (k) => read.reduce((n, r) => n + r[k], 0);
    const table = `<div class="lv-table-wrap"><table class="lv-table"><thead><tr><th scope="col">Distributor</th>${many ? '<th scope="col">State</th>' : ''}<th scope="col" class="r">Unplanned</th><th scope="col" class="r">Planned, under way</th><th scope="col" class="r">Scheduled</th><th scope="col" class="r">Customers off now</th></tr></thead><tbody>${
      rows.map((r) => {
        const st = many ? `<td>${statePill(r.d.state)}</td>` : '';
        if (!r.d.read) return `<tr><td>${esc(r.d.name)}</td>${st}<td colspan="4" class="lv-muted">Not reporting — AusAware is not reading this distributor at the moment</td></tr>`;
        return `<tr><td>${esc(r.d.name)}</td>${st}<td class="r">${num(r.unplanned)}</td><td class="r">${num(r.planned)}</td><td class="r">${num(r.scheduled)}</td><td class="r">${num(r.customers)}</td></tr>`;
      }).join('')
    }</tbody></table></div>`;
    const html = pr.html
      + figs([[sum('customers'), 'customers off now'], [sum('unplanned'), 'unplanned outages'], [sum('planned'), 'planned, under way'], [sum('scheduled'), 'scheduled']])
      + bars('Customers without power, by distributor', read.slice().sort((a, b) => b.customers - a.customers)
        .map((r) => ({ label: r.d.name, value: r.customers, note: many ? r.d.state : '' })), { empty: 'No customers are reported without power.' })
      + `<h3 class="lv-sub">By distributor</h3>` + table
      + seeMore('Individual outages');
    set('power', ['outages'], html);
  }

  const ROAD_STATES = ['NSW', 'QLD', 'VIC', 'SA', 'ACT'];
  const ROAD_COLS = [['incidents', 'Incidents'], ['closures', 'Floods and fire closures'], ['events', 'Major events'], ['roadwork', 'Roadwork']];
  function renderRoads() {
    const keys = ['roadIncidents', 'roadFloods', 'roadFires', 'roadEvents', 'roadwork'];
    const pr = problem(keys, 'the road feeds');
    if (pr.block) return set('roads', keys, pr.html);
    // A kind whose feed has not answered has no count: a dash, never a zero.
    const have = (k) => ROAD_KINDS[k].every((key) => store[key].data);
    const by = {};
    for (const [k] of ROAD_COLS) by[k] = have(k) ? roads(k) : null;
    const n = (k, st) => (by[k] ? by[k].filter((x) => !st || x.state === st).length : null);
    const cell = (v) => (v == null ? '<span class="lv-muted">—</span>' : num(v));
    const states = ROAD_STATES.filter(inState);
    const table = `<div class="lv-table-wrap"><table class="lv-table"><thead><tr><th scope="col">State</th>${ROAD_COLS.map(([, label]) => `<th scope="col" class="r">${esc(label)}</th>`).join('')}</tr></thead><tbody>${
      states.map((st) => `<tr><td>${statePill(st)}</td>${ROAD_COLS.map(([k]) => `<td class="r">${cell(n(k, st))}</td>`).join('')}</tr>`).join('')
    }</tbody></table></div>`;
    const cats = {};
    for (const x of by.incidents || []) { const c = x.cat || 'Other'; cats[c] = (cats[c] || 0) + 1; }
    const top = Object.entries(cats).sort((a, b) => b[1] - a[1]).slice(0, 6);
    const major = (by.incidents || []).filter((x) => x.major).length;
    const html = pr.html
      + figs([by.incidents ? [by.incidents.length, 'incidents'] : null, by.incidents ? [major, 'marked major'] : null, by.closures ? [by.closures.length, 'flood or fire closures'] : null,
        by.events ? [by.events.length, 'major events'] : null, by.roadwork ? [by.roadwork.length, 'roadwork'] : null])
      + (states.length > 1 ? bars('Incidents by state', states.map((st) => ({ label: st, value: n('incidents', st) || 0 })).sort((a, b) => b.value - a.value), { empty: 'No incidents are listed.' }) : '')
      + bars('Incidents by kind', top.map(([label, value]) => ({ label, value })), { empty: ui.state === 'ALL' ? 'No incidents are listed.' : `No incidents are listed for ${ui.state}.` })
      + `<h3 class="lv-sub">By state</h3>` + table
      + seeMore('Individual incidents and roadwork');
    set('roads', keys, html);
  }

  const SEV = { emergency: 'emergency', high: 'watch', medium: 'advice' };
  function renderRadio() {
    const pr = problem(['summary'], 'the radio summary');
    if (pr.block) return set('radio', ['summary'], pr.html);
    const h = store.summary.data && store.summary.data.hourly;
    if (!h) return set('radio', ['summary'], pr.html + '<p class="lv-empty">No hourly summary yet. The next one is released on the hour.</p>');
    let d = h.details;
    if (typeof d === 'string') { try { d = JSON.parse(d); } catch (_e) { d = null; } }
    const s = d && d.structured;
    const from = toMs(h.period_start), to = toMs(h.period_end);
    let html = pr.html + `<p class="lv-figs"><span><b>${esc(from && to ? `${clock(from)} – ${clock(to)}` : 'Latest hour')}</b></span><span><b>${num(h.call_count)}</b>receptions</span>${s ? `<span><b>${num(s.incident_count)}</b>incidents</span>${s.activity_level ? `<span><b>${esc(tidy(s.activity_level))}</b>activity</span>` : ''}` : ''}</p>`;
    if (!s) {
      html += `<p class="lv-sum-overview" style="white-space:pre-wrap">${esc(h.summary || 'The summary for this hour is empty.')}</p>`;
      return set('radio', ['summary'], html);
    }
    if (s.overview) html += `<p class="lv-sum-overview">${esc(s.overview)}</p>`;
    if (Array.isArray(s.notable) && s.notable.length) html += `<p class="lv-sum-notable"><b>Notable:</b> ${s.notable.map(esc).join(' · ')}</p>`;
    const incs = Array.isArray(s.incidents) ? s.incidents : [];
    html += list('radio', incs, (inc) => {
      const locs = Array.isArray(inc.locations) ? inc.locations.map((l) => (typeof l === 'string' ? l : (l && (l.raw || l.region)) || '')).filter(Boolean).join(' · ') : '';
      const ags = Array.isArray(inc.agencies) ? inc.agencies.filter(Boolean).join(' · ') : '';
      const win = inc.window && (inc.window.start || inc.window.end) ? [inc.window.start, inc.window.end].filter(Boolean).join('–') : '';
      const tx = (Array.isArray(inc.transcripts) ? inc.transcripts : []).slice(0, 4);
      const lvl = SEV[String(inc.severity || '').toLowerCase()] || '';
      return `<li class="lv-row ${lvl}">
        <div class="lv-row-title">${esc(tidy(inc.type) || tidy(inc.incident_type) || 'Incident')}</div>
        <div class="lv-row-side">${pill(tidy(inc.status))}</div>
        <div class="lv-row-meta">${[locs, ags, win].filter(Boolean).map((x) => `<span>${esc(x)}</span>`).join('')}</div>
        <div class="lv-row-text">${esc(inc.summary || '')}${tx.length ? `<ul class="lv-tx">${tx.map((t) => `<li><time>${esc(t && t.time || '')}</time><span>${esc(t && t.text || '')}</span></li>`).join('')}</ul>` : ''}</div>
      </li>`;
    }, { first: 4, cap: 60, noun: 'incident', empty: s.quiet_hour ? 'A quiet hour: nothing operational was heard.' : 'This hour has an overview only.' });
    html += '<p class="lv-note">Written by an AI model from radio transcripts, so it can be wrong. Earlier hours are in the <a href="logs">logs</a>.</p>';
    set('radio', ['summary'], html);
  }

  function renderAircraft() {
    const pr = problem(['aircraft'], 'aircraft');
    if (pr.block) return set('aircraft', ['aircraft'], pr.html);
    const ac = (store.aircraft.data && store.aircraft.data.aircraft) || [];
    const airborne = ac.filter((a) => !a.onGround).length;
    const tagged = {};
    for (const a of ac) if (a.esTag) tagged[a.esTag] = (tagged[a.esTag] || 0) + 1;
    const emerg = ac.filter((a) => a.emergencySquawk);
    const tagList = Object.entries(tagged).sort((a, b) => b[1] - a[1]);
    const html = pr.html
      + figs([[ac.length, 'tracked'], [airborne, 'airborne'], [ac.length - airborne, 'on the ground'], [emerg.length, 'emergency squawk']])
      + (emerg.length ? `<ul class="lv-list">${emerg.map((a) => row({
        cls: 'emergency', title: plain(a.callsign) || plain(a.reg) || a.hex, href: `map?icao=${encodeURIComponent(a.hex)}`,
        side: [pill(`Squawk ${plain(a.squawk)}`, 'emergency')], meta: [esc(plain(a.reg)), esc(plain(a.type)), a.altFt != null ? `${num(a.altFt)} ft` : ''],
      })).join('')}</ul>` : '')
      + (tagList.length ? `<p class="lv-note">Emergency-service aircraft in the air or on the ground: ${tagList.map(([t, n]) => `${esc(tidy(t).toLowerCase())} ${num(n)}`).join(', ')}.</p>` : '')
      + '<p class="lv-note"><a href="map">Open the map</a> to see them.</p>';
    set('aircraft', ['aircraft'], html);
  }

  /**
   * A column chart over time. slots: [{ from, to, n }], oldest first; `ticks`
   * are the slot indexes that get a clock time under them. Columns are plain
   * boxes with their count as text for assistive tech and on hover.
   */
  function columns(title, slots, ticks) {
    const max = Math.max(1, ...slots.map((sl) => sl.n));
    const total = slots.reduce((n, sl) => n + sl.n, 0);
    return `<h3 class="lv-sub">${esc(title)}</h3>
      <div class="lv-cols" role="img" aria-label="${esc(`${title}: ${total} in all, at most ${max} in one step`)}">
        <div class="lv-cols-scale" aria-hidden="true"><span>${num(max)}</span><span>0</span></div>
        <div class="lv-cols-plot">${slots.map((sl) => `<div class="lv-col${sl.n ? '' : ' is-zero'}" title="${esc(`${clock(sl.from)} – ${clock(sl.to)}: ${sl.n}`)}"><span style="height:${sl.n ? Math.max(6, (sl.n / max) * 100).toFixed(1) : 0}%"></span></div>`).join('')}</div>
        <div class="lv-cols-axis" aria-hidden="true">${slots.map((sl, i) => `<span>${ticks.includes(i) ? esc(clock(sl.from)) : ''}</span>`).join('')}</div>
      </div>`;
  }

  function renderPager() {
    const pr = problem(['pager'], 'pager messages');
    if (pr.block) return set('pager', ['pager'], pr.html);
    const items = feats('pager').map((f) => {
      const p = f.properties || {};
      return { type: tidy(p.type), stop: p.is_stop === true, time: toMs(p.timestamp) || toMs(p.fetched_at) };
    });
    // Twelve quarter-hour steps on the clock's own quarters (6:00, 6:15, …),
    // the last one still filling, so a column keeps its meaning between refreshes.
    const Q = 900_000, now = Date.now();
    const end = Math.floor(now / Q) * Q + Q;
    const slots = Array.from({ length: 12 }, (_, i) => ({ from: end - (12 - i) * Q, to: end - (11 - i) * Q, n: 0 }));
    for (const x of items) {
      if (x.time == null) continue;
      const i = Math.floor((x.time - slots[0].from) / Q);
      if (i >= 0 && i < 12) slots[i].n += 1;
    }
    // A time under each column that starts an hour.
    const ticks = slots.map((sl, i) => (new Date(sl.from).getMinutes() === 0 ? i : -1)).filter((i) => i >= 0);
    const types = {};
    for (const x of items) { const t = x.stop ? 'Stop message' : (x.type || 'Type not read'); types[t] = (types[t] || 0) + 1; }
    const last = items.reduce((m, x) => Math.max(m, x.time || 0), 0);
    const chips = Object.entries(types).sort((a, b) => b[1] - a[1]).slice(0, 8);
    const html = pr.html
      + figs([[items.length, 'located messages, last 3 hours'], last ? [ago(last), 'most recent'] : null])
      + (items.length
        ? columns('Messages per quarter hour', slots, ticks)
          + `<h3 class="lv-sub">By type</h3><p class="lv-chips">${chips.map(([label, n]) => `<span><b class="lv-num">${num(n)}</b>${esc(label)}</span>`).join('')}</p>`
        : '<p class="lv-empty">No located pager messages in the last three hours.</p>')
      + '<p class="lv-note">Counts only the messages that could be placed on the map. The messages themselves are on the <a href="map">map</a> and in the <a href="logs">logs</a>.</p>';
    set('pager', ['pager'], html);
  }

  function renderTransport() {
    const pr = problem(['transport'], 'transport alerts');
    if (pr.block) return set('transport', ['transport'], pr.html);
    const d = store.transport.data || {};
    if (d.configured === false) return set('transport', ['transport'], '<p class="lv-empty">Transport alerts are not switched on.</p>');
    const SEVR = { SEVERE: 2, WARNING: 1 };
    const nowS = Date.now() / 1000;
    const items = (d.alerts || []).filter((a) => !(a.end && a.end < nowS)).map((a) => ({
      title: plain(a.header) || 'Alert', sev: String(a.severity || '').toUpperCase(), effect: tidy(a.effect), cause: tidy(a.cause),
      start: toMs(a.start), end: toMs(a.end), url: httpUrl(a.url), current: !(a.start && a.start > nowS),
    })).sort((a, b) => (b.current - a.current) || ((SEVR[b.sev] || 0) - (SEVR[a.sev] || 0)) || ((b.start || 0) - (a.start || 0)));
    const html = pr.html + list('transport', items, (a) => row({
      cls: a.sev === 'SEVERE' ? 'watch' : (a.current ? '' : 'planned'), title: a.title, href: a.url,
      side: [a.sev === 'SEVERE' ? pill('Severe', 'watch') : '', a.current ? '' : pill('Upcoming', 'planned'), pill(a.effect)],
      meta: [esc(a.cause), a.start ? timeTag(a.start, `From ${when(a.start)}`) : '', a.end ? timeTag(a.end, `Until ${when(a.end)}`) : ''],
    }), { first: 5, cap: 80, noun: 'alert', empty: 'No current Transport for NSW alerts.' });
    set('transport', ['transport'], html);
  }

  function renderAct() {
    if (!ui.actOn) {
      $('act-state').innerHTML = '';
      $('act-body').innerHTML = '<p class="lv-card-src" style="margin:0 0 0.7rem">Where ACT Ambulance crews are responding. Loaded only when asked for.</p><button type="button" class="lv-btn" data-act="act-on">Show ACT ambulance responses</button>';
      return;
    }
    const pr = problem(['act'], 'ACT ambulance responses');
    if (pr.block) return set('act', ['act'], pr.html);
    const items = feats('act').map((f) => {
      const p = f.properties || {};
      return { title: plain(p.location_text) || plain(p.title) || 'Response', status: tidy(p.status), called: plain(p.time_of_call), updated: plain(p.updated), ts: toMs(p.timestamp), pt: pointOf(f) };
    }).sort((a, b) => (b.ts || 0) - (a.ts || 0));
    set('act', ['act'], pr.html + list('act', items, (x) => row({
      title: x.title, side: [pill(x.status)],
      meta: [x.called ? `Called ${esc(x.called)}` : '', x.updated ? `Updated ${esc(x.updated)}` : '', mapLink(x.pt)],
    }), { first: 6, noun: 'response', empty: 'No ambulance responses are listed.' }) + '<p class="lv-note">Times are as ACT Ambulance publishes them, in Canberra time.</p>');
  }

  function renderBeaches() {
    const pr = problem(['beachwatch'], 'beach water quality');
    if (pr.block) return set('beaches', ['beachwatch', 'beachsafe'], pr.html);
    const bs = feats('beachwatch').map((f) => {
      const p = f.properties || {};
      return { result: tidy(p.latestResult).toLowerCase(), forecast: tidy(p.pollutionForecast) };
    });
    const rated = (r) => bs.filter((b) => b.result === r).length;
    const known = ['good', 'fair', 'poor', 'bad'];
    const likely = bs.filter((b) => /likely/i.test(b.forecast) && !/unlikely/i.test(b.forecast)).length;
    const safe = Array.isArray(store.beachsafe.data) ? store.beachsafe.data : null;
    const patrolled = safe ? safe.filter((b) => b.isPatrolledToday || b.patrolled).length : null;
    const html = pr.html
      + figs([[bs.length, 'beaches sampled'], [rated('poor') + rated('bad'), 'rated Poor or Bad'], [likely, 'pollution likely today'],
        patrolled == null ? (stateOf('beachsafe') === 'loading' ? null : ['—', 'patrol data unavailable']) : [patrolled, 'patrolled today']])
      + stack('Latest water quality result', [
        { label: 'Good', value: rated('good'), cls: 'is-good' }, { label: 'Fair', value: rated('fair'), cls: 'is-fair' },
        { label: 'Poor', value: rated('poor'), cls: 'is-poor' }, { label: 'Bad', value: rated('bad'), cls: 'is-bad' },
        { label: 'No result', value: bs.filter((b) => !known.includes(b.result)).length, cls: 'is-none' },
      ])
      + '<p class="lv-note">Each beach and its result are on the <a href="map">map</a>.</p>';
    set('beaches', ['beachwatch', 'beachsafe'], html);
  }

  function renderNews() {
    const pr = problem(['news'], 'news');
    if (pr.block) return set('news', ['news'], pr.html);
    const all = ((store.news.data && store.news.data.items) || []).map((n) => ({
      title: plain(n.title), source: plain(n.source), cat: n.category, link: httpUrl(n.link), time: toMs(n.timestamp) || toMs(n.published), text: plain(n.description),
    }));
    const cat = ui.tab.news;
    const items = cat === 'all' ? all : all.filter((n) => n.cat === cat);
    const n = (c) => all.filter((x) => x.cat === c).length;
    let html = pr.html + tabs('news', [['all', 'All', all.length], ['emergency', 'Emergency', n('emergency')], ['weather', 'Weather', n('weather')]]);
    if (!items.length) html += '<p class="lv-empty">No stories in this category right now.</p>';
    else {
      const open = !!ui.open.news;
      html += `<ul class="lv-list lv-news">${items.slice(0, open ? 60 : 9).map((x) => row({
        title: x.title, href: x.link, meta: [esc(x.source), timeTag(x.time, ago(x.time))],
      })).join('')}</ul>`;
      if (items.length > 9) html += `<button type="button" class="lv-more" data-act="more" data-arg="news" aria-expanded="${open}">${open ? 'Show fewer' : `Show all ${Math.min(items.length, 60)}`}</button>`;
    }
    set('news', ['news'], html);
  }

  // ----------------------------------------------------------------- trends

  /**
   * One small chart per state for the chosen measure, on a shared look but each
   * on its own scale: the question a trend answers is "is this state's number
   * going up or down", not "which state is biggest" (the tiles answer that).
   * `legacy` reads NSW from snapshots written before states were recorded.
   */
  const METRICS = {
    fires: { label: 'Fires', key: 'fires', legacy: (d) => d.emergency && d.emergency.rfs_incidents },
    roads: { label: 'Road incidents', key: 'roads', legacy: (d) => d.traffic && d.traffic.incidents },
    floods: { label: 'Flooded roads', key: 'road_floods', legacy: (d) => d.traffic && d.traffic.floods },
    roadwork: { label: 'Roadwork', key: 'roadwork', legacy: (d) => d.traffic && d.traffic.roadwork },
    customers: { label: 'Customers without power', key: 'customers', legacy: (d) => d.power && d.power.total_customers },
    outages: { label: 'Power outages', key: 'outages', legacy: (d) => d.power && d.power.total_outages },
  };
  const charts = {};

  function trendSeries() {
    const m = METRICS[ui.trend.metric];
    const rows = Array.isArray(store.history.data) ? store.history.data : [];
    const series = {};
    rows.forEach((r, i) => {
      let d = r.data;
      if (typeof d === 'string') { try { d = JSON.parse(d); } catch (_e) { d = null; } }
      if (!d) return;
      const by = d.by_state || {};
      for (const st of Object.keys(by)) {
        const v = by[st] && by[st][m.key];
        if (typeof v === 'number') (series[st] = series[st] || new Array(rows.length).fill(null))[i] = v;
      }
      if (!(by.NSW && typeof by.NSW[m.key] === 'number')) {
        const v = m.legacy(d);
        if (typeof v === 'number') (series.NSW = series.NSW || new Array(rows.length).fill(null))[i] = v;
      }
    });
    return { times: rows.map((r) => Number(r.timestamp)), series };
  }

  function renderTrends() {
    const head = `<div class="lv-tabs" role="group" aria-label="Measure">${Object.entries(METRICS).map(([id, m]) =>
      `<button type="button" data-act="metric" data-arg="${id}" aria-pressed="${ui.trend.metric === id}">${esc(m.label)}</button>`).join('')}</div>
      <div class="lv-tabs" role="group" aria-label="Period">${Object.entries(PERIODS).map(([id, p]) =>
      `<button type="button" data-act="period" data-arg="${id}" aria-pressed="${ui.trend.period === id}">${esc(p.label)}</button>`).join('')}</div>`;
    for (const k of Object.keys(charts)) { charts[k].destroy(); delete charts[k]; }
    const pr = problem(['history'], 'the history');
    if (pr.block) return set('trends', ['history'], head + pr.html);
    const { times, series } = trendSeries();
    const wanted = (ui.state === 'ALL' ? STATES.slice(1) : [ui.state]).filter((st) => ui.state !== 'ALL' || series[st]);
    if (!wanted.length) return set('trends', ['history'], head + pr.html + '<p class="lv-empty">No history has been recorded for this measure yet.</p>');
    const html = head + pr.html + `<div class="lv-trends">${wanted.map((st) => {
      const vals = (series[st] || []).filter((v) => v != null);
      if (vals.length < 2) return `<div class="lv-trend"><div class="lv-trend-head"><b>${st}</b></div><div class="lv-trend-none">No history recorded for ${st} yet</div></div>`;
      const lo = Math.min(...vals), hi = Math.max(...vals);
      return `<div class="lv-trend"><div class="lv-trend-head"><b>${st}</b><span class="lv-trend-range">${lo === hi ? 'steady' : `${num(lo)} to ${num(hi)}`}</span><span class="lv-trend-now">${num(vals[vals.length - 1])}</span></div>
        <div class="lv-trend-plot"><canvas id="trend-${st}" role="img" aria-label="${esc(METRICS[ui.trend.metric].label)} in ${st}, ${esc(PERIODS[ui.trend.period].label.toLowerCase())}: now ${num(vals[vals.length - 1])}, between ${num(lo)} and ${num(hi)}"></canvas></div></div>`;
    }).join('')}</div><p class="lv-note">Recorded every five minutes. Each chart has its own scale. States other than NSW have history only from when AusAware began recording them.</p>`;
    set('trends', ['history'], html);
    if (typeof window.Chart !== 'function') return;
    const long = ui.trend.period === '7d' || ui.trend.period === '24h';
    const labels = times.map((t) => (long ? when(t) : clock(t)));
    for (const st of wanted) {
      const cv = $(`trend-${st}`);
      if (!cv) continue;
      charts[st] = new window.Chart(cv.getContext('2d'), {
        type: 'line',
        data: { labels, datasets: [{ data: series[st], borderColor: '#cbd5e1', backgroundColor: 'rgba(203,213,225,0.08)', fill: true, borderWidth: 2, tension: 0.25, pointRadius: 0, pointHoverRadius: 4, pointHitRadius: 12, spanGaps: true }] },
        options: {
          responsive: true, maintainAspectRatio: false, animation: false,
          interaction: { intersect: false, mode: 'index' },
          plugins: { legend: { display: false }, tooltip: { displayColors: false, callbacks: { label: (c) => num(c.parsed.y) } } },
          scales: { x: { display: false }, y: { display: false, grace: '10%' } },
        },
      });
    }
  }

  // ------------------------------------------------------------ now + chrome

  function tile(href, label, value, sub, cls) {
    return `<a class="lv-tile ${cls || ''}" href="#${href}"><div class="lv-tile-label">${esc(label)}</div><div class="lv-tile-value${typeof value === 'number' ? '' : ' na'}">${typeof value === 'number' ? num(value) : esc(value)}</div><div class="lv-tile-sub">${sub || ''}</div></a>`;
  }
  // The figure, or what stands in for it while there is none. Never a zero.
  const figure = (keys, fn) => {
    const st = worst(keys);
    if (st === 'loading') return '…';
    if (keys.some((k) => !store[k].data)) return 'Unavailable';
    return fn();
  };
  const covers = (list) => ui.state === 'ALL' || list.includes(ui.state);

  function renderNow() {
    const f = store.fires.data ? fires() : [];
    const burning = f.filter((x) => x.kind !== 'warning' && x.layer === 'fire');
    const top = f.reduce((m, x) => Math.max(m, LEVEL_RANK[x.level]), 0);
    const lv = (l) => f.filter((x) => x.level === l).length;
    const levels = [lv('emergency') ? `${lv('emergency')} Emergency Warning` : '', lv('watch') ? `${lv('watch')} Watch and Act` : '', lv('advice') ? `${lv('advice')} Advice` : ''].filter(Boolean);
    const out = [];
    out.push(tile('fires', 'Fires', figure(['fires'], () => burning.length), store.fires.data ? esc(levels.join(' · ') || 'No warnings in force') : '', top === 3 ? 'is-emergency' : top === 2 ? 'is-watch' : ''));

    const fireWarn = f.filter((x) => x.kind === 'warning').length;
    const bom = covers(['NSW']) && store.bom.data ? (store.bom.data.warnings || []).length : null;
    // The fire warnings still count when the Bureau's feed is down; the tile says which half is missing.
    const bomDown = covers(['NSW']) && bom == null && stateOf('bom') !== 'loading';
    out.push(tile('weather', 'Warnings', figure(['fires'], () => fireWarn + (bom || 0)),
      store.fires.data ? esc([`${fireWarn} fire`, bom == null ? '' : `${bom} weather (NSW)`, bomDown ? 'NSW weather warnings unavailable' : ''].filter(Boolean).join(' · ')) : ''));

    if (covers(powerStates())) {
      const o = store.outages.data ? outages().filter((x) => x.active) : [];
      const silent = distributors().filter((d) => inState(d.state) && !d.read).map((d) => d.name);
      const quiet = !silent.length ? '' : silent.length > 2 ? `${silent.length} distributors not reporting` : `${silent.join(', ')} not reporting`;
      out.push(tile('power', 'Customers without power', figure(['outages'], () => o.reduce((n, x) => n + x.customers, 0)),
        store.outages.data ? esc([plural(o.length, 'outage'), quiet].filter(Boolean).join(' · ')) : ''));
    } else out.push(tile('power', 'Customers without power', 'No feed', `No distributor feed for ${ui.state}`));

    if (covers(['NSW', 'QLD', 'VIC', 'SA', 'ACT'])) {
      const cl = ROAD_KINDS.closures.every((k) => store[k].data) ? roads('closures').length : null;
      const rw = store.roadwork.data ? roads('roadwork').length : null;
      out.push(tile('roads', 'Road incidents', figure(['roadIncidents'], () => roads('incidents').length),
        esc([cl == null ? '' : `${num(cl)} flood or fire closures`, rw == null ? '' : `${num(rw)} roadwork`].filter(Boolean).join(' · '))));
    } else out.push(tile('roads', 'Road incidents', 'No feed', `No road feed for ${ui.state}`));

    const ac = (store.aircraft.data && store.aircraft.data.aircraft) || [];
    const sq = ac.filter((a) => a.emergencySquawk).length;
    out.push(tile('aircraft', 'Aircraft tracked', figure(['aircraft'], () => ac.length), store.aircraft.data ? esc(`Australia-wide · ${sq ? plural(sq, 'emergency squawk') : 'no emergency squawks'}`) : '', sq ? 'is-emergency' : ''));

    if (covers(['NSW'])) out.push(tile('pager', 'Pager messages', figure(['pager'], () => feats('pager').length), 'Located, last 3 hours · NSW'));
    $('now').innerHTML = out.join('');
  }

  // Which states each section has anything to say about (null = all).
  const SECTIONS = {
    fires: { keys: ['fires'], render: renderFires, states: null },
    danger: { keys: ['fdr'], render: renderDanger, states: ['NSW'] },
    weather: { keys: ['bom'], render: renderWeather, states: ['NSW'] },
    // Which states have a distributor is read from the feed itself (see renderChrome).
    power: { keys: ['outages'], render: renderPower, states: ['NSW'], dynamic: powerStates },
    roads: { keys: ['roadIncidents', 'roadFloods', 'roadFires', 'roadEvents', 'roadwork'], render: renderRoads, states: ['NSW', 'QLD', 'VIC', 'SA', 'ACT'] },
    radio: { keys: ['summary'], render: renderRadio, states: ['NSW'] },
    aircraft: { keys: ['aircraft'], render: renderAircraft, states: null },
    pager: { keys: ['pager'], render: renderPager, states: ['NSW'] },
    transport: { keys: ['transport'], render: renderTransport, states: ['NSW'] },
    act: { keys: ['act'], render: renderAct, states: ['ACT'] },
    beaches: { keys: ['beachwatch', 'beachsafe'], render: renderBeaches, states: ['NSW'] },
    news: { keys: ['news'], render: renderNews, states: null },
    trends: { keys: ['history'], render: renderTrends, states: null },
  };
  const TITLES = { danger: 'fire danger ratings', weather: 'weather warnings', power: 'power outages', roads: 'roads', radio: 'the radio summary', pager: 'pager messages', transport: 'transport alerts', act: 'ACT ambulance responses', beaches: 'beaches' };

  function renderChrome() {
    const all = Object.keys(SOURCES).filter((k) => !(SOURCES[k].optIn && !ui.actOn));
    const bad = all.filter((k) => stateOf(k) === 'error' || stateOf(k) === 'stale');
    const newest = Math.max(0, ...all.map((k) => store[k].at));
    $('updated').textContent = newest ? `Updated ${clock(newest)}` : 'Loading…';
    const btn = $('sources-btn');
    btn.textContent = bad.length ? `${bad.length} of ${all.length} sources have a problem` : `All ${all.length} sources live`;
    btn.setAttribute('aria-expanded', String(ui.sourcesOpen));
    const panel = $('sources');
    panel.hidden = !ui.sourcesOpen;
    if (ui.sourcesOpen) {
      const word = { ok: 'Live', stale: 'Not refreshing', error: 'Unavailable', loading: 'Loading' };
      panel.innerHTML = all.map((k) => {
        const st = stateOf(k);
        return `<div class="lv-src"><span class="lv-dot ${st === 'loading' ? '' : st}"></span><b>${esc(SOURCES[k].label)}</b><span>${word[st]}${store[k].at ? ` · ${clock(store[k].at)}` : ''}</span></div>`;
      }).join('');
    }
    const hidden = [];
    for (const [id, sec] of Object.entries(SECTIONS)) {
      const states = sec.dynamic ? sec.dynamic() : sec.states;
      const show = !states || ui.state === 'ALL' || states.includes(ui.state);
      const el = $(id);
      if (el) el.hidden = !show;
      if (!show && TITLES[id]) hidden.push(TITLES[id]);
    }
    $('not-here').textContent = hidden.length ? `AusAware has no feed for ${ui.state} for: ${hidden.join(', ')}.` : '';
    for (const b of document.querySelectorAll('[data-act="state"]')) b.setAttribute('aria-pressed', String(b.dataset.arg === ui.state));
  }

  /** Redraw what a source feeds, keeping keyboard focus where it was. */
  function paint(key) {
    const a = document.activeElement;
    const focus = a && a.dataset && a.dataset.act ? `[data-act="${a.dataset.act}"][data-arg="${a.dataset.arg || ''}"]` : null;
    for (const sec of Object.values(SECTIONS)) if (!key || sec.keys.includes(key)) sec.render();
    renderNow();
    renderChrome();
    if (focus) { const el = document.querySelector(focus); if (el && el !== document.activeElement) el.focus({ preventScroll: true }); }
  }

  // ------------------------------------------------------------- scheduling

  let ticker = null, heart = null;
  function tick() {
    const now = Date.now();
    for (const k of Object.keys(SOURCES)) if (now - store[k].tried >= SOURCES[k].every * 1000) load(k);
  }

  let pageId = null;
  try { pageId = localStorage.getItem('nswpsn_session_id'); } catch (_e) { /* storage blocked */ }
  if (!pageId) {
    pageId = 'session-' + ((window.crypto && crypto.randomUUID) ? crypto.randomUUID() : String(Date.now()) + Math.random().toString(16).slice(2));
    try { localStorage.setItem('nswpsn_session_id', pageId); } catch (_e) { /* storage blocked */ }
  }
  // Tells the backend a viewer is here, so its collectors run at the active cadence.
  function heartbeat(action) {
    fetch(`${API}/api/heartbeat?action=${action}&page_id=${encodeURIComponent(pageId)}&page_type=live&data_page=true`, { keepalive: true }).catch(() => {});
  }

  function start() {
    stop();
    heartbeat('open');
    heart = setInterval(() => heartbeat('ping'), 60_000);
    ticker = setInterval(tick, 15_000);
    tick();
  }
  function stop() {
    if (heart) { clearInterval(heart); heart = null; }
    if (ticker) { clearInterval(ticker); ticker = null; }
  }

  // ----------------------------------------------------------------- events

  document.addEventListener('click', (ev) => {
    const el = ev.target.closest('[data-act]');
    if (!el) return;
    const act = el.dataset.act, arg = el.dataset.arg || '';
    if (act === 'state') {
      ui.state = STATES.includes(arg) ? arg : 'ALL';
      try { localStorage.setItem('ausaware_live_state', ui.state); } catch (_e) { /* storage blocked */ }
      const u = new URL(location.href);
      if (ui.state === 'ALL') u.searchParams.delete('state'); else u.searchParams.set('state', ui.state);
      history.replaceState(null, '', u);
      if (window.umami) window.umami.track('live-state', { state: ui.state });
      paint();
    } else if (act === 'more') { ui.open[arg] = !ui.open[arg]; paint(); }
    else if (act === 'tab') { const [g, id] = arg.split(':'); ui.tab[g] = id; paint(); }
    else if (act === 'metric') { ui.trend.metric = arg; renderTrends(); }
    else if (act === 'period') { ui.trend.period = arg; store.history.tried = 0; store.history.data = null; store.history.status = 'idle'; renderTrends(); load('history'); }
    else if (act === 'act-on') { ui.actOn = true; store.act.tried = 0; paint(); load('act'); }
    else if (act === 'sources') { ui.sourcesOpen = !ui.sourcesOpen; renderChrome(); }
    else if (act === 'menu') {
      $('sidebar').classList.toggle('open');
      const ov = document.querySelector('.menu-overlay');
      if (ov) ov.classList.toggle('active');
    }
  });

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) { stop(); heartbeat('close'); }
    else if (!(window.idleGuard && window.idleGuard.isIdle())) start();
  });
  window.addEventListener('pagehide', () => heartbeat('close'));
  if (window.idleGuard) {
    window.idleGuard.onChange((idle) => { if (idle) { stop(); heartbeat('close'); } else start(); });
  }

  document.addEventListener('DOMContentLoaded', async () => {
    let st = new URLSearchParams(location.search).get('state');
    if (!st) { try { st = localStorage.getItem('ausaware_live_state'); } catch (_e) { /* storage blocked */ } }
    st = String(st || '').toUpperCase();
    if (STATES.includes(st)) ui.state = st;
    $('states').innerHTML = STATES.map((s) => `<button type="button" data-act="state" data-arg="${s}" aria-pressed="${s === ui.state}">${s === 'ALL' ? 'All states' : s}</button>`).join('');
    paint();
    // Mint the session token once, so the first burst of requests shares it.
    try { await window.AusApi.token(); } catch (_e) { /* each request reports its own failure */ }
    if (!document.hidden) start();
  });
})();
