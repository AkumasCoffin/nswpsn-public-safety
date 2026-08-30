/**
 * Shared Live Traffic NSW vocabulary.
 *
 * map.html, logs.html and live.html each used to carry their own copy of
 * the "which LiveTraffic feed is this, and what colour/icon/label does it
 * get" mapping, so the same feed could appear as three different colours
 * with three different names depending on which page you were looking at.
 * This is the single definition all three now read.
 *
 * `archiveSource` is the value written into archive_traffic and accepted
 * by /api/data/history?source=… . `endpoint` is the live per-source route
 * map.html and live.html poll. `icon` is a Font Awesome class.
 *
 * Load order matters: this file must come before the page's own script.
 */
(function () {
  const FEEDS = [
    {
      id: 'incident',
      archiveSource: 'traffic_incident',
      label: 'Incidents',
      short: 'Incidents',
      icon: 'fa-car-burst',
      color: '#f97316',
      endpoint: '/api/traffic/incidents',
      blurb: 'Crashes, breakdowns and hazards on the state road network.',
    },
    {
      id: 'roadwork',
      archiveSource: 'traffic_roadwork',
      label: 'Roadwork',
      short: 'Roadwork',
      icon: 'fa-road',
      color: '#eab308',
      endpoint: '/api/traffic/roadwork',
      blurb: 'Planned and emergency roadwork on state roads.',
    },
    {
      id: 'flood',
      archiveSource: 'traffic_flood',
      label: 'Flooding',
      short: 'Floods',
      icon: 'fa-water',
      color: '#3b82f6',
      endpoint: '/api/traffic/flood',
      blurb: 'Road closures and hazards caused by flooding.',
    },
    {
      id: 'fire',
      archiveSource: 'traffic_fire',
      label: 'Fire',
      short: 'Fire',
      icon: 'fa-fire',
      color: '#ef4444',
      endpoint: '/api/traffic/fire',
      blurb: 'Fire activity affecting the road network.',
    },
    {
      id: 'majorevent',
      archiveSource: 'traffic_majorevent',
      label: 'Major Events',
      short: 'Events',
      icon: 'fa-star',
      color: '#8b5cf6',
      endpoint: '/api/traffic/majorevent',
      blurb: 'Sporting fixtures, marches and other planned closures.',
    },
    {
      id: 'alpine',
      archiveSource: 'traffic_alpine',
      label: 'Alpine',
      short: 'Alpine',
      icon: 'fa-snowflake',
      color: '#7dd3fc',
      endpoint: '/api/traffic/alpine',
      blurb: 'Snow, ice and chain-fitting conditions on alpine roads.',
    },
    {
      id: 'lga',
      archiveSource: 'traffic_lga',
      label: 'Council Roads',
      short: 'Council',
      icon: 'fa-building-columns',
      color: '#a3e635',
      endpoint: '/api/traffic/lga',
      blurb:
        'Local-road records submitted by councils. A separate reporting ' +
        'stream from the state-road feeds — no overlap with them.',
    },
    {
      id: 'works',
      archiveSource: 'traffic_works',
      label: 'Works & ACT',
      short: 'Works',
      icon: 'fa-helmet-safety',
      color: '#fb923c',
      endpoint: '/api/traffic/works',
      blurb:
        'Works, utilities and light-rail closures from the aggregate web ' +
        'feed. The only Live Traffic source with ACT/Canberra coverage.',
    },
  ];

  const byId = {};
  const bySource = {};
  for (const f of FEEDS) {
    byId[f.id] = f;
    bySource[f.archiveSource] = f;
  }

  window.LiveTrafficVocab = {
    feeds: FEEDS,
    byId,
    bySource,
    /** Feed descriptor for an id or an archive source name. */
    get(key) {
      return byId[key] || bySource[key] || null;
    },
    /** Colour for an id/source, falling back to a neutral slate. */
    color(key) {
      const f = this.get(key);
      return f ? f.color : '#64748b';
    },
    /** Font Awesome class for an id/source. */
    icon(key) {
      const f = this.get(key);
      return f ? f.icon : 'fa-circle';
    },
    /** Human label for an id/source. */
    label(key) {
      const f = this.get(key);
      return f ? f.label : String(key || '');
    },
    /**
     * Standard panel header used by every Live Traffic filter panel, so
     * the eight feeds read as one family instead of eight unrelated
     * widgets. Returns an <h4> matching the existing panel markup.
     */
    panelHeader(key) {
      const f = this.get(key);
      if (!f) return '<h4>Live Traffic</h4>';
      return (
        '<h4><i class="fa-solid ' + f.icon + '" style="color:' + f.color + ';"></i> ' +
        f.label + '</h4>'
      );
    },
  };
})();
