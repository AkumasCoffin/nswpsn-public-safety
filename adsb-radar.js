/* Polar-plot dressing for a receiver's tracks map (feeder.html, staff.html).
 *
 * The map stays a Leaflet map, so tracks keep their hover labels and the map
 * still pans and zooms. This makes it read as a plot around the receiver:
 * a navy field, thin range rings at a round step with their distances up the
 * north line, eight faint spokes, N/E/S/W, and a frame centred on the
 * receiver rather than on the traffic. The basemap stays as a faint grey
 * outline for bearings (CSS class "adsb-radar" on the map container).
 *
 * Rings are drawn on the map, not over it, so their distances stay true when
 * the map is zoomed.
 */
(function () {
  'use strict';

  const RING = '#94a3b8';   // rings, spokes and their labels: one quiet slate
  const FIELD = '#38bdf8';  // the faint disc the rings sit on
  const R_EARTH = 6371;

  function toRad(d) { return d * Math.PI / 180; }

  function distKm(a, b) {
    const dLat = toRad(b[0] - a[0]), dLon = toRad(b[1] - a[1]);
    const h = Math.sin(dLat / 2) ** 2
      + Math.cos(toRad(a[0])) * Math.cos(toRad(b[0])) * Math.sin(dLon / 2) ** 2;
    return 2 * R_EARTH * Math.asin(Math.min(1, Math.sqrt(h)));
  }

  function dest(lat, lon, bearingDeg, km) {
    const d = km / R_EARTH, br = toRad(bearingDeg), la1 = toRad(lat), lo1 = toRad(lon);
    const la2 = Math.asin(Math.sin(la1) * Math.cos(d) + Math.cos(la1) * Math.sin(d) * Math.cos(br));
    const lo2 = lo1 + Math.atan2(Math.sin(br) * Math.sin(d) * Math.cos(la1),
      Math.cos(d) - Math.sin(la1) * Math.sin(la2));
    return [la2 * 180 / Math.PI, ((lo2 * 180 / Math.PI) + 540) % 360 - 180];
  }

  /** Ring step and count for traffic reaching maxKm: three to five rings at a
   *  round distance, so each label is a number worth reading. */
  function rings(maxKm) {
    const km = Math.max(maxKm, 5);
    const steps = [5, 10, 20, 25, 50, 100, 150, 200];
    let step = steps[steps.length - 1];
    for (const s of steps) { if (km / s <= 5) { step = s; break; } }
    const n = Math.max(3, Math.ceil(km / step));
    return { step, n, outer: step * n };
  }

  function label(latlng, text, cls) {
    return L.marker(latlng, {
      interactive: false, keyboard: false,
      icon: L.divIcon({ className: 'adsb-radar-lbl ' + (cls || ''), html: '<span>' + text + '</span>', iconSize: null }),
    });
  }

  /**
   * Draw the scope around `site` ({lat, lon}) and return
   * { layer, outerKm, bounds }. `traces` is the tracks response's traces, each
   * with points [[lat, lon, ...], ...]; they only set the scale.
   */
  function build(site, traces) {
    let maxKm = 0;
    const c = [site.lat, site.lon];
    for (const tr of traces || []) {
      for (const p of tr.points || []) {
        const k = distKm(c, p);
        if (k > maxKm) maxKm = k;
      }
    }
    const r = rings(maxKm);
    const layer = L.layerGroup();

    for (let i = 1; i <= r.n; i += 1) {
      const km = i * r.step;
      const outer = i === r.n;
      // Solid hairlines; the outer one a touch stronger and carrying the
      // faint disc, so the plot has an edge without a frame.
      L.circle(c, {
        radius: km * 1000, interactive: false,
        color: RING, weight: 1, opacity: outer ? 0.42 : 0.2,
        fill: outer, fillColor: FIELD, fillOpacity: 0.035,
      }).addTo(layer);
      // Distances climb the north line, just right of it. Only the outer one
      // says "km": the rest are plainly the same unit.
      label(dest(site.lat, site.lon, 0, km), outer ? km + ' km' : String(km), 'adsb-radar-km').addTo(layer);
    }

    for (let b = 0; b < 360; b += 45) {
      const cardinal = b % 90 === 0;
      L.polyline([c, dest(site.lat, site.lon, b, r.outer)], {
        interactive: false, color: RING, weight: 1, opacity: cardinal ? 0.2 : 0.1,
      }).addTo(layer);
      if (!cardinal) continue;
      label(dest(site.lat, site.lon, b, r.outer * 1.07), 'NESW'[b / 90], 'adsb-radar-cardinal').addTo(layer);
    }

    const bounds = L.latLngBounds([
      dest(site.lat, site.lon, 0, r.outer * 1.12), dest(site.lat, site.lon, 90, r.outer * 1.12),
      dest(site.lat, site.lon, 180, r.outer * 1.12), dest(site.lat, site.lon, 270, r.outer * 1.12),
    ]);
    return { layer, outerKm: r.outer, bounds };
  }

  // Styles live here so both pages get the same scope. Labels are centred on
  // their point through the inner span: Leaflet positions the marker element
  // itself with a transform, so one set on it would be overwritten.
  const css = `
    .adsb-radar { background: #0b1220; }
    /* The map is context, not content: a faint grey outline under the plot. */
    .adsb-radar .leaflet-tile-pane { filter: grayscale(1) brightness(0.9); opacity: 0.28; }
    .adsb-radar-lbl { background: none; border: 0; }
    .adsb-radar-lbl span {
      position: absolute; transform: translate(-50%, -50%); white-space: nowrap;
      font: 500 9.5px/1 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      color: #64748b; text-shadow: 0 0 3px #0b1220, 0 0 3px #0b1220; pointer-events: none;
    }
    .adsb-radar-lbl.adsb-radar-cardinal span { font-size: 10.5px; color: #94a3b8; }
    .adsb-radar-lbl.adsb-radar-km span { transform: translate(5px, -50%); }
    /* Leaflet's own controls, toned down to sit on the navy field. */
    .adsb-radar .leaflet-bar a {
      background: #111a2c; color: #94a3b8; border-bottom-color: rgba(148,163,184,0.18);
    }
    .adsb-radar .leaflet-bar a:hover { background: #18243b; color: #e2e8f0; }
    .adsb-radar .leaflet-bar { border: 1px solid rgba(148,163,184,0.22); box-shadow: none; }
    .adsb-radar .leaflet-control-attribution {
      background: rgba(11,18,32,0.7); color: #64748b; font-size: 9px;
    }
    .adsb-radar .leaflet-control-attribution a { color: #7c8ba1; }
  `;
  if (!document.getElementById('adsb-radar-css')) {
    const el = document.createElement('style');
    el.id = 'adsb-radar-css';
    el.textContent = css;
    document.head.appendChild(el);
  }

  window.AdsbRadar = { build };
})();
