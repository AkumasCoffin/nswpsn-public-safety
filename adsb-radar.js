/* Radar-scope dressing for a receiver's tracks map (feeder.html, staff.html).
 *
 * The map stays a Leaflet map, so tracks keep their hover labels and the map
 * still pans and zooms. This adds what makes it read as a scope around the
 * receiver: range rings at a round step, bearing spokes every 30 degrees with
 * compass labels, a green-tinted basemap (CSS class "adsb-radar" on the map
 * container), and a frame centred on the receiver rather than on the traffic.
 *
 * Rings are drawn on the map, not over it, so their distances stay true when
 * the map is zoomed.
 */
(function () {
  'use strict';

  const RING = '#4ade80';
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
      L.circle(c, {
        radius: km * 1000, interactive: false, fill: false,
        color: RING, weight: outer ? 1.4 : 1, opacity: outer ? 0.6 : 0.32,
        dashArray: outer ? null : '3 5',
      }).addTo(layer);
      // Distance just inside each ring, on the north-east diagonal where a
      // spoke does not run through it.
      label(dest(site.lat, site.lon, 45, km), km + ' km', 'adsb-radar-km').addTo(layer);
    }

    for (let b = 0; b < 360; b += 30) {
      L.polyline([c, dest(site.lat, site.lon, b, r.outer)], {
        interactive: false, color: RING, weight: 1, opacity: b % 90 === 0 ? 0.3 : 0.16,
      }).addTo(layer);
      const txt = b === 0 ? 'N' : b === 90 ? 'E' : b === 180 ? 'S' : b === 270 ? 'W' : String(b);
      label(dest(site.lat, site.lon, b, r.outer * 1.07), txt,
        b % 90 === 0 ? 'adsb-radar-cardinal' : 'adsb-radar-deg').addTo(layer);
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
    .adsb-radar { background: #020805; }
    .adsb-radar .leaflet-tile-pane {
      filter: sepia(1) hue-rotate(75deg) saturate(1.6) brightness(0.62) contrast(1.1);
    }
    .adsb-radar-lbl { background: none; border: 0; }
    .adsb-radar-lbl span {
      position: absolute; transform: translate(-50%, -50%); white-space: nowrap;
      font: 600 10px/1 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      color: #86efac; text-shadow: 0 0 3px #000, 0 0 3px #000; pointer-events: none;
    }
    .adsb-radar-lbl.adsb-radar-cardinal span { font-size: 12px; color: #bbf7d0; }
    .adsb-radar-lbl.adsb-radar-deg span { opacity: 0.7; }
    .adsb-radar-lbl.adsb-radar-km span { opacity: 0.85; transform: translate(4px, -50%); }
  `;
  if (!document.getElementById('adsb-radar-css')) {
    const el = document.createElement('style');
    el.id = 'adsb-radar-css';
    el.textContent = css;
    document.head.appendChild(el);
  }

  window.AdsbRadar = { build };
})();
