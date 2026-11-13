/* Polar plot of a receiver's tracks (feeder.html, staff.html).
 *
 * It is drawn with Leaflet, so tracks keep their hover labels and distances
 * stay true, but it is a plot and not a map: no basemap, and it cannot be
 * dragged or zoomed (MAP_OPTIONS). It is always the receiver in the middle,
 * out to its outer ring (frame). On a navy field (CSS class "adsb-radar" on
 * the container): thin range rings at a round step with their distances up
 * the north line, eight faint spokes, N/E/S/W.
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

  function bearing(a, b) {
    const la1 = toRad(a[0]), la2 = toRad(b[0]), dLon = toRad(b[1] - a[1]);
    const y = Math.sin(dLon) * Math.cos(la2);
    const x = Math.cos(la1) * Math.sin(la2) - Math.sin(la1) * Math.cos(la2) * Math.cos(dLon);
    return (Math.atan2(y, x) * 180 / Math.PI + 360) % 360;
  }

  /**
   * The traces as they should be drawn against the coverage envelope: no
   * point outside it.
   *
   * The envelope (`buckets`: km per bearing bucket, null where nothing was
   * heard) only counts a distance once it has been seen twice, so that one
   * bad decode cannot stretch it. A track draws every point. Left alone, a
   * track could poke past the coverage line it is supposed to be evidence
   * for: the one point that was not believed for the envelope was still
   * being drawn. It is measured from `site` as drawn, which is rounded, so
   * the cut agrees with the picture rather than with the exact pin.
   *
   * Each trace comes back with `points` (those inside, flat) and `lines`
   * (the same points in unbroken runs, so no line is drawn across a gap).
   * Traces left with no run of two points are dropped. With no site or no
   * envelope the traces are returned untouched.
   */
  function inside(site, traces, buckets) {
    const list = traces || [];
    if (!site || !Array.isArray(buckets) || !buckets.some((v) => typeof v === 'number' && v > 0)) return list;
    const c = [site.lat, site.lon];
    const n = buckets.length;
    const step = 360 / n;
    const within = (p) => {
      const i = Math.min(n - 1, Math.floor(bearing(c, p) / step));
      const lim = buckets[i];
      return typeof lim === 'number' && lim > 0 && distKm(c, p) <= lim;
    };
    const out = [];
    for (const tr of list) {
      const lines = [];
      let run = [];
      for (const p of tr.points || []) {
        if (within(p)) { run.push(p); continue; }
        if (run.length >= 2) lines.push(run);
        run = [];
      }
      if (run.length >= 2) lines.push(run);
      if (!lines.length) continue;
      out.push(Object.assign({}, tr, { points: lines.flat(), lines }));
    }
    return out;
  }

  /**
   * Options for L.map: a fixed plot. Nothing moves it, so there are no zoom
   * buttons, and with no basemap there is nothing to credit. zoomSnap 0 lets
   * the frame fit the outer ring exactly rather than at the nearest whole
   * zoom level.
   */
  const MAP_OPTIONS = {
    zoomControl: false, attributionControl: false,
    dragging: false, scrollWheelZoom: false, doubleClickZoom: false,
    boxZoom: false, keyboard: false, touchZoom: false,
    zoomSnap: 0, zoomAnimation: false, fadeAnimation: false,
  };

  /**
   * Fit `map` to `bounds` and keep it there: the plot is re-fitted whenever
   * its box changes size, since nothing else can bring it back to centre.
   */
  function frame(map, bounds) {
    map._adsbFrame = bounds;
    if (!map._adsbFrameHooked) {
      map._adsbFrameHooked = true;
      map.on('resize', () => {
        if (map._adsbFrame && map._adsbFrame.isValid()) map.fitBounds(map._adsbFrame, { animate: false });
      });
    }
    if (bounds && bounds.isValid()) map.fitBounds(bounds, { animate: false });
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
    .adsb-radar, .adsb-radar.leaflet-container { background: #0b1220; cursor: default; }
    .adsb-radar-lbl { background: none; border: 0; }
    .adsb-radar-lbl span {
      position: absolute; transform: translate(-50%, -50%); white-space: nowrap;
      font: 500 9.5px/1 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      color: #64748b; text-shadow: 0 0 3px #0b1220, 0 0 3px #0b1220; pointer-events: none;
    }
    .adsb-radar-lbl.adsb-radar-cardinal span { font-size: 10.5px; color: #94a3b8; }
    .adsb-radar-lbl.adsb-radar-km span { transform: translate(5px, -50%); }
  `;
  if (!document.getElementById('adsb-radar-css')) {
    const el = document.createElement('style');
    el.id = 'adsb-radar-css';
    el.textContent = css;
    document.head.appendChild(el);
  }

  window.AdsbRadar = { build, inside, frame, MAP_OPTIONS };
})();
