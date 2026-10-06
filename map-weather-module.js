/* ==========================================================================
   NSW PSN — Map Weather Field Module
   ==========================================================================
   Renders the gridded weather field from /api/weather/grid as a continuous
   Windy-style colour wash on the Leaflet map, with animated wind particles
   on top. Self-contained: include it with a <script> tag after Leaflet and
   call WeatherField.create(map, opts). Nothing here touches the DOM or
   Leaflet until create() runs, so load order only has to put Leaflet first
   by the time the map exists — not by the time this file parses.

   THE DATA CONTRACT (backends/node/src/sources/weatherGrid.ts is authority)
     geometry  { west, south, stepDeg, cols, rows } — 0.5 deg over Australia,
               85 x 69 = 5,865 cells.
     values    Int16Array, one entry per cell, ROW-MAJOR FROM THE SOUTH-WEST
               CORNER. Index 0 is the bottom-left; index runs east, then
               north by rows.
     NODATA    -32768 means genuinely absent (wave height inland). It is not
               a zero and must never be given a colour.
     real      stored / scale, with scale from the manifest's vars entry.

   WHY THE FIELD IS NOT INTERPOLATED PER PIXEL
   A 1920x1080 viewport is ~2M pixels; bilinear-sampling the grid for each
   one, each frame, in JS is hopeless. Instead the grid is colour-mapped into
   an offscreen canvas at ONE PIXEL PER CELL and that ~85x138 image is
   drawImage'd up to the viewport with smoothing on. The browser's own
   bilinear scaler produces the smooth gradient for free, on the GPU, and the
   expensive colour mapping happens once per data change rather than once per
   frame.

   Depends only on globals the map page already has: L (Leaflet 1.9.x).
   ========================================================================== */
(function () {
  'use strict';

  /** Sentinel for "no observation here", from the backend. */
  const NODATA = -32768;

  // Leaflet's stock panes are tilePane 200, overlayPane 400, markerPane 600.
  // The field goes at 250: above the basemap, below boundaries and pins. A
  // weather wash that covers the incident markers is a weather wash nobody
  // can use.
  const FIELD_PANE = 'weatherFieldPane';
  const PARTICLE_PANE = 'weatherParticlePane';
  const FIELD_Z = 250;
  const PARTICLE_Z = 251;

  // Device pixel ratio is capped: a 3x phone screen would otherwise make the
  // particle canvas nine times the fill cost for no visible gain on 1px lines.
  const MAX_DPR = 2;

  // ------------------------------------------------------------------------
  // Colour scales
  // ------------------------------------------------------------------------
  // Ordered stops, each [value, r, g, b] or [value, r, g, b, alpha]. Alpha
  // defaults to 255. Values ascend; below the first stop and above the last
  // the colour clamps rather than extrapolating, because an extrapolated
  // channel wraps into a wrong hue and a wrong hue reads as real weather.
  //
  // EXPORTED on purpose: the legend must be generated from this same table.
  // A legend that drifts from the map is worse than no legend at all.
  const SCALES = {
    // Rain accumulated over the next 24 hours. A much wider range than the
    // per-step precipitation ramp, which tops out where this one starts —
    // reusing that ramp would paint every wet day the same saturated red.
    precip_accum: {
      unit: 'mm',
      stops: [
        [0, 40, 80, 120, 0],
        [1, 70, 150, 200, 0.4],
        [5, 80, 200, 170],
        [15, 230, 220, 90],
        [30, 240, 150, 60],
        [60, 230, 70, 70],
        [120, 170, 40, 170]
      ]
    },
    // Relative humidity. Dry is left deliberately near-transparent: the
    // interesting end is the muggy one, and a brown wash over every desert is
    // not information.
    relative_humidity_2m: {
      unit: '%',
      stops: [
        [0, 120, 80, 40, 0.05],
        [20, 150, 120, 60, 0.35],
        [40, 120, 160, 120],
        [60, 60, 170, 170],
        [80, 40, 130, 200],
        [100, 30, 70, 190]
      ]
    },
    // Mean sea-level pressure. The band is narrow on purpose — real pressure
    // almost never leaves 960-1040, and a scale wide enough for the
    // theoretical range would render every weather system the same colour.
    pressure_msl: {
      unit: 'hPa',
      stops: [
        [960, 140, 40, 160],
        [980, 90, 70, 200],
        [1000, 60, 150, 200],
        [1013, 120, 200, 170],
        [1025, 230, 200, 90],
        [1040, 230, 120, 60]
      ]
    },
    // UV index, banded to the public advisory thresholds rather than a smooth
    // ramp — 3, 6, 8 and 11 are the numbers the advice actually changes at, so
    // the colour should change there too.
    uv_index: {
      unit: '',
      stops: [
        [0, 40, 90, 120, 0.1],
        [3, 90, 190, 120],
        [6, 240, 210, 80],
        [8, 240, 140, 60],
        [11, 220, 60, 60],
        [15, 150, 40, 170]
      ]
    },
    // CAPE — convective available potential energy, i.e. thunderstorm fuel.
    // Near-transparent below ~300 because most of the map is not convective
    // most of the time, and the whole point is to see where it IS.
    cape: {
      unit: 'J/kg',
      stops: [
        [0, 60, 80, 120, 0],
        [300, 90, 160, 200, 0.3],
        [800, 230, 210, 90],
        [1500, 240, 150, 60],
        [2500, 230, 70, 60],
        [4000, 170, 40, 160]
      ]
    },
    // Swell height. Deliberately NOT the same ramp as total wave height: a
    // two-metre groundswell and a two-metre windchop are the same number and
    // completely different days on the water, so they should not look alike.
    swell_wave_height: {
      unit: 'm',
      stops: [
        [0, 30, 60, 110, 0.2],
        [1, 50, 130, 190],
        [2, 70, 190, 190],
        [3, 220, 200, 110],
        [5, 230, 120, 70],
        [8, 200, 50, 90]
      ]
    },
    // Surface current. Metres per second, and the top of the scale is low
    // because anything over about 1.5 m/s is a genuinely strong current.
    ocean_current_velocity: {
      unit: 'm/s',
      stops: [
        [0, 30, 60, 100, 0.1],
        [0.25, 60, 150, 170],
        [0.5, 90, 200, 150],
        [1, 230, 200, 90],
        [1.5, 230, 120, 70],
        [2.5, 200, 50, 90]
      ]
    },
    // US AQI, banded at the published breakpoints (50/100/150/200/300) and
    // coloured with the standard palette, because those colours are already
    // what people have seen on every other air-quality map.
    us_aqi: {
      unit: 'AQI',
      stops: [
        [0, 80, 200, 120, 0.25],
        [50, 230, 220, 90],
        [100, 240, 160, 70],
        [150, 230, 80, 70],
        [200, 160, 60, 160],
        [300, 130, 30, 60]
      ]
    },
    // Fine particulates. The layer that matters on a smoke day.
    pm2_5: {
      unit: 'µg/m³',
      stops: [
        [0, 80, 200, 120, 0.2],
        [12, 230, 220, 90],
        [35, 240, 160, 70],
        [55, 230, 80, 70],
        [150, 160, 60, 160],
        [250, 130, 30, 60]
      ]
    },
    // Degrees Celsius. Range covers Australian extremes with headroom at both
    // ends — Oodnadatta summers and alpine winters are both inside it.
    temperature_2m: {
      unit: '°C',
      stops: [
        [-30, 60, 0, 90],
        [-20, 90, 20, 150],
        [-10, 40, 80, 220],
        [0, 70, 160, 230],
        [5, 120, 210, 230],
        [10, 160, 230, 180],
        [15, 200, 240, 130],
        [20, 250, 230, 100],
        [25, 250, 180, 70],
        [30, 240, 120, 50],
        [35, 220, 50, 40],
        [40, 170, 20, 40],
        [50, 110, 10, 60]
      ]
    },
    // Millimetres in the timestep (3-hourly). The zero stop is deliberately
    // transparent: dry ground is most of Australia most of the time, and a
    // faint blue veil over the whole continent would hide the basemap for no
    // information. Note this is a property of the SCALE, not of NODATA —
    // absent data is transparent under every scale.
    precipitation: {
      unit: 'mm',
      stops: [
        [0, 160, 220, 255, 0],
        [0.2, 150, 210, 250, 90],
        [1, 80, 170, 240, 160],
        [2.5, 50, 130, 230, 200],
        [5, 50, 200, 130, 215],
        [10, 240, 220, 80, 230],
        [20, 240, 140, 50, 240],
        [40, 220, 50, 60, 245],
        [80, 160, 40, 160, 250]
      ]
    },
    // km/h, matching Open-Meteo's default wind unit and the unit Australians
    // hear in forecasts. 140 tops out above any non-cyclonic 10m reading.
    wind_speed_10m: {
      unit: 'km/h',
      stops: [
        [0, 60, 100, 160],
        [10, 70, 150, 200],
        [20, 90, 200, 190],
        [30, 130, 220, 140],
        [45, 230, 220, 100],
        [60, 240, 160, 60],
        [80, 230, 80, 60],
        [100, 190, 40, 120],
        [140, 120, 20, 140]
      ]
    },
    // Significant wave height in metres.
    wave_height: {
      unit: 'm',
      stops: [
        [0, 20, 60, 120],
        [0.5, 30, 110, 180],
        [1, 50, 170, 210],
        [1.5, 80, 210, 200],
        [2, 140, 230, 160],
        [3, 230, 220, 110],
        [4, 240, 160, 70],
        [6, 225, 70, 60],
        [8, 170, 30, 110],
        [12, 110, 20, 130]
      ]
    }
  };

  // Variables that share another variable's scale. Apparent temperature is
  // still a temperature; a gust is still a wind speed. Keeping them aliased
  // means the two layers are directly comparable by eye.
  // Aliases are consulted BEFORE SCALES, so an entry here overrides a
  // dedicated ramp of the same name — which silently happened to gusts and
  // swell: both had their own scale written and neither was ever used. Only
  // list a variable here if it genuinely has no scale of its own.
  const SCALE_ALIASES = {
    apparent_temperature: 'temperature_2m',
    // Gusts deliberately share the sustained-wind ramp. They are read AGAINST
    // each other - "gusting to 80 where it is blowing 45" - and that
    // comparison only works if the same number is the same colour on both.
    // A dedicated gust ramp was written here once and removed for this reason.
    wind_gusts_10m: 'wind_speed_10m'
  };

  /**
   * The colour scale for a variable, or null if it has none.
   *
   * Direction variables (wind_direction_10m, wave_direction) return null on
   * purpose: 0-360 run through a linear ramp is a meaningless rainbow, and a
   * direction is an input to the particle field rather than a field itself.
   */
  function scaleFor(name) {
    const key = SCALE_ALIASES[name] || name;
    return SCALES[key] || null;
  }

  function stopAlpha(stop) {
    return stop.length > 4 ? stop[4] : 255;
  }

  /**
   * Interpolate a scale at `value`, clamping outside its ends.
   *
   * Non-finite input becomes fully transparent rather than the bottom stop —
   * a NaN that renders as "coldest" is a lie, whereas a hole is visibly a
   * hole.
   */
  function colorFor(stops, value) {
    const blank = { r: 0, g: 0, b: 0, a: 0 };
    if (!stops || !stops.length) return blank;
    if (typeof value !== 'number' || !isFinite(value)) return blank;
    const last = stops.length - 1;
    if (value <= stops[0][0]) {
      return { r: stops[0][1], g: stops[0][2], b: stops[0][3], a: stopAlpha(stops[0]) };
    }
    if (value >= stops[last][0]) {
      return { r: stops[last][1], g: stops[last][2], b: stops[last][3], a: stopAlpha(stops[last]) };
    }
    for (let i = 1; i <= last; i += 1) {
      if (value <= stops[i][0]) {
        const lo = stops[i - 1];
        const hi = stops[i];
        const span = hi[0] - lo[0];
        const t = span > 0 ? (value - lo[0]) / span : 0;
        return {
          r: Math.round(lo[1] + (hi[1] - lo[1]) * t),
          g: Math.round(lo[2] + (hi[2] - lo[2]) * t),
          b: Math.round(lo[3] + (hi[3] - lo[3]) * t),
          a: Math.round(stopAlpha(lo) + (stopAlpha(hi) - stopAlpha(lo)) * t)
        };
      }
    }
    return blank;
  }

  function rgbaCss(c) {
    return 'rgba(' + c.r + ',' + c.g + ',' + c.b + ',' + (c.a / 255).toFixed(3) + ')';
  }

  /**
   * Everything a legend needs, derived from SCALES so it cannot drift: the
   * stop list with its colours, the range, and a ready-made CSS gradient
   * whose colour positions match the map's own interpolation.
   */
  function legendFor(name) {
    const scale = scaleFor(name);
    if (!scale) return null;
    const stops = scale.stops;
    const min = stops[0][0];
    const max = stops[stops.length - 1][0];
    const span = max - min;
    const entries = stops.map((s) => {
      const c = { r: s[1], g: s[2], b: s[3], a: stopAlpha(s) };
      return {
        value: s[0],
        color: rgbaCss(c),
        // Position along the bar. Linear in VALUE, like the map, so a
        // perceptual mid-point on the legend is the same number on the field.
        offset: span > 0 ? (s[0] - min) / span : 0
      };
    });
    return {
      variable: name,
      unit: scale.unit,
      min: min,
      max: max,
      stops: entries,
      gradientCss:
        'linear-gradient(to right,' +
        entries.map((e) => e.color + ' ' + (e.offset * 100).toFixed(2) + '%').join(',') +
        ')'
    };
  }

  // ------------------------------------------------------------------------
  // Grid maths
  // ------------------------------------------------------------------------

  /** Unpack one stored Int16, NODATA becoming null. Mirrors the backend. */
  function dequantise(stored, scale) {
    if (stored === NODATA) return null;
    const s = scale > 0 ? scale : 1;
    return stored / s;
  }

  /** Web Mercator y for a latitude in degrees (unit sphere, no radius). */
  function mercY(lat) {
    const clamped = lat > 85.05 ? 85.05 : lat < -85.05 ? -85.05 : lat;
    return Math.log(Math.tan(Math.PI / 4 + (clamped * Math.PI) / 360));
  }

  /** Inverse of mercY. */
  function mercLat(y) {
    return ((Math.atan(Math.exp(y)) - Math.PI / 4) * 360) / Math.PI;
  }

  /**
   * Which source row each output image row should take, so the cell image can
   * be pre-stretched into Mercator y.
   *
   * Without this the field is drawn with latitude spaced linearly down the
   * screen, which over Australia's 34 degrees puts the middle of the image
   * about 4% of the field height away from where Mercator puts it — tens of
   * pixels at continent zoom, enough that a cold front visibly sits off its
   * coastline. The mapping depends only on the latitude range, not on zoom or
   * pan (Mercator y is linear in screen y up to scale and offset), so it is
   * computed once per geometry and reused.
   *
   * Returned entries are fractional source rows counted FROM THE SOUTH, index
   * 0 being the southern edge of the image.
   */
  function rowSampleMap(geometry, outRows) {
    const step = geometry.stepDeg;
    const half = step / 2;
    // The grid is point-sampled, so the image spans half a cell beyond the
    // outermost sample in each direction.
    const southEdge = geometry.south - half;
    const northEdge = geometry.south + (geometry.rows - 1) * step + half;
    const yS = mercY(southEdge);
    const yN = mercY(northEdge);
    const out = new Float64Array(outRows);
    for (let j = 0; j < outRows; j += 1) {
      const t = (j + 0.5) / outRows;
      const lat = mercLat(yS + (yN - yS) * t);
      let row = (lat - geometry.south) / step;
      if (row < 0) row = 0;
      if (row > geometry.rows - 1) row = geometry.rows - 1;
      out[j] = row;
    }
    return out;
  }

  /**
   * Colour-map the grid into RGBA, one pixel per cell across.
   *
   * THE ROW FLIP IS THE WHOLE POINT. Grid row 0 is the SOUTHERN edge; canvas
   * y 0 is the TOP of the image. Writing rows straight through renders
   * Australia upside down — Tasmania off Cape York — which is subtle enough
   * to ship unnoticed, so it has its own test.
   *
   * `rowMap` (from rowSampleMap) optionally remaps output rows onto fractional
   * source rows; the nearest source row is taken rather than blended, because
   * blending across a NODATA neighbour would average -32768 into a real value
   * and paint a wild colour in the middle of the field. The browser's upscale
   * does the smoothing anyway.
   *
   * Writes into `out` (a Uint8ClampedArray / array of cols*outRows*4) and
   * returns the number of rows written.
   */
  function paintCells(values, cols, rows, stops, scale, out, rowMap) {
    const outRows = rowMap ? rowMap.length : rows;
    const s = scale > 0 ? scale : 1;
    for (let y = 0; y < outRows; y += 1) {
      // Output row 0 is the north edge, so count the source row down from the
      // top of the (south-origin) grid.
      const fromSouth = outRows - 1 - y;
      const srcRow = rowMap ? Math.round(rowMap[fromSouth]) : fromSouth;
      const srcBase = srcRow * cols;
      const dstBase = y * cols * 4;
      for (let col = 0; col < cols; col += 1) {
        const o = dstBase + col * 4;
        const stored = values[srcBase + col];
        if (stored === NODATA || stored === undefined) {
          // Genuinely absent. Zero the colour too: `out` may be a reused
          // buffer, and a stale RGB under alpha 0 can still bleed through a
          // smoothed upscale.
          out[o] = 0;
          out[o + 1] = 0;
          out[o + 2] = 0;
          out[o + 3] = 0;
          continue;
        }
        const c = colorFor(stops, stored / s);
        out[o] = c.r;
        out[o + 1] = c.g;
        out[o + 2] = c.b;
        out[o + 3] = c.a;
      }
    }
    return outRows;
  }

  /**
   * Bilinear sample of the grid at fractional cell coordinates (cx east from
   * the west edge, cy north from the SOUTH edge), in real units.
   *
   * Returns null outside the grid or when any of the four corners is NODATA.
   * Refusing a partial stencil is deliberate: silently treating absent as
   * zero would make wind blow out of a coastline into the interior.
   */
  function sampleBilinear(values, cols, rows, scale, cx, cy) {
    if (!(cx >= 0) || !(cy >= 0) || cx > cols - 1 || cy > rows - 1) return null;
    const s = scale > 0 ? scale : 1;
    const x0 = Math.floor(cx);
    const y0 = Math.floor(cy);
    const x1 = x0 + 1 > cols - 1 ? x0 : x0 + 1;
    const y1 = y0 + 1 > rows - 1 ? y0 : y0 + 1;
    const fx = cx - x0;
    const fy = cy - y0;
    const v00 = values[y0 * cols + x0];
    const v10 = values[y0 * cols + x1];
    const v01 = values[y1 * cols + x0];
    const v11 = values[y1 * cols + x1];
    if (v00 === NODATA || v10 === NODATA || v01 === NODATA || v11 === NODATA) return null;
    if (v00 === undefined || v10 === undefined || v01 === undefined || v11 === undefined) return null;
    const bottom = v00 + (v10 - v00) * fx;
    const top = v01 + (v11 - v01) * fx;
    return (bottom + (top - bottom) * fy) / s;
  }

  /**
   * Meteorological wind direction to a velocity vector.
   *
   * Direction is the direction the wind comes FROM — this is the classic sign
   * error in every wind renderer. 0 degrees is a northerly, which BLOWS
   * TOWARD THE SOUTH, so the vector must point south. Hence the leading
   * minus on both components.
   *
   * Returns { u, v } with u positive EAST and v positive NORTH, in whatever
   * unit `speed` arrived in. Canvas y grows downward, so a screen offset is
   * (u, -v).
   */
  function windUV(speed, dirFromDeg) {
    if (typeof speed !== 'number' || !isFinite(speed)) return null;
    if (typeof dirFromDeg !== 'number' || !isFinite(dirFromDeg)) return null;
    const rad = (dirFromDeg * Math.PI) / 180;
    return {
      u: -speed * Math.sin(rad),
      v: -speed * Math.cos(rad)
    };
  }

  // ------------------------------------------------------------------------
  // Motion policy
  // ------------------------------------------------------------------------

  /**
   * How many particles a viewport of this size should carry.
   *
   * Scaled by area, then capped. The cap is driven by the SHORT side rather
   * than the area because the thing being protected is phone battery: a
   * full-viewport rAF loop is the most expensive thing on this page, and a
   * narrow viewport is the signal for "this is a handset".
   */
  function particleBudget(width, height) {
    const w = width > 0 ? width : 0;
    const h = height > 0 ? height : 0;
    const shortSide = Math.min(w, h);
    // ~1 particle per 4,000 CSS px^2: about 320 on a 1440x900 window and
    // about 520 on a 1080p one, which is dense enough to read the flow
    // without the stroke pass becoming the frame budget.
    let n = Math.round((w * h) / 4000);
    const cap = shortSide < 480 ? 110 : shortSide < 700 ? 320 : 900;
    if (n > cap) n = cap;
    if (n < 24) n = 24;
    return n;
  }

  /** Read the OS accessibility setting. Guarded: matchMedia is not universal. */
  function prefersReducedMotion() {
    try {
      return !!(
        typeof window !== 'undefined' &&
        window.matchMedia &&
        window.matchMedia('(prefers-reduced-motion: reduce)').matches
      );
    } catch (e) {
      return false;
    }
  }

  /** 'arrows' when the user has asked for less motion, otherwise 'particles'. */
  function animationMode() {
    return prefersReducedMotion() ? 'arrows' : 'particles';
  }

  /**
   * The rAF driver, with the reduced-motion decision inside it.
   *
   * Reduced motion means NO LOOP AT ALL — not a slower one. A slowed loop is
   * still a wake-up every frame and still a battery complaint, and the setting
   * is a request for stillness, not for sluggishness. So the arrow path draws
   * once and returns without ever calling requestAnimationFrame.
   *
   * `host` supplies { reducedMotion, raf, caf, step, drawStatic }, which is
   * also what makes this decision testable without a canvas.
   */
  function createAnimator(host) {
    let rafId = null;
    let running = false;
    let mode = null;
    let lastTs = 0;

    function stop() {
      if (rafId !== null && host.caf) {
        try {
          host.caf(rafId);
        } catch (e) {
          /* a cancel that throws must not take the layer down */
        }
      }
      rafId = null;
      running = false;
    }

    function frame(ts) {
      // A frame can land after stop(): rAF callbacks already queued still
      // fire. Bailing here is what guarantees the loop actually ends.
      if (!running) return;
      const now = typeof ts === 'number' ? ts : 0;
      // Clamp the delta. A tab restored after thirty seconds hidden would
      // otherwise teleport every particle clean off the screen in one step.
      let dt = lastTs ? now - lastTs : 16;
      if (!(dt > 0) || dt > 64) dt = 16;
      lastTs = now;
      host.step(dt);
      if (!running) return;
      rafId = host.raf(frame);
    }

    function start() {
      stop();
      if (host.reducedMotion()) {
        mode = 'arrows';
        if (host.drawStatic) host.drawStatic();
        return mode;
      }
      mode = 'particles';
      running = true;
      lastTs = 0;
      rafId = host.raf(frame);
      return mode;
    }

    return {
      start: start,
      stop: stop,
      isRunning: function () {
        return running;
      },
      mode: function () {
        return mode;
      }
    };
  }

  // ------------------------------------------------------------------------
  // The Leaflet layer
  // ------------------------------------------------------------------------

  let LayerClass = null;

  /**
   * Built lazily, not at parse time.
   *
   * L.Layer.extend at the top level would throw if this file were ever placed
   * before Leaflet in the page, and would make the module unloadable in a
   * plain-node test harness. Neither is worth a top-level dependency.
   */
  function layerClass() {
    if (LayerClass) return LayerClass;
    if (typeof L === 'undefined' || !L.Layer) {
      throw new Error('WeatherField: Leaflet (L) is not loaded');
    }

    LayerClass = L.Layer.extend({
      initialize: function (opts) {
        this._o = opts || {};
        this._geometry = this._o.geometry || null;
        this._variable = this._o.variable || null;
        this._values = null;
        this._valueScale = 1;
        this._windSpeed = null;
        this._windSpeedScale = 1;
        this._windDir = null;
        this._windDirScale = 1;
        this._cellCanvas = null;
        this._cellCtx = null;
        this._cellRows = 0;
        this._rowMap = null;
        this._particles = null;
        this._proj = null;
        this._animated = this._o.animated !== false;
        this._size = { x: 0, y: 0 };
        this._dpr = 1;
        this._windPxPerSec = this._o.windSpeedPxPerSec > 0 ? this._o.windSpeedPxPerSec : 0.9;
        this._opacity = typeof this._o.opacity === 'number' ? this._o.opacity : 0.88;
        this._onVisibility = this._visibilityChanged.bind(this);
      },

      onAdd: function (map) {
        this._map = map;
        ensurePane(map, this._o.fieldPane || FIELD_PANE, this._o.fieldZIndex || FIELD_Z);
        ensurePane(map, this._o.particlePane || PARTICLE_PANE, this._o.particleZIndex || PARTICLE_Z);

        this._field = makeCanvas(map, this._o.fieldPane || FIELD_PANE, 'weather-field-canvas');
        this._wind = makeCanvas(map, this._o.particlePane || PARTICLE_PANE, 'weather-wind-canvas');
        if (this._field) this._field.el.style.opacity = String(this._opacity);

        this._animator = createAnimator({
          reducedMotion: prefersReducedMotion,
          raf: windowRaf,
          caf: windowCaf,
          step: this._stepParticles.bind(this),
          drawStatic: this._drawArrows.bind(this)
        });

        // A loop that survives the tab going away is the battery complaint, so
        // the listener is not optional. Removed again in onRemove.
        try {
          document.addEventListener('visibilitychange', this._onVisibility);
        } catch (e) {
          /* no document in this host; nothing to listen to */
        }

        this._resize();
        this._rebuildCells();
        this._reset();
        this._syncAnimation();
      },

      onRemove: function () {
        if (this._animator) this._animator.stop();
        try {
          document.removeEventListener('visibilitychange', this._onVisibility);
        } catch (e) {
          /* nothing was listening */
        }
        [this._field, this._wind].forEach(function (c) {
          if (c && c.el && c.el.parentNode) c.el.parentNode.removeChild(c.el);
        });
        this._field = null;
        this._wind = null;
        this._particles = null;
        this._cellCanvas = null;
        this._cellCtx = null;
        this._map = null;
      },

      getEvents: function () {
        // `move` fires every drag frame, and that is fine: a field redraw is a
        // clearRect plus one drawImage of a tiny bitmap. The costly colour
        // mapping only reruns on a data change.
        return {
          move: this._reset,
          moveend: this._reset,
          zoomend: this._reset,
          viewreset: this._reset,
          resize: this._onResize
        };
      },

      // --- public-ish surface, driven by the facade ---

      setVariable: function (name) {
        if (this._variable === name) return;
        this._variable = name;
        // The values on hand belong to the old variable; keep painting them
        // and the field would be the new scale over the wrong numbers. Drop
        // them and show nothing until setData arrives.
        this._values = null;
        this._cellRows = 0;
        this._rebuildCells();
        this._reset();
      },

      setData: function (values, manifestVar) {
        const mv = manifestVar || {};
        if (mv.name) this._variable = mv.name;
        let scale = mv.scale;
        if (!(scale > 0)) {
          // Dividing by 1 when the manifest meant 10 renders temperature ten
          // times too hot and looks plausible, so it has to be said out loud.
          console.warn('[weather] no scale for', this._variable, '- values will be unscaled');
          scale = 1;
        }
        this._values = values || null;
        this._valueScale = scale;
        this._rebuildCells();
        this._reset();
      },

      /**
       * Wind is sampled on its own geometry, not the field's.
       *
       * Particles overlay EVERY layer, the way Windy does — so the field
       * underneath may be on the coarse marine or air grid while the wind
       * arrays are still the land grid they were fetched on. Sharing one
       * geometry would index the wind array with the wrong stride and send
       * every particle off in a confidently wrong direction.
       */
      setWindGeometry: function (geometry) {
        this._windGeometry = geometry || null;
        this._seedParticles();
      },

      _windGeo: function () {
        return this._windGeometry || this._geometry;
      },

      setWind: function (speed, dir, speedVar, dirVar) {
        this._windSpeed = speed || null;
        this._windDir = dir || null;
        this._windSpeedScale = speedVar && speedVar.scale > 0 ? speedVar.scale : 1;
        this._windDirScale = dirVar && dirVar.scale > 0 ? dirVar.scale : 1;
        this._seedParticles();
        this._syncAnimation();
      },

      setGeometry: function (geometry) {
        this._geometry = geometry || null;
        this._rowMap = null;
        this._cellCanvas = null;
        this._rebuildCells();
        this._reset();
      },

      setAnimated: function (on) {
        this._animated = !!on;
        this._syncAnimation();
      },

      setOpacity: function (value) {
        this._opacity = typeof value === 'number' ? value : this._opacity;
        if (this._field && this._field.el) this._field.el.style.opacity = String(this._opacity);
      },

      isAnimating: function () {
        return !!(this._animator && this._animator.isRunning());
      },

      animationMode: function () {
        return this._animator ? this._animator.mode() : null;
      },

      variable: function () {
        return this._variable;
      },

      /** Real-unit value of the active field at a point, or null. */
      valueAt: function (lat, lon) {
        const g = this._geometry;
        if (!g || !this._values) return null;
        return sampleBilinear(
          this._values,
          g.cols,
          g.rows,
          this._valueScale,
          (lon - g.west) / g.stepDeg,
          (lat - g.south) / g.stepDeg
        );
      },

      /** Wind at a point as { speed, direction, u, v }, or null. */
      windAt: function (lat, lon) {
        const g = this._windGeo();
        if (!g || !this._windSpeed || !this._windDir) return null;
        const cx = (lon - g.west) / g.stepDeg;
        const cy = (lat - g.south) / g.stepDeg;
        const speed = sampleBilinear(this._windSpeed, g.cols, g.rows, this._windSpeedScale, cx, cy);
        const dir = sampleDirection(this._windDir, g, this._windDirScale, cx, cy);
        if (speed === null || dir === null) return null;
        const uv = windUV(speed, dir);
        if (!uv) return null;
        return { speed: speed, direction: dir, u: uv.u, v: uv.v };
      },

      // --- internals ---

      _visibilityChanged: function () {
        // Hidden tab: stop dead. Visible again: start only if everything else
        // still says we should be animating.
        if (document && document.hidden) {
          if (this._animator) this._animator.stop();
        } else {
          this._syncAnimation();
        }
      },

      _syncAnimation: function () {
        if (!this._animator || !this._map) return;
        const haveWind = !!(this._windSpeed && this._windDir && this._windGeo());
        const hidden = !!(typeof document !== 'undefined' && document.hidden);
        if (!this._animated || !haveWind || hidden) {
          this._animator.stop();
          clearCanvas(this._wind, this._size);
          return;
        }
        if (!this._particles) this._seedParticles();
        this._animator.start();
      },

      _onResize: function () {
        this._resize();
        this._seedParticles();
        this._reset();
      },

      _resize: function () {
        if (!this._map) return;
        const size = this._map.getSize();
        this._size = { x: size.x, y: size.y };
        let dpr = 1;
        try {
          dpr = window.devicePixelRatio || 1;
        } catch (e) {
          dpr = 1;
        }
        this._dpr = Math.min(MAX_DPR, dpr > 0 ? dpr : 1);
        sizeCanvas(this._field, this._size, this._dpr);
        sizeCanvas(this._wind, this._size, this._dpr);
      },

      /** Colour-map the grid once, into the small offscreen canvas. */
      _rebuildCells: function () {
        const g = this._geometry;
        const scale = scaleFor(this._variable);
        this._cellRows = 0;
        if (!g || !this._values || !scale) return;
        if (this._values.length < g.cols * g.rows) {
          console.warn(
            '[weather] grid is', this._values.length, 'values but geometry wants', g.cols * g.rows
          );
          return;
        }
        // Interpolated up here rather than by the browser — see UPSAMPLE.
        const up = upsampleGrid(this._values, g.cols, g.rows, UPSAMPLE, NODATA);
        // The dense grid covers the same ground, so its geometry is the same
        // extent with a proportionally smaller step. The Mercator row map
        // needs that to place rows correctly.
        const ug = {
          south: g.south,
          west: g.west,
          stepDeg: g.stepDeg * (g.rows - 1) / (up.rows - 1),
          cols: up.cols,
          rows: up.rows
        };
        const outRows = up.rows;
        if (!this._rowMap || this._rowMap.length !== outRows) {
          this._rowMap = rowSampleMap(ug, outRows);
        }
        if (!this._cellCanvas || this._cellCanvas.width !== up.cols || this._cellCanvas.height !== outRows) {
          const made = offscreen(up.cols, outRows);
          if (!made) return;
          this._cellCanvas = made.el;
          this._cellCtx = made.ctx;
        }
        if (!this._cellCtx) return;
        const img = this._cellCtx.createImageData(up.cols, outRows);
        paintCells(up.values, up.cols, up.rows, scale.stops, this._valueScale, img.data, this._rowMap);
        this._cellCtx.putImageData(img, 0, 0);
        this._cellRows = outRows;
      },

      /** Reposition both canvases to the viewport and repaint. */
      _reset: function () {
        if (!this._map) return;
        const origin = this._map.containerPointToLayerPoint([0, 0]);
        if (this._field) L.DomUtil.setPosition(this._field.el, origin);
        if (this._wind) L.DomUtil.setPosition(this._wind.el, origin);
        this._recomputeProjection();
        this._drawField();
        // Trails are drawn in screen space, so any pan or zoom invalidates
        // every pixel of them. Clear rather than smear.
        clearCanvas(this._wind, this._size);
        if (this._animator && this._animator.mode() === 'arrows') this._drawArrows();
      },

      /**
       * Cache the container-pixel to grid-cell transform.
       *
       * Calling map.containerPointToLatLng per particle per frame is thousands
       * of Leaflet calls and object allocations a second. Longitude is linear
       * in screen x and Mercator y is linear in screen y, so the whole mapping
       * is four numbers. (This assumes the viewport does not straddle the
       * antimeridian, which for an Australian map it does not.)
       */
      _recomputeProjection: function () {
        const g = this._geometry;
        const w = this._size.x;
        const h = this._size.y;
        if (!this._map || !g || !(w > 0) || !(h > 0)) {
          this._proj = null;
          return;
        }
        const nw = this._map.containerPointToLatLng([0, 0]);
        const se = this._map.containerPointToLatLng([w, h]);
        const yTop = mercY(nw.lat);
        const yBot = mercY(se.lat);
        this._proj = {
          lon0: nw.lng,
          lonPerPx: (se.lng - nw.lng) / w,
          yTop: yTop,
          yPerPx: (yBot - yTop) / h
        };
      },

      /** Screen point -> fractional grid cell coords, or null off-grid. */
      _cellAt: function (px, py) {
        const p = this._proj;
        const g = this._geometry;
        if (!p || !g) return null;
        const lon = p.lon0 + px * p.lonPerPx;
        const lat = mercLat(p.yTop + py * p.yPerPx);
        return {
          cx: (lon - g.west) / g.stepDeg,
          cy: (lat - g.south) / g.stepDeg
        };
      },

      /**
       * One clearRect and one scaled drawImage. The destination spans half a
       * cell beyond the outermost sample in each direction, because the grid
       * is point-sampled: that puts each source pixel's CENTRE on its sample
       * point instead of half a cell off.
       */
      _drawField: function () {
        const c = this._field;
        if (!c || !c.ctx) return;
        const ctx = c.ctx;
        ctx.clearRect(0, 0, this._size.x, this._size.y);
        const g = this._geometry;
        if (!g || !this._cellCanvas || !this._cellRows) return;
        const half = g.stepDeg / 2;
        const north = g.south + (g.rows - 1) * g.stepDeg + half;
        const east = g.west + (g.cols - 1) * g.stepDeg + half;
        const topLeft = this._map.latLngToContainerPoint([north, g.west - half]);
        const bottomRight = this._map.latLngToContainerPoint([g.south - half, east]);
        const w = bottomRight.x - topLeft.x;
        const h = bottomRight.y - topLeft.y;
        if (!(w > 0) || !(h > 0)) return;
        // This is where the smooth gradient comes from: the browser's own
        // bilinear filter, on an 85-pixel-wide bitmap.
        ctx.imageSmoothingEnabled = true;
        if ('imageSmoothingQuality' in ctx) ctx.imageSmoothingQuality = 'high';
        try {
          ctx.drawImage(this._cellCanvas, topLeft.x, topLeft.y, w, h);
        } catch (e) {
          console.warn('[weather] field draw failed:', e && e.message);
        }
      },

      _seedParticles: function () {
        const w = this._size.x;
        const h = this._size.y;
        if (!(w > 0) || !(h > 0)) {
          this._particles = null;
          return;
        }
        const n = particleBudget(w, h);
        const list = new Array(n);
        for (let i = 0; i < n; i += 1) list[i] = spawnParticle(w, h);
        this._particles = list;
      },

      /** One animation frame. dt is milliseconds, already clamped. */
      _stepParticles: function (dt) {
        const c = this._wind;
        if (!c || !c.ctx || !this._particles || !this._proj) return;
        const ctx = c.ctx;
        const w = this._size.x;
        const h = this._size.y;
        const g = this._windGeo();

        // Fade, do not clear: this is what leaves trails. It has to be
        // destination-out — painting a low-alpha black rectangle instead would
        // build a dark veil over the basemap, since this canvas is transparent
        // and sits on top of the map rather than over its own background.
        ctx.globalCompositeOperation = 'destination-out';
        ctx.fillStyle = 'rgba(0,0,0,0.14)';
        ctx.fillRect(0, 0, w, h);
        ctx.globalCompositeOperation = 'source-over';

        ctx.lineWidth = 1.1;
        ctx.lineCap = 'round';
        ctx.strokeStyle = 'rgba(255,255,255,0.75)';
        ctx.beginPath();

        const seconds = dt / 1000;
        const list = this._particles;
        for (let i = 0; i < list.length; i += 1) {
          const p = list[i];
          p.age += 1;
          const cell = this._cellAt(p.x, p.y);
          let uv = null;
          if (cell) {
            const speed = sampleBilinear(
              this._windSpeed, g.cols, g.rows, this._windSpeedScale, cell.cx, cell.cy
            );
            const dir = sampleDirection(this._windDir, g, this._windDirScale, cell.cx, cell.cy);
            if (speed !== null && dir !== null) uv = windUV(speed, dir);
          }
          if (!uv || p.age > p.life) {
            // Off-grid, over NODATA, or simply old. Respawning on a lifetime
            // counter is what stops every particle piling into the same
            // convergence line and leaving the rest of the map bare.
            list[i] = spawnParticle(w, h);
            continue;
          }
          const x0 = p.x;
          const y0 = p.y;
          // Screen speed, not ground speed: real wind moves a few metres a
          // second, which is invisible at any zoom this map uses. Keeping it
          // zoom-invariant in pixels keeps the field equally legible zoomed
          // right in and right out; only the DIRECTION is geographic.
          p.x += uv.u * this._windPxPerSec * seconds;
          // Canvas y grows downward, so a northward component moves UP.
          p.y -= uv.v * this._windPxPerSec * seconds;
          if (p.x < -4 || p.x > w + 4 || p.y < -4 || p.y > h + 4) {
            list[i] = spawnParticle(w, h);
            continue;
          }
          ctx.moveTo(x0, y0);
          ctx.lineTo(p.x, p.y);
        }
        ctx.stroke();
      },

      /**
       * The reduced-motion field: a static lattice of arrows, drawn once.
       * Same data, same direction convention, no loop.
       */
      _drawArrows: function () {
        const c = this._wind;
        if (!c || !c.ctx || !this._proj || !this._windSpeed || !this._windDir) return;
        const ctx = c.ctx;
        const g = this._windGeo();
        const w = this._size.x;
        const h = this._size.y;
        ctx.clearRect(0, 0, w, h);
        ctx.globalCompositeOperation = 'source-over';
        ctx.strokeStyle = 'rgba(255,255,255,0.8)';
        ctx.fillStyle = 'rgba(255,255,255,0.8)';
        ctx.lineWidth = 1.2;
        ctx.lineCap = 'round';
        const spacing = 58;
        for (let py = spacing / 2; py < h; py += spacing) {
          for (let px = spacing / 2; px < w; px += spacing) {
            const cell = this._cellAt(px, py);
            if (!cell) continue;
            const speed = sampleBilinear(
              this._windSpeed, g.cols, g.rows, this._windSpeedScale, cell.cx, cell.cy
            );
            const dir = sampleDirection(this._windDir, g, this._windDirScale, cell.cx, cell.cy);
            if (speed === null || dir === null) continue;
            const uv = windUV(speed, dir);
            if (!uv) continue;
            const mag = Math.sqrt(uv.u * uv.u + uv.v * uv.v);
            if (!(mag > 0.1)) continue;
            // Length reads speed, clamped so a gale does not draw an arrow
            // across three neighbours.
            const len = Math.min(spacing * 0.42, 6 + speed * 0.28);
            const dx = (uv.u / mag) * len;
            const dy = (-uv.v / mag) * len;
            drawArrow(ctx, px - dx / 2, py - dy / 2, px + dx / 2, py + dy / 2);
          }
        }
      }
    });

    return LayerClass;
  }

  // ------------------------------------------------------------------------
  // Small helpers. Every canvas/context acquisition is guarded: both can
  // return null (memory pressure, a blocked context) and the map must keep
  // working without the weather rather than throw on load.
  // ------------------------------------------------------------------------

  function windowRaf(fn) {
    if (typeof window !== 'undefined' && window.requestAnimationFrame) {
      return window.requestAnimationFrame(fn);
    }
    return setTimeout(function () {
      fn(Date.now());
    }, 16);
  }

  function windowCaf(id) {
    if (typeof window !== 'undefined' && window.cancelAnimationFrame) {
      window.cancelAnimationFrame(id);
      return;
    }
    clearTimeout(id);
  }

  function ensurePane(map, name, zIndex) {
    const existing = map.getPane(name);
    // An existing pane is somebody else's — a caller can point us at one, and
    // restyling it would silently take the pointer events off whatever already
    // lives there. Only panes we create get styled.
    if (existing) return existing;
    map.createPane(name);
    const pane = map.getPane(name);
    if (pane) {
      pane.style.zIndex = String(zIndex);
      // The field is decoration; clicks belong to the markers underneath it.
      pane.style.pointerEvents = 'none';
    }
    return pane;
  }

  function offscreen(width, height) {
    try {
      const el = document.createElement('canvas');
      el.width = width;
      el.height = height;
      const ctx = el.getContext('2d');
      if (!ctx) return null;
      return { el: el, ctx: ctx };
    } catch (e) {
      console.warn('[weather] no 2d canvas:', e && e.message);
      return null;
    }
  }

  /**
   * How much to upsample the grid before it is handed to the browser.
   *
   * WHY THIS EXISTS. The field was built at one pixel per grid cell — 85 wide
   * — and drawImage'd up to a ~1700px viewport with imageSmoothingEnabled on,
   * trusting the browser's bilinear filter to make it smooth. It does not: at
   * a 20x upscale, from a bitmap whose rows were already nearest-neighbour
   * duplicated for the Mercator stretch, the cell structure survives and the
   * field renders as visible hard-edged blocks.
   *
   * So the interpolation happens here instead, in VALUE space rather than
   * colour space. Interpolating values and then colour-mapping follows the
   * scale correctly; interpolating the mapped colours would blend across
   * scale stops and invent shades the ramp never defines.
   *
   * Five is enough that whatever the browser does on the remaining ~4x is
   * invisible. It runs once per data change, never per frame.
   */
  const UPSAMPLE = 5;

  /**
   * Bilinear upsample of an Int16 grid, NODATA-aware.
   *
   * A cell with no data is not zero, so it cannot be averaged in — a single
   * absent neighbour would otherwise drag a real reading toward whatever zero
   * means on that scale. Any sample whose four corners are not all present
   * stays NODATA, which keeps coastlines and data edges honest instead of
   * smearing them outward.
   */
  function upsampleGrid(values, cols, rows, factor, nodata) {
    const outCols = (cols - 1) * factor + 1;
    const outRows = (rows - 1) * factor + 1;
    const out = new Int16Array(outCols * outRows);
    for (let oy = 0; oy < outRows; oy++) {
      const sy = oy / factor;
      const y0 = Math.min(rows - 1, Math.floor(sy));
      const y1 = Math.min(rows - 1, y0 + 1);
      const fy = sy - y0;
      for (let ox = 0; ox < outCols; ox++) {
        const sx = ox / factor;
        const x0 = Math.min(cols - 1, Math.floor(sx));
        const x1 = Math.min(cols - 1, x0 + 1);
        const fx = sx - x0;

        const v00 = values[y0 * cols + x0];
        const v10 = values[y0 * cols + x1];
        const v01 = values[y1 * cols + x0];
        const v11 = values[y1 * cols + x1];
        if (v00 === nodata || v10 === nodata || v01 === nodata || v11 === nodata) {
          out[oy * outCols + ox] = nodata;
          continue;
        }
        const top = v00 + (v10 - v00) * fx;
        const bot = v01 + (v11 - v01) * fx;
        out[oy * outCols + ox] = Math.round(top + (bot - top) * fy);
      }
    }
    return { values: out, cols: outCols, rows: outRows };
  }

  function makeCanvas(map, paneName, className) {
    const pane = map.getPane(paneName);
    if (!pane) return null;
    let el;
    try {
      el = document.createElement('canvas');
    } catch (e) {
      return null;
    }
    // Deliberately NOT leaflet-zoom-animated. Leaflet transforms the whole
    // map pane during a zoom animation, so this canvas already scales with
    // the basemap as a child of it; adding the class would also hand it
    // Leaflet's 0.25s transform transition, which during a drag leaves the
    // field visibly sliding along behind the tiles.
    el.className = className;
    el.style.position = 'absolute';
    el.style.left = '0';
    el.style.top = '0';
    el.style.pointerEvents = 'none';
    const ctx = el.getContext ? el.getContext('2d') : null;
    if (!ctx) {
      console.warn('[weather] 2d context unavailable; weather field disabled');
      return null;
    }
    pane.appendChild(el);
    return { el: el, ctx: ctx };
  }

  function sizeCanvas(c, size, dpr) {
    if (!c || !c.el || !c.ctx) return;
    c.el.width = Math.max(1, Math.round(size.x * dpr));
    c.el.height = Math.max(1, Math.round(size.y * dpr));
    c.el.style.width = size.x + 'px';
    c.el.style.height = size.y + 'px';
    // Draw in CSS pixels and let the backing store carry the extra density,
    // so every coordinate in this file is a CSS pixel.
    c.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  function clearCanvas(c, size) {
    if (!c || !c.ctx) return;
    c.ctx.clearRect(0, 0, size.x, size.y);
  }

  function spawnParticle(w, h) {
    return {
      x: Math.random() * w,
      y: Math.random() * h,
      age: 0,
      // Spread the lifetimes so respawns do not all land on the same frame
      // and strobe.
      life: 40 + Math.floor(Math.random() * 90)
    };
  }

  /**
   * Bilinear sampling of a DIRECTION grid.
   *
   * Averaging degrees directly is wrong across the 360/0 seam: 350 and 10 are
   * twenty degrees apart but average to 180, a wind blowing exactly backwards.
   * Resolve each corner to a unit vector, average those, and take the angle.
   */
  function sampleDirection(values, g, scale, cx, cy) {
    if (!values) return null;
    const cols = g.cols;
    const rows = g.rows;
    if (!(cx >= 0) || !(cy >= 0) || cx > cols - 1 || cy > rows - 1) return null;
    const s = scale > 0 ? scale : 1;
    const x0 = Math.floor(cx);
    const y0 = Math.floor(cy);
    const x1 = x0 + 1 > cols - 1 ? x0 : x0 + 1;
    const y1 = y0 + 1 > rows - 1 ? y0 : y0 + 1;
    const fx = cx - x0;
    const fy = cy - y0;
    const idx = [y0 * cols + x0, y0 * cols + x1, y1 * cols + x0, y1 * cols + x1];
    const wts = [(1 - fx) * (1 - fy), fx * (1 - fy), (1 - fx) * fy, fx * fy];
    let sx = 0;
    let sy = 0;
    for (let i = 0; i < 4; i += 1) {
      const raw = values[idx[i]];
      if (raw === NODATA || raw === undefined) return null;
      const rad = ((raw / s) * Math.PI) / 180;
      sx += Math.sin(rad) * wts[i];
      sy += Math.cos(rad) * wts[i];
    }
    if (sx === 0 && sy === 0) return null;
    let deg = (Math.atan2(sx, sy) * 180) / Math.PI;
    if (deg < 0) deg += 360;
    return deg;
  }

  function drawArrow(ctx, x0, y0, x1, y1) {
    ctx.beginPath();
    ctx.moveTo(x0, y0);
    ctx.lineTo(x1, y1);
    ctx.stroke();
    const ang = Math.atan2(y1 - y0, x1 - x0);
    const head = 4.5;
    ctx.beginPath();
    ctx.moveTo(x1, y1);
    ctx.lineTo(x1 - head * Math.cos(ang - 0.42), y1 - head * Math.sin(ang - 0.42));
    ctx.lineTo(x1 - head * Math.cos(ang + 0.42), y1 - head * Math.sin(ang + 0.42));
    ctx.closePath();
    ctx.fill();
  }

  // ------------------------------------------------------------------------
  // Public API
  // ------------------------------------------------------------------------

  /**
   * Put a weather field on the map.
   *
   * opts: { geometry, variable, values, manifestVar, windSpeed, windDir,
   *         windSpeedVar, windDirVar, animated, opacity, windSpeedPxPerSec,
   *         fieldPane, particlePane, fieldZIndex, particleZIndex }
   *
   * Returns a controller. Every method is safe to call after remove() — the
   * caller has timers and fetches in flight and should not have to track
   * teardown ordering.
   */
  function create(map, opts) {
    const o = opts || {};
    const layer = new (layerClass())(o);
    let added = false;
    try {
      layer.addTo(map);
      added = true;
    } catch (e) {
      console.warn('[weather] could not add the field layer:', e && e.message);
    }

    if (added && o.values) layer.setData(o.values, o.manifestVar || { name: o.variable });
    if (added && o.windSpeed && o.windDir) {
      layer.setWind(o.windSpeed, o.windDir, o.windSpeedVar, o.windDirVar);
    }

    function guard(name) {
      return function () {
        if (!added) return null;
        return layer[name].apply(layer, arguments);
      };
    }

    return {
      layer: layer,
      setVariable: guard('setVariable'),
      setData: guard('setData'),
      setWind: guard('setWind'),
      setGeometry: guard('setGeometry'),
      setAnimated: guard('setAnimated'),
      setOpacity: guard('setOpacity'),
      redraw: guard('_reset'),
      valueAt: guard('valueAt'),
      windAt: guard('windAt'),
      variable: guard('variable'),
      isAnimating: function () {
        return added ? layer.isAnimating() : false;
      },
      animationMode: function () {
        return added ? layer.animationMode() : null;
      },
      legend: function () {
        return legendFor(layer.variable());
      },
      remove: function () {
        if (!added) return;
        added = false;
        try {
          map.removeLayer(layer);
        } catch (e) {
          /* already gone */
        }
      }
    };
  }

  window.WeatherField = {
    create: create,
    // The colour scales and the legend built from them: one source of truth.
    SCALES: SCALES,
    SCALE_ALIASES: SCALE_ALIASES,
    scaleFor: scaleFor,
    colorFor: colorFor,
    rgbaCss: rgbaCss,
    legendFor: legendFor,
    // Grid maths, exported so a tooltip, a readout or a test can do the same
    // arithmetic the renderer does rather than a second version of it.
    NODATA: NODATA,
    dequantise: dequantise,
    paintCells: paintCells,
    rowSampleMap: rowSampleMap,
    sampleBilinear: sampleBilinear,
    sampleDirection: sampleDirection,
    windUV: windUV,
    mercY: mercY,
    mercLat: mercLat,
    // Motion policy.
    particleBudget: particleBudget,
    prefersReducedMotion: prefersReducedMotion,
    animationMode: animationMode,
    createAnimator: createAnimator
  };
})();
