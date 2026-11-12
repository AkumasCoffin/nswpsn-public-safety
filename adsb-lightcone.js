/**
 * ADS-B receiver coverage in 3D — the "lightcone".
 *
 * Shared by the owner's node modal (feeder.html) and the staff ADS-B coverage
 * tab (staff.html). Draws, around the receiver:
 *   - bright dots: cells (5 degrees x 10 km x 500 m) where it heard aircraft;
 *   - dark dots: cells where the public feed saw aircraft inside the
 *     line-of-sight limit that this receiver did not report;
 *   - the terrain limit per altitude band (purple), and the measured reach
 *     (white) for the same band;
 *   - possible obstructions: bearings where the measured reach falls well
 *     short of the terrain limit AND aircraft were missed in between.
 *
 * Usage: const view = AdsbLightcone.mount(el, tracksResponse, opts);
 *        view.update(tracksResponse); view.destroy();
 * opts.heightHint: text shown when the antenna height is the assumed default.
 *
 * Plain canvas, no libraries. Renders on demand (only when something
 * changes), so an idle view costs nothing.
 */
(function () {
  'use strict';

  const EXAG = 14;              // altitude exaggeration
  const BANDS = [
    { id: 'all', label: 'All', s0: 0, s1: 25 },
    { id: 'b0', label: '0–1.5 km', s0: 0, s1: 2 },
    { id: 'b1', label: '1.5–3 km', s0: 3, s1: 5 },
    { id: 'b2', label: '3–6 km', s0: 6, s1: 11 },
    { id: 'b3', label: '6–9 km', s0: 12, s1: 17 },
    { id: 'b4', label: '9–13 km', s0: 18, s1: 25 },
  ];
  const COL = {
    heard: [224, 242, 254],     // near-white sky
    heardDim: [56, 189, 248],
    missed: [71, 85, 105],
    terrain: '#a78bfa',
    measured: '#f8fafc',
    obstruction: '#f59e0b',
    ring: 'rgba(148,163,184,0.20)',
    spoke: 'rgba(148,163,184,0.08)',
    label: '#94a3b8',
  };
  const COMPASS = ['north', 'north-east', 'east', 'south-east', 'south', 'south-west', 'west', 'north-west'];

  let styleDone = false;
  function injectStyle() {
    if (styleDone) return;
    styleDone = true;
    const css = `
.alc { position: relative; background: #050a13; border: 1px solid rgba(148,163,184,0.16); border-radius: 10px; overflow: hidden; color: #e2e8f0; }
.alc-stage { position: relative; }
.alc canvas { display: block; width: 100%; touch-action: none; cursor: grab; outline: none; }
.alc canvas:active { cursor: grabbing; }
.alc canvas:focus-visible { box-shadow: inset 0 0 0 2px rgba(56,189,248,0.6); }
.alc-hero { position: absolute; left: 14px; top: 12px; pointer-events: none; max-width: 60%; }
.alc-kicker { font-size: 9.5px; letter-spacing: 0.14em; text-transform: uppercase; color: #64748b; margin-bottom: 6px; }
.alc-big { font-size: 46px; font-weight: 300; line-height: 1; color: #f1f5f9; font-variant-numeric: tabular-nums; }
.alc-big small { font-size: 15px; color: #38bdf8; margin-left: 4px; font-weight: 500; }
.alc-sub { font-size: 12px; color: #e2e8f0; margin-top: 6px; }
.alc-sub2 { font-size: 10.5px; color: #64748b; margin-top: 1px; }
.alc-pct { font-size: 20px; color: #a78bfa; margin-top: 10px; font-variant-numeric: tabular-nums; }
.alc-pct-l { font-size: 10.5px; color: #64748b; }
.alc-status { position: absolute; right: 12px; top: 12px; font-size: 10.5px; color: #94a3b8; text-align: right; max-width: 44%; pointer-events: none; line-height: 1.4; }
.alc-empty { position: absolute; left: 50%; top: 58%; transform: translate(-50%, -50%); font-size: 12px; color: #94a3b8; text-align: center; max-width: 80%; pointer-events: none; line-height: 1.5; background: rgba(5,10,19,0.75); padding: 8px 12px; border-radius: 8px; }
.alc-tip { position: absolute; pointer-events: none; background: rgba(15,23,42,0.95); border: 1px solid rgba(148,163,184,0.25); border-radius: 6px; padding: 5px 8px; font-size: 11px; color: #e2e8f0; white-space: nowrap; transform: translate(-50%, calc(-100% - 10px)); display: none; z-index: 2; }
.alc-ctl { display: flex; flex-wrap: wrap; gap: 8px 16px; align-items: center; padding: 9px 12px; border-top: 1px solid rgba(148,163,184,0.12); background: #070d18; }
.alc-grp { display: flex; flex-wrap: wrap; gap: 4px; align-items: center; }
.alc-grp-l { font-size: 9px; letter-spacing: 0.12em; text-transform: uppercase; color: #64748b; margin-right: 4px; }
.alc-b { font: inherit; font-size: 11px; padding: 4px 8px; border-radius: 5px; border: 1px solid rgba(148,163,184,0.22); background: transparent; color: #cbd5e1; cursor: pointer; }
.alc-b:hover { border-color: rgba(148,163,184,0.5); }
.alc-b.on { background: rgba(56,189,248,0.14); border-color: rgba(56,189,248,0.55); color: #e0f2fe; }
.alc-b.lay-terrain.on { background: rgba(167,139,250,0.14); border-color: rgba(167,139,250,0.6); color: #ede9fe; }
.alc-b.lay-obs.on { background: rgba(245,158,11,0.12); border-color: rgba(245,158,11,0.6); color: #fef3c7; }
.alc-b.lay-missed.on { background: rgba(100,116,139,0.18); border-color: rgba(148,163,184,0.5); }
.alc-b:disabled { opacity: 0.4; cursor: default; }
.alc-foot { padding: 8px 12px 10px; font-size: 10.5px; color: #64748b; line-height: 1.55; border-top: 1px solid rgba(148,163,184,0.08); }
.alc-legend { display: flex; flex-wrap: wrap; gap: 4px 14px; color: #94a3b8; margin-bottom: 3px; }
.alc-legend span { display: inline-flex; align-items: center; gap: 5px; }
.alc-k-dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block; }
.alc-k-box { width: 9px; height: 9px; display: inline-block; border: 1.5px solid; }
.alc-k-line { width: 14px; height: 0; display: inline-block; border-top: 2px solid; }
@media (max-width: 560px) { .alc-big { font-size: 34px; } .alc-hero { max-width: 70%; } .alc-status { display: none; } }
`;
    const st = document.createElement('style');
    st.textContent = css;
    document.head.appendChild(st);
  }

  function esc(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  /** Deterministic jitter in [0,1) for a cell, so dots don't sit on a grid and
   *  don't shimmer between renders. */
  function jit(cell, k) {
    let h = (cell + 1) * 2654435761 + k * 40503;
    h = Math.imul(h ^ (h >>> 15), 2246822507);
    h = Math.imul(h ^ (h >>> 13), 3266489909);
    return ((h ^ (h >>> 16)) >>> 0) / 4294967296;
  }

  function compassName(deg) {
    return COMPASS[Math.round(((deg % 360) + 360) % 360 / 45) % 8];
  }

  /** Decode the response into per-cell arrays the renderer can loop over. */
  function prepare(d) {
    const lc = d && d.lightcone;
    if (!lc) return null;
    const R = lc.rings, B = lc.bearings, ringKm = lc.ringKm, sliceKm = lc.sliceM / 1000;
    const decode = (cells, counts) => {
      const n = cells.length;
      const out = { n, slice: new Uint8Array(n), b: new Uint8Array(n), ring: new Uint8Array(n),
        x: new Float32Array(n), y: new Float32Array(n), z: new Float32Array(n), km: new Float32Array(n),
        w: new Float32Array(n) };
      let maxN = 1;
      if (counts) for (let i = 0; i < n; i += 1) maxN = Math.max(maxN, counts[i]);
      for (let i = 0; i < n; i += 1) {
        const c = cells[i];
        const ring = c % R, t = Math.floor(c / R), b = t % B, slice = Math.floor(t / B);
        const brg = (b + jit(c, 1)) * (360 / B) * Math.PI / 180;
        const km = (ring + jit(c, 2)) * ringKm;
        out.slice[i] = slice; out.b[i] = b; out.ring[i] = ring; out.km[i] = km;
        out.x[i] = km * Math.sin(brg);
        out.y[i] = km * Math.cos(brg);
        out.z[i] = (slice + jit(c, 3)) * sliceKm;
        out.w[i] = counts ? Math.log1p(counts[i]) / Math.log1p(maxN) : 1;
      }
      return out;
    };
    const heard = decode(lc.heard || [], lc.heardN || null);
    const missed = decode(lc.missed || [], null);
    const limits = lc.terrain && Array.isArray(lc.terrain.limits) ? lc.terrain.limits : null;

    // Per band: measured reach per bearing, missed presence per bearing ring,
    // and the obstruction verdict.
    const bands = {};
    for (const band of BANDS) {
      const reach = new Float32Array(B);
      const cellsIn = { heard: 0 };
      for (let i = 0; i < heard.n; i += 1) {
        const s = heard.slice[i];
        if (s < band.s0 || s > band.s1) continue;
        cellsIn.heard += 1;
        const far = (heard.ring[i] + 1) * ringKm;
        if (far > reach[heard.b[i]]) reach[heard.b[i]] = far;
      }
      const terr = limits ? limits[band.s1] : null;
      // Missed cells per bearing, beyond the reach.
      const missedBeyond = new Uint16Array(B);
      for (let i = 0; i < missed.n; i += 1) {
        const s = missed.slice[i];
        if (s < band.s0 || s > band.s1) continue;
        const b = missed.b[i];
        if (missed.ring[i] * ringKm >= reach[b] - ringKm) missedBeyond[b] += 1;
      }
      const obs = new Uint8Array(B);
      // Per band only: across all altitudes a short reach is just low traffic.
      if (terr && band.id !== 'all' && cellsIn.heard >= 30) {
        for (let b = 0; b < B; b += 1) {
          const L = terr[b], M = reach[b];
          if (L > 40 && M < L * 0.6 && L - M > 40 && missedBeyond[b] >= 2) obs[b] = 1;
        }
      }
      let maxReach = 0, maxB = 0;
      for (let b = 0; b < B; b += 1) if (reach[b] > maxReach) { maxReach = reach[b]; maxB = b; }
      let pct = null;
      if (terr) {
        let a = 0, t = 0;
        for (let b = 0; b < B; b += 1) {
          const L = terr[b];
          t += L * L;
          const M = Math.min(reach[b], L);
          a += M * M;
        }
        pct = t > 0 ? Math.round((a / t) * 100) : null;
      }
      bands[band.id] = { reach, terr, obs, maxReach, maxDeg: (maxB + 0.5) * (360 / B), heard: cellsIn.heard, pct };
    }

    let maxR = 100;
    for (let i = 0; i < heard.n; i += 1) maxR = Math.max(maxR, heard.km[i]);
    if (limits) for (const row of limits) for (const v of row) maxR = Math.max(maxR, v);
    maxR = Math.min(600, Math.ceil(maxR / 100) * 100);

    return { lc, heard, missed, limits, bands, maxR, B, ringKm, sliceKm };
  }

  function mount(el, data, opts) {
    injectStyle();
    opts = opts || {};
    if (el._alc) { el._alc.update(data); return el._alc; }

    el.innerHTML = `
<div class="alc">
  <div class="alc-stage">
    <canvas tabindex="0" role="img"></canvas>
    <div class="alc-hero"></div>
    <div class="alc-status"></div>
    <div class="alc-empty" hidden></div>
    <div class="alc-tip"></div>
  </div>
  <div class="alc-ctl">
    <div class="alc-grp" data-g="band"><span class="alc-grp-l">Altitude</span>
      ${BANDS.map((b) => `<button type="button" class="alc-b" data-band="${b.id}">${b.label}</button>`).join('')}</div>
    <div class="alc-grp"><span class="alc-grp-l">Layers</span>
      <button type="button" class="alc-b on" data-lay="heard">Measured coverage</button>
      <button type="button" class="alc-b lay-missed on" data-lay="missed">Missed aircraft</button>
      <button type="button" class="alc-b lay-terrain on" data-lay="terrain">Terrain limit</button>
      <button type="button" class="alc-b lay-obs on" data-lay="obs">Possible obstructions</button></div>
    <div class="alc-grp"><span class="alc-grp-l">View</span>
      <button type="button" class="alc-b on" data-view="3d">3D</button>
      <button type="button" class="alc-b" data-view="top">Top</button>
      <button type="button" class="alc-b" data-view="rotate">Rotate</button>
      <button type="button" class="alc-b" data-view="in" aria-label="Zoom in">+</button>
      <button type="button" class="alc-b" data-view="out" aria-label="Zoom out">−</button></div>
  </div>
  <div class="alc-foot"></div>
</div>`;

    const canvas = el.querySelector('canvas');
    const ctx = canvas.getContext('2d');
    const hero = el.querySelector('.alc-hero');
    const statusEl = el.querySelector('.alc-status');
    const emptyEl = el.querySelector('.alc-empty');
    const tip = el.querySelector('.alc-tip');
    const foot = el.querySelector('.alc-foot');

    const st = {
      band: 'b4', layers: { heard: true, missed: true, terrain: true, obs: true },
      yaw: -0.35, pitch: 0.62, zoom: 1, rotating: false, top: false,
      w: 0, h: 0, dpr: 1, P: null, raf: 0, proj: null,
    };

    function setButtons() {
      el.querySelectorAll('[data-band]').forEach((b) => b.classList.toggle('on', b.dataset.band === st.band));
      el.querySelectorAll('[data-lay]').forEach((b) => b.classList.toggle('on', !!st.layers[b.dataset.lay]));
      el.querySelector('[data-view="3d"]').classList.toggle('on', !st.top);
      el.querySelector('[data-view="top"]').classList.toggle('on', st.top);
      el.querySelector('[data-view="rotate"]').classList.toggle('on', st.rotating);
      const P = st.P;
      const noTerr = !P || !P.limits;
      el.querySelector('[data-lay="terrain"]').disabled = noTerr;
      el.querySelector('[data-lay="obs"]').disabled = noTerr;
    }

    function resize() {
      const w = el.clientWidth || 600;
      const h = Math.round(Math.max(300, Math.min(560, w * 0.62)));
      const dpr = Math.min(2, window.devicePixelRatio || 1);
      if (w === st.w && h === st.h && dpr === st.dpr) return;
      st.w = w; st.h = h; st.dpr = dpr;
      canvas.width = Math.round(w * dpr);
      canvas.height = Math.round(h * dpr);
      canvas.style.height = h + 'px';
      draw();
    }

    function request() {
      if (st.raf) return;
      st.raf = requestAnimationFrame(() => {
        st.raf = 0;
        if (st.rotating) { st.yaw += 0.004; request(); }
        draw();
      });
    }

    /** km-space (east, north, altitude-km) -> screen px. */
    function makeProj() {
      const P = st.P;
      const maxR = P ? P.maxR : 300;
      const pitch = st.top ? Math.PI / 2 : st.pitch;
      const cy = Math.cos(st.yaw), sy = Math.sin(st.yaw);
      const sp = Math.sin(pitch), cp = Math.cos(pitch);
      // Fit the ground disc AND the tallest band: the disc spans 2R*sin(pitch)
      // vertically, and the cone rises 13 km * EXAG * cos(pitch) above it.
      const lift = 13 * EXAG * cp;
      const vExt = 2 * maxR * sp + lift;
      const fit = Math.min((st.w * 0.94) / (2 * maxR), (st.h * 0.9) / vExt);
      const scale = st.zoom * fit;
      const cx0 = st.w / 2;
      const cy0 = st.h / 2 + (lift * fit) / 2;
      const persp = maxR * 4;
      return function (x, y, z) {
        const xr = x * cy - y * sy;
        const yr = x * sy + y * cy;
        const zz = z * EXAG;
        const up = yr * sp + zz * cp;
        const depth = yr * cp - zz * sp;
        const f = persp / (persp + depth);
        return [cx0 + xr * scale * f, cy0 - up * scale * f, depth];
      };
    }

    function polyline(pr, pts, close) {
      ctx.beginPath();
      pts.forEach((p, i) => {
        const s = pr(p[0], p[1], p[2]);
        if (i === 0) ctx.moveTo(s[0], s[1]); else ctx.lineTo(s[0], s[1]);
      });
      if (close) ctx.closePath();
    }

    function bandLine(values, zKm) {
      const P = st.P;
      const pts = [];
      for (let b = 0; b < P.B; b += 1) {
        const a = (b + 0.5) * (360 / P.B) * Math.PI / 180;
        const r = values[b];
        pts.push([r * Math.sin(a), r * Math.cos(a), zKm]);
      }
      return pts;
    }

    function draw() {
      if (!st.w) return;
      const P = st.P;
      ctx.setTransform(st.dpr, 0, 0, st.dpr, 0, 0);
      ctx.clearRect(0, 0, st.w, st.h);
      const bg = ctx.createRadialGradient(st.w / 2, st.h * 0.55, 10, st.w / 2, st.h * 0.55, Math.max(st.w, st.h) * 0.8);
      bg.addColorStop(0, '#0b1626');
      bg.addColorStop(1, '#04080f');
      ctx.fillStyle = bg;
      ctx.fillRect(0, 0, st.w, st.h);
      const pr = makeProj();
      const maxR = P ? P.maxR : 300;

      // Ground: rings every 100 km, spokes every 30 degrees.
      ctx.lineWidth = 1;
      for (let r = 100; r <= maxR; r += 100) {
        const pts = [];
        for (let a = 0; a <= 360; a += 4) {
          const t = a * Math.PI / 180;
          pts.push([r * Math.sin(t), r * Math.cos(t), 0]);
        }
        ctx.strokeStyle = COL.ring;
        polyline(pr, pts, false);
        ctx.stroke();
      }
      ctx.strokeStyle = COL.spoke;
      for (let a = 0; a < 360; a += 30) {
        const t = a * Math.PI / 180;
        polyline(pr, [[0, 0, 0], [maxR * Math.sin(t), maxR * Math.cos(t), 0]], false);
        ctx.stroke();
      }
      ctx.font = '10px ui-monospace, SFMono-Regular, Menlo, monospace';
      ctx.fillStyle = COL.label;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      for (let r = 100; r <= maxR; r += 100) {
        const t = 135 * Math.PI / 180;
        const s = pr(r * Math.sin(t), r * Math.cos(t), 0);
        ctx.fillText(r + ' km', s[0], s[1]);
      }
      {
        const s = pr(0, maxR + maxR * 0.05, 0);
        ctx.fillStyle = '#e2e8f0';
        ctx.font = 'bold 11px ui-monospace, SFMono-Regular, Menlo, monospace';
        ctx.fillText('N', s[0], s[1]);
      }
      // The receiver.
      {
        const s = pr(0, 0, 0);
        ctx.fillStyle = '#38bdf8';
        ctx.beginPath(); ctx.arc(s[0], s[1], 3, 0, Math.PI * 2); ctx.fill();
      }
      if (!P) { st.proj = null; return; }

      const band = BANDS.find((b) => b.id === st.band) || BANDS[0];
      const inBand = (s) => s >= band.s0 && s <= band.s1;
      const bd = P.bands[band.id];

      // Obstructions: wedges on the ground from measured reach out to the
      // terrain limit, on flagged bearings.
      if (st.layers.obs && bd.terr) {
        const step = 360 / P.B;
        let labelled = false;
        for (let b = 0; b < P.B; b += 1) {
          if (!bd.obs[b]) continue;
          const a0 = b * step * Math.PI / 180, a1 = (b + 1) * step * Math.PI / 180;
          const m = bd.reach[b], L = bd.terr[b];
          // Drawn at the band's own altitude, where the gap actually is.
          const z = (band.s1 + 1) * P.sliceKm;
          const pts = [
            [m * Math.sin(a0), m * Math.cos(a0), z], [L * Math.sin(a0), L * Math.cos(a0), z],
            [L * Math.sin(a1), L * Math.cos(a1), z], [m * Math.sin(a1), m * Math.cos(a1), z],
          ];
          polyline(pr, pts, true);
          ctx.fillStyle = 'rgba(245,158,11,0.16)';
          ctx.fill();
          ctx.strokeStyle = 'rgba(245,158,11,0.75)';
          ctx.lineWidth = 1;
          ctx.stroke();
          if (!labelled || (b > 0 && !bd.obs[b - 1])) {
            const am = (b + 0.5) * step * Math.PI / 180;
            const s = pr(L * Math.sin(am), L * Math.cos(am), (band.s1 + 1) * P.sliceKm);
            ctx.fillStyle = COL.obstruction;
            ctx.font = '10px ui-monospace, SFMono-Regular, Menlo, monospace';
            ctx.fillText('obstruction?', s[0], s[1] - 8);
            labelled = true;
          }
        }
      }

      // Dots grow gently with zoom so a zoomed-in view doesn't look emptier.
      const dotScale = Math.sqrt(st.zoom);
      // Missed: small, dim, under the heard dots.
      const proj = { x: [], y: [], i: [], kind: [] };
      if (st.layers.missed) {
        const m = P.missed;
        ctx.fillStyle = 'rgba(100,116,139,0.6)';
        const mr = 1.5 * dotScale;
        for (let i = 0; i < m.n; i += 1) {
          if (!inBand(m.slice[i])) continue;
          const s = pr(m.x[i], m.y[i], m.z[i]);
          ctx.fillRect(s[0] - mr, s[1] - mr, mr * 2, mr * 2);
          proj.x.push(s[0]); proj.y.push(s[1]); proj.i.push(i); proj.kind.push(1);
        }
      }

      // Terrain limit per band, at the band's top altitude. Selected band
      // strong; the others (for All) faint, which draws the cone.
      if (st.layers.terrain && P.limits) {
        const list = band.id === 'all' ? BANDS.slice(1) : [band];
        for (const b of list) {
          ctx.strokeStyle = COL.terrain;
          ctx.globalAlpha = band.id === 'all' ? 0.55 : 0.95;
          ctx.lineWidth = band.id === 'all' ? 1 : 1.6;
          polyline(pr, bandLine(P.limits[b.s1], (b.s1 + 1) * P.sliceKm), true);
          ctx.stroke();
        }
        ctx.globalAlpha = 1;
      }

      // Heard dots, additive so dense regions glow by accumulation only.
      if (st.layers.heard) {
        const h = P.heard;
        ctx.globalCompositeOperation = 'lighter';
        for (let i = 0; i < h.n; i += 1) {
          if (!inBand(h.slice[i])) continue;
          const s = pr(h.x[i], h.y[i], h.z[i]);
          const w = h.w[i];
          const c = w > 0.5 ? COL.heard : COL.heardDim;
          // Big enough to read at a glance: the old 1-3 px squares at low
          // alpha all but vanished on a sparse receiver.
          ctx.fillStyle = `rgba(${c[0]},${c[1]},${c[2]},${(0.45 + 0.45 * w).toFixed(2)})`;
          const r = (1.8 + w * 1.6) * dotScale;
          ctx.beginPath(); ctx.arc(s[0], s[1], r, 0, Math.PI * 2); ctx.fill();
          proj.x.push(s[0]); proj.y.push(s[1]); proj.i.push(i); proj.kind.push(0);
        }
        ctx.globalCompositeOperation = 'source-over';
        // Measured reach for the band, as a white line at its top altitude.
        if (band.id !== 'all' && bd.heard > 0) {
          ctx.strokeStyle = COL.measured;
          ctx.globalAlpha = 0.85;
          ctx.lineWidth = 1.4;
          polyline(pr, bandLine(bd.reach, (band.s1 + 1) * P.sliceKm), true);
          ctx.stroke();
          ctx.globalAlpha = 1;
        }
      }
      st.proj = proj;
    }

    function fmtKm(v) { return Math.round(v).toLocaleString(); }

    function renderText() {
      const P = st.P;
      const band = BANDS.find((b) => b.id === st.band) || BANDS[0];
      if (!P) {
        hero.innerHTML = '';
        statusEl.textContent = '';
        emptyEl.hidden = false;
        emptyEl.textContent = 'No 3D coverage yet. It needs the antenna pin set, and fills in as aircraft are heard.';
        canvas.setAttribute('aria-label', 'Receiver coverage: no data yet');
        foot.innerHTML = '';
        return;
      }
      const bd = P.bands[band.id];
      const rangeLabel = band.id === 'all' ? 'range at any altitude'
        : band.id === 'b4' ? 'range at cruise altitude' : `range at ${band.label}`;
      const dir = bd.maxReach > 0 ? `to the ${compassName(bd.maxDeg)} · ${band.id === 'all' ? 'all altitudes' : band.label + ' altitude'}` : 'nothing heard in this band yet';
      hero.innerHTML = `<div class="alc-kicker">This receiver's measured coverage</div>`
        + `<div class="alc-big">${bd.maxReach > 0 ? fmtKm(bd.maxReach) : '—'}<small>km</small></div>`
        + `<div class="alc-sub">${esc(rangeLabel)}</div>`
        + `<div class="alc-sub2">${esc(dir)}</div>`
        + (bd.pct !== null && bd.heard > 0
          ? `<div class="alc-pct">${bd.pct}%</div><div class="alc-pct-l">of the area the terrain allows</div>` : '');
      canvas.setAttribute('aria-label', `Receiver coverage in 3D. ${bd.maxReach > 0 ? 'Furthest ' + fmtKm(bd.maxReach) + ' km to the ' + compassName(bd.maxDeg) : 'Nothing heard'} at ${band.label === 'All' ? 'any altitude' : band.label}.${bd.pct !== null ? ' ' + bd.pct + '% of the area the terrain allows.' : ''}`);

      const t = P.lc.terrain || {};
      const statusLines = [];
      if (t.status === 'building') statusLines.push('Terrain limit is being worked out — check back in a few minutes.');
      else if (t.status === 'waiting') statusLines.push('Terrain limit is queued behind the daily elevation-data allowance.');
      let obsCount = 0;
      for (let b = 0; b < P.B; b += 1) obsCount += bd.obs[b];
      if (st.layers.obs && obsCount > 0) statusLines.push(`${obsCount * 5}° of bearings look blocked at this altitude.`);
      statusEl.innerHTML = statusLines.map(esc).join('<br>');

      emptyEl.hidden = P.heard.n > 0;
      emptyEl.textContent = 'Nothing heard into the 3D picture yet. It fills in as aircraft are heard — give it a day of traffic.';

      const days = P.lc.daysCovered || 0;
      const hNote = !P.lc.antennaHeightSet && opts.heightHint ? ' ' + opts.heightHint : '';
      foot.innerHTML = `<div class="alc-legend">`
        + `<span><i class="alc-k-dot" style="background:#e0f2fe"></i>aircraft heard</span>`
        + `<span><i class="alc-k-dot" style="background:#64748b"></i>passed in range and not heard</span>`
        + `<span><i class="alc-k-line" style="border-color:#f8fafc"></i>measured reach</span>`
        + `<span><i class="alc-k-box" style="border-color:#a78bfa"></i>terrain line-of-sight limit</span>`
        + `<span><i class="alc-k-box" style="border-color:#f59e0b"></i>possible obstruction</span></div>`
        + `Altitude exaggerated ×${EXAG} · rings every 100 km · drag to rotate, pinch or Ctrl+wheel to zoom, arrow keys work too. `
        + `Built from the last ${days} ${days === 1 ? 'day' : 'days'}${days < 2 ? ' — too little history yet to judge this' : ''} (up to 7). `
        + `Terrain limit assumes the antenna is ${esc(P.lc.antennaHeightM)} m above the ground${P.lc.antennaHeightSet ? '' : ' (not set)'}`
        + (typeof t.groundM === 'number'
          ? `, on ground ${esc(t.groundM)} m above sea level — ${esc(Math.round(t.groundM + P.lc.antennaHeightM))} m in all.`
          : '.')
        + esc(hNote);
    }

    function update(d) {
      st.P = prepare(d);
      if (st.P && st.band === 'b4' && st.P.bands.b4.heard === 0 && st.P.bands.all.heard > 0) st.band = 'all';
      setButtons();
      renderText();
      resize();
      draw();
    }

    // ---- interaction ----
    const pointers = new Map();
    let dragFrom = null, pinchFrom = null;
    canvas.addEventListener('pointerdown', (e) => {
      canvas.setPointerCapture(e.pointerId);
      pointers.set(e.pointerId, [e.clientX, e.clientY]);
      if (pointers.size === 1) dragFrom = { x: e.clientX, y: e.clientY, yaw: st.yaw, pitch: st.pitch };
      if (pointers.size === 2) {
        const [a, b] = Array.from(pointers.values());
        pinchFrom = { d: Math.hypot(a[0] - b[0], a[1] - b[1]), zoom: st.zoom };
        dragFrom = null;
      }
      tip.style.display = 'none';
    });
    canvas.addEventListener('pointermove', (e) => {
      if (pointers.has(e.pointerId)) {
        pointers.set(e.pointerId, [e.clientX, e.clientY]);
        if (pinchFrom && pointers.size === 2) {
          const [a, b] = Array.from(pointers.values());
          const d = Math.hypot(a[0] - b[0], a[1] - b[1]);
          st.zoom = Math.max(0.6, Math.min(5, pinchFrom.zoom * d / Math.max(1, pinchFrom.d)));
          request();
        } else if (dragFrom) {
          st.yaw = dragFrom.yaw + (e.clientX - dragFrom.x) * 0.008;
          if (!st.top) st.pitch = Math.max(0.2, Math.min(Math.PI / 2, dragFrom.pitch + (e.clientY - dragFrom.y) * 0.006));
          request();
        }
        return;
      }
      hover(e);
    });
    const endPointer = (e) => {
      pointers.delete(e.pointerId);
      if (pointers.size < 2) pinchFrom = null;
      if (pointers.size === 0) dragFrom = null;
    };
    canvas.addEventListener('pointerup', endPointer);
    canvas.addEventListener('pointercancel', endPointer);
    canvas.addEventListener('pointerleave', () => { tip.style.display = 'none'; });
    canvas.addEventListener('wheel', (e) => {
      // Ctrl+wheel (and trackpad pinch, which arrives as ctrl+wheel) zooms;
      // a plain wheel keeps scrolling the page.
      if (!e.ctrlKey) return;
      e.preventDefault();
      st.zoom = Math.max(0.6, Math.min(5, st.zoom * Math.exp(-e.deltaY * 0.002)));
      request();
    }, { passive: false });
    canvas.addEventListener('keydown', (e) => {
      const k = e.key;
      if (k === 'ArrowLeft') st.yaw -= 0.08;
      else if (k === 'ArrowRight') st.yaw += 0.08;
      else if (k === 'ArrowUp') st.pitch = Math.min(Math.PI / 2, st.pitch + 0.06);
      else if (k === 'ArrowDown') st.pitch = Math.max(0.2, st.pitch - 0.06);
      else if (k === '+' || k === '=') st.zoom = Math.min(5, st.zoom * 1.15);
      else if (k === '-') st.zoom = Math.max(0.6, st.zoom / 1.15);
      else return;
      e.preventDefault();
      request();
    });

    function hover(e) {
      const pj = st.proj, P = st.P;
      if (!pj || !P) return;
      const rect = canvas.getBoundingClientRect();
      const mx = e.clientX - rect.left, my = e.clientY - rect.top;
      let best = -1, bestD = 64;
      for (let j = 0; j < pj.x.length; j += 1) {
        const dx = pj.x[j] - mx, dy = pj.y[j] - my;
        const d = dx * dx + dy * dy;
        if (d < bestD) { bestD = d; best = j; }
      }
      if (best < 0) { tip.style.display = 'none'; return; }
      const src = pj.kind[best] === 0 ? P.heard : P.missed;
      const i = pj.i[best];
      const s = src.slice[i];
      const brg = (src.b[i] + 0.5) * (360 / P.B);
      const r0 = src.ring[i] * P.ringKm;
      const what = pj.kind[best] === 0 ? 'Heard · ' : 'Passed in range, not heard · ';
      tip.textContent = `${what}${r0}–${r0 + P.ringKm} km ${compassName(brg)} · ${(s * 0.5).toFixed(1)}–${((s + 1) * 0.5).toFixed(1)} km up`;
      tip.style.left = pj.x[best] + 'px';
      tip.style.top = pj.y[best] + 'px';
      tip.style.display = 'block';
    }

    el.querySelector('.alc-ctl').addEventListener('click', (e) => {
      const btn = e.target.closest('button');
      if (!btn || btn.disabled) return;
      if (btn.dataset.band) st.band = btn.dataset.band;
      else if (btn.dataset.lay) st.layers[btn.dataset.lay] = !st.layers[btn.dataset.lay];
      else if (btn.dataset.view === '3d') st.top = false;
      else if (btn.dataset.view === 'top') st.top = true;
      else if (btn.dataset.view === 'rotate') st.rotating = !st.rotating;
      else if (btn.dataset.view === 'in') st.zoom = Math.min(5, st.zoom * 1.2);
      else if (btn.dataset.view === 'out') st.zoom = Math.max(0.6, st.zoom / 1.2);
      setButtons();
      renderText();
      request();
    });

    const ro = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(() => resize()) : null;
    if (ro) ro.observe(el); else window.addEventListener('resize', resize);

    const api = {
      update,
      destroy() {
        if (st.raf) cancelAnimationFrame(st.raf);
        st.rotating = false;
        if (ro) ro.disconnect(); else window.removeEventListener('resize', resize);
        el._alc = null;
        el.innerHTML = '';
      },
    };
    el._alc = api;
    update(data);
    return api;
  }

  window.AdsbLightcone = { mount };
})();
