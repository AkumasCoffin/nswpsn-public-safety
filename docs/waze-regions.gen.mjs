/**
 * Generator for the NSWPSN Waze Forwarder REGIONS grid.
 *
 * Goal: gapless coverage of populated/coastal NSW at zoom 14, with extra
 * overlap along the coastline, plus the 4 intentional zoom-13 "Wide Zone"
 * catch-alls kept verbatim. Sized to the user-measured z14 viewport:
 *   0.2235 deg lat x 0.6337 deg lon  (~25 km N-S x ~58 km E-W)
 * We step tighter than the viewport so adjacent tiles overlap (no gaps).
 *
 * Output is a JS array literal pasted into docs/waze-userscript.user.js.
 */

// Measured z14 viewport (deg). Step < viewport => overlap => no gaps.
const VIEW_LAT = 0.2235;
const VIEW_LON = 0.6337;

// Coast gets the tightest spacing (full overlap, user priority).
const COAST_LAT_STEP = 0.15;          // ~33% overlap N-S along the coast
// Inland grids: ~20% overlap on both axes.
const GRID_LAT_STEP = 0.18;
const GRID_LON_STEP = 0.50;

const Z = 14;

// ---- The 4 intentional wide-zone catch-alls (kept verbatim, z13) --------
const WIDE = [
  { name: 'Wide Zone 1', lat: -36.778458, lon: 149.950325, zoom: 13 },
  { name: 'Wide Zone 2', lat: -35.639371, lon: 149.883138, zoom: 13 },
  { name: 'Wide Zone 3', lat: -30.059437, lon: 152.449813, zoom: 13 },
  { name: 'Wide Zone 4', lat: -34.015957, lon: 146.195516, zoom: 13 },
];

// ---- NSW coastline waypoints (lat -> coast lon), N to S ------------------
// Coarse but enough to track the coast for tile placement.
const COAST = [
  [-28.17, 153.55], [-28.55, 153.55], [-28.87, 153.60], [-29.43, 153.36],
  [-29.69, 153.30], [-30.30, 153.14], [-30.71, 152.93], [-31.09, 152.90],
  [-31.43, 152.91], [-31.65, 152.82], [-31.90, 152.73], [-32.18, 152.51],
  [-32.45, 152.52], [-32.72, 152.16], [-32.93, 151.78], [-33.28, 151.57],
  [-33.43, 151.45], [-33.70, 151.32], [-33.86, 151.28], [-34.07, 151.15],
  [-34.30, 150.95], [-34.47, 150.90], [-34.70, 150.85], [-34.87, 150.77],
  [-35.07, 150.80], [-35.36, 150.47], [-35.55, 150.32], [-35.71, 150.18],
  [-36.00, 150.15], [-36.22, 150.13], [-36.42, 150.06], [-36.69, 149.95],
  [-36.88, 149.92], [-37.07, 149.91], [-37.30, 149.80], [-37.50, 149.76],
];

function coastLonAt(lat) {
  // Linear interp across COAST waypoints (lat descends).
  if (lat >= COAST[0][0]) return COAST[0][1];
  if (lat <= COAST[COAST.length - 1][0]) return COAST[COAST.length - 1][1];
  for (let i = 0; i < COAST.length - 1; i++) {
    const [laA, loA] = COAST[i];
    const [laB, loB] = COAST[i + 1];
    if (lat <= laA && lat >= laB) {
      const t = (lat - laA) / (laB - laA);
      return loA + t * (loB - loA);
    }
  }
  return COAST[COAST.length - 1][1];
}

// ---- Gazetteer for nearest-town labelling --------------------------------
const TOWNS = [
  // Coast / metro
  ['Tweed Heads', -28.18, 153.54], ['Kingscliff', -28.25, 153.58],
  ['Ballina', -28.87, 153.56], ['Lennox Head', -28.79, 153.59],
  ['Byron Bay', -28.64, 153.61], ['Yamba', -29.44, 153.36],
  ['Grafton', -29.69, 152.93], ['Coffs Harbour', -30.30, 153.12],
  ['Sawtell', -30.37, 153.10], ['Nambucca Heads', -30.64, 152.99],
  ['Macksville', -30.71, 152.92], ['Kempsey', -31.08, 152.84],
  ['South West Rocks', -30.88, 153.04], ['Port Macquarie', -31.43, 152.90],
  ['Laurieton', -31.65, 152.80], ['Taree', -31.90, 152.46],
  ['Forster', -32.18, 152.51], ['Tea Gardens', -32.66, 152.16],
  ['Nelson Bay', -32.72, 152.15], ['Newcastle', -32.93, 151.78],
  ['Maitland', -32.73, 151.55], ['Cessnock', -32.83, 151.36],
  ['Gosford', -33.43, 151.34], ['Terrigal', -33.45, 151.45],
  ['Hornsby', -33.70, 151.10], ['Northern Beaches', -33.74, 151.29],
  ['Sydney CBD', -33.87, 151.21], ['Parramatta', -33.81, 151.00],
  ['Penrith', -33.75, 150.69], ['Blacktown', -33.77, 150.91],
  ['Liverpool', -33.92, 150.92], ['Campbelltown', -34.07, 150.81],
  ['Cronulla', -34.06, 151.15], ['Sutherland', -34.03, 151.06],
  ['Katoomba', -33.71, 150.31], ['Lithgow', -33.48, 150.15],
  ['Wollongong', -34.42, 150.89], ['Shellharbour', -34.58, 150.87],
  ['Kiama', -34.67, 150.85], ['Nowra', -34.87, 150.60],
  ['Culburra', -34.93, 150.76], ['Ulladulla', -35.36, 150.47],
  ['Batemans Bay', -35.71, 150.18], ['Moruya', -35.91, 150.08],
  ['Narooma', -36.22, 150.13], ['Bega', -36.67, 149.84],
  ['Merimbula', -36.90, 149.91], ['Eden', -37.07, 149.90],
  // Inland
  ['Bowral', -34.48, 150.42], ['Goulburn', -34.75, 149.72],
  ['Yass', -34.84, 148.91], ['Queanbeyan', -35.35, 149.23],
  ['Canberra', -35.28, 149.13], ['Cooma', -36.23, 149.13],
  ['Bombala', -36.91, 149.24], ['Jindabyne', -36.41, 148.62],
  ['Tumut', -35.30, 148.22], ['Gundagai', -35.07, 148.10],
  ['Wagga Wagga', -35.12, 147.37], ['Junee', -34.87, 147.58],
  ['Temora', -34.45, 147.54], ['Cootamundra', -34.64, 148.03],
  ['Young', -34.31, 148.30], ['Cowra', -33.83, 148.69],
  ['Bathurst', -33.42, 149.58], ['Orange', -33.28, 149.10],
  ['Mudgee', -32.59, 149.59], ['Dubbo', -32.24, 148.60],
  ['Wellington', -32.55, 148.95], ['Parkes', -33.14, 148.18],
  ['Forbes', -33.38, 148.01], ['Condobolin', -33.09, 147.15],
  ['West Wyalong', -33.92, 147.23], ['Griffith', -34.29, 146.05],
  ['Leeton', -34.55, 146.41], ['Narrandera', -34.75, 146.55],
  ['Hay', -34.51, 144.84], ['Deniliquin', -35.53, 144.96],
  ['Finley', -35.65, 145.57], ['Tocumwal', -35.81, 145.57],
  ['Albury', -36.07, 146.92], ['Corowa', -36.00, 146.39],
  ['Holbrook', -35.72, 147.31], ['Tumbarumba', -35.78, 148.01],
  ['Wentworth', -34.11, 141.92], ['Broken Hill', -31.96, 141.47],
  ['Cobar', -31.50, 145.84], ['Nyngan', -31.56, 147.19],
  ['Bourke', -30.09, 145.94], ['Brewarrina', -29.96, 146.86],
  ['Walgett', -30.02, 148.12], ['Lightning Ridge', -29.43, 147.98],
  ['Coonamble', -30.95, 148.39], ['Gilgandra', -31.71, 148.66],
  ['Coonabarabran', -31.27, 149.28], ['Mudgee East', -32.27, 149.84],
  ['Moree', -29.46, 149.84], ['Narrabri', -30.32, 149.78],
  ['Wee Waa', -30.22, 149.44], ['Gunnedah', -30.98, 150.25],
  ['Tamworth', -31.09, 150.93], ['Quirindi', -31.51, 150.68],
  ['Armidale', -30.51, 151.67], ['Uralla', -30.64, 151.50],
  ['Glen Innes', -29.74, 151.74], ['Inverell', -29.78, 151.11],
  ['Tenterfield', -29.05, 152.02], ['Guyra', -30.22, 151.67],
  ['Tingha', -29.96, 151.21], ['Warialda', -29.54, 150.58],
  ['Mungindi', -28.98, 148.99], ['Goondiwindi (border)', -28.55, 150.31],
  ['Singleton', -32.57, 151.17], ['Muswellbrook', -32.27, 150.89],
  ['Scone', -32.05, 150.87], ['Murrurundi', -31.76, 150.83],
  ['Kurri Kurri', -32.82, 151.48], ['Raymond Terrace', -32.76, 151.74],
  ['Lake Macquarie', -33.05, 151.55], ['Wyong', -33.28, 151.42],
  ['Mittagong', -34.45, 150.45], ['Marulan', -34.71, 150.00],
  ['Crookwell', -34.46, 149.47], ['Boorowa', -34.44, 148.72],
  ['Grenfell', -33.90, 148.16], ['Gloucester', -32.01, 151.96],
  ['Dungog', -32.40, 151.75], ['Bulahdelah', -32.41, 152.21],
  ['Wauchope', -31.46, 152.73], ['Dorrigo', -30.34, 152.71],
  ['Bellingen', -30.45, 152.90], ['Casino', -28.86, 153.05],
  ['Lismore', -28.81, 153.28], ['Kyogle', -28.62, 152.99],
  ['Murwillumbah', -28.33, 153.39], ['Bega Valley', -36.50, 149.84],
  ['Cobargo', -36.39, 149.88], ['Pambula', -36.93, 149.87],
];

function distKm(la1, lo1, la2, lo2) {
  const dLa = (la1 - la2) * 111;
  const dLo = (lo1 - lo2) * 111 * Math.cos((la1 * Math.PI) / 180);
  return Math.sqrt(dLa * dLa + dLo * dLo);
}

function nearestTown(lat, lon) {
  let best = null;
  let bestD = Infinity;
  for (const [name, la, lo] of TOWNS) {
    const d = distKm(lat, lon, la, lo);
    if (d < bestD) { bestD = d; best = name; }
  }
  return best;
}

// ---- Build candidate tiles -----------------------------------------------
const tiles = [];

// 1. Coastal column (priority, tight overlap). Centre each tile slightly
//    inland (coastLon - 0.17) so the ~0.63-wide viewport covers coast +
//    hinterland, with the coastline sitting inside the eastern half.
for (let lat = -28.20; lat >= -37.45; lat -= COAST_LAT_STEP) {
  const lon = +(coastLonAt(lat) - 0.17).toFixed(4);
  tiles.push({ kind: 'coast', lat: +lat.toFixed(4), lon, zoom: Z });
}

// 2. Sydney basin / metro inland fill (denser than the generic grid; this
//    is the highest-incident-density area where Waze caps at 200/tile).
for (let lat = -33.55; lat >= -34.20; lat -= 0.13) {
  for (let lon = 150.55; lon <= 151.20; lon += 0.28) {
    tiles.push({ kind: 'metro', lat: +lat.toFixed(4), lon: +lon.toFixed(4), zoom: Z });
  }
}

// 3. Inland town tiles — one per gazetteer town inland of the coast strip
//    (coast tiles already blanket the seaboard). Anything within ~25 km of
//    the coast is dropped by the dedup pass below.
for (const [, la, lo] of TOWNS) {
  if (lo < coastLonAt(la) - 0.30) {
    tiles.push({ kind: 'town', lat: +la.toFixed(4), lon: +lo.toFixed(4), zoom: Z });
  }
}

// 4. Light corridor fill along the two busiest inland highways (Hume and
//    Newell) so long gaps between gazetteer towns still get a tile. Kept
//    deliberately sparse — the rest of the interior has little/no Waze
//    traffic and is covered by the wide zones + explicit town tiles, so a
//    full inland grid would just waste the sweep on empty paddock.
const CORRIDORS = [
  // Hume Hwy (Sydney -> Albury): lon roughly tracks these waypoints.
  { pts: [[-34.6, 150.2], [-34.9, 149.7], [-35.1, 148.1], [-35.6, 147.3], [-35.9, 147.1]] },
  // Newell Hwy (Tocumwal -> Goondiwindi): the inland N-S spine.
  { pts: [[-35.4, 145.6], [-34.7, 146.2], [-33.9, 147.2], [-33.1, 148.0], [-32.2, 148.6], [-31.3, 148.9], [-30.3, 149.5], [-29.5, 149.8]] },
];
function interpCorridor(pts, lat) {
  for (let i = 0; i < pts.length - 1; i++) {
    const [laA, loA] = pts[i];
    const [laB, loB] = pts[i + 1];
    const hi = Math.max(laA, laB), lo = Math.min(laA, laB);
    if (lat <= hi && lat >= lo) {
      const t = (lat - laA) / (laB - laA);
      return loA + t * (loB - loA);
    }
  }
  return null;
}
for (const c of CORRIDORS) {
  const latHi = Math.max(...c.pts.map((p) => p[0]));
  const latLo = Math.min(...c.pts.map((p) => p[0]));
  for (let lat = latHi; lat >= latLo; lat -= GRID_LAT_STEP) {
    const lon = interpCorridor(c.pts, lat);
    if (lon == null) continue;
    tiles.push({ kind: 'grid', lat: +lat.toFixed(4), lon: +lon.toFixed(4), zoom: Z });
  }
}

// ---- Dedup: drop any tile "within view of" an already-kept one ------------
// Threshold = ~70% of the generic step on each axis, so deliberate overlap
// survives but genuine redundancy (the old Coolamon/Coolamon South,
// Junee/Junee South near-duplicates) is removed.
const DEDUP_LAT = GRID_LAT_STEP * 0.7;   // ~0.126
const DEDUP_LON = GRID_LON_STEP * 0.7;   // ~0.35
// Priority order so the gapless coast + dense metro win over generic grid.
const order = { coast: 0, metro: 1, town: 2, grid: 3 };
tiles.sort((a, b) => order[a.kind] - order[b.kind]);

const kept = [];
for (const t of tiles) {
  let redundant = false;
  for (const k of kept) {
    if (Math.abs(t.lat - k.lat) < DEDUP_LAT && Math.abs(t.lon - k.lon) < DEDUP_LON) {
      redundant = true;
      break;
    }
  }
  if (!redundant) kept.push(t);
}

// ---- Label + order N->S for readable logs --------------------------------
kept.sort((a, b) => b.lat - a.lat); // north first
const nameCounts = {};
const labelled = kept.map((t) => {
  let name = nearestTown(t.lat, t.lon);
  nameCounts[name] = (nameCounts[name] || 0) + 1;
  return { name, lat: t.lat, lon: t.lon, zoom: t.zoom, _count: nameCounts[name] };
});
// Disambiguate repeated names with a counter.
const totalForName = {};
for (const l of labelled) totalForName[l.name] = (totalForName[l.name] || 0) + 1;
const seen = {};
for (const l of labelled) {
  if (totalForName[l.name] > 1) {
    seen[l.name] = (seen[l.name] || 0) + 1;
    l.name = `${l.name} ${seen[l.name]}`;
  }
  delete l._count;
}

const REGIONS = [...WIDE, ...labelled];

// ---- Coastline gap self-check -------------------------------------------
const coastTiles = kept.filter((t) => t.kind === 'coast').sort((a, b) => b.lat - a.lat);
let maxGap = 0;
for (let i = 0; i < coastTiles.length - 1; i++) {
  maxGap = Math.max(maxGap, Math.abs(coastTiles[i].lat - coastTiles[i + 1].lat));
}

// ---- Emit ----------------------------------------------------------------
const lines = REGIONS.map(
  (r) => `        { name: ${JSON.stringify(r.name)}, lat: ${r.lat}, lon: ${r.lon}, zoom: ${r.zoom} },`,
);
const literal = `    const REGIONS = [\n${lines.join('\n')}\n    ];`;

console.error(`total tiles: ${REGIONS.length} (4 wide + ${labelled.length} z14)`);
console.error(`coastal tiles: ${coastTiles.length}, max coastal lat gap: ${maxGap.toFixed(3)} deg (viewport ${VIEW_LAT})`);
console.error(`gapless coast: ${maxGap < VIEW_LAT ? 'YES' : 'NO — GAP!'}`);
console.log(literal);
