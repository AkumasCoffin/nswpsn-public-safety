#!/usr/bin/env node
/**
 * Import administrative boundaries from ArcGIS into the `boundaries`
 * table (migration 083).
 *
 * Three datasets, all national:
 *   locality  15,785  suburbs and gazetted localities
 *   lga        2,210  councils
 *   state     12,844  state/territory polygons (islands are separate rows)
 *
 * ONE ROW PER PART, NOT PER PLACE. Upstream stores a multipart boundary
 * as several features sharing one persistent id: 15,785 locality features
 * cover 15,667 localities, 2,210 LGA features cover 564 councils, and the
 * 12,844 state features are 9 states - Tasmania alone is 5,887 polygons,
 * nearly all of them islets. Keeping the parts separate is what the API
 * wants (it filters by bounding box and ships the largest first), so the
 * pid alone cannot be the key: parts of one place would collide on
 * (kind, ext_id) and Postgres rejects the whole batch with "ON CONFLICT
 * DO UPDATE command cannot affect row a second time". partKeys() below
 * hands each part its own id.
 *
 * WHY maxAllowableOffset=0.0005. The service generalises server-side
 * before it sends, and whatever detail it drops there cannot be
 * recovered afterwards. An earlier build of the state mask queried at
 * 0.01 (~1.1 km) and the ACT came back as a 20-point blob that swallowed
 * Queanbeyan, while the Murray border drifted over Moama and Barham.
 * 0.0005 (~55 m) is fine enough that borders land in the right place and
 * still keeps the whole import to tens of MB rather than hundreds.
 *
 * Re-running is safe and cheap: pages are cached under
 * data/boundaries/source/ (gitignored) so a repeat run doesn't hit their
 * servers again, and rows upsert on (kind, ext_id).
 *
 * Usage (from backends/node):
 *   node scripts/import-boundaries.mjs                   # report only
 *   node scripts/import-boundaries.mjs --apply
 *   node scripts/import-boundaries.mjs --apply --kind=lga
 *   node scripts/import-boundaries.mjs --apply --refetch  # ignore the cache
 *   node scripts/import-boundaries.mjs --apply --resume   # skip stored kinds
 *
 * The import is a few hundred requests to someone else's server over
 * several minutes, so a blip somewhere in the middle is expected rather
 * than exceptional - one ETIMEDOUT on the first LGA request once threw
 * away a completed 15,785-row locality import's worth of progress from
 * the same run. Every request retries with backoff, and each kind lands
 * in its own transaction, so a kind that finished stays finished.
 */
import pg from 'pg';
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const NODE_DIR = dirname(HERE);
const REPO_ROOT = join(NODE_DIR, '..', '..');
const CACHE_DIR = join(REPO_ROOT, 'data', 'boundaries', 'source');

const APPLY = process.argv.includes('--apply');
const REFETCH = process.argv.includes('--refetch');
const RESUME = process.argv.includes('--resume');
const kindArg = process.argv.find((a) => a.startsWith('--kind='));
const ONLY_KIND = kindArg ? kindArg.slice('--kind='.length) : null;

const BASE = 'https://services-ap1.arcgis.com/ypkPEy1AmwPKGNNv/arcgis/rest/services';

/**
 * Which upstream field fills each column. `ext_id` must be upstream's
 * own persistent id, not objectid — objectid is per-release and would
 * duplicate every row on the next import.
 */
const DATASETS = [
  {
    kind: 'locality',
    url: `${BASE}/Localities/FeatureServer/0`,
    fields: 'loc_pid,loc_name,loc_class,state',
    map: (a) => ({
      ext_id: a.loc_pid,
      name: a.loc_name,
      short_name: null,
      state: a.state,
      class: a.loc_class,
    }),
  },
  {
    kind: 'lga',
    url: `${BASE}/LGA/FeatureServer/9`,
    fields: 'lga_pid,lga_name,abb_name,state',
    map: (a) => ({
      ext_id: a.lga_pid,
      name: a.lga_name,
      short_name: a.abb_name,
      state: a.state,
      class: null,
    }),
  },
  {
    kind: 'state',
    url: `${BASE}/State/FeatureServer/11`,
    fields: 'state_pid,state_name,state_abbrev',
    map: (a) => ({
      ext_id: a.state_pid,
      name: a.state_name,
      short_name: a.state_abbrev,
      state: a.state_abbrev,
      class: null,
    }),
  },
];

const PAGE = 1000; // the services' maxRecordCount
const OFFSET = 0.0005;
const PRECISION = 5;

function loadEnvFile(path) {
  let text;
  try {
    text = readFileSync(path, 'utf8');
  } catch {
    return false;
  }
  for (const line of text.split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith('#')) continue;
    const eq = t.indexOf('=');
    if (eq < 1) continue;
    const key = t.slice(0, eq).trim();
    let val = t.slice(eq + 1).trim().replace(/\r$/, '');
    if (
      (val.startsWith('"') && val.endsWith('"')) ||
      (val.startsWith("'") && val.endsWith("'"))
    ) {
      val = val.slice(1, -1);
    }
    if (process.env[key] === undefined) process.env[key] = val;
  }
  return true;
}

const ENV_PATH = join(NODE_DIR, '..', '.env');
if (!process.env.DATABASE_URL) loadEnvFile(ENV_PATH);

const url = process.env.DATABASE_URL;
if (!url) {
  console.error(`DATABASE_URL is not set, and no usable value in ${ENV_PATH}`);
  process.exit(1);
}

const pool = new pg.Pool({ connectionString: url, max: 4 });

const TRIES = 4;
const REQ_TIMEOUT_MS = 90_000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * One request, retried. Without this a single dropped connection ends the
 * run, and with three national datasets behind a few hundred requests
 * that is a matter of when rather than if. 5xx and network errors are
 * worth another go; a 4xx is us asking wrongly and repeating it would
 * only be slower.
 */
async function getJson(url, label) {
  let last;
  for (let attempt = 1; attempt <= TRIES; attempt += 1) {
    try {
      const r = await fetch(url, { signal: AbortSignal.timeout(REQ_TIMEOUT_MS) });
      if (r.ok) return await r.json();
      if (r.status < 500) throw new Error(`${label}: HTTP ${r.status}`);
      last = new Error(`${label}: HTTP ${r.status}`);
    } catch (err) {
      if (/HTTP 4\d\d/.test(err.message)) throw err;
      last = err;
    }
    if (attempt < TRIES) {
      const wait = 2000 * 2 ** (attempt - 1);
      process.stdout.write(`\n  ${label} failed (${last.message}); retrying in ${wait / 1000}s\n`);
      await sleep(wait);
    }
  }
  throw last;
}

async function countRemote(ds) {
  const q = `${ds.url}/query?where=1%3D1&returnCountOnly=true&f=json`;
  return (await getJson(q, `count ${ds.kind}`)).count;
}

/** One page, from the cache when we already have it. */
async function fetchPage(ds, offset) {
  const file = join(CACHE_DIR, `${ds.kind}_${offset}.json`);
  if (!REFETCH && existsSync(file)) {
    return JSON.parse(readFileSync(file, 'utf8'));
  }
  const q =
    `${ds.url}/query?where=1%3D1&outFields=${encodeURIComponent(ds.fields)}` +
    `&outSR=4326&maxAllowableOffset=${OFFSET}&geometryPrecision=${PRECISION}` +
    `&resultOffset=${offset}&resultRecordCount=${PAGE}&f=geojson`;
  const body = await getJson(q, `${ds.kind} @${offset}`);
  mkdirSync(CACHE_DIR, { recursive: true });
  writeFileSync(file, JSON.stringify(body));
  return body;
}

/** Bounding box of a GeoJSON Polygon / MultiPolygon. */
function bboxOf(geom) {
  let minLat = Infinity, minLon = Infinity, maxLat = -Infinity, maxLon = -Infinity;
  const ring = (r) => {
    for (const pt of r) {
      if (!Array.isArray(pt) || pt.length < 2) continue;
      const [x, y] = pt;
      if (typeof x !== 'number' || typeof y !== 'number') continue;
      if (x < minLon) minLon = x;
      if (x > maxLon) maxLon = x;
      if (y < minLat) minLat = y;
      if (y > maxLat) maxLat = y;
    }
  };
  if (!geom) return null;
  if (geom.type === 'Polygon') (geom.coordinates || []).forEach(ring);
  else if (geom.type === 'MultiPolygon') {
    for (const poly of geom.coordinates || []) (poly || []).forEach(ring);
  } else return null;
  if (!Number.isFinite(minLat) || !Number.isFinite(minLon)) return null;
  return { minLat, minLon, maxLat, maxLon };
}

/**
 * Give every feature a key of its own.
 *
 * A place with a single part keeps its bare upstream pid, which is the
 * overwhelming majority of rows and keeps them traceable. The parts of a
 * multipart place are ordered largest first and suffixed - so `#0` is the
 * mainland and the islets follow. Ordering by size rather than by the
 * order upstream happened to send them means the suffixes stay put across
 * re-imports even if that ordering changes.
 */
function partKeys(rows) {
  const byPid = new Map();
  for (const r of rows) {
    const list = byPid.get(r.ext_id);
    if (list) list.push(r);
    else byPid.set(r.ext_id, [r]);
  }
  let multipart = 0;
  for (const [pid, parts] of byPid) {
    if (parts.length === 1) continue;
    multipart += 1;
    const area = (r) => (r.bb.maxLat - r.bb.minLat) * (r.bb.maxLon - r.bb.minLon);
    // Ties broken on the box itself so the order is fully determined.
    parts.sort(
      (a, b) =>
        area(b) - area(a) ||
        a.bb.minLat - b.bb.minLat ||
        a.bb.minLon - b.bb.minLon,
    );
    parts.forEach((r, i) => {
      r.ext_id = `${pid}#${i}`;
    });
  }
  return { places: byPid.size, multipart };
}

async function importDataset(ds) {
  const remote = await countRemote(ds);
  console.log(`\n${ds.kind}: ${remote} features upstream`);

  const rows = [];
  let skipped = 0;
  for (let offset = 0; offset < remote; offset += PAGE) {
    const page = await fetchPage(ds, offset);
    for (const f of page.features || []) {
      const a = f.properties || {};
      const m = ds.map(a);
      const bb = bboxOf(f.geometry);
      // No id or no usable geometry means nothing can be drawn or
      // matched — count it rather than storing an unusable row.
      if (!m.ext_id || !m.name || !bb) {
        skipped += 1;
        continue;
      }
      rows.push({ ...m, bb, geom: f.geometry });
    }
    process.stdout.write(`\r  fetched ${Math.min(offset + PAGE, remote)}/${remote}`);
  }
  process.stdout.write('\n');

  const byState = {};
  for (const r of rows) byState[r.state || '?'] = (byState[r.state || '?'] ?? 0) + 1;
  const { places, multipart } = partKeys(rows);
  console.log(`  usable: ${rows.length}${skipped ? `, skipped ${skipped}` : ''}`);
  console.log(`  places: ${places}${multipart ? ` (${multipart} multipart, split into their parts)` : ''}`);
  console.log(`  by state: ${JSON.stringify(byState)}`);
  if (!APPLY) return rows.length;

  // Insert with UNNEST rather than a built-up VALUES list: one array per
  // column keeps the parameter count at 11 whatever the chunk size, so
  // the column list and the placeholders cannot drift apart.
  // Replace the kind wholesale inside one transaction. An upsert alone
  // would leave behind any row whose key changed since the last import -
  // including the ones a half-finished earlier run wrote under the old
  // scheme - and a failure partway through would leave the table holding
  // a mix of both. Either the new set lands complete or nothing moves.
  const CHUNK = 500;
  let written = 0;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM boundaries WHERE kind = $1', [ds.kind]);
    for (let i = 0; i < rows.length; i += CHUNK) {
      const chunk = rows.slice(i, i + CHUNK);
      await client.query(
        `INSERT INTO boundaries
           (kind, ext_id, name, short_name, state, class,
            min_lat, min_lon, max_lat, max_lon, geom)
         SELECT * FROM UNNEST(
           $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
           $7::float8[], $8::float8[], $9::float8[], $10::float8[], $11::jsonb[]
         )
         ON CONFLICT (kind, ext_id) DO UPDATE SET
           name = EXCLUDED.name,
           short_name = EXCLUDED.short_name,
           state = EXCLUDED.state,
           class = EXCLUDED.class,
           min_lat = EXCLUDED.min_lat,
           min_lon = EXCLUDED.min_lon,
           max_lat = EXCLUDED.max_lat,
           max_lon = EXCLUDED.max_lon,
           geom = EXCLUDED.geom,
           imported_at = now()`,
        [
          chunk.map(() => ds.kind),
          chunk.map((r) => r.ext_id),
          chunk.map((r) => r.name),
          chunk.map((r) => r.short_name),
          chunk.map((r) => r.state),
          chunk.map((r) => r.class),
          chunk.map((r) => r.bb.minLat),
          chunk.map((r) => r.bb.minLon),
          chunk.map((r) => r.bb.maxLat),
          chunk.map((r) => r.bb.maxLon),
          chunk.map((r) => JSON.stringify(r.geom)),
        ],
      );
      written += chunk.length;
      process.stdout.write(`  written ${written}/${rows.length}`);
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
  process.stdout.write('\n');
  return rows.length;
}

async function run() {
  let sets = ONLY_KIND ? DATASETS.filter((d) => d.kind === ONLY_KIND) : DATASETS;
  if (!sets.length) {
    console.error(`unknown --kind. Use one of: ${DATASETS.map((d) => d.kind).join(', ')}`);
    process.exit(1);
  }

  if (RESUME && APPLY) {
    const { rows } = await pool.query(
      `SELECT kind FROM boundaries GROUP BY kind HAVING count(*) > 0`,
    );
    const done = new Set(rows.map((r) => r.kind));
    const skip = sets.filter((d) => done.has(d.kind)).map((d) => d.kind);
    if (skip.length) console.log(`--resume: ${skip.join(', ')} already stored, skipping`);
    sets = sets.filter((d) => !done.has(d.kind));
  }

  // A kind that fails should not discard the ones that worked - each is
  // its own transaction, so report and carry on rather than exiting.
  const failed = [];
  for (const ds of sets) {
    try {
      await importDataset(ds);
    } catch (err) {
      failed.push(ds.kind);
      console.error(`\n  ${ds.kind} failed: ${err.message}`);
    }
  }
  if (failed.length) {
    console.error(`\nincomplete: ${failed.join(', ')} — re-run to finish (cached pages make it quick)`);
    process.exitCode = 1;
  }

  if (APPLY) {
    const { rows } = await pool.query(
      `SELECT kind, count(*)::int AS n FROM boundaries GROUP BY kind ORDER BY kind`,
    );
    console.log('\nstored:', JSON.stringify(Object.fromEntries(rows.map((r) => [r.kind, r.n]))));
  } else {
    console.log('\nDry run — re-run with --apply to write.');
  }
}

run()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => pool.end());
