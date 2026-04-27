/**
 * One-shot script: hit every important Python-backend endpoint in
 * production, save the JSON response to test/fixtures/contract/.
 *
 * These fixtures become the contract the Node port must match
 * byte-for-byte (or at least shape-compatibly) before each route's
 * cutover in the strangler-fig migration plan.
 *
 * Run with:
 *   API_BASE=https://api.forcequit.xyz \
 *   NSWPSN_API_KEY=xxxxx \
 *   npx tsx scripts/capture-fixtures.ts
 *
 * Notes:
 *   - test/fixtures/contract/ is .gitignored (potentially has live data).
 *   - Endpoints that need params (e.g. ?source=, ?source_id=) get a
 *     representative example call. Add more as new contract cases come up.
 *   - Endpoints that mutate (POST/PUT/DELETE) are skipped — fixtures are
 *     read-only and capturing them would create real records.
 *   - 4xx/5xx responses are captured too — those shapes also matter.
 */
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const OUT_DIR = join(__dirname, '..', 'test', 'fixtures', 'contract');

const API_BASE = process.env.API_BASE ?? 'https://api.forcequit.xyz';
const API_KEY = process.env.NSWPSN_API_KEY ?? '';

if (!API_KEY) {
  console.error(
    'NSWPSN_API_KEY is required. Set it in your shell before running.',
  );
  process.exit(1);
}

// Endpoint groups. Add more as routes get migrated. The shape is
// (filename, path) — filename gets `.json` appended, path is appended
// to API_BASE.
const ENDPOINTS: ReadonlyArray<readonly [string, string]> = [
  // Health / status — public, no key required.
  ['health', '/api/health'],
  ['config', '/api/config'],
  ['status', '/api/status'],

  // Live data feeds — every page hits these.
  ['rfs-incidents', `/api/rfs/incidents?api_key=${API_KEY}`],
  ['bom-warnings', `/api/bom/warnings?api_key=${API_KEY}`],
  ['traffic-incidents', `/api/traffic/incidents?api_key=${API_KEY}`],
  ['traffic-roadwork', `/api/traffic/roadwork?api_key=${API_KEY}`],
  ['traffic-flood', `/api/traffic/flood?api_key=${API_KEY}`],
  ['traffic-fire', `/api/traffic/fire?api_key=${API_KEY}`],
  ['traffic-majorevent', `/api/traffic/majorevent?api_key=${API_KEY}`],
  ['traffic-cameras', `/api/traffic/cameras?api_key=${API_KEY}`],
  ['waze-hazards', `/api/waze/hazards?api_key=${API_KEY}`],
  ['waze-police', `/api/waze/police?api_key=${API_KEY}`],
  ['waze-roadwork', `/api/waze/roadwork?api_key=${API_KEY}`],
  ['waze-metrics', `/api/waze/metrics?api_key=${API_KEY}`],
  ['endeavour-current', `/api/endeavour/current?api_key=${API_KEY}`],
  ['endeavour-future', `/api/endeavour/future?api_key=${API_KEY}`],
  ['endeavour-planned', `/api/endeavour/planned?api_key=${API_KEY}`],
  ['endeavour-maintenance', `/api/endeavour/maintenance?api_key=${API_KEY}`],
  ['ausgrid-outages', `/api/ausgrid/outages?api_key=${API_KEY}`],
  ['ausgrid-stats', `/api/ausgrid/stats?api_key=${API_KEY}`],
  ['essential-outages', `/api/essential/outages?lite=1&api_key=${API_KEY}`],
  ['essential-planned', `/api/essential/planned?api_key=${API_KEY}`],
  ['essential-future', `/api/essential/future?api_key=${API_KEY}`],
  ['beachsafe', `/api/beachsafe?api_key=${API_KEY}`],
  ['beachwatch', `/api/beachwatch?api_key=${API_KEY}`],
  ['weather-current', `/api/weather/current?api_key=${API_KEY}`],
  ['weather-radar', `/api/weather/radar?api_key=${API_KEY}`],
  ['aviation-cameras', `/api/aviation/cameras?api_key=${API_KEY}`],
  ['centralwatch-cameras', `/api/centralwatch/cameras?api_key=${API_KEY}`],
  ['news-rss', `/api/news/rss?limit=10&api_key=${API_KEY}`],
  ['pager-hits', `/api/pager/hits?hours=1&limit=100&api_key=${API_KEY}`],

  // Archive / historical
  [
    'data-history',
    `/api/data/history?limit=20&offset=0&unique=1&hours=24&api_key=${API_KEY}`,
  ],
  [
    'data-history-filters',
    `/api/data/history/filters?hours=24&api_key=${API_KEY}`,
  ],
  ['data-history-sources', `/api/data/history/sources?api_key=${API_KEY}`],
  ['data-history-stats', `/api/data/history/stats?api_key=${API_KEY}`],

  // User / incidents
  ['incidents-active', `/api/incidents?active=true&api_key=${API_KEY}`],
];

async function captureOne(name: string, path: string): Promise<void> {
  const url = `${API_BASE}${path}`;
  const start = Date.now();
  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${API_KEY}` },
    });
    const body = await res.text();
    const elapsed = Date.now() - start;
    const fixture = {
      meta: {
        captured_at: new Date().toISOString(),
        url,
        status: res.status,
        elapsed_ms: elapsed,
        content_type: res.headers.get('content-type'),
      },
      body,
    };
    await writeFile(
      join(OUT_DIR, `${name}.json`),
      JSON.stringify(fixture, null, 2),
      'utf8',
    );
    console.log(`  [${res.status}] ${elapsed}ms  ${name}`);
  } catch (err) {
    console.error(`  [ERR]       ${name}: ${(err as Error).message}`);
  }
}

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  console.log(`Capturing ${ENDPOINTS.length} endpoints to ${OUT_DIR}`);
  console.log(`Base: ${API_BASE}`);
  for (const [name, path] of ENDPOINTS) {
    await captureOne(name, path);
  }
  console.log('Done.');
}

void main();
