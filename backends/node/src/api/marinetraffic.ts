/**
 * MarineTraffic vessel-position proxy.
 *
 *   GET /api/marinetraffic/vessels
 *
 * Fetches a hardcoded set of MarineTraffic tiles via the headless browser
 * worker (services/marinetrafficBrowser.ts), merges them into a single
 * `{ data: { rows, areaShips } }` payload, dedupes by SHIP_ID/MMSI, and
 * caches the result. The tile list below was hand-picked by the user
 * after verifying each URL returns vessels in their browser.
 *
 * Caching strategy:
 *   - A background refresh loop keeps the cache populated so user
 *     requests never wait on a cold fetch. The loop schedules its next
 *     run after each completion, with the gap depending on whether a
 *     user has hit the endpoint recently:
 *       - Active (request within last ACTIVE_WINDOW_MS): 60 s gap.
 *       - Idle: 120 s gap.
 *   - The HTTP handler always serves whatever's in the cache. It only
 *     blocks on a live fetch if there's no cached payload at all
 *     (cold-start, before the first background refresh completes).
 *
 * Failure modes:
 *   - 503 if the browser worker isn't ready (chromium not installed,
 *     `MARINETRAFFIC_DISABLED=true`, or initial page load failed) AND
 *     we have no cached payload to fall back on.
 *   - 502 on cold-start if every tile fetch fails. Partial successes
 *     are merged and returned regardless.
 *   - On any failure with a previous cached payload available, that
 *     payload is served (stale-while-failing).
 *
 * /api/marinetraffic/vessels and /api/marinetraffic/vessel/:id are also
 * served at /api/v1/ (server.ts mirrors them), wrapped in `data` there
 * (api/v1/respond.ts). /api/marinetraffic/vessel-image/:id is not.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute, resolver } from 'hono-openapi';
import { log } from '../lib/log.js';
import { getPool } from '../db/pool.js';
import { marinetrafficBrowser } from '../services/marinetrafficBrowser.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  ErrorBody,
  GATED_ERRORS,
  okJson,
  UNAVAILABLE_ERROR,
  type SameShape,
} from './v1/schemas.js';

export const marinetrafficRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_VESSELS = scoped({ source: 'marinetraffic', type: 'vessels', window: 'live' });

/** The merged tiles, as mergeUpstream() builds them. */
interface MarineTiles {
  type: 1;
  data: { rows: Record<string, unknown>[]; areaShips: number };
}

// What /api/v1/ documents (api/v1/openapi.ts). The vessel records are
// MarineTraffic's own, passed through unchanged, so they are described loosely.
const errorJson = (description: string) => ({
  description,
  content: { 'application/json': { schema: resolver(ErrorBody) } },
});

const MarineTilesSchema = z.object({
  type: z.literal(1),
  data: z.object({
    rows: z
      .array(z.record(z.string(), z.unknown()))
      .describe(
        "One per vessel, as MarineTraffic's map tiles give it: MarineTraffic's own upper-case fields (SHIP_ID, SHIPNAME, LAT, LON, …), unchanged and not described here",
      ),
    areaShips: z
      .number()
      .describe("The sum of MarineTraffic's own per-tile vessel counts — the tiles overlap, so it is not a count of distinct vessels — or the number of rows when that sum is 0"),
  }),
});
assertShape<SameShape<z.infer<typeof MarineTilesSchema>, MarineTiles>>();

const MarineVessels = MarineTilesSchema.extend({
  warming: z
    .literal(true)
    .optional()
    .describe('true until the first batch since a restart has come in, and rows is empty until then; absent afterwards'),
}).meta({ id: 'MarineVessels' });

const MarineVessel = z
  .record(z.string(), z.unknown())
  .describe("MarineTraffic's own record for the vessel, as its vessel page loads it: the fields are MarineTraffic's, vary between vessels and are not described here")
  .meta({ id: 'MarineVessel' });

// User-verified working tiles (paste one in a browser and you get vessel
// JSON back). Together they cover the major shipping basins worldwide
// plus Australian waters at multiple zoom levels.
const UPSTREAM_URLS = [
  // Global mid-ocean basins (zoom 3)
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:2/Y:1/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:1/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:2/Y:2/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:2/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:0/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:1/Y:2/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:1/Y:1/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:3/station:0',
  // Australia: broad
  'https://www.marinetraffic.com/getData/get_data_json_4/z:2/X:1/Y:1/station:0',
  // Australia: medium (zoom 5 — six tiles around AU coast)
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:15/Y:10/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:13/Y:10/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:15/Y:8/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:13/Y:8/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:15/Y:7/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:12/Y:10/station:0',
  // Australia: tight (zoom 6 — four tiles around NSW/VIC/QLD)
  'https://www.marinetraffic.com/getData/get_data_json_4/z:6/X:30/Y:19/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:6/X:28/Y:19/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:6/X:30/Y:18/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:6/X:28/Y:18/station:0',
  // NSW coast wider (zoom 9 — seven tiles)
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:233/Y:153/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:233/Y:154/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:233/Y:155/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:234/Y:155/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:235/Y:153/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:235/Y:154/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:235/Y:155/station:0',
  // Sydney metro (zoom 10 — six tiles)
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:469/Y:305/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:470/Y:306/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:471/Y:305/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:472/Y:305/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:472/Y:306/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:472/Y:307/station:0',
  // Sydney harbour / Botany Bay tight (zoom 11 — eight tiles).
  // These need the per-tile landing URL handling in
  // services/marinetrafficBrowser.ts to return data — MT only serves
  // tiles inside the SPA's current viewport at the matching zoom.
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:940/Y:612/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:940/Y:613/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:941/Y:612/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:941/Y:613/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:942/Y:612/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:943/Y:612/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:943/Y:614/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:943/Y:615/station:0',
];

// Background refresh cadence — gap between the end of one batch and the
// start of the next, while someone is actually watching ships.
const REFRESH_ACTIVE_MS = 60_000;
// With no viewer inside ACTIVE_WINDOW_MS the loop fetches NOTHING — it just
// wakes to check for one. "Idle" used to mean a 120s gap instead of a 60s
// one, and that near-idled the whole box: every one of the 40 tiles loads
// the full MarineTraffic SPA — a WebGL map, software-rendered through
// SwiftShader because this host has no GPU — so a batch is ~3 minutes of
// heavy rendering, and running it around the clock for a layer nobody had
// open averaged ~12 of the box's 16 cores (18,610 CPU-minutes over 26h,
// measured). Ships move slowly; a viewer returning to a stale cache gets a
// kicked refresh (see the route) and fresh data within one batch.
const IDLE_CHECK_MS = 60_000;
const ACTIVE_WINDOW_MS = 5 * 60_000;
// A returning viewer whose cache is older than this kicks a refresh rather
// than waiting for the loop's next tick to notice them.
const STALE_KICK_MS = 3 * 60_000;
// Initial delay before the first background refresh — gives the browser
// worker time to launch chromium and complete its prewarm fetch.
const REFRESH_INITIAL_DELAY_MS = 30_000;

let cache: { data: MarineTiles; fetchedAt: number } | null = null;
let inFlight: Promise<MarineTiles | null> | null = null;
let lastUserHitMs = 0;
let refreshTimer: NodeJS.Timeout | null = null;

// Per-vessel detail cache for /api/marinetraffic/vessel/:id. Two
// layers:
//   1. In-process Map (30 min freshness) for hot vessels.
//   2. Postgres marinetraffic_vessel_cache table (24 h freshness)
//      so every detail we ever fetched persists across restarts and
//      cold-start requests don't trigger a 2-3 s browser navigation
//      for ships we've already seen.
// On a hit, we always re-mint the in-process entry from the DB so
// the next request short-circuits at the Map layer.
const VESSEL_DETAIL_TTL_MS = 30 * 60_000;
const VESSEL_DETAIL_DB_TTL_MS = 24 * 60 * 60_000;
const vesselDetailCache = new Map<
  string,
  { data: unknown; expires: number }
>();

// These per-vessel Maps check TTL on read but nothing ever *removes*
// expired entries, and /api/marinetraffic/vessel-image/:id is a public
// endpoint with integer ids — trivially enumerable. Every other cache in
// the codebase is capacity-bounded (DATA_CACHE_MAX etc.); these were
// missed, so process memory grew monotonically (image entries hold raw
// photo bytes). Cap them with oldest-first eviction — Map preserves
// insertion order, so the first key is the oldest entry.
const VESSEL_CACHE_MAX = 500;
function capMap(m: Map<string, unknown>, max = VESSEL_CACHE_MAX): void {
  while (m.size > max) {
    const oldest = m.keys().next().value;
    if (oldest === undefined) break;
    m.delete(oldest);
  }
}

async function dbReadVesselDetail(id: string): Promise<{
  data: unknown;
  fetchedAtMs: number;
} | null> {
  const pool = await getPool();
  if (!pool) return null;
  try {
    const res = await pool.query<{ data: unknown; fetched_at: Date }>(
      'SELECT data, fetched_at FROM marinetraffic_vessel_cache WHERE ship_id = $1',
      [id],
    );
    const row = res.rows[0];
    if (!row) return null;
    return { data: row.data, fetchedAtMs: row.fetched_at.getTime() };
  } catch (err) {
    log.warn({ err, id }, 'marinetraffic vessel cache: db read failed');
    return null;
  }
}

async function dbWriteVesselDetail(id: string, data: unknown): Promise<void> {
  const pool = await getPool();
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO marinetraffic_vessel_cache (ship_id, data, fetched_at)
       VALUES ($1, $2::jsonb, now())
       ON CONFLICT (ship_id) DO UPDATE
         SET data = EXCLUDED.data, fetched_at = now()`,
      [id, JSON.stringify(data)],
    );
  } catch (err) {
    log.warn({ err, id }, 'marinetraffic vessel cache: db write failed');
  }
}
// Per-vessel image cache for /api/marinetraffic/vessel-image/:id.
// Vessel collection photos change rarely; hold image bytes for 6 h.
const VESSEL_IMAGE_TTL_MS = 6 * 60 * 60_000;
const vesselImageCache = new Map<
  string,
  { bytes: Uint8Array<ArrayBuffer>; contentType: string; expires: number }
>();
// Negative cache for missing images so we don't hammer MT for vessels
// that have no collection photo.
const VESSEL_IMAGE_NEGATIVE_TTL_MS = 30 * 60_000;
const vesselImageMissCache = new Map<string, number>();

// Merge several upstream payloads. MarineTraffic returns
// `{ type: 1, data: { rows: [...], areaShips: N } }` per tile. We merge
// all rows, dedupe by SHIP_ID / SHIPID / MMSI, and sum areaShips counts.
function mergeUpstream(payloads: unknown[]): MarineTiles {
  const seen = new Set<string>();
  const merged: Record<string, unknown>[] = [];
  let total = 0;
  for (const p of payloads) {
    if (!p || typeof p !== 'object') continue;
    const data = (p as { data?: { rows?: unknown[]; areaShips?: number } }).data;
    const rows = Array.isArray(data?.rows) ? data!.rows : [];
    if (typeof data?.areaShips === 'number') total += data.areaShips;
    for (const row of rows as Record<string, unknown>[]) {
      const id = String(row['SHIP_ID'] ?? row['SHIPID'] ?? row['MMSI'] ?? '');
      if (id && seen.has(id)) continue;
      if (id) seen.add(id);
      merged.push(row);
    }
  }
  return { type: 1, data: { rows: merged, areaShips: total || merged.length } };
}

// Fetch every tile in sequence (the browser worker serialises page access
// anyway, so parallel calls would just queue). Returns the merged payload,
// or null if every tile failed.
async function fetchAllTiles(): Promise<MarineTiles | null> {
  const payloads: unknown[] = [];
  let succeeded = 0;
  let failed = 0;
  for (const url of UPSTREAM_URLS) {
    const data = await marinetrafficBrowser.fetchJson(url);
    if (data != null) {
      payloads.push(data);
      succeeded++;
    } else {
      failed++;
    }
  }
  log.info(
    { succeeded, failed, total: UPSTREAM_URLS.length },
    'marinetraffic: tile batch complete',
  );
  if (succeeded === 0) return null;
  return mergeUpstream(payloads);
}

// Background refresh loop. While a viewer is active each tick fetches all
// tiles and replaces the cache; with no viewer it fetches nothing and just
// wakes to check again (see IDLE_CHECK_MS for why that distinction carries
// twelve CPU cores). Runs forever; errors are swallowed so a single failed
// batch doesn't stop the loop.
let idleLogged = false;
async function backgroundRefresh(): Promise<void> {
  refreshTimer = null;
  // Decided once per tick: a viewer arriving mid-batch is picked up by the
  // next tick, which the active cadence keeps a minute away.
  const idle = Date.now() - lastUserHitMs >= ACTIVE_WINDOW_MS;
  try {
    if (idle) {
      // Logged on the transition, not per tick — an idle night is one line,
      // not four hundred.
      if (!idleLogged) {
        idleLogged = true;
        log.info('marinetraffic: no viewers — tile refresh parked');
      }
    } else if (!marinetrafficBrowser.isReady()) {
      log.debug('marinetraffic: skipping refresh — browser not ready');
    } else if (inFlight) {
      log.debug('marinetraffic: skipping refresh — fetch already in flight');
    } else {
      if (idleLogged) {
        idleLogged = false;
        log.info('marinetraffic: viewer returned — tile refresh resumed');
      }
      inFlight = fetchAllTiles().finally(() => {
        inFlight = null;
      });
      const data = await inFlight;
      if (data != null) {
        cache = { data, fetchedAt: Date.now() };
        log.info('marinetraffic: background refresh complete');
      }
    }
  } catch (err) {
    log.warn({ err }, 'marinetraffic: background refresh failed');
  } finally {
    refreshTimer = setTimeout(backgroundRefresh, idle ? IDLE_CHECK_MS : REFRESH_ACTIVE_MS);
    refreshTimer.unref?.();
  }
}

// Kick off the loop once the module loads. The first tick waits a bit
// so the browser worker has time to prewarm; the HTTP handler will
// block on a live fetch in the meantime if anyone hits the endpoint.
refreshTimer = setTimeout(backgroundRefresh, REFRESH_INITIAL_DELAY_MS);
refreshTimer.unref?.();

marinetrafficRouter.get(
  '/api/marinetraffic/vessels',
  SCOPE_VESSELS,
  describeRoute({
    operationId: 'getMarineVessels',
    tags: ['Ships'],
    summary: 'Vessel positions',
    description:
      "Vessels as MarineTraffic's live map shows them, from a fixed set of its map tiles covering the major shipping basins and Australian waters (the NSW coast and Sydney in most detail), merged into one list. The tiles are fetched in batches of a few minutes, about a minute apart while anyone is asking and not at all otherwise, so after a quiet spell the first answers carry the last batch while a new one is fetched. Until the first batch after a restart has come in, the answer is empty and says `warming` — or is a 503 while the browser that fetches the tiles is not running.",
    responses: {
      ...okJson(envelope(MarineVessels), 'The vessels'),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    lastUserHitMs = Date.now();

    // Always serve whatever's in the cache — stale data beats blocking the
    // request for ~3 min on a batch refetch. But a viewer returning after an
    // idle stretch is looking at however old the last active period left it,
    // so a stale cache kicks a refresh in the background; the layer's own
    // polling picks the fresh picture up within a batch.
    if (cache) {
      if (
        Date.now() - cache.fetchedAt > STALE_KICK_MS &&
        !inFlight &&
        marinetrafficBrowser.isReady()
      ) {
        inFlight = fetchAllTiles().finally(() => {
          inFlight = null;
        });
        void inFlight
          .then((data) => {
            if (data != null) cache = { data, fetchedAt: Date.now() };
          })
          .catch(() => { /* fetchAllTiles logs its own failures */ });
      }
      return ok(c, cache.data);
    }

    // Cold start: no cache yet. Block on the first batch.
    if (!marinetrafficBrowser.isReady()) {
      log.warn('marinetraffic: browser not ready');
      return fail(c, 503, 'service_unavailable', 'The MarineTraffic feed is not available just now.', {
        error: 'browser worker not ready',
        message:
          'MarineTraffic proxy requires the headless browser worker. Check that playwright is installed and MARINETRAFFIC_DISABLED is not set.',
        vessels: [],
      });
    }

    if (!inFlight) {
      inFlight = fetchAllTiles().finally(() => {
        inFlight = null;
      });
    }
    // Don't hold the request for the ~3 minutes a full batch takes — that
    // used to be rare (cold start lost a race with the boot-time batch) but
    // with the idle stop it is every first viewer after a quiet spell. Let
    // the batch run and give this response what the batch has NOT yet
    // filled: nothing, plus a flag the client is free to ignore. The layer
    // polls; the next poll serves from cache.
    void inFlight
      .then((data) => {
        if (data != null) cache = { data, fetchedAt: Date.now() };
      })
      .catch(() => { /* fetchAllTiles logs its own failures */ });
    return ok(c, {
      type: 1,
      data: { rows: [], areaShips: 0 },
      warming: true,
    } satisfies z.infer<typeof MarineVessels>);
  },
);

// GET /api/marinetraffic/vessel/:id
//   Returns extended vessel info from MT (name, IMO, MMSI, dimensions,
//   country, type, etc.). Lookup order:
//     1. In-process Map (30 min) — sub-ms hit.
//     2. Postgres marinetraffic_vessel_cache (24 h) — single SELECT,
//        promoted into the Map on hit.
//     3. MT origin via the headless browser worker — slow (~2-3 s),
//        result is written back to both caches.
marinetrafficRouter.get(
  '/api/marinetraffic/vessel/:id',
  SCOPE_VESSELS,
  describeRoute({
    operationId: 'getMarineVessel',
    tags: ['Ships'],
    summary: 'One vessel',
    description:
      "MarineTraffic's details for one vessel (name, IMO, MMSI, dimensions, flag, type and so on). Served from a cache for up to 24 hours, and older when MarineTraffic cannot be reached; otherwise read from MarineTraffic, which takes a few seconds.",
    parameters: [
      {
        in: 'path',
        name: 'id',
        required: true,
        schema: { type: 'string', pattern: '^[0-9]+$' },
        description: "MarineTraffic's ship id: SHIP_ID in the vessels list",
      },
    ],
    responses: {
      ...okJson(envelope(MarineVessel), 'The vessel'),
      ...GATED_ERRORS,
      400: errorJson('The id is not a number (validation_failed)'),
      502: errorJson('MarineTraffic gave no record for the vessel: it may not exist, or the lookup failed; a failure is not remembered, so a later try may work (service_unavailable)'),
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    const id = c.req.param('id');
    if (!/^\d+$/.test(id)) {
      return fail(
        c,
        400,
        'validation_failed',
        'id: not a MarineTraffic ship id (digits only)',
        { error: 'invalid ship id' },
        { issues: [{ path: 'id', message: 'not a MarineTraffic ship id (digits only)' }] },
      );
    }
    const now = Date.now();
    // Layer 1: hot in-process cache.
    const memCached = vesselDetailCache.get(id);
    if (memCached && memCached.expires > now) {
      return ok(c, memCached.data);
    }
    // Layer 2: warm DB cache. Always cheaper than navigating MT, even
    // when stale we promote the row into the Map so the next request
    // skips the DB hop.
    const dbRow = await dbReadVesselDetail(id);
    if (dbRow && now - dbRow.fetchedAtMs < VESSEL_DETAIL_DB_TTL_MS) {
      vesselDetailCache.set(id, {
        data: dbRow.data,
        expires: now + VESSEL_DETAIL_TTL_MS,
      });
      capMap(vesselDetailCache);
      return ok(c, dbRow.data);
    }
    // Layer 3: origin fetch.
    if (!marinetrafficBrowser.isReady()) {
      if (dbRow) return ok(c, dbRow.data); // serve stale DB row
      if (memCached) return ok(c, memCached.data);
      return fail(c, 503, 'service_unavailable', 'MarineTraffic cannot be read just now.', {
        error: 'browser worker not ready',
      });
    }
    const data = await marinetrafficBrowser.fetchVesselDetail(id);
    if (data == null) {
      if (dbRow) return ok(c, dbRow.data); // stale-while-failing
      if (memCached) return ok(c, memCached.data);
      return fail(c, 502, 'service_unavailable', 'MarineTraffic gave no record for this vessel.', {
        error: 'vessel detail not found',
      });
    }
    vesselDetailCache.set(id, { data, expires: now + VESSEL_DETAIL_TTL_MS });
    capMap(vesselDetailCache);
    // Persist to DB asynchronously — don't block the response on it.
    void dbWriteVesselDetail(id, data);
    return ok(c, data);
  },
);

// GET /api/marinetraffic/vessel-image/:id
//   Proxies the MarineTraffic collection photo for a vessel. Bypasses
//   the cross-origin resource policy that blocks the upstream WebP
//   when loaded directly via <img src="https://images.marinetraffic.com/...">.
//   Bytes are cached in memory for 6 h; misses (no photo for the
//   vessel) are negative-cached for 30 min.
marinetrafficRouter.get('/api/marinetraffic/vessel-image/:id', SCOPE_VESSELS, async (c) => {
  const id = c.req.param('id');
  if (!/^\d+$/.test(id)) {
    return c.text('invalid id', 400);
  }
  const now = Date.now();
  const cached = vesselImageCache.get(id);
  if (cached && cached.expires > now) {
    return c.body(cached.bytes, 200, {
      'Content-Type': cached.contentType,
      'Cache-Control': 'public, max-age=21600',
      'Access-Control-Allow-Origin': '*',
    });
  }
  const missAt = vesselImageMissCache.get(id);
  if (missAt && now - missAt < VESSEL_IMAGE_NEGATIVE_TTL_MS) {
    return c.text('no image', 404);
  }
  if (!marinetrafficBrowser.isReady()) {
    if (cached) {
      return c.body(cached.bytes, 200, {
        'Content-Type': cached.contentType,
        'Access-Control-Allow-Origin': '*',
      });
    }
    return c.text('browser worker not ready', 503);
  }
  // Forward MT's curated collection thumbnail. Only set for ships
  // with a chosen primary photo; for the rest we 404 quickly. The
  // earlier gallery-scrape fallback was slow (~10 s page navigation
  // per miss) and 404'd anyway for the vast majority of vessels —
  // not worth the latency or load on the worker browser.
  const collectionUrl = `https://images.marinetraffic.com/collection/${encodeURIComponent(id)}.webp?size=800`;
  const result = await marinetrafficBrowser.fetchBinary(collectionUrl);
  if (!result || result.status >= 400 || result.bytes.length === 0) {
    vesselImageMissCache.set(id, now);
    capMap(vesselImageMissCache);
    if (cached) {
      return c.body(cached.bytes, 200, {
        'Content-Type': cached.contentType,
        'Access-Control-Allow-Origin': '*',
      });
    }
    return c.text('no image', 404);
  }
  vesselImageCache.set(id, {
    bytes: result.bytes,
    contentType: result.contentType,
    expires: now + VESSEL_IMAGE_TTL_MS,
  });
  // Image entries hold raw photo bytes — cap tighter than the others.
  capMap(vesselImageCache, 200);
  return c.body(result.bytes, 200, {
    'Content-Type': result.contentType,
    'Cache-Control': 'public, max-age=21600',
    'Access-Control-Allow-Origin': '*',
  });
});
