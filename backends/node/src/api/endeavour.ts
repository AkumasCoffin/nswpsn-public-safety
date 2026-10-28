/**
 * Endeavour Energy outage routes.
 *
 * Reads from LiveStore — the poller (src/sources/endeavour.ts) keeps
 * three keys fresh: endeavour_current, endeavour_planned (future
 * maintenance), endeavour_maintenance (current maintenance).
 *
 * Response shapes mirror Python (external_api_proxy.py):
 *   - /api/endeavour/current         → array (line 6799-6816)
 *   - /api/endeavour/maintenance     → array (line 5308-5325)
 *   - /api/endeavour/future          → array (line 5328-5345)
 *   - /api/endeavour/planned         → array of planned + future merged
 *                                       (line 5348-5364)
 *
 * The `/all` and `/raw` variants and `/postcodes` read Endeavour's own
 * public outage service (callSupabase — the distributor's Supabase
 * project, not ours) on each request and skip the normalisation step.
 *
 * Empty LiveStore returns `[]` rather than 500 — same as Python's
 * `cached_data is not None` branch returning the live-fetch fallback,
 * which itself returns `[]` on error.
 *
 * All of these but the two `/raw` routes are also served at /api/v1/
 * (server.ts mirrors them), wrapped in `data` there (api/v1/respond.ts).
 * `/current/raw` and `/future/raw` are not: they answer exactly what
 * `/current/all` and `/future/all` do. Where a route reads Endeavour on
 * the request (`/all`, `/postcodes`), the old path answers an empty 200
 * when Endeavour cannot be read, as it always has; /api/v1/ answers 503
 * service_unavailable, so an outage does not read as "no outages".
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { liveStore } from '../store/live.js';
import type { EndeavourOutage } from '../sources/endeavour.js';
import { callSupabase } from '../sources/endeavour.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, isV1, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  GATED_ERRORS,
  okJson,
  UNAVAILABLE_ERROR,
  type SameShape,
} from './v1/schemas.js';

export const endeavourRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_OUTAGES = scoped({ source: 'endeavour', type: 'outages', window: 'live' });

interface SupabaseAreaRaw {
  incident_id?: string;
  outage_type?: string;
  incident_status?: string;
  customers_affected?: number | null;
  center_lat?: number | null;
  center_lng?: number | null;
  [k: string]: unknown;
}

/** get_outage_areas_fast's answer; null when it is not a list. */
async function readAreas(): Promise<SupabaseAreaRaw[] | null> {
  const data = await callSupabase('/rpc/get_outage_areas_fast', {
    method: 'POST',
    body: {},
  });
  return Array.isArray(data) ? (data as SupabaseAreaRaw[]) : null;
}

async function fetchAreas(): Promise<SupabaseAreaRaw[]> {
  return (await readAreas()) ?? [];
}

function readArray(key: string): EndeavourOutage[] {
  const data = liveStore.getData<EndeavourOutage[]>(key);
  return Array.isArray(data) ? data : [];
}

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const AS_GIVEN = 'as Endeavour Energy gives it';

const EndeavourOutageRecord = z
  .object({
    id: z.string().describe("Endeavour Energy's incident id; empty when it gives none"),
    suburb: z
      .string()
      .describe(
        'Title case. When Endeavour Energy stops giving one for an outage, the last it gave; "Unknown" when there has been none',
      ),
    streets: z
      .string()
      .describe(`The street affected, ${AS_GIVEN}; kept from earlier when it stops giving one, else empty`),
    customersAffected: z.number().describe('Customers without power; 0 when not given'),
    status: z
      .string()
      .describe(
        "Endeavour Energy's status in words: Active, Scheduled, Crew Dispatched or Damage Assessed, otherwise its own status in title case",
      ),
    cause: z
      .string()
      .describe(`The cause (else the sub-cause), ${AS_GIVEN}; for planned work with none, "Planned maintenance"; otherwise empty when not given`),
    outageType: z
      .enum(['Unplanned', 'Current Maintenance', 'Future Maintenance'])
      .describe(
        'Unplanned; Current Maintenance (planned work under way — its status says so, or its start has passed); Future Maintenance (planned work not yet started)',
      ),
    startTime: z
      .string()
      .nullable()
      .describe(`When it started or starts, ${AS_GIVEN}; null when not given or not a usable date`),
    estimatedRestoration: z
      .string()
      .nullable()
      .describe(
        `When power is due back — the end time, else the estimated time of restoration — ${AS_GIVEN}; null when not given or not a usable date`,
      ),
    lastUpdated: z.string().describe(`When the record was last updated, ${AS_GIVEN}; empty when not given`),
    latitude: z.number().nullable().describe('Centre of the affected area; null when not given'),
    longitude: z.number().nullable().describe('Centre of the affected area; null when not given'),
    postcode: z.string().describe('As Endeavour Energy gives it; kept from earlier when it stops giving one, else empty'),
    hasGPS: z.boolean().describe('true when latitude and longitude are both given'),
    endTime: z.string().nullable().optional().describe('Planned work only: the same as estimatedRestoration'),
    duration: z.string().optional().describe('Planned work only; empty'),
  })
  .meta({ id: 'EndeavourOutage' });
assertShape<SameShape<z.infer<typeof EndeavourOutageRecord>, EndeavourOutage>>();

const EndeavourOutages = z.array(EndeavourOutageRecord).meta({ id: 'EndeavourOutages' });

// get_outage_areas_fast's records, passed on as they come — this names only
// the fields the poller reads from them.
const EndeavourArea = z
  .object({
    incident_id: z.string().optional().describe('The id the processed outage lists carry as `id`'),
    outage_type: z.string().optional().describe('PLANNED for planned work, in some letter case'),
    incident_status: z.string().optional(),
    customers_affected: z.number().nullable().optional().describe('Customers without power'),
    center_lat: z.number().nullable().optional().describe('Centre of the affected area'),
    center_lng: z.number().nullable().optional().describe('Centre of the affected area'),
  })
  .catchall(z.unknown())
  .describe('An outage area exactly as Endeavour Energy publishes it');
assertShape<SameShape<z.infer<typeof EndeavourArea>, SupabaseAreaRaw>>();

const EndeavourAreas = z.array(EndeavourArea).meta({ id: 'EndeavourAreas' });

const EndeavourPostcodes = z
  .object({
    postcodes: z.array(z.string()).describe('Each once, sorted'),
    count: z.number().int().describe('Number of postcodes'),
  })
  .meta({ id: 'EndeavourPostcodes' });

const UNREADABLE = "Endeavour Energy's outage data could not be read just now.";
const NOT_THE_LIST = 'Endeavour Energy did not answer with its outage data just now.';

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

endeavourRouter.get(
  '/api/endeavour/current',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEndeavourCurrent',
    tags: ['Outages'],
    summary: 'Endeavour Energy unplanned outages',
    description:
      "Unplanned electricity outages on Endeavour Energy's network now, from Endeavour Energy's public outage data, with the address, cause and timing joined onto each outage area. Refreshed every minute.",
    responses: {
      ...okJson(envelope(EndeavourOutages), 'Every unplanned outage'),
      ...GATED_ERRORS,
    },
  }),
  (c) => ok(c, readArray('endeavour_current')),
);

endeavourRouter.get(
  '/api/endeavour/maintenance',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEndeavourMaintenance',
    tags: ['Outages'],
    summary: 'Endeavour Energy planned work under way',
    description:
      "Planned outages on Endeavour Energy's network that are under way: the work's status says so, or its start time has passed. From Endeavour Energy's public outage data; refreshed every five minutes.",
    responses: {
      ...okJson(envelope(EndeavourOutages), 'Every planned outage under way'),
      ...GATED_ERRORS,
    },
  }),
  (c) => ok(c, readArray('endeavour_maintenance')),
);

// Python `/api/endeavour/future` returns the future_maintenance bucket,
// which we store under the LiveStore key `endeavour_planned` (matching
// Python's source-name registry). The route name stays `future` so the
// frontend keeps working.
endeavourRouter.get(
  '/api/endeavour/future',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEndeavourFuture',
    tags: ['Outages'],
    summary: 'Endeavour Energy planned work to come',
    description:
      "Planned outages on Endeavour Energy's network that have not started yet. From Endeavour Energy's public outage data; refreshed every five minutes.",
    responses: {
      ...okJson(envelope(EndeavourOutages), 'Every planned outage still to come'),
      ...GATED_ERRORS,
    },
  }),
  (c) => ok(c, readArray('endeavour_planned')),
);

// `/planned` is the bot-canonical alias: current maintenance + future
// scheduled, flat list. Mirrors Python line 5348-5364.
endeavourRouter.get(
  '/api/endeavour/planned',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEndeavourPlanned',
    tags: ['Outages'],
    summary: 'Endeavour Energy planned outages',
    description:
      "Every planned outage on Endeavour Energy's network: the work under way (as /endeavour/maintenance) followed by the work still to come (as /endeavour/future). Refreshed every five minutes.",
    responses: {
      ...okJson(envelope(EndeavourOutages), 'Planned work under way, then planned work to come'),
      ...GATED_ERRORS,
    },
  }),
  (c) => {
    const items: EndeavourOutage[] = [];
    for (const k of ['endeavour_maintenance', 'endeavour_planned'] as const) {
      items.push(...readArray(k));
    }
    return ok(c, items);
  },
);

// Raw Supabase passthroughs. Python (lines 5367, 5383, 6819, 6835) forwards
// directly to /rpc/get_outage_areas_fast and filters by outage_type. We do
// the same — no LiveStore involvement — for clients that need the un-
// normalised area record. Failures degrade to [] so a Supabase blip
// doesn't 500 the route.
endeavourRouter.get('/api/endeavour/current/raw', SCOPE_OUTAGES, async (c) => {
  try {
    const areas = await fetchAreas();
    return c.json(
      areas.filter((a) => (a.outage_type ?? '').toUpperCase() !== 'PLANNED'),
    );
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'endeavour current/raw failed');
    return c.json([]);
  }
});

endeavourRouter.get(
  '/api/endeavour/current/all',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEndeavourCurrentAreas',
    tags: ['Outages'],
    summary: 'Endeavour Energy outage areas other than planned work, unprocessed',
    description:
      "Endeavour Energy's outage areas that are not planned work, exactly as Endeavour Energy publishes them, for clients that process them themselves — no address, cause or timing joined on (/endeavour/current has those). Read from Endeavour Energy on each request.",
    responses: {
      ...okJson(envelope(EndeavourAreas), 'Every outage area not marked PLANNED'),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    try {
      const areas = await readAreas();
      if (!areas) {
        // The old path has always answered an empty list (200) rather than an error.
        if (!isV1(c)) return ok(c, []);
        log.warn('endeavour current/all: not a list');
        return fail(c, 503, 'service_unavailable', NOT_THE_LIST);
      }
      return ok(
        c,
        areas.filter((a) => (a.outage_type ?? '').toUpperCase() !== 'PLANNED') satisfies z.infer<typeof EndeavourAreas>,
      );
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'endeavour current/all failed');
      // The old path has always answered an empty list (200) rather than an error.
      if (!isV1(c)) return ok(c, []);
      return fail(c, 503, 'service_unavailable', UNREADABLE);
    }
  },
);

endeavourRouter.get('/api/endeavour/future/raw', SCOPE_OUTAGES, async (c) => {
  try {
    const areas = await fetchAreas();
    return c.json(
      areas.filter((a) => (a.outage_type ?? '').toUpperCase() === 'PLANNED'),
    );
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'endeavour future/raw failed');
    return c.json([]);
  }
});

endeavourRouter.get(
  '/api/endeavour/future/all',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEndeavourPlannedAreas',
    tags: ['Outages'],
    summary: 'Endeavour Energy planned-work outage areas, unprocessed',
    description:
      "Endeavour Energy's outage areas marked as planned work, exactly as Endeavour Energy publishes them, for clients that process them themselves. Despite the path, this includes planned work already under way, not only work to come. Read from Endeavour Energy on each request.",
    responses: {
      ...okJson(envelope(EndeavourAreas), 'Every outage area marked PLANNED'),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    try {
      const areas = await readAreas();
      if (!areas) {
        // The old path has always answered an empty list (200) rather than an error.
        if (!isV1(c)) return ok(c, []);
        log.warn('endeavour future/all: not a list');
        return fail(c, 503, 'service_unavailable', NOT_THE_LIST);
      }
      return ok(
        c,
        areas.filter((a) => (a.outage_type ?? '').toUpperCase() === 'PLANNED') satisfies z.infer<typeof EndeavourAreas>,
      );
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'endeavour future/all failed');
      // The old path has always answered an empty list (200) rather than an error.
      if (!isV1(c)) return ok(c, []);
      return fail(c, 503, 'service_unavailable', UNREADABLE);
    }
  },
);

// /api/endeavour/postcodes — distinct postcodes pulled from /outage-points.
// Python caches for 1h via @cached(ttl=3600). We add a tiny module-local
// cache here matching that. Only a good answer is cached: a failure is
// tried again on the next request.
let postcodesCache: { data: { postcodes: string[]; count: number }; expiresAt: number } | null = null;
const POSTCODE_TTL_MS = 60 * 60_000;

interface OutagePointRow { postcode?: string | null }

endeavourRouter.get(
  '/api/endeavour/postcodes',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEndeavourPostcodes',
    tags: ['Outages'],
    summary: 'Postcodes in Endeavour Energy outages',
    description:
      "The postcodes in Endeavour Energy's outage data — taken from its outage points, at most 5,000 of them read — each once, sorted. Read from Endeavour Energy when asked, and a good answer is kept for an hour.",
    responses: {
      ...okJson(envelope(EndeavourPostcodes), 'The postcodes'),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    const now = Date.now();
    if (postcodesCache && now < postcodesCache.expiresAt) {
      return ok(c, postcodesCache.data);
    }
    try {
      const data = await callSupabase('/outage-points', {
        method: 'GET',
        query: { select: 'postcode', limit: '5000' },
      });
      if (!Array.isArray(data)) {
        // The old path has always answered an empty list (200) rather than an error.
        if (!isV1(c)) return ok(c, { postcodes: [], count: 0 });
        log.warn('endeavour postcodes: not a list');
        return fail(c, 503, 'service_unavailable', NOT_THE_LIST);
      }
      const seen = new Set<string>();
      for (const row of data as OutagePointRow[]) {
        const pc = row.postcode;
        if (typeof pc === 'string' && pc.length > 0) seen.add(pc);
      }
      const sorted = Array.from(seen).sort();
      const payload = { postcodes: sorted, count: sorted.length } satisfies z.infer<typeof EndeavourPostcodes>;
      postcodesCache = { data: payload, expiresAt: now + POSTCODE_TTL_MS };
      return ok(c, payload);
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'endeavour postcodes failed');
      // The old path has always answered an empty list (200) rather than an error.
      if (!isV1(c)) return ok(c, { postcodes: [], count: 0 });
      return fail(c, 503, 'service_unavailable', UNREADABLE);
    }
  },
);

export function _resetEndeavourPostcodesCacheForTests(): void {
  postcodesCache = null;
}
