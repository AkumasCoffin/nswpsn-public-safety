/**
 * Essential Energy outage routes.
 *
 * Reads from LiveStore (`essential_current`, `essential_future`) which
 * the poller in src/sources/essential.ts populates from KML feeds.
 *
 * Response shapes mirror Python (external_api_proxy.py):
 *   /api/essential/outages       → line 5171-5234.
 *     {outages, count, planned, unplanned, future, totalCustomersAffected}.
 *     Query params `?feed=current|future`, `?type=planned|unplanned`,
 *     `?lite=1` (strips polygons).
 *   /api/essential/planned       → line 5263-5277.
 *     {outages, count, totalCustomersAffected} for planned items only.
 *   /api/essential/future        → line 5280-5290.
 *     {outages, count, totalCustomersAffected} for the future feed.
 *
 * All of these but /api/essential/outages/future are also served at
 * /api/v1/ (server.ts mirrors them), wrapped in `data` there
 * (api/v1/respond.ts). /outages/future is not: it answers exactly what
 * /essential/future does. /outages/raw reads both feeds on the request;
 * when neither can be read the old path answers `[]` (200), as it always
 * has, and /api/v1/ answers 503 service_unavailable.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { liveStore } from '../store/live.js';
import type { EssentialOutage } from '../sources/essential.js';
import { fetchFeed } from '../sources/essential.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, isV1, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  flag,
  GATED_ERRORS,
  LonLat,
  okJson,
  queryValidator,
  UNAVAILABLE_ERROR,
  type SameShape,
} from './v1/schemas.js';

export const essentialRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_OUTAGES = scoped({ source: 'essential', type: 'outages', window: 'live' });

function readArray(key: string): EssentialOutage[] {
  const data = liveStore.getData<EssentialOutage[]>(key);
  return Array.isArray(data) ? data : [];
}

function totalCustomers(items: EssentialOutage[]): number {
  let n = 0;
  for (const o of items) n += o.customersAffected || 0;
  return n;
}

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const AS_WRITTEN = 'As Essential Energy writes it; empty when not given';

const essentialOutageShape = {
  incidentId: z.string().describe("Essential Energy's id for the outage"),
  title: z.string().describe('The locality, else the reason, else "Power Outage"'),
  suburb: z.string().describe('The locality the outage is named after, as published; may be empty'),
  outageType: z
    .enum(['planned', 'unplanned'])
    .describe('As Essential Energy marks it; unplanned when it marks neither'),
  feedType: z
    .enum(['current', 'future'])
    .describe("Which of Essential Energy's feeds it is in: outages now, or outages to come"),
  latitude: z.number().nullable().describe("The outage's marker; null when the feed gives none"),
  longitude: z.number().nullable().describe("The outage's marker; null when the feed gives none"),
  cause: z.string().describe('The reason Essential Energy gives; "Unknown" when it gives none'),
  customersAffected: z.number().int().describe('Customers without power; 0 when not given'),
  timeOff: z
    .string()
    .describe('When the power went or goes off, as Essential Energy writes it (normally DD/MM/YYYY HH:mm:ss); empty when not given'),
  estTimeOn: z.string().describe(`When the power is expected back. ${AS_WRITTEN}`),
  lastUpdated: z.string().describe(AS_WRITTEN),
  status: z.enum(['active', 'scheduled']).describe('active in the current feed, scheduled in the future feed'),
  source: z
    .enum(['essential_current', 'essential_planned', 'essential_future'])
    .describe('essential_current: unplanned, in the current feed; essential_planned: planned, in the current feed; essential_future: in the future feed'),
  sourceTimestamp: z
    .string()
    .nullable()
    .describe('timeOff rewritten as YYYY-MM-DDTHH:mm:ss, with no time zone; timeOff unchanged when it is not in the usual form; null when there is none'),
  polygon: z
    .array(LonLat)
    .nullable()
    .describe("The outage area's outline as [longitude, latitude] points; null when the feed gives none"),
  provider: z.literal('Essential Energy'),
};

const EssentialOutageRecord = z.object(essentialOutageShape).meta({ id: 'EssentialOutage' });
assertShape<SameShape<z.infer<typeof EssentialOutageRecord>, EssentialOutage>>();

const EssentialOutageList = z
  .object({
    outages: z.array(EssentialOutageRecord),
    count: z.number().int().describe('Number of outages'),
    totalCustomersAffected: z.number().int().describe('customersAffected summed over the outages'),
  })
  .meta({ id: 'EssentialOutageList' });

const EssentialOutages = z
  .object({
    outages: z
      .array(
        z.object({
          ...essentialOutageShape,
          polygon: essentialOutageShape.polygon.optional().describe(
            "The outage area's outline as [longitude, latitude] points; null when the feed gives none. Left out when lite=1",
          ),
        }),
      )
      .describe('The current feed, then the future feed, each in its own order'),
    count: z.number().int().describe('Number of outages'),
    planned: z.number().int().describe('How many of them are planned'),
    unplanned: z.number().int().describe('How many of them are unplanned'),
    future: z.number().int().describe('How many of them are in the future feed'),
    totalCustomersAffected: z.number().int().describe('customersAffected summed over the outages'),
  })
  .meta({ id: 'EssentialOutages' });

const EssentialOutagesRaw = z.array(EssentialOutageRecord).meta({ id: 'EssentialOutagesRaw' });

// The handler reads the query string itself, exactly as it always has; this
// only describes it. Nothing here refuses — an unknown value is ignored.
const EssentialOutagesQuery = z.object({
  feed: z
    .enum(['current', 'future'])
    .optional()
    .catch(undefined)
    .describe('Only the outages in this feed: current (outages now) or future (outages to come). Letter case does not matter; any other value is ignored.'),
  type: z
    .enum(['planned', 'unplanned'])
    .optional()
    .catch(undefined)
    .describe('Only outages of this kind. Letter case does not matter; any other value is ignored.'),
  lite: flag("1 leaves out each outage's polygon, the heaviest field"),
});

const FEEDS = "Essential Energy's public outage feeds";
const EVERY = 'Refreshed every three minutes.';

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

essentialRouter.get(
  '/api/essential/outages',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEssentialOutages',
    tags: ['Outages'],
    summary: 'Essential Energy outages',
    description: `Electricity outages on Essential Energy's network — unplanned and planned, now and to come — from ${FEEDS} (current and future), with counts. feed and type narrow the list (the counts are of what is left), and lite=1 leaves out the polygons. ${EVERY}`,
    responses: {
      ...okJson(envelope(EssentialOutages), 'The outages, with counts'),
      ...GATED_ERRORS,
    },
  }),
  queryValidator(EssentialOutagesQuery),
  (c) => {
    const all: EssentialOutage[] = [
      ...readArray('essential_current'),
      ...readArray('essential_future'),
    ];

    const url = new URL(c.req.url);
    const feed = (url.searchParams.get('feed') ?? '').toLowerCase();
    const type = (url.searchParams.get('type') ?? '').toLowerCase();
    const lite = url.searchParams.get('lite') === '1';

    let filtered = all;
    if (feed === 'current' || feed === 'future') {
      filtered = filtered.filter((o) => o.feedType === feed);
    }
    if (type === 'planned' || type === 'unplanned') {
      filtered = filtered.filter((o) => o.outageType === type);
    }

    const planned = filtered.filter((o) => o.outageType === 'planned').length;
    const unplanned = filtered.filter((o) => o.outageType === 'unplanned').length;
    const future = filtered.filter((o) => o.feedType === 'future').length;
    const total = totalCustomers(filtered);

    // Lite mode strips the polygon ring data — those are the heaviest
    // field on the wire and only the embed-card consumers need them.
    const outages = lite
      ? filtered.map((o) => {
          const { polygon: _drop, ...rest } = o;
          // Discard `_drop` deliberately; underscored to silence
          // unused-var lint without changing the destructure.
          void _drop;
          return rest;
        })
      : filtered;

    return ok(c, {
      outages,
      count: filtered.length,
      planned,
      unplanned,
      future,
      totalCustomersAffected: total,
    } satisfies z.infer<typeof EssentialOutages>);
  },
);

// Bot-canonical alias: planned items from current + future feeds combined.
essentialRouter.get(
  '/api/essential/planned',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEssentialPlanned',
    tags: ['Outages'],
    summary: 'Essential Energy planned outages',
    description: `Planned outages on Essential Energy's network, under way and to come: the outages marked planned in ${FEEDS}, the current feed's first. ${EVERY}`,
    responses: {
      ...okJson(envelope(EssentialOutageList), 'Every planned outage'),
      ...GATED_ERRORS,
    },
  }),
  (c) => {
    const items = [
      ...readArray('essential_current'),
      ...readArray('essential_future'),
    ].filter((o) => o.outageType === 'planned');
    return ok(c, {
      outages: items,
      count: items.length,
      totalCustomersAffected: totalCustomers(items),
    } satisfies z.infer<typeof EssentialOutageList>);
  },
);

// Bot-canonical alias: just the future feed contents.
essentialRouter.get(
  '/api/essential/future',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEssentialFuture',
    tags: ['Outages'],
    summary: 'Essential Energy outages to come',
    description: `Outages on Essential Energy's network that are still to come: everything in Essential Energy's future-outages feed. ${EVERY}`,
    responses: {
      ...okJson(envelope(EssentialOutageList), 'Every outage in the future feed'),
      ...GATED_ERRORS,
    },
  }),
  (c) => {
    const items = readArray('essential_future');
    return ok(c, {
      outages: items,
      count: items.length,
      totalCustomersAffected: totalCustomers(items),
    } satisfies z.infer<typeof EssentialOutageList>);
  },
);

// /api/essential/outages/current — current-feed outages only. Python
// line 5237-5247 reads cache_get('essential_energy'); we read the same
// LiveStore key our poller writes to.
essentialRouter.get(
  '/api/essential/outages/current',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEssentialCurrent',
    tags: ['Outages'],
    summary: 'Essential Energy outages now',
    description: `Outages on Essential Energy's network now, unplanned and planned: everything in Essential Energy's current-outages feed. ${EVERY}`,
    responses: {
      ...okJson(envelope(EssentialOutageList), 'Every outage in the current feed'),
      ...GATED_ERRORS,
    },
  }),
  (c) => {
    const items = readArray('essential_current');
    return ok(c, {
      outages: items,
      count: items.length,
      totalCustomersAffected: totalCustomers(items),
    } satisfies z.infer<typeof EssentialOutageList>);
  },
);

// /api/essential/outages/future — future-feed outages only. Python
// line 5250-5260.
essentialRouter.get('/api/essential/outages/future', SCOPE_OUTAGES, (c) => {
  const items = readArray('essential_future');
  return c.json({
    outages: items,
    count: items.length,
    totalCustomersAffected: totalCustomers(items),
  });
});

// /api/essential/outages/raw — fetches both KMLs upstream and returns
// the merged parsed list. Python line 5293-5305 hits both feeds without
// the LiveStore intermediary so debug tooling can see exactly what
// upstream is shipping. We keep the same behaviour for parity.
essentialRouter.get(
  '/api/essential/outages/raw',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getEssentialOutagesRaw',
    tags: ['Outages'],
    summary: 'Essential Energy outages, read now',
    description: `Every outage in both of ${FEEDS}, read from Essential Energy on this request rather than taken from the last poll: the current feed's outages, then the future feed's, in the same record shape as the other Essential Energy endpoints. When only one feed can be read, the answer is what that one gave, with nothing to say the other failed; when neither can be read, the answer is 503.`,
    responses: {
      ...okJson(envelope(EssentialOutagesRaw), 'The outages in both feeds'),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    const out: EssentialOutage[] = [];
    let failed = 0;
    for (const feed of ['current', 'future'] as const) {
      try {
        const items = await fetchFeed(feed);
        out.push(...items);
      } catch (err) {
        failed += 1;
        log.warn(
          { err: (err as Error).message, feed },
          'essential raw fetch failed',
        );
      }
    }
    // The old path has always answered what it read — an empty list (200)
    // when neither feed could be read.
    if (failed === 2 && isV1(c)) {
      return fail(c, 503, 'service_unavailable', "Essential Energy's outage feeds could not be read just now.");
    }
    return ok(c, out satisfies z.infer<typeof EssentialOutagesRaw>);
  },
);
