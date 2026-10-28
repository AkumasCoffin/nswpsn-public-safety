/**
 * Ausgrid outage routes.
 *
 * Reads from LiveStore — the poller (src/sources/ausgrid.ts) keeps two
 * keys fresh:
 *   - ausgrid         → {Markers, Polygons} payload
 *   - ausgrid_stats   → aggregate counter dict
 *
 * Response shape mirrors Python (external_api_proxy.py:5127-5168):
 *   /api/ausgrid/outages → `{Markers, Polygons}`. On error, Python
 *                          returns `{error, Markers: [], Polygons: []}`
 *                          with HTTP 200; we mirror that by returning
 *                          the empty payload on missing LiveStore data.
 *   /api/ausgrid/stats   → upstream counter object verbatim.
 *
 * Both are also served at /api/v1/ (server.ts mirrors them), wrapped in
 * `data` there (api/v1/respond.ts). The poller is off by default
 * (AUSGRID_DISABLED) while Ausgrid's endpoints are not answering.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { liveStore } from '../store/live.js';
import type { AusgridOutage, AusgridOutagesPayload } from '../sources/ausgrid.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { ok } from './v1/respond.js';
import { assertShape, envelope, GATED_ERRORS, okJson, type SameShape } from './v1/schemas.js';

export const ausgridRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_OUTAGES = scoped({ source: 'ausgrid', type: 'outages', window: 'live' });

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const NOT_READ = "Empty: the outage's details are not read from Ausgrid";
const AS_GIVEN = 'As Ausgrid gives it; empty when not given';
const same = (field: string) => `The same as ${field}`;

// Every field comes twice — PascalCase for older consumers, camelCase for
// newer ones (sources/ausgrid.ts normaliseAusgridOutage).
const AusgridOutageRecord = z.object({
  OutageId: z.union([z.string(), z.number()]).nullable().describe("Ausgrid's id for the outage"),
  JobId: z.string().describe(NOT_READ),
  Suburb: z.string().describe('The area Ausgrid names; empty when not given'),
  StreetName: z.string().describe(NOT_READ),
  Streets: z.string().describe(NOT_READ),
  Postcode: z.string().describe(NOT_READ),
  CustomersAffected: z.number().int().describe('Customers without power; 0 when not given'),
  OutageType: z.enum(['Planned', 'Unplanned']).describe('Planned when Ausgrid shows it as planned, otherwise Unplanned'),
  Cause: z.string().describe(AS_GIVEN),
  Reason: z.string().describe(NOT_READ),
  Detail: z.string().describe(NOT_READ),
  StatusText: z.string().describe(NOT_READ),
  StartTime: z.string().describe(`When it started. ${AS_GIVEN}`),
  EstRestoration: z.string().describe(`When power is expected back. ${AS_GIVEN}`),
  EndDateTime: z.string().describe('The same as EstRestoration'),
  Latitude: z.number().nullable().describe("The outage's marker; null when not given"),
  Longitude: z.number().nullable().describe("The outage's marker; null when not given"),
  Status: z.union([z.string(), z.number()]).nullable().describe('As Ausgrid gives it'),
  Classification: z.string().nullable().describe('As Ausgrid gives it'),
  Polygons: z.array(z.unknown()).describe("The outage's areas, as Ausgrid gives them"),
  outageId: z.union([z.string(), z.number()]).nullable().describe(same('OutageId')),
  suburb: z.string().describe(same('Suburb')),
  streetName: z.string().describe(same('StreetName')),
  streets: z.string().describe(same('Streets')),
  postcode: z.string().describe(same('Postcode')),
  customersAffected: z.number().int().describe(same('CustomersAffected')),
  outageType: z.enum(['Planned', 'Unplanned']).describe(same('OutageType')),
  cause: z.string().describe(same('Cause')),
  startTime: z.string().describe(same('StartTime')),
  estRestoration: z.string().describe(same('EstRestoration')),
  endDateTime: z.string().describe(same('EndDateTime')),
  latitude: z.number().nullable().describe(same('Latitude')),
  longitude: z.number().nullable().describe(same('Longitude')),
});
assertShape<SameShape<z.infer<typeof AusgridOutageRecord>, AusgridOutage>>();

const AusgridOutages = z
  .object({
    Markers: z.array(AusgridOutageRecord).describe('One record per outage, each field under two names: PascalCase and camelCase'),
    Polygons: z.array(z.unknown()).describe("Every outage's areas together, as Ausgrid gives them"),
  })
  .meta({ id: 'AusgridOutages' });
assertShape<SameShape<z.infer<typeof AusgridOutages>, AusgridOutagesPayload>>();

const AusgridStats = z.unknown().meta({
  id: 'AusgridStats',
  description: "Ausgrid's outage counters, exactly as Ausgrid publishes them; null when there are none to give",
});

const POLLING_OFF =
  "Polling is off for now because Ausgrid's outage map service has stopped answering, so this is empty until it returns.";

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

ausgridRouter.get(
  '/api/ausgrid/outages',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getAusgridOutages',
    tags: ['Outages'],
    summary: 'Ausgrid outages',
    description: `Electricity outages on Ausgrid's network, from Ausgrid's public outage map, polled every two minutes when polling is on. ${POLLING_OFF}`,
    responses: {
      ...okJson(envelope(AusgridOutages), 'The outages and their areas'),
      ...GATED_ERRORS,
    },
  }),
  (c) => {
    const data = liveStore.getData<AusgridOutagesPayload>('ausgrid');
    if (data && (data.Markers || data.Polygons)) {
      return ok(c, data);
    }
    // Empty fallback — Python uses the same empty shape when the live
    // fetch errors so the frontend's "render markers" code path is happy.
    return ok(c, { Markers: [], Polygons: [] } satisfies z.infer<typeof AusgridOutages>);
  },
);

ausgridRouter.get(
  '/api/ausgrid/stats',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getAusgridStats',
    tags: ['Outages'],
    summary: 'Ausgrid outage counts',
    description: `Ausgrid's own outage counters, as its public outage map publishes them, polled every two minutes when polling is on. ${POLLING_OFF}`,
    responses: {
      ...okJson(envelope(AusgridStats), "Ausgrid's counters, or null"),
      ...GATED_ERRORS,
    },
  }),
  (c) => {
    const data = liveStore.getData<unknown>('ausgrid_stats');
    // Python's ausgrid_stats handler returns the upstream payload as-is
    // (whatever shape it happens to have); on missing cache it returns
    // {error: ...}. We just return null when missing — frontend is
    // already null-tolerant for stats since the value is cosmetic.
    return ok(c, data ?? null);
  },
);
