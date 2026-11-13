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
 * The snapshot holds every outage Ausgrid lists that is not over: the power
 * off now and the planned work of the next 30 days. Both are empty when the
 * poller is switched off (AUSGRID_DISABLED=true).
 *
 * This site's routes, for the map, the live page and the Discord bot:
 * neither is on /api/v1/ nor declares a scope, so no scoped API key reaches
 * them. An API key reads every distributor's outages together, at
 * /api/v1/outages (api/outages.ts).
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { liveStore } from '../store/live.js';
import { ausgridScheduled, type AusgridOutage, type AusgridOutagesPayload } from '../sources/ausgrid.js';
import { ok } from './v1/respond.js';
import { assertShape, envelope, GATED_ERRORS, okJson, type SameShape } from './v1/schemas.js';

export const ausgridRouter = new Hono();

// ---------------------------------------------------------------------------
// The answers' shapes
// ---------------------------------------------------------------------------

const NOT_GIVEN = 'Always empty: Ausgrid does not publish it';
const AS_GIVEN = 'As Ausgrid gives it; empty when not given';
const same = (field: string) => `The same as ${field}`;

// Every field comes twice — PascalCase for older consumers, camelCase for
// newer ones (sources/ausgrid.ts normaliseAusgridOutage).
const AusgridOutageRecord = z.object({
  OutageId: z.union([z.string(), z.number()]).nullable().describe("Ausgrid's id for the outage"),
  JobId: z.string().describe(NOT_GIVEN),
  Suburb: z.string().describe('The localities Ausgrid names, joined with ", "; empty when not given'),
  StreetName: z.string().describe('The first of the streets affected; empty when none are given'),
  Streets: z.string().describe('The streets affected, joined with ", "; empty when none are given'),
  Postcode: z.string().describe(NOT_GIVEN),
  CustomersAffected: z.number().int().describe('Customers affected; 0 when not given'),
  OutageType: z.enum(['Planned', 'Unplanned']).describe('Planned when Ausgrid shows it as planned, otherwise Unplanned'),
  Cause: z.string().describe(AS_GIVEN),
  Reason: z.string().describe(`Ausgrid's note on a change to the outage. ${AS_GIVEN}, which is most of the time`),
  Detail: z.string().describe(NOT_GIVEN),
  StatusText: z.string().describe('Status in words: "In progress", "Planned", "Proceeding as scheduled", "Restoration has commenced"; empty when not given'),
  StartTime: z.string().describe(`When it started, or for planned work to come when it starts, ISO 8601 UTC. ${AS_GIVEN}`),
  EstRestoration: z.string().describe(`When power is expected back, ISO 8601 UTC. ${AS_GIVEN}`),
  EndDateTime: z.string().describe('The same as EstRestoration'),
  WhenUpdated: z.string().describe(`When Ausgrid last updated the outage, ISO 8601 UTC. ${AS_GIVEN}`),
  Latitude: z.number().nullable().describe("The outage's marker; null when not given"),
  Longitude: z.number().nullable().describe("The outage's marker; null when not given"),
  Status: z
    .union([z.string(), z.number()])
    .nullable()
    .describe("Ausgrid's status as it writes it: InProgress for an unplanned outage; Planned, ProceedingAsScheduled, InProgress or RestorationHasCommenced for planned work. Null when not given"),
  Classification: z.string().nullable().describe('Always null: Ausgrid does not publish it'),
  Polygons: z.array(z.unknown()).describe("Always empty: Ausgrid gives a box around the outage, not its outline"),
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
    Markers: z
      .array(AusgridOutageRecord)
      .describe('One record per outage that is not over, most fields under two names: PascalCase and camelCase. Planned work with a StartTime still to come has not cut anyone off yet'),
    Polygons: z.array(z.unknown()).describe('Always empty: Ausgrid gives no outlines'),
  })
  .meta({ id: 'AusgridOutages' });
assertShape<SameShape<z.infer<typeof AusgridOutages>, AusgridOutagesPayload>>();

const AusgridStats = z.unknown().meta({
  id: 'AusgridStats',
  description: "Ausgrid's outage counters, exactly as Ausgrid publishes them; null when there are none to give",
});

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

ausgridRouter.get(
  '/api/ausgrid/outages',
  describeRoute({
    operationId: 'getAusgridOutages',
    tags: ['Power outages'],
    summary: 'Ausgrid outages',
    description:
      "Electricity outages under way on Ausgrid's network, from Ausgrid's public outage map: the unplanned outages and the planned work that has started, " +
      'read every two minutes. Outages that are over (restored, completed, cancelled) are left out, and so is planned work yet to start, which /api/v1/outages lists with status scheduled.',
    responses: {
      ...okJson(envelope(AusgridOutages), 'The outages and their areas'),
      ...GATED_ERRORS,
    },
  }),
  (c) => {
    const data = liveStore.getData<AusgridOutagesPayload>('ausgrid');
    if (data && (data.Markers || data.Polygons)) {
      // Outages under way only. The Discord bot announces every record it
      // has not seen here, and this route answered current outages alone
      // before Ausgrid's change; handing it the 1,300 planned jobs of the
      // month ahead would post each of them to every subscribed channel.
      // The scheduled ones are in /api/v1/outages (status=scheduled).
      const now = Date.now();
      const Markers = Array.isArray(data.Markers) ? data.Markers.filter((m) => !ausgridScheduled(m, now)) : [];
      return ok(c, { ...data, Markers });
    }
    // Empty fallback — Python uses the same empty shape when the live
    // fetch errors so the frontend's "render markers" code path is happy.
    return ok(c, { Markers: [], Polygons: [] } satisfies z.infer<typeof AusgridOutages>);
  },
);

ausgridRouter.get(
  '/api/ausgrid/stats',
  describeRoute({
    operationId: 'getAusgridStats',
    tags: ['Power outages'],
    summary: 'Ausgrid outage counts',
    description: "Ausgrid's own outage counters, as its public outage map publishes them, read every two minutes.",
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
