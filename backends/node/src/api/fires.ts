/**
 * GET /api/v1/fires — every fire service's incidents in one feed.
 *
 * NSW RFS, the QLD Fire Department, VIC Emergency, WA DFES, SA CFS and MFS
 * and NT Fire and Rescue, read from the same LiveStore snapshots the
 * per-agency routes serve (sources/*.ts), in one shape. The per-agency routes
 * (/api/rfs/incidents, /api/qld-fire/incidents, …) stay /api/ routes for this
 * site's map and the Discord bot, which draw each agency its own way; an API
 * key reads this one (feed `fires`, apiKeyScopes.ts).
 *
 * Every feature carries the same properties, snake_case like the rest of
 * /api/v1/: an id, which service and state it came from, the map layer, the
 * title, status, location, category and warning level as the agency words
 * them, when it was last updated (UTC), whether it is still open, any
 * fire-ground polygons as [longitude, latitude] rings, and `details` — the
 * agency's own extra fields, under their own names, so nothing the
 * per-agency feed carries is lost.
 *
 * The services publish more than fires: VIC Emergency carries floods and
 * other hazards, WA and SA floods, the QLD and NT services rescues and
 * hazmat. Those come too, each tagged by `layer` (fire, flood or hazard), and
 * `layer=fire` keeps only fires. Closed incidents are left out unless all=1,
 * as on the NT and SA routes. `sources` says, per service, how many records
 * it gave and when it was last read, so a missing state shows as a missing
 * state rather than as no fires.
 *
 * Registered at /api/fires and mirrored to /api/v1/ like every other feed;
 * the old path is nobody's and goes when old paths are retired.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { liveStore } from '../store/live.js';
import { rfsSnapshot } from '../sources/rfs.js';
import { ntFireSnapshot } from '../sources/ntFire.js';
import { qldIncidentsSnapshot } from '../sources/qldFire.js';
import { vicSnapshot } from '../sources/vicEmergency.js';
import { waIncidentsSnapshot } from '../sources/waEmergency.js';
import { saCfsSnapshot, saMfsSnapshot } from '../sources/saFire.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { geo } from './v1/respond.js';
import {
  feature,
  featureCollection,
  flag,
  GATED_ERRORS,
  LonLat,
  okJson,
  queryValidator,
  Rings,
  VALIDATION_ERROR,
} from './v1/schemas.js';

export const firesRouter = new Hono();

// What a scoped API key needs to read this route (services/auth/apiKeyScopes.ts).
const SCOPE_FIRES = scoped({ feed: 'fires', window: 'live' });

type Props = Record<string, unknown>;
interface AnyFeature {
  type: 'Feature';
  geometry: { type: string; coordinates?: unknown } | null;
  properties: Props;
}
interface AnyCollection {
  features: readonly AnyFeature[];
}

const STATES = ['NSW', 'QLD', 'VIC', 'NT', 'WA', 'SA'] as const;
type State = (typeof STATES)[number];
const LAYERS = ['fire', 'flood', 'hazard'] as const;

/** One fire service: its source key, state, LiveStore key and snapshot. */
interface Service {
  source: string;
  name: string;
  state: State;
  /** The agency when the feed does not say (RFS's records carry none). */
  agency: string;
  store: string;
  snapshot: () => AnyCollection;
}

/** In the order the answer lists them. */
const SERVICES: readonly Service[] = [
  { source: 'rfs', name: 'NSW Rural Fire Service', state: 'NSW', agency: 'RFS', store: 'rfs_incidents', snapshot: rfsSnapshot },
  { source: 'qld_fire', name: 'QLD Fire Department', state: 'QLD', agency: 'QFD', store: 'qld_fire', snapshot: qldIncidentsSnapshot },
  { source: 'vic_emergency', name: 'VIC Emergency', state: 'VIC', agency: 'EMV', store: 'vic_emergency', snapshot: vicSnapshot },
  { source: 'nt_fire', name: 'NT Fire and Rescue', state: 'NT', agency: 'NTFRS', store: 'nt_fire', snapshot: ntFireSnapshot },
  { source: 'wa_incident', name: 'WA DFES', state: 'WA', agency: 'DFES', store: 'wa_incident', snapshot: waIncidentsSnapshot },
  { source: 'sa_cfs', name: 'SA Country Fire Service', state: 'SA', agency: 'SACFS', store: 'sa_cfs', snapshot: saCfsSnapshot },
  { source: 'sa_mfs', name: 'SA Metropolitan Fire Service', state: 'SA', agency: 'SAMFS', store: 'sa_mfs', snapshot: saMfsSnapshot },
];

const SOURCES = SERVICES.map((s) => s.source) as [string, ...string[]];

/** The properties every feed shares, which become the common fields; the rest go in `details`. */
const COMMON = new Set([
  'title', 'guid', 'status', 'location', 'alertLevel', 'fireType', 'responsibleAgency',
  'updated', 'updatedISO', 'layer', 'agency', 'source', 'is_active', 'polygons',
]);

const str = (v: unknown): string => (typeof v === 'string' ? v : '');

/** ISO 8601 with a zone (Z or an offset): the only form whose instant is certain. */
const ISO_WITH_ZONE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})$/;

/**
 * An agency time as UTC ISO 8601, or null when it is not one with a zone. RFS
 * sends its own text where it has no readable time ("1 Nov 2026 17:07"), which
 * Date.parse would read in the server's own zone — right in Sydney, wrong
 * anywhere else — so anything without a zone is not guessed at.
 */
function utc(v: unknown): string | null {
  if (typeof v !== 'string' || !ISO_WITH_ZONE.test(v.trim())) return null;
  const ms = Date.parse(v.trim());
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
}

/** RFS's GeoRSS "lat lon lat lon …" strings, as [longitude, latitude] rings. */
function geoRssRings(list: readonly unknown[]): number[][][] {
  const rings: number[][][] = [];
  for (const s of list) {
    if (typeof s !== 'string') continue;
    const n = s.trim().split(/\s+/).map(Number);
    const ring: number[][] = [];
    for (let i = 0; i + 1 < n.length; i += 2) {
      const lat = n[i]!;
      const lon = n[i + 1]!;
      if (Number.isFinite(lat) && Number.isFinite(lon)) ring.push([lon, lat]);
    }
    if (ring.length >= 3) rings.push(ring);
  }
  return rings;
}

function rings(v: unknown): number[][][] {
  if (!Array.isArray(v) || !v.length) return [];
  if (v.every((r) => typeof r === 'string')) return geoRssRings(v);
  return v.filter((r): r is number[][] => Array.isArray(r));
}

/**
 * The record's map layer. RFS records carry none and are all fires; any other
 * value this list does not know reads as `hazard`, so a new kind of record is
 * never passed off as a fire to `layer=fire`.
 */
function layerOf(v: unknown, svc: Service): (typeof LAYERS)[number] {
  if ((LAYERS as readonly string[]).includes(v as string)) return v as (typeof LAYERS)[number];
  return v == null && svc.source === 'rfs' ? 'fire' : 'hazard';
}

/** One agency feature in the shared shape. */
export function toFire(f: AnyFeature, svc: Service): FireFeature {
  const p = f.properties ?? {};
  const details: Props = {};
  for (const [k, v] of Object.entries(p)) if (!COMMON.has(k)) details[k] = v;
  const updatedAt = utc(p['updatedISO']);
  // No time with a zone to give: keep the agency's own wording instead.
  if (!updatedAt && (str(p['updated']) || str(p['updatedISO']))) details['updated'] = str(p['updated']) || str(p['updatedISO']);
  return {
    type: 'Feature',
    geometry: f.geometry as FireFeature['geometry'],
    properties: {
      id: str(p['guid']),
      source: svc.source,
      agency: str(p['agency']) || svc.agency,
      state: svc.state,
      layer: layerOf(p['layer'], svc),
      title: str(p['title']),
      status: str(p['status']),
      location: str(p['location']),
      category: str(p['fireType']),
      alert_level: str(p['alertLevel']),
      responsible_agency: str(p['responsibleAgency']),
      updated_at: updatedAt,
      is_active: p['is_active'] !== false,
      polygons: rings(p['polygons']),
      details,
    },
  };
}

// What /api/v1/ documents (api/v1/openapi.ts).
const FireProperties = z.object({
  id: z.string().describe("Stable across polls, built from the agency's own id"),
  source: z.enum(SOURCES).describe('Which feed the record came from; also the name stored history uses'),
  agency: z.string().describe('The publishing service, e.g. RFS, QFD, CFA, SES, DFES, SACFS, SAMFS, NTFRS'),
  state: z.enum(STATES),
  layer: z.enum(LAYERS).describe('fire, or flood or hazard for the other incidents these services publish (hazard too for any kind of record not yet known)'),
  title: z.string(),
  status: z.string().describe('As the agency words it'),
  location: z.string(),
  category: z.string().describe('Kind of incident as the agency classes it, e.g. "Grass Fire", "Building Fire"'),
  alert_level: z.string().describe('Warning level as published (Advice, Watch and Act, Emergency Warning); empty or "Not Applicable" when none'),
  responsible_agency: z.string(),
  updated_at: z.string().nullable().describe("Last update, ISO 8601 UTC; null when the agency gives no time with a zone, and then details.updated has the agency's own wording"),
  is_active: z.boolean().describe('False once closed (only with all=1)'),
  polygons: Rings.describe('Fire grounds or warning areas, as outer rings of [longitude, latitude]; empty when none'),
  details: z
    .record(z.string(), z.unknown())
    .describe("The agency's own extra fields, under the per-agency feed's names (RFS: size, councilArea, link; QFD: vehiclesOnScene; VIC: capEvent, text; …)"),
});
export type FireFeature = {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] } | { type: 'Polygon'; coordinates: number[][][] };
  properties: z.infer<typeof FireProperties>;
};

const ServiceStatus = z.object({
  source: z.enum(SOURCES),
  name: z.string(),
  state: z.enum(STATES),
  count: z.number().int().describe('Records from this service in this answer, after the filters'),
  updated_at: z.string().nullable().describe('When this server last read the service, ISO 8601 UTC; null if it has not yet'),
});

const Fires = featureCollection(
  feature(
    z.union([
      z.object({ type: z.literal('Point'), coordinates: LonLat }),
      z.object({ type: z.literal('Polygon'), coordinates: Rings }),
    ]),
    FireProperties,
  ),
  { sources: z.array(ServiceStatus).describe('One entry per fire service asked for, in this order') },
).meta({ id: 'Fires' });

/** A comma list of allowed values, case-insensitive; empty means all. */
const list = <T extends string>(values: readonly T[], what: string, upper = false) =>
  z
    .string()
    .optional()
    .transform((v, ctx) => {
      const items = (v ?? '').split(',').map((s) => (upper ? s.trim().toUpperCase() : s.trim().toLowerCase())).filter(Boolean);
      const bad = items.filter((s) => !(values as readonly string[]).includes(s));
      if (bad.length) {
        ctx.addIssue({ code: 'custom', message: `unknown ${what}: ${bad.join(', ')} (one of ${values.join(', ')})` });
        return z.NEVER;
      }
      return items as T[];
    })
    .describe(`Comma-separated; any of ${values.join(', ')}. Leave out for all.`);

const Query = z.object({
  source: list(SOURCES, 'source'),
  state: list(STATES, 'state', true),
  layer: list(LAYERS, 'layer'),
  all: flag('1 includes closed incidents as well'),
});

firesRouter.get(
  '/api/fires',
  SCOPE_FIRES,
  describeRoute({
    operationId: 'getFires',
    tags: ['Fires'],
    summary: 'Every fire service, one feed',
    description:
      'Current incidents from NSW RFS, the QLD Fire Department, VIC Emergency, WA DFES, SA CFS and MFS and NT Fire and Rescue, ' +
      "in one shape: the common fields every service has, plus `details` for each agency's own. The services also publish " +
      'floods and other hazards they attend; each record says which in `layer`, and layer=fire keeps only fires. Closed ' +
      'incidents are left out unless all=1. `sources` says per service how many records it gave and when this server last ' +
      'read it. Records from NT Fire and Rescue are theirs: credit NTFRS if you republish them.',
    responses: { ...okJson(Fires, 'A GeoJSON FeatureCollection of incidents'), ...VALIDATION_ERROR, ...GATED_ERRORS },
  }),
  queryValidator(Query),
  (c) => {
    const q = c.req.valid('query');
    const states = new Set<string>(q.state ?? []);
    const layers = new Set<string>(q.layer ?? []);
    const wanted = SERVICES.filter(
      (s) => (!q.source?.length || q.source.includes(s.source)) && (!states.size || states.has(s.state)),
    );
    const features: FireFeature[] = [];
    const sources: Array<z.infer<typeof ServiceStatus>> = [];
    for (const svc of wanted) {
      const ts = liveStore.get(svc.store)?.ts;
      let count = 0;
      for (const f of svc.snapshot().features) {
        const fire = toFire(f, svc);
        if (q.all !== '1' && !fire.properties.is_active) continue;
        if (layers.size && !layers.has(fire.properties.layer)) continue;
        features.push(fire);
        count++;
      }
      sources.push({
        source: svc.source as z.infer<typeof ServiceStatus>['source'],
        name: svc.name,
        state: svc.state,
        count,
        updated_at: ts ? new Date(ts * 1000).toISOString() : null,
      });
    }
    return geo(c, { type: 'FeatureCollection', features, count: features.length, sources } satisfies z.infer<typeof Fires>);
  },
);
