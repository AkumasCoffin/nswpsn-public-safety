/**
 * Live ADS-B aircraft endpoints.
 *
 *   GET /api/adsb/aircraft — merged aircraft positions over NSW from
 *                            adsb.lol / adsb.fi / airplanes.live /
 *                            adsb.one, deduped by ICAO hex. Live-only;
 *                            deliberately NOT in CACHEABLE_PATHS — a
 *                            30 s max-age would defeat the frontend's
 *                            15 s refresh.
 *
 *   GET /api/adsb/photo/:hex?reg=VH-ABC
 *                          — aircraft photo lookup via the Planespotters
 *                            public API, proxied because they reject
 *                            browser-originated requests (their policy
 *                            wants a server UA with a contact URL, which
 *                            our default UA in sources/shared/http.ts
 *                            carries). Tries the hex first, falls back
 *                            to the registration. Cached 24 h — photos
 *                            for a given airframe essentially never
 *                            change and Planespotters rate-limits.
 *
 * These two, /api/adsb/history and /api/adsb/trails are also served at
 * /api/v1/ (server.ts mirrors them), wrapped in `data` there
 * (api/v1/respond.ts). /api/adsb/receivers is not: it is node status, for
 * the site's own map.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute, resolver } from 'hono-openapi';
import {
  adsbSnapshot,
  adsbTrailsSnapshot,
  type AdsbSnapshot,
  type AdsbTrailsSnapshot,
} from '../sources/adsb.js';
import {
  adsbHistoryAt,
  adsbHistoryOldestMs,
  snapHistoryBucket,
  HISTORY_TRAIL_DEFAULT_MIN,
  type AdsbHistory,
} from '../services/adsbHistory.js';
import { DATA_RETENTION_DAYS } from '../lib/retention.js';
import { requireRole, canViewNodeData } from '../services/auth/roles.js';
import { listNodes } from '../services/nodes/registry.js';
import { adsbNodeSourceId } from '../services/nodes/adsbNodeStore.js';
import { hub } from '../services/nodes/hub.js';
import { fetchJson } from '../sources/shared/http.js';
import { SwrCache } from '../services/swrCache.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  ErrorBody,
  GATED_ERRORS,
  okJson,
  queryValidator,
  UNAVAILABLE_ERROR,
  VALIDATION_ERROR,
  type SameShape,
} from './v1/schemas.js';

export const adsbRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_AIRCRAFT = scoped({ source: 'adsb', type: 'aircraft', window: 'live' });
const SCOPE_AIRCRAFT_HISTORY = scoped({ source: 'adsb', type: 'aircraft', window: 'history' });

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const errorJson = (description: string) => ({
  description,
  content: { 'application/json': { schema: resolver(ErrorBody) } },
});

const AdsbAircraftRecord = z.object({
  hex: z.string().describe('ICAO 24-bit address, lowercase hex; a leading ~ marks a non-ICAO (TIS-B) id'),
  callsign: z.string().nullable().describe('Callsign as broadcast'),
  reg: z.string().nullable().describe('Registration'),
  type: z.string().nullable().describe('ICAO aircraft type designator, e.g. B738'),
  lat: z.number(),
  lon: z.number(),
  altFt: z.number().nullable().describe('Barometric altitude, feet; null on the ground or when not known'),
  onGround: z.boolean(),
  gsKt: z.number().nullable().describe('Ground speed, knots'),
  trackDeg: z.number().nullable().describe('Track over the ground, degrees true'),
  category: z.string().nullable().describe('ADS-B emitter category, e.g. A3'),
  squawk: z.string().nullable().describe('Transponder code'),
  emergencySquawk: z
    .boolean()
    .describe('Squawking 7500, 7600 or 7700, or broadcasting an ADS-B emergency status'),
  esTag: z
    .enum(['polair', 'rescue', 'firebomber', 'ambulance', 'military'])
    .nullable()
    .describe("An emergency-service aircraft recognised by its callsign, or a military one by the aggregator's database flag"),
  ageSec: z.number().describe('How old the position is, seconds'),
  sourceCount: z.number().int(),
  sources: z
    .array(z.string())
    .describe('Who reported it: the aggregators (adsb_lol, adsb_fi, airplanes_live, adsb_one) and AusAware receivers (node:<name>)'),
  estimated: z
    .boolean()
    .describe('true when the position is dead-reckoned from the last heading and speed, not received — show it as a guess'),
  estimatedSec: z.number().int().nullable().describe('When estimated, seconds since the last real fix; otherwise null'),
});

const AdsbSnapshotSchema = z.object({
  aircraft: z
    .array(AdsbAircraftRecord)
    .describe('Those with an esTag or emergencySquawk first, then by altitude, lowest first; no altitude (including on the ground) last'),
  count: z.number().int(),
  emergency_count: z.number().int().describe('Aircraft with an esTag (emergency service or military)'),
  upstreams: z
    .array(
      z.object({
        id: z.string().describe('The aggregator, or "nodes" for AusAware receivers'),
        ok: z.boolean(),
        circles_ok: z.number().int().describe('Area queries it answered this poll; for "nodes", the receivers feeding'),
        circles_total: z.number().int().describe('Area queries made to it this poll; for "nodes", the receivers feeding'),
        count: z.number().int().describe('Positions it gave this poll, before merging'),
        error: z.string().optional().describe('When it answered none of its queries, the first error'),
      }),
    )
    .describe('How each source fared on the last poll'),
  bbox: z
    .tuple([z.number(), z.number(), z.number(), z.number()])
    .describe('The area covered, [west, south, east, north]'),
  fetched_at: z.string().describe('When the last poll finished, ISO 8601 (the epoch before the first one)'),
});
assertShape<SameShape<z.infer<typeof AdsbSnapshotSchema>, AdsbSnapshot>>();

const AdsbAircraft = AdsbSnapshotSchema.extend({
  history: z
    .object({
      retentionDays: z.number().int().describe('Days of history kept'),
      oldestMs: z.number().describe('The oldest instant /adsb/history can show, unix milliseconds'),
      newestMs: z.number().describe('Now, unix milliseconds'),
    })
    .describe('What /adsb/history can be asked for'),
}).meta({ id: 'AdsbAircraft' });

const AdsbHistorySchema = z
  .object({
    atMs: z.number().describe('The instant served, unix milliseconds: `at` snapped down to the minute'),
    trailMinutes: z.number().int().describe('Minutes of track before atMs, as served'),
    retentionDays: z.number().int().describe('Days of history kept'),
    oldestMs: z.number().describe('The oldest instant that can be asked for, unix milliseconds'),
    count: z.number().int(),
    truncated: z
      .boolean()
      .describe('true when the answer was cut to 4,000 aircraft, keeping those heard most recently'),
    aircraft: z.array(
      z.object({
        hex: z.string().describe('ICAO 24-bit address, lowercase hex; a leading ~ marks a non-ICAO (TIS-B) id'),
        callsign: z.string().nullable(),
        reg: z.string().nullable(),
        type: z.string().nullable().describe('ICAO aircraft type designator'),
        esTag: z.string().nullable().describe('As on the live aircraft'),
        sources: z
          .array(z.string())
          .describe('Who reported it in the hour of atMs — across the whole track when that hour recorded none'),
        points: z
          .array(z.tuple([z.number(), z.number(), z.number(), z.number().nullable()]))
          .describe('The track, oldest first: [unix milliseconds, latitude, longitude, altitude in feet or null]'),
      }),
    ),
  })
  .meta({ id: 'AdsbHistory' });
assertShape<SameShape<z.infer<typeof AdsbHistorySchema>, AdsbHistory>>();

const AdsbHistoryQuery = z.object({
  at: z.coerce
    .number()
    .optional()
    .catch(undefined)
    .describe(
      'The instant, unix milliseconds; snapped down to the minute. It may be no older than the retention window (oldestMs) or the answer is 400. Send it: absent or empty reads as 0, which is outside the window. A value that is not a number reads as now.',
    ),
  trail: z.coerce
    .number()
    .optional()
    .catch(undefined)
    .describe(`Minutes of track before the instant, 0 to 30 (rounded, and held to that range). Default ${HISTORY_TRAIL_DEFAULT_MIN}.`),
});

const AdsbTrails = z
  .object({
    trails: z
      .record(z.string(), z.array(z.tuple([z.number(), z.number(), z.number().nullable()])))
      .describe(
        'By aircraft hex: where it has been seen while tracked, oldest first, each [latitude, longitude, altitude in feet or null] — latitude first. Observed positions only; older stretches are thinned. Aircraft with fewer than two points are left out.',
      ),
    fetched_at: z.string().describe('When the trails were last rebuilt, ISO 8601 (the epoch before the first poll)'),
  })
  .meta({ id: 'AdsbTrails' });
assertShape<SameShape<z.infer<typeof AdsbTrails>, AdsbTrailsSnapshot>>();

adsbRouter.get(
  '/api/adsb/aircraft',
  SCOPE_AIRCRAFT,
  describeRoute({
    operationId: 'getAdsbAircraft',
    tags: ['Aircraft'],
    summary: 'Aircraft over Australia',
    description:
      "Aircraft over Australia now, merged from four public ADS-B aggregators (adsb.lol, adsb.fi, airplanes.live, ADSB One) and AusAware's own receivers, one record per ICAO address. Polled about every 15 seconds. An aircraft that stops reporting is held at its last position for a while; one in flight with a known heading and speed is then projected forward, marked `estimated`, until its last real fix is three minutes old.",
    responses: {
      ...okJson(envelope(AdsbAircraft), 'Every aircraft, with how each source fared and the history window'),
      ...GATED_ERRORS,
    },
  }),
  (c) => {
    // The history block rides along on the live poll rather than getting its own
    // endpoint: the map already fetches this every 15s, and the date picker
    // needs the window bounds before it can offer a first scrub.
    const now = Date.now();
    return ok(c, {
      ...adsbSnapshot(),
      history: {
        retentionDays: DATA_RETENTION_DAYS,
        oldestMs: adsbHistoryOldestMs(now),
        newestMs: now,
      },
    } satisfies z.infer<typeof AdsbAircraft>);
  },
);

// ---------------------------------------------------------------------------
// GET /api/adsb/receivers
//
// The ADS-B receivers in the fleet, so the map can offer a per-node filter to
// someone allowed to see node data. Same gate as the staff Data tab
// (canViewNodeData = owner | feeder:manager | feeder:monitor), because it is
// the same question: which receivers exist.
//
// DELIBERATELY MINIMAL. `sourceId` is the label the merge already stamps onto
// each record's `sources[]`, so the filter is a client-side set test and the
// client never has to reconstruct that string itself. Everything else a node
// row carries — the antenna pin above all, but also token prefixes, install
// ids and notes — is absent: this lands in a PUBLIC page's JavaScript, and the
// only thing that page needs is a name to show and a label to match.
// ---------------------------------------------------------------------------
adsbRouter.get('/api/adsb/receivers', requireRole(canViewNodeData), async (c) => {
  try {
    const nodes = (await listNodes()).filter((n) => n.kind === 'adsb');
    return c.json({
      receivers: nodes.map((n) => ({
        id: n.id,
        name: n.name,
        sourceId: adsbNodeSourceId(n.id, n.name),
        online: hub.isOnline(n.id),
      })),
    });
  } catch (err) {
    log.error({ err }, '/api/adsb/receivers failed');
    return c.json({ error: 'failed to list receivers' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/adsb/history?at=<epoch ms>&trail=<minutes>
//
// Where everything was at an instant, plus the track flown into it. Backed by
// adsb_tracks (migration 103), which stores one row per aircraft per hour.
//
// `at` is SNAPPED to a fixed grid inside the service and echoed back, and the
// response is deliberately not scoped to a viewport. Both choices exist so
// that many people scrubbing independently converge on the same URLs: a
// bbox-scoped or millisecond-exact response would give every viewer a private
// cache entry. The client interpolates within the bucket, so the coarse grid
// costs nothing on screen.
//
// The query string is read by hand below, exactly as it always was; the
// validator only describes it and never refuses (every field falls back).
// ---------------------------------------------------------------------------
adsbRouter.get(
  '/api/adsb/history',
  SCOPE_AIRCRAFT_HISTORY,
  describeRoute({
    operationId: 'getAdsbHistory',
    tags: ['Aircraft'],
    summary: 'Aircraft at an instant in the past',
    description:
      "Where every aircraft was at an instant within the retention window, with the track each flew in the minutes before it. The instant is snapped down to the minute, so everyone asking about the same minute shares one cacheable answer; the points either side let a client interpolate within it. An instant older than the window is a 400 whose `details.oldest_ms` says how far back it goes; the live aircraft endpoint's `history` block gives the window too.",
    responses: {
      ...okJson(envelope(AdsbHistorySchema), 'Every aircraft heard in the window, with its track'),
      ...GATED_ERRORS,
      ...VALIDATION_ERROR,
      ...UNAVAILABLE_ERROR,
      500: errorJson('The history could not be read (internal_error)'),
    },
  }),
  queryValidator(AdsbHistoryQuery),
  async (c) => {
    const url = new URL(c.req.url);
    const atRaw = Number(url.searchParams.get('at') ?? '');
    const at = Number.isFinite(atRaw) ? atRaw : Date.now();
    const trail = Number(url.searchParams.get('trail') ?? HISTORY_TRAIL_DEFAULT_MIN);

    const now = Date.now();
    const oldest = adsbHistoryOldestMs(now);
    if (at < oldest) {
      return fail(
        c,
        400,
        'validation_failed',
        'at: older than the retention window',
        { error: 'outside the retention window', oldestMs: oldest },
        { issues: [{ path: 'at', message: 'older than the retention window' }], oldest_ms: oldest },
      );
    }

    try {
      const data = await adsbHistoryAt(at, trail, now);
      if (!data) {
        return fail(c, 503, 'service_unavailable', 'The aircraft history is not available just now.', {
          error: 'history unavailable',
        });
      }

      // A closed bucket can never change, so it is worth caching hard. The most
      // recent one is still filling — the archive flushes once a minute — so it
      // gets the short window instead. Set here rather than left to
      // CACHEABLE_PATHS, whose blanket 30s would throw away most of the sharing
      // the bucketing exists to create (that middleware only fills in a
      // Cache-Control header that is absent).
      const settled = snapHistoryBucket(now) - data.atMs >= 2 * 60_000;
      c.header(
        'Cache-Control',
        settled
          ? 'public, max-age=3600, stale-while-revalidate=86400'
          : 'public, max-age=30, stale-while-revalidate=120',
      );
      return ok(c, data);
    } catch (err) {
      log.warn({ err }, '/api/adsb/history failed');
      return fail(c, 500, 'internal_error', 'The aircraft history could not be read.', {
        error: 'history query failed',
      });
    }
  },
);

// Position trails, prebuilt once per poll by the source (see
// updateTrails in sources/adsb.ts). Fixed path with no query — listed
// in CACHEABLE_PATHS so the CDN/browser 30s cache absorbs any number
// of clients; the frontend polls it at the same 30s cadence.
adsbRouter.get(
  '/api/adsb/trails',
  SCOPE_AIRCRAFT,
  describeRoute({
    operationId: 'getAdsbTrails',
    tags: ['Aircraft'],
    summary: 'Aircraft tracks',
    description:
      'The track each aircraft has flown while tracked, rebuilt after every poll of the live aircraft. A track ends ten minutes after its aircraft was last seen.',
    responses: {
      ...okJson(envelope(AdsbTrails), 'A track per aircraft'),
      ...GATED_ERRORS,
    },
  }),
  (c) => ok(c, adsbTrailsSnapshot()),
);

interface PlanespottersPhoto {
  id?: string;
  thumbnail?: { src?: string };
  thumbnail_large?: { src?: string };
  link?: string;
  photographer?: string;
}
interface PlanespottersResponse {
  photos?: PlanespottersPhoto[];
  error?: string;
}

export interface AdsbPhoto {
  src: string;
  link: string | null;
  photographer: string | null;
}

const PLANESPOTTERS_BASE = 'https://api.planespotters.net/pub/photos';
// Planespotters classifies undici's fetch as a browser request (it sends
// sec-fetch-* headers) and then requires an Origin/Referer identifying
// the embedding site — without this the API answers 403.
const PLANESPOTTERS_HEADERS = { Referer: 'https://nswpsn.forcequit.xyz/' };

// Photos per airframe are effectively static; keep them a full day and
// serve stale for a week rather than re-hitting the rate-limited API.
const photoCache = new SwrCache<AdsbPhoto | null>(2000);
const PHOTO_FRESH_MS = 24 * 3600_000;
const PHOTO_STALE_MS = 7 * 24 * 3600_000;

function firstPhoto(body: PlanespottersResponse): AdsbPhoto | null {
  const p = body.photos?.[0];
  const src = p?.thumbnail_large?.src ?? p?.thumbnail?.src;
  if (!src) return null;
  return {
    src,
    link: p?.link ?? null,
    photographer: p?.photographer ?? null,
  };
}

async function lookupPhoto(hex: string, reg: string): Promise<AdsbPhoto | null> {
  // TIS-B targets ('~'-prefixed) have synthetic ids Planespotters can't
  // know — go straight to the registration for those.
  if (!hex.startsWith('~')) {
    const byHex = await fetchJson<PlanespottersResponse>(
      `${PLANESPOTTERS_BASE}/hex/${encodeURIComponent(hex)}`,
      { timeoutMs: 8_000, headers: PLANESPOTTERS_HEADERS },
    );
    const photo = firstPhoto(byHex);
    if (photo) return photo;
  }
  if (reg) {
    const byReg = await fetchJson<PlanespottersResponse>(
      `${PLANESPOTTERS_BASE}/reg/${encodeURIComponent(reg)}`,
      { timeoutMs: 8_000, headers: PLANESPOTTERS_HEADERS },
    );
    return firstPhoto(byReg);
  }
  return null;
}

const AdsbPhotoRecord = z.object({
  src: z.string().describe("The image's URL, as Planespotters gives it (their large thumbnail where there is one)"),
  link: z.string().nullable().describe("The photo's page on Planespotters"),
  photographer: z.string().nullable(),
});
assertShape<SameShape<z.infer<typeof AdsbPhotoRecord>, AdsbPhoto>>();

const AdsbPhotoAnswer = z
  .object({
    photo: AdsbPhotoRecord.nullable().describe('null when Planespotters has none, or could not be reached'),
  })
  .meta({ id: 'AdsbPhoto' });

// `reg` is checked by the handler, after the hex, as it always was — the
// validator only describes it, so a bad hex is still the error reported first.
const AdsbPhotoQuery = z.object({
  reg: z
    .string()
    .optional()
    .describe('Registration to try when the address finds no photo, e.g. VH-ABC: 2 to 10 letters, digits or hyphens, in any case'),
});

adsbRouter.get(
  '/api/adsb/photo/:hex',
  SCOPE_AIRCRAFT,
  describeRoute({
    operationId: 'getAdsbPhoto',
    tags: ['Aircraft'],
    summary: 'A photo of an aircraft',
    description:
      'A photo of the aircraft from Planespotters.net, looked up by its ICAO address and then, failing that, by `reg`. A TIS-B id (one starting with ~) is looked up by `reg` alone. Answers are kept for a day.',
    parameters: [
      {
        in: 'path',
        name: 'hex',
        required: true,
        schema: { type: 'string' },
        description: 'ICAO 24-bit address as the live aircraft give it: 4 to 8 hex digits, optionally after a ~',
      },
    ],
    responses: {
      ...okJson(envelope(AdsbPhotoAnswer), 'The photo, or null'),
      ...GATED_ERRORS,
      400: errorJson('The hex or reg is not valid (validation_failed)'),
    },
  }),
  queryValidator(AdsbPhotoQuery),
  async (c) => {
    const hex = c.req.param('hex').trim().toLowerCase();
    const reg = (c.req.query('reg') ?? '').trim().toUpperCase();
    if (!/^~?[0-9a-f]{4,8}$/.test(hex)) {
      return fail(
        c,
        400,
        'validation_failed',
        'hex: not an ICAO address (4 to 8 hex digits, optionally after a ~)',
        { error: 'invalid hex' },
        { issues: [{ path: 'hex', message: 'not an ICAO address (4 to 8 hex digits, optionally after a ~)' }] },
      );
    }
    if (reg && !/^[A-Z0-9-]{2,10}$/.test(reg)) {
      return fail(
        c,
        400,
        'validation_failed',
        'reg: not a registration (2 to 10 letters, digits or hyphens)',
        { error: 'invalid reg' },
        { issues: [{ path: 'reg', message: 'not a registration (2 to 10 letters, digits or hyphens)' }] },
      );
    }
    try {
      const { value } = await photoCache.get(
        `${hex}|${reg}`,
        () => lookupPhoto(hex, reg),
        {
          fresh: PHOTO_FRESH_MS,
          stale: PHOTO_STALE_MS,
          onError: (err) => log.warn({ err, hex }, 'adsb: photo refresh failed'),
        },
      );
      return ok(c, { photo: value } satisfies z.infer<typeof AdsbPhotoAnswer>);
    } catch (err) {
      // Cold-path failure (nothing cached). Not worth a 5xx — the frontend
      // just shows no photo.
      log.warn({ err, hex }, 'adsb: photo lookup failed');
      return ok(c, { photo: null } satisfies z.infer<typeof AdsbPhotoAnswer>);
    }
  },
);
