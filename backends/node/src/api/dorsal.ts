/**
 * Dorsal Watch shark reports.
 *
 * Served straight out of the LiveStore snapshot the source maintains — it
 * already holds a rolling 14-day window, so there is nothing to read back
 * out of the archive the way /api/pager/hits has to.
 *
 * THE 48-HOUR DEFAULT IS LOAD-BEARING. The Discord bot polls this endpoint
 * with no query string and de-duplicates against `seen_alerts`, which its
 * retention job prunes at 7 days (discord-bot/database.py cleanup_old_seen).
 * Serve a default window longer than that and every report re-fires once it
 * ages past the dedup horizon. 48 hours leaves a wide margin; the map asks
 * for its 7 days explicitly via ?hours=168.
 *
 * Also served at /api/v1/ (server.ts mirrors it). The answer is GeoJSON, so
 * it is bare on both paths.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { dorsalSnapshot } from '../sources/dorsal.js';
import type { DorsalCategory, DorsalFeature } from '../sources/dorsal.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { geo } from './v1/respond.js';
import {
  assertShape,
  feature,
  featureCollection,
  GATED_ERRORS,
  okJson,
  PointGeometry,
  queryValidator,
  type SameShape,
} from './v1/schemas.js';

export const dorsalRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_HAZARDS = scoped({ source: 'dorsal', type: 'hazards', window: 'live' });

const DEFAULT_HOURS = 48;
/** 14 days — the most the source's rolling window can actually supply. */
const MAX_HOURS = 336;

function parseHours(raw: string | null): number {
  if (raw === null || raw.trim() === '') return DEFAULT_HOURS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_HOURS;
  return Math.min(Math.floor(n), MAX_HOURS);
}

/** Comma-separated list, lower-cased and de-blanked. Empty set = no filter. */
function parseList(raw: string | null): Set<string> {
  const out = new Set<string>();
  if (!raw) return out;
  for (const part of raw.split(',')) {
    const v = part.trim().toLowerCase();
    if (v) out.add(v);
  }
  return out;
}

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const CATEGORIES = ['detection', 'sighting', 'drone', 'incident', 'tagging', 'closure', 'advisory', 'carcass'] as const;
assertShape<SameShape<(typeof CATEGORIES)[number], DorsalCategory>>();

const DorsalReport = feature(
  PointGeometry,
  z.object({
    report_id: z.string().describe("Dorsal's id for the report"),
    title: z.string().describe('species — location, or the species alone when there is no location'),
    category: z
      .enum(CATEGORIES)
      .describe(
        "What kind of report it is, from Dorsal's encounter type: a receiver detection of a tagged shark, a sighting, a drone report, an incident (a bite or a bump), a tagging (tagged and released, or caught and released), a closure (a beach closed or the water evacuated), an advisory (a general warning, a precautionary report or an unconfirmed sighting) or a carcass (whale or shark). An encounter type not recognised is a sighting",
      ),
    encounter: z.string().describe("Dorsal's own encounter type, as published"),
    species: z
      .string()
      .describe('The species, with common spellings made one (tiger, tiger shark and tiger sharks all read as Tiger Shark); "Unknown" when not given, unknown or "other"'),
    shark_length_m: z.number().nullable().describe('The reported length of the shark, metres; null when not given'),
    distance_from_shore_m: z.number().nullable().describe('Metres; null when not given'),
    location: z.string().describe('Where, as the report gives it'),
    zone: z.string().describe("Dorsal's zone for the report"),
    state: z.string().describe('State, as Dorsal gives it, e.g. NSW'),
    reported_at: z.string().describe('When it was reported, ISO 8601 (UTC)'),
    reported_at_unix: z.number().int().describe('The same, unix seconds'),
    official: z.boolean().describe('true for a report Dorsal marks official (posted by Dorsal itself), false for a community report'),
    provenance: z.enum(['Official', 'Community report']).describe('official, in words'),
    comment: z.string().describe("The reporter's comment; empty when there is none"),
    photo_url: z.string().nullable().describe('A small photo from the report, hosted by Dorsal; null when there is none'),
    url: z.string().describe("Dorsal's website — the same for every report"),
  }),
);
assertShape<SameShape<z.infer<typeof DorsalReport>, DorsalFeature>>();

const DorsalReports = featureCollection(DorsalReport, {
  hours: z.number().int().describe('The window served, hours'),
  fetched_at: z
    .string()
    .describe('When Dorsal was last read, ISO 8601 (the epoch when nothing has been read from it yet)'),
}).meta({ id: 'DorsalReports' });

// Read by hand below, exactly as it always was; the validator only describes
// the parameters and never refuses (every field falls back).
const DorsalReportsQuery = z.object({
  hours: z.coerce
    .number()
    .optional()
    .catch(undefined)
    .describe(
      `Hours back to go, whole hours (a fraction is rounded down), at most ${MAX_HOURS} (14 days). Default ${DEFAULT_HOURS}; a value that is not a positive number reads as the default.`,
    ),
  category: z
    .string()
    .optional()
    .catch(undefined)
    .describe(`Only these categories, comma-separated, in any case: ${CATEGORIES.join(', ')}. Absent or empty: all.`),
  state: z
    .string()
    .optional()
    .catch(undefined)
    .describe('Only reports in these states, comma-separated, in any case, as the reports name them (e.g. NSW,WA). Absent or empty: all.'),
});

dorsalRouter.get(
  '/api/dorsal/reports',
  SCOPE_HAZARDS,
  describeRoute({
    operationId: 'getDorsalReports',
    tags: ['Hazards'],
    summary: 'Shark reports',
    description:
      'Shark reports around Australia from Dorsal, the community shark-reporting app: sightings by the public and by drone, detections of tagged sharks by receivers, beach closures, bites and other incidents, newest first. Dorsal is read every five minutes and the last 14 days are kept; reports placed outside Australia are left out. Asked with no `hours`, the window is the last 48 hours.',
    responses: { ...okJson(DorsalReports, 'A GeoJSON FeatureCollection of reports'), ...GATED_ERRORS },
  }),
  queryValidator(DorsalReportsQuery),
  (c) => {
    const url = new URL(c.req.url);
    const hours = parseHours(url.searchParams.get('hours'));
    const categories = parseList(url.searchParams.get('category'));
    const states = parseList(url.searchParams.get('state'));

    const cutoff = Math.floor(Date.now() / 1000) - hours * 3600;
    const snap = dorsalSnapshot();

    const features: DorsalFeature[] = [];
    for (const f of snap.features) {
      const p = f.properties;
      if (p.reported_at_unix < cutoff) continue;
      if (categories.size > 0 && !categories.has(p.category)) continue;
      if (states.size > 0 && !states.has(p.state.toLowerCase())) continue;
      features.push(f);
    }

    return geo(c, {
      type: 'FeatureCollection',
      features,
      count: features.length,
      hours,
      fetched_at: snap.fetched_at,
    } satisfies z.infer<typeof DorsalReports>);
  },
);
