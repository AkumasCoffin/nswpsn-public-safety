/**
 * Dorsal Watch shark reports — sightings, tagged-shark receiver detections,
 * beach closures and the occasional bite, Australia-wide.
 *
 * Upstream is `POST /api/public/report/list` with a `publicKey` in the body.
 * That key is `Const.API_PUBLIC_TOKEN_KEY` out of dorsalwatch.com's own
 * `js/common.min.js` — a public site token shipped to every browser that
 * loads their homepage, not a credential of ours. It lives here as a module
 * constant rather than in config.ts for the same reason BEACHWATCH_URL does:
 * config is for secrets, kill switches and self-hostable upstreams.
 *
 * Only two request parameters are honoured by the upstream, both verified:
 * `pageSize` and `pageIndex` (plus `state` / `zone`, which we don't need).
 * `limit`, `size`, `page`, `offset`, `fromDate`, `typeOfEncounter` and
 * `official` are all silently ignored — asking for a date range does not
 * narrow anything, it just returns the default first page. So the window is
 * maintained here rather than requested.
 *
 * WHY A ROLLING WINDOW. The map wants ~7 days, which is ~172 reports at the
 * measured rate of 24.5/day. Re-fetching that every poll means pageSize 400,
 * 374 KB a request, ~108 MB/day — for a feed that produces 25 rows a day. So
 * one deep page primes the window at startup and each poll afterwards pulls
 * the newest 50 and merges by id. At the busiest day observed (40 reports) a
 * 50-report page is about 30 hours of slack before anything could be missed.
 *
 * WHAT WE DELIBERATELY DO NOT INGEST. `/api/public/surfspot/search` returns
 * the global spot catalogue and embeds the owning account's `email`,
 * `resetPasscode` and `facebookId` in every single record. We never call it.
 * `report/list` is clean by comparison (emails and mobiles are all "-") but
 * it does carry reporter display names on community submissions — 89 of 2000
 * sampled, some of them full names. `account` is dropped on the floor here;
 * provenance is carried as the upstream `official` flag instead, which is all
 * anything downstream needs.
 *
 * THE REST OF THEIR API, so nobody has to re-derive it from main.min.js. Base
 * is https://www.dorsalwatch.com/api.
 *   POST /public/report/list                      what we poll
 *        /public/report/<id>                      one report
 *        /public/countries                        id, name, lengthMetric,
 *                                                 distanceMetric  <- units are
 *                                                 a property of the COUNTRY,
 *                                                 not the report: AU and
 *                                                 Réunion are metric, USA and
 *                                                 Hawaii are feet/yards
 *        /public/<countryId>/states               AU: 1 QLD, 2 NSW, 3 SA,
 *                                                 4 TAS, 5 VIC, 6 WA
 *        /public/surfspot/zone/<country>/<state>/<token>   zone list
 *        /image/<media.filename>/<n>              bucketed resizer; the
 *                                                 media.cdn* fields are better
 *        /surfspot/search                         *** DO NOT CALL *** — the
 *                                                 account-data leak above
 * Nothing here is needed today: the report feed already carries the state and
 * zone names as strings, and we derive the archive's state from coordinates.
 */
import { fetchJson } from './shared/http.js';
import { config } from '../config.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';
import { getPool } from '../db/pool.js';
import type { ArchiveRow } from '../store/archive.js';
import { log } from '../lib/log.js';
import { inAuBbox } from './adsb.js';

const DORSAL_REPORT_URL = 'https://www.dorsalwatch.com/api/public/report/list';
/** dorsalwatch.com's own `Const.API_PUBLIC_TOKEN_KEY`; public, not a secret. */
const DORSAL_PUBLIC_TOKEN = 'ab61cd9427bea80f22e641c04c312195';

/** One deep page at startup to prime the window (~17 days at observed rates). */
const PRIME_PAGE_SIZE = 500;
/** Steady-state page. ~47 KB; ~30 hours of slack at the busiest observed day. */
const POLL_PAGE_SIZE = 50;

/** How much history the LiveStore snapshot holds. The map asks for 7 days. */
const WINDOW_DAYS = 14;
/**
 * How far back the archive fan-out will reach. Also the backfill depth on a
 * cold start. Deliberately shorter than WINDOW_DAYS so the map has headroom
 * without widening what gets written.
 */
const ARCHIVE_BACKFILL_DAYS = 7;

const DAY_MS = 86_400_000;

/** Canonical archive `source` value — see SOURCE_TO_FAMILY and filterCache. */
export const DORSAL_ARCHIVE_SOURCE = 'dorsal';

export type DorsalCategory =
  | 'detection'
  | 'sighting'
  | 'drone'
  | 'incident'
  | 'tagging'
  | 'closure'
  | 'advisory'
  | 'carcass';

/**
 * `typeOfEncounter` is free text and the upstream has accumulated 29 distinct
 * values (including a blank and a misspelled "Aerial Survelliance"). Mapped
 * here against every value observed across a 4000-report sample; anything new
 * or blank falls through to 'sighting', which is the safe default — it shows
 * on the map and does not claim an incident happened.
 */
const ENCOUNTER_CATEGORY: Record<string, DorsalCategory> = {
  'receiver tag': 'detection',
  // Drone is its own category, not a plain sighting. It is the second most
  // common encounter upstream (37 of 181 over a sample week, ~20%), and a
  // lifeguard drone overflight is a different kind of claim from someone on
  // the sand saying they saw something — folding the two together made the
  // drone flights invisible as a group and unfilterable on their own.
  drone: 'drone',
  'tag and release': 'tagging',
  'caught & released': 'tagging',
  bite: 'incident',
  bump: 'incident',
  'beach closure': 'closure',
  'water evacuated': 'closure',
  'general warning': 'advisory',
  'precautionary report': 'advisory',
  'unconfirmed sighting': 'advisory',
  'whale carcass': 'carcass',
  'deceased whale': 'carcass',
  'shark carcass': 'carcass',
};

/**
 * Species is free text too, and inconsistently cased and pluralised —
 * "White Shark", "white shark" and "Great White" are all the same animal,
 * as are "Tiger", "tiger shark" and "tiger sharks". Without this the logs
 * page subcategory filter would offer a dozen spellings of three species.
 */
const SPECIES_CANON: Record<string, string> = {
  'white shark': 'White Shark',
  'great white': 'Great White Shark',
  'great white shark': 'Great White Shark',
  bull: 'Bull Shark',
  'bull shark': 'Bull Shark',
  tiger: 'Tiger Shark',
  'tiger shark': 'Tiger Shark',
  'tiger sharks': 'Tiger Shark',
  whaler: 'Whaler',
  'bronze whaler': 'Whaler',
  mako: 'Mako',
  hammerhead: 'Hammerhead',
  'hammerhead shark': 'Hammerhead',
  blacktip: 'Blacktip',
  sevengill: 'Sevengill',
  'port jackson': 'Port Jackson',
  reef: 'Reef Shark',
  'reef shark': 'Reef Shark',
};

export interface DorsalFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    /** Upstream report id — stable, and the archive `source_id`. */
    report_id: string;
    title: string;
    category: DorsalCategory;
    /** Upstream's own `typeOfEncounter`, unmapped. */
    encounter: string;
    species: string;
    /** Metres, when the upstream gave a number rather than "N/A". */
    shark_length_m: number | null;
    distance_from_shore_m: number | null;
    location: string;
    zone: string;
    state: string;
    reported_at: string;
    reported_at_unix: number;
    /** True when posted by Dorsal HQ (DPIRD receivers, SLS, Westpac heli). */
    official: boolean;
    provenance: 'Official' | 'Community report';
    comment: string;
    photo_url: string | null;
    url: string;
  };
}

export interface DorsalSnapshot {
  type: 'FeatureCollection';
  features: DorsalFeature[];
  count: number;
  fetched_at: string;
}

const EMPTY: DorsalSnapshot = {
  type: 'FeatureCollection',
  features: [],
  count: 0,
  fetched_at: new Date(0).toISOString(),
};

interface DorsalRawReport {
  id?: unknown;
  location?: unknown;
  state?: unknown;
  zone?: unknown;
  country?: unknown;
  coordinate?: unknown;
  reportTime?: unknown;
  typeOfEncounter?: unknown;
  typeOfShark?: unknown;
  sharkLength?: unknown;
  distanceFromShore?: unknown;
  comment?: unknown;
  official?: unknown;
  sharkPhotos?: unknown;
  // `account` is intentionally absent: see the header note on reporter names.
}

function asString(v: unknown): string {
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return '';
}

/**
 * Upstream sends measurements as strings, and uses the literal "N/A" far more
 * often than a value (3448 of 4000 for distanceFromShore). Treat "N/A", blank
 * and anything non-finite as absent rather than coercing to 0 — a shark "0 m
 * from shore" reads as a fact, and it isn't one.
 */
/**
 * Shark length / distance from shore, as metres.
 *
 * The unit is NOT carried on the report — it is a property of the report's
 * country, from /api/public/countries: Australia is {lengthMetric: 'm',
 * distanceMetric: 'm'}, but Hawaii and USA are {'f', 'y'} — feet and yards.
 * Calling these metres is therefore only correct because the AU bounding-box
 * filter has already dropped every overseas report (28 of 4000 sampled, in
 * Hawaii and California). If that filter is ever widened, this function must
 * take the country and convert, or the map will under-report a 10-foot shark
 * as 10 metres. Réunion is metric; USA/Hawaii are not.
 */
function asMetres(v: unknown): number | null {
  const s = asString(v).trim();
  if (s === '' || s.toUpperCase() === 'N/A') return null;
  const n = Number(s);
  return Number.isFinite(n) && n >= 0 ? n : null;
}

/** Upstream `coordinate` is a "lat,lon" string. 4000/4000 sampled parsed. */
function parseCoordinate(v: unknown): [number, number] | null {
  const parts = asString(v).split(',');
  if (parts.length !== 2) return null;
  const lat = Number(parts[0]);
  const lon = Number(parts[1]);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return [lat, lon];
}

export function categoryFor(encounter: string): DorsalCategory {
  return ENCOUNTER_CATEGORY[encounter.trim().toLowerCase()] ?? 'sighting';
}

export function speciesFor(raw: string): string {
  const key = raw.trim().toLowerCase();
  if (key === '' || key === 'unknown' || key === 'unknown sp.' || key === 'other') {
    return 'Unknown';
  }
  return SPECIES_CANON[key] ?? raw.trim();
}

/** First usable photo, hotlinked from their CDN — never proxied or rehosted. */
/**
 * Hotlink the smallest usable variant, never the original.
 *
 * `media` carries cdnSmall / cdnMedium / cdnLarge / cdnOrigin. Measured over
 * 1000 reports (only 10 carry a photo at all): cdnSmall and cdnOrigin are
 * populated 10/10, cdnMedium 1/10 and cdnLarge 0/10 — so medium and large are
 * tried but cannot be relied on. cdnSmall is ~250px wide at 12-17 KB against
 * 36-58 KB for the origin, and the tooltip renders the image in a ~260px box,
 * so the origin is pure waste on someone else's CDN. (Their own /api/image/
 * <filename>/<n> resizer is bucketed rather than exact — 50 and 200 both
 * return the same 250px rendition — so these fields are the better handle.)
 */
function firstPhotoUrl(v: unknown): string | null {
  if (!Array.isArray(v)) return null;
  for (const p of v) {
    if (!p || typeof p !== 'object') continue;
    const media = (p as Record<string, unknown>)['media'];
    if (!media || typeof media !== 'object') continue;
    const m = media as Record<string, unknown>;
    for (const field of ['cdnSmall', 'cdnMedium', 'cdnLarge', 'cdnOrigin']) {
      const url = m[field];
      if (typeof url === 'string' && url.startsWith('https://')) return url;
    }
  }
  return null;
}

function normalise(raw: DorsalRawReport): DorsalFeature | null {
  const id = raw.id;
  if (id === undefined || id === null || id === '') return null;

  const coord = parseCoordinate(raw.coordinate);
  if (!coord) return null;
  const [lat, lon] = coord;
  // Dorsal carries a handful of overseas spots (Hawaii, California — 28 of
  // 4000 sampled). This site is Australian; drop them rather than drawing a
  // pin in the Pacific.
  if (!inAuBbox(lat, lon)) return null;

  const ms = typeof raw.reportTime === 'number' ? raw.reportTime : Number(raw.reportTime);
  if (!Number.isFinite(ms) || ms <= 0) return null;

  const encounter = asString(raw.typeOfEncounter).trim();
  const category = categoryFor(encounter);
  const species = speciesFor(asString(raw.typeOfShark));
  const location = asString(raw.location).trim();
  const official = raw.official === true;

  // `data->>'title'` is what the logs page projects as the row title; without
  // one every row renders "Unknown" (see api/data-history.ts formatRecord).
  const title = location ? `${species} — ${location}` : species;

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [lon, lat] },
    properties: {
      report_id: String(id),
      title,
      category,
      encounter,
      species,
      shark_length_m: asMetres(raw.sharkLength),
      distance_from_shore_m: asMetres(raw.distanceFromShore),
      location,
      zone: asString(raw.zone).trim(),
      state: asString(raw.state).trim(),
      reported_at: new Date(ms).toISOString(),
      reported_at_unix: Math.floor(ms / 1000),
      official,
      provenance: official ? 'Official' : 'Community report',
      comment: asString(raw.comment).trim(),
      photo_url: firstPhotoUrl(raw.sharkPhotos),
      url: 'https://www.dorsalwatch.com/',
    },
  };
}

async function fetchPage(pageSize: number): Promise<DorsalRawReport[]> {
  const body = JSON.stringify({ publicKey: DORSAL_PUBLIC_TOKEN, pageSize });
  const res = await fetchJson<unknown>(DORSAL_REPORT_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
    timeoutMs: 20_000,
  });
  if (!res || typeof res !== 'object') return [];
  const obj = res as Record<string, unknown>;
  // Upstream wraps everything as {error, responseData} and answers HTTP 200
  // even for failures — a missing publicKey comes back as error code 1016
  // with a 200, so the status code alone proves nothing.
  const err = obj['error'];
  if (err && typeof err === 'object') {
    const msg = asString((err as Record<string, unknown>)['message']);
    throw new Error(`dorsal upstream error: ${msg || 'unknown'}`);
  }
  const rows = obj['responseData'];
  return Array.isArray(rows) ? (rows as DorsalRawReport[]) : [];
}

// ---------------------------------------------------------------------------
// Rolling window + archive fan-out bookkeeping
// ---------------------------------------------------------------------------

/** report_id -> feature, pruned to WINDOW_DAYS on every merge. */
const windowById = new Map<string, DorsalFeature>();
/**
 * Report ids already written to the archive. Seeded once from the sidecar so
 * a restart re-imports nothing, then maintained in memory. Bounded by the
 * same retention the sidecar is pruned on, so it cannot grow without limit.
 */
const archivedIds = new Set<string>();
let primed = false;
let seedAttempted = false;

/**
 * Seed `archivedIds` from archive_misc_latest so the cold-start backfill is
 * idempotent. Without this, every deploy restart would re-insert the whole
 * 7-day window — and deploys are frequent.
 *
 * A failure here is not fatal: the worst case is duplicate archive rows, and
 * the logs page collapses those via DISTINCT ON / unique=1 anyway. Better to
 * serve the map than to refuse to start because the sidecar was unreachable.
 */
async function seedArchivedIds(): Promise<void> {
  if (seedAttempted) return;
  seedAttempted = true;
  try {
    const pool = await getPool();
    if (!pool) return;
    const res = await pool.query<{ source_id: string | null }>(
      `SELECT source_id FROM archive_misc_latest
        WHERE source = $1 AND source_id IS NOT NULL`,
      [DORSAL_ARCHIVE_SOURCE],
    );
    for (const r of res.rows) {
      if (r.source_id) archivedIds.add(r.source_id);
    }
    log.info({ seeded: archivedIds.size }, 'dorsal: seeded archived ids from sidecar');
  } catch (err) {
    log.warn({ err }, 'dorsal: could not seed archived ids; may re-archive the window once');
  }
}

export async function fetchDorsal(): Promise<DorsalSnapshot> {
  if (config.DORSAL_DISABLED) return { ...EMPTY, fetched_at: new Date().toISOString() };

  if (!primed) await seedArchivedIds();

  const pageSize = primed ? POLL_PAGE_SIZE : PRIME_PAGE_SIZE;
  const raw = await fetchPage(pageSize);

  const now = Date.now();
  const cutoff = now - WINDOW_DAYS * DAY_MS;

  for (const r of raw) {
    const f = normalise(r);
    if (!f) continue;
    // Upstream can edit a report (there is an "Updated report" encounter
    // type), so a re-seen id overwrites rather than being skipped.
    windowById.set(f.properties.report_id, f);
  }
  for (const [id, f] of windowById) {
    if (f.properties.reported_at_unix * 1000 < cutoff) windowById.delete(id);
  }

  primed = true;

  const features = Array.from(windowById.values()).sort(
    (a, b) => b.properties.reported_at_unix - a.properties.reported_at_unix,
  );
  return {
    type: 'FeatureCollection',
    features,
    count: features.length,
    fetched_at: new Date(now).toISOString(),
  };
}

/**
 * Per-report archive fan-out.
 *
 * The archive parents are append-only (store/archive.ts insertChunk), so the
 * default "write the whole snapshot every poll" behaviour would put ~49,000
 * rows a day into archive_misc for a feed that produces 25 real reports. That
 * is the entire reason this override exists.
 *
 * A report is written exactly once — the first time this process sees its id
 * — bounded to ARCHIVE_BACKFILL_DAYS so a cold start imports a week rather
 * than everything the prime page happened to reach. On the first poll after a
 * restart `archivedIds` has been seeded from the sidecar, so nothing is
 * rewritten; on a fresh database that same pass IS the backfill.
 *
 * Consequence worth knowing: because a row is written once and never
 * re-touched, its sidecar `last_seen_at` stops advancing immediately, so
 * cleanup prunes it on the normal DATA_RETENTION_DAYS schedule like any other
 * archived record.
 */
export function dorsalArchiveItems(
  data: unknown,
  fetched_at: number,
  source: string,
): ArchiveRow[] {
  const snap = (data as DorsalSnapshot | null) ?? null;
  if (!snap || !Array.isArray(snap.features)) return [];

  const cutoffUnix = Math.floor(Date.now() / 1000) - ARCHIVE_BACKFILL_DAYS * 86_400;
  const out: ArchiveRow[] = [];

  for (const f of snap.features) {
    const p = f?.properties;
    if (!p || !p.report_id) continue;
    if (archivedIds.has(p.report_id)) continue;
    if (p.reported_at_unix < cutoffUnix) continue;

    archivedIds.add(p.report_id);
    out.push({
      source,
      source_id: p.report_id,
      fetched_at,
      source_timestamp_unix: p.reported_at_unix,
      lat: f.geometry.coordinates[1],
      lng: f.geometry.coordinates[0],
      category: p.category,
      subcategory: p.species || null,
      // lat/lng are repeated INSIDE data on purpose. The row's own lat/lng
      // columns are not what derives archive_*_latest.state — stateForRow()
      // reads the coordinate back out of the data JSON (lat/lng, latitude/
      // longitude, lon). Without these two keys every shark row lands with
      // state NULL and the logs page's state filter never matches one.
      data: { ...p, lat: f.geometry.coordinates[1], lng: f.geometry.coordinates[0] },
    });
  }
  return out;
}

export default function register(): void {
  registerSource<DorsalSnapshot>({
    name: 'dorsal_reports',
    archiveSource: DORSAL_ARCHIVE_SOURCE,
    family: 'misc',
    intervalMs: 300_000,
    fetch: fetchDorsal,
    archiveItems: dorsalArchiveItems,
  });
}

export function dorsalSnapshot(): DorsalSnapshot {
  return liveStore.getData<DorsalSnapshot>('dorsal_reports') ?? EMPTY;
}

/** Test hook — clears the rolling window and the archived-id bookkeeping. */
export function _resetDorsalState(): void {
  windowById.clear();
  archivedIds.clear();
  primed = false;
  seedAttempted = false;
}
