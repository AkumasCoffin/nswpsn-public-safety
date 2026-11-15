/**
 * What every power-outage source shares.
 *
 * GET /api/v1/outages (api/outages.ts) answers in one record shape, `Outage`.
 * The three NSW distributors were written before that shape existed: their
 * sources store each distributor's own records and api/outages.ts maps them.
 * Every distributor added since stores `Outage[]` under its own name, so the
 * merged feed reads its store as it is. This file is what such a source is
 * written against:
 *
 *   - the record, its small lists, and each distributor's name and state;
 *   - zonedUtc: a distributor's local time, in any of the ways they write
 *     one, as ISO 8601 UTC;
 *   - outageOf: one record with the fields a source should not have to
 *     repeat (source, distributor, state) filled in;
 *   - isScheduled / withStatus: whether planned work has started is asked of
 *     the clock when the feed is read, never stored;
 *   - outageArchiveItems: one history row per outage that changes only when
 *     the outage does;
 *   - registerOutages: the poller entry, with a <NAME>_DISABLED switch;
 *   - keepParts: for a distributor that publishes several files;
 *   - num, text, titleCase, ring, outerRings: reading what the files hold.
 *
 * A source file is a pure parse() from the distributor's JSON to records, and
 * a fetch() that reads the URL and calls it. A response of the wrong type
 * must throw: the poller then keeps the last good snapshot, where an empty
 * list would say there are no outages. Outages that are over (restored,
 * completed, cancelled) are left out by the source.
 */
import { registerSource } from '../../services/sourceRegistry.js';
import { liveStore } from '../../store/live.js';
import { log } from '../../lib/log.js';
import type { ArchiveRow } from '../../store/archive.js';

// ---------------------------------------------------------------------------
// The record
// ---------------------------------------------------------------------------

export const OUTAGE_STATES = ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'NT', 'ACT'] as const;
export type OutageState = (typeof OUTAGE_STATES)[number];

/** In the order the merged feed lists them: NSW's three, then state by state. */
export const DISTRIBUTORS = [
  'endeavour',
  'essential',
  'ausgrid',
  'ausnet',
  'citipower',
  'powercor',
  'jemena',
  'sapn',
  'westernpower',
  'horizon',
] as const;
export type Distributor = (typeof DISTRIBUTORS)[number];

/**
 * The distributors whose source stores `Outage[]`. Each one's name is its
 * LiveStore key, the `source` its history is archived under, and (upper-cased)
 * the start of its <NAME>_DISABLED switch.
 */
export const OUTAGE_STORES = [
  'ausnet',
  'citipower',
  'powercor',
  'jemena',
  'sapn',
  'westernpower',
  'horizon',
] as const;
export type OutageStore = (typeof OUTAGE_STORES)[number];

export const OUTAGE_KINDS = ['unplanned', 'planned'] as const;
export type OutageKind = (typeof OUTAGE_KINDS)[number];

export const OUTAGE_STATUSES = ['active', 'scheduled'] as const;
export type OutageStatus = (typeof OUTAGE_STATUSES)[number];

/** What each distributor's records are archived under (the `source` of an archive_power row). */
export const OUTAGE_SOURCES = [
  'endeavour_current',
  'endeavour_planned',
  'essential_current',
  'essential_planned',
  'essential_future',
  'ausgrid',
  ...OUTAGE_STORES,
] as const;
export type OutageSource = (typeof OUTAGE_SOURCES)[number];

/**
 * Each distributor's name as people know it, and the state its network is in.
 * `also` is any other state the network reaches into: Essential Energy is a
 * NSW distributor that supplies towns in southern Queensland, so its outages
 * there are QLD's, and a request for QLD has to read it.
 */
export const DISTRIBUTOR_INFO: Readonly<Record<Distributor, { name: string; state: OutageState; also?: readonly OutageState[] }>> = {
  endeavour: { name: 'Endeavour Energy', state: 'NSW' },
  essential: { name: 'Essential Energy', state: 'NSW', also: ['QLD'] },
  ausgrid: { name: 'Ausgrid', state: 'NSW' },
  ausnet: { name: 'AusNet', state: 'VIC' },
  citipower: { name: 'CitiPower', state: 'VIC' },
  powercor: { name: 'Powercor', state: 'VIC' },
  jemena: { name: 'Jemena', state: 'VIC' },
  sapn: { name: 'SA Power Networks', state: 'SA' },
  westernpower: { name: 'Western Power', state: 'WA' },
  horizon: { name: 'Horizon Power', state: 'WA' },
};

/** The time zone a state's distributors write their times in, for zonedUtc. */
export const STATE_ZONE: Readonly<Record<OutageState, string>> = {
  NSW: 'Australia/Sydney',
  VIC: 'Australia/Melbourne',
  QLD: 'Australia/Brisbane',
  SA: 'Australia/Adelaide',
  WA: 'Australia/Perth',
  NT: 'Australia/Darwin',
  ACT: 'Australia/Sydney',
};

/** One outage in the shared shape, before it becomes a feature. */
export interface Outage {
  /** The distributor's own id for the outage; unique with `source`. */
  id: string;
  source: OutageSource;
  distributor: Distributor;
  state: OutageState;
  kind: OutageKind;
  status: OutageStatus;
  /** The suburb or locality; empty when the distributor gives none. */
  area: string;
  customers_affected: number;
  cause: string;
  /** ISO 8601 UTC, or null. */
  start_at: string | null;
  restore_at: string | null;
  updated_at: string | null;
  lat: number | null;
  lon: number | null;
  /** Outer rings of [longitude, latitude]; empty when the distributor gives no outline. */
  polygons: number[][][];
  /** The distributor's other fields, under its own names. */
  details: Record<string, unknown>;
}

/**
 * An outage as a source makes it: `status` is optional because the clock
 * decides it (withStatus). A source gives one only to pass on what the
 * distributor says about planned work whose start time it could not read.
 */
export type OutageRecord = Omit<Outage, 'status'> & { status?: OutageStatus };

// ---------------------------------------------------------------------------
// Reading what the files hold
// ---------------------------------------------------------------------------

/** A finite number, also when it is written as text ("-37.81"); otherwise null. */
export function num(v: unknown): number | null {
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v !== 'string' || !v.trim()) return null;
  const n = Number(v.trim());
  return Number.isFinite(n) ? n : null;
}

/** Trimmed text; a number as written; empty for anything else. */
export function text(v: unknown): string {
  if (typeof v === 'string') return v.trim();
  return typeof v === 'number' && Number.isFinite(v) ? String(v) : '';
}

/**
 * "MOUNT GAMBIER" → "Mount Gambier". Only a name written all in capitals is
 * changed; one that already has lower-case letters is the distributor's own
 * spelling and stays.
 */
export function titleCase(v: unknown): string {
  const s = text(v).replace(/\s+/g, ' ');
  if (!s || /[a-z]/.test(s)) return s;
  return s.toLowerCase().replace(/(^|[\s\-'’(/.])([a-z])/g, (_m, before: string, letter: string) => before + letter.toUpperCase());
}

const pick = (o: Record<string, unknown>, keys: readonly string[]): number | null => {
  for (const k of keys) {
    const n = num(o[k]);
    if (n !== null) return n;
  }
  return null;
};

/**
 * One ring of [longitude, latitude] from the ways a distributor writes an
 * outline: a list of pairs, a list of objects ({lat, lng}, {latitude,
 * longitude}, {y, x}), or text of "a,b a,b …" pairs. `order` says which way
 * round the pairs are; objects name their own. Points that are not two
 * numbers are dropped, and fewer than three points is no outline: [].
 */
export function ring(v: unknown, order: 'lonlat' | 'latlon' = 'lonlat'): number[][] {
  const items: unknown[] =
    typeof v === 'string' ? v.split(/[\s;]+/).filter(Boolean).map((p) => p.split(',')) : Array.isArray(v) ? v : [];
  const out: number[][] = [];
  for (const item of items) {
    let lon: number | null = null;
    let lat: number | null = null;
    if (Array.isArray(item)) {
      const a = num(item[0]);
      const b = num(item[1]);
      lon = order === 'lonlat' ? a : b;
      lat = order === 'lonlat' ? b : a;
    } else if (item && typeof item === 'object') {
      const o = item as Record<string, unknown>;
      lon = pick(o, ['lng', 'lon', 'long', 'longitude', 'x']);
      lat = pick(o, ['lat', 'latitude', 'y']);
    }
    if (lon !== null && lat !== null) out.push([lon, lat]);
  }
  return out.length >= 3 ? out : [];
}

/**
 * The outer rings of a GeoJSON Polygon or MultiPolygon (or a Feature holding
 * one, or any of them as JSON text). Holes are left out; anything else is [].
 */
export function outerRings(geometry: unknown): number[][][] {
  let g: unknown = geometry;
  if (typeof g === 'string') {
    try {
      g = JSON.parse(g);
    } catch {
      return [];
    }
  }
  if (!g || typeof g !== 'object') return [];
  const o = g as { type?: unknown; geometry?: unknown; coordinates?: unknown };
  if (o.type === 'Feature') return outerRings(o.geometry);
  if (!Array.isArray(o.coordinates)) return [];
  const polygons: unknown[] = o.type === 'Polygon' ? [o.coordinates] : o.type === 'MultiPolygon' ? o.coordinates : [];
  return polygons.map((p) => ring(Array.isArray(p) ? p[0] : null)).filter((r) => r.length > 0);
}

// ---------------------------------------------------------------------------
// Times
// ---------------------------------------------------------------------------

// A clock time: hours, minutes, optional seconds and fraction, optional AM/PM.
// Five groups.
const CLOCK = String.raw`(\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?(?:\s*([AP])M)?`;
const OFFSET = String.raw`(Z|[+-]\d{2}(?::?\d{2})?)`;

/** 2026-11-13T06:55[:00[.123]], with a space for the T, AM/PM, and a zone or none. */
const ISO = new RegExp(String.raw`^(\d{4})-(\d{2})-(\d{2})[T ]${CLOCK}\s*${OFFSET}?$`, 'i');
/** 20261113T075326+08:00, as Western Power writes it. */
const COMPACT = new RegExp(String.raw`^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})?(?:\.(\d+))?\s*${OFFSET}?$`, 'i');
/** 13/11/2026 15:53[:00] and 13/11/2026 03:53 PM. */
const DMY = new RegExp(String.raw`^(\d{1,2})\/(\d{1,2})\/(\d{4}),?[ T]+${CLOCK}$`, 'i');
/** 15:53 13-11-2026, as CitiPower and Powercor write it. */
const CLOCK_DMY = new RegExp(String.raw`^${CLOCK}\s+(\d{1,2})[-/](\d{1,2})[-/](\d{4})$`, 'i');

/**
 * A date and clock time as the milliseconds they would be in UTC, or null
 * when they are not a real date and time (month 13, 30 February, 25:00,
 * 13:00 PM).
 */
function wallMs(
  y: number,
  mo: number,
  d: number,
  h: string | undefined,
  mi: string | undefined,
  s: string | undefined,
  fraction: string | undefined,
  meridiem: string | undefined,
): number | null {
  let hh = Number(h);
  const mm = Number(mi);
  const ss = Number(s ?? 0);
  if (meridiem) {
    if (hh < 1 || hh > 12) return null;
    hh = (hh % 12) + (meridiem.toUpperCase() === 'P' ? 12 : 0);
  }
  if (mo < 1 || mo > 12 || d < 1 || hh > 23 || mm > 59 || ss > 59) return null;
  const ms = fraction ? Number(`${fraction}00`.slice(0, 3)) : 0;
  const t = Date.UTC(y, mo - 1, d, hh, mm, ss, ms);
  // Date.UTC rolls 30 February over into March; that is not a date anyone wrote.
  return Number.isFinite(t) && new Date(t).getUTCDate() === d ? t : null;
}

/** "+08:00", "+0800", "+08" or "Z" in milliseconds east of UTC; null when it is no offset. */
function offsetMs(offset: string): number | null {
  if (offset.toUpperCase() === 'Z') return 0;
  const hours = Number(offset.slice(1, 3));
  const minutes = Number(offset.slice(-2)) * (offset.length > 3 ? 1 : 0);
  if (hours > 14 || minutes > 59) return null;
  return (offset.startsWith('-') ? -1 : 1) * (hours * 60 + minutes) * 60_000;
}

const _zoneClocks = new Map<string, Intl.DateTimeFormat>();

/** How far the zone's clock is ahead of UTC at an instant, in milliseconds. */
function zoneAheadMs(ms: number, zone: string): number {
  let clock = _zoneClocks.get(zone);
  if (!clock) {
    clock = new Intl.DateTimeFormat('en-CA', {
      timeZone: zone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      // 00..23; plain hour12:false can say "24" at midnight (lib/sydneyTime.ts).
      hourCycle: 'h23',
    });
    _zoneClocks.set(zone, clock);
  }
  const whole = Math.floor(ms / 1000) * 1000;
  const p: Record<string, number> = {};
  for (const part of clock.formatToParts(new Date(whole))) p[part.type] = Number(part.value);
  return Date.UTC(p['year']!, p['month']! - 1, p['day']!, p['hour']! % 24, p['minute']!, p['second']!) - whole;
}

/**
 * A distributor's time as ISO 8601 UTC, or null when it is empty or cannot
 * be read. A time written with a zone (Z or an offset) is that instant and
 * `zone` is not used; one written without is the wall clock of `zone`, an
 * IANA name such as Australia/Melbourne (STATE_ZONE has each state's).
 *
 * Read: 2026-11-13T06:55[:00[.123]][Z|+11:00] and the same with a space;
 * 2026-11-13 06:55PM (AusNet); 13/11/2026 15:53[:00] and 13/11/2026 03:53 PM;
 * 15:53 13-11-2026 (CitiPower, Powercor); 20261113T075326+08:00 (Western
 * Power).
 *
 * Daylight saving: the offset is the zone's own at that date, found the way
 * lib/sydneyTime.ts sydneyUnixFromNaive finds Sydney's — guess, then correct
 * once, which settles the days the clocks change. An hour that happens twice
 * when the clocks go back is read as the second, standard-time one. A zone
 * name Intl does not know throws: that is a mistake in the source file, not
 * in the distributor's data.
 */
export function zonedUtc(value: unknown, zone: string): string | null {
  if (typeof value !== 'string') return null;
  const s = value.trim();
  if (!s) return null;

  let wall: number | null = null;
  let offset: string | undefined;
  let m: RegExpExecArray | null;
  if ((m = ISO.exec(s))) {
    wall = wallMs(+m[1]!, +m[2]!, +m[3]!, m[4], m[5], m[6], m[7], m[8]);
    offset = m[9];
  } else if ((m = COMPACT.exec(s))) {
    wall = wallMs(+m[1]!, +m[2]!, +m[3]!, m[4], m[5], m[6], m[7], undefined);
    offset = m[8];
  } else if ((m = DMY.exec(s))) {
    wall = wallMs(+m[3]!, +m[2]!, +m[1]!, m[4], m[5], m[6], m[7], m[8]);
  } else if ((m = CLOCK_DMY.exec(s))) {
    wall = wallMs(+m[8]!, +m[7]!, +m[6]!, m[1], m[2], m[3], m[4], m[5]);
  }
  if (wall === null) return null;

  if (offset) {
    const east = offsetMs(offset);
    return east === null ? null : new Date(wall - east).toISOString();
  }
  let instant = wall - zoneAheadMs(wall, zone);
  instant = wall - zoneAheadMs(instant, zone);
  return new Date(instant).toISOString();
}

// ---------------------------------------------------------------------------
// One record, and whether its work has started
// ---------------------------------------------------------------------------

/** What a source gives for one outage; everything but the id and kind may be left out. */
export interface OutageFields {
  id: string | number;
  kind: OutageKind;
  /** Only for planned work whose start time could not be read — see withStatus. */
  status?: OutageStatus;
  area?: string;
  customers_affected?: number | null;
  cause?: string;
  start_at?: string | null;
  restore_at?: string | null;
  updated_at?: string | null;
  lat?: number | null;
  lon?: number | null;
  polygons?: number[][][];
  details?: Record<string, unknown>;
}

/**
 * One record of a distributor that stores `Outage[]`: its `source` is the
 * distributor's name and its state the distributor's. Customers are a whole
 * number, 0 when not given. A position needs both numbers, and 0,0 (what
 * some feeds write for "unknown") is none.
 *
 * `details` is archived with the record, so nothing in it may change from
 * poll to poll unless the outage did (see outageArchiveItems): no "file
 * generated at", no "minutes since".
 */
export function outageOf(distributor: OutageStore, f: OutageFields): OutageRecord {
  const lat = f.lat ?? null;
  const lon = f.lon ?? null;
  const placed = lat !== null && lon !== null && Number.isFinite(lat) && Number.isFinite(lon) && !(lat === 0 && lon === 0);
  const record: OutageRecord = {
    id: text(f.id),
    source: distributor,
    distributor,
    state: DISTRIBUTOR_INFO[distributor].state,
    kind: f.kind,
    area: f.area ?? '',
    customers_affected: Math.max(0, Math.round(f.customers_affected ?? 0) || 0),
    cause: f.cause ?? '',
    start_at: f.start_at ?? null,
    restore_at: f.restore_at ?? null,
    updated_at: f.updated_at ?? null,
    lat: placed ? lat : null,
    lon: placed ? lon : null,
    polygons: f.polygons ?? [],
    details: f.details ?? {},
  };
  if (f.status) record.status = f.status;
  return record;
}

/** Planned work that has not started yet: nobody's power is off. */
export function isScheduled(o: Pick<Outage, 'kind' | 'start_at'>, nowMs: number): boolean {
  if (o.kind !== 'planned' || !o.start_at) return false;
  const start = Date.parse(o.start_at);
  return Number.isFinite(start) && start > nowMs;
}

/**
 * The record with its status as of `nowMs`. Distributors list planned work
 * days or weeks ahead, and a status stored when the file was read would still
 * say "scheduled" after the work began, so the merged feed asks again every
 * time it is read.
 *
 * An unplanned outage is active. Planned work with a start time is scheduled
 * until that time and active after it. Planned work whose start could not be
 * read keeps the status the source gave it — the distributor's own word, from
 * a "future" file or a status field — and is active when it gave none.
 */
export function withStatus(o: OutageRecord, nowMs: number): Outage {
  let status: OutageStatus = 'active';
  if (o.kind === 'planned') {
    const start = o.start_at ? Date.parse(o.start_at) : Number.NaN;
    if (Number.isFinite(start)) status = start > nowMs ? 'scheduled' : 'active';
    else if (o.status === 'scheduled') status = 'scheduled';
  }
  return { ...o, status };
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

/**
 * One archive row per outage. The poller's default extractor would store the
 * records as they are, `status` included, and that field flips with the clock.
 *
 * The archive writer hashes each row's `data` and inserts the row again only
 * when the hash has changed (store/archive.ts, "Write-time dedup"); an
 * unchanged outage costs a hash lookup and a last-seen bump. A distributor
 * lists hundreds of outages and is read every few minutes, so anything here
 * that changed with every poll would insert every outage every poll. So
 * `data` holds only what the distributor says about the outage:
 *
 *   - no `status`: it is decided by the clock, not by the distributor;
 *   - nothing about the poll (no fetch time, no age);
 *   - the distributor's own update time goes in as `lastUpdated`, a name the
 *     hash leaves out (DEDUP_HASH_IGNORE), because some distributors re-stamp
 *     every outage each time they write their file;
 *   - `title` and `location_text`, which the logs page lists and searches by,
 *     are left out of the hash under those names too;
 *   - no `polygons`: an outline is hundreds of points, and it would be
 *     written again each time the customer count moved.
 *
 * `latitude` and `longitude` are what the writer finds the row's state from
 * (lib/stateMask.ts stateForRow).
 */
export function outageArchiveItems(data: unknown, fetched_at: number, source: string): ArchiveRow[] {
  if (!Array.isArray(data)) return [];
  const rows: ArchiveRow[] = [];
  for (const o of data as Outage[]) {
    if (!o || typeof o !== 'object') continue;
    const lat = num(o.lat);
    const lon = num(o.lon);
    rows.push({
      source,
      source_id: text(o.id) || null,
      fetched_at,
      lat,
      lng: lon,
      category: o.kind || null,
      subcategory: o.cause || null,
      data: {
        id: o.id,
        distributor: o.distributor,
        state: o.state,
        kind: o.kind,
        area: o.area,
        customers_affected: o.customers_affected,
        cause: o.cause,
        start_at: o.start_at,
        restore_at: o.restore_at,
        lastUpdated: o.updated_at,
        latitude: lat,
        longitude: lon,
        details: o.details,
        title: o.area || o.cause || 'Outage',
        ...(o.area ? { location_text: o.area } : {}),
      },
    });
  }
  return rows;
}

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

export interface OutageSourceDef {
  /** The distributor: its LiveStore key and the `source` its history is archived under. */
  name: OutageStore;
  intervalMs: number;
  /** Every outage under way or to come. Throws when the distributor's answer is not what it should be. */
  fetch: () => Promise<OutageRecord[]>;
  /**
   * Off unless <NAME>_DISABLED=false. For a distributor whose site refuses
   * this server: polling it would only ask, be refused and log a failure
   * every few minutes. It stays written and tested, and one env line turns
   * it on if the distributor allows the server or the refusal stops.
   */
  offByDefault?: boolean;
}

/**
 * <NAME>_DISABLED: 'true' is off, 'false' is on, anything else the source's
 * default. Read when the source registers, and by the network areas
 * (boundaries.ts) each time they are due, so one switch decides both.
 */
export const switchedOff = (name: string, offByDefault = false): boolean => {
  const v = (process.env[`${name.toUpperCase()}_DISABLED`] ?? '').trim().toLowerCase();
  return v === 'true' ? true : v === 'false' ? false : offByDefault;
};

/**
 * Register a distributor with the poller: family `power`, its records stored
 * as `Outage[]` under its name and archived one row per outage. Each record's
 * status is set as of the poll, so the store holds whole records; the merged
 * feed sets it again as of the read.
 *
 * <NAME>_DISABLED=true (AUSNET_DISABLED, WESTERNPOWER_DISABLED, …) switches a
 * distributor off, for when its upstream breaks: a dead URL is then not
 * polled every few minutes and shown as a failing source. Its saved snapshot
 * is deleted as well. LiveStore loads every saved snapshot at startup, before
 * sources register, and with polling off nothing replaces it, so the last
 * outages read before the switch would be served as current indefinitely;
 * a missing store is what the merged feed reports as "not read".
 */
export function registerOutages(def: OutageSourceDef): void {
  const { name } = def;
  if (switchedOff(name, def.offByDefault)) {
    liveStore.delete(name);
    return;
  }
  registerSource<Outage[]>({
    name,
    family: 'power',
    intervalMs: def.intervalMs,
    fetch: async () => {
      const records = await def.fetch();
      // Anything but a list is a parser gone wrong; failing the poll keeps
      // the last good snapshot.
      if (!Array.isArray(records)) throw new Error(`${name}: outages are not a list`);
      const now = Date.now();
      // One record per id, should two of a distributor's files list the same outage.
      const seen = new Set<string>();
      const outages: Outage[] = [];
      for (const r of records) {
        if (r.id) {
          if (seen.has(r.id)) continue;
          seen.add(r.id);
        }
        outages.push(withStatus(r, now));
      }
      return outages;
    },
    archiveItems: outageArchiveItems,
  });
}

/** How long a part's last good answer stands in for a failed request. */
export const PART_KEEP_MS = 30 * 60_000;
const _kept = new Map<string, { at: number; items: unknown[] }>();

/** Test hook: forget every part's last good answer. */
export function _resetKeptParts(): void {
  _kept.clear();
}

/**
 * One part of a distributor's data: a name, the request, and optionally how
 * long a good answer is reused before the request is made again (a large file
 * of planned work does not need reading every three minutes).
 */
export type OutagePart<T> = readonly [name: string, run: () => Promise<T[]>, refreshMs?: number];

/**
 * A distributor that publishes several files (unplanned, planned, future):
 * run each part's request and answer with all their records, in the order
 * the parts are given. A part that fails answers with its last good records
 * while those are under PART_KEEP_MS old, so one file that cannot be read
 * does not empty the others' outages or its own. When every part fails with nothing kept,
 * the first error is thrown: the poll fails and the snapshot held stays.
 *
 * `owner` keeps one distributor's parts apart from another's of the same
 * name. Kept in memory only: after a restart there is nothing to fall back on
 * until each part has answered once.
 */
export async function keepParts<T>(owner: string, parts: ReadonlyArray<OutagePart<T>>): Promise<T[]> {
  const started = Date.now();
  const results = await Promise.allSettled(
    parts.map(([name, run, refreshMs]) => {
      const kept = _kept.get(`${owner}:${name}`);
      // Still fresh enough not to ask again.
      if (kept && refreshMs !== undefined && started - kept.at < refreshMs) return Promise.resolve(kept.items as T[]);
      return run().then((items) => {
        if (!Array.isArray(items)) throw new Error(`${owner}:${name}: not a list`);
        _kept.set(`${owner}:${name}`, { at: Date.now(), items });
        return items;
      });
    }),
  );
  const out: T[] = [];
  let anyAnswer = false;
  let firstError: unknown = null;
  const now = Date.now();
  results.forEach((r, i) => {
    const key = `${owner}:${parts[i]![0]}`;
    if (r.status === 'fulfilled') {
      out.push(...r.value);
      anyAnswer = true;
      return;
    }
    firstError ??= r.reason;
    const kept = _kept.get(key);
    if (kept && now - kept.at < PART_KEEP_MS) {
      log.warn({ part: key, kept: kept.items.length, err: (r.reason as Error)?.message }, 'outages: a part failed; keeping its last good records');
      out.push(...(kept.items as T[]));
      anyAnswer = true;
    } else {
      log.warn({ part: key, err: (r.reason as Error)?.message }, 'outages: a part failed with nothing to keep');
    }
  });
  if (!anyAnswer) throw firstError instanceof Error ? firstError : new Error(`${owner}: every part failed`);
  return out;
}
