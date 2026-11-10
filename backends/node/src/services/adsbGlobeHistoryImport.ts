/**
 * Fill gaps in a day of aircraft history from adsb.lol's globe_history archive.
 *
 * adsb.lol publishes every aircraft's full trace for each UTC day as a GitHub
 * release (ODbL), about 14:30 AEDT the next day. Its traces are readsb's own:
 * positions as received, every few seconds, where ours are one position per
 * aircraft per poll (~20 s) plus whatever our receivers heard. This reads the
 * release once, keeps the points inside Australia, and adds to each stored
 * minute only what the minute was missing.
 *
 * "Missing" is decided against what is already stored for that minute:
 *   - a history point within NEAR_LIVE_MS of a stored point is dropped. Ours
 *     are stamped when the poll looked, not when the fix was taken, so a
 *     history point squeezed between them can sit a few seconds "behind" the
 *     stored one and make the playback step backwards;
 *   - the rest are reduced together with the stored points by the same rule
 *     the live writer uses (reduceMinute), and only the history points that
 *     survive are kept: the ones that add a turn, or fill a gap of 30 s+.
 * A minute with nothing stored at all (an outage, an aircraft the circle
 * rotation missed) gets the history reduced on its own.
 *
 * Written as seq -2 rows next to the live ones (seq 0+; -1 was the one-off
 * copy from adsb_tracks). The reader already unions every row of a minute, so
 * nothing downstream changes. A '{}' row at the day's first minute marks the
 * day imported, so a day that turned up nothing isn't downloaded again.
 *
 * Runs in a worker thread (adsbGlobeHistoryWorker.ts): gunzipping and scanning
 * the whole world's traces is a couple of CPU-minutes that must not block the
 * API's event loop.
 */
import { gunzipSync } from 'node:zlib';
import { getWriterPool } from '../db/pool.js';
import { inAuBbox } from '../lib/auBbox.js';
import { tarEntries } from '../lib/tarEntries.js';
import type { TrackPoint } from '../lib/simplifyTrail.js';
import { reduceMinute } from './adsbTrackArchive.js';

const MINUTE_MS = 60_000;
const HOUR_MS = 60 * MINUTE_MS;
export const DAY_MS = 24 * HOUR_MS;

/** The seq globe-history rows are written under. */
export const GLOBE_HISTORY_SEQ = -2;

/** A history point this close to a stored one adds nothing but jitter. */
export const NEAR_LIVE_MS = 10_000;

/** Source id the filled points are attributed to. */
const SOURCE_ID = 'adsb_lol';

/** Abort the download when no bytes arrive for this long. */
const STALL_MS = 2 * 60_000;

const USER_AGENT = 'AusAware/1.0 (+https://nswpsn.forcequit.xyz)';

/** `./traces/3c/trace_full_7c6b3c.json` → `7c6b3c`; null for anything else. */
export function traceFileHex(name: string): string | null {
  const m = /(?:^|\/)traces\/[0-9a-f]{2}\/trace_full_(~?[0-9a-f]{6})\.json$/.exec(name);
  return m ? m[1]! : null;
}

/**
 * Cheap test on the raw JSON for any point at Australian latitude and
 * longitude, so the ~99% of the world's traces that never come near are not
 * parsed. A superset of the bbox (-9.x to -45.x, 110-155): the parsed points
 * are filtered exactly afterwards. readsb writes one point per line as
 * `[secs,lat,lon,...`.
 */
const AU_HINT = /\[[0-9.]+,-(?:9|[1-3]\d|4[0-5])\.\d+,1(?:1\d|[2-4]\d|5[0-5])\.\d/;
export function mightTouchAustralia(text: string): boolean {
  return AU_HINT.test(text);
}

export interface HistoryAircraftMinute {
  c: string | null;
  r: string | null;
  t: string | null;
  points: TrackPoint[];
}

/** minute start (ms) → hex → what the archive has for it. */
export type HistoryDay = Map<number, Map<string, HistoryAircraftMinute>>;

interface Trace {
  icao?: string;
  r?: string;
  t?: string;
  timestamp?: number;
  trace?: unknown[];
}

/**
 * Add one aircraft's trace to the day, Australian points only. A trace point
 * is [secs after `timestamp`, lat, lon, alt|"ground"|null, gs, track, flags,
 * vrate, details|null, ...]; the callsign is in `details.flight`, sent every
 * so often and carried forward until it changes.
 */
export function addTrace(
  day: HistoryDay,
  hex: string,
  trace: Trace,
  dayStartMs: number,
): number {
  if (!Array.isArray(trace.trace) || typeof trace.timestamp !== 'number') return 0;
  const baseMs = trace.timestamp * 1000;
  const reg = typeof trace.r === 'string' && trace.r.trim() ? trace.r.trim() : null;
  const type = typeof trace.t === 'string' && trace.t.trim() ? trace.t.trim() : null;
  let callsign: string | null = null;
  let added = 0;
  for (const raw of trace.trace) {
    if (!Array.isArray(raw)) continue;
    const details = raw[8] as { flight?: unknown } | null | undefined;
    if (details && typeof details.flight === 'string' && details.flight.trim()) {
      callsign = details.flight.trim();
    }
    const [secs, lat, lon, alt] = raw as [unknown, unknown, unknown, unknown];
    if (typeof secs !== 'number' || typeof lat !== 'number' || typeof lon !== 'number') continue;
    const tMs = Math.round(baseMs + secs * 1000);
    if (tMs < dayStartMs || tMs >= dayStartMs + DAY_MS) continue;
    if (!inAuBbox(lat, lon)) continue;
    const minute = Math.floor(tMs / MINUTE_MS) * MINUTE_MS;
    let byHex = day.get(minute);
    if (!byHex) {
      byHex = new Map();
      day.set(minute, byHex);
    }
    let e = byHex.get(hex);
    if (!e) {
      e = { c: null, r: reg, t: type, points: [] };
      byHex.set(hex, e);
    }
    if (callsign) e.c = callsign;
    e.points.push([tMs, lat, lon, typeof alt === 'number' ? alt : null]);
    added += 1;
  }
  return added;
}

/**
 * The history points worth adding to one aircraft-minute, given what is
 * already stored for it (see the module comment).
 */
export function fillFromHistory(stored: readonly TrackPoint[], history: readonly TrackPoint[]): TrackPoint[] {
  const candidates = history.filter((h) => !stored.some((s) => Math.abs(s[0] - h[0]) < NEAR_LIVE_MS));
  if (candidates.length === 0) return [];
  if (stored.length === 0) return reduceMinute(candidates);
  const fromHistory = new Set(candidates);
  return reduceMinute([...stored, ...candidates]).filter((p) => fromHistory.has(p));
}

const r5 = (v: number): number => Math.round(v * 1e5) / 1e5;

interface StoredEntry {
  p?: Array<[number, number, number, number | null]> | null;
}

/**
 * One hour's rows to insert: minute → json. `stored` is that hour's existing
 * rows (every seq but ours), as the reader would see them.
 */
export function buildHourRows(
  day: HistoryDay,
  hourStartMs: number,
  stored: ReadonlyArray<{ minute: Date; aircraft: unknown }>,
): Array<{ minuteMs: number; json: Record<string, unknown>; points: number }> {
  const storedPts = new Map<number, Map<string, TrackPoint[]>>();
  for (const row of stored) {
    const m = row.minute.getTime();
    if (!row.aircraft || typeof row.aircraft !== 'object') continue;
    let byHex = storedPts.get(m);
    if (!byHex) {
      byHex = new Map();
      storedPts.set(m, byHex);
    }
    for (const [hex, entry] of Object.entries(row.aircraft as Record<string, StoredEntry>)) {
      const list = byHex.get(hex) ?? [];
      for (const p of entry?.p ?? []) list.push([m + p[0], p[1], p[2], p[3]]);
      byHex.set(hex, list);
    }
  }

  const out: Array<{ minuteMs: number; json: Record<string, unknown>; points: number }> = [];
  for (let m = hourStartMs; m < hourStartMs + HOUR_MS; m += MINUTE_MS) {
    const hist = day.get(m);
    if (!hist) continue;
    const json: Record<string, unknown> = {};
    let points = 0;
    for (const [hex, e] of hist) {
      const add = fillFromHistory(storedPts.get(m)?.get(hex) ?? [], e.points);
      if (add.length === 0) continue;
      json[hex] = {
        c: e.c,
        r: e.r,
        t: e.t,
        e: null,
        s: [SOURCE_ID],
        p: add.map((p) => [p[0] - m, r5(p[1]), r5(p[2]), p[3] === null ? null : Math.round(p[3])]),
      };
      points += add.length;
    }
    if (points > 0) out.push({ minuteMs: m, json, points });
  }
  return out;
}

/** The release's parts, in order, as one byte stream. */
async function* download(urls: readonly string[], signal: AbortSignal): AsyncGenerator<Uint8Array> {
  for (const url of urls) {
    const ac = new AbortController();
    const onAbort = (): void => ac.abort(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    let stall = setTimeout(() => ac.abort(new Error('download stalled')), STALL_MS);
    try {
      const res = await fetch(url, { headers: { 'User-Agent': USER_AGENT }, signal: ac.signal });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status} for ${url}`);
      for await (const chunk of res.body as unknown as AsyncIterable<Uint8Array>) {
        clearTimeout(stall);
        stall = setTimeout(() => ac.abort(new Error('download stalled')), STALL_MS);
        yield chunk;
      }
    } finally {
      clearTimeout(stall);
      signal.removeEventListener('abort', onAbort);
    }
  }
}

export interface GlobeImportResult {
  traceFiles: number;
  australianAircraft: number;
  historyPoints: number;
  minutesWritten: number;
  pointsWritten: number;
}

/** Read one day's release and write what it adds. Idempotent. */
export async function importGlobeHistoryDay(
  dayStartMs: number,
  assetUrls: readonly string[],
  signal: AbortSignal = new AbortController().signal,
): Promise<GlobeImportResult> {
  const pool = await getWriterPool();
  if (!pool) throw new Error('no database configured');

  const day: HistoryDay = new Map();
  const aircraft = new Set<string>();
  let traceFiles = 0;
  let historyPoints = 0;
  for await (const f of tarEntries(download(assetUrls, signal), (n) => traceFileHex(n) !== null)) {
    traceFiles += 1;
    const hex = traceFileHex(f.name)!;
    const raw = f.data[0] === 0x1f && f.data[1] === 0x8b ? gunzipSync(f.data) : f.data;
    const text = raw.toString('utf8');
    if (!mightTouchAustralia(text)) continue;
    let trace: Trace;
    try {
      trace = JSON.parse(text) as Trace;
    } catch {
      continue;
    }
    const n = addTrace(day, hex, trace, dayStartMs);
    if (n > 0) {
      aircraft.add(hex);
      historyPoints += n;
    }
  }
  if (traceFiles === 0) throw new Error('no trace files in the release');

  let minutesWritten = 0;
  let pointsWritten = 0;
  for (let h = dayStartMs; h < dayStartMs + DAY_MS; h += HOUR_MS) {
    const stored = await pool.query<{ minute: Date; aircraft: unknown }>(
      `SELECT minute, aircraft FROM adsb_track_chunks
        WHERE minute >= $1::timestamptz AND minute < $2::timestamptz AND seq <> $3`,
      [new Date(h).toISOString(), new Date(h + HOUR_MS).toISOString(), GLOBE_HISTORY_SEQ],
    );
    const rows = buildHourRows(day, h, stored.rows);
    for (let m = h; m < h + HOUR_MS; m += MINUTE_MS) day.delete(m);
    if (rows.length === 0) continue;
    const res = await pool.query(
      `INSERT INTO adsb_track_chunks (minute, seq, aircraft)
       SELECT u.minute, $3, u.aircraft
         FROM unnest($1::timestamptz[], $2::json[]) AS u(minute, aircraft)
       ON CONFLICT (minute, seq) DO NOTHING`,
      [
        rows.map((r) => new Date(r.minuteMs).toISOString()),
        rows.map((r) => JSON.stringify(r.json)),
        GLOBE_HISTORY_SEQ,
      ],
    );
    minutesWritten += res.rowCount ?? 0;
    // Counted as offered: on a redo the conflicting minutes add none.
    pointsWritten += rows.reduce((a, r) => a + r.points, 0);
  }

  // Last, so a run cut short is redone; the rows above are DO NOTHING on a redo.
  await pool.query(
    `INSERT INTO adsb_track_chunks (minute, seq, aircraft)
     VALUES ($1::timestamptz, $2, '{}'::json)
     ON CONFLICT (minute, seq) DO NOTHING`,
    [new Date(dayStartMs).toISOString(), GLOBE_HISTORY_SEQ],
  );

  return {
    traceFiles,
    australianAircraft: aircraft.size,
    historyPoints,
    minutesWritten,
    pointsWritten,
  };
}
