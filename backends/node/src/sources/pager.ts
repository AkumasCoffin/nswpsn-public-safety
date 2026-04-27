/**
 * Pager messages — proxy to the self-hosted Pagermon `/api/messages`
 * endpoint, with the same coordinate / incident-id parsing the Python
 * backend's `_prewarm_fetch_pager` does at external_api_proxy.py:4262.
 *
 * Output shape mirrors Python's prewarm: a flat list of message
 * objects, each with normalised id / incident_id / capcode / lat / lon
 * fields. The HTTP route handler (`api/pager.ts`) wraps these into a
 * GeoJSON FeatureCollection in the same shape Python's
 * `/api/pager/hits` returns at line 12449.
 *
 * If PAGERMON_URL is unset, the source still registers but the fetcher
 * returns an empty snapshot — same fallback the Python implementation
 * uses when the env var is missing.
 */
import { fetchJson } from './shared/http.js';
import { config } from '../config.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';
import { log } from '../lib/log.js';

export interface PagerMessage {
  id: number | string;
  incident_id: string;
  capcode: string;
  alias: string;
  agency: string;
  source: string;
  message: string;
  lat: number;
  lon: number;
  /** ISO 8601 string built from upstream `timestamp` (unix seconds). */
  incident_time: string | null;
  /** Original upstream unix timestamp; preserved so the route handler
   *  can apply `?hours=` filtering without re-parsing the ISO string. */
  timestamp: number | null;
}

export interface PagerSnapshot {
  messages: PagerMessage[];
  count: number;
}

const EMPTY: PagerSnapshot = { messages: [], count: 0 };

/** Browser-ish headers — Pagermon installs sometimes 403 plain
 *  requests, mirroring the Python fetcher's headers. */
const PAGER_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  Accept: 'application/json, text/plain, */*',
  'Accept-Language': 'en-US,en;q=0.9',
};

/** Extract `[lon, lat]` style coords from a pager message body. Returns
 *  `[lat, lon]` to match Python's _parse_pager_coords. */
export function parsePagerCoords(message: string): [number | null, number | null] {
  const text = message || '';
  let m = text.match(/\[(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)\]/);
  if (!m) {
    m = text.match(/\[?(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)(?:\]|\s|$)/);
  }
  if (!m || !m[1] || !m[2]) return [null, null];
  const lon = Number(m[1]);
  const lat = Number(m[2]);
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return [null, null];
  return [lat, lon];
}

/** Pull an incident ID like `25-139605` or `0053-6653` from a message
 *  body. Mirrors Python's _parse_pager_incident_id at line 4239. */
export function parsePagerIncidentId(message: string): string | null {
  let text = (message || '').trim();
  // Strip zero-width / directional characters.
  text = text.replace(/[‎‏‪‬­]/g, '');
  // Normalise dash variants to plain ASCII hyphen.
  text = text.replace(/[‐-―−⁃]/g, '-');
  const long = text.match(/\b(\d{2}-\d{6})\b/);
  if (long?.[1]) return long[1];
  const short = text.match(/\b(\d{4}-\d{4})\b/);
  if (short?.[1]) return short[1];
  return null;
}

interface RawPagerMsg {
  id?: unknown;
  message?: unknown;
  address?: unknown;
  alias?: unknown;
  agency?: unknown;
  source?: unknown;
  timestamp?: unknown;
}

function asString(v: unknown): string {
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return '';
}

export async function fetchPager(): Promise<PagerSnapshot> {
  const base = config.PAGERMON_URL;
  if (!base || !/^https?:/i.test(base)) {
    return EMPTY;
  }

  const apiKey = config.PAGERMON_API_KEY;
  const url = apiKey
    ? `${base}?apikey=${encodeURIComponent(apiKey)}&limit=100`
    : `${base}?limit=100`;

  const data = await fetchJson<{ messages?: RawPagerMsg[] }>(url, {
    headers: PAGER_HEADERS,
  });
  const messages = Array.isArray(data?.messages) ? data.messages : [];

  // Two-pass: first parse coords/ids, then group by incident_id so
  // every message in an incident inherits whichever message in the
  // group had explicit coords. Mirrors the python implementation.
  interface Enriched {
    msg: RawPagerMsg;
    incident_id: string | null;
    lat: number | null;
    lon: number | null;
  }
  const enriched: Enriched[] = [];
  for (const msg of messages) {
    const text = asString(msg.message);
    const id = parsePagerIncidentId(text);
    const [lat, lon] = parsePagerCoords(text);
    enriched.push({ msg, incident_id: id, lat, lon });
  }

  const groups = new Map<string, Enriched[]>();
  for (const item of enriched) {
    let inc = item.incident_id;
    if (!inc) {
      // No real id — synthesise one from timestamp + capcode if we have
      // coords on this row. Otherwise drop.
      if (item.lat !== null && item.lon !== null) {
        const ts = typeof item.msg.timestamp === 'number' ? item.msg.timestamp : 0;
        const cap = asString(item.msg.address) || 'nocap';
        inc = `noid-${ts}-${cap}`;
        item.incident_id = inc;
      } else {
        continue;
      }
    }
    const list = groups.get(inc);
    if (list) list.push(item);
    else groups.set(inc, [item]);
  }

  const out: PagerMessage[] = [];
  for (const [incId, items] of groups) {
    let canonLat: number | null = null;
    let canonLon: number | null = null;
    for (const it of items) {
      if (it.lat !== null && it.lon !== null) {
        canonLat = it.lat;
        canonLon = it.lon;
        break;
      }
    }
    if (canonLat === null || canonLon === null) continue;

    for (const it of items) {
      const m = it.msg;
      const pagerMsgId = m.id;
      if (pagerMsgId === undefined || pagerMsgId === null) continue;
      const tsRaw = m.timestamp;
      let incidentTime: string | null = null;
      let tsNum: number | null = null;
      if (typeof tsRaw === 'number' && Number.isFinite(tsRaw)) {
        tsNum = tsRaw;
        try {
          incidentTime = new Date(tsRaw * 1000).toISOString();
        } catch {
          incidentTime = null;
        }
      }
      out.push({
        id: typeof pagerMsgId === 'string' || typeof pagerMsgId === 'number' ? pagerMsgId : String(pagerMsgId),
        incident_id: incId,
        capcode: asString(m.address),
        alias: asString(m.alias),
        agency: asString(m.agency),
        source: asString(m.source),
        message: asString(m.message),
        lat: canonLat,
        lon: canonLon,
        incident_time: incidentTime,
        timestamp: tsNum,
      });
    }
  }

  return { messages: out, count: out.length };
}

let _missingUrlLogged = false;

export default function register(): void {
  if (!config.PAGERMON_URL) {
    if (!_missingUrlLogged) {
      log.info('PAGERMON_URL not configured; pager source will return empty snapshots');
      _missingUrlLogged = true;
    }
  }
  registerSource<PagerSnapshot>({
    name: 'pager',
    family: 'misc',
    intervalActiveMs: 60_000,
    intervalIdleMs: 120_000,
    fetch: fetchPager,
  });
}

export function pagerSnapshot(): PagerSnapshot {
  return liveStore.getData<PagerSnapshot>('pager') ?? EMPTY;
}
