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
import type { ArchiveRow } from '../store/archive.js';
import { log } from '../lib/log.js';
import { formatSydneyNaive } from '../lib/sydneyTime.js';
import { learnAliasesFromMessages } from '../services/capcodeAliasSync.js';
import { capcodeAliases, normalizeCapcode } from '../api/node-data.js';

export interface PagerMessage {
  id: number | string;
  incident_id: string;
  capcode: string;
  alias: string;
  agency: string;
  source: string;
  message: string;
  /** Incident TYPE parsed from the body (e.g. "Bush Fire", "AFA", "MVA"). */
  type: string;
  /** Call class token following the type ("FIRECALL", "INCIDENT CALL",
   *  "CFR CALL", "TURNOUT" for FRNSW headers). Empty when absent. */
  call_class: string;
  /** Address / location segment of the body, coords + decorations removed. */
  address_text: string;
  /** True when this page is a Stop Message / Stand Down / NNTA. Flag only —
   *  the incident keeps its pin (per the product decision). */
  is_stop: boolean;
  /** null when the incident has no coordinates (e.g. FRNSW FRINC turnouts).
   *  Coordless messages are archived (logs page) but never mapped. */
  lat: number | null;
  lon: number | null;
  /** Naive Sydney-local datetime string (YYYY-MM-DDTHH:mm:ss) built
   *  from upstream `timestamp` (unix seconds). Matches python's
   *  `datetime.fromtimestamp(ts).isoformat()` output exactly. */
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
  // FRNSW turnout id: `INC: 146685-28072026` (6 digits - ddmmyyyy). Distinct
  // format from the RFS ids below, so no collision; keep it whole so every
  // FRINC page for the same turnout groups together.
  const frinc = text.match(/\bINC:\s*(\d{5,6}-\d{8})\b/i);
  if (frinc?.[1]) return frinc[1];
  const long = text.match(/\b(\d{2}-\d{6})\b/);
  if (long?.[1]) return long[1];
  const short = text.match(/\b(\d{4}-\d{4})\b/);
  if (short?.[1]) return short[1];
  return null;
}

// --- Structured body parsing ----------------------------------------------
// The pager body carries far more than an id + coords. NSW RFS/FRNSW detail
// lines look like:
//   [date] CAP - 26-121910 - MVA - INCIDENT CALL - <address> - [lon,lat]
//   CAP - 26-121910 - MVA - <address> - [lon,lat]        (no call class)
// and FRNSW turnout headers like:
//   FRINC TYPE: AFA TURNOUT: 405 INC: 146685-28072026    (no coords)
// The helpers below extract TYPE / call class / address / stop-intent /
// agency so the map, logs and API all share one canonical parse.

/** Stop Message / Stand Down / NNTA detector across every variant seen in the
 *  feed (`... Stop Message // ...`, `STOP MESSAGE - ...`, `STOP MSG`,
 *  inline `- STOP MESSAGE -`, `STAND DOWN`, `NNTA`, "no need to attend"). */
const STOP_RE =
  /\bstop\s*(?:message|msg)\b|\bstand\s*down\b|\bnnta\b|\bno need to attend\b/i;

/** Call-class tokens that can follow the incident TYPE in a detail line. */
const CALL_CLASS_RE =
  /^(FIRECALL|INCIDENT CALL|CFR CALL|STRUCTURE CALL|MEDICAL|RESCUE|[A-Z]{2,}(?: [A-Z]{2,})? CALL)$/i;

/** Remove zero-width chars, trailing `[lon,lat]` (even if truncated) and a
 *  leading `DD Month YYYY HH:MM:SS` / `HH:MM:SS` timestamp, so the remainder
 *  is just the `CAP - ID - TYPE - ...` payload. */
function stripBodyDecorations(message: string): string {
  let t = (message || '').replace(/[‎‏‪‬­]/g, '');
  t = t.replace(/[‐-―−⁃]/g, '-');
  t = t.trim();
  t = t.replace(/\s*\[[\d.,\-\s]*\]?\s*$/, '').trim(); // trailing coords
  t = t.replace(/\s*-\s*$/, '').trim(); // dangling `-` separator left by coord removal
  t = t.replace(/^\d{1,2}\s+[A-Za-z]+\s+\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+/, '');
  t = t.replace(/^\d{1,2}:\d{2}:\d{2}\s+/, '');
  return t.trim();
}

export function parsePagerStop(message: string): boolean {
  return STOP_RE.test(message || '');
}

/** A service tag that can sit in the TYPE slot ahead of the real type, e.g.
 *  `... - 26-121994 - VRA - ROAD CRASH RESCUE - addr` (VRA rescue units). When
 *  present it's the agency marker, not the incident type — skip it. */
const SERVICE_TAG_RE = /^(VRA|RFS|NSWRFS|FRNSW|SES|NSWAS|POLICE|NSWPF)$/i;

/** Locate the body parts and the index of the incident TYPE segment. The id
 *  must be its OWN segment (not embedded in stop free-text); the type sits one
 *  segment later, unless a service tag (VRA/…) occupies that slot first. */
function locateBody(message: string): { parts: string[]; typeIdx: number } {
  const parts = stripBodyDecorations(message)
    .split(/\s+-\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
  // Only treat a segment that IS the id (not one that merely contains it) as
  // the split point. In the standard detail line the id is its own ` - `
  // segment; in a Stop Message the id is embedded in a larger segment
  // (`26-121998 CVGRACI1 Stop Message // ...`), which must NOT yield a bogus
  // "type" from the trailing free text.
  const idIdx = parts.findIndex(
    (p) => /^\d{2}-\d{6}$/.test(p) || /^\d{4}-\d{4}$/.test(p),
  );
  if (idIdx < 0) return { parts, typeIdx: -1 };
  let typeIdx = idIdx + 1;
  if (SERVICE_TAG_RE.test(parts[typeIdx] ?? '')) typeIdx += 1;
  return { parts, typeIdx };
}

/** Extract incident TYPE + call class from a body. Returns empty strings when
 *  the body has no recognisable `- ID - TYPE -` structure (stop-only pages,
 *  free text). FRNSW `FRINC TYPE: X TURNOUT:` headers yield the type with a
 *  synthetic "TURNOUT" call class. */
export function parsePagerType(message: string): { type: string; callClass: string } {
  const raw = message || '';
  const frinc = raw.match(/\bFRINC\b.*?TYPE:\s*(.+?)\s+TURNOUT:/i);
  if (frinc?.[1]) return { type: frinc[1].replace(/\s+/g, ' ').trim(), callClass: 'TURNOUT' };

  const { parts, typeIdx } = locateBody(raw);
  const type = typeIdx >= 0 ? (parts[typeIdx] ?? '') : '';
  // A "type" that is itself stop language (`STOP -STAND DOWN`) is not a real
  // incident type — the page is a stop notice, not a dispatch.
  if (!type || STOP_RE.test(type)) return { type: '', callClass: '' };
  const next = parts[typeIdx + 1] ?? '';
  const callClass = CALL_CLASS_RE.test(next) ? next.toUpperCase() : '';
  return { type, callClass };
}

/** Extract the address/location segment(s): everything after the TYPE (and the
 *  optional call class), with the trailing coords already removed. */
export function parsePagerAddress(message: string): string {
  const { parts, typeIdx } = locateBody(message);
  if (typeIdx < 0) return '';
  let addrIdx = typeIdx + 1; // skip the type
  if (CALL_CLASS_RE.test(parts[addrIdx] ?? '')) addrIdx += 1; // skip call class
  return parts.slice(addrIdx).join(' - ').trim();
}

/** Normalise a free-form agency string to a canonical NSW service code. */
function normaliseAgency(a: string): string {
  const s = (a || '').trim();
  if (!s) return '';
  const u = s.toUpperCase();
  if (/RFS|RURAL FIRE/.test(u)) return 'NSWRFS';
  if (/FRNSW|FIRE\s*(?:&|AND)?\s*RESCUE|^FIRE$/.test(u)) return 'FRNSW';
  if (/AMBUL|NSWAS|PARAMED/.test(u)) return 'NSWAS';
  if (/\bSES\b|STATE EMERGENCY/.test(u)) return 'SES';
  if (/POLICE|NSWPF/.test(u)) return 'POLICE';
  return s;
}

/** Infer the responding agency from the body's format when Pagermon hasn't
 *  tagged it. FRNSW turnout headers → FRNSW; an RFS incident id → NSWRFS;
 *  otherwise fall back to a normalised upstream tag. */
export function inferPagerAgency(message: string, upstream: string): string {
  const raw = message || '';
  if (/\bFRINC\b|\bTURNOUT:/i.test(raw)) return 'FRNSW';
  // Respect an explicit upstream (Pagermon) tag before guessing from the body.
  const up = normaliseAgency(upstream);
  if (up) return up;
  // SES Zone rescue callouts use `SEZ<area>` capcodes and rescue mnemonics
  // (GLR/RCR/VRA). Detect them before the RFS id check — some carry a short
  // `0055-xxxx` id but are not RFS jobs.
  if (/\bSEZ[A-Z]{2,}\b|\bSES\b|\bSES\d/.test(raw)) return 'SES';
  if (/\bVRA\b/.test(raw)) return 'VRA';
  // An RFS incident id (26-xxxxxx) is the strongest RFS signal.
  if (/\b\d{2}-\d{6}\b/.test(raw)) return 'NSWRFS';
  // A police assistance callout with no rescue-service signal above.
  if (/\bNSWPF\b|\bNSW POLICE\b/.test(raw)) return 'POLICE';
  return '';
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
    // Canonical coords = first coord-bearing message in the group; may stay
    // null (e.g. an FRNSW FRINC turnout group has no coords at all). Coordless
    // groups are still emitted so they reach the archive/logs — the map paths
    // skip null coords, so they never draw a pin.
    let canonLat: number | null = null;
    let canonLon: number | null = null;
    for (const it of items) {
      if (it.lat !== null && it.lon !== null) {
        canonLat = it.lat;
        canonLon = it.lon;
        break;
      }
    }

    for (const it of items) {
      const m = it.msg;
      const pagerMsgId = m.id;
      if (pagerMsgId === undefined || pagerMsgId === null) continue;
      const tsRaw = m.timestamp;
      let incidentTime: string | null = null;
      let tsNum: number | null = null;
      if (typeof tsRaw === 'number' && Number.isFinite(tsRaw)) {
        tsNum = tsRaw;
        // Mirror python's `datetime.fromtimestamp(int(ts)).isoformat()`
        // (line 4363): naive Sydney-local-time string with no offset.
        // Earlier revisions emitted `.toISOString()` which produced a
        // UTC `Z` string; consumers reading it as wall-clock then
        // displayed the Sydney time 10–11h in the future.
        try {
          incidentTime = formatSydneyNaive(tsRaw * 1000);
        } catch {
          incidentTime = null;
        }
      }
      const body = asString(m.message);
      const { type, callClass } = parsePagerType(body);
      out.push({
        id: typeof pagerMsgId === 'string' || typeof pagerMsgId === 'number' ? pagerMsgId : String(pagerMsgId),
        incident_id: incId,
        capcode: asString(m.address),
        // Pagermon usually names the brigade itself. When it doesn't, fall
        // back to the operator's exported capcode map so the message still
        // carries a readable name — which is also what makes it findable by
        // name in the logs search (title projects from alias).
        alias: asString(m.alias) || capcodeAliasFor(asString(m.address)),
        agency: inferPagerAgency(body, asString(m.agency)),
        source: asString(m.source),
        message: body,
        type,
        call_class: callClass,
        address_text: parsePagerAddress(body),
        is_stop: parsePagerStop(body),
        lat: canonLat,
        lon: canonLon,
        incident_time: incidentTime,
        timestamp: tsNum,
      });
    }
  }

  // Teach the capcode->alias table what this poll saw. Pagermon owns the
  // mapping and every message carries it, so this keeps the staff Data tab's
  // alias lookup current without anyone re-exporting a CSV. Fire-and-forget:
  // a failure here must never break the pager snapshot.
  void learnAliasesFromMessages(out);

  return { messages: out, count: out.length };
}

/**
 * Brigade/unit name for a capcode from the operator's exported alias CSV.
 * Display-only fallback: returns '' when the capcode isn't in the export,
 * leaving the existing type/agency fallback chain to label the row.
 */
function capcodeAliasFor(capcode: string): string {
  if (!capcode) return '';
  try {
    return capcodeAliases().get(normalizeCapcode(capcode))?.alias ?? '';
  } catch {
    return '';
  }
}

let _missingUrlLogged = false;

/**
 * Per-message archive fan-out. The pager snapshot is `{ messages, count }`
 * — neither a GeoJSON FeatureCollection nor a flat array, so the default
 * extractor would store the whole snapshot as a single wrapper row with
 * null title / lat / lng / etc. (the "Unknown" entry at the top of the
 * logs page dropdown). Emitting one row per PagerMessage gives each
 * brigade a real title (alias) + agency category + capcode subcategory,
 * matching what the python writer used to produce.
 */
function pagerArchiveItems(
  data: unknown,
  fetched_at: number,
  source: string,
): ArchiveRow[] {
  const snap = (data as PagerSnapshot | null) ?? null;
  if (!snap || !Array.isArray(snap.messages)) return [];
  const out: ArchiveRow[] = [];
  for (const msg of snap.messages) {
    if (!msg || typeof msg !== 'object') continue;
    out.push({
      source,
      source_id: msg.id !== undefined && msg.id !== null ? String(msg.id) : null,
      fetched_at,
      lat: typeof msg.lat === 'number' && Number.isFinite(msg.lat) ? msg.lat : null,
      lng: typeof msg.lon === 'number' && Number.isFinite(msg.lon) ? msg.lon : null,
      category: msg.agency || null,
      subcategory: msg.capcode || null,
      // `data->>'title'` projects to the row's title in /api/data/history;
      // alias is the human-readable brigade/unit name. Without this the
      // logs page renders every pager row as "Unknown". Fall back to the
      // parsed type (e.g. FRNSW FRINC turnouts have no alias) then agency.
      data: { ...msg, title: msg.alias || msg.type || msg.agency || '' },
    });
  }
  return out;
}

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
    intervalMs: 60_000,
    fetch: fetchPager,
    archiveItems: pagerArchiveItems,
  });
}

export function pagerSnapshot(): PagerSnapshot {
  return liveStore.getData<PagerSnapshot>('pager') ?? EMPTY;
}
