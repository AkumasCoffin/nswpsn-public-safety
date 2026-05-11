/**
 * POST /api/waze/ingest — userscript pushes scraped Waze georss here.
 *
 * Replaces the Python `waze_ingest` endpoint. Behaviour:
 *   - Writes the bbox-keyed snapshot to LiveStore via WazeIngestCache
 *     (in-memory; backs the live /api/waze/* endpoints).
 *   - Fans each alert / jam out to one append-only row in archive_waze
 *     so /api/data/history and the police-heatmap aggregation see
 *     fresh data. Source tag matches python's split:
 *       waze_police   — POLICE-typed alerts
 *       waze_roadwork — CONSTRUCTION-typed alerts
 *       waze_hazard   — everything else (HAZARD / ACCIDENT / etc.)
 *       waze_jam      — jam line features
 *     Bypasses ARCHIVE_WAZE feature flag because the new partitioned
 *     archive_waze table is what every downstream consumer queries.
 *
 * Auth: X-Ingest-Key matched against WAZE_INGEST_KEY env var.
 */
import { Hono } from 'hono';
import { requireIngestKey } from '../services/auth/ingestKey.js';
import { WazeIngestPayloadSchema, type WazeAlert, type WazeJam } from '../types/waze.js';
import { ingest } from '../store/wazeIngestCache.js';
import { archiveWriter, type ArchiveRow } from '../store/archive.js';
import {
  isPoliceAlert,
  isRoadworkAlert,
  isHazardAlert,
} from '../services/wazeAlerts.js';
import { log } from '../lib/log.js';

export const wazeIngestRouter = new Hono();

function alertSource(a: WazeAlert): string | null {
  if (isPoliceAlert(a)) return 'waze_police';
  if (isRoadworkAlert(a)) return 'waze_roadwork';
  if (isHazardAlert(a)) return 'waze_hazard';
  // Bare-type 'POLICE' / 'CONSTRUCTION' / 'HAZARD' alerts are caught
  // above; anything else (e.g. JAM-typed alerts that aren't already
  // classified) we drop because the live read path doesn't surface
  // them and archiving would just bloat the table.
  return null;
}

function alertLatLng(a: WazeAlert): { lat: number | null; lng: number | null } {
  const loc = a.location ?? {};
  const lat =
    typeof loc.y === 'number'
      ? loc.y
      : typeof loc.latitude === 'number'
        ? loc.latitude
        : typeof a.lat === 'number'
          ? a.lat
          : null;
  const lng =
    typeof loc.x === 'number'
      ? loc.x
      : typeof loc.longitude === 'number'
        ? loc.longitude
        : typeof a.lon === 'number'
          ? a.lon
          : null;
  return { lat, lng };
}

function jamCenter(j: WazeJam): { lat: number | null; lng: number | null } {
  const j0 = j as Record<string, unknown>;
  const line = j0['line'];
  if (!Array.isArray(line) || line.length === 0) {
    return { lat: null, lng: null };
  }
  // Use the midpoint of the polyline so a jam pin lands on the road
  // rather than at one of its arbitrary endpoints.
  const mid = line[Math.floor(line.length / 2)] as Record<string, unknown> | undefined;
  if (!mid || typeof mid !== 'object') return { lat: null, lng: null };
  const lat = typeof mid['y'] === 'number' ? (mid['y'] as number) : null;
  const lng = typeof mid['x'] === 'number' ? (mid['x'] as number) : null;
  return { lat, lng };
}

function archiveAlertsAndJams(alerts: WazeAlert[], jams: WazeJam[]): number {
  const fetchedAt = Math.floor(Date.now() / 1000);
  let queued = 0;
  for (const a of alerts) {
    const source = alertSource(a);
    if (!source) continue;
    const { lat, lng } = alertLatLng(a);
    if (lat === null || lng === null) continue;
    const raw = a as Record<string, unknown>;
    const rawSubtype = typeof raw['subtype'] === 'string' ? (raw['subtype'] as string).trim() : '';
    const type = typeof raw['type'] === 'string' ? (raw['type'] as string) : null;
    // Default-to-POLICE_VISIBLE policy: Waze sometimes ships bare
    // type='POLICE' alerts with no subtype. Python's heatmap and the
    // user-confirmed convention treat these as visible police, not
    // unclassified. Apply the default at write time so every
    // downstream read (filters, heatmap, /api/data/history) sees a
    // consistent value without needing per-query COALESCE chains.
    let subcategory: string | null;
    if (rawSubtype) {
      subcategory = rawSubtype;
    } else if (source === 'waze_police') {
      subcategory = 'POLICE_VISIBLE';
    } else {
      subcategory = type ?? null;
    }
    const row: ArchiveRow = {
      source,
      source_id: String(a.uuid ?? a.id ?? '') || null,
      fetched_at: fetchedAt,
      lat,
      lng,
      category: type ?? null,
      subcategory,
      // The data blob mirrors what defaultArchiveItems would produce
      // for a GeoJSON Feature.properties: top-level title/subtype/etc
      // so dataHistoryQuery's JSONB projection finds them.
      data: a as unknown as Record<string, unknown>,
    };
    archiveWriter.push('archive_waze', row);
    queued += 1;
  }
  for (const j of jams) {
    const { lat, lng } = jamCenter(j);
    if (lat === null || lng === null) continue;
    const row: ArchiveRow = {
      source: 'waze_jam',
      source_id: String(j.uuid ?? j.id ?? '') || null,
      fetched_at: fetchedAt,
      lat,
      lng,
      category: 'JAM',
      subcategory: null,
      data: j as unknown as Record<string, unknown>,
    };
    archiveWriter.push('archive_waze', row);
    queued += 1;
  }
  return queued;
}

wazeIngestRouter.post('/api/waze/ingest', requireIngestKey, async (c) => {
  let body: unknown;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: 'bad json' }, 400);
  }
  const parsed = WazeIngestPayloadSchema.safeParse(body);
  if (!parsed.success) {
    // Surface the failure shape so userscript drift is greppable in
    // the log instead of just a wall of `400` lines. Zod issues are
    // small (path + message) so logging them at warn-once is cheap.
    log.warn(
      { issues: parsed.error.issues.slice(0, 3) },
      'waze ingest 400 — schema rejected payload',
    );
    return c.json(
      { error: 'bad payload', issues: parsed.error.issues },
      400,
    );
  }
  const result = ingest(parsed.data);
  // Mirror to the partitioned archive table so historical / heatmap
  // queries see fresh data. The archive writer is async-batched so
  // this doesn't block the ingest response.
  let archived = 0;
  try {
    archived = archiveAlertsAndJams(parsed.data.alerts, parsed.data.jams);
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'waze ingest archive enqueue failed',
    );
  }
  log.debug(
    {
      bbox: result.bboxKey,
      alerts: result.alerts,
      jams: result.jams,
      regions: result.regions,
      archived,
    },
    'waze ingest',
  );
  return c.json({
    ok: true,
    regions_cached: result.regions,
    received: { alerts: result.alerts, jams: result.jams },
    archived,
  });
});
