/**
 * User-submitted map incidents → logs-page history.
 *
 * Unlike every other source this one doesn't poll an external upstream:
 * it snapshots the local `incidents` table (user-created map pins) into
 * archive_misc under source `user_incident`, so user incidents appear on
 * the logs page like any other feed.
 *
 * The archive writer's hash dedup means a stable incident stores one
 * parent row per state change (edits, status flips, expiry) — polling
 * every minute does NOT mean a row per minute. Retention is the archive's
 * normal partition-drop window (DATA_RETENTION_DAYS), which also covers
 * deleted pins: the map DELETE only soft-deletes the live row, and its
 * archive snapshots stay searchable until the partition ages out.
 *
 * Incidents that staff ARCHIVE (permanent snapshot) are removed from
 * archive_misc by the archive endpoint so they appear only in the logs
 * page's "Archived Incidents" area, not the regular feed.
 *
 * RFS/pager unit-stub rows (is_rfs_stub) are excluded — they're log/unit
 * carriers for incidents that already appear via the rfs/pager sources.
 */
import { getPool } from '../db/pool.js';
import { registerSource } from '../services/sourceRegistry.js';
import type { ArchiveRow } from '../store/archive.js';

export interface UserIncidentRow {
  id: string;
  title: string | null;
  description: string | null;
  lat: number | string | null;
  lng: number | string | null;
  location: string | null;
  type: unknown;
  status: string | null;
  size: string | null;
  responding_agencies: unknown;
  units: unknown;
  created_at: Date | string | null;
  updated_at: Date | string | null;
  expires_at: Date | string | null;
}

function asStringArray(v: unknown): string[] {
  let parsed = v;
  // Deployed DBs created by the legacy python init store JSONB-ish
  // columns as TEXT; parse defensively like normaliseIncident does.
  if (typeof parsed === 'string') {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      return [];
    }
  }
  if (!Array.isArray(parsed)) return [];
  return parsed.filter((x): x is string => typeof x === 'string');
}

function toEpoch(v: Date | string | null): number | null {
  if (v instanceof Date) return Math.floor(v.getTime() / 1000);
  if (typeof v === 'string' && v) {
    const t = Date.parse(v);
    return Number.isFinite(t) ? Math.floor(t / 1000) : null;
  }
  return null;
}

function toIso(v: Date | string | null): string | null {
  const epoch = toEpoch(v);
  return epoch === null ? null : new Date(epoch * 1000).toISOString();
}

function coerceNum(v: number | string | null): number | null {
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/**
 * Map one incidents-table row to its archive_misc snapshot. Exported so
 * the DELETE endpoint can push a final is_active:false snapshot at
 * delete time instead of leaving the last archived state "active"
 * forever (the poller stops seeing soft-deleted rows).
 *
 * The data blob deliberately contains no per-poll noise fields — every
 * key changes only when a human edits the incident (or it expires), so
 * the writer's hash dedup collapses repeat polls to sidecar bumps.
 */
export function userIncidentArchiveRow(
  row: UserIncidentRow,
  fetchedAt: number,
  opts?: { forceInactive?: boolean },
): ArchiveRow {
  const types = asStringArray(row.type);
  const expiresEpoch = toEpoch(row.expires_at);
  const isActive = opts?.forceInactive
    ? false
    : expiresEpoch === null || expiresEpoch > fetchedAt;
  return {
    source: 'user_incident',
    source_id: row.id,
    fetched_at: fetchedAt,
    source_timestamp_unix: toEpoch(row.updated_at) ?? toEpoch(row.created_at),
    lat: coerceNum(row.lat),
    lng: coerceNum(row.lng),
    category: types[0] ?? null,
    subcategory: null,
    data: {
      title: row.title ?? '',
      location_text: row.location ?? '',
      description: row.description ?? '',
      status: row.status ?? '',
      size: row.size ?? '',
      type: types,
      responding_agencies: asStringArray(row.responding_agencies),
      units: asStringArray(row.units),
      is_active: isActive,
      created_at: toIso(row.created_at),
      updated_at: toIso(row.updated_at),
      expires_at: toIso(row.expires_at),
    },
  };
}

async function fetchUserIncidents(): Promise<UserIncidentRow[]> {
  const pool = await getPool();
  if (!pool) return [];
  const r = await pool.query<UserIncidentRow>(
    `SELECT id, title, description, lat, lng, location, type, status, size,
            responding_agencies, units, created_at, updated_at, expires_at
       FROM incidents
      WHERE deleted_at IS NULL AND is_rfs_stub IS NOT TRUE`,
  );
  return r.rows;
}

export default function registerUserIncidents(): void {
  registerSource<UserIncidentRow[]>({
    name: 'user_incidents',
    family: 'misc',
    archiveSource: 'user_incident',
    intervalMs: 60_000,
    fetch: fetchUserIncidents,
    archiveItems: (data, fetchedAt) => {
      if (!Array.isArray(data)) return [];
      return (data as UserIncidentRow[]).map((row) =>
        userIncidentArchiveRow(row, fetchedAt),
      );
    },
  });
}
