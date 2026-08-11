/**
 * Persistent callsign ("unit name") dictionary shared across features.
 *
 * The `callsigns` table (migration 031_incident_units_callsigns.sql) is the
 * cross-feature autocomplete source: the map-editor incident form and The
 * Wire compose form both save units against it so a callsign typed once can
 * be Tab-completed everywhere afterwards.
 *
 * This module was extracted from api/incidents.ts (rememberCallsigns +
 * the GET /api/incidents/callsigns query) so wire.ts can reuse the exact
 * same upsert/read without importing the incidents router.
 */
import type { Pool } from 'pg';
import { log } from '../lib/log.js';

/**
 * Normalise an editor-supplied unit into the canonical dictionary form:
 * trimmed, uppercased, capped at 24 chars. Returns '' for non-strings /
 * blanks so callers can skip them.
 */
export function normaliseCallsign(raw: unknown): string {
  if (typeof raw !== 'string') return '';
  return raw.trim().toUpperCase().slice(0, 24);
}

/**
 * Sanitize an editor-supplied units list: strings only, trimmed,
 * uppercased, de-duped, bounded (24 chars each, 50 units max) so a
 * malformed payload can't bloat a row or the callsign dictionary.
 */
export function sanitizeUnits(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const u of raw) {
    const s = normaliseCallsign(u);
    if (!s || seen.has(s)) continue;
    seen.add(s);
    out.push(s);
    if (out.length >= 50) break;
  }
  return out;
}

/**
 * Upsert saved callsigns into the persistent dictionary (best-effort — a
 * failure here must never fail the caller's save). Bumps use_count/last_used
 * on conflict so the autocomplete can order by popularity.
 */
export async function rememberCallsigns(pool: Pool, units: readonly string[]): Promise<void> {
  for (const cs of units) {
    if (!cs) continue;
    try {
      await pool.query(
        `INSERT INTO callsigns (callsign) VALUES ($1)
         ON CONFLICT (callsign) DO UPDATE
           SET last_used = now(), use_count = callsigns.use_count + 1`,
        [cs],
      );
    } catch (err) {
      log.warn({ err, cs }, 'callsigns: upsert failed');
      return;
    }
  }
}

/**
 * The dictionary for tab-completion, most-used first. Tolerates a missing
 * table (un-run migration) by returning [] rather than throwing.
 */
export async function getCallsignDict(pool: Pool): Promise<string[]> {
  try {
    const r = await pool.query<{ callsign: string }>(
      'SELECT callsign FROM callsigns ORDER BY use_count DESC, last_used DESC LIMIT 500',
    );
    return r.rows.map((row) => row.callsign);
  } catch {
    return [];
  }
}
