/**
 * Runtime Wire settings, backed by app_settings.
 *
 * These used to be environment variables, which meant flipping The Wire live
 * required editing .env and restarting the process. They're now rows an owner
 * can toggle from the staff panel.
 *
 * Every getter falls back to the old environment value when the row is absent,
 * so nothing changes on deploy: the current .env keeps deciding until an owner
 * actually flips the switch, and the first write takes ownership from there.
 *
 * Reads are cached briefly. wirePublic() in particular is consulted on every
 * Wire read for the soft-launch gate, and that shouldn't become a database
 * round trip per request.
 */
import type { Pool } from 'pg';
import { config } from '../config.js';
import { log } from '../lib/log.js';

const CACHE_MS = 10_000;
const cache = new Map<string, { value: string | null; at: number }>();

/** Read one setting, or null when unset. Cached for CACHE_MS. */
async function readSetting(pool: Pool, key: string): Promise<string | null> {
  const hit = cache.get(key);
  const now = Date.now();
  if (hit && now - hit.at < CACHE_MS) return hit.value;
  try {
    const r = await pool.query<{ value: string }>(
      'SELECT value FROM app_settings WHERE key = $1',
      [key],
    );
    const value = r.rowCount === 0 ? null : r.rows[0]!.value;
    cache.set(key, { value, at: now });
    return value;
  } catch (err) {
    log.warn({ err, key }, 'wireSettings: read failed');
    // Serve a stale value rather than silently flipping a gate because the
    // database hiccuped — a transient error must not make a private Wire
    // public, or vice versa.
    return hit ? hit.value : null;
  }
}

/** Drop the cache so a staff toggle takes effect immediately. */
export function invalidateWireSettings(): void {
  cache.clear();
}

/**
 * Is The Wire public? While false it's visible only to contributors and
 * moderators (the gate itself lives in api/wire.ts).
 *
 * Falls back to the WIRE_PUBLIC env var until an owner sets the row.
 */
export async function wirePublic(pool: Pool | null): Promise<boolean> {
  if (!pool) return config.WIRE_PUBLIC === 'true';
  const v = await readSetting(pool, 'wire_public');
  if (v === null) return config.WIRE_PUBLIC === 'true';
  return v === 'true';
}

/**
 * Whether posting automatically awards the OG / First Contributor badges.
 * Defaults ON — the badges exist to reward the pre-launch period, and that
 * window closes on its own when the Wire goes public.
 */
export async function autoTagsEnabled(pool: Pool | null): Promise<boolean> {
  if (!pool) return true;
  const v = await readSetting(pool, 'wire_auto_tags');
  return v === null ? true : v !== 'false';
}

/** Persist a setting. Caller is responsible for the permission check. */
export async function setWireSetting(
  pool: Pool,
  key: 'wire_public' | 'wire_auto_tags',
  value: boolean,
  updatedBy: string | null,
): Promise<void> {
  await pool.query(
    `INSERT INTO app_settings (key, value, updated_by, updated_at)
     VALUES ($1, $2, $3, now())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value,
       updated_by = EXCLUDED.updated_by, updated_at = now()`,
    [key, value ? 'true' : 'false', updatedBy],
  );
  invalidateWireSettings();
}
