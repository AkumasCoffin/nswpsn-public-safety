/**
 * Learned capcode -> alias sync (Pagermon).
 *
 * Why this exists: the staff Data tab resolved capcode aliases only from the
 * static data/pager/Capcode-Aliases.csv, which covers a small subset of what
 * the feeder nodes actually receive — so the Top Capcodes table showed bare
 * numbers while the live map (which reads Pagermon directly) showed proper
 * unit names. Rather than have the operator re-export a CSV by hand, we learn
 * the mapping from Pagermon itself.
 *
 * Two sources, both optional and both safe to fail:
 *   1. The live message feed — every Pagermon message already carries
 *      `alias`/`agency`, so each poll teaches us the capcodes that are
 *      actually paging. Zero extra config (reuses the existing PAGERMON_URL).
 *   2. Pagermon's capcode list endpoint (derived from PAGERMON_URL, e.g.
 *      .../api/messages -> .../api/capcodes) for a FULL sync including
 *      capcodes that haven't paged recently. Skipped if it isn't reachable.
 *
 * Results land in pager_capcode_aliases (migration 060) and are used as the
 * primary lookup in api/node-data.ts, with the CSV as fallback. The capcode
 * key is stored normalised (leading zeros stripped) to match those lookups.
 */
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { config } from '../config.js';
import { normalizeCapcode } from '../api/node-data.js';

export interface LearnedAlias {
  capcode: string;
  alias: string;
  agency: string | null;
}

/** Upsert learned aliases. Newer non-empty values win; no-ops on an empty list. */
export async function storeLearnedAliases(
  pool: Pool,
  rows: readonly LearnedAlias[],
  source = 'pagermon-feed',
): Promise<number> {
  const clean = rows
    .map((r) => ({
      capcode: normalizeCapcode(r.capcode),
      alias: (r.alias ?? '').trim(),
      agency: (r.agency ?? '')?.trim() || null,
    }))
    .filter((r) => r.capcode !== '' && r.alias !== '');
  if (clean.length === 0) return 0;

  // De-dupe within the batch (a single poll can carry the same capcode twice)
  // — ON CONFLICT can't fire twice for one row in one statement.
  const byCapcode = new Map<string, LearnedAlias>();
  for (const r of clean) byCapcode.set(r.capcode, r);

  let n = 0;
  for (const r of byCapcode.values()) {
    try {
      await pool.query(
        `INSERT INTO pager_capcode_aliases (capcode, alias, agency, source, updated_at)
         VALUES ($1, $2, $3, $4, now())
         ON CONFLICT (capcode) DO UPDATE
           SET alias = EXCLUDED.alias,
               agency = COALESCE(EXCLUDED.agency, pager_capcode_aliases.agency),
               source = EXCLUDED.source,
               updated_at = now()
         WHERE pager_capcode_aliases.alias IS DISTINCT FROM EXCLUDED.alias
            OR pager_capcode_aliases.agency IS DISTINCT FROM EXCLUDED.agency`,
        [r.capcode, r.alias, r.agency, source],
      );
      n += 1;
    } catch (err) {
      log.debug({ err, capcode: r.capcode }, 'capcode alias upsert failed');
    }
  }
  return n;
}

/**
 * Learn aliases from a Pagermon message snapshot. Called after each pager poll
 * — cheap (only rows that actually changed are written) and best-effort.
 */
export async function learnAliasesFromMessages(
  messages: readonly { capcode?: unknown; alias?: unknown; agency?: unknown }[],
): Promise<void> {
  try {
    const rows: LearnedAlias[] = [];
    for (const m of messages) {
      const capcode = String(m.capcode ?? '').trim();
      const alias = String(m.alias ?? '').trim();
      if (!capcode || !alias) continue;
      rows.push({ capcode, alias, agency: String(m.agency ?? '').trim() || null });
    }
    if (rows.length === 0) return;
    const pool = await getPool();
    if (!pool) return;
    await storeLearnedAliases(pool, rows, 'pagermon-feed');
  } catch (err) {
    log.debug({ err }, 'capcode alias learn-from-feed failed');
  }
}

/**
 * Derive Pagermon's capcode-list URL from the configured messages URL.
 * `https://host/api/messages` -> `https://host/api/capcodes`. Returns null when
 * PAGERMON_URL isn't set or doesn't look like an /api/ URL we can rewrite.
 */
export function capcodeListUrl(): string | null {
  const base = (config.PAGERMON_URL ?? '').trim();
  if (!base || !/^https?:/i.test(base)) return null;
  try {
    const u = new URL(base);
    if (!/\/api\//i.test(u.pathname)) return null;
    u.pathname = u.pathname.replace(/\/api\/[^/]*\/?$/i, '/api/capcodes');
    u.search = '';
    return u.toString();
  } catch {
    return null;
  }
}

/**
 * Full sync from Pagermon's capcode endpoint (all known capcodes, not just
 * those that paged recently). Best-effort: returns 0 if the endpoint isn't
 * available — the message-feed harvest still keeps the table current.
 */
export async function syncAliasesFromPagermon(): Promise<number> {
  const url = capcodeListUrl();
  if (!url) return 0;
  const apiKey = (config.PAGERMON_API_KEY ?? '').trim();
  const full = apiKey ? `${url}?apikey=${encodeURIComponent(apiKey)}` : url;
  try {
    const res = await fetch(full, {
      headers: { accept: 'application/json' },
      signal: AbortSignal.timeout(15_000),
    });
    if (!res.ok) {
      log.info({ status: res.status, url }, 'pagermon capcode list unavailable — using feed harvest only');
      return 0;
    }
    const body = (await res.json()) as unknown;
    // Accept either a bare array or {capcodes:[…]} / {rows:[…]}.
    const list: unknown[] = Array.isArray(body)
      ? body
      : Array.isArray((body as Record<string, unknown>)?.['capcodes'])
        ? ((body as Record<string, unknown>)['capcodes'] as unknown[])
        : Array.isArray((body as Record<string, unknown>)?.['rows'])
          ? ((body as Record<string, unknown>)['rows'] as unknown[])
          : [];
    const rows: LearnedAlias[] = [];
    for (const item of list) {
      if (!item || typeof item !== 'object') continue;
      const o = item as Record<string, unknown>;
      // Pagermon calls the capcode `address`; tolerate `capcode` too.
      const capcode = String(o['address'] ?? o['capcode'] ?? '').trim();
      const alias = String(o['alias'] ?? '').trim();
      if (!capcode || !alias) continue;
      // Respect Pagermon's own ignore flag when present.
      if (o['ignore'] === 1 || o['ignore'] === true) continue;
      rows.push({ capcode, alias, agency: String(o['agency'] ?? '').trim() || null });
    }
    if (rows.length === 0) return 0;
    const pool = await getPool();
    if (!pool) return 0;
    const n = await storeLearnedAliases(pool, rows, 'pagermon-api');
    log.info({ fetched: rows.length, written: n }, 'synced capcode aliases from Pagermon');
    return n;
  } catch (err) {
    log.info({ err, url }, 'pagermon capcode sync failed — using feed harvest only');
    return 0;
  }
}

/** capcode (normalised) -> {alias, agency} learned from Pagermon. */
export async function learnedAliasMap(
  pool: Pool,
): Promise<Map<string, { alias: string; agency: string | null }>> {
  const map = new Map<string, { alias: string; agency: string | null }>();
  try {
    const r = await pool.query<{ capcode: string; alias: string; agency: string | null }>(
      'SELECT capcode, alias, agency FROM pager_capcode_aliases',
    );
    for (const row of r.rows) map.set(row.capcode, { alias: row.alias, agency: row.agency });
  } catch (err) {
    log.debug({ err }, 'learned capcode alias read failed');
  }
  return map;
}
