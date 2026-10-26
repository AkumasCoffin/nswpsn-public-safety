/**
 * What each named API key has been doing — api_key_usage_hourly (migration 127).
 *
 * The gate (apiKey.ts) marks every request whose key secret matched with
 * `apiKeyUsageId`, whether it then lets the request through or refuses it
 * (web page, revoked, expired, over the limit, a path keys may not read). The
 * middleware below reads that mark once the response exists and tallies one
 * request under (key, hour, path, status). The tally is written once a minute:
 * a key at its default 120/min is 170k requests a day, and a write per request
 * would cost more than the request.
 *
 * The flush takes the tally BEFORE writing and drops it if the write fails —
 * the flushAdsbDaily posture (services/nodes/adsbNodeStore.ts): a lost minute
 * beats a doubled one, and nothing here is billing. A restart loses at most
 * the minute in hand; shutdown writes it out.
 *
 * Paths are normalised (normaliseUsagePath) and each key may add at most
 * MAX_PATHS_PER_KEY_HOUR distinct paths in an hour; past that they are counted
 * as OTHER_PATHS. The path is chosen by the caller, so without the cap a key
 * scanning random URLs would add a row per URL.
 */
import type { MiddlewareHandler } from 'hono';
import { getWriterPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';

declare module 'hono' {
  interface ContextVariableMap {
    /**
     * Set by the gate when a request presented a key whose secret matched —
     * accepted or refused. Unlike apiKeyId it does NOT mean the request was
     * authenticated; it only says whose usage this request is.
     */
    apiKeyUsageId?: string;
  }
}

const HOUR_MS = 3_600_000;
const FLUSH_INTERVAL_MS = 60_000;
export const MAX_PATHS_PER_KEY_HOUR = 100;
export const OTHER_PATHS = '(other paths)';
const PATH_MAX = 120;

/** Segments that are an identifier rather than part of the route. */
const ID_SEGMENT_RE = /^(?:\d+|[0-9a-f]{8,}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i;
const LONG_SEGMENT = 24;

/**
 * The route a request was for, as the usage table keeps it: id-like segments
 * become `:id` so one route is one row, a method other than GET/HEAD is kept
 * in front (keys may only read, so a write attempt is worth seeing as such),
 * and the whole is capped at PATH_MAX characters.
 */
export function normaliseUsagePath(method: string, pathname: string): string {
  const segs = pathname.split('/').map((s) => (s && (s.length > LONG_SEGMENT || ID_SEGMENT_RE.test(s)) ? ':id' : s));
  let path = segs.join('/') || '/';
  const m = method.toUpperCase();
  if (m !== 'GET' && m !== 'HEAD') path = `${m} ${path}`;
  return path.length > PATH_MAX ? path.slice(0, PATH_MAX) : path;
}

/** key|hour|path|status → requests, waiting for the next flush. */
const pending = new Map<string, number>();
/**
 * key|hour → the distinct paths that key has used in that hour. Outlives a
 * flush on purpose: the cap is per hour, and resetting it every minute would
 * let a scan add a hundred rows a minute instead of a hundred an hour.
 */
const pathsSeen = new Map<string, Set<string>>();

/** Count one request against a key. Cheap and synchronous — the hot path. */
export function recordApiKeyUse(keyId: string, method: string, pathname: string, status: number, now = Date.now()): void {
  const hour = Math.floor(now / HOUR_MS) * HOUR_MS;
  let path = normaliseUsagePath(method, pathname);
  const seenKey = `${keyId}|${hour}`;
  let seen = pathsSeen.get(seenKey);
  if (!seen) {
    seen = new Set();
    pathsSeen.set(seenKey, seen);
  }
  if (!seen.has(path)) {
    if (seen.size >= MAX_PATHS_PER_KEY_HOUR) path = OTHER_PATHS;
    else seen.add(path);
  }
  const k = `${keyId}|${hour}|${path}|${status}`;
  pending.set(k, (pending.get(k) ?? 0) + 1);
}

/**
 * Hono middleware: tally each request the gate attributed to a key, with the
 * status it finally got. Mounted outside the gate (server.ts), so the gate's
 * own refusals are seen as well as whatever the route answered.
 */
export const apiKeyUsage: MiddlewareHandler = async (c, next) => {
  await next();
  const keyId = c.get('apiKeyUsageId');
  if (!keyId) return;
  recordApiKeyUse(keyId, c.req.method, new URL(c.req.url).pathname, c.res.status);
};

/** Write the tally to Postgres and clear it. See the header for the failure posture. */
export async function flushApiKeyUsage(now = Date.now()): Promise<void> {
  // Hours that can no longer receive a request no longer need their path set.
  const currentHour = Math.floor(now / HOUR_MS) * HOUR_MS;
  for (const k of pathsSeen.keys()) {
    if (Number(k.slice(k.lastIndexOf('|') + 1)) < currentHour) pathsSeen.delete(k);
  }
  if (pending.size === 0) return;
  const batch = Array.from(pending.entries());
  pending.clear();

  const ids: string[] = [];
  const hours: string[] = [];
  const paths: string[] = [];
  const statuses: number[] = [];
  const counts: number[] = [];
  const totals = new Map<string, number>();
  for (const [k, n] of batch) {
    // The path may itself contain '|', so split from both ends.
    const a = k.indexOf('|');
    const b = k.indexOf('|', a + 1);
    const z = k.lastIndexOf('|');
    const id = k.slice(0, a);
    ids.push(id);
    hours.push(new Date(Number(k.slice(a + 1, b))).toISOString());
    paths.push(k.slice(b + 1, z));
    statuses.push(Number(k.slice(z + 1)));
    counts.push(n);
    totals.set(id, (totals.get(id) ?? 0) + n);
  }

  const pool = await getWriterPool();
  if (!pool) return;
  const client = await pool.connect().catch(() => null);
  if (!client) return;
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO api_key_usage_hourly (key_id, hour, path, status, requests)
       SELECT * FROM unnest($1::uuid[], $2::timestamptz[], $3::text[], $4::smallint[], $5::int[])
       ON CONFLICT (key_id, hour, path, status) DO UPDATE
         SET requests = api_key_usage_hourly.requests + EXCLUDED.requests`,
      [ids, hours, paths, statuses, counts],
    );
    await client.query(
      `UPDATE api_keys k SET requests_total = k.requests_total + t.n
         FROM unnest($1::uuid[], $2::bigint[]) AS t(id, n)
        WHERE k.id = t.id`,
      [Array.from(totals.keys()), Array.from(totals.values())],
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    log.debug({ err, rows: batch.length }, 'api key usage flush failed');
  } finally {
    client.release();
  }
}

let flushTimer: NodeJS.Timeout | null = null;

/** Start the periodic flush. Idempotent. */
export function startApiKeyUsageFlush(): void {
  if (flushTimer) return;
  flushTimer = setInterval(() => {
    void flushApiKeyUsage().catch(() => {});
  }, FLUSH_INTERVAL_MS);
  flushTimer.unref?.();
}

/** Stop the timer and write out the minute in hand. */
export async function stopApiKeyUsageFlush(): Promise<void> {
  if (flushTimer) {
    clearInterval(flushTimer);
    flushTimer = null;
  }
  await flushApiKeyUsage();
}

/** Test seam. */
export function _resetApiKeyUsage(): void {
  pending.clear();
  pathsSeen.clear();
}

/** Test seam: the tally as plain rows. */
export function _pendingApiKeyUsage(): Array<{ keyId: string; hour: number; path: string; status: number; requests: number }> {
  return Array.from(pending.entries()).map(([k, n]) => {
    const a = k.indexOf('|');
    const b = k.indexOf('|', a + 1);
    const z = k.lastIndexOf('|');
    return { keyId: k.slice(0, a), hour: Number(k.slice(a + 1, b)), path: k.slice(b + 1, z), status: Number(k.slice(z + 1)), requests: n };
  });
}
