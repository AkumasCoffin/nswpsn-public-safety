/**
 * X-Ingest-Key auth middleware for /api/waze/ingest.
 *
 * Separate from the main API-key middleware because it has a separate set
 * of shared secrets (WAZE_INGEST_KEY + WAZE_INGEST_KEYS) and a separate
 * consumer (the Tampermonkey userscript). Keeps the userscript from needing
 * the full backend NSWPSN_API_KEY.
 *
 * Multi-key: each feeder/operator runs the userscript with their own key so
 * keys can be revoked or attributed individually. A POST is accepted if its
 * X-Ingest-Key matches ANY configured key. WAZE_INGEST_KEY stays for the
 * single-key/legacy case; WAZE_INGEST_KEYS is a comma/whitespace/newline
 * separated list of the rest.
 */
import { timingSafeEqual } from 'node:crypto';
import type { MiddlewareHandler } from 'hono';
import { config } from '../../config.js';

// The first 5 chars of the matched ingest key, so handlers/logs can show
// WHICH feeder a request came from without exposing the full key.
declare module 'hono' {
  interface ContextVariableMap {
    ingestKeyPrefix?: string;
  }
}

/**
 * Constant-time string comparison. Length mismatch short-circuits before
 * timingSafeEqual (which throws on unequal-length buffers).
 */
function safeEqual(a: string, b: string): boolean {
  const aBuf = Buffer.from(a, 'utf8');
  const bBuf = Buffer.from(b, 'utf8');
  if (aBuf.length !== bBuf.length) return false;
  return timingSafeEqual(aBuf, bBuf);
}

/**
 * All accepted ingest keys: the single WAZE_INGEST_KEY plus every entry in
 * WAZE_INGEST_KEYS (split on commas/whitespace/newlines), de-duped. Parsed
 * fresh per call so config changes (and tests) are honoured immediately.
 */
export function configuredIngestKeys(): string[] {
  const keys: string[] = [];
  if (config.WAZE_INGEST_KEY) keys.push(config.WAZE_INGEST_KEY);
  if (config.WAZE_INGEST_KEYS) {
    for (const raw of config.WAZE_INGEST_KEYS.split(/[\s,]+/)) {
      const k = raw.trim();
      if (k) keys.push(k);
    }
  }
  return [...new Set(keys)];
}

/** True when at least one ingest key is configured (used by the status page). */
export function ingestKeysConfigured(): boolean {
  return configuredIngestKeys().length > 0;
}

export const requireIngestKey: MiddlewareHandler = async (c, next) => {
  const keys = configuredIngestKeys();
  if (keys.length === 0) {
    // No key configured server-side — reject everything. Avoids
    // accidentally accepting unauthenticated POSTs in dev.
    return c.json({ error: 'ingest disabled' }, 403);
  }
  const supplied = c.req.header('X-Ingest-Key') ?? '';
  // Compare against every configured key WITHOUT short-circuiting, so
  // response timing doesn't leak which key matched or how many exist.
  let matched: string | null = null;
  for (const k of keys) {
    if (safeEqual(supplied, k)) matched = k;
  }
  if (matched === null) {
    return c.json({ error: 'unauthorized' }, 401);
  }
  // Expose the first 5 chars so the ingest handler can log which feeder
  // this request came from.
  c.set('ingestKeyPrefix', matched.slice(0, 5));
  await next();
};
