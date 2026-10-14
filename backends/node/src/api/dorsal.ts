/**
 * Dorsal Watch shark reports.
 *
 * Served straight out of the LiveStore snapshot the source maintains — it
 * already holds a rolling 14-day window, so there is nothing to read back
 * out of the archive the way /api/pager/hits has to.
 *
 * THE 48-HOUR DEFAULT IS LOAD-BEARING. The Discord bot polls this endpoint
 * with no query string and de-duplicates against `seen_alerts`, which its
 * retention job prunes at 7 days (discord-bot/database.py cleanup_old_seen).
 * Serve a default window longer than that and every report re-fires once it
 * ages past the dedup horizon. 48 hours leaves a wide margin; the map asks
 * for its 7 days explicitly via ?hours=168.
 */
import { Hono } from 'hono';
import { dorsalSnapshot } from '../sources/dorsal.js';
import type { DorsalFeature } from '../sources/dorsal.js';

export const dorsalRouter = new Hono();

const DEFAULT_HOURS = 48;
/** 14 days — the most the source's rolling window can actually supply. */
const MAX_HOURS = 336;

function parseHours(raw: string | null): number {
  if (raw === null || raw.trim() === '') return DEFAULT_HOURS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_HOURS;
  return Math.min(Math.floor(n), MAX_HOURS);
}

/** Comma-separated list, lower-cased and de-blanked. Empty set = no filter. */
function parseList(raw: string | null): Set<string> {
  const out = new Set<string>();
  if (!raw) return out;
  for (const part of raw.split(',')) {
    const v = part.trim().toLowerCase();
    if (v) out.add(v);
  }
  return out;
}

dorsalRouter.get('/api/dorsal/reports', (c) => {
  const url = new URL(c.req.url);
  const hours = parseHours(url.searchParams.get('hours'));
  const categories = parseList(url.searchParams.get('category'));
  const states = parseList(url.searchParams.get('state'));

  const cutoff = Math.floor(Date.now() / 1000) - hours * 3600;
  const snap = dorsalSnapshot();

  const features: DorsalFeature[] = [];
  for (const f of snap.features) {
    const p = f.properties;
    if (p.reported_at_unix < cutoff) continue;
    if (categories.size > 0 && !categories.has(p.category)) continue;
    if (states.size > 0 && !states.has(p.state.toLowerCase())) continue;
    features.push(f);
  }

  return c.json({
    type: 'FeatureCollection',
    features,
    count: features.length,
    hours,
    fetched_at: snap.fetched_at,
  });
});
