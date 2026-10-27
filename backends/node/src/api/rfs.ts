/**
 * GET /api/rfs/incidents       — parsed GeoJSON FeatureCollection
 * GET /api/rfs/incidents/raw   — pass-through of the upstream XML feed
 *                                 converted to JSON, for clients that
 *                                 want to do their own parsing
 *
 * Mirrors the Python routes at external_api_proxy.py:10621 + 10726.
 * The /incidents path serves out of LiveStore (filled by the poller).
 * The /raw path hits upstream on every request — same as Python which
 * caches it at HTTP layer rather than via the prewarm pipeline.
 */
import { Hono } from 'hono';
import { rfsSnapshot, fetchRfsRaw } from '../sources/rfs.js';
import { fetchText } from '../sources/shared/http.js';
import { parseXml, asArray, textOf } from '../sources/shared/xml.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';

export const rfsRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'rfs', type: 'incidents', window: 'live' });
const SCOPE_WARNINGS = scoped({ source: 'rfs', type: 'warnings', window: 'live' });

rfsRouter.get('/api/rfs/incidents', SCOPE_INCIDENTS, (c) => c.json(rfsSnapshot()));

rfsRouter.get('/api/rfs/incidents/raw', SCOPE_INCIDENTS, async (c) => {
  try {
    const raw = await fetchRfsRaw();
    return c.json(raw);
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'rfs raw fetch failed');
    return c.json({ channel: {}, items: [], count: 0 });
  }
});

// /api/rfs/fdr — Fire Danger Ratings. Mirrors python line 10772-10799.
// Fetched per request because the cadence is slow (daily) and the
// payload is small; not worth a poller slot. Description's HTML is
// stripped to match python's re.sub(r'<[^>]+>', '', ...) behaviour.
rfsRouter.get('/api/rfs/fdr', SCOPE_WARNINGS, async (c) => {
  try {
    const xml = await fetchText('https://www.rfs.nsw.gov.au/feeds/fdrToban.xml', {
      timeoutMs: 15_000,
      headers: { 'User-Agent': 'Mozilla/5.0' },
    });
    const parsed = parseXml(xml);
    const rss =
      (parsed['rss'] as Record<string, unknown> | undefined) ?? parsed;
    const channel =
      rss && typeof rss === 'object'
        ? ((rss as Record<string, unknown>)['channel'] as
            | Record<string, unknown>
            | undefined)
        : undefined;
    const items = asArray(channel?.['item']) as Array<Record<string, unknown>>;
    const ratings = items.map((item) => ({
      title: textOf(item, 'title'),
      description: textOf(item, 'description')
        .replace(/<[^>]+>/g, '')
        .trim(),
      source: 'rfs_fdr' as const,
    }));
    return c.json({ ratings });
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'rfs fdr fetch failed');
    return c.json({ ratings: [] });
  }
});
