/**
 * Essential Energy outage routes.
 *
 * Reads from LiveStore (`essential_current`, `essential_future`) which
 * the poller in src/sources/essential.ts populates from KML feeds.
 *
 * Response shapes mirror Python (external_api_proxy.py):
 *   /api/essential/outages       → line 5171-5234.
 *     {outages, count, planned, unplanned, future, totalCustomersAffected}.
 *     Query params `?feed=current|future`, `?type=planned|unplanned`,
 *     `?lite=1` (strips polygons).
 *   /api/essential/planned       → line 5263-5277.
 *     {outages, count, totalCustomersAffected} for planned items only.
 *   /api/essential/future        → line 5280-5290.
 *     {outages, count, totalCustomersAffected} for the future feed.
 */
import { Hono } from 'hono';
import { liveStore } from '../store/live.js';
import type { EssentialOutage } from '../sources/essential.js';

export const essentialRouter = new Hono();

function readArray(key: string): EssentialOutage[] {
  const data = liveStore.getData<EssentialOutage[]>(key);
  return Array.isArray(data) ? data : [];
}

function totalCustomers(items: EssentialOutage[]): number {
  let n = 0;
  for (const o of items) n += o.customersAffected || 0;
  return n;
}

essentialRouter.get('/api/essential/outages', (c) => {
  const all: EssentialOutage[] = [
    ...readArray('essential_current'),
    ...readArray('essential_future'),
  ];

  const url = new URL(c.req.url);
  const feed = (url.searchParams.get('feed') ?? '').toLowerCase();
  const type = (url.searchParams.get('type') ?? '').toLowerCase();
  const lite = url.searchParams.get('lite') === '1';

  let filtered = all;
  if (feed === 'current' || feed === 'future') {
    filtered = filtered.filter((o) => o.feedType === feed);
  }
  if (type === 'planned' || type === 'unplanned') {
    filtered = filtered.filter((o) => o.outageType === type);
  }

  const planned = filtered.filter((o) => o.outageType === 'planned').length;
  const unplanned = filtered.filter((o) => o.outageType === 'unplanned').length;
  const future = filtered.filter((o) => o.feedType === 'future').length;
  const total = totalCustomers(filtered);

  // Lite mode strips the polygon ring data — those are the heaviest
  // field on the wire and only the embed-card consumers need them.
  const outages = lite
    ? filtered.map((o) => {
        const { polygon: _drop, ...rest } = o;
        // Discard `_drop` deliberately; underscored to silence
        // unused-var lint without changing the destructure.
        void _drop;
        return rest;
      })
    : filtered;

  return c.json({
    outages,
    count: filtered.length,
    planned,
    unplanned,
    future,
    totalCustomersAffected: total,
  });
});

// Bot-canonical alias: planned items from current + future feeds combined.
essentialRouter.get('/api/essential/planned', (c) => {
  const items = [
    ...readArray('essential_current'),
    ...readArray('essential_future'),
  ].filter((o) => o.outageType === 'planned');
  return c.json({
    outages: items,
    count: items.length,
    totalCustomersAffected: totalCustomers(items),
  });
});

// Bot-canonical alias: just the future feed contents.
essentialRouter.get('/api/essential/future', (c) => {
  const items = readArray('essential_future');
  return c.json({
    outages: items,
    count: items.length,
    totalCustomersAffected: totalCustomers(items),
  });
});
