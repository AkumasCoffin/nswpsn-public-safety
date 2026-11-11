/**
 * The owner's decode-health chart: GET /api/feeder/nodes/:id/decode.
 *
 * Gated on ownership like the other owner routes. Past the gate: one series
 * per site this node reported, on a shared gap-filled time axis, zero readings
 * treated as "not measured" (the SQL filters them), busiest site first.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const OWNER = 'user-owner';
const RADIO = { id: 'node-radio-1', kind: 'radio', user_id: OWNER, name: 'radio-01' };
const PAGER = { id: 'node-pager-1', kind: 'pager', user_id: OWNER, name: 'pager-01' };
const FOREIGN = { ...RADIO, id: 'node-radio-2', user_id: 'someone-else' };
const NODES: Record<string, Record<string, unknown>> = {
  [RADIO.id]: RADIO, [PAGER.id]: PAGER, [FOREIGN.id]: FOREIGN,
};

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));
vi.mock('../../../src/services/nodes/registry.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodes/registry.js')>();
  return { ...actual, getNode: vi.fn((id: string) => Promise.resolve(NODES[id] ?? null)) };
});
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, hasRole: vi.fn(() => Promise.resolve(true)) };
});
vi.mock('../../../src/services/auth/supabaseJwt.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/supabaseJwt.js')>();
  return {
    ...actual,
    requireSupabaseJwt: async (_c: unknown, next: () => Promise<void>) => { await next(); },
  };
});
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

async function ownerApp() {
  const { feederRouter } = await import('../../../src/api/feeder.js');
  const app = new Hono();
  app.use('*', async (c, next) => { c.set('userId', OWNER); await next(); });
  app.route('/', feederRouter);
  return app;
}

/** The start of the bucket `minutesAgo` before now, at 10-minute buckets. */
function bucketAgo(minutesAgo: number): Date {
  const ms = 10 * 60_000;
  return new Date(Math.floor((Date.now() - minutesAgo * 60_000) / ms) * ms);
}

beforeEach(() => {
  queryMock.mockReset();
  queryMock.mockImplementation(async (sql: string) => {
    if (sql.includes('node_site_snapshots')) {
      return { rows: [{ system_id: 1, rfss: 1, site_id: 7, channel_name: 'Knights Hill' }] };
    }
    if (sql.includes('node_site_decode_samples')) {
      return {
        rows: [
          // The same tower, once before the system was identified (-1):
          // one site, named from the real system.
          { system_id: -1, rfss: 1, site_id: 7, bucket: bucketAgo(30), decode_pct: '96.5', samples: 20 },
          { system_id: 1, rfss: 1, site_id: 7, bucket: bucketAgo(0), decode_pct: '94', decode_min: '71', samples: 15 },
          { system_id: 1, rfss: 1, site_id: 9, bucket: bucketAgo(20), decode_pct: '70', samples: 25 },
          { system_id: 1, rfss: 1, site_id: 9, bucket: bucketAgo(0), decode_pct: '61', samples: 10 },
          // A glimpse: too few readings to be a site the node decodes.
          { system_id: 1, rfss: 3, site_id: 2, bucket: bucketAgo(0), decode_pct: '40', samples: 5 },
          // A bucket that only ever read zero: "not measured", not a site.
          { system_id: 1, rfss: 2, site_id: 1, bucket: bucketAgo(0), decode_pct: null, samples: 0 },
        ],
      };
    }
    return { rows: [] };
  });
});

describe('GET /api/feeder/nodes/:id/decode', () => {
  it('404s on somebody else’s node and 400s on a pager node', async () => {
    const app = await ownerApp();
    expect((await app.request(`/api/feeder/nodes/${FOREIGN.id}/decode`)).status).toBe(404);
    expect((await app.request(`/api/feeder/nodes/${PAGER.id}/decode`)).status).toBe(400);
    expect(queryMock.mock.calls.some(([sql]) => String(sql).includes('node_site_decode_samples'))).toBe(false);
  });

  it('gives one gap-filled series per site, busiest first, scoped to the node', async () => {
    const app = await ownerApp();
    const res = await app.request(`/api/feeder/nodes/${RADIO.id}/decode?range=24h`);
    expect(res.status).toBe(200);
    const d = (await res.json()) as {
      range: string; bucketMinutes: number; at: string[];
      sites: Array<{ site: number; name: string | null; decodePct: Array<number | null>; latestPct: number | null }>;
    };
    expect(d.range).toBe('24h');
    expect(d.bucketMinutes).toBe(10);
    // 24 hours of 10-minute buckets, continuous.
    expect(d.at.length).toBeGreaterThanOrEqual(144);
    expect(d.sites.map((s) => s.site)).toEqual([7, 9]);
    const kh = d.sites[0]!;
    expect(kh.name).toBe('Knights Hill');
    expect(kh.decodePct).toHaveLength(d.at.length);
    expect(kh.latestPct).toBe(94);
    expect(kh.decodePct.filter((v) => v != null)).toEqual([96.5, 94]);
    // The worst reading rides alongside, on the same axis.
    const mins = (kh as unknown as { decodeMin: Array<number | null> }).decodeMin;
    expect(mins).toHaveLength(d.at.length);
    expect(mins.filter((v) => v != null)).toEqual([71]);
    expect((kh as unknown as { avgPct: number }).avgPct).toBeCloseTo(95.25);

    const sql = queryMock.mock.calls.find(([q]) => String(q).includes('node_site_decode_samples'))!;
    expect(sql[1]).toEqual([RADIO.id, '24 hours']);
  });

  it('falls back to 24h on an unknown range', async () => {
    const app = await ownerApp();
    const res = await app.request(`/api/feeder/nodes/${RADIO.id}/decode?range=30d`);
    expect(((await res.json()) as { range: string }).range).toBe('24h');
  });
});
