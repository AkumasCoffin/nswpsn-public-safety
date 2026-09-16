/**
 * The owner's view of their own ADS-B receiver.
 *
 * Two things are being protected here. The first is the GATE: these routes are
 * reachable by ownership rather than by a staff role, so somebody else's node
 * must 404 and a radio node must 400 rather than returning an empty ADS-B
 * shape. The second is that owner and staff answer from ONE implementation —
 * this codebase has twice shipped a bug caused by a second copy of shared node
 * logic drifting, so the test asserts the two responses are byte-identical
 * rather than merely similar.
 *
 * And the standing rule for anything ADS-B: the exact antenna pin never leaves
 * the backend. Both routes are grepped for it.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const OWNER = 'user-owner';
const OTHER = 'user-other';

const ADSB_NODE = {
  id: 'node-adsb-1', name: 'adsb-syd-01', kind: 'adsb',
  user_id: OWNER, lat: -33.868812, lon: 151.209321,
};
const RADIO_NODE = {
  id: 'node-radio-1', name: 'radio-01', kind: 'radio',
  user_id: OWNER, lat: null, lon: null,
};
const FOREIGN_NODE = { ...ADSB_NODE, id: 'node-adsb-2', user_id: OTHER };

const NODES: Record<string, Record<string, unknown>> = {
  [ADSB_NODE.id]: ADSB_NODE,
  [RADIO_NODE.id]: RADIO_NODE,
  [FOREIGN_NODE.id]: FOREIGN_NODE,
};

const queryMock = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));

vi.mock('../../../src/services/nodes/registry.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodes/registry.js')>();
  return {
    ...actual,
    getNode: vi.fn((id: string) => Promise.resolve(NODES[id] ?? null)),
  };
});

// Both gates open, so what is being compared below is the BODY rather than
// which role reached it. The ownership check (ownedNodeById) stays REAL — it is
// the gate under test.
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return {
    ...actual,
    canViewNodeData: vi.fn(() => Promise.resolve(true)),
    hasRole: vi.fn(() => Promise.resolve(true)),
  };
});

// The JWT itself is not what these routes are about; the userId the test
// middleware sets is what ownership resolves against.
vi.mock('../../../src/services/auth/supabaseJwt.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/supabaseJwt.js')>();
  return {
    ...actual,
    requireSupabaseJwt: async (_c: unknown, next: () => Promise<void>) => { await next(); },
  };
});

vi.mock('../../../src/services/nodes/hub.js', () => ({
  hub: {
    liveStatus: () => ({ status: { adsbAircraftNow: 17, adsbMsgRate: 92.5, adsbMaxRangeKm: 244.2 } }),
    isOnline: () => true,
    recordUpload: () => {},
    clearNode: () => {},
    forceDisconnectAgent: () => {},
  },
}));

/** The three queries adsbNodeView makes, plus the one adsbNodeTracks makes. */
function stubQueries() {
  queryMock.mockImplementation((sql: string) => {
    const s = String(sql).replace(/\s+/g, ' ');
    if (s.includes('SELECT id, name, kind, lat, lon')) return { rows: [ADSB_NODE] };
    if (s.includes('SELECT name, lat, lon')) return { rows: [ADSB_NODE] };
    if (s.includes('COALESCE(SUM(snapshots)')) {
      return {
        rows: [{
          snapshots: 51, positions: '4200', max_aircraft: 19,
          max_range_km: 244.2, msg_rate_max: 101.4, tracks_max: 880, days: 2,
        }],
      };
    }
    if (s.includes('to_char(day')) {
      return {
        rows: [
          { day: '2026-09-16', snapshots: 20, positions: '1800', max_aircraft: 18, max_range_km: 230.0, msg_rate_max: 99.5 },
          { day: '2026-09-17', snapshots: 31, positions: '2400', max_aircraft: 19, max_range_km: 244.2, msg_rate_max: 101.4 },
        ],
      };
    }
    return { rows: [] };
  });
}

async function ownerApp(userId: string) {
  const { feederRouter } = await import('../../../src/api/feeder.js');
  const app = new Hono();
  app.use('*', async (c, next) => { c.set('userId', userId); await next(); });
  app.route('/', feederRouter);
  return app;
}

async function staffApp() {
  const { nodeDataRouter } = await import('../../../src/api/node-data.js');
  const app = new Hono();
  app.use('*', async (c, next) => { c.set('userId', 'staff-1'); await next(); });
  app.route('/', nodeDataRouter);
  return app;
}

beforeEach(() => {
  queryMock.mockReset();
  stubQueries();
});

describe('owner ADS-B routes — the gate', () => {
  it('404s on somebody else’s receiver', async () => {
    const app = await ownerApp(OWNER);
    for (const path of ['adsb', 'adsb-tracks']) {
      const res = await app.request(`/api/feeder/nodes/${FOREIGN_NODE.id}/${path}`);
      expect(res.status).toBe(404);
      expect((await res.json()).error).toBe('not your node');
    }
  });

  it('400s on the owner’s own RADIO node', async () => {
    // Not an empty ADS-B shape: asking here for a radio node is a caller bug,
    // and the same guard already exists on /stats the other way round.
    const app = await ownerApp(OWNER);
    const res = await app.request(`/api/feeder/nodes/${RADIO_NODE.id}/adsb`);
    expect(res.status).toBe(400);
    expect((await res.json()).error).toBe('not an adsb node');
  });

  it('404s on a node id that does not exist', async () => {
    const app = await ownerApp(OWNER);
    const res = await app.request('/api/feeder/nodes/no-such-node/adsb');
    expect(res.status).toBe(404);
  });
});

describe('owner ADS-B routes — one implementation, two gates', () => {
  it('returns exactly what the staff route returns', async () => {
    const owner = await ownerApp(OWNER);
    const staff = await staffApp();

    const o = await (await owner.request(`/api/feeder/nodes/${ADSB_NODE.id}/adsb?window=7d`)).json();
    const s = await (await staff.request(`/api/node-data/adsb-node?nodeId=${ADSB_NODE.id}&window=7d`)).json();
    expect(o).toEqual(s);
    expect(o.window).toBe('7d');
    expect(o.totals.positions).toBe(4200);

    const ot = await (await owner.request(`/api/feeder/nodes/${ADSB_NODE.id}/adsb-tracks?minutes=480`)).json();
    const st = await (await staff.request(`/api/node-data/adsb-tracks?nodeId=${ADSB_NODE.id}&minutes=480`)).json();
    expect(ot).toEqual(st);
  });

  it('falls back to a 24h window on junk, like the staff route', async () => {
    const owner = await ownerApp(OWNER);
    const res = await owner.request(`/api/feeder/nodes/${ADSB_NODE.id}/adsb?window=forever`);
    expect((await res.json()).window).toBe('24h');
  });

  it('clamps the coverage window rather than answering short', async () => {
    const owner = await ownerApp(OWNER);
    const res = await owner.request(`/api/feeder/nodes/${ADSB_NODE.id}/adsb-tracks?minutes=99999`);
    expect((await res.json()).windowMinutes).toBe(480);
  });
});

describe('owner ADS-B routes — the antenna pin', () => {
  it('never carries the exact position, on either route', async () => {
    const owner = await ownerApp(OWNER);

    const view = await (await owner.request(`/api/feeder/nodes/${ADSB_NODE.id}/adsb`)).json();
    expect(view.node.hasPosition).toBe(true);
    expect(view.node).not.toHaveProperty('lat');
    expect(JSON.stringify(view)).not.toContain('151.209321');

    const tracks = await (await owner.request(`/api/feeder/nodes/${ADSB_NODE.id}/adsb-tracks`)).json();
    // Rounded inside the shared service, with no flag to skip it — the owner
    // already knows their own pin, so there is nothing to buy by shipping it.
    expect(tracks.site.approx).toBe(true);
    expect(tracks.site.lat).toBe(-33.87);
    expect(tracks.site.lon).toBe(151.21);
    expect(JSON.stringify(tracks)).not.toContain('151.209321');
  });
});
