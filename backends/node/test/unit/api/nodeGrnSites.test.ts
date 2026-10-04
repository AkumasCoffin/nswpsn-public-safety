/**
 * GET /api/nodes/:id/grn-sites — the GRN dataset offered as channel
 * candidates for one node: what it includes, what it flags as unusable, and
 * how it ranks (straight-line distance when the node has an antenna pin,
 * council-border hops when it only has an LGA).
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';
import type { NodeRow } from '../../../src/services/nodes/registry.js';

function nodeRow(overrides: Partial<NodeRow> = {}): NodeRow {
  return {
    id: 'node-1', kind: 'radio', user_id: 'user-1', install_id: null,
    name: 'radio-user-abcd1234', enabled: true, feed_enabled: true,
    config_override: {}, config_version: null, agent_version: null,
    sdrtrunk_version: null, rdio_version: null, os: null, arch: null,
    last_seen_at: null, notes: null, created_at: '2026-01-01T00:00:00Z',
    token_prefix: null, lat: null, lon: null, zone: null,
    state: 'NSW', lga: 'Blue Mountains', suburb: null,
    ...overrides,
  } as NodeRow;
}

vi.mock('../../../src/services/nodes/registry.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodes/registry.js')>();
  return { ...actual, getNode: vi.fn(async () => nodeRow()) };
});

vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canViewNodeData: vi.fn(async () => true), canManageNodes: vi.fn(async () => true) };
});

// The dataset and the LGA adjacency are the survey's own; stub both so the
// ranking is the only thing under test.
const site = (over: Record<string, unknown> = {}) => ({
  name: 'Site', grnKey: null, lga: 'Blue Mountains', mhz: 420, altMhz: null,
  rawCc: '420.0000', lat: -33.7, lon: 150.3, system: 'NSWPSN', ...over,
});
let fakeSites: ReturnType<typeof site>[] = [];
let fakeDepths = new Map<string, number>();
vi.mock('../../../src/services/grnCandidates.js', () => ({
  allTaggedSites: vi.fn(async () => fakeSites),
  lgaRingDepths: vi.fn(async () => fakeDepths),
}));

const { nodesRouter } = await import('../../../src/api/nodes.js');
const registry = await import('../../../src/services/nodes/registry.js');

function get(path: string) {
  const app = new Hono();
  app.use('*', async (c, next) => { c.set('userId', 'staff-1'); await next(); });
  app.route('/', nodesRouter);
  return app.request(path);
}

beforeEach(() => {
  vi.mocked(registry.getNode).mockClear().mockResolvedValue(nodeRow());
  fakeSites = [];
  fakeDepths = new Map();
});

describe('GET /api/nodes/:id/grn-sites', () => {
  it('ranks by distance when the node has an antenna pin', async () => {
    vi.mocked(registry.getNode).mockResolvedValue(nodeRow({ lat: -33.7, lon: 150.3 }));
    fakeSites = [
      site({ name: 'Far', lat: -34.6, lon: 150.3 }),   // ~100 km south
      site({ name: 'Near', lat: -33.71, lon: 150.3 }), // ~1 km south
      site({ name: 'Middle', lat: -33.9, lon: 150.3 }),
    ];
    const res = await get('/api/nodes/node-1/grn-sites');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.located).toBe(true);
    expect(body.sites.map((s: { name: string }) => s.name)).toEqual(['Near', 'Middle', 'Far']);
    expect(body.sites[0].km).toBeGreaterThan(0);
    expect(body.sites[0].km).toBeLessThan(2);
    expect(body.sites[2].km).toBeGreaterThan(90);
  });

  it('falls back to council-border hops when there is no pin', async () => {
    fakeDepths = new Map([['Blue Mountains', 0], ['Lithgow', 1], ['Bathurst', 2]]);
    fakeSites = [
      site({ name: 'Two Away', lga: 'Bathurst' }),
      site({ name: 'Elsewhere', lga: 'Sydney' }),   // outside the rings
      site({ name: 'Home', lga: 'Blue Mountains' }),
      site({ name: 'Next Door', lga: 'Lithgow' }),
    ];
    const body = await (await get('/api/nodes/node-1/grn-sites')).json();
    expect(body.located).toBe(false);
    expect(body.nodeLga).toBe('Blue Mountains');
    expect(body.sites.map((s: { name: string }) => s.name)).toEqual(['Home', 'Next Door', 'Two Away', 'Elsewhere']);
    expect(body.sites.map((s: { ring: number | null }) => s.ring)).toEqual([0, 1, 2, null]);
    expect(body.sites[0].km).toBeNull();
  });

  it('returns sites with no usable control channel, flagged rather than hidden', async () => {
    fakeSites = [site({ name: 'Mystery', mhz: null, rawCc: 'TBA' })];
    const body = await (await get('/api/nodes/node-1/grn-sites')).json();
    expect(body.sites).toHaveLength(1);
    expect(body.sites[0]).toMatchObject({ name: 'Mystery', mhz: null });
    expect(body.sites[0].note).toContain('TBA');
  });

  it('carries the alternate control channel so it can ride along on the channel', async () => {
    fakeSites = [site({ name: 'Two CCs', mhz: 422.375, altMhz: 421 })];
    const body = await (await get('/api/nodes/node-1/grn-sites')).json();
    expect(body.sites[0]).toMatchObject({ mhz: 422.375, altMhz: 421, note: null });
  });

  it('works for a node with no location at all', async () => {
    vi.mocked(registry.getNode).mockResolvedValue(nodeRow({ lga: null }));
    fakeSites = [site({ name: 'A' }), site({ name: 'B' })];
    const body = await (await get('/api/nodes/node-1/grn-sites')).json();
    expect(body.nodeLga).toBeNull();
    expect(body.sites.map((s: { name: string }) => s.name)).toEqual(['A', 'B']);
  });

  it('404s for a node that does not exist', async () => {
    vi.mocked(registry.getNode).mockResolvedValue(null);
    expect((await get('/api/nodes/nope/grn-sites')).status).toBe(404);
  });
});
