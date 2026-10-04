/**
 * GRN candidate selection: the control-frequency parser over the dataset's
 * real mess, LGA adjacency from shared polygon vertices, and the
 * LGA-neighbourhood filter with ring depth.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

let resultQueue: Array<{ rows: unknown[] }> = [];
const fakePool = {
  query: vi.fn(async () => resultQueue.shift() ?? { rows: [] }),
};
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => fakePool),
}));

// Site→LGA tagging calls boundaryForPoint per site — fake it by longitude band.
vi.mock('../../../src/api/boundaries.js', () => ({
  boundaryForPoint: vi.fn(async (_kind: string, lon: number) => {
    if (lon < 150) return { name: 'Alpha', shortName: 'Alpha', state: 'NSW' };
    if (lon < 151) return { name: 'Beta', shortName: 'Beta', state: 'NSW' };
    return { name: 'Gamma', shortName: 'Gamma', state: 'NSW' };
  }),
}));

const { parseControlMhz, lgaAdjacency, lgaNeighbourhood, candidatesForNode, _resetGrnCandidateCaches } =
  await import('../../../src/services/grnCandidates.js');

beforeEach(() => {
  resultQueue = [];
  _resetGrnCandidateCaches();
});

describe('parseControlMhz', () => {
  it('takes the first plausible MHz from the real dataset shapes', () => {
    expect(parseControlMhz('422.3750')).toBe(422.375);
    expect(parseControlMhz('416.1250 - 419.0125 (416.3000 HH)')).toBe(416.125);
    expect(parseControlMhz('new 419.0875 old 416.2500')).toBe(419.0875);
    expect(parseControlMhz('416.1875 Low Power')).toBe(416.1875);
    expect(parseControlMhz('415.262500 ALT CC')).toBe(415.2625);
  });
  it('refuses placeholders and junk', () => {
    expect(parseControlMhz('TBA')).toBeNull();
    expect(parseControlMhz('NA')).toBeNull();
    expect(parseControlMhz('Pending Confirmation')).toBeNull();
    expect(parseControlMhz('See below')).toBeNull();
    expect(parseControlMhz('')).toBeNull();
    expect(parseControlMhz(undefined)).toBeNull();
    expect(parseControlMhz(422.375)).toBeNull(); // strings only — jsonb may carry anything
  });
});

// Two squares sharing an edge (Alpha/Beta share x=1 vertices), Gamma detached.
const square = (x0: number): unknown => ({
  type: 'Polygon',
  coordinates: [[[x0, 0], [x0 + 1, 0], [x0 + 1, 1], [x0, 1], [x0, 0]]],
});

describe('lgaAdjacency / lgaNeighbourhood', () => {
  it('shared vertices make neighbours; detached polygons stay isolated', async () => {
    resultQueue = [{ rows: [
      { name: 'Alpha', geom: square(0) },
      { name: 'Beta', geom: square(1) },
      { name: 'Gamma', geom: square(5) },
    ] }];
    const adj = await lgaAdjacency('NSW');
    expect([...(adj.get('Alpha') ?? [])]).toEqual(['Beta']);
    expect([...(adj.get('Beta') ?? [])]).toEqual(['Alpha']);
    expect(adj.get('Gamma')).toBeUndefined();
  });

  it('ring depth walks the chain', async () => {
    // A-B-C-D in a line, each sharing an edge with the next.
    resultQueue = [{ rows: [
      { name: 'A', geom: square(0) }, { name: 'B', geom: square(1) },
      { name: 'C', geom: square(2) }, { name: 'D', geom: square(3) },
    ] }];
    expect([...(await lgaNeighbourhood('NSW', 'A', 1))].sort()).toEqual(['A', 'B']);
    _resetGrnCandidateCaches();
    resultQueue = [{ rows: [
      { name: 'A', geom: square(0) }, { name: 'B', geom: square(1) },
      { name: 'C', geom: square(2) }, { name: 'D', geom: square(3) },
    ] }];
    expect([...(await lgaNeighbourhood('NSW', 'A', 3))].sort()).toEqual(['A', 'B', 'C', 'D']);
  });
});

describe('candidatesForNode', () => {
  it('filters by LGA neighbourhood, parses freqs, reports skips', async () => {
    resultQueue = [
      // lgaNeighbourhood runs first: boundaries read — Alpha/Beta adjacent
      { rows: [
        { name: 'Alpha', geom: square(0) },
        { name: 'Beta', geom: square(1) },
        { name: 'Gamma', geom: square(5) },
      ] },
      // then taggedSites: grn_sites read (lon decides the faked LGA)
      { rows: [
        { id: 1, data: { NAME: 'Good Hill', Latitude: -33, Longitude: 149.5, 'Control Channel': '422.3750', 'Alt Control Channel': '421.0000', 'GRN Site ID #': '004-083' } },
        { id: 2, data: { NAME: 'Broken CC', Latitude: -33, Longitude: 149.6, 'Control Channel': 'TBA' } },
        { id: 3, data: { NAME: 'Next Door', Latitude: -33, Longitude: 150.5, 'Control Channel': '419.5000' } },
        { id: 4, data: { NAME: 'Far Away', Latitude: -33, Longitude: 151.5, 'Control Channel': '418.0000' } },
      ] },
    ];
    const { candidates, skipped } = await candidatesForNode({ state: 'NSW', lga: 'Alpha' }, 1);
    expect(candidates.map((c) => c.name)).toEqual(['Good Hill', 'Next Door']); // own LGA first
    expect(candidates[0]).toMatchObject({ mhz: 422.375, altMhz: 421, grnKey: '004-083' });
    expect(skipped).toHaveLength(1);
    expect(skipped[0]!.name).toBe('Broken CC');
    expect(skipped[0]!.note).toContain('TBA');
  });

  it('returns nothing without an LGA', async () => {
    const { candidates, skipped } = await candidatesForNode({ state: 'NSW', lga: null }, 1);
    expect(candidates).toEqual([]);
    expect(skipped).toEqual([]);
  });
});
