/**
 * GRN candidate selection: the control-frequency parser over the dataset's
 * real mess, LGA adjacency from shared polygon vertices, the bulk site
 * tagging (LGA + suburb in one query per layer), and the LGA-neighbourhood
 * filter with ring depth.
 *
 * The boundary machinery is NOT mocked — the fake pool returns real polygons
 * and the real ray-cast decides what is inside what, which is the part worth
 * testing.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

/** A unit square with its lower-left corner at (x0, 0). */
const square = (x0: number): unknown => ({
  type: 'Polygon',
  coordinates: [[[x0, 0], [x0 + 1, 0], [x0 + 1, 1], [x0, 1], [x0, 0]]],
});

/** A boundary row as the tag query selects it, bbox included. */
const poly = (name: string, x0: number, state = 'NSW') => ({
  name, state, geom: square(x0),
  min_lon: x0, max_lon: x0 + 1, min_lat: 0, max_lat: 1,
});

// What the fake database holds, per test.
let grnRows: unknown[] = [];
let lgaPolys: unknown[] = [];
let localityPolys: unknown[] = [];
let queries: string[] = [];

const fakePool = {
  query: vi.fn(async (sql: string, params: unknown[] = []) => {
    queries.push(sql);
    if (sql.includes('FROM grn_sites')) return { rows: grnRows };
    if (sql.includes('WITH pts(lon, lat)')) {
      // The kind is the last parameter of the bulk tag query.
      const kind = params[params.length - 1];
      return { rows: kind === 'lga' ? lgaPolys : localityPolys };
    }
    if (sql.includes("kind = 'lga'")) return { rows: lgaPolys };
    return { rows: [] };
  }),
};
vi.mock('../../../src/db/pool.js', () => ({ getPool: vi.fn(async () => fakePool) }));

const { parseControlMhz, lgaAdjacency, lgaNeighbourhood, lgaRingDepths, candidatesForNode, allTaggedSites, _resetGrnCandidateCaches } =
  await import('../../../src/services/grnCandidates.js');

beforeEach(() => {
  grnRows = [];
  lgaPolys = [];
  localityPolys = [];
  queries = [];
  fakePool.query.mockClear();
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

describe('lgaAdjacency / ring depth', () => {
  it('shared vertices make neighbours; detached polygons stay isolated', async () => {
    lgaPolys = [poly('Alpha', 0), poly('Beta', 1), poly('Gamma', 5)];
    const adj = await lgaAdjacency('NSW');
    expect([...(adj.get('Alpha') ?? [])]).toEqual(['Beta']);
    expect([...(adj.get('Beta') ?? [])]).toEqual(['Alpha']);
    expect(adj.get('Gamma')).toBeUndefined();
  });

  it('ring depth walks the chain and counts the hops', async () => {
    lgaPolys = [poly('A', 0), poly('B', 1), poly('C', 2), poly('D', 3)];
    expect([...(await lgaNeighbourhood('NSW', 'A', 1))].sort()).toEqual(['A', 'B']);
    expect([...(await lgaNeighbourhood('NSW', 'A', 3))].sort()).toEqual(['A', 'B', 'C', 'D']);
    const depths = await lgaRingDepths('NSW', 'A', 4);
    expect([...depths.entries()].sort()).toEqual([['A', 0], ['B', 1], ['C', 2], ['D', 3]]);
  });

  it('caches the adjacency rather than re-reading the boundaries each time', async () => {
    lgaPolys = [poly('A', 0), poly('B', 1)];
    await lgaAdjacency('NSW');
    await lgaAdjacency('NSW');
    expect(queries.filter((q) => q.includes("kind = 'lga'"))).toHaveLength(1);
  });
});

describe('site tagging', () => {
  const site = (name: string, lon: number) => ({
    id: 1,
    data: { NAME: name, Latitude: 0.5, Longitude: lon, 'Control Channel': '422.3750' },
  });

  it('tags every site in one query per layer, not one per site', async () => {
    grnRows = [site('One', 0.5), site('Two', 1.5), site('Three', 2.5)];
    lgaPolys = [poly('Alpha', 0), poly('Beta', 1), poly('Gamma', 2)];
    localityPolys = [poly('Smallville', 0), poly('Bigtown', 1), poly('Elsewhere', 2)];

    const sites = await allTaggedSites();
    expect(sites.map((s) => [s.name, s.lga, s.suburb, s.state])).toEqual([
      ['One', 'Alpha', 'Smallville', 'NSW'],
      ['Two', 'Beta', 'Bigtown', 'NSW'],
      ['Three', 'Gamma', 'Elsewhere', 'NSW'],
    ]);
    // Three sites, two layers: two tag queries, not six.
    expect(queries.filter((q) => q.includes('WITH pts(lon, lat)'))).toHaveLength(2);
  });

  it('leaves a site outside every polygon untagged rather than guessing', async () => {
    grnRows = [site('Offshore', 40)];
    lgaPolys = [poly('Alpha', 0)];
    const sites = await allTaggedSites();
    expect(sites[0]).toMatchObject({ name: 'Offshore', lga: null, suburb: null, state: null });
  });

  it('skips sites with no coordinates without upsetting the batch', async () => {
    grnRows = [
      { id: 1, data: { NAME: 'Nowhere', 'Control Channel': '422.3750' } },
      site('Somewhere', 0.5),
    ];
    lgaPolys = [poly('Alpha', 0)];
    const sites = await allTaggedSites();
    expect(sites.map((s) => [s.name, s.lga])).toEqual([['Nowhere', null], ['Somewhere', 'Alpha']]);
  });

  it('caches the tagged set', async () => {
    grnRows = [site('One', 0.5)];
    lgaPolys = [poly('Alpha', 0)];
    await allTaggedSites();
    await allTaggedSites();
    expect(queries.filter((q) => q.includes('FROM grn_sites'))).toHaveLength(1);
  });
});

describe('candidatesForNode', () => {
  it('filters by LGA neighbourhood, parses freqs, reports skips', async () => {
    lgaPolys = [poly('Alpha', 0), poly('Beta', 1), poly('Gamma', 5)];
    grnRows = [
      { id: 1, data: { NAME: 'Good Hill', Latitude: 0.5, Longitude: 0.5, 'Control Channel': '422.3750', 'Alt Control Channel': '421.0000', 'GRN Site ID #': '004-083' } },
      { id: 2, data: { NAME: 'Broken CC', Latitude: 0.5, Longitude: 0.6, 'Control Channel': 'TBA' } },
      { id: 3, data: { NAME: 'Next Door', Latitude: 0.5, Longitude: 1.5, 'Control Channel': '419.5000' } },
      { id: 4, data: { NAME: 'Far Away', Latitude: 0.5, Longitude: 5.5, 'Control Channel': '418.0000' } },
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
