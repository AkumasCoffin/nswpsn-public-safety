/**
 * Energex and Ergon outage tests (sources/outages/energyq.ts).
 *
 * The features are real records from the two distributors' files (13 November
 * 2026), with outlines and street lists cut short. The fetch tests mock the
 * shared HTTP module, so neither site is ever asked.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// vi.hoisted: see ../endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import register, { energyqUrl, fetchEnergex, fetchErgon, parseEnergyq } from '../../../../src/sources/outages/energyq.js';
import { PART_KEEP_MS, _resetKeptParts, withStatus } from '../../../../src/sources/outages/common.js';
import * as registry from '../../../../src/services/sourceRegistry.js';

const NOW = Date.parse('2026-11-13T08:40:00Z');

const file = (company: string, state: 'Current' | 'Future', features: unknown[]) => ({
  type: 'FeatureCollection',
  name: 'outages',
  features,
  company,
  state,
  extracted: 1794559200.588725,
});

const ENERGEX_OUTLINE = [[152.418643, -28.001916], [152.474846, -28.0016], [152.564405, -27.976943], [152.44721, -27.929974], [152.418643, -28.001916]];
const ENERGEX_UNPLANNED = {
  type: 'Feature',
  geometry: {
    type: 'GeometryCollection',
    geometries: [
      { type: 'Point', coordinates: [152.492373, -27.966073] },
      { type: 'Polygon', coordinates: [ENERGEX_OUTLINE] },
    ],
  },
  properties: {
    EVENT_ID: 'INCD-997760-g',
    TYPE: 'UNPLANNED',
    CUSTOMERS_AFFECTED: '265',
    REASON: 'Under Investigation',
    STATUS: 'Awaiting',
    START: '5:50PM 13 Nov 2026',
    EST_FIX_TIME: '8:30PM 13 Nov 2026',
    STREETS: 'ADAM DR, CARTER RD, CUNNINGHAM HWY',
    SUBURBS: 'ARATULA, FRAZERVIEW, MOORANG, MORWINCHA, TAROME',
    ETR_FLAG: 'N',
  },
};

const ERGON_OUTLINE = [[152.24641, -28.064889], [152.244097, -28.063674], [152.243402, -28.063831], [152.24358, -28.064448]];
/** No estimate yet: Ergon writes words where the time would be. */
const ERGON_UNPLANNED = {
  type: 'Feature',
  geometry: {
    type: 'GeometryCollection',
    geometries: [
      { type: 'Point', coordinates: [152.244995, -28.064668] },
      { type: 'Polygon', coordinates: [ERGON_OUTLINE] },
    ],
  },
  properties: {
    EVENT_ID: '26SW14914',
    TYPE: 'UNPLANNED',
    CUSTOMERS_AFFECTED: '3',
    REASON: 'The loss of supply is due to damage caused by storm activity.',
    STATUS: 'Outage reported',
    START: '5:37PM 13 Nov 2026',
    EST_FIX_TIME: 'Under Investigation',
    STREETS: 'DAYS RD',
    SUBURBS: 'MARYVALE',
  },
};

/** Work spread over seven weeks: in the current-planned file and the future one at once. */
const ERGON_PLANNED_NOW = {
  type: 'Feature',
  geometry: { type: 'Point', coordinates: [139.492636, -20.731297] },
  properties: {
    EVENT_ID: '26NQ11733',
    TYPE: 'PLANNED',
    CUSTOMERS_AFFECTED: '41',
    REASON: 'The loss of supply is to connect new customers to the electricity network',
    STATUS: 'Crews working on-site',
    START: '8:49AM 15 Oct 2026',
    EST_FIX_TIME: '12:00PM 03 Dec 2026',
    STREETS: 'BURKE ST, CAMOOWEAL ST, DORA ST, DOREEN ST, ISA ST, LANDY ST',
    SUBURBS: 'MORNINGTON, MOUNT ISA, MOUNT ISA CITY',
  },
};

const ERGON_FUTURE = {
  type: 'Feature',
  geometry: { type: 'Point', coordinates: [145.08189, -15.282037] },
  properties: {
    EVENT_ID: '26FN10579',
    TYPE: 'PLANNED',
    CUSTOMERS_AFFECTED: '10',
    REASON: 'The loss of supply is to replace power poles and improve the electricity network',
    STATUS: 'Work scheduled',
    START: '8:00AM 16 Nov 2026',
    EST_FIX_TIME: '2:00PM 16 Nov 2026',
    STREETS: 'MCIVOR RD',
    SUBURBS: 'HOPE VALE',
  },
};

const ERGON_ON_HOLD = {
  type: 'Feature',
  geometry: { type: 'Point', coordinates: [143.3432, -12.7869] },
  properties: {
    EVENT_ID: '26FN10214',
    TYPE: 'PLANNED',
    CUSTOMERS_AFFECTED: '132',
    REASON: 'The loss of supply is to replace power poles and improve the electricity network',
    STATUS: 'On hold until further notice',
    START: '8:00AM 14 Nov 2026',
    EST_FIX_TIME: '2:00PM 14 Nov 2026',
    STREETS: 'ILLA ST, ILWAY ST',
    SUBURBS: 'LOCKHART RIVER',
  },
};

const withProps = (feature: { properties: Record<string, unknown> }, props: Record<string, unknown>) => ({
  ...feature,
  properties: { ...feature.properties, ...props },
});

describe('parseEnergyq', () => {
  it('an Energex unplanned outage: the marker from the point, the outline from the polygon', () => {
    expect(parseEnergyq(file('Energex', 'Current', [ENERGEX_UNPLANNED]), 'energex')).toEqual([
      {
        id: 'INCD-997760-g',
        source: 'energex',
        distributor: 'energex',
        state: 'QLD',
        kind: 'unplanned',
        area: 'Aratula, Frazerview, Moorang, Morwincha, Tarome',
        customers_affected: 265,
        cause: 'Under Investigation',
        // Brisbane is UTC+10 all year.
        start_at: '2026-11-13T07:50:00.000Z',
        restore_at: '2026-11-13T10:30:00.000Z',
        updated_at: null,
        lat: -27.966073,
        lon: 152.492373,
        polygons: [ENERGEX_OUTLINE],
        details: { streets: 'Adam Dr, Carter Rd, Cunningham Hwy', status: 'Awaiting', etr_flag: 'N' },
      },
    ]);
  });

  it("an Ergon unplanned outage: its status phrase, and its words for no estimate", () => {
    const [o, ...rest] = parseEnergyq(file('Ergon', 'Current', [ERGON_UNPLANNED]), 'ergon');
    expect(rest).toEqual([]);
    expect(o).toMatchObject({
      id: '26SW14914',
      source: 'ergon',
      distributor: 'ergon',
      state: 'QLD',
      kind: 'unplanned',
      area: 'Maryvale',
      customers_affected: 3,
      cause: 'The loss of supply is due to damage caused by storm activity.',
      start_at: '2026-11-13T07:37:00.000Z',
      restore_at: null,
      lat: -28.064668,
      lon: 152.244995,
      polygons: [ERGON_OUTLINE],
      details: { streets: 'Days Rd', status: 'Outage reported', est_fix: 'Under Investigation' },
    });
    expect(o).not.toHaveProperty('status');
  });

  it('planned work: a point only, and no status of its own when the start can be read', () => {
    const out = parseEnergyq(file('Ergon', 'Future', [ERGON_PLANNED_NOW, ERGON_FUTURE, ERGON_ON_HOLD]), 'ergon');
    expect(out.map((o) => [o.id, o.kind, o.start_at, o.restore_at, o.lat, o.lon, o.polygons.length])).toEqual([
      ['26NQ11733', 'planned', '2026-10-14T22:49:00.000Z', '2026-12-03T02:00:00.000Z', -20.731297, 139.492636, 0],
      ['26FN10579', 'planned', '2026-11-15T22:00:00.000Z', '2026-11-16T04:00:00.000Z', -15.282037, 145.08189, 0],
      // On hold is still listed, with its times: kept, and the phrase with it.
      ['26FN10214', 'planned', '2026-11-13T22:00:00.000Z', '2026-11-14T04:00:00.000Z', -12.7869, 143.3432, 0],
    ]);
    expect(out[0]!.area).toBe('Mornington, Mount Isa, Mount Isa City');
    expect(out[2]!.details).toEqual({ streets: 'Illa St, Ilway St', status: 'On hold until further notice' });
    for (const o of out) expect(o).not.toHaveProperty('status');
    // The clock decides: under way since October, and to come.
    expect(out.map((o) => withStatus(o, NOW).status)).toEqual(['active', 'scheduled', 'scheduled']);
  });

  it("planned work whose start cannot be read takes the future file's word, and only then", () => {
    const unread = withProps(ERGON_FUTURE, { START: 'TBA' });
    expect(parseEnergyq(file('Ergon', 'Future', [unread]), 'ergon')[0]).toMatchObject({ start_at: null, status: 'scheduled' });
    expect(parseEnergyq(file('Ergon', 'Current', [unread]), 'ergon')[0]).not.toHaveProperty('status');
    expect(parseEnergyq(file('Ergon', 'Future', [withProps(ERGON_UNPLANNED, { START: '' })]), 'ergon')[0]).not.toHaveProperty('status');
  });

  it('drops work that is cancelled or completed, and outages restored', () => {
    const over = ['Cancelled', 'Completed', 'CANCELLED', 'Work completed', 'Power restored'].map((STATUS, i) =>
      withProps(ERGON_FUTURE, { EVENT_ID: `over-${i}`, STATUS }),
    );
    const out = parseEnergyq(file('Energex', 'Future', [...over, withProps(ERGON_FUTURE, { STATUS: 'Scheduled' })]), 'energex');
    expect(out.map((o) => o.id)).toEqual(['26FN10579']);
  });

  it('reads geometry in the other shapes GeoJSON allows, and does without when there is none', () => {
    const second = [[153, -27], [153.1, -27], [153.1, -27.1], [153, -27]];
    const geom = (geometry: unknown) => {
      const o = parseEnergyq(file('Energex', 'Current', [{ ...ENERGEX_UNPLANNED, geometry }]), 'energex')[0]!;
      return [o.lat, o.lon, o.polygons];
    };
    // The polygon first, and two of them.
    expect(
      geom({
        type: 'GeometryCollection',
        geometries: [
          { type: 'Polygon', coordinates: [ENERGEX_OUTLINE] },
          { type: 'Point', coordinates: [152.5, -27.9] },
          { type: 'Polygon', coordinates: [second] },
        ],
      }),
    ).toEqual([-27.9, 152.5, [ENERGEX_OUTLINE, second]]);
    expect(geom({ type: 'GeometryCollection', geometries: [{ type: 'MultiPolygon', coordinates: [[ENERGEX_OUTLINE], [second]] }] })).toEqual([
      null,
      null,
      [ENERGEX_OUTLINE, second],
    ]);
    expect(geom({ type: 'Point', coordinates: ['152.5', '-27.9'] })).toEqual([-27.9, 152.5, []]);
    for (const g of [null, undefined, 'x', { type: 'GeometryCollection' }, { type: 'Point', coordinates: [152.5] }]) {
      expect(geom(g), JSON.stringify(g)).toEqual([null, null, []]);
    }
  });

  it('leaves the file time off the records, so a record is the same poll after poll', () => {
    const first = parseEnergyq(file('Energex', 'Current', [ENERGEX_UNPLANNED]), 'energex');
    const later = parseEnergyq({ ...file('Energex', 'Current', [ENERGEX_UNPLANNED]), extracted: 1794559500.1 }, 'energex');
    expect(later).toEqual(first);
    expect(JSON.stringify(first)).not.toContain('1794559200');
  });

  it('skips what is not a feature, and takes missing fields as empty', () => {
    const out = parseEnergyq(file('Energex', 'Current', [null, 'x', { type: 'Feature' }, { type: 'Feature', properties: { EVENT_ID: 'E1' } }]), 'energex');
    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject({ id: 'E1', kind: 'unplanned', area: '', customers_affected: 0, cause: '', start_at: null, lat: null, polygons: [], details: {} });
    expect(parseEnergyq(file('Energex', 'Current', []), 'energex')).toEqual([]);
  });

  it('throws when the answer is not a feature collection', () => {
    for (const bad of [null, 42, '<html>Just a moment...</html>', [], {}, { features: 'none' }]) {
      expect(() => parseEnergyq(bad, 'ergon'), JSON.stringify(bad)).toThrow('ergon: outage file is not a feature collection');
    }
  });
});

describe('fetchEnergex / fetchErgon', () => {
  const U = energyqUrl('ergon', 'current_unplanned');
  const P = energyqUrl('ergon', 'current_planned');
  const F = energyqUrl('ergon', 'future_planned');

  beforeEach(() => {
    fetchJson.mockReset();
    _resetKeptParts();
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  const blocked = (): never => {
    throw new Error('HTTP 403');
  };
  /** Answers each URL from the map; a function is called per request. */
  function upstream(answers: Record<string, unknown>) {
    fetchJson.mockImplementation(async (url: string) => {
      if (!(url in answers)) throw new Error(`unexpected url ${url}`);
      const a = answers[url];
      return typeof a === 'function' ? (a as () => unknown)() : a;
    });
  }
  const urls = () => fetchJson.mock.calls.map((c) => String(c[0]));
  const ids = (rs: Array<{ id: string }>) => rs.map((r) => r.id);

  it("the files' addresses", () => {
    expect([U, P, F]).toEqual([
      'https://www.ergon.com.au/static/PRD/ee_map_current_unplanned.geojson',
      'https://www.ergon.com.au/static/PRD/ee_map_current_planned.geojson',
      'https://www.ergon.com.au/static/PRD/ee_map_future_planned.geojson',
    ]);
    expect(energyqUrl('energex', 'future_planned')).toBe('https://www.energex.com.au/static/PRD/ex_map_future_planned.geojson');
  });

  it('reads the three files in order, as plain requests with a 20 second timeout', async () => {
    upstream({
      [U]: file('Ergon', 'Current', [ERGON_UNPLANNED]),
      [P]: file('Ergon', 'Current', []),
      [F]: file('Ergon', 'Future', [ERGON_FUTURE]),
    });
    expect(ids(await fetchErgon())).toEqual(['26SW14914', '26FN10579']);
    expect(urls()).toEqual([U, P, F]);
    // Nothing sent to look like a browser.
    for (const call of fetchJson.mock.calls) expect(call[1]).toEqual({ timeoutMs: 20_000 });
  });

  it('asks for one file at a time, the next only when the one before has answered or failed', async () => {
    let open = 0;
    let most = 0;
    fetchJson.mockImplementation(async (url: string) => {
      open++;
      most = Math.max(most, open);
      await new Promise((r) => setTimeout(r, 5));
      open--;
      if (url === energyqUrl('energex', 'current_planned')) throw new Error('HTTP 403');
      return file('Energex', 'Current', []);
    });
    expect(await fetchEnergex()).toEqual([]);
    expect(most).toBe(1);
    expect(urls()).toEqual(['current_unplanned', 'current_planned', 'future_planned'].map((f) => energyqUrl('energex', f)));
  });

  it('an event in the current-planned and the future file is kept once, as the current file has it', async () => {
    upstream({
      [U]: file('Ergon', 'Current', []),
      [P]: file('Ergon', 'Current', [ERGON_PLANNED_NOW]),
      [F]: file('Ergon', 'Future', [ERGON_FUTURE, withProps(ERGON_PLANNED_NOW, { CUSTOMERS_AFFECTED: '99' })]),
    });
    const out = await fetchErgon();
    expect(ids(out)).toEqual(['26NQ11733', '26FN10579']);
    expect(out[0]!.customers_affected).toBe(41);
  });

  it('a file that is refused keeps its last good answer for half an hour; the others are read as usual', async () => {
    upstream({
      [U]: file('Ergon', 'Current', [ERGON_UNPLANNED]),
      [P]: file('Ergon', 'Current', [ERGON_PLANNED_NOW]),
      [F]: file('Ergon', 'Future', [ERGON_FUTURE]),
    });
    await fetchErgon();

    vi.setSystemTime(NOW + 5 * 60_000);
    upstream({ [U]: file('Ergon', 'Current', []), [P]: file('Ergon', 'Current', [ERGON_PLANNED_NOW]), [F]: blocked });
    fetchJson.mockClear();
    expect(ids(await fetchErgon())).toEqual(['26NQ11733', '26FN10579']);
    // One request per file: a refusal is not asked again within the poll.
    expect(urls()).toEqual([U, P, F]);

    // A challenge page that parsed as something is a failed read as well.
    vi.setSystemTime(NOW + 10 * 60_000);
    upstream({ [U]: file('Ergon', 'Current', []), [P]: file('Ergon', 'Current', []), [F]: { error: 'Just a moment...' } });
    expect(ids(await fetchErgon())).toEqual(['26FN10579']);

    // Half an hour after the last good read of it, the held copy is let go.
    vi.setSystemTime(NOW + PART_KEEP_MS);
    upstream({ [U]: file('Ergon', 'Current', [ERGON_UNPLANNED]), [P]: file('Ergon', 'Current', []), [F]: blocked });
    expect(ids(await fetchErgon())).toEqual(['26SW14914']);
  });

  it('every file refused: the held copies while there are any, and a failed poll when there are none', async () => {
    upstream({ [U]: blocked, [P]: blocked, [F]: blocked });
    await expect(fetchErgon()).rejects.toThrow('HTTP 403');

    upstream({
      [U]: file('Ergon', 'Current', [ERGON_UNPLANNED]),
      [P]: file('Ergon', 'Current', []),
      [F]: file('Ergon', 'Future', [ERGON_FUTURE]),
    });
    await fetchErgon();
    vi.setSystemTime(NOW + 5 * 60_000);
    upstream({ [U]: blocked, [P]: blocked, [F]: blocked });
    expect(ids(await fetchErgon())).toEqual(['26SW14914', '26FN10579']);

    vi.setSystemTime(NOW + PART_KEEP_MS + 1);
    await expect(fetchErgon()).rejects.toThrow('HTTP 403');
  });

  it("Energex's held files are not Ergon's", async () => {
    fetchJson.mockImplementation(async (url: string) => {
      if (url.includes('ergon.com.au')) throw new Error('HTTP 403');
      return file('Energex', 'Current', url.endsWith('current_unplanned.geojson') ? [ENERGEX_UNPLANNED] : []);
    });
    expect(ids(await fetchEnergex())).toEqual(['INCD-997760-g']);
    await expect(fetchErgon()).rejects.toThrow('HTTP 403');
  });
});

describe('register()', () => {
  afterEach(() => vi.restoreAllMocks());

  it('registers neither by default: both sites refuse this server', () => {
    delete process.env['ENERGEX_DISABLED'];
    delete process.env['ERGON_DISABLED'];
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    register();
    expect(spy).not.toHaveBeenCalled();
  });

  it('registers both as power sources, every five minutes, when switched on', () => {
    process.env['ENERGEX_DISABLED'] = 'false';
    process.env['ERGON_DISABLED'] = 'false';
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    try {
      register();
      expect(spy.mock.calls.map((c) => [c[0].name, c[0].family, c[0].intervalMs])).toEqual([
        ['energex', 'power', 300_000],
        ['ergon', 'power', 300_000],
      ]);
    } finally {
      delete process.env['ENERGEX_DISABLED'];
      delete process.env['ERGON_DISABLED'];
    }
  });
});
