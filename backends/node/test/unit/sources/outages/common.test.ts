/**
 * What every power-outage source shares (sources/outages/common.ts).
 *
 * Pinned here: the table of distributors; zonedUtc across every way a
 * distributor writes a time, in each state's zone, in summer and winter and on
 * the days the clocks change; when planned work counts as started; the history
 * row an outage becomes, and that it is the same row poll after poll;
 * registerOutages and its <NAME>_DISABLED switch; keepParts carrying a failed
 * part's last good records, and letting go of them after half an hour.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createHash } from 'node:crypto';

import {
  DISTRIBUTOR_INFO,
  DISTRIBUTORS,
  OUTAGE_SOURCES,
  OUTAGE_STATES,
  OUTAGE_STORES,
  PART_KEEP_MS,
  STATE_ZONE,
  _resetKeptParts,
  isScheduled,
  keepParts,
  num,
  outageArchiveItems,
  outageOf,
  outerRings,
  registerOutages,
  ring,
  text,
  titleCase,
  withStatus,
  zonedUtc,
  type Outage,
  type OutageRecord,
} from '../../../../src/sources/outages/common.js';
import { getSource } from '../../../../src/services/sourceRegistry.js';
import { liveStore } from '../../../../src/store/live.js';

const NOW = Date.UTC(2026, 10, 13, 2, 0, 0); // 2026-11-13T02:00:00Z

describe('the distributors', () => {
  it('twelve of them, NSW first, each with a name and a state', () => {
    expect(DISTRIBUTORS).toEqual([
      'endeavour', 'essential', 'ausgrid', 'ausnet', 'citipower', 'powercor', 'jemena', 'energex', 'ergon', 'sapn',
      'westernpower', 'horizon',
    ]);
    expect(Object.keys(DISTRIBUTOR_INFO)).toEqual([...DISTRIBUTORS]);
    expect(Object.fromEntries(DISTRIBUTORS.map((d) => [d, `${DISTRIBUTOR_INFO[d].name} / ${DISTRIBUTOR_INFO[d].state}`]))).toEqual({
      endeavour: 'Endeavour Energy / NSW',
      essential: 'Essential Energy / NSW',
      ausgrid: 'Ausgrid / NSW',
      ausnet: 'AusNet / VIC',
      citipower: 'CitiPower / VIC',
      powercor: 'Powercor / VIC',
      jemena: 'Jemena / VIC',
      energex: 'Energex / QLD',
      ergon: 'Ergon Energy / QLD',
      sapn: 'SA Power Networks / SA',
      westernpower: 'Western Power / WA',
      horizon: 'Horizon Power / WA',
    });
  });

  it('the ones that store the shared shape archive under their own names', () => {
    expect(OUTAGE_STORES).toEqual(['ausnet', 'citipower', 'powercor', 'jemena', 'energex', 'ergon', 'sapn', 'westernpower', 'horizon']);
    for (const store of OUTAGE_STORES) {
      expect(DISTRIBUTORS).toContain(store);
      expect(OUTAGE_SOURCES).toContain(store);
    }
    expect(OUTAGE_SOURCES.slice(0, 6)).toEqual([
      'endeavour_current', 'endeavour_planned', 'essential_current', 'essential_planned', 'essential_future', 'ausgrid',
    ]);
  });

  it('every state has a zone, and every distributor a state that is one', () => {
    expect(OUTAGE_STATES).toEqual(['NSW', 'VIC', 'QLD', 'SA', 'WA', 'NT', 'ACT']);
    expect(Object.keys(STATE_ZONE)).toEqual([...OUTAGE_STATES]);
    for (const d of DISTRIBUTORS) expect(OUTAGE_STATES).toContain(DISTRIBUTOR_INFO[d].state);
  });
});

describe('zonedUtc', () => {
  const MEL = 'Australia/Melbourne';

  it('a time with a zone is that instant, whatever zone is given', () => {
    for (const zone of [MEL, 'Australia/Perth', 'Australia/Brisbane']) {
      expect(zonedUtc('2026-11-13T06:55:00Z', zone)).toBe('2026-11-13T06:55:00.000Z');
      expect(zonedUtc('2026-11-13T06:55:00+11:00', zone)).toBe('2026-11-12T19:55:00.000Z');
      expect(zonedUtc('2026-11-13 06:55:00+11:00', zone)).toBe('2026-11-12T19:55:00.000Z');
      expect(zonedUtc('2026-11-13T06:55+1100', zone)).toBe('2026-11-12T19:55:00.000Z');
      expect(zonedUtc('2026-11-13T06:55:00+08', zone)).toBe('2026-11-12T22:55:00.000Z');
      expect(zonedUtc('2026-11-13T06:55:00-03:30', zone)).toBe('2026-11-13T10:25:00.000Z');
      // Fractions of a second are kept to the millisecond.
      expect(zonedUtc('2026-11-02T01:10:13.919746+00:00', zone)).toBe('2026-11-02T01:10:13.919Z');
      expect(zonedUtc('2026-11-02T01:10:13.9Z', zone)).toBe('2026-11-02T01:10:13.900Z');
    }
  });

  it('Western Power: compact, with its offset', () => {
    expect(zonedUtc('20261113T075326+08:00', 'Australia/Perth')).toBe('2026-11-12T23:53:26.000Z');
    expect(zonedUtc('20261113T075326Z', 'Australia/Perth')).toBe('2026-11-13T07:53:26.000Z');
    // Without an offset it is Perth's clock.
    expect(zonedUtc('20261113T075326', 'Australia/Perth')).toBe('2026-11-12T23:53:26.000Z');
    expect(zonedUtc('20261113T0753', 'Australia/Perth')).toBe('2026-11-12T23:53:00.000Z');
  });

  it('naive ISO, with a T or a space, seconds and fractions optional', () => {
    expect(zonedUtc('2026-11-13T06:55', MEL)).toBe('2026-11-12T19:55:00.000Z');
    expect(zonedUtc('2026-11-13T06:55:30', MEL)).toBe('2026-11-12T19:55:30.000Z');
    expect(zonedUtc('2026-11-13T06:55:30.25', MEL)).toBe('2026-11-12T19:55:30.250Z');
    expect(zonedUtc('2026-11-13 06:55:30', MEL)).toBe('2026-11-12T19:55:30.000Z');
    expect(zonedUtc('  2026-11-13T06:55:30  ', MEL)).toBe('2026-11-12T19:55:30.000Z');
  });

  it('AusNet: a date and a 12-hour clock', () => {
    expect(zonedUtc('2026-11-13 06:55PM', MEL)).toBe('2026-11-13T07:55:00.000Z');
    expect(zonedUtc('2026-11-13 06:55AM', MEL)).toBe('2026-11-12T19:55:00.000Z');
    expect(zonedUtc('2026-11-13 12:05AM', MEL)).toBe('2026-11-12T13:05:00.000Z');
    expect(zonedUtc('2026-11-13 12:05PM', MEL)).toBe('2026-11-13T01:05:00.000Z');
    expect(zonedUtc('2026-11-13 6:55 pm', MEL)).toBe('2026-11-13T07:55:00.000Z');
  });

  it('day/month/year, 24-hour and 12-hour', () => {
    expect(zonedUtc('13/11/2026 15:53', MEL)).toBe('2026-11-13T04:53:00.000Z');
    expect(zonedUtc('13/11/2026 15:53:20', MEL)).toBe('2026-11-13T04:53:20.000Z');
    expect(zonedUtc('13/11/2026 03:53 PM', MEL)).toBe('2026-11-13T04:53:00.000Z');
    expect(zonedUtc('13/11/2026 03:53 AM', MEL)).toBe('2026-11-12T16:53:00.000Z');
    expect(zonedUtc('3/1/2027 9:05', MEL)).toBe('2027-01-02T22:05:00.000Z');
  });

  it('CitiPower and Powercor: the clock, then day-month-year', () => {
    expect(zonedUtc('15:53 13-11-2026', MEL)).toBe('2026-11-13T04:53:00.000Z');
    expect(zonedUtc('00:05 01-07-2026', MEL)).toBe('2026-06-30T14:05:00.000Z');
  });

  it('Energex and Ergon: the clock, then the day, the month by name and the year', () => {
    const BNE = 'Australia/Brisbane';
    expect(zonedUtc('5:50PM 13 Nov 2026', BNE)).toBe('2026-11-13T07:50:00.000Z');
    expect(zonedUtc('12:00AM 1 Jan 2027', BNE)).toBe('2026-12-31T14:00:00.000Z');
    expect(zonedUtc('11:59 pm 28 February 2027', BNE)).toBe('2027-02-28T13:59:00.000Z');
    expect(zonedUtc('5:50PM 13 Sept 2026', BNE)).toBe('2026-09-13T07:50:00.000Z');
    expect(zonedUtc('5:50PM 13 Nod 2026', BNE)).toBeNull();
  });

  it("each state's zone, in summer and in winter", () => {
    const cases: Array<[zone: string, summer: string, winter: string]> = [
      // Daylight saving: +11:00 in January, +10:00 in July.
      ['Australia/Sydney', '2026-01-15T01:00:00.000Z', '2026-07-15T02:00:00.000Z'],
      ['Australia/Melbourne', '2026-01-15T01:00:00.000Z', '2026-07-15T02:00:00.000Z'],
      ['Australia/Hobart', '2026-01-15T01:00:00.000Z', '2026-07-15T02:00:00.000Z'],
      // Queensland has none: +10:00 all year.
      ['Australia/Brisbane', '2026-01-15T02:00:00.000Z', '2026-07-15T02:00:00.000Z'],
      // South Australia: +10:30 in January, +09:30 in July.
      ['Australia/Adelaide', '2026-01-15T01:30:00.000Z', '2026-07-15T02:30:00.000Z'],
      // Western Australia has none: +08:00 all year.
      ['Australia/Perth', '2026-01-15T04:00:00.000Z', '2026-07-15T04:00:00.000Z'],
      // The Northern Territory has none: +09:30 all year.
      ['Australia/Darwin', '2026-01-15T02:30:00.000Z', '2026-07-15T02:30:00.000Z'],
    ];
    for (const [zone, summer, winter] of cases) {
      expect(zonedUtc('2026-01-15T12:00:00', zone), `${zone} summer`).toBe(summer);
      expect(zonedUtc('15/07/2026 12:00', zone), `${zone} winter`).toBe(winter);
    }
    for (const zone of Object.values(STATE_ZONE)) expect(zonedUtc('2026-01-15T12:00:00', zone), zone).not.toBeNull();
  });

  it('the day the clocks go forward (4 October 2026, 02:00 becomes 03:00)', () => {
    for (const zone of ['Australia/Sydney', MEL, 'Australia/Hobart']) {
      expect(zonedUtc('2026-10-04T01:59:00', zone), zone).toBe('2026-10-03T15:59:00.000Z');
      expect(zonedUtc('2026-10-04T03:00:00', zone), zone).toBe('2026-10-03T16:00:00.000Z');
      expect(zonedUtc('2026-10-03T23:30:00', zone), zone).toBe('2026-10-03T13:30:00.000Z');
      expect(zonedUtc('2026-10-04T12:00:00', zone), zone).toBe('2026-10-04T01:00:00.000Z');
    }
    expect(zonedUtc('2026-10-04T01:59:00', 'Australia/Adelaide')).toBe('2026-10-03T16:29:00.000Z');
    expect(zonedUtc('2026-10-04T03:00:00', 'Australia/Adelaide')).toBe('2026-10-03T16:30:00.000Z');
    // Where there is no daylight saving the day is like any other.
    expect(zonedUtc('2026-10-04T02:30:00', 'Australia/Brisbane')).toBe('2026-10-03T16:30:00.000Z');
    expect(zonedUtc('2026-10-04T02:30:00', 'Australia/Perth')).toBe('2026-10-03T18:30:00.000Z');
  });

  it('the day the clocks go back (5 April 2026, 03:00 becomes 02:00)', () => {
    for (const zone of ['Australia/Sydney', MEL, 'Australia/Hobart']) {
      expect(zonedUtc('2026-04-05T01:59:00', zone), zone).toBe('2026-04-04T14:59:00.000Z');
      expect(zonedUtc('2026-04-05T03:00:00', zone), zone).toBe('2026-04-04T17:00:00.000Z');
      // 02:30 happens twice; it is read as the second one, in standard time.
      expect(zonedUtc('2026-04-05T02:30:00', zone), zone).toBe('2026-04-04T16:30:00.000Z');
    }
    expect(zonedUtc('2026-04-05T01:59:00', 'Australia/Adelaide')).toBe('2026-04-04T15:29:00.000Z');
    expect(zonedUtc('2026-04-05T03:00:00', 'Australia/Adelaide')).toBe('2026-04-04T17:30:00.000Z');
  });

  it('what is not a time is null', () => {
    const bad = [
      '', '   ', 'soon', 'TBA', '2026-11-13', '13/11/2026', '15:53', '2026-13-01T10:00:00', '2026-02-30T10:00:00',
      '31/04/2026 10:00', '31/13/2026 10:00:00', '2026-11-13T24:00:00', '2026-11-13T10:60:00', '2026-11-13T10:00:61',
      '2026-11-13 13:05PM', '2026-11-13 00:05AM', '2026-11-13T10:00:00+25:00', '2026-11-13T10:00:00 AEDT', '1763000000',
      null, undefined, 1763000000, new Date(NOW), {},
    ];
    for (const v of bad) expect(zonedUtc(v, MEL), String(v)).toBeNull();
  });

  it('a zone that is not one is a mistake in the source file, and throws', () => {
    expect(() => zonedUtc('2026-11-13T06:55:00', 'Australia/Nowhere')).toThrow();
    // Not reached when the time carries its own zone.
    expect(zonedUtc('2026-11-13T06:55:00Z', 'Australia/Nowhere')).toBe('2026-11-13T06:55:00.000Z');
  });
});

describe('reading what the files hold', () => {
  it('num: a finite number, also as text', () => {
    expect(num(12.5)).toBe(12.5);
    expect(num(' -37.81 ')).toBe(-37.81);
    expect(num(0)).toBe(0);
    for (const v of ['', ' ', 'abc', '12abc', Number.NaN, Infinity, null, undefined, true, [], {}]) expect(num(v), String(v)).toBeNull();
  });

  it('text: trimmed, a number as written, anything else empty', () => {
    expect(text('  Ryde ')).toBe('Ryde');
    expect(text(1234)).toBe('1234');
    for (const v of [null, undefined, true, {}, [], Number.NaN]) expect(text(v), String(v)).toBe('');
  });

  it('titleCase: only a name written all in capitals is changed', () => {
    expect(titleCase('MOUNT GAMBIER')).toBe('Mount Gambier');
    expect(titleCase("  O'CONNOR   NORTH ")).toBe("O'Connor North");
    expect(titleCase('WOY-WOY (EAST)')).toBe('Woy-Woy (East)');
    expect(titleCase('ST. KILDA/ELWOOD')).toBe('St. Kilda/Elwood');
    expect(titleCase('McKinnon')).toBe('McKinnon');
    expect(titleCase('Port Hedland')).toBe('Port Hedland');
    expect(titleCase(null)).toBe('');
  });

  it('ring: pairs, objects or text, as [longitude, latitude]', () => {
    const want = [[138.6, -34.9], [138.7, -34.9], [138.7, -35]];
    expect(ring(want)).toEqual(want);
    expect(ring([[-34.9, 138.6], [-34.9, 138.7], [-35, 138.7]], 'latlon')).toEqual(want);
    expect(ring([{ lat: -34.9, lng: 138.6 }, { lat: -34.9, lng: 138.7 }, { lat: -35, lng: 138.7 }])).toEqual(want);
    expect(ring([{ latitude: '-34.9', longitude: '138.6' }, { y: -34.9, x: 138.7 }, { lat: -35, lon: 138.7 }])).toEqual(want);
    expect(ring('138.6,-34.9 138.7,-34.9,0 138.7,-35')).toEqual(want);
    // A point that is not two numbers is dropped; fewer than three is no outline.
    expect(ring([[138.6, -34.9], ['x', -34.9], [138.7, -34.9], null, [138.7, -35]])).toEqual(want);
    expect(ring([[138.6, -34.9], [138.7, -34.9]])).toEqual([]);
    for (const v of [null, undefined, 5, {}, '', 'nothing']) expect(ring(v), String(v)).toEqual([]);
  });

  it('outerRings: a GeoJSON Polygon or MultiPolygon, without its holes', () => {
    const a = [[144.9, -37.8], [145, -37.8], [145, -37.9], [144.9, -37.8]];
    const hole = [[144.95, -37.82], [144.96, -37.82], [144.96, -37.83], [144.95, -37.82]];
    const b = [[146, -38], [146.1, -38], [146.1, -38.1], [146, -38]];
    expect(outerRings({ type: 'Polygon', coordinates: [a, hole] })).toEqual([a]);
    expect(outerRings({ type: 'MultiPolygon', coordinates: [[a, hole], [b]] })).toEqual([a, b]);
    expect(outerRings({ type: 'Feature', properties: {}, geometry: { type: 'Polygon', coordinates: [b] } })).toEqual([b]);
    expect(outerRings(JSON.stringify({ type: 'Polygon', coordinates: [a] }))).toEqual([a]);
    for (const v of [null, undefined, '', 'not json', { type: 'Point', coordinates: [144.9, -37.8] }, { type: 'Polygon' }, { type: 'Polygon', coordinates: [[[1, 2]]] }]) {
      expect(outerRings(v), JSON.stringify(v)).toEqual([]);
    }
  });
});

describe('outageOf, isScheduled and withStatus', () => {
  it("fills in the source, the distributor and its state, and what was left out", () => {
    expect(outageOf('sapn', { id: 12345, kind: 'unplanned' })).toEqual({
      id: '12345',
      source: 'sapn',
      distributor: 'sapn',
      state: 'SA',
      kind: 'unplanned',
      area: '',
      customers_affected: 0,
      cause: '',
      start_at: null,
      restore_at: null,
      updated_at: null,
      lat: null,
      lon: null,
      polygons: [],
      details: {},
    });
    const full = outageOf('horizon', {
      id: ' HP-1 ', kind: 'planned', status: 'scheduled', area: 'Broome', customers_affected: 41.6, cause: 'Maintenance',
      start_at: '2026-11-14T00:00:00.000Z', restore_at: '2026-11-14T04:00:00.000Z', updated_at: '2026-11-12T01:00:00.000Z',
      lat: -17.96, lon: 122.24, polygons: [[[122.2, -17.9], [122.3, -17.9], [122.3, -18]]], details: { region: 'Kimberley' },
    });
    expect(full).toMatchObject({
      id: 'HP-1', source: 'horizon', distributor: 'horizon', state: 'WA', status: 'scheduled', customers_affected: 42,
      lat: -17.96, lon: 122.24, details: { region: 'Kimberley' },
    });
    for (const store of OUTAGE_STORES) expect(outageOf(store, { id: 1, kind: 'planned' }).state).toBe(DISTRIBUTOR_INFO[store].state);
  });

  it('a position needs both numbers, and 0,0 is none', () => {
    const at = (lat: number | null, lon: number | null) => {
      const o = outageOf('ausnet', { id: 1, kind: 'unplanned', lat, lon });
      return [o.lat, o.lon];
    };
    expect(at(-37.8, 144.9)).toEqual([-37.8, 144.9]);
    expect(at(-37.8, null)).toEqual([null, null]);
    expect(at(null, 144.9)).toEqual([null, null]);
    expect(at(0, 0)).toEqual([null, null]);
    expect(at(Number.NaN, 144.9)).toEqual([null, null]);
    expect(outageOf('ausnet', { id: 1, kind: 'unplanned', customers_affected: -3 }).customers_affected).toBe(0);
  });

  it('isScheduled: planned, and not yet started', () => {
    const ahead = new Date(NOW + 3_600_000).toISOString();
    const earlier = new Date(NOW - 3_600_000).toISOString();
    expect(isScheduled({ kind: 'planned', start_at: ahead }, NOW)).toBe(true);
    expect(isScheduled({ kind: 'planned', start_at: earlier }, NOW)).toBe(false);
    // The moment it starts it is under way.
    expect(isScheduled({ kind: 'planned', start_at: new Date(NOW).toISOString() }, NOW)).toBe(false);
    expect(isScheduled({ kind: 'unplanned', start_at: ahead }, NOW)).toBe(false);
    expect(isScheduled({ kind: 'planned', start_at: null }, NOW)).toBe(false);
    expect(isScheduled({ kind: 'planned', start_at: 'soon' }, NOW)).toBe(false);
  });

  it('withStatus: decided by the clock each time, not kept from the last time', () => {
    const start = new Date(NOW + 3_600_000).toISOString();
    const planned = outageOf('energex', { id: 'P1', kind: 'planned', start_at: start });
    const before = withStatus(planned, NOW);
    expect(before.status).toBe('scheduled');
    // The stored record says scheduled; an hour on it is under way all the same.
    expect(withStatus(before, NOW + 3_600_000).status).toBe('active');
    expect(withStatus(before, NOW + 3_600_000)).toEqual({ ...planned, status: 'active' });
    // A source's own word does not outrank a start time that was read.
    expect(withStatus({ ...planned, status: 'active' }, NOW).status).toBe('scheduled');
    expect(withStatus(outageOf('energex', { id: 'U1', kind: 'unplanned', start_at: start, status: 'scheduled' }), NOW).status).toBe('active');
  });

  it('withStatus: planned work with no readable start keeps what the source said', () => {
    const noStart = (status?: 'active' | 'scheduled') => outageOf('ergon', { id: 'P2', kind: 'planned', ...(status ? { status } : {}) });
    expect(withStatus(noStart('scheduled'), NOW).status).toBe('scheduled');
    expect(withStatus(noStart('active'), NOW).status).toBe('active');
    expect(withStatus(noStart(), NOW).status).toBe('active');
    // Read again later, it has not changed its mind.
    expect(withStatus(withStatus(noStart('scheduled'), NOW), NOW + 86_400_000).status).toBe('scheduled');
  });
});

describe('outageArchiveItems', () => {
  const OUTAGE: Outage = withStatus(
    outageOf('ausnet', {
      id: 'INC-77',
      kind: 'planned',
      area: 'Wodonga',
      customers_affected: 120,
      cause: 'Pole replacement',
      start_at: new Date(NOW + 3_600_000).toISOString(),
      restore_at: new Date(NOW + 5 * 3_600_000).toISOString(),
      updated_at: '2026-11-13T01:55:00.000Z',
      lat: -36.12,
      lon: 146.88,
      polygons: [[[146.8, -36.1], [146.9, -36.1], [146.9, -36.2]]],
      details: { towns: ['Wodonga', 'Bandiana'] },
    }),
    NOW,
  );

  /** What the archive writer compares (store/archive.ts hashRowData): `data` without the names it leaves out. */
  const HASH_IGNORED = ['lastUpdated', 'upstreamId', 'streets', 'title', 'location_text'];
  const hashed = (data: unknown): string => {
    const kept = Object.fromEntries(Object.entries(data as Record<string, unknown>).filter(([k]) => !HASH_IGNORED.includes(k)).sort(([a], [b]) => a.localeCompare(b)));
    return createHash('sha1').update(JSON.stringify(kept)).digest('hex');
  };

  it('one row per outage: its id, position, kind and cause, and the state the writer can place it in', () => {
    const [row, ...rest] = outageArchiveItems([OUTAGE], 1_763_000_000, 'ausnet');
    expect(rest).toEqual([]);
    expect(row).toEqual({
      source: 'ausnet',
      source_id: 'INC-77',
      fetched_at: 1_763_000_000,
      lat: -36.12,
      lng: 146.88,
      category: 'planned',
      subcategory: 'Pole replacement',
      data: {
        id: 'INC-77',
        distributor: 'ausnet',
        state: 'VIC',
        kind: 'planned',
        area: 'Wodonga',
        customers_affected: 120,
        cause: 'Pole replacement',
        start_at: OUTAGE.start_at,
        restore_at: OUTAGE.restore_at,
        lastUpdated: '2026-11-13T01:55:00.000Z',
        latitude: -36.12,
        longitude: 146.88,
        details: { towns: ['Wodonga', 'Bandiana'] },
        title: 'Wodonga',
        location_text: 'Wodonga',
      },
    });
  });

  it('the same outage read again is the same row: no status, nothing of the poll', () => {
    const first = outageArchiveItems([OUTAGE], 1_763_000_000, 'ausnet')[0]!;
    // Three hours on: read at another time, the work has started, and the
    // distributor has re-stamped its file.
    const later = withStatus({ ...OUTAGE, updated_at: '2026-11-13T04:55:00.000Z' }, NOW + 3 * 3_600_000);
    expect(later.status).not.toBe(OUTAGE.status);
    const second = outageArchiveItems([later], 1_763_010_800, 'ausnet')[0]!;
    expect(hashed(second.data)).toBe(hashed(first.data));
    expect({ ...(second.data as object), lastUpdated: null }).toEqual({ ...(first.data as object), lastUpdated: null });
    for (const key of ['status', 'fetched_at', 'updated_at', 'polygons']) expect(first.data).not.toHaveProperty(key);
  });

  it('a change to the outage is a different row', () => {
    const first = outageArchiveItems([OUTAGE], 1, 'ausnet')[0]!;
    for (const change of [{ customers_affected: 80 }, { restore_at: new Date(NOW + 9 * 3_600_000).toISOString() }, { cause: 'Fault' }, { details: { towns: ['Wodonga'] } }]) {
      const next = outageArchiveItems([{ ...OUTAGE, ...change }], 2, 'ausnet')[0]!;
      expect(hashed(next.data), Object.keys(change)[0]).not.toBe(hashed(first.data));
    }
  });

  it('an outage with no area, cause, id or position still makes a row', () => {
    const bare = withStatus(outageOf('jemena', { id: '', kind: 'unplanned' }), NOW);
    expect(outageArchiveItems([bare], 5, 'jemena')).toEqual([
      {
        source: 'jemena', source_id: null, fetched_at: 5, lat: null, lng: null, category: 'unplanned', subcategory: null,
        data: {
          id: '', distributor: 'jemena', state: 'VIC', kind: 'unplanned', area: '', customers_affected: 0, cause: '',
          start_at: null, restore_at: null, lastUpdated: null, latitude: null, longitude: null, details: {}, title: 'Outage',
        },
      },
    ]);
    expect((outageArchiveItems([{ ...bare, cause: 'Storm' }], 5, 'jemena')[0]!.data as { title: string }).title).toBe('Storm');
  });

  it('anything but a list of records is no rows', () => {
    for (const v of [null, undefined, {}, 'x', { Markers: [] }]) expect(outageArchiveItems(v, 1, 'ausnet'), String(v)).toEqual([]);
    expect(outageArchiveItems([null, 'x', OUTAGE], 1, 'ausnet')).toHaveLength(1);
  });
});

describe('registerOutages', () => {
  const ENV = ['WESTERNPOWER_DISABLED', 'HORIZON_DISABLED', 'SAPN_DISABLED', 'JEMENA_DISABLED'];
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
    for (const k of ENV) delete process.env[k];
  });
  afterEach(() => {
    vi.useRealTimers();
    for (const k of ENV) delete process.env[k];
    for (const s of ['westernpower', 'horizon', 'sapn']) liveStore.delete(s);
  });

  it('registers a power source that archives one row per outage, each record with its status as of the poll', async () => {
    const ahead = new Date(NOW + 86_400_000).toISOString();
    const records: OutageRecord[] = [
      outageOf('westernpower', { id: 'A', kind: 'unplanned', area: 'Kalgoorlie' }),
      outageOf('westernpower', { id: 'B', kind: 'planned', start_at: ahead }),
      // The same outage in two of the distributor's files: once.
      outageOf('westernpower', { id: 'A', kind: 'unplanned', area: 'Kalgoorlie again' }),
    ];
    registerOutages({ name: 'westernpower', intervalMs: 180_000, fetch: async () => records });
    const def = getSource('westernpower')!;
    expect(def).toMatchObject({ name: 'westernpower', family: 'power', intervalMs: 180_000 });
    expect(def.archiveSource).toBeUndefined();

    const stored = (await def.fetch()) as Outage[];
    expect(stored.map((o) => [o.id, o.status, o.area])).toEqual([['A', 'active', 'Kalgoorlie'], ['B', 'scheduled', '']]);
    const rows = def.archiveItems!(stored, 99, 'westernpower');
    expect(rows.map((r) => [r.source, r.source_id, r.category])).toEqual([
      ['westernpower', 'A', 'unplanned'],
      ['westernpower', 'B', 'planned'],
    ]);
  });

  it('a fetch that fails, or answers something that is not a list, fails the poll', async () => {
    registerOutages({ name: 'horizon', intervalMs: 180_000, fetch: async () => { throw new Error('HTTP 403'); } });
    await expect(getSource('horizon')!.fetch()).rejects.toThrow('HTTP 403');
    registerOutages({ name: 'horizon', intervalMs: 180_000, fetch: async () => ({ outages: [] }) as unknown as OutageRecord[] });
    await expect(getSource('horizon')!.fetch()).rejects.toThrow('horizon: outages are not a list');
  });

  it('<NAME>_DISABLED=true: not registered, and its saved snapshot is deleted', () => {
    liveStore.set('sapn', [withStatus(outageOf('sapn', { id: 'old', kind: 'unplanned' }), NOW)]);
    process.env['SAPN_DISABLED'] = ' True ';
    const fetch = vi.fn(async () => []);
    registerOutages({ name: 'sapn', intervalMs: 180_000, fetch });
    expect(getSource('sapn')).toBeUndefined();
    expect(liveStore.get('sapn')).toBeNull();
    expect(fetch).not.toHaveBeenCalled();
  });

  it('any other value leaves it on, and the snapshot where it is', () => {
    liveStore.set('sapn', [withStatus(outageOf('sapn', { id: 'kept', kind: 'unplanned' }), NOW)]);
    for (const v of ['false', '', '1', 'yes']) {
      process.env['SAPN_DISABLED'] = v;
      registerOutages({ name: 'sapn', intervalMs: 180_000, fetch: async () => [] });
      expect(getSource('sapn'), v).toBeDefined();
    }
    expect(liveStore.getData<Outage[]>('sapn')?.[0]?.id).toBe('kept');
  });

  it('offByDefault: off unless <NAME>_DISABLED=false, for a distributor whose site refuses this server', () => {
    // A name no earlier test in this file has registered.
    liveStore.set('jemena', [withStatus(outageOf('jemena', { id: 'old', kind: 'unplanned' }), NOW)]);
    registerOutages({ name: 'jemena', intervalMs: 180_000, fetch: async () => [], offByDefault: true });
    expect(getSource('jemena')).toBeUndefined();
    expect(liveStore.get('jemena')).toBeNull();

    process.env['JEMENA_DISABLED'] = 'false';
    registerOutages({ name: 'jemena', intervalMs: 180_000, fetch: async () => [], offByDefault: true });
    expect(getSource('jemena')).toBeDefined();
  });
});

describe('keepParts', () => {
  beforeEach(() => {
    _resetKeptParts();
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  const fails = async (): Promise<string[]> => {
    throw new Error('HTTP 403');
  };

  it('answers with every part, in the order given', async () => {
    expect(await keepParts('energex', [['unplanned', async () => ['u1', 'u2']], ['planned', async () => ['p1']], ['future', async () => []]])).toEqual(['u1', 'u2', 'p1']);
  });

  it("a part that fails answers with its last good records, and the others with today's", async () => {
    await keepParts('energex', [['unplanned', async () => ['old-u']], ['planned', async () => ['old-p']]]);
    vi.setSystemTime(NOW + 5 * 60_000);
    expect(await keepParts('energex', [['unplanned', async () => ['new-u']], ['planned', fails]])).toEqual(['new-u', 'old-p']);
    // Every part failing is still an answer while something is held.
    expect(await keepParts('energex', [['unplanned', fails], ['planned', fails]])).toEqual(['new-u', 'old-p']);
  });

  it('what is held is let go of after half an hour', async () => {
    await keepParts('energex', [['unplanned', async () => ['u']], ['planned', async () => ['old-p']]]);
    vi.setSystemTime(NOW + PART_KEEP_MS - 1);
    expect(await keepParts('energex', [['unplanned', async () => ['u']], ['planned', fails]])).toEqual(['u', 'old-p']);
    vi.setSystemTime(NOW + PART_KEEP_MS);
    expect(await keepParts('energex', [['unplanned', async () => ['u']], ['planned', fails]])).toEqual(['u']);
    // A good answer starts the half hour again.
    await keepParts('energex', [['unplanned', async () => ['u']], ['planned', async () => ['new-p']]]);
    vi.setSystemTime(NOW + PART_KEEP_MS + 20 * 60_000);
    expect(await keepParts('energex', [['unplanned', fails], ['planned', fails]])).toEqual(['u', 'new-p']);
  });

  it('throws the first error when every part fails and nothing is held', async () => {
    await expect(keepParts('ergon', [['unplanned', fails], ['planned', async () => { throw new Error('timeout'); }]])).rejects.toThrow('HTTP 403');
    // Held, but too old: the same.
    await keepParts('ergon', [['unplanned', async () => ['u']]]);
    vi.setSystemTime(NOW + PART_KEEP_MS + 1);
    await expect(keepParts('ergon', [['unplanned', fails]])).rejects.toThrow('HTTP 403');
  });

  it("one distributor's parts are not another's", async () => {
    await keepParts('energex', [['unplanned', async () => ['energex-u']]]);
    await expect(keepParts('ergon', [['unplanned', fails]])).rejects.toThrow('HTTP 403');
  });

  it('a part that answers something that is not a list has failed', async () => {
    await keepParts('sapn', [['current', async () => ['c']]]);
    expect(await keepParts('sapn', [['current', async () => ({ error: 'x' }) as unknown as string[]]])).toEqual(['c']);
  });

  it('a part with a refresh time is asked again only when its answer is that old', async () => {
    const planned = vi.fn(async () => ['p1']);
    const parts = (): Array<readonly [string, () => Promise<string[]>, number?]> => [
      ['current', async () => ['c']],
      ['planned', planned, 15 * 60_000],
    ];
    expect(await keepParts('sapn', parts())).toEqual(['c', 'p1']);
    vi.setSystemTime(NOW + 14 * 60_000);
    expect(await keepParts('sapn', parts())).toEqual(['c', 'p1']);
    expect(planned).toHaveBeenCalledTimes(1);
    vi.setSystemTime(NOW + 15 * 60_000);
    planned.mockResolvedValueOnce(['p2']);
    expect(await keepParts('sapn', parts())).toEqual(['c', 'p2']);
    expect(planned).toHaveBeenCalledTimes(2);
    // Due again and failing: the last good answer, asked for again at every poll.
    vi.setSystemTime(NOW + 31 * 60_000);
    planned.mockRejectedValue(new Error('HTTP 500'));
    expect(await keepParts('sapn', parts())).toEqual(['c', 'p2']);
    expect(await keepParts('sapn', parts())).toEqual(['c', 'p2']);
    expect(planned).toHaveBeenCalledTimes(4);
  });
});
