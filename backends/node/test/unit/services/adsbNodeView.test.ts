// The shared ADS-B node view — one implementation behind two gates.
//
// The staff routes and the owner routes both call these functions, so the tests
// that matter are about the contract they share: that the exact antenna
// position never comes out, that the window is clamped here rather than by a
// caller, and that "never reported" stays distinguishable from "zero".

import { describe, it, expect, vi, beforeEach } from 'vitest';

const ROW_NODE = {
  id: 'n1', name: 'adsb-syd-01', kind: 'adsb',
  lat: -33.868812, lon: 151.209321,
};
const ROW_TOTALS = {
  snapshots: 53, positions: '4711', max_aircraft: 22,
  max_range_km: 287.4, msg_rate_max: 118.3, tracks_max: 941, days: 3,
};
const ROWS_DAYS = [
  { day: '2026-09-15', snapshots: 20, positions: '1800', max_aircraft: 18, max_range_km: 250.1, msg_rate_max: 99.5 },
  { day: '2026-09-16', snapshots: 33, positions: '2911', max_aircraft: 22, max_range_km: 287.4, msg_rate_max: 118.3 },
];

let nodeRow: Record<string, unknown> | undefined = { ...ROW_NODE };
let totalsRow: Record<string, unknown> | undefined = { ...ROW_TOTALS };
let dayRows = ROWS_DAYS;
let poolAvailable = true;
/** Rows node_adsb_tracks returns — the half of the picture that outlives a
 *  restart. Empty unless a test is about the merge. */
let storedRows: Array<Record<string, unknown>> = [];
/** Make the stored-track read throw, to prove the view survives losing it. */
let storedFails = false;

vi.mock('../../../src/db/pool.js', () => ({
  getPool: async () =>
    poolAvailable
      ? {
          async query(sql: string) {
            const s = sql.replace(/\s+/g, ' ');
            if (s.includes('SELECT id, name, kind, lat, lon')) {
              return { rows: nodeRow ? [nodeRow] : [] };
            }
            if (s.includes('SELECT name, lat, lon')) {
              return { rows: nodeRow ? [nodeRow] : [] };
            }
            if (s.includes('COALESCE(SUM(snapshots)')) {
              return { rows: totalsRow ? [totalsRow] : [] };
            }
            if (s.includes('to_char(day')) return { rows: dayRows };
            if (s.includes('FROM node_adsb_tracks')) {
              if (storedFails) throw new Error('relation missing');
              return { rows: storedRows };
            }
            if (s.includes('FROM node_adsb_coverage')) return { rows: [] };
            throw new Error('unexpected SQL: ' + s.slice(0, 60));
          },
        }
      : null,
}));

// The live half comes from the heartbeat, which is absent for an offline node.
vi.mock('../../../src/services/nodes/hub.js', () => ({
  hub: { liveStatus: () => ({ status: null }), isOnline: () => false },
}));

const { adsbNodeView, adsbNodeTracks, ADSB_TRACKS_MAX_MINUTES } = await import(
  '../../../src/services/nodes/adsbNodeView.js'
);

describe('adsbNodeView', () => {
  beforeEach(() => {
    nodeRow = { ...ROW_NODE };
    totalsRow = { ...ROW_TOTALS };
    dayRows = ROWS_DAYS;
    storedRows = [];
    storedFails = false;
    poolAvailable = true;
  });

  it('never returns the exact antenna position', () => {
    // The whole reason this field is a boolean. Callers only need to know
    // whether range figures can exist; the coordinates are a home address.
    return adsbNodeView('n1', '7d').then((v) => {
      expect(v).not.toBeNull();
      expect(v!.node.hasPosition).toBe(true);
      expect(v!.node).not.toHaveProperty('lat');
      expect(v!.node).not.toHaveProperty('lon');
      expect(JSON.stringify(v)).not.toContain('151.2093');
    });
  });

  it('reports no position when the pin is unset', async () => {
    nodeRow = { ...ROW_NODE, lat: null, lon: null };
    const v = await adsbNodeView('n1', '24h');
    expect(v!.node.hasPosition).toBe(false);
  });

  it('coerces a bigint column out of its string form', async () => {
    // node-postgres hands bigint back as a string; untouched, the UI would
    // render "4711" but arithmetic on it would concatenate.
    const v = await adsbNodeView('n1', '7d');
    expect(v!.totals.positions).toBe(4711);
    expect(typeof v!.totals.positions).toBe('number');
  });

  it('keeps "never reported" distinct from zero', async () => {
    // A receiver that has never reported a range must read as "—", not "0 km",
    // which would look like a receiver hearing nothing at all.
    totalsRow = { ...ROW_TOTALS, max_range_km: null, msg_rate_max: null, tracks_max: null };
    const v = await adsbNodeView('n1', '24h');
    expect(v!.totals.maxRangeKm).toBeNull();
    expect(v!.totals.msgRateMax).toBeNull();
    expect(v!.totals.tracksMax).toBeNull();
    // ...while the counters genuinely are zero-able.
    expect(v!.totals.snapshots).toBe(53);
  });

  it('still answers for a node that has been deleted', async () => {
    // The figures outlive the registry row and are worth seeing.
    nodeRow = undefined;
    const v = await adsbNodeView('gone', '7d');
    expect(v!.node.id).toBe('gone');
    expect(v!.node.name).toBeNull();
    expect(v!.node.hasPosition).toBe(false);
    expect(v!.days).toHaveLength(2);
  });

  it('returns null rather than throwing when there is no database', async () => {
    poolAvailable = false;
    expect(await adsbNodeView('n1', '7d')).toBeNull();
  });

  it('passes day strings through untouched', async () => {
    // Formatted in SQL precisely so the server's timezone cannot shift them.
    const v = await adsbNodeView('n1', '7d');
    expect(v!.days.map((d) => d.day)).toEqual(['2026-09-15', '2026-09-16']);
  });
});

describe('Recently heard survives a restart too', () => {
  // The gap left by the first pass at this: the TRACK MAP was served from disk
  // but "Recently heard" still read memory alone, so it went on claiming one
  // aircraft after a restart while the map beside it showed the full window.
  // Both are derived from the same traces and both had to be fixed.
  const HOUR_UTC2 = Date.UTC(2026, 8, 17, 4, 0, 0);
  beforeEach(() => {
    nodeRow = { ...ROW_NODE };
    storedRows = [];
    storedFails = false;
    poolAvailable = true;
  });

  const storedRow = (hex: string, offs: number[], reports: number) => ({
    hex, callsign: null, hour_bucket: new Date(HOUR_UTC2),
    points: offs.map((o, i) => [o, -33 - i * 0.1, 151 + i * 0.1, null]),
    reports, last_alt_ft: 30000, max_alt_ft: 34000,
    first_seen: new Date(HOUR_UTC2 + offs[0]! * 1000),
    last_seen: new Date(HOUR_UTC2 + offs[offs.length - 1]! * 1000),
  });

  it('lists aircraft heard before the process started', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(HOUR_UTC2 + 30 * 60_000));
    storedRows = [
      storedRow('aaa111', [60, 600], 42),
      storedRow('bbb222', [120, 700], 17),
    ];
    const v = await adsbNodeView('n1', '24h');
    expect(v!.recent).toHaveLength(2);
    const a = v!.recent.find((x) => x.hex === 'aaa111')!;
    expect(a.reports).toBe(42);
    expect(a.maxAltFt).toBe(34000);
    expect(a.points).toBe(2);
    vi.useRealTimers();
  });

  it('orders by last heard, newest first', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(HOUR_UTC2 + 30 * 60_000));
    storedRows = [
      storedRow('older0', [60, 120], 3),
      storedRow('newer0', [60, 900], 4),
    ];
    const v = await adsbNodeView('n1', '24h');
    expect(v!.recent.map((x) => x.hex)).toEqual(['newer0', 'older0']);
    vi.useRealTimers();
  });

  it('still answers when the stored read fails', async () => {
    storedFails = true;
    const v = await adsbNodeView('n1', '24h');
    expect(Array.isArray(v!.recent)).toBe(true);
  });
});

describe('adsbNodeTracks survives a restart', () => {
  // The reported fault: a receiver that had heard sixty-odd aircraft in a day
  // showed ONE on its eight-hour map, because the traces were memory-only and
  // every deploy emptied them. The coverage envelope beside it was persisted
  // and looked healthy, which made the map read as broken rather than young.
  beforeEach(() => {
    nodeRow = { ...ROW_NODE };
    storedRows = [];
    storedFails = false;
    poolAvailable = true;
  });

  const HOUR_UTC = Date.UTC(2026, 8, 17, 4, 0, 0);
  const stored = (hex: string, pts: Array<[number, number, number]>) => ({
    hex, callsign: null, hour_bucket: new Date(HOUR_UTC),
    points: pts.map((p) => [p[0], p[1], p[2], null]),
    reports: 9, last_alt_ft: 30000, max_alt_ft: 34000,
    first_seen: new Date(HOUR_UTC + pts[0]![0] * 1000),
    last_seen: new Date(HOUR_UTC + pts[pts.length - 1]![0] * 1000),
  });

  it('serves tracks from disk when memory holds none', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(HOUR_UTC + 30 * 60_000));
    storedRows = [
      stored('aaa111', [[60, -33.0, 151.0], [600, -33.2, 151.2]]),
      stored('bbb222', [[120, -34.0, 152.0], [700, -34.2, 152.2]]),
    ];
    const t = await adsbNodeTracks('n1', 480);
    expect(t.aircraft).toBe(2);
    expect(t.traces).toHaveLength(2);
    expect(t.points).toBe(4);
    vi.useRealTimers();
  });

  it('still answers when the stored read fails', async () => {
    // Losing the older half of the picture must not lose the view.
    storedFails = true;
    const t = await adsbNodeTracks('n1', 480);
    expect(t.site).not.toBeNull();
    expect(Array.isArray(t.traces)).toBe(true);
  });

  it('drops a stored track too short to draw', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(HOUR_UTC + 30 * 60_000));
    storedRows = [stored('aaa111', [[60, -33.0, 151.0]])];
    const t = await adsbNodeTracks('n1', 480);
    // Counted, because it was heard — but a single point is a dot, not a path.
    expect(t.aircraft).toBe(1);
    expect(t.traces).toHaveLength(0);
    vi.useRealTimers();
  });
});

describe('adsbNodeTracks', () => {
  beforeEach(() => {
    nodeRow = { ...ROW_NODE };
    poolAvailable = true;
  });

  it('rounds the receiver position, and says it is approximate', async () => {
    const t = await adsbNodeTracks('n1', 60);
    expect(t.site).not.toBeNull();
    expect(t.site!.approx).toBe(true);
    expect(t.site!.lat).toBe(-33.87);
    expect(t.site!.lon).toBe(151.21);
    // Coarser than a street, finer than the scale a coverage plot is read at.
    expect(Math.abs(t.site!.lat - ROW_NODE.lat) * 111).toBeLessThan(1.5);
    expect(JSON.stringify(t)).not.toContain('151.209321');
  });

  it('clamps the window here, not in the caller', async () => {
    // A caller asking for more than the store retains gets the full window,
    // never a silently short answer — and clamping inside means a new route
    // cannot be written without it.
    expect((await adsbNodeTracks('n1', 9999)).windowMinutes).toBe(ADSB_TRACKS_MAX_MINUTES);
    expect((await adsbNodeTracks('n1', 0)).windowMinutes).toBe(1);
    expect((await adsbNodeTracks('n1', -5)).windowMinutes).toBe(1);
    expect((await adsbNodeTracks('n1', Number.NaN)).windowMinutes).toBe(ADSB_TRACKS_MAX_MINUTES);
  });

  it('omits the site when the node has no pin', async () => {
    nodeRow = { ...ROW_NODE, lat: null, lon: null };
    expect((await adsbNodeTracks('n1', 60)).site).toBeNull();
  });

  it('still returns traces when there is no database', async () => {
    // The traces live in memory; losing the database costs the centre, not the
    // coverage picture.
    poolAvailable = false;
    const t = await adsbNodeTracks('n1', 60);
    expect(t.site).toBeNull();
    expect(Array.isArray(t.traces)).toBe(true);
  });
});
