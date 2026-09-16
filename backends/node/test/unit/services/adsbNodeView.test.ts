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
