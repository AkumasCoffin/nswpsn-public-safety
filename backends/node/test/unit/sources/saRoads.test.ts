/**
 * SA's main roads as DIT draws them (sources/saRoads.ts), and the zip reader
 * the download needs (sources/shared/zip.ts): a road one line from its
 * pieces however DIT's layers overlap them, a point placed along it, the
 * stretch between two places cut out (in parts where the drawing has a gap),
 * and the download read once, kept, and tried again an hour after failing.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const { fetchBufferMock } = vi.hoisted(() => ({ fetchBufferMock: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchBuffer: fetchBufferMock,
  fetchJson: vi.fn(),
  fetchText: vi.fn(),
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

import { parseSaRoads, placeOnRoad, roadStretch, saRoads, simplifyLine, _resetSaRoads } from '../../../src/sources/saRoads.js';
import { unzip } from '../../../src/sources/shared/zip.js';

// A directory, a stored file and a deflated one (Python's zipfile).
const ZIP_SMALL =
  'UEsDBBQAAAAAAAAAIQAAAAAAAAAAAAAAAAAEAAAAZGlyL1BLAwQUAAAAAAA6CGNdtQGvDwsAAAALAAAABQAAAGEudHh0c3RvcmVkIHRleHRQSwMEFAAAAAgAOghjXQfWb5AOAAAAtAAAAAUAAABiLnR4dEtJTctJLElNUUgZOgwAUEsBAhQAFAAAAAAAAAAhAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAIABAAAAAGRpci9QSwECFAAUAAAAAAA6CGNdtQGvDwsAAAALAAAABQAAAAAAAAAAAAAAgAEiAAAAYS50eHRQSwECFAAUAAAACAA6CGNdB9ZvkA4AAAC0AAAABQAAAAAAAAAAAAAAgAFQAAAAYi50eHRQSwUGAAAAAAMAAwCYAAAAgQAAAAAA';
// Roads_GDA2020.geojson (road 03400, 0–2 km), an empty Roads_GDA94.geojson, and License.txt.
const ZIP_ROADS =
  'UEsDBBQAAAAIADoIY10PDcXBpgAAAAMBAAAVAAAAUm9hZHNfR0RBMjAyMC5nZW9qc29uXY49C4MwEIb/Srg5tfFjKK6FTnYqnSRIMFcJ2ETOcxDxvxdjXRzfh+d97xbgeUAoBTzQ8ER4D32PLbvgQQr47HCEUtTLWd2EgcKAxC4qC3RubCgY2/iwiSovlNq00dnYfF+rGNkQN0QWSqGkAPT2n7JVCugwfJFpjpPH0efUs6ucxxeT890204ZA1nnD+4N1nea3RElxyYtEaSn2nJ5BdgCt11WvP1BLAwQUAAAACAA6CGNdP702qisAAAAtAAAAEwAAAFJvYWRzX0dEQTk0Lmdlb2pzb26rViqpLEhVslJQcktNLCktSnXOz8lJTS7JzM9T0lFQSoMIFitZKUTH1gIAUEsDBBQAAAAIADoIY125pHWPDgAAAAwAAAALAAAATGljZW5zZS50eHRzdlZwilQw1jNQcAwFAFBLAQIUABQAAAAIADoIY10PDcXBpgAAAAMBAAAVAAAAAAAAAAAAAACAAQAAAABSb2Fkc19HREEyMDIwLmdlb2pzb25QSwECFAAUAAAACAA6CGNdP702qisAAAAtAAAAEwAAAAAAAAAAAAAAgAHZAAAAUm9hZHNfR0RBOTQuZ2VvanNvblBLAQIUABQAAAAIADoIY125pHWPDgAAAAwAAAALAAAAAAAAAAAAAACAATUBAABMaWNlbnNlLnR4dFBLBQYAAAAAAwADAL0AAABsAQAAAAA=';
const bytes = (b64: string): Uint8Array => new Uint8Array(Buffer.from(b64, 'base64'));

describe('unzip', () => {
  it('reads stored and deflated entries, skips directories and what is not wanted', () => {
    const all = unzip(bytes(ZIP_SMALL));
    expect([...all.keys()]).toEqual(['a.txt', 'b.txt']);
    expect(all.get('a.txt')!.toString()).toBe('stored text');
    expect(all.get('b.txt')!.toString()).toBe('deflated '.repeat(20));
    expect([...unzip(bytes(ZIP_SMALL), (n) => n === 'b.txt').keys()]).toEqual(['b.txt']);
  });

  it('refuses what is not a zip', () => {
    expect(() => unzip(new TextEncoder().encode('not a zip at all, just some text that is long enough'))).toThrow(/not a zip/);
  });
});

// Road 03400 along latitude -34, eastward: two pieces of 1 km each, and a
// second layer's piece over the join; a right-hand carriageway beside it;
// then a gap in the drawing until a piece from 3 to 4 km.
const piece = (from: number, to: number, lons: number[], over: Record<string, unknown> = {}) => ({
  type: 'Feature',
  properties: { gis_road_no: '03400', side: 'U/L', start_rrd: from, end_rrd: to, ...over },
  geometry: { type: 'MultiLineString', coordinates: [lons.map((lon) => [lon, -34 + ((over['lat'] as number) ?? 0)])] },
});
const layerA = { type: 'FeatureCollection', features: [piece(0, 1, [138.0, 138.005, 138.01]), piece(1, 2, [138.01, 138.02]), piece(3, 4, [138.03, 138.04])] };
const layerB = {
  type: 'FeatureCollection',
  features: [piece(0, 1, [138.0, 138.005, 138.01]), piece(0.5, 1.5, [138.005, 138.015]), piece(0, 2, [138.0, 138.02], { side: 'R', lat: -0.001 })],
};

describe('SA roads', () => {
  const roads = parseSaRoads([layerA, layerB]);

  it('one line per road: its numbers without leading zeros, the right-hand carriageway and repeats left out', () => {
    expect([...roads.keys()]).toEqual(['3400']);
    expect(roads.get('3400')!.map((p) => [p.from, p.to])).toEqual([[0, 1], [1, 2], [3, 4], [0.5, 1.5]]);
  });

  it('places a point along its road, by how far along the road measures it', () => {
    const p = placeOnRoad(roads, '3400', 138.015, -34.0001)!;
    expect(p.at).toBeCloseTo(1.5, 3);
    expect(p.km).toBeCloseTo(0.011, 3);
    expect(placeOnRoad(roads, '9999', 138.015, -34)).toBeNull();
  });

  it('cuts out the stretch between two places, one line across the pieces', () => {
    const [line, ...rest] = roadStretch(roads, '3400', 0.25, 1.75);
    expect(rest).toEqual([]);
    expect(line![0]![0]).toBeCloseTo(138.0025, 6);
    expect(line![line!.length - 1]![0]).toBeCloseTo(138.0175, 6);
    // Straight, so nothing between the ends is left.
    expect(line).toHaveLength(2);
  });

  it('a gap in the drawing makes two lines', () => {
    const lines = roadStretch(roads, '3400', 1.5, 3.5);
    expect(lines.map((l) => [l[0]![0], l[l.length - 1]![0]].map((x) => Number(x!.toFixed(4))))).toEqual([
      [138.015, 138.02],
      [138.03, 138.035],
    ]);
  });

  it('simplifying keeps a bend and drops points along a straight', () => {
    expect(simplifyLine([[138, -34], [138.001, -34], [138.002, -34]])).toEqual([[138, -34], [138.002, -34]]);
    expect(simplifyLine([[138, -34], [138.001, -33.999], [138.002, -34]])).toHaveLength(3);
  });
});

describe('reading the roads', () => {
  beforeEach(() => {
    _resetSaRoads();
    fetchBufferMock.mockReset();
  });

  it('reads the GDA2020 layers once, then keeps them', async () => {
    fetchBufferMock.mockResolvedValue(bytes(ZIP_ROADS));
    const now = Date.now();
    const first = await saRoads(now);
    expect([...first!.keys()]).toEqual(['3400']);
    expect(await saRoads(now + 60_000)).toBe(first);
    expect(fetchBufferMock).toHaveBeenCalledTimes(1);
  });

  it('a failed read gives no roads, and is tried again an hour later', async () => {
    fetchBufferMock.mockRejectedValueOnce(new Error('HTTP 503')).mockResolvedValue(bytes(ZIP_ROADS));
    const now = Date.now();
    expect(await saRoads(now)).toBeNull();
    expect(await saRoads(now + 60_000)).toBeNull();
    expect(fetchBufferMock).toHaveBeenCalledTimes(1);
    expect((await saRoads(now + 3_700_000))?.size).toBe(1);
  });
});
