/**
 * Ausgrid fetcher tests.
 *
 * Pure-function coverage on the marker normaliser; the actual fetch
 * test mocks the shared HTTP module so the upstream URL never gets hit.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

// vi.hoisted: see ../endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import {
  normaliseAusgridOutage,
  fetchAusgridOutages,
} from '../../../src/sources/ausgrid.js';

describe('normaliseAusgridOutage', () => {
  it('produces both PascalCase and camelCase mirrors', () => {
    const out = normaliseAusgridOutage({
      WebId: 12345,
      Area: 'Bondi',
      MarkerLocation: { lat: -33.89, lng: 151.27 },
      OutageDisplayType: 'R',
      Customers: 42,
      Cause: 'Equipment failure',
      StartDateTime: '2024-06-01T00:00:00Z',
      EstRestTime: '2024-06-01T02:00:00Z',
      Status: 1,
      Polygons: [{ ring: 'data' }],
    });
    expect(out).not.toBeNull();
    expect(out?.OutageId).toBe(12345);
    expect(out?.outageId).toBe(12345);
    expect(out?.Suburb).toBe('Bondi');
    expect(out?.suburb).toBe('Bondi');
    expect(out?.OutageType).toBe('Unplanned');
    expect(out?.outageType).toBe('Unplanned');
    expect(out?.CustomersAffected).toBe(42);
    expect(out?.customersAffected).toBe(42);
    expect(out?.Latitude).toBe(-33.89);
    expect(out?.Longitude).toBe(151.27);
    expect(out?.Polygons).toEqual([{ ring: 'data' }]);
  });

  it('maps OutageDisplayType=P to Planned', () => {
    const out = normaliseAusgridOutage({
      WebId: 1,
      OutageDisplayType: 'P',
    });
    expect(out?.OutageType).toBe('Planned');
    expect(out?.outageType).toBe('Planned');
  });

  it('falls back to CustomersAffectedText when Customers is missing', () => {
    const out = normaliseAusgridOutage({
      WebId: 'abc',
      CustomersAffectedText: '17',
    });
    expect(out?.CustomersAffected).toBe(17);
  });

  it('coerces non-numeric customer counts to 0', () => {
    const out = normaliseAusgridOutage({ WebId: 1, Customers: 'lots' });
    expect(out?.CustomersAffected).toBe(0);
  });

  it('returns null for non-object input', () => {
    // @ts-expect-error — exercising the runtime guard
    expect(normaliseAusgridOutage(null)).toBeNull();
    // @ts-expect-error — exercising the runtime guard
    expect(normaliseAusgridOutage('nope')).toBeNull();
  });
});

describe('fetchAusgridOutages', () => {
  beforeEach(() => fetchJson.mockReset());

  it('flattens an array response into {Markers, Polygons}', async () => {
    fetchJson.mockResolvedValueOnce([
      {
        WebId: 1,
        Area: 'A',
        MarkerLocation: { lat: -33, lng: 151 },
        OutageDisplayType: 'R',
        Customers: 1,
        Polygons: [{ id: 'p1' }],
      },
      {
        WebId: 2,
        Area: 'B',
        OutageDisplayType: 'P',
        Customers: 2,
        Polygons: [{ id: 'p2' }, { id: 'p3' }],
      },
    ]);
    const out = await fetchAusgridOutages();
    expect(out.Markers).toHaveLength(2);
    expect(out.Polygons).toHaveLength(3);
    expect(out.Markers[0]?.outageType).toBe('Unplanned');
    expect(out.Markers[1]?.outageType).toBe('Planned');
  });

  it('passes through legacy {Markers, Polygons} shape unchanged', async () => {
    fetchJson.mockResolvedValueOnce({
      Markers: [{ OutageId: 1 }],
      Polygons: [{ ring: 'x' }],
    });
    const out = await fetchAusgridOutages();
    expect(out.Markers).toHaveLength(1);
    expect(out.Polygons).toHaveLength(1);
  });

  it('returns empty payload for unexpected response shape', async () => {
    fetchJson.mockResolvedValueOnce(42);
    const out = await fetchAusgridOutages();
    expect(out).toEqual({ Markers: [], Polygons: [] });
  });
});
