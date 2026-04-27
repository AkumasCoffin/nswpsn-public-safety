/**
 * Endeavour fetcher unit tests.
 *
 * Mocks the shared HTTP layer so the merge/normalise logic can be
 * exercised without touching Supabase. Covers:
 *   - happy path produces the three-bucket split with correct counts
 *   - planned outages with past start_date land in current_maintenance
 *   - planned outages with future start_date land in future_maintenance
 *   - unplanned outages always land in `current`
 *   - missing enrichment row still produces a usable record (no throw)
 *   - throws when /rpc/get_outage_areas_fast returns a non-array
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

// vi.hoisted: vi.mock factories are hoisted above imports, so they
// can't reference module-scope `const`s. Hoisted state is the supported
// way to share mock fns between the factory and the test bodies.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {
    status: number | null;
    url: string;
    constructor(message: string, status: number | null, url: string) {
      super(message);
      this.status = status;
      this.url = url;
    }
  },
}));

// Imported AFTER the mock is set up. The mock registration above
// runs before module evaluation thanks to vitest's hoisting of vi.mock.
import {
  fetchEndeavourSplit,
  _resetEndeavourMemo,
} from '../../../src/sources/endeavour.js';

const PAST = '2020-01-01T00:00:00Z';
const FUTURE = '2099-01-01T00:00:00Z';

describe('fetchEndeavourSplit', () => {
  beforeEach(() => {
    fetchJson.mockReset();
    _resetEndeavourMemo();
    process.env['ENDEAVOUR_SUPABASE_URL'] = 'https://example.supabase.co';
    process.env['ENDEAVOUR_SUPABASE_KEY'] = 'test-key';
  });

  it('splits unplanned, current_maintenance, future_maintenance buckets', async () => {
    fetchJson.mockResolvedValueOnce([
      // Unplanned with full enrichment.
      {
        incident_id: 'A',
        outage_type: 'UNPLANNED',
        incident_status: 'NEW',
        customers_affected: 12,
        center_lat: -33.8,
        center_lng: 151.0,
      },
      // Planned with status that means in-progress.
      {
        incident_id: 'B',
        outage_type: 'PLANNED',
        incident_status: 'DESPATCHED',
        customers_affected: 5,
        center_lat: null,
        center_lng: null,
      },
      // Planned with future start_date.
      {
        incident_id: 'C',
        outage_type: 'PLANNED',
        incident_status: 'SCHEDULED',
        customers_affected: 7,
      },
    ]);
    fetchJson.mockResolvedValueOnce([
      {
        incident_id: 'A',
        cause: 'Tree on lines',
        cityname: 'penrith',
        postcode: '2750',
        street_name: 'High St',
        start_date_time: PAST,
        updated_at: '2024-01-01T00:00:00Z',
      },
      // No enrichment for B.
      {
        incident_id: 'C',
        cause: 'Maintenance',
        cityname: 'wollongong',
        start_date_time: FUTURE,
      },
    ]);

    const split = await fetchEndeavourSplit(new Date('2024-06-01T00:00:00Z'));

    expect(split.current).toHaveLength(1);
    expect(split.current[0]).toMatchObject({
      id: 'A',
      outageType: 'Unplanned',
      suburb: 'Penrith',
      streets: 'High St',
      customersAffected: 12,
      cause: 'Tree on lines',
      hasGPS: true,
      latitude: -33.8,
      longitude: 151.0,
    });

    expect(split.current_maintenance).toHaveLength(1);
    expect(split.current_maintenance[0]).toMatchObject({
      id: 'B',
      outageType: 'Current Maintenance',
      // Active-status path → label, not the start-date path.
      status: 'Crew Dispatched',
      cause: 'Planned maintenance',
    });

    expect(split.future_maintenance).toHaveLength(1);
    expect(split.future_maintenance[0]).toMatchObject({
      id: 'C',
      outageType: 'Future Maintenance',
      suburb: 'Wollongong',
    });
  });

  it('treats planned outage with past start_date as current_maintenance', async () => {
    fetchJson.mockResolvedValueOnce([
      {
        incident_id: 'P1',
        outage_type: 'PLANNED',
        incident_status: 'LODGED', // not in active set
      },
    ]);
    fetchJson.mockResolvedValueOnce([
      {
        incident_id: 'P1',
        start_date_time: '2020-01-01T00:00:00Z', // long past
      },
    ]);
    const split = await fetchEndeavourSplit(new Date('2024-06-01T00:00:00Z'));
    expect(split.current_maintenance).toHaveLength(1);
    expect(split.future_maintenance).toHaveLength(0);
  });

  it('continues without enrichment when /outage-points fails', async () => {
    fetchJson.mockResolvedValueOnce([
      { incident_id: 'X', outage_type: 'UNPLANNED' },
    ]);
    fetchJson.mockRejectedValueOnce(new Error('500'));
    const split = await fetchEndeavourSplit();
    expect(split.current).toHaveLength(1);
    // Suburb falls back to "Unknown" when no enrichment row matched.
    expect(split.current[0]?.suburb).toBe('Unknown');
  });

  it('throws if get_outage_areas_fast returns non-array', async () => {
    fetchJson.mockResolvedValueOnce({ error: 'oops' });
    await expect(fetchEndeavourSplit()).rejects.toThrow(/non-array/);
  });
});
