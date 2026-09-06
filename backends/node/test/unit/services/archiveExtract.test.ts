/**
 * Tests for the per-poll → per-incident fan-out used when the poller
 * archives a snapshot. Without this, /api/data/history shows one
 * "Unknown" row per source (the python data_history shape stored one
 * row per incident).
 */
import { describe, it, expect } from 'vitest';
import { defaultArchiveItems, extractSourceTimestampUnix } from '../../../src/services/archiveExtract.js';

describe('defaultArchiveItems', () => {
  it('fans out a GeoJSON FeatureCollection one row per feature', () => {
    const snapshot = {
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [151.21, -33.86] },
          properties: {
            id: 'rfs-1',
            title: 'Bushfire near Foo',
            alertLevel: 'Advice',
            status: 'Being controlled',
          },
        },
        {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [150.0, -32.5] },
          properties: {
            guid: 'rfs-2',
            title: 'Watch and Act fire',
            alertLevel: 'Watch and Act',
          },
        },
      ],
    };
    const out = defaultArchiveItems('rfs', snapshot, 1_700_000_000);
    expect(out.length).toBe(2);
    const r0 = out[0]!;
    expect(r0.source).toBe('rfs');
    expect(r0.source_id).toBe('rfs-1');
    expect(r0.lat).toBe(-33.86);
    expect(r0.lng).toBe(151.21);
    expect(r0.category).toBe('Advice');
    expect(r0.fetched_at).toBe(1_700_000_000);
    // data must surface title at the top level so /api/data/history's
    // pickStr(data, 'title') finds it.
    expect((r0.data as Record<string, unknown>)['title']).toBe(
      'Bushfire near Foo',
    );
    // status comes from properties verbatim.
    expect((r0.data as Record<string, unknown>)['status']).toBe(
      'Being controlled',
    );
    // second row falls back to guid for source_id.
    expect(out[1]!.source_id).toBe('rfs-2');
  });

  it('fans out a flat array one row per element', () => {
    const snapshot = [
      {
        id: 'pager-1',
        title: 'Job assigned',
        latitude: -33.5,
        longitude: 151.0,
        severity: 'med',
      },
      {
        id: 'pager-2',
        title: 'Job complete',
      },
    ];
    const out = defaultArchiveItems('pager', snapshot, 1_700_000_000);
    expect(out.length).toBe(2);
    expect(out[0]!.source_id).toBe('pager-1');
    expect(out[0]!.lat).toBe(-33.5);
    expect(out[0]!.lng).toBe(151.0);
    expect((out[0]!.data as Record<string, unknown>)['severity']).toBe('med');
    // second row has no coords — lat/lng null but still archives.
    expect(out[1]!.lat).toBeNull();
    expect(out[1]!.lng).toBeNull();
  });

  it('falls back to a single row for opaque snapshots', () => {
    const snapshot = { someField: 'value' };
    const out = defaultArchiveItems('aviation_cameras', snapshot, 1_700_000_000);
    expect(out.length).toBe(1);
    expect(out[0]!.data).toEqual(snapshot);
  });

  it('returns no rows for null/undefined', () => {
    expect(defaultArchiveItems('x', null, 1)).toEqual([]);
    expect(defaultArchiveItems('x', undefined, 1)).toEqual([]);
  });

  it('uses property aliases for title/location_text', () => {
    const snapshot = {
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [151, -33] },
          properties: {
            id: 'a',
            // No 'title' — must fall back to 'name'/'headline'/'displayName'.
            headline: 'Crash on M1',
            // No 'location_text' — must fall back to 'streets'/'suburb'/etc.
            streets: 'Pacific Highway',
          },
        },
      ],
    };
    const out = defaultArchiveItems('traffic_incident', snapshot, 1);
    const data = out[0]!.data as Record<string, unknown>;
    expect(data['title']).toBe('Crash on M1');
    expect(data['location_text']).toBe('Pacific Highway');
  });
});

describe('extractSourceTimestampUnix', () => {
  const at = (props: Record<string, unknown>, source?: string) =>
    extractSourceTimestampUnix(props, source);

  it('prefers updatedISO over the locale updated string', () => {
    // The interstate fire sources pair both; the locale one is
    // ambiguous and must lose.
    const ts = at({
      updated: '06/09/2026, 11:34:15 pm',
      updatedISO: '2026-09-06T13:34:15.000Z',
    });
    expect(ts).toBe(Date.UTC(2026, 8, 6, 13, 34, 15) / 1000);
  });

  it('parses slash-dates day-first, never month-first', () => {
    // "06/09/2026" is 6 September in every feed we ingest. Date.parse
    // would read it as June 9 and push the incident months out of the
    // logs window — the bug that zeroed the QFD counts.
    const ts = at({ updated: '06/09/2026, 11:34:15 pm' });
    expect(ts).toBe(Date.UTC(2026, 8, 6, 23, 34, 15) / 1000);
  });

  it('handles 24h slash-dates and bare dates', () => {
    expect(at({ updated: '06/09/2026 22:35' })).toBe(
      Date.UTC(2026, 8, 6, 22, 35, 0) / 1000,
    );
    expect(at({ updated: '25/12/2026' })).toBe(
      Date.UTC(2026, 11, 25, 0, 0, 0) / 1000,
    );
    expect(at({ updated: '12:30am 06/09/2026' })).toBe(null); // unrecognised shape stays null
  });

  it('12am is midnight, 12pm is noon', () => {
    expect(at({ updated: '06/09/2026, 12:05:00 am' })).toBe(
      Date.UTC(2026, 8, 6, 0, 5, 0) / 1000,
    );
    expect(at({ updated: '06/09/2026, 12:05:00 pm' })).toBe(
      Date.UTC(2026, 8, 6, 12, 5, 0) / 1000,
    );
  });

  it('rejects impossible day-first dates instead of guessing', () => {
    // Month 13 can't be a month even day-first — null, not a
    // month-first reinterpretation.
    expect(at({ updated: '06/13/2026, 1:00:00 pm' })).toBe(null);
  });

  it('still parses ISO and epoch forms', () => {
    expect(at({ updated: '2026-09-06T13:34:15Z' })).toBe(
      Date.UTC(2026, 8, 6, 13, 34, 15) / 1000,
    );
    expect(at({ pubMillis: 1757166855000 }, 'waze_alerts')).toBe(1757166855);
  });
});
