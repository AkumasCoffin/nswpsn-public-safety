/**
 * ACT Ambulance (ESA feed) unit tests.
 *
 * The upstream feed is a mixed bag — NSW RFS incidents re-published by
 * ESA, ACT fire, and ACT ambulance — so most of what matters here is
 * what gets THROWN AWAY. Covers:
 *   - only live ACT ambulance survives (NSW / ACT-fire / Finished dropped)
 *   - Sydney wall-clock timestamps resolve to the right epoch
 *   - the feed's per-poll `date` field never reaches the stored record
 *   - records without usable coordinates are skipped
 *   - a non-array payload and an upstream failure both throw
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import {
  fetchActAmbulance,
  parseEsaTimestamp,
  isWantedRecord,
  toFeature,
} from '../../../src/sources/actAmbulance.js';

const ACT_AMBO = {
  agency: 'Ambulance',
  region: 'act',
  event_type: 'incident',
  id: '061972-29082026',
  title: 'AMBULANCE RESPONSE - KAMBAH',
  location: 'KAMBAH',
  latitude: '-35.384074',
  longitude: '149.06111',
  type: 'AMBULANCE RESPONSE',
  status: 'On Scene',
  status_filter_label: 'On Scene',
  alert_level: '',
  time_of_call: '29 Aug 2026 06:20:16',
  updated: '29 Aug 2026 08:26:37',
  date: '29 Aug 2026 14:51:00',
};

const ACT_FIRE = {
  agency: 'Fire',
  region: 'act',
  event_type: 'incident',
  id: '013105-14082026',
  title: 'HAZARD REDUCTION BURN - STROMLO',
  location: 'RIVERVIEW COTTAGE, COTTER ROAD, STROMLO, 2611',
  latitude: '-35.33201519',
  longitude: '148.97451709',
  type: 'HAZARD REDUCTION BURN',
  status: 'Resource Allocation Pending',
  updated: '19 Aug 2026 23:24:36',
};

// ESA re-publishes NSW RFS incidents; their id is a mangled RFS API URL
// for an incident our own `rfs` source already ingests.
const NSW_RFS = {
  region: 'nsw',
  event_type: 'burnt area',
  id: 'https___incidents_rfs_nsw_gov_au_api_v1_incidents_673202',
  title: 'HUNTER EXP, ALLANDALE',
  latitude: '-32.72469',
  longitude: '151.403221',
  status: 'Responding',
  type: 'Grass Fire',
  updated: '29 Aug 2026 14:44',
};

const ACT_AMBO_FINISHED = {
  ...ACT_AMBO,
  id: '062074-29082026',
  title: 'AMBULANCE RESPONSE - CHARNWOOD',
  location: 'CHARNWOOD',
  status: 'Finished',
};

describe('fetchActAmbulance', () => {
  beforeEach(() => {
    fetchJson.mockReset();
  });

  it('keeps only live ACT ambulance responses', async () => {
    fetchJson.mockResolvedValueOnce([ACT_AMBO, ACT_FIRE, NSW_RFS, ACT_AMBO_FINISHED]);
    const snap = await fetchActAmbulance();

    expect(snap.type).toBe('FeatureCollection');
    expect(snap.count).toBe(1);
    expect(snap.features).toHaveLength(1);
    expect(snap.features[0]?.properties.id).toBe('061972-29082026');
  });

  it('never lets a re-published NSW RFS incident through', async () => {
    fetchJson.mockResolvedValueOnce([NSW_RFS]);
    const snap = await fetchActAmbulance();
    expect(snap.features).toHaveLength(0);
  });

  it('maps a response onto the archive-facing property shape', async () => {
    fetchJson.mockResolvedValueOnce([ACT_AMBO]);
    const snap = await fetchActAmbulance();
    const f = snap.features[0]!;

    // GeoJSON is [lng, lat], not [lat, lng].
    expect(f.geometry.coordinates).toEqual([149.06111, -35.384074]);
    expect(f.properties).toMatchObject({
      id: '061972-29082026',
      title: 'AMBULANCE RESPONSE - KAMBAH',
      location_text: 'KAMBAH',
      status: 'On Scene',
      category: 'Ambulance',   // promoted to archive category
      subcategory: 'On Scene', // promoted to archive subcategory
      is_active: true,
    });
  });

  it('drops the feed-generation `date` so write-time dedup still works', async () => {
    fetchJson.mockResolvedValueOnce([ACT_AMBO]);
    const snap = await fetchActAmbulance();
    // `date` is the same on every record and changes every poll; keeping
    // it would re-hash each row every minute and defeat dedup.
    expect(snap.features[0]?.properties).not.toHaveProperty('date');
  });

  it('skips records whose coordinates are missing or unparseable', async () => {
    fetchJson.mockResolvedValueOnce([
      { ...ACT_AMBO, id: 'a', latitude: '', longitude: '' },
      { ...ACT_AMBO, id: 'b', latitude: 'not-a-number', longitude: '149.0' },
      { ...ACT_AMBO, id: 'c' },
    ]);
    const snap = await fetchActAmbulance();
    expect(snap.features.map((f) => f.properties.id)).toEqual(['c']);
  });

  it('throws when the feed is not an array', async () => {
    fetchJson.mockResolvedValueOnce({ error: 'nope' });
    await expect(fetchActAmbulance()).rejects.toThrow(/non-array/);
  });

  it('propagates an upstream failure so the poller backs off', async () => {
    fetchJson.mockRejectedValueOnce(new Error('503'));
    await expect(fetchActAmbulance()).rejects.toThrow('503');
  });
});

describe('parseEsaTimestamp', () => {
  it('reads the stamp as ACT wall-clock, not server-local', () => {
    // 29 Aug 2026 is AEST (UTC+10), so 08:26:37 Canberra = 22:26:37Z the
    // previous day. Parsing it as UTC would be 10h adrift.
    const unix = parseEsaTimestamp('29 Aug 2026 08:26:37');
    expect(unix).toBe(Math.floor(Date.parse('2026-08-28T22:26:37Z') / 1000));
  });

  it('handles a daylight-saving date at the other offset', () => {
    // January is AEDT (UTC+11).
    const unix = parseEsaTimestamp('15 Jan 2026 08:00:00');
    expect(unix).toBe(Math.floor(Date.parse('2026-01-14T21:00:00Z') / 1000));
  });

  it('accepts a stamp with no seconds', () => {
    expect(parseEsaTimestamp('29 Aug 2026 14:44')).toBe(
      Math.floor(Date.parse('2026-08-29T04:44:00Z') / 1000),
    );
  });

  it('returns null for empty or unrecognised input', () => {
    expect(parseEsaTimestamp('')).toBeNull();
    expect(parseEsaTimestamp(undefined)).toBeNull();
    expect(parseEsaTimestamp('yesterday')).toBeNull();
  });
});

describe('isWantedRecord', () => {
  it('accepts a live ACT ambulance response', () => {
    expect(isWantedRecord(ACT_AMBO)).toBe(true);
  });

  it('rejects other regions, other agencies, and finished jobs', () => {
    expect(isWantedRecord(NSW_RFS)).toBe(false);
    expect(isWantedRecord(ACT_FIRE)).toBe(false);
    expect(isWantedRecord(ACT_AMBO_FINISHED)).toBe(false);
  });

  it('is not case-sensitive about region/agency/status', () => {
    expect(isWantedRecord({ ...ACT_AMBO, region: 'ACT', agency: 'AMBULANCE' })).toBe(true);
    expect(isWantedRecord({ ...ACT_AMBO, status: 'FINISHED' })).toBe(false);
  });
});

describe('toFeature', () => {
  it('returns null without an id, since a row with no source_id is invisible', () => {
    expect(toFeature({ ...ACT_AMBO, id: '' })).toBeNull();
  });

  it('falls back to a location-derived title when the feed omits one', () => {
    const f = toFeature({ ...ACT_AMBO, title: '' });
    expect(f?.properties.title).toBe('Ambulance response — KAMBAH');
  });

  it('falls back to time_of_call when updated is missing', () => {
    const f = toFeature({ ...ACT_AMBO, updated: '' });
    expect(f?.properties.timestamp).toBe(parseEsaTimestamp(ACT_AMBO.time_of_call));
  });
});
