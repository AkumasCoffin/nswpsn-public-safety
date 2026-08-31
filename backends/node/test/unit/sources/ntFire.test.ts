/**
 * NT Fire & Rescue source unit tests.
 *
 * The things that matter here are the ones that would quietly corrupt
 * the archive: which layer a category lands on, that a closed record is
 * KEPT (not dropped, the way ACT Ambulance drops Finished and loses the
 * final transition), that `_status` beats a stale `Status`, and above all
 * that an incident's id survives closing — if it doesn't, the closing
 * snapshot archives as a new incident and the history splits in two.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import {
  fetchNtFire,
  toNtFeature,
  ntLayerFor,
  ntIncidentId,
} from '../../../src/sources/ntFire.js';

const FIRE = {
  type: 'Feature',
  geometry: { type: 'Point', coordinates: [133.80775292941, -23.75526134541] },
  properties: {
    _category: 'fire',
    _status: 'active',
    _eventtype: 'Grass and Scrub Fire',
    _location: 'GREATOREX RD, ILPARPA',
    _datenotified: '2026-08-30T19:11:00+09:30',
    _lastupdate: '2026-08-30T19:16:57+09:30',
    'Fire Type': 'Grass and Scrub Fire',
    Location: 'GREATOREX RD, ILPARPA',
    Status: 'Active',
    'Alert Level': 'Advice',
    'Responsible Agency': 'NTFRS',
    'Last Update': '30/08/2026 19:16',
    Notified: '30/08/2026 19:11',
  },
};

const wrap = (features: unknown[]) => ({
  title: 'NT Incident Map',
  note: 'The format of this file may change without notice.',
  incidents: { type: 'FeatureCollection', features },
});

describe('ntLayerFor', () => {
  it('sends road crashes to the hazard layer', () => {
    expect(ntLayerFor('roadcrash')).toBe('hazard');
    expect(ntLayerFor('roadcrash-closed')).toBe('hazard');
  });

  it('sends everything else to the fire layer', () => {
    for (const c of [
      'fire',
      'alarm',
      'advice',
      'other',
      'plannedburn',
      'bushfire-emergency',
      'bushfire-watch',
      'bushfire-advice',
    ]) {
      expect(ntLayerFor(c)).toBe('fire');
    }
  });
});

describe('toNtFeature', () => {
  it('maps a record onto the RFS property shape', () => {
    const f = toNtFeature(FIRE)!;
    expect(f.geometry).toEqual({
      type: 'Point',
      coordinates: [133.80775292941, -23.75526134541],
    });
    expect(f.properties).toMatchObject({
      // Named for RFS on purpose: archiveExtract promotes alertLevel ->
      // category and fireType -> subcategory generically.
      alertLevel: 'Advice',
      fireType: 'Grass and Scrub Fire',
      status: 'Active',
      location: 'GREATOREX RD, ILPARPA',
      agency: 'NTFRS',
      source: 'nt_fire',
      layer: 'fire',
      is_active: true,
    });
  });

  it('builds a headline from the event type and location', () => {
    expect(toNtFeature(FIRE)!.properties.title).toBe(
      'Grass and Scrub Fire - GREATOREX RD, ILPARPA',
    );
  });

  it('keeps a closed record but marks it inactive', () => {
    // Dropping it would lose the incident's final transition, which is
    // the whole reason the logs page can show a timeline.
    const closed = { ...FIRE, properties: { ...FIRE.properties, _status: 'closed' } };
    const f = toNtFeature(closed)!;
    expect(f).not.toBeNull();
    expect(f.properties.is_active).toBe(false);
  });

  it('believes _status over a stale Status', () => {
    // Seen in the real feed: the map-key status says closed while the
    // operational status still reads Going.
    const f = toNtFeature({
      ...FIRE,
      properties: { ...FIRE.properties, _status: 'closed', Status: 'Going' },
    })!;
    expect(f.properties.is_active).toBe(false);
    expect(f.properties.status).toBe('Going');
  });

  it('carries the BushfiresNT advice text when present', () => {
    const f = toNtFeature({
      ...FIRE,
      properties: {
        ...FIRE.properties,
        _category: 'bushfire-advice',
        'Current Situation': 'Response Underway',
        Risks: 'Active fire may occur close to the roadside',
        'What to do': 'Conditions may change,\n monitor conditions',
        'Advice to the Public': 'Drive safely',
      },
    })!;
    expect(f.properties.currentSituation).toBe('Response Underway');
    // newlines collapse rather than survive into the UI
    expect(f.properties.whatToDo).toBe('Conditions may change, monitor conditions');
  });

  it('accepts a polygon fire ground and derives a point from it', () => {
    const f = toNtFeature({
      type: 'Feature',
      geometry: {
        type: 'Polygon',
        coordinates: [[[130, -12], [131, -12], [131, -13], [130, -13], [130, -12]]],
      },
      properties: { ...FIRE.properties, _category: 'bushfire-advice' },
    })!;
    expect(f.geometry.type).toBe('Polygon');
    expect(f.properties.guid).toMatch(/^ntf:/);
  });

  it('returns null when the record cannot be placed', () => {
    expect(toNtFeature({ ...FIRE, geometry: { type: 'Point', coordinates: [] } })).toBeNull();
    expect(toNtFeature({ ...FIRE, geometry: undefined })).toBeNull();
    expect(toNtFeature(null)).toBeNull();
  });
});

describe('ntIncidentId', () => {
  it('is stable across everything that changes during an incident', () => {
    const open = toNtFeature(FIRE)!;
    const closed = toNtFeature({
      ...FIRE,
      properties: {
        ...FIRE.properties,
        _status: 'closed',
        Status: 'Closed',
        'Alert Level': 'Advice - Decreasing Threat',
        _lastupdate: '2026-08-31T04:00:00+09:30',
        'Last Update': '31/08/2026 04:00',
        _dateclosed: '2026-08-31T04:00:00+09:30',
      },
    })!;
    // Same incident, later in its life — must archive under one id or
    // its history splits into two records.
    expect(closed.properties.guid).toBe(open.properties.guid);
  });

  it('separates two incidents at the same spot notified at different times', () => {
    const a = ntIncidentId(133.8, -23.7, 'Grass Fire', '2026-08-30T19:11:00+09:30');
    const b = ntIncidentId(133.8, -23.7, 'Grass Fire', '2026-08-31T06:00:00+09:30');
    expect(a).not.toBe(b);
  });
});

describe('fetchNtFire', () => {
  beforeEach(() => {
    fetchJson.mockReset();
  });

  it('unwraps the nested incidents.features array', async () => {
    fetchJson.mockResolvedValueOnce(wrap([FIRE]));
    const snap = await fetchNtFire();
    expect(snap.type).toBe('FeatureCollection');
    expect(snap.count).toBe(1);
  });

  it('keeps closed records in the snapshot', async () => {
    fetchJson.mockResolvedValueOnce(
      wrap([FIRE, { ...FIRE, properties: { ...FIRE.properties, _status: 'closed', _datenotified: 'x' } }]),
    );
    const snap = await fetchNtFire();
    expect(snap.count).toBe(2);
    expect(snap.features.filter((f) => !f.properties.is_active)).toHaveLength(1);
  });

  it('collapses a duplicate rather than stacking two markers', async () => {
    fetchJson.mockResolvedValueOnce(wrap([FIRE, FIRE]));
    expect((await fetchNtFire()).count).toBe(1);
  });

  it('throws when the payload has no incidents.features', async () => {
    fetchJson.mockResolvedValueOnce({ title: 'NT Incident Map' });
    await expect(fetchNtFire()).rejects.toThrow(/incidents.features/);
  });

  it('propagates an upstream failure so the poller backs off', async () => {
    fetchJson.mockRejectedValueOnce(new Error('503'));
    await expect(fetchNtFire()).rejects.toThrow('503');
  });
});
