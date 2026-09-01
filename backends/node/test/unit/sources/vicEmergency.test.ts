/**
 * Victorian emergency feed tests.
 *
 * Two things here would be wrong silently rather than loudly: routing a
 * flood onto the Fires layer because a fire service published it, and
 * reading `category1` as an alert level on records where it is really
 * the incident type. Both are pinned below, along with the NSW/RFS
 * re-publication that must never be kept.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import {
  fetchVicEmergency,
  isInterstateRepublication,
  toVicFeature,
  vicLayerFor,
  vicAgencyFor,
} from '../../../src/sources/vicEmergency.js';

const CFA_FIRE = {
  type: 'Feature',
  geometry: { type: 'Point', coordinates: [144.36, -38.02] },
  properties: {
    feedType: 'incident',
    sourceOrg: 'VIC/CFA',
    sourceFeed: 'cfa-incident',
    sourceId: '291372',
    id: 'ESTA:260827763',
    sourceTitle: 'Sparrowhawk Av',
    category1: 'Fire',
    category2: 'Bushfire',
    status: 'Responding',
    location: 'Sparrowhawk Av, Lara',
    created: '2026-08-31T05:40:00.000Z',
    updated: '2026-08-31T05:41:00.000Z',
    resources: '1',
    sizeFmt: 'Small',
  },
};

const FLOOD_WARNING = {
  type: 'Feature',
  geometry: {
    type: 'GeometryCollection',
    geometries: [
      { type: 'Point', coordinates: [144.9, -35.9] },
      { type: 'Polygon', coordinates: [[[144, -35], [145, -35], [145, -36], [144, -36], [144, -35]]] },
    ],
  },
  properties: {
    feedType: 'warning',
    sourceOrg: 'EMV',
    sourceFeed: 'cop-cap',
    id: '43054',
    sourceTitle: 'Advice',
    category1: 'Advice',
    category2: 'Met',
    status: 'Minor',
    action: 'Stay Informed',
    location: 'Murray River downstream of Tocumwal to Barham',
    updated: '2026-08-31T02:28:48.000Z',
    text: 'ADVICE - RIVERINE FLOOD - Stay Informed',
    cap: {
      category: 'Met',
      event: 'Riverine Flood',
      urgency: 'Expected',
      severity: 'Minor',
      certainty: 'Unknown',
      responseType: 'Monitor',
      senderName: 'State Emergency Service',
    },
  },
};

const wrap = (features: unknown[]) => ({ type: 'FeatureCollection', features });

describe('vicAgencyFor', () => {
  it.each([
    ['VIC/CFA', 'CFA'],
    ['VIC/DEECA', 'DEECA'],
    ['VIC/SES', 'SES'],
    ['EMV', 'EMV'],
    ['VIC/ESTA', 'ESTA'],
  ])('maps %s to %s', (org, code) => {
    expect(vicAgencyFor(org)).toBe(code);
  });

  it('keeps an unrecognised publisher as itself rather than bucketing it', () => {
    expect(vicAgencyFor('VIC/SOMETHING-NEW')).toBe('VIC/SOMETHING-NEW');
  });
});

describe('vicLayerFor — routing is by event, not by agency', () => {
  it('sends a CFA fire to Fires', () => {
    expect(vicLayerFor({ category1: 'Fire', category2: 'Bushfire' })).toBe('fire');
    expect(vicLayerFor({ category1: 'Planned Burn', category2: 'Planned Burn' })).toBe('fire');
  });

  it('sends what CFA attends that is not a fire to Hazards', () => {
    // A fire service runs to more than fires; the layer follows the
    // event, so these must not sit with the bushfires.
    expect(vicLayerFor({ category1: 'Accident / Rescue', category2: 'Road Accident' })).toBe('hazard');
    expect(vicLayerFor({ category1: 'Hazardous Material', category2: 'Hazardous Material' })).toBe('hazard');
    expect(vicLayerFor({ category1: 'Other', category2: 'Other' })).toBe('hazard');
  });

  it('sends flood warnings to Floods even though EMV publishes them', () => {
    expect(vicLayerFor({ category1: 'Advice', category2: 'Met', cap: { event: 'Riverine Flood', category: 'Met' } })).toBe('flood');
    expect(vicLayerFor({ category1: 'Accident / Rescue', category2: 'Washaway' })).toBe('hazard');
  });

  it('prefers flood over fire when a record names both', () => {
    // A flood rescue names both; the water is the thing to show.
    expect(vicLayerFor({ category1: 'Fire', category2: 'Flood Rescue' })).toBe('flood');
  });

  it('sends a CAP fire warning to Fires', () => {
    expect(vicLayerFor({ category1: 'Advice', category2: 'Fire', cap: { event: 'Grass Fire', category: 'Fire' } })).toBe('fire');
  });
});

describe('toVicFeature', () => {
  it('maps a CFA incident onto the shared fire property shape', () => {
    const f = toVicFeature(CFA_FIRE)!;
    expect(f.properties).toMatchObject({
      guid: 'vic:ESTA:260827763',
      status: 'Responding',
      fireType: 'Bushfire',
      location: 'Sparrowhawk Av, Lara',
      agency: 'CFA',
      layer: 'fire',
      source: 'vic_emergency',
    });
  });

  it('gives an incident NO alert level, because category1 is its type there', () => {
    // Reading category1 as a level would label this callout "Fire".
    expect(toVicFeature(CFA_FIRE)!.properties.alertLevel).toBe('');
  });

  it('reads the alert level on a warning, where category1 does hold it', () => {
    const f = toVicFeature(FLOOD_WARNING)!;
    expect(f.properties.alertLevel).toBe('Advice');
    expect(f.properties.layer).toBe('flood');
    expect(f.properties.agency).toBe('EMV');
  });

  it('keeps the CAP block for the detail panel', () => {
    const f = toVicFeature(FLOOD_WARNING)!;
    expect(f.properties).toMatchObject({
      capEvent: 'Riverine Flood',
      capSeverity: 'Minor',
      capUrgency: 'Expected',
      capResponseType: 'Monitor',
      capSender: 'State Emergency Service',
    });
  });

  it('takes the point and the rings out of a warning GeometryCollection', () => {
    const f = toVicFeature(FLOOD_WARNING)!;
    expect(f.geometry).toEqual({ type: 'Point', coordinates: [144.9, -35.9] });
    expect(f.properties.polygons).toHaveLength(1);
    expect(f.properties.polygons[0]).toHaveLength(5);
  });

  it('falls back to a ring centroid when a warning has no point', () => {
    const f = toVicFeature({
      ...FLOOD_WARNING,
      geometry: {
        type: 'GeometryCollection',
        geometries: [
          { type: 'Polygon', coordinates: [[[144, -35], [146, -35], [146, -37], [144, -37], [144, -35]]] },
        ],
      },
    })!;
    expect(f.geometry.coordinates[0]).toBeCloseTo(144.8, 1);
  });

  it('drops any state re-publishing its neighbour, not just NSW', () => {
    // We ingest every one of these states directly. The SA/CFS case is
    // how this was found: one Paringa building fire drew two pins, the
    // Victorian copy saying "Going" and the CFS original "GOING", with
    // the appliance count on the original only.
    for (const org of ['NSW/RFS', 'SA/CFS', 'QLD/QFES', 'NT/NTFRS', 'WA/DFES']) {
      expect(toVicFeature({
        ...CFA_FIRE,
        properties: { ...CFA_FIRE.properties, sourceOrg: org },
      })).toBeNull();
    }
  });

  it('keeps the Victorian agencies and the national relays', () => {
    // EMV is the state warning layer and AU/* the Bureau and Geoscience
    // Australia — neither duplicates a pin drawn from another feed.
    for (const org of ['VIC/CFA', 'VIC/SES', 'VIC/DEECA', 'EMV', 'AU/BOM']) {
      expect(isInterstateRepublication(org)).toBe(false);
    }
    for (const org of ['NSW/RFS', 'SA/CFS', 'TAS/TFS']) {
      expect(isInterstateRepublication(org)).toBe(true);
    }
  });

  it('names a warning by its event and place, not by its alert level', () => {
    // sourceTitle on a warning is the level - six live records all
    // called "Advice" would be six identical pins.
    expect(toVicFeature(FLOOD_WARNING)!.properties.title).toBe(
      'Riverine Flood - Murray River downstream of Tocumwal to Barham',
    );
  });

  it('replaces the literal "Undefined" SES sends as a title', () => {
    const f = toVicFeature({
      ...CFA_FIRE,
      properties: {
        ...CFA_FIRE.properties,
        sourceOrg: 'VIC/SES',
        sourceTitle: 'Undefined',
        category1: 'Building Damage',
        category2: 'Building Damage',
      },
    })!;
    expect(f.properties.title).toBe('Building Damage');
  });

  it('returns null without an id or a usable geometry', () => {
    expect(toVicFeature({ ...CFA_FIRE, properties: { ...CFA_FIRE.properties, id: '', sourceId: '' } })).toBeNull();
    expect(toVicFeature({ ...CFA_FIRE, geometry: { type: 'Point', coordinates: [] } })).toBeNull();
    expect(toVicFeature(null)).toBeNull();
  });
});

describe('fetchVicEmergency', () => {
  beforeEach(() => {
    fetchJson.mockReset();
  });

  it('keeps the Victorian records and drops the NSW one', async () => {
    fetchJson.mockResolvedValueOnce(
      wrap([
        CFA_FIRE,
        FLOOD_WARNING,
        { ...CFA_FIRE, properties: { ...CFA_FIRE.properties, id: 'x', sourceOrg: 'NSW/RFS' } },
      ]),
    );
    const snap = await fetchVicEmergency();
    expect(snap.count).toBe(2);
    expect(snap.features.every((f) => f.properties.agency !== 'NSW/RFS')).toBe(true);
  });

  it('collapses a duplicate id', async () => {
    fetchJson.mockResolvedValueOnce(wrap([CFA_FIRE, CFA_FIRE]));
    expect((await fetchVicEmergency()).count).toBe(1);
  });

  it('throws when the payload carries no features', async () => {
    fetchJson.mockResolvedValueOnce({ type: 'FeatureCollection' });
    await expect(fetchVicEmergency()).rejects.toThrow(/no features array/);
  });

  it('propagates an upstream failure so the poller backs off', async () => {
    fetchJson.mockRejectedValueOnce(new Error('503'));
    await expect(fetchVicEmergency()).rejects.toThrow('503');
  });
});
