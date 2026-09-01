/**
 * Western Australia (DFES) feed tests.
 *
 * Three things here would be wrong silently rather than loudly:
 *
 *   - a chemical incident landing on the Fires layer, because WA words
 *     one as `hazmat-type: "HAZMAT Fire"` and the word "Fire" is in it;
 *   - a warning pinned at its suburb centroid instead of the hazard,
 *     because `location` and the geo-source Point are different places;
 *   - the incident/warning join breaking, which is the whole reason both
 *     feeds are read.
 *
 * All three are pinned below.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import {
  fetchWaIncidents,
  fetchWaWarnings,
  toWaIncident,
  toWaWarning,
  waAlertLevel,
  waFireType,
  waLayerFor,
  stripHtml,
} from '../../../src/sources/waEmergency.js';

/** A burn-off: the shape three quarters of the incidents feed takes. */
const BURN_OFF = {
  'geo-source': {
    type: 'FeatureCollection',
    features: [
      { type: 'Feature', geometry: { type: 'Point', coordinates: [115.714, -33.214] }, properties: {} },
    ],
  },
  suburbs: ['LESCHENAULT'],
  lga: ['SHIRE OF HARVEY'],
  'dfes-regions': ['SOUTH WEST'],
  entityType: 'incidents',
  entitySubType: 'incidents_other',
  'incident-type': 'Other Incident',
  'incident-status': 'Active',
  'cad-id': '814163',
  name: 'Burn Off',
  location: { latitude: -33.214, longitude: 115.714, value: 'CATHEDRAL AV' },
  id: 'inc-burnoff',
  event: 'ev-burnoff',
  'updated-date-time': '2026-09-01T11:08:23.000+08:00',
};

/** A bushfire that also has a public warning — same `event` as ADVICE. */
const BUSHFIRE = {
  'geo-source': {
    type: 'FeatureCollection',
    features: [
      { type: 'Feature', geometry: { type: 'Point', coordinates: [126.41, -16.51] }, properties: {} },
    ],
  },
  suburbs: ['DURACK'],
  lga: ['SHIRE OF WYNDHAM-EAST KIMBERLEY'],
  entityType: 'incidents',
  entitySubType: 'incidents_bushfire',
  'incident-type': 'Bushfire',
  'incident-status': 'Monitoring',
  name: 'Bushfire',
  location: { latitude: -16.51, longitude: 126.41, value: 'Gibb River Road' },
  id: 'inc-bushfire',
  event: 'ev-shared',
  'updated-date-time': '2026-08-31T16:14:37.000+08:00',
};

const ROAD_CRASH = {
  ...BURN_OFF,
  name: 'Road Crash',
  'incident-status': 'On scene',
  id: 'inc-crash',
  event: 'ev-crash',
};

/** The Advice for BUSHFIRE: a Point AND a Polygon, joined on `event`. */
const ADVICE = {
  'geo-source': {
    type: 'FeatureCollection',
    features: [
      { type: 'Feature', geometry: { type: 'Point', coordinates: [126.42, -16.52] }, properties: {} },
      {
        type: 'Feature',
        geometry: {
          type: 'Polygon',
          coordinates: [[[126.4, -16.5], [126.5, -16.5], [126.5, -16.6], [126.4, -16.5]]],
        },
        properties: {},
      },
    ],
  },
  suburbs: ['DURACK'],
  lga: ['SHIRE OF WYNDHAM-EAST KIMBERLEY'],
  entityType: 'warnings',
  entitySubType: 'warnings_bushfire--advice',
  'warning-type': 'Bushfire Advice',
  'cap-category': 'Fire',
  'cap-event-type': ['Bushfire'],
  'cap-severity': 'Minor - minimal threat',
  'alert-line': '<p>A Bushfire Advice is in place for DURACK.</p>',
  'what-to-do-note': '<ul><li><p>Stay alert &amp; monitor your surroundings.</p></li></ul>',
  'action-statement': 'Monitor conditions',
  'action-statement--type': 'Escalating',
  headline: 'DURACK and GIBB',
  title: 'MONITOR CONDITIONS - DURACK AND GIBB',
  name: 'Bushfire Advice',
  location: { latitude: -16.51, longitude: 126.4156, value: 'Gibb, Western Australia' },
  id: 'warn-advice',
  event: 'ev-shared',
  'issued-date-time': '2026-09-01T10:45:47.132+08:00',
};

/**
 * The trap. `hazmat-type` says "HAZMAT Fire" and `cap-severity` says
 * Extreme, but this is a chemical incident with no AWS level at all.
 */
const HAZMAT = {
  'geo-source': {
    type: 'FeatureCollection',
    features: [
      { type: 'Feature', geometry: { type: 'Point', coordinates: [114.1103, -22.0208] }, properties: {} },
      {
        type: 'Feature',
        geometry: {
          type: 'Polygon',
          coordinates: [[[114.12, -22.01], [114.11, -22.01], [114.1, -22.03], [114.12, -22.01]]],
        },
        properties: {},
      },
    ],
  },
  suburbs: ['NORTH WEST CAPE'],
  lga: ['SHIRE OF EXMOUTH'],
  entityType: 'warnings',
  entitySubType: 'warnings_hazmat-general-warning',
  'warning-type': 'Hazmat General Warning',
  'hazmat-type': 'HAZMAT Fire',
  'cap-category': 'CBRNE',
  'cap-event-type': ['Chemical', 'Pollution', 'Radiological', 'Toxic Plume'],
  'cap-severity': 'Extreme - extraordinary threat',
  'cap-urgency': 'Immediate - act now',
  headline: 'RUBBISH TIP FIRE in NORTH WEST CAPE',
  name: 'Hazmat General Warning',
  // Deliberately 13 km from the geo-source Point: this is the suburb, the
  // Point is the tip site.
  location: { latitude: -21.907646, longitude: 114.06092, value: 'North West Cape, Western Australia' },
  id: 'warn-hazmat',
  event: 'ev-hazmat',
  'issued-date-time': '2026-08-28T11:34:20.644+08:00',
};

const wrapInc = (incidents: unknown[]) => ({ incidents });
const wrapWarn = (warnings: unknown[]) => ({ warnings });

describe('waLayerFor — routing is by event, not by agency', () => {
  it('sends a bushfire to Fires', () => {
    expect(waLayerFor('Bushfire incidents_bushfire')).toBe('fire');
  });

  it('sends a burn-off to Fires', () => {
    expect(waLayerFor('Burn Off Other Incident')).toBe('fire');
  });

  it('sends a road crash to Hazards', () => {
    expect(waLayerFor('Road Crash Other Incident')).toBe('hazard');
  });

  it('sends a flood to Floods, not Fires', () => {
    expect(waLayerFor('Flood Warning warnings_flood Met')).toBe('flood');
  });

  it('sends a HAZMAT Fire to Hazards, not Fires', () => {
    // The whole reason hazmat is tested before fire. This string is what
    // the live feed actually publishes for a chemical incident.
    expect(waLayerFor('Hazmat General Warning HAZMAT Fire CBRNE Chemical Toxic Plume')).toBe(
      'hazard',
    );
  });

  it('sends anything it does not recognise to Hazards rather than Fires', () => {
    expect(waLayerFor('School Closure')).toBe('hazard');
  });
});

describe('waAlertLevel', () => {
  it.each([
    ['warnings_bushfire--emergency-warning', 'Bushfire Emergency Warning', 'Emergency Warning'],
    ['warnings_bushfire--watch-and-act', 'Bushfire Watch and Act', 'Watch and Act'],
    ['warnings_bushfire--advice', 'Bushfire Advice', 'Advice'],
    ['warnings_bushfire--all-clear', 'All Clear', 'All Clear'],
  ])('reads %s as %s', (subtype, type, level) => {
    expect(waAlertLevel(type, subtype)).toBe(level);
    // Either field alone is enough; upstream may reword one of them.
    expect(waAlertLevel(type, '')).toBe(level);
    expect(waAlertLevel('', subtype)).toBe(level);
  });

  it('leaves a warning with no level in its name blank', () => {
    // Not a gap: it lands in the existing "Not Applicable" bucket, where
    // the pin falls back to showing what kind of thing it is.
    expect(waAlertLevel('Hazmat General Warning', 'warnings_hazmat-general-warning')).toBe('');
  });

  it('does not read the level off cap-severity', () => {
    // The live hazmat warning is "Extreme - extraordinary threat" while
    // being an ordinary general warning. Severity is not alert level.
    const f = toWaWarning(HAZMAT)!;
    expect(f.properties.capSeverity).toBe('Extreme - extraordinary threat');
    expect(f.properties.alertLevel).toBe('');
  });
});

describe('waFireType', () => {
  it('takes the level out so one fire is one type pill', () => {
    expect(waFireType('Bushfire Advice')).toBe('Bushfire');
    expect(waFireType('Bushfire Emergency Warning')).toBe('Bushfire');
    expect(waFireType('Bushfire Watch and Act')).toBe('Bushfire');
  });

  it('keeps a type that is nothing but a level rather than emptying it', () => {
    expect(waFireType('All Clear')).toBe('All Clear');
  });
});

describe('stripHtml', () => {
  it('unwraps the HTML fragments WA writes its notes in', () => {
    expect(stripHtml('<ul><li><p>Stay alert &amp; monitor.</p></li></ul>')).toBe(
      'Stay alert & monitor.',
    );
  });

  it('drops anchors but keeps what they said', () => {
    expect(stripHtml('<p>call <a href="tel:1800633422">1800 633 422</a>.</p>')).toBe(
      'call 1800 633 422.',
    );
  });
});

describe('toWaIncident', () => {
  it('maps a burn-off', () => {
    const f = toWaIncident(BURN_OFF)!;
    expect(f.geometry.coordinates).toEqual([115.714, -33.214]);
    expect(f.properties).toMatchObject({
      guid: 'wa:inc-burnoff',
      title: 'Burn Off - LESCHENAULT',
      status: 'Active',
      fireType: 'Burn Off',
      alertLevel: '',
      layer: 'fire',
      eventRef: 'ev-burnoff',
      cadId: '814163',
      region: 'SOUTH WEST',
      agency: 'DFES',
      source: 'wa_incident',
      is_active: true,
    });
  });

  it('uses `name`, not `incident-type`, as the type', () => {
    // `incident-type` reads "Other Incident" on three quarters of the
    // feed; keying pills on it would collapse burn-offs and road crashes
    // into one meaningless bucket.
    expect(toWaIncident(BURN_OFF)!.properties.fireType).toBe('Burn Off');
    expect(toWaIncident(ROAD_CRASH)!.properties.fireType).toBe('Road Crash');
  });

  it('sends the road crash to Hazards while the burn-off stays on Fires', () => {
    expect(toWaIncident(ROAD_CRASH)!.properties.layer).toBe('hazard');
    expect(toWaIncident(BURN_OFF)!.properties.layer).toBe('fire');
  });

  it('names street, suburb and shire once each', () => {
    expect(toWaIncident(BURN_OFF)!.properties.location).toBe(
      'CATHEDRAL AV, LESCHENAULT, SHIRE OF HARVEY',
    );
  });

  it('carries no alert level — that is what the warnings feed is for', () => {
    expect(toWaIncident(BUSHFIRE)!.properties.alertLevel).toBe('');
  });

  it('drops a record with no id and one that cannot be placed', () => {
    expect(toWaIncident({ ...BURN_OFF, id: '' })).toBeNull();
    expect(toWaIncident({ ...BURN_OFF, 'geo-source': null, location: null })).toBeNull();
    expect(toWaIncident(null)).toBeNull();
  });
});

describe('toWaWarning', () => {
  it('maps an Advice, level and all', () => {
    const f = toWaWarning(ADVICE)!;
    expect(f.properties).toMatchObject({
      guid: 'waw:warn-advice',
      alertLevel: 'Advice',
      fireType: 'Bushfire',
      layer: 'fire',
      action: 'Monitor conditions',
      actionType: 'Escalating',
      agency: 'DFES',
      source: 'wa_warning',
    });
    expect(f.properties.alertLine).toBe('A Bushfire Advice is in place for DURACK.');
  });

  it('pins at the geo-source Point, not the suburb centroid', () => {
    // These are 13 km apart on the live record. The Point is the hazard.
    const f = toWaWarning(HAZMAT)!;
    expect(f.geometry.coordinates).toEqual([114.1103, -22.0208]);
    expect(f.geometry.coordinates).not.toEqual([114.06092, -21.907646]);
  });

  it('falls back to `location` when there is no Point feature', () => {
    const noPoint = {
      ...HAZMAT,
      'geo-source': { type: 'FeatureCollection', features: [] },
    };
    expect(toWaWarning(noPoint)!.geometry.coordinates).toEqual([114.06092, -21.907646]);
  });

  it('keeps the warning area as GeoJSON outer rings', () => {
    const rings = toWaWarning(ADVICE)!.properties.polygons;
    expect(rings).toHaveLength(1);
    expect(rings[0]).toHaveLength(4);
    // [lon, lat] order preserved — the frontend flips it for Leaflet.
    expect(rings[0]![0]).toEqual([126.4, -16.5]);
  });

  it('leaves status blank rather than filing an instruction as one', () => {
    // "Monitor conditions" is what to do, not how contained the fire is.
    // Putting it in `status` would fill the logs page STATUS facet with
    // things that are not statuses.
    const f = toWaWarning(ADVICE)!;
    expect(f.properties.status).toBe('');
    expect(f.properties.action).toBe('Monitor conditions');
  });

  it('exposes the event id so the warning can be folded into its incident', () => {
    expect(toWaWarning(ADVICE)!.properties.incidentRef).toBe('ev-shared');
    expect(toWaIncident(BUSHFIRE)!.properties.eventRef).toBe('ev-shared');
  });
});

describe('the incident/warning join', () => {
  it('matches a warning to its incident on `event` — the field QLD lacks', () => {
    const inc = [BURN_OFF, BUSHFIRE, ROAD_CRASH].map(toWaIncident).map((f) => f!.properties);
    const warn = [ADVICE, HAZMAT].map(toWaWarning).map((f) => f!.properties);

    const byEvent = new Map(warn.map((w) => [w.incidentRef, w]));
    const joined = inc.filter((i) => byEvent.has(i.eventRef));

    expect(joined.map((i) => i.guid)).toEqual(['wa:inc-bushfire']);
    expect(byEvent.get(joined[0]!.eventRef)!.alertLevel).toBe('Advice');
    // The hazmat warning belongs to no listed incident and must survive
    // on its own rather than being folded into nothing.
    expect(warn.filter((w) => !inc.some((i) => i.eventRef === w.incidentRef))).toHaveLength(1);
  });
});

describe('fetchWaIncidents / fetchWaWarnings', () => {
  beforeEach(() => {
    fetchJson.mockReset();
  });

  it('reads the incidents array', async () => {
    fetchJson.mockResolvedValue(wrapInc([BURN_OFF, BUSHFIRE]));
    const snap = await fetchWaIncidents();
    expect(snap.count).toBe(2);
    expect(snap.features.map((f) => f.properties.guid)).toEqual([
      'wa:inc-burnoff',
      'wa:inc-bushfire',
    ]);
  });

  it('reads the warnings array', async () => {
    fetchJson.mockResolvedValue(wrapWarn([ADVICE, HAZMAT]));
    const snap = await fetchWaWarnings();
    expect(snap.count).toBe(2);
  });

  it('keeps one record per guid', async () => {
    fetchJson.mockResolvedValue(wrapInc([BURN_OFF, BURN_OFF]));
    expect((await fetchWaIncidents()).count).toBe(1);
  });

  it('throws when the payload is reshaped, so the poller backs off', async () => {
    // Publishing an empty snapshot here would read as "WA has gone
    // quiet" rather than "we can no longer parse WA".
    fetchJson.mockResolvedValue({ somethingElse: [] });
    await expect(fetchWaIncidents()).rejects.toThrow(/no incidents array/);
    fetchJson.mockResolvedValue({ incidents: [] });
    await expect(fetchWaWarnings()).rejects.toThrow(/no warnings array/);
  });

  it('propagates an upstream failure', async () => {
    fetchJson.mockRejectedValue(new Error('502 Bad Gateway'));
    await expect(fetchWaWarnings()).rejects.toThrow('502 Bad Gateway');
  });

  it('drops unplaceable records without failing the batch', async () => {
    fetchJson.mockResolvedValue(
      wrapInc([BURN_OFF, { ...BUSHFIRE, 'geo-source': null, location: null }]),
    );
    expect((await fetchWaIncidents()).count).toBe(1);
  });
});
