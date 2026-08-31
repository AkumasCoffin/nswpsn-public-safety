/**
 * Queensland Fire Department source tests.
 *
 * The things worth pinning are the ones that would be silently wrong:
 * epoch-millisecond dates passed through unconverted (which would date
 * records to the year 58,000), road crashes landing on the Fires layer,
 * and the two feeds' ids colliding now they share one archive table.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import {
  fetchQldIncidents,
  fetchQldWarnings,
  toIncidentFeature,
  toWarningFeature,
  qldLayerFor,
  esriDateToIso,
  warningIncidentKey,
} from '../../../src/sources/qldFire.js';

const INCIDENT = {
  type: 'Feature',
  geometry: { type: 'Point', coordinates: [145.456449, -17.271371] },
  properties: {
    OBJECTID: 18286,
    Master_Incident_Number: 'QF7-26-104515',
    Response_Date: 1786580181000,
    LastUpdate: 1788045753000,
    CurrentStatus: 'Going',
    Location: 'Rifle Range Rd',
    Jurisdiction: '7 Far Northern Region',
    VehiclesAssigned: 0,
    VehiclesOnRoute: 2,
    VehiclesOnScene: 1,
    GroupedType: 'FIRE VEGETATION',
    Locality: 'ATHERTON',
  },
};

const WARNING = {
  type: 'Feature',
  geometry: { type: 'Point', coordinates: [151.9, -25.0] },
  properties: {
    UniqueID: 'WARN-293',
    WarningTitle: 'STAY INFORMED - Doughboy (near Gin Gin) - fire',
    MasterIncidentNum: null,
    WarningLevel: 'Advice',
    CallToAction: 'Stay Informed',
    WarningText: 'A fire is burning near the Doughboy Campground.\r\n\r\nStay informed.',
    ModifiedDate: 1788045753000,
    WarningArea: 'Doughboy',
    EventType: 'Fire',
    Impacts: 'Some roads may be closed.',
    ShouldDo: 'Check for updates often.',
  },
};

const wrap = (features: unknown[]) => ({ type: 'FeatureCollection', features });

describe('esriDateToIso', () => {
  it('reads epoch milliseconds, not seconds', () => {
    // Passed through as seconds this lands in the year 58,000.
    expect(esriDateToIso(1788045753000)).toBe(new Date(1788045753000).toISOString());
    expect(new Date(esriDateToIso(1788045753000)).getUTCFullYear()).toBe(2026);
  });

  it('returns empty for anything unusable', () => {
    expect(esriDateToIso(null)).toBe('');
    expect(esriDateToIso(0)).toBe('');
    expect(esriDateToIso('not a date')).toBe('');
  });
});

describe('qldLayerFor', () => {
  it('sends rescues and road crashes to the hazard layer', () => {
    expect(qldLayerFor('RESCUE ROAD CRASH')).toBe('hazard');
    expect(qldLayerFor('RESCUE VERTICAL')).toBe('hazard');
  });

  it('keeps every fire type on the fire layer', () => {
    for (const k of ['FIRE VEGETATION', 'FIRE PERMITTED BURN', 'FIRE VEHICLE', 'Fire']) {
      expect(qldLayerFor(k)).toBe('fire');
    }
  });

  it('treats a non-fire warning type as a hazard', () => {
    expect(qldLayerFor('Flood')).toBe('hazard');
    expect(qldLayerFor('Cyclone')).toBe('hazard');
  });
});

describe('toIncidentFeature', () => {
  it('maps onto the shared fire property shape', () => {
    const f = toIncidentFeature(INCIDENT)!;
    expect(f.properties).toMatchObject({
      guid: 'qfd:QF7-26-104515',
      status: 'Going',
      fireType: 'FIRE VEGETATION',
      location: 'Rifle Range Rd, ATHERTON',
      agency: 'QFD',
      source: 'qld_fire',
      layer: 'fire',
      vehiclesOnRoute: 2,
      vehiclesOnScene: 1,
    });
    // ESCAD publishes no alert level; blank drops these into the same
    // "Not Applicable" bucket as RFS records without one.
    expect(f.properties.alertLevel).toBe('');
    expect(f.properties.updatedISO).toBe(new Date(1788045753000).toISOString());
  });

  it('falls back to the response time when LastUpdate is missing', () => {
    const f = toIncidentFeature({
      ...INCIDENT,
      properties: { ...INCIDENT.properties, LastUpdate: null },
    })!;
    expect(f.properties.updatedISO).toBe(new Date(1786580181000).toISOString());
  });

  it('routes a road crash to the hazard layer', () => {
    const f = toIncidentFeature({
      ...INCIDENT,
      properties: { ...INCIDENT.properties, GroupedType: 'RESCUE ROAD CRASH' },
    })!;
    expect(f.properties.layer).toBe('hazard');
  });

  it('returns null without an incident number or a point', () => {
    expect(toIncidentFeature({ ...INCIDENT, properties: { ...INCIDENT.properties, Master_Incident_Number: null } })).toBeNull();
    expect(toIncidentFeature({ ...INCIDENT, geometry: { type: 'Point', coordinates: [] } })).toBeNull();
    expect(toIncidentFeature(null)).toBeNull();
  });
});

describe('toWarningFeature', () => {
  it('maps the warning level onto alertLevel, which drives the colours', () => {
    const f = toWarningFeature(WARNING)!;
    expect(f.properties).toMatchObject({
      guid: 'qfdw:WARN-293',
      alertLevel: 'Advice',
      status: 'Stay Informed',
      fireType: 'Fire',
      location: 'Doughboy',
      source: 'qld_warning',
      layer: 'fire',
    });
  });

  it('keeps the public advice text, collapsed to prose', () => {
    const f = toWarningFeature(WARNING)!;
    expect(f.properties.warningText).toBe(
      'A fire is burning near the Doughboy Campground. Stay informed.',
    );
    expect(f.properties.impacts).toBe('Some roads may be closed.');
  });

  it('carries the incident reference when upstream sets one', () => {
    // Null on every record today, so the join is dormant — but the
    // moment it is populated the map can fold the warning into its
    // incident with no code change.
    expect(warningIncidentKey(WARNING.properties)).toBe('');
    expect(warningIncidentKey({ MasterIncidentNum: 'QF7-26-104515' })).toBe('QF7-26-104515');
    const f = toWarningFeature({
      ...WARNING,
      properties: { ...WARNING.properties, MasterIncidentNum: 'QF7-26-104515' },
    })!;
    expect(f.properties.incidentRef).toBe('QF7-26-104515');
  });
});

describe('ids', () => {
  it('cannot collide between the two feeds sharing one archive table', () => {
    const i = toIncidentFeature(INCIDENT)!;
    const w = toWarningFeature({
      ...WARNING,
      properties: { ...WARNING.properties, UniqueID: 'QF7-26-104515' },
    })!;
    // Same underlying string, different prefixes.
    expect(i.properties.guid).not.toBe(w.properties.guid);
  });
});

describe('fetch', () => {
  beforeEach(() => {
    fetchJson.mockReset();
  });

  it('returns both feeds as FeatureCollections', async () => {
    fetchJson.mockResolvedValueOnce(wrap([INCIDENT]));
    expect((await fetchQldIncidents()).count).toBe(1);
    fetchJson.mockResolvedValueOnce(wrap([WARNING]));
    expect((await fetchQldWarnings()).count).toBe(1);
  });

  it('collapses a duplicate rather than stacking two pins', async () => {
    fetchJson.mockResolvedValueOnce(wrap([INCIDENT, INCIDENT]));
    expect((await fetchQldIncidents()).count).toBe(1);
  });

  it('throws on an ArcGIS error body, which arrives with status 200', async () => {
    fetchJson.mockResolvedValueOnce({ error: { code: 400, message: 'Invalid query' } });
    await expect(fetchQldIncidents()).rejects.toThrow(/no features array/);
  });

  it('propagates an upstream failure so the poller backs off', async () => {
    fetchJson.mockRejectedValueOnce(new Error('503'));
    await expect(fetchQldWarnings()).rejects.toThrow('503');
  });
});
