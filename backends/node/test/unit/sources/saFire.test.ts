/**
 * South Australia (CFS + MFS) feed tests.
 *
 * Four things here would be wrong silently rather than loudly:
 *
 *   - a car crash landing on the bushfire layer, because SA writes
 *     "Vehicle Accident" and no other state's crash pattern matches it;
 *   - a timestamp read as an American date, because the feed publishes
 *     DD/MM/YYYY with no offset, from a state on the half hour;
 *   - SA ESS's "File Unavailable" HTML being parsed as a feed, because
 *     it is served with a 200;
 *   - the two agencies' guids colliding, because they share one
 *     dispatch sequence.
 *
 * All four are pinned below.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import {
  adelaideIso,
  fetchSaCfs,
  fetchSaMfs,
  saAlertLevel,
  saLayerFor,
  saResourcesText,
  toSaFeature,
} from '../../../src/sources/saFire.js';

/** A CFS record, verbatim in shape from the live feed. */
const CFS_BURN = {
  IncidentNo: '1722523',
  Date: '01/09/2026',
  Time: '14:47',
  Message: '',
  Message_link: '',
  Location_name: 'POINT TURTON, SAVIO ROAD',
  Region: '2',
  Type: 'Burn Off',
  Status: 'CONTROLLED',
  Level: 1,
  FBD: 'YORKE PENINSULA',
  Resources: 0,
  Aircraft: 0,
  Location: '-34.9516113050384,137.362538723942',
};

const CFS_CRASH = {
  ...CFS_BURN,
  IncidentNo: '1722524',
  Type: 'Vehicle Accident',
  Status: 'GOING',
  Location_name: 'HUMBUG SCRUB, KELLY HILL RD/KERSBROOK RD',
  FBD: 'MOUNT LOFTY RANGES',
  Resources: 3,
  Aircraft: 1,
  Location: '-34.7351868159934,138.798431627002',
};

const CFS_DONE = { ...CFS_BURN, IncidentNo: '1722527', Status: 'COMPLETE' };

/** MFS: the same shape minus Resources/Aircraft/Message, plus DIN. */
const MFS_ALARM = {
  IncidentNo: '1722545',
  Date: '01/09/2026',
  Time: '16:58',
  Location_name: 'GILLES STREET, ADELAIDE',
  Region: 'MFS',
  Type: 'Fire Alarm',
  Status: 'GOING',
  Level: 1,
  FBD: 'ADELAIDE METROPOLITAN',
  Location: '-34.9335194634621,138.611121652341',
  DIN: '119',
};

/** What SA ESS serves for a missing file — with a 200. */
const UNAVAILABLE_HTML =
  '<html><head><title>SA ESS - File Unavailable</title></head>'
  + '<body><p><h3>File currently unavailable, please try again</h3></p></body></html>';

describe('saLayerFor — routing is by event, not by agency', () => {
  it('sends a vehicle accident to Hazards', () => {
    // The reason `accident` had to join the crash pattern: SA's wording
    // contains none of road crash / crash / rescue / collision, so the
    // fire test below would have caught it and put a car crash on the
    // bushfire layer.
    expect(saLayerFor('Vehicle Accident')).toBe('hazard');
  });

  it.each([
    ['Burn Off', 'fire'],
    ['Fire Alarm', 'fire'],
    ['Vehicle Fire', 'fire'],
    ['Bushfire', 'fire'],
    ['Grass Fire', 'fire'],
    ['Flood', 'flood'],
    ['Hazmat', 'hazard'],
    ['Rescue', 'hazard'],
  ])('routes %s to %s', (type, layer) => {
    expect(saLayerFor(type)).toBe(layer);
  });

  it('sends wording it does not recognise to Hazards rather than Fires', () => {
    expect(saLayerFor('Assist Agency')).toBe('hazard');
    expect(saLayerFor('Other')).toBe('hazard');
  });
});

describe('saAlertLevel', () => {
  it.each([
    [1, 'Advice'],
    [2, 'Watch and Act'],
    [3, 'Emergency Warning'],
  ])('reads level %s as %s', (level, label) => {
    expect(saAlertLevel(level)).toBe(label);
    // CFS sends a number and MFS a string for the same field.
    expect(saAlertLevel(String(level))).toBe(label);
  });

  it('treats anything outside the scale as no level', () => {
    for (const v of [0, 4, null, undefined, '', 'x', {}]) {
      expect(saAlertLevel(v)).toBe('');
    }
  });
});

describe('adelaideIso', () => {
  it('reads the date as DD/MM/YYYY, not as an American one', () => {
    // "01/09/2026" is the first of September here and the ninth of
    // January to a JavaScript engine, which is why the raw string is
    // never handed to new Date().
    expect(adelaideIso('01/09/2026', '15:37')).toBe('2026-09-01T15:37:00+09:30');
  });

  it('uses ACST in winter and ACDT in summer', () => {
    expect(adelaideIso('14/08/2026', '09:24')).toBe('2026-08-14T09:24:00+09:30');
    expect(adelaideIso('15/01/2026', '03:05')).toBe('2026-01-15T03:05:00+10:30');
  });

  it('produces a string that parses to the right instant', () => {
    expect(new Date(adelaideIso('01/09/2026', '15:37')).toISOString())
      .toBe('2026-09-01T06:07:00.000Z');
  });

  it('returns nothing rather than a guess when the date is unusable', () => {
    expect(adelaideIso('', '15:37')).toBe('');
    expect(adelaideIso('2026-09-01', '15:37')).toBe('');
    expect(adelaideIso('not a date', '')).toBe('');
  });
});

describe('toSaFeature', () => {
  it('maps a CFS record', () => {
    const f = toSaFeature(CFS_BURN, 'SACFS')!;
    // "lat,lon" as a string becomes [lon, lat] as GeoJSON wants.
    expect(f.geometry.coordinates).toEqual([137.362538723942, -34.9516113050384]);
    expect(f.properties).toMatchObject({
      guid: 'sa:cfs:1722523',
      title: 'Burn Off - POINT TURTON, SAVIO ROAD',
      status: 'CONTROLLED',
      fireType: 'Burn Off',
      alertLevel: 'Advice',
      layer: 'fire',
      district: 'YORKE PENINSULA',
      agency: 'SACFS',
      source: 'sa_cfs',
      is_active: true,
    });
    expect(f.properties.location).toBe('POINT TURTON, SAVIO ROAD, YORKE PENINSULA');
  });

  it('namespaces the guid per agency, because they share a sequence', () => {
    // 1722523 is a CFS incident and 1722545 an MFS one, from one
    // dispatch counter. Nothing stops the numbers meeting.
    const a = toSaFeature({ ...CFS_BURN, IncidentNo: '999' }, 'SACFS')!;
    const b = toSaFeature({ ...MFS_ALARM, IncidentNo: '999' }, 'SAMFS')!;
    expect(a.properties.guid).toBe('sa:cfs:999');
    expect(b.properties.guid).toBe('sa:mfs:999');
    expect(a.properties.guid).not.toBe(b.properties.guid);
  });

  it('closes a record the feed calls COMPLETE and leaves the rest running', () => {
    expect(toSaFeature(CFS_DONE, 'SACFS')!.properties.is_active).toBe(false);
    expect(toSaFeature(CFS_BURN, 'SACFS')!.properties.is_active).toBe(true);
    expect(toSaFeature(CFS_CRASH, 'SACFS')!.properties.is_active).toBe(true);
  });

  it('counts what is turned out, and says nothing when there is nothing', () => {
    expect(saResourcesText(toSaFeature(CFS_CRASH, 'SACFS')!)).toBe('3 appliances, 1 aircraft');
    // Zero is the common case and means the feed has not said.
    expect(saResourcesText(toSaFeature(CFS_BURN, 'SACFS')!)).toBe('');
    // MFS carries neither field at all.
    expect(saResourcesText(toSaFeature(MFS_ALARM, 'SAMFS')!)).toBe('');
  });

  it('says one appliance rather than 1 appliances', () => {
    const f = toSaFeature({ ...CFS_CRASH, Resources: 1, Aircraft: 0 }, 'SACFS')!;
    expect(saResourcesText(f)).toBe('1 appliance');
  });

  it('does not repeat the district when the location already names it', () => {
    const f = toSaFeature(MFS_ALARM, 'SAMFS')!;
    expect(f.properties.location).toBe('GILLES STREET, ADELAIDE, ADELAIDE METROPOLITAN');
  });

  it('drops a record with no id and one that cannot be placed', () => {
    expect(toSaFeature({ ...CFS_BURN, IncidentNo: '' }, 'SACFS')).toBeNull();
    expect(toSaFeature({ ...CFS_BURN, Location: '' }, 'SACFS')).toBeNull();
    expect(toSaFeature({ ...CFS_BURN, Location: 'not,coords' }, 'SACFS')).toBeNull();
    expect(toSaFeature({ ...CFS_BURN, Location: '-34.9' }, 'SACFS')).toBeNull();
    expect(toSaFeature(null, 'SACFS')).toBeNull();
  });
});

describe('fetchSaCfs / fetchSaMfs', () => {
  beforeEach(() => {
    fetchJson.mockReset();
  });

  it('reads the CFS array', async () => {
    fetchJson.mockResolvedValue([CFS_BURN, CFS_CRASH]);
    const snap = await fetchSaCfs();
    expect(snap.count).toBe(2);
    expect(snap.features.map((f) => f.properties.source)).toEqual(['sa_cfs', 'sa_cfs']);
  });

  it('reads the MFS array', async () => {
    fetchJson.mockResolvedValue([MFS_ALARM]);
    const snap = await fetchSaMfs();
    expect(snap.count).toBe(1);
    expect(snap.features[0]!.properties.agency).toBe('SAMFS');
  });

  it('throws on the "File Unavailable" page, which arrives with a 200', async () => {
    // The status code proves nothing on this host. If fetchJson ever
    // manages to parse the body, the array check is what stops an error
    // page being published as an empty state.
    fetchJson.mockResolvedValue(UNAVAILABLE_HTML);
    await expect(fetchSaCfs()).rejects.toThrow(/did not return an array/);
  });

  it('throws when the payload is reshaped into an object', async () => {
    fetchJson.mockResolvedValue({ incidents: [CFS_BURN] });
    await expect(fetchSaMfs()).rejects.toThrow(/did not return an array/);
  });

  it('keeps one record per guid', async () => {
    fetchJson.mockResolvedValue([CFS_BURN, CFS_BURN]);
    expect((await fetchSaCfs()).count).toBe(1);
  });

  it('propagates an upstream failure so the poller backs off', async () => {
    fetchJson.mockRejectedValue(new Error('503'));
    await expect(fetchSaCfs()).rejects.toThrow('503');
  });

  it('drops unplaceable records without failing the batch', async () => {
    fetchJson.mockResolvedValue([CFS_BURN, { ...CFS_CRASH, Location: '' }]);
    expect((await fetchSaCfs()).count).toBe(1);
  });
});
