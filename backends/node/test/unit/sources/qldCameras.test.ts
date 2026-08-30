/**
 * QLD Traffic camera source unit tests.
 *
 * The two upstream files share a schema but not an id space, and the
 * flood feed carries attribution the webcam feed doesn't, so most of
 * what matters here is that both map onto the NSW camera property shape
 * without colliding. Covers:
 *   - upstream GeoJSON -> the shared camera property shape
 *   - the two feeds' ids stay distinct (they are separate id spaces)
 *   - records with no image / no coords / no id are dropped
 *   - duplicate ids collapse to one marker
 *   - messy attribution text is whitespace-normalised
 *   - a payload with no features array and an upstream failure both throw
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import {
  fetchQldCameras,
  fetchQldFloodCameras,
  toCameraFeature,
} from '../../../src/sources/qldCameras.js';

const WEBCAM = {
  type: 'Feature',
  geometry: {
    type: 'Point',
    crs: { type: 'name', properties: { name: 'EPSG:7844' } },
    coordinates: [153.0086975, -27.5551796],
  },
  properties: {
    id: 1,
    url: 'https://api.qldtraffic.qld.gov.au/v1/webcams/1',
    description: 'Archerfield - Ipswich Motorway & Granard Rd - North',
    direction: 'NorthEast',
    district: 'Metropolitan',
    locality: 'Archerfield',
    postcode: '4108',
    image_url: 'https://cameras.qldtraffic.qld.gov.au/Metropolitan/Archerfield_Ipswich_Mwy_sth.jpg',
    image_sourced_from: null,
    isCustom: false,
    extra_info: null,
  },
};

const FLOODCAM = {
  type: 'Feature',
  geometry: {
    type: 'Point',
    crs: { type: 'name', properties: { name: 'EPSG:7844' } },
    coordinates: [145.371718, -16.451611],
  },
  properties: {
    id: 190,
    url: 'https://api.qldtraffic.qld.gov.au/v1/floodcams/190',
    description: 'Mossman - Mossman Daintree Road at Foxton bridge - North',
    direction: 'North',
    district: 'Far North',
    locality: 'Mossman',
    postcode: '4873',
    image_url: 'https://cameras.qldtraffic.qld.gov.au/resized/7d90cbc7.jpg',
    image_sourced_from: 'Douglas Shire Council',
    isCustom: true,
    extra_info: 'New images available every 30 minutes.\nImages between 6pm and 6am may be of reduced quality',
  },
};

const collection = (features: unknown[]) => ({ type: 'FeatureCollection', features });

describe('fetchQldCameras', () => {
  beforeEach(() => {
    fetchJson.mockReset();
  });

  it('maps a webcam onto the shared camera property shape', async () => {
    fetchJson.mockResolvedValueOnce(collection([WEBCAM]));
    const snap = await fetchQldCameras();

    expect(snap.count).toBe(1);
    // GeoJSON is [lng, lat] and stays that way.
    expect(snap.features[0]?.geometry.coordinates).toEqual([153.0086975, -27.5551796]);
    expect(snap.features[0]?.properties).toEqual({
      id: 'qld:1',
      title: 'Archerfield - Ipswich Motorway & Granard Rd - North',
      view: 'Archerfield',
      direction: 'NorthEast',
      region: 'Metropolitan',
      imageUrl: WEBCAM.properties.image_url,
      path: '',
      credit: '',
      note: '',
      source: 'qldtraffic_cameras',
    });
  });

  it('carries the flood feed’s attribution and refresh caveat', async () => {
    fetchJson.mockResolvedValueOnce(collection([FLOODCAM]));
    const snap = await fetchQldFloodCameras();

    expect(snap.features[0]?.properties.source).toBe('qldtraffic_floodcameras');
    expect(snap.features[0]?.properties.credit).toBe('Douglas Shire Council');
    // The newline inside extra_info is collapsed, not dropped.
    expect(snap.features[0]?.properties.note).toBe(
      'New images available every 30 minutes. Images between 6pm and 6am may be of reduced quality',
    );
  });

  it('keeps the two feeds’ id spaces apart', async () => {
    // Same numeric id in both files must not become one marker.
    fetchJson.mockResolvedValueOnce(collection([{ ...WEBCAM, properties: { ...WEBCAM.properties, id: 190 } }]));
    const web = await fetchQldCameras();
    fetchJson.mockResolvedValueOnce(collection([FLOODCAM]));
    const flood = await fetchQldFloodCameras();

    expect(web.features[0]?.properties.id).toBe('qld:190');
    expect(flood.features[0]?.properties.id).toBe('qldflood:190');
    expect(web.features[0]?.properties.id).not.toBe(flood.features[0]?.properties.id);
  });

  it('normalises the inconsistent whitespace upstream leaves in attribution', async () => {
    fetchJson.mockResolvedValueOnce(
      collection([
        {
          ...FLOODCAM,
          properties: { ...FLOODCAM.properties, image_sourced_from: 'Department of Transport  and Main Roads ' },
        },
      ]),
    );
    const snap = await fetchQldFloodCameras();
    expect(snap.features[0]?.properties.credit).toBe('Department of Transport and Main Roads');
  });

  it('drops records that cannot be placed or shown', async () => {
    fetchJson.mockResolvedValueOnce(
      collection([
        { ...WEBCAM, properties: { ...WEBCAM.properties, id: 2, image_url: '' } },      // nothing to show
        { ...WEBCAM, geometry: { type: 'Point', coordinates: [] }, properties: { ...WEBCAM.properties, id: 3 } },
        { ...WEBCAM, properties: { ...WEBCAM.properties, id: '' } },                     // no source id
        { ...WEBCAM, geometry: { type: 'LineString', coordinates: [[1, 2]] } },          // not a point
        WEBCAM,
      ]),
    );
    const snap = await fetchQldCameras();
    expect(snap.features.map((f) => f.properties.id)).toEqual(['qld:1']);
  });

  it('collapses a duplicate id instead of stacking two markers', async () => {
    fetchJson.mockResolvedValueOnce(collection([WEBCAM, WEBCAM]));
    const snap = await fetchQldCameras();
    expect(snap.count).toBe(1);
  });

  it('throws when the payload carries no features array', async () => {
    fetchJson.mockResolvedValueOnce({ type: 'FeatureCollection' });
    await expect(fetchQldCameras()).rejects.toThrow(/no features array/);
  });

  it('propagates an upstream failure so the poller backs off', async () => {
    fetchJson.mockRejectedValueOnce(new Error('503'));
    await expect(fetchQldFloodCameras()).rejects.toThrow('503');
  });
});

describe('toCameraFeature', () => {
  it('falls back to a generic title when the feed omits a description', () => {
    const f = toCameraFeature(
      { ...WEBCAM, properties: { ...WEBCAM.properties, description: '' } },
      'qldtraffic_cameras',
      'qld:',
    );
    expect(f?.properties.title).toBe('QLD Traffic Camera');
  });

  it('returns null for a non-object input', () => {
    expect(toCameraFeature(null, 'qldtraffic_cameras', 'qld:')).toBeNull();
    expect(toCameraFeature('nope', 'qldtraffic_cameras', 'qld:')).toBeNull();
  });
});
