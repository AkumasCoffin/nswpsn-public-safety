/**
 * Aviation source unit tests. The mocked http layer covers nonce
 * scraping (HTML), the airport list ajax, and the per-airport modal
 * ajax. Each test resets the nonce + detail caches so they don't bleed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchTextMock = vi.fn();
const fetchJsonMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: fetchTextMock,
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

const NONCE_PAGE = `<!doctype html><html><head><script>
  var config = {"nonce":"abc123def4","other":"x"};
</script></head><body></body></html>`;

const AIRPORT_LIST_RESPONSE = {
  airport_list: [
    {
      id: 'syd',
      title: 'Sydney Airport',
      name: 'Sydney',
      state: 'NSW',
      state_full: 'New South Wales',
      link: 'https://example.com/syd',
      thumbnail: 'https://cdn.example.com/syd.jpg',
      lat: '-33.94',
      long: '151.18',
    },
    {
      // Missing coords — must be dropped.
      id: 'nocoord',
      title: 'No coords',
    },
    {
      // Bad coords — must be dropped.
      id: 'bad',
      title: 'Bad coords',
      lat: 'NaN',
      long: '0',
    },
  ],
};

const MODAL_RESPONSE = {
  modal: {
    north_image: 'https://cdn.example.com/syd-n.jpg',
    north_thumbnail: 'https://cdn.example.com/syd-n-thumb.jpg',
    north_angle: '0',
    east_image: 'https://cdn.example.com/syd-e.jpg',
    east_thumbnail: 'https://cdn.example.com/syd-e-thumb.jpg',
    east_angle: '90',
    // No south/west: must be skipped, not crashed
  },
};

describe('aviation.getAirservicesNonce', () => {
  beforeEach(async () => {
    fetchTextMock.mockReset();
    fetchJsonMock.mockReset();
    const mod = await import('../../../src/sources/aviation.js');
    mod._resetNonceCacheForTests();
    mod._resetDetailCacheForTests();
  });

  it('extracts the nonce from the first matching pattern', async () => {
    fetchTextMock.mockResolvedValueOnce(NONCE_PAGE);
    const { getAirservicesNonce } = await import(
      '../../../src/sources/aviation.js'
    );
    expect(await getAirservicesNonce()).toBe('abc123def4');
  });

  it('returns the fallback nonce when the page cannot be parsed', async () => {
    fetchTextMock.mockResolvedValueOnce('<html>no nonce here</html>');
    const { getAirservicesNonce } = await import(
      '../../../src/sources/aviation.js'
    );
    expect(await getAirservicesNonce()).toBe('da9010b391');
  });

  it('caches the nonce across calls', async () => {
    fetchTextMock.mockResolvedValueOnce(NONCE_PAGE);
    const { getAirservicesNonce } = await import(
      '../../../src/sources/aviation.js'
    );
    await getAirservicesNonce();
    await getAirservicesNonce();
    expect(fetchTextMock).toHaveBeenCalledTimes(1);
  });
});

describe('aviation.fetchAviationCameras', () => {
  beforeEach(async () => {
    fetchTextMock.mockReset();
    fetchJsonMock.mockReset();
    const mod = await import('../../../src/sources/aviation.js');
    mod._resetNonceCacheForTests();
    mod._resetDetailCacheForTests();
  });

  it('shapes airport list as a GeoJSON FeatureCollection', async () => {
    fetchTextMock.mockResolvedValueOnce(NONCE_PAGE);
    fetchJsonMock.mockResolvedValueOnce(AIRPORT_LIST_RESPONSE);
    const { fetchAviationCameras } = await import(
      '../../../src/sources/aviation.js'
    );
    const out = await fetchAviationCameras();
    expect(out.type).toBe('FeatureCollection');
    expect(out.count).toBe(1);
    const f = out.features[0];
    if (!f) throw new Error('no feature');
    expect(f.geometry.coordinates).toEqual([151.18, -33.94]);
    expect(f.properties.id).toBe('syd');
    expect(f.properties.imageUrl).toBe('https://cdn.example.com/syd.jpg');
    expect(f.properties.source).toBe('airservices_australia');
  });
});

describe('aviation.fetchAviationCameraDetail', () => {
  beforeEach(async () => {
    fetchTextMock.mockReset();
    fetchJsonMock.mockReset();
    const mod = await import('../../../src/sources/aviation.js');
    mod._resetNonceCacheForTests();
    mod._resetDetailCacheForTests();
  });

  it('returns only directions that have an imageUrl', async () => {
    fetchTextMock.mockResolvedValueOnce(NONCE_PAGE);
    fetchJsonMock.mockResolvedValueOnce(MODAL_RESPONSE);
    const { fetchAviationCameraDetail } = await import(
      '../../../src/sources/aviation.js'
    );
    const out = await fetchAviationCameraDetail('sydney');
    expect(out.airport).toBe('sydney');
    expect(out.count).toBe(2);
    // Direction is the labelled form (matches python's
    // direction_labels[direction] at external_api_proxy.py:7718).
    expect(out.cameras.map((c) => c.direction)).toEqual(['North', 'East']);
    expect(out.cameras[1]?.angle).toBe('90');
  });

  it('caches detail responses per airport', async () => {
    fetchTextMock.mockResolvedValueOnce(NONCE_PAGE);
    fetchJsonMock.mockResolvedValueOnce(MODAL_RESPONSE);
    const { fetchAviationCameraDetail } = await import(
      '../../../src/sources/aviation.js'
    );
    await fetchAviationCameraDetail('sydney');
    await fetchAviationCameraDetail('sydney');
    // Second call hits the cache: no extra fetchJson invocation.
    expect(fetchJsonMock).toHaveBeenCalledTimes(1);
  });
});
