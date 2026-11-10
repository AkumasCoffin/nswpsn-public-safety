/**
 * HttpError must not carry an upstream credential in its message.
 *
 * A source's last error is published on /api/status and the FIRMS snapshot
 * copies it into source_status[].error, which Cloudflare then caches — and two
 * sources put their key in the request URL (FIRMS's MAP_KEY in the path,
 * Pagermon's ?apikey=). So the message gets a redacted URL and the real one
 * stays on a field that no serialisation picks up.
 */
import { describe, it, expect } from 'vitest';
import { HttpError, redactUrl } from '../../../src/sources/shared/http.js';

const FIRMS_KEY = '0123456789abcdef0123456789abcdef';
const FIRMS_URL =
  `https://firms.modaps.eosdis.nasa.gov/api/area/csv/${FIRMS_KEY}` +
  '/VIIRS_SNPP_NRT/112,-44,154,-10/1';
const PAGER_URL = 'https://pager.example.org/api/messages?apikey=s3cr3t-pager-key&limit=100';

describe('redactUrl', () => {
  it('masks a credential carried in the path', () => {
    const safe = redactUrl(FIRMS_URL);
    expect(safe).not.toContain(FIRMS_KEY);
    expect(safe).toContain('firms.modaps.eosdis.nasa.gov');
    expect(safe).toContain('/api/area/csv/');
  });

  it('drops the query string whole', () => {
    const safe = redactUrl(PAGER_URL);
    expect(safe).not.toContain('s3cr3t-pager-key');
    expect(safe).not.toContain('apikey');
    expect(safe).toBe('https://pager.example.org/api/messages?***');
  });

  it('leaves route-shaped path segments readable, long ones included', () => {
    expect(redactUrl('https://api.open-meteo.com/v1/forecast?latitude=-33')).toBe(
      'https://api.open-meteo.com/v1/forecast?***',
    );
    // Words with separators and no digits read as a route, not a credential.
    expect(redactUrl('https://x.supabase.co/rest/v1/rpc/get_outage_areas_fast')).toBe(
      'https://x.supabase.co/rest/v1/rpc/get_outage_areas_fast',
    );
  });

  it('redacts a bare path the same way, since it has no host to leak', () => {
    expect(redactUrl('/rpc/get_outage_areas_fast')).toBe('/rpc/get_outage_areas_fast');
    expect(redactUrl(`/api/area/csv/${FIRMS_KEY}/x?k=v`)).toBe('/api/area/csv/***/x?***');
  });

  it('says nothing at all about a URL it cannot parse', () => {
    expect(redactUrl('not a url at all')).toBe('(url)');
  });
});

describe('HttpError', () => {
  it('redacts the url in the message and in the public field', () => {
    const err = new HttpError(`HTTP 503 for ${redactUrl(FIRMS_URL)}`, 503, FIRMS_URL);
    expect(err.message).not.toContain(FIRMS_KEY);
    expect(err.url).not.toContain(FIRMS_KEY);
    expect(err.status).toBe(503);
  });

  it('keeps the full url for server logs, but off every serialisation', () => {
    const err = new HttpError('HTTP 503', 503, PAGER_URL);
    expect(err.fullUrl).toBe(PAGER_URL);
    expect(JSON.stringify(err)).not.toContain('s3cr3t-pager-key');
    expect(Object.keys(err)).not.toContain('fullUrl');
    expect({ ...err }).not.toHaveProperty('fullUrl');
  });
});
