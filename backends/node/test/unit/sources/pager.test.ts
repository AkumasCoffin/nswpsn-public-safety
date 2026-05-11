/**
 * Unit tests for the pager source fetcher and message parsers.
 *
 * `fetchPager` short-circuits when PAGERMON_URL is unset (the default
 * in the test env). For the "happy path" test we override the config
 * lookup by mocking ../../../src/config.js.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchJsonMock = vi.fn();
// Keep all the other config fields the rest of the import graph reads
// (LOG_LEVEL is used by lib/log.ts when pager.ts imports it). We only
// override PAGERMON_URL / PAGERMON_API_KEY per test.
const configMock: Record<string, unknown> = {
  LOG_LEVEL: 'warn',
  NODE_ENV: 'test',
  STATE_DIR: './test/.tmp-state',
  LIVE_PERSIST_INTERVAL_MS: 30_000,
  ARCHIVE_FLUSH_INTERVAL_MS: 30_000,
  NSWPSN_API_KEY: 'test-key',
  PORT: 3001,
  WAZE_INGEST_MAX_AGE_SECS: 2400,
};

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

vi.mock('../../../src/config.js', () => ({
  config: configMock,
  modeLabel: () => 'dev',
}));

describe('pager.parsePagerCoords', () => {
  it('extracts strict [lon,lat] format', async () => {
    const { parsePagerCoords } = await import('../../../src/sources/pager.js');
    expect(parsePagerCoords('foo [151.2,-33.8] bar')).toEqual([-33.8, 151.2]);
  });

  it('returns nulls when no coords present', async () => {
    const { parsePagerCoords } = await import('../../../src/sources/pager.js');
    expect(parsePagerCoords('no coords here')).toEqual([null, null]);
  });
});

describe('pager.parsePagerIncidentId', () => {
  it('extracts xx-xxxxxx format', async () => {
    const { parsePagerIncidentId } = await import('../../../src/sources/pager.js');
    expect(parsePagerIncidentId('Incident 25-139605 reported')).toBe('25-139605');
  });

  it('falls back to xxxx-xxxx', async () => {
    const { parsePagerIncidentId } = await import('../../../src/sources/pager.js');
    expect(parsePagerIncidentId('code 0053-6653 ack')).toBe('0053-6653');
  });

  it('returns null when no id', async () => {
    const { parsePagerIncidentId } = await import('../../../src/sources/pager.js');
    expect(parsePagerIncidentId('plain text')).toBeNull();
  });
});

describe('pager.fetchPager', () => {
  beforeEach(() => {
    fetchJsonMock.mockReset();
    delete configMock['PAGERMON_URL'];
    delete configMock['PAGERMON_API_KEY'];
  });

  it('returns empty when PAGERMON_URL is unset', async () => {
    const { fetchPager } = await import('../../../src/sources/pager.js');
    const out = await fetchPager();
    expect(out).toEqual({ messages: [], count: 0 });
    expect(fetchJsonMock).not.toHaveBeenCalled();
  });

  it('groups messages by incident id and inherits canonical coords', async () => {
    configMock['PAGERMON_URL'] = 'http://pager.example/api/messages';
    fetchJsonMock.mockResolvedValueOnce({
      messages: [
        {
          id: 1,
          address: 'C001',
          alias: 'STN1',
          agency: 'Fire',
          source: 'pocsag',
          timestamp: 1700000000,
          message: 'Incident 25-139605 [151.2,-33.8] respond',
        },
        {
          id: 2,
          address: 'C002',
          timestamp: 1700000010,
          // Same incident — no coords here, should inherit from msg 1.
          message: 'Incident 25-139605 ack',
        },
        {
          id: 3,
          address: 'C003',
          timestamp: 1700000020,
          // No id, no coords — dropped.
          message: 'just a status update',
        },
      ],
    });

    const { fetchPager } = await import('../../../src/sources/pager.js');
    const out = await fetchPager();
    expect(out.count).toBe(2);
    for (const m of out.messages) {
      expect(m.lat).toBe(-33.8);
      expect(m.lon).toBe(151.2);
      expect(m.incident_id).toBe('25-139605');
    }
    expect(fetchJsonMock).toHaveBeenCalledWith(
      'http://pager.example/api/messages?limit=100',
      expect.any(Object),
    );
  });

  it('includes apikey query param when configured', async () => {
    configMock['PAGERMON_URL'] = 'http://pager.example/api/messages';
    configMock['PAGERMON_API_KEY'] = 'sek=ret';
    fetchJsonMock.mockResolvedValueOnce({ messages: [] });
    const { fetchPager } = await import('../../../src/sources/pager.js');
    await fetchPager();
    expect(fetchJsonMock).toHaveBeenCalledWith(
      'http://pager.example/api/messages?apikey=sek%3Dret&limit=100',
      expect.any(Object),
    );
  });
});
