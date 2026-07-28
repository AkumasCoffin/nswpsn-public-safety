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

describe('pager.parsePagerIncidentId (FRNSW)', () => {
  it('extracts an FRNSW FRINC turnout id', async () => {
    const { parsePagerIncidentId } = await import('../../../src/sources/pager.js');
    expect(parsePagerIncidentId('FRINC TYPE: AFA TURNOUT: 405 INC: 146685-28072026')).toBe(
      '146685-28072026',
    );
  });
});

describe('pager.parsePagerType', () => {
  it('parses type + call class from a standard RFS detail line', async () => {
    const { parsePagerType } = await import('../../../src/sources/pager.js');
    expect(
      parsePagerType(
        'CVCOUCR7 - 26-122002 - Bush Fire - FIRECALL - 526 MIDDLE CREEK RD,KANGAROO CREEK,CLARENCE VALLEY (NSW),2460 - [152.91203,-29.926772]',
      ),
    ).toEqual({ type: 'Bush Fire', callClass: 'FIRECALL' });
  });

  it('parses type when the call class is absent', async () => {
    const { parsePagerType } = await import('../../../src/sources/pager.js');
    expect(
      parsePagerType('ISDO - 26-121910 - MVA - 6 LARKINS LANE,YALLAH,WOLLONGONG CITY (NSW),2530 - [150.77867,-34.53929]'),
    ).toEqual({ type: 'MVA', callClass: '' });
  });

  it('skips a VRA service tag to find the real type', async () => {
    const { parsePagerType } = await import('../../../src/sources/pager.js');
    expect(
      parsePagerType('VRCENTR414 - 26-121994 - VRA - ROAD CRASH RESCUE - AML SOUTHBOUND M1'),
    ).toEqual({ type: 'ROAD CRASH RESCUE', callClass: '' });
  });

  it('parses an FRNSW FRINC header type with a TURNOUT class', async () => {
    const { parsePagerType } = await import('../../../src/sources/pager.js');
    expect(parsePagerType('FRINC TYPE: HOUSE FIRE TURNOUT: 405 INC: 146685-28072026')).toEqual({
      type: 'HOUSE FIRE',
      callClass: 'TURNOUT',
    });
  });

  it('returns no type for a Stop Message (no bogus free-text type)', async () => {
    const { parsePagerType } = await import('../../../src/sources/pager.js');
    expect(
      parsePagerType('26-121998 CVGRACI1 Stop Message // SWALLOW RD SOUTH GRAFTON - NO NEED TO ATTEND THANKS.'),
    ).toEqual({ type: '', callClass: '' });
    expect(
      parsePagerType('HKHORNS - 26-119173 - STOP -STAND DOWN - NNTA THANKS FOR YOUR TURNOUT.'),
    ).toEqual({ type: '', callClass: '' });
  });
});

describe('pager.parsePagerStop', () => {
  it('detects the Stop Message variants', async () => {
    const { parsePagerStop } = await import('../../../src/sources/pager.js');
    expect(parsePagerStop('26-121998 CVGRACI1 Stop Message // no need to attend')).toBe(true);
    expect(parsePagerStop('HKHORNS - 26-119173 - STOP -STAND DOWN - NNTA THANKS')).toBe(true);
    expect(parsePagerStop('STOP MESSAGE - STAND DOWN NNTA THANKS')).toBe(true);
    expect(parsePagerStop('CVCOUCR7 - 26-122002 - Bush Fire - FIRECALL - x')).toBe(false);
  });
});

describe('pager.inferPagerAgency', () => {
  it('infers FRNSW / NSWRFS / VRA / SES from the body', async () => {
    const { inferPagerAgency } = await import('../../../src/sources/pager.js');
    expect(inferPagerAgency('FRINC TYPE: AFA TURNOUT: 405 INC: 146685-28072026', '')).toBe('FRNSW');
    expect(inferPagerAgency('CVCOUCR7 - 26-122002 - Bush Fire - FIRECALL - x', '')).toBe('NSWRFS');
    expect(inferPagerAgency('VRCENTR414 - 26-121994 - VRA - ROAD CRASH RESCUE - x', '')).toBe('VRA');
    expect(inferPagerAgency('SEZWCB GLR at ASCOT ROAD, BOWRAL', '')).toBe('SES');
  });

  it('respects an explicit upstream tag over body guessing', async () => {
    const { inferPagerAgency } = await import('../../../src/sources/pager.js');
    expect(inferPagerAgency('26-122002 Bush Fire', 'Rural Fire Service')).toBe('NSWRFS');
    expect(inferPagerAgency('plain text', 'Ambulance')).toBe('NSWAS');
  });
});

describe('pager.parsePagerAddress', () => {
  it('returns the address after type/call class', async () => {
    const { parsePagerAddress } = await import('../../../src/sources/pager.js');
    expect(
      parsePagerAddress('CVCOUCR7 - 26-122002 - Bush Fire - FIRECALL - 526 MIDDLE CREEK RD,KANGAROO CREEK - [152.9,-29.9]'),
    ).toBe('526 MIDDLE CREEK RD,KANGAROO CREEK');
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
