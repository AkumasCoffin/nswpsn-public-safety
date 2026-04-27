/**
 * /api/rdio/transcripts/search and /api/rdio/calls/:id smoke tests.
 *
 * Covers:
 *   - 503 when RDIO_DATABASE_URL is unset
 *   - 400 input validation
 *   - happy-path query shape (mocked rdio pool)
 *   - call_id branch
 *   - radio-id extraction (source vs sources[])
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const queryMock = vi.fn();
const getRdioPoolMock = vi.fn();
const isRdioConfiguredMock = vi.fn();
const resolveLabelsMock = vi.fn();

vi.mock('../../../src/services/rdio.js', async () => {
  const actual = await vi.importActual<typeof import('../../../src/services/rdio.js')>(
    '../../../src/services/rdio.js',
  );
  return {
    ...actual,
    isRdioConfigured: () => isRdioConfiguredMock() as boolean,
    getRdioPool: () => getRdioPoolMock() as Promise<{ query: typeof queryMock } | null>,
    resolveLabels: (s: number, t: number) => resolveLabelsMock(s, t),
    ensureUnitLabelsLoaded: vi.fn(async () => undefined),
    getUnitLabel: () => null,
  };
});

const API_KEY = 'test-api-key';
const AUTH = { 'X-API-Key': API_KEY } as const;

describe('/api/rdio/transcripts/search', () => {
  beforeEach(() => {
    queryMock.mockReset();
    getRdioPoolMock.mockReset();
    isRdioConfiguredMock.mockReset();
    resolveLabelsMock.mockReset();
    isRdioConfiguredMock.mockReturnValue(true);
    getRdioPoolMock.mockResolvedValue({ query: queryMock });
    resolveLabelsMock.mockResolvedValue({
      systemLabel: 'NSW Police',
      talkgroupLabel: 'PolAir Sydney',
    });
  });

  it('503s when RDIO_DATABASE_URL is unset', async () => {
    isRdioConfiguredMock.mockReturnValue(false);
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/rdio/transcripts/search?q=fire', {
      headers: AUTH,
    });
    expect(res.status).toBe(503);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['error']).toBe('RDIO_DATABASE_URL not configured');
  });

  it('400s without q or call_id', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/rdio/transcripts/search', {
      headers: AUTH,
    });
    expect(res.status).toBe(400);
  });

  it('400s when q has only short fragments', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/rdio/transcripts/search?q=a,b', {
      headers: AUTH,
    });
    expect(res.status).toBe(400);
  });

  it('returns the python response shape on a happy path', async () => {
    queryMock.mockImplementationOnce(async () => ({ rows: [{ n: 2 }] }));
    queryMock.mockImplementationOnce(async () => ({
      rows: [
        {
          id: 100,
          date_time: new Date('2026-04-25T12:34:56Z'),
          system: 1,
          talkgroup: 50,
          transcript: 'fire reported at penrith',
          source: 2010167,
          sources: null,
        },
        {
          id: 101,
          date_time: new Date('2026-04-25T12:35:01Z'),
          system: 1,
          talkgroup: 50,
          transcript: 'fire under control',
          source: null,
          // Radio id falls through to sources[].src
          sources: [{ src: 2099999, position: 0 }],
        },
      ],
    }));
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request(
      '/api/rdio/transcripts/search?q=fire&limit=10',
      { headers: AUTH },
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      total: number;
      results: Array<{
        id: number;
        radio_id: number | null;
        system_label: string | null;
        call_url: string;
      }>;
    };
    expect(body.total).toBe(2);
    expect(body.results.length).toBe(2);
    expect(body.results[0]?.id).toBe(100);
    expect(body.results[0]?.radio_id).toBe(2010167);
    expect(body.results[0]?.system_label).toBe('NSW Police');
    expect(body.results[0]?.call_url).toContain('?call=100');
    expect(body.results[1]?.radio_id).toBe(2099999);
  });

  it('supports the call_id branch', async () => {
    queryMock.mockImplementationOnce(async () => ({ rows: [{ n: 1 }] }));
    queryMock.mockImplementationOnce(async () => ({
      rows: [
        {
          id: 42,
          date_time: new Date('2026-04-25T00:00:00Z'),
          system: 2,
          talkgroup: 7,
          transcript: 'unit 51 responding',
          source: null,
          sources: null,
        },
      ],
    }));
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request(
      '/api/rdio/transcripts/search?call_id=42',
      { headers: AUTH },
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { total: number; call_id: number };
    expect(body.total).toBe(1);
    expect(body.call_id).toBe(42);
  });
});

describe('/api/rdio/calls/:id', () => {
  beforeEach(() => {
    queryMock.mockReset();
    getRdioPoolMock.mockReset();
    isRdioConfiguredMock.mockReset();
    resolveLabelsMock.mockReset();
    isRdioConfiguredMock.mockReturnValue(true);
    getRdioPoolMock.mockResolvedValue({ query: queryMock });
    resolveLabelsMock.mockResolvedValue({
      systemLabel: 'NSW Police',
      talkgroupLabel: 'PolAir Sydney',
    });
  });

  it('400s on non-numeric callId', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/rdio/calls/abc', { headers: AUTH });
    expect(res.status).toBe(400);
  });

  it('404s when row is missing', async () => {
    queryMock.mockResolvedValueOnce({ rows: [] });
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/rdio/calls/99', { headers: AUTH });
    expect(res.status).toBe(404);
  });

  it('returns shaped row on hit', async () => {
    queryMock.mockResolvedValueOnce({
      rows: [
        {
          id: 7,
          date_time: new Date('2026-04-25T01:02:03Z'),
          system: 1,
          talkgroup: 99,
          transcript: 'all units stand down',
          source: 2010167,
          sources: null,
        },
      ],
    });
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/rdio/calls/7', { headers: AUTH });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['id']).toBe(7);
    expect(body['system_label']).toBe('NSW Police');
    expect(body['radio_id']).toBe(2010167);
    expect(body['call_url']).toContain('?call=7');
  });
});
