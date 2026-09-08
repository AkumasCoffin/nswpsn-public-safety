/**
 * /api/rdio/calls/:id smoke tests (mocked rdio pool).
 *
 * The transcripts/search suite that lived here was removed with the route
 * (2026-09) — see src/api/transcripts.ts header for why.
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
