/**
 * user_incidents source tests — the poller that snapshots the local
 * `incidents` table into archive_misc so user incidents appear on the
 * logs page like any other feed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

let getPoolReturn: unknown = null;
const queryMock = vi.fn(async () => ({ rows: [] }));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => getPoolReturn),
}));

const { userIncidentArchiveRow } = await import('../../../src/sources/userIncidents.js');
const registerUserIncidents = (await import('../../../src/sources/userIncidents.js')).default;
const { getSource } = await import('../../../src/services/sourceRegistry.js');

const NOW = 1_800_000_000;

function baseRow() {
  return {
    id: 'inc-1',
    title: 'Grass fire near oval',
    description: 'Small grass fire',
    lat: -33.9,
    lng: 151.1,
    location: 'Coraki',
    type: ['Grass Fire', 'Bush Fire'],
    status: 'Going',
    size: '1 ha',
    responding_agencies: ['RFS'],
    units: ['P1', 'CAT7'],
    created_at: new Date((NOW - 3600) * 1000),
    updated_at: new Date((NOW - 60) * 1000),
    expires_at: new Date((NOW + 3600) * 1000),
  };
}

describe('userIncidentArchiveRow', () => {
  it('maps an incidents-table row to an archive_misc snapshot', () => {
    const row = userIncidentArchiveRow(baseRow(), NOW);
    expect(row.source).toBe('user_incident');
    expect(row.source_id).toBe('inc-1');
    expect(row.fetched_at).toBe(NOW);
    expect(row.source_timestamp_unix).toBe(NOW - 60);
    expect(row.lat).toBe(-33.9);
    expect(row.category).toBe('Grass Fire');
    const data = row.data as Record<string, unknown>;
    expect(data['title']).toBe('Grass fire near oval');
    expect(data['location_text']).toBe('Coraki');
    expect(data['status']).toBe('Going');
    expect(data['type']).toEqual(['Grass Fire', 'Bush Fire']);
    expect(data['units']).toEqual(['P1', 'CAT7']);
    expect(data['is_active']).toBe(true);
  });

  it('parses JSONB-as-TEXT columns and flags expired incidents inactive', () => {
    const row = userIncidentArchiveRow(
      {
        ...baseRow(),
        type: '["Flood"]',
        responding_agencies: '["SES"]',
        units: 'not-json',
        expires_at: new Date((NOW - 10) * 1000),
      },
      NOW,
    );
    expect(row.category).toBe('Flood');
    const data = row.data as Record<string, unknown>;
    expect(data['responding_agencies']).toEqual(['SES']);
    expect(data['units']).toEqual([]);
    expect(data['is_active']).toBe(false);
  });

  it('forceInactive overrides a live expiry (delete-time snapshot)', () => {
    const row = userIncidentArchiveRow(baseRow(), NOW, { forceInactive: true });
    expect((row.data as Record<string, unknown>)['is_active']).toBe(false);
  });

  it('never-expiring incidents are active; missing timestamps stay null', () => {
    const row = userIncidentArchiveRow(
      { ...baseRow(), expires_at: null, updated_at: null, created_at: null },
      NOW,
    );
    expect((row.data as Record<string, unknown>)['is_active']).toBe(true);
    expect(row.source_timestamp_unix).toBeNull();
  });
});

describe('registerUserIncidents', () => {
  beforeEach(() => {
    queryMock.mockClear();
    getPoolReturn = { query: queryMock };
  });

  it('registers a misc-family source that excludes deleted rows and stubs', async () => {
    registerUserIncidents();
    const src = getSource('user_incidents');
    expect(src).toBeDefined();
    expect(src!.family).toBe('misc');
    expect(src!.archiveSource).toBe('user_incident');

    await src!.fetch();
    expect(queryMock).toHaveBeenCalledOnce();
    const sql = (queryMock.mock.calls[0] as unknown[])[0] as string;
    expect(sql).toContain('deleted_at IS NULL');
    expect(sql).toContain('is_rfs_stub IS NOT TRUE');
  });

  it('archiveItems fans each row out and tolerates non-array data', () => {
    registerUserIncidents();
    const src = getSource('user_incidents')!;
    const rows = src.archiveItems!([baseRow(), baseRow()], NOW, 'user_incident');
    expect(rows).toHaveLength(2);
    expect(rows[0]!.source).toBe('user_incident');
    expect(src.archiveItems!({ nope: true }, NOW, 'user_incident')).toEqual([]);
  });

  it('fetch returns [] when no DB is configured', async () => {
    getPoolReturn = null;
    registerUserIncidents();
    const src = getSource('user_incidents')!;
    await expect(src.fetch()).resolves.toEqual([]);
  });
});
