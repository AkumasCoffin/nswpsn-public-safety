/**
 * Deep P25 site metadata "site view parity" (migration 047) — focused unit
 * tests, mock-pool pattern (no live Postgres):
 *
 *   - upsertSiteSnapshots (services/nodeEvents.ts): the idempotent UPSERT —
 *     natural-key columns, null systemId/rfss coalesced to -1, nested facts
 *     serialised as JSONB, null-site rows skipped, rowCount returned,
 *     fire-safe (DB failure never throws).
 *   - GET /api/node-data/site meta shape: the node_site_snapshots row is
 *     mapped into the `meta` object the site drill-down renders, and meta is
 *     null when no snapshot exists.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

// One pool mock serving both paths: getWriterPool().connect() for the store,
// getPool().query for the read route.
const clientQuery = vi.fn();
const clientRelease = vi.fn();
const connectMock = vi.fn(async () => ({ query: clientQuery, release: clientRelease }));
const queryMock = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => ({ query: queryMock })),
  getWriterPool: vi.fn(async () => ({ connect: connectMock })),
  closePool: vi.fn(async () => undefined),
}));

// requireRole stays real; canManageNodes stubbed true so the read route runs.
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canManageNodes: vi.fn(() => Promise.resolve(true)) };
});

// Keep talkgroup-label resolution from touching the global config / DB.
vi.mock('../../../src/services/nodes/globalConfig.js', () => ({
  getGlobalConfig: vi.fn(async () => ({ sdrtrunkConfig: { aliases: [] } })),
}));

import { upsertSiteSnapshots, type SiteSnapshotInput } from '../../../src/services/nodeEvents.js';

function callWith(sqlFragment: string, nth = 0): unknown[] | undefined {
  const calls = clientQuery.mock.calls.filter(
    (args) => typeof args[0] === 'string' && (args[0] as string).includes(sqlFragment),
  );
  return calls[nth]?.[1] as unknown[] | undefined;
}

const baseSite: SiteSnapshotInput = {
  systemId: 1186,
  rfss: 1,
  siteId: 12,
  guid: 'rc-guid-abc',
  systemName: 'NSWPSN',
  wacn: 0xbee00,
  nac: 0x2f4,
  lra: 5,
  channelName: 'Sydney North',
  controlFrequencyMhz: 856.2375,
  controlLcn: '1-23',
  affiliatedRadioCount: 44,
  observationCount: 9,
  firstSeenMs: 1_700_000_000_000,
  lastSeenMs: 1_700_000_900_000,
  status: { dataService: true, voiceService: true, tdma: true, dataAccess: 'F' },
  channels: [{ type: 'primary_control', lcn: '1-23', frequencyMhz: 856.2375, tags: ['CURRENT_CONTROL'] }],
  neighbors: [{ systemId: 1186, rfss: 1, siteId: 13, controlFrequencyMhz: 856.5 }],
  bands: [{ bandId: 0, baseMhz: 851.0, spacingKhz: 12.5, txOffsetMhz: -45, bandwidthKhz: 12.5 }],
  quality: { decodeHealthPct: 99.2, signalDbfs: -58.4, validFrames: 1000, invalidFrames: 8 },
};

describe('upsertSiteSnapshots', () => {
  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
  });

  it('upserts a site on its natural key with nested facts as JSONB', async () => {
    clientQuery.mockResolvedValue({ rowCount: 1, rows: [] });
    const written = await upsertSiteSnapshots('node-aaaa', [{ ...baseSite }]);
    expect(written).toBe(1);

    const ins = callWith('INSERT INTO node_site_snapshots');
    expect(ins).toBeDefined();
    // [nodeId, systemId, rfss, siteId, guid, systemName, wacn, nac, lra,
    //  channelName, controlFreqMhz, controlLcn, affiliated, observation,
    //  firstSeenMs, lastSeenMs, status, channels, neighbors, bands, quality]
    expect(ins?.[0]).toBe('node-aaaa');
    expect(ins?.[1]).toBe(1186);
    expect(ins?.[2]).toBe(1);
    expect(ins?.[3]).toBe(12);
    expect(ins?.[5]).toBe('NSWPSN');
    expect(ins?.[10]).toBe(856.2375);
    expect(ins?.[11]).toBe('1-23');
    expect(ins?.[12]).toBe(44);
    // Nested facts are JSON strings (bound to ::jsonb). Params are 0-based:
    // $17 status = ins[16], channels[17], neighbors[18], bands[19], quality[20].
    expect(typeof ins?.[16]).toBe('string'); // status
    expect(JSON.parse(ins?.[17] as string)).toHaveLength(1); // channels
    expect(JSON.parse(ins?.[18] as string)[0]?.siteId).toBe(13); // neighbors
    expect(JSON.parse(ins?.[19] as string)[0]?.bandId).toBe(0); // bands
    expect(JSON.parse(ins?.[20] as string)?.decodeHealthPct).toBe(99.2); // quality
    expect(clientRelease).toHaveBeenCalled();
  });

  it('coalesces unknown systemId/rfss to -1 (natural key can\'t be null)', async () => {
    clientQuery.mockResolvedValue({ rowCount: 1, rows: [] });
    await upsertSiteSnapshots('node-aaaa', [{ ...baseSite, systemId: null, rfss: null }]);
    const ins = callWith('INSERT INTO node_site_snapshots');
    expect(ins?.[1]).toBe(-1);
    expect(ins?.[2]).toBe(-1);
    expect(ins?.[3]).toBe(12);
  });

  it('skips a site with no resolvable site id', async () => {
    clientQuery.mockResolvedValue({ rowCount: 1, rows: [] });
    const written = await upsertSiteSnapshots('node-aaaa', [{ ...baseSite, siteId: null }]);
    expect(written).toBe(0);
    expect(callWith('INSERT INTO node_site_snapshots')).toBeUndefined();
  });

  it('empty/missing channels default to [] JSON, quality/status null passthrough', async () => {
    clientQuery.mockResolvedValue({ rowCount: 1, rows: [] });
    await upsertSiteSnapshots('node-aaaa', [
      { ...baseSite, channels: [], neighbors: [], bands: [], status: null, quality: null },
    ]);
    const ins = callWith('INSERT INTO node_site_snapshots');
    expect(ins?.[16]).toBeNull(); // status
    expect(ins?.[17]).toBe('[]'); // channels
    expect(ins?.[20]).toBeNull(); // quality
  });

  it('never throws when the DB fails and returns 0', async () => {
    clientQuery.mockImplementation(async () => {
      throw new Error('boom');
    });
    const written = await upsertSiteSnapshots('node-aaaa', [{ ...baseSite }]);
    expect(written).toBe(0);
    expect(clientRelease).toHaveBeenCalled();
  });
});

async function setupApp() {
  const { nodeDataRouter } = await import('../../../src/api/node-data.js');
  const app = new Hono();
  app.use('*', async (c, next) => {
    c.set('userId', 'u1');
    await next();
  });
  app.route('/', nodeDataRouter);
  return app;
}

/** Route the /site handler's queries by SQL fragment. `metaRow` = the
 *  node_site_snapshots row (or undefined for the empty state). */
function armSiteRoute(metaRow: Record<string, unknown> | undefined) {
  queryMock.mockImplementation((sql: string) => {
    if (sql.includes('FROM node_site_snapshots')) {
      return { rows: metaRow ? [metaRow] : [] };
    }
    // scopedRadioDetail totals.
    if (sql.includes('AS talkgroups')) {
      return { rows: [{ calls: 3, logical: 2, enc: 1, talkgroups: 2, radios: 2, sites: 1 }] };
    }
    // everything else (topTalkgroups / topRadios / series / nodes) → empty.
    return { rows: [] };
  });
}

describe('GET /api/node-data/site meta shape', () => {
  beforeEach(() => {
    queryMock.mockReset();
  });

  it('maps the latest node_site_snapshots row into the meta object', async () => {
    armSiteRoute({
      guid: 'rc-guid-abc',
      system_name: 'NSWPSN',
      wacn: 0xbee00,
      nac: 0x2f4,
      lra: 5,
      channel_name: 'Sydney North',
      control_frequency_mhz: 856.2375,
      control_lcn: '1-23',
      affiliated_radio_count: 44,
      observation_count: 9,
      site_first_seen_ms: '1700000000000',
      site_last_seen_ms: '1700000900000',
      status: { tdma: true },
      channels: [{ type: 'primary_control', lcn: '1-23' }],
      neighbors: [{ siteId: 13 }],
      bands: [{ bandId: 0 }],
      quality: { decodeHealthPct: 99.2 },
      received_at: new Date(1_700_000_900_000),
    });

    const app = await setupApp();
    const res = await app.request('/api/node-data/site?system=1186&rfss=1&site=12');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    const meta = body['meta'] as Record<string, unknown>;
    expect(meta).toBeTruthy();
    expect(meta['systemName']).toBe('NSWPSN');
    expect(meta['controlFrequencyMhz']).toBe(856.2375);
    expect(meta['controlLcn']).toBe('1-23');
    expect(meta['affiliatedRadioCount']).toBe(44);
    // BIGINT ms columns arrive as strings → coerced to numbers.
    expect(meta['firstSeenMs']).toBe(1_700_000_000_000);
    expect(meta['lastSeenMs']).toBe(1_700_000_900_000);
    expect(meta['channels']).toHaveLength(1);
    expect(meta['neighbors']).toHaveLength(1);
    expect(meta['bands']).toHaveLength(1);
    expect((meta['quality'] as Record<string, unknown>)['decodeHealthPct']).toBe(99.2);
    expect(typeof meta['updatedAt']).toBe('string');
  });

  it('returns meta: null when no snapshot exists yet', async () => {
    armSiteRoute(undefined);
    const app = await setupApp();
    const res = await app.request('/api/node-data/site?system=1186&rfss=1&site=12');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['meta']).toBeNull();
  });
});
