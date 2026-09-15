/**
 * getPagerIngest(state) — the per-state Pagermon relay target.
 *
 * NSW (and null/'' — pre-097 rows) keeps the legacy behaviour: the DB
 * singleton row wins, falling back to the unsuffixed env pair. Every other
 * state is env-only via its PAGERMON_INGEST_URL_<STATE> pair; a state with no
 * pair configured gets nulls (the relay 503s rather than cross-feeding another
 * state's Pagermon).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

let nextRows: unknown[] = [];
let poolAvailable = true;
const fakePool = {
  query: vi.fn(async () => ({ rows: nextRows })),
};

vi.mock('../../../src/db/pool.js', () => ({
  getPool: async () => (poolAvailable ? fakePool : null),
}));

// LOG_LEVEL/NODE_ENV keep lib/log.ts (imported transitively) from throwing.
vi.mock('../../../src/config.js', () => ({
  config: {
    LOG_LEVEL: 'silent',
    NODE_ENV: 'test',
    PAGERMON_INGEST_URL: 'https://nsw-env.example',
    PAGERMON_INGEST_API_KEY: 'nsw-env-key',
    PAGERMON_INGEST_URL_QLD: 'https://qld.example',
    PAGERMON_INGEST_API_KEY_QLD: 'qld-key',
    NODE_PRESET_DIR: '.',
  },
}));

const { getPagerIngest } = await import('../../../src/services/nodes/globalConfig.js');

beforeEach(() => {
  nextRows = [];
  poolAvailable = true;
  fakePool.query.mockClear();
});

describe('getPagerIngest (per-state routing)', () => {
  it('NSW: the DB singleton row wins over env', async () => {
    nextRows = [{ pagermon_ingest_url: 'https://nsw-db.example', pagermon_ingest_api_key: 'nsw-db-key' }];
    expect(await getPagerIngest('NSW')).toEqual({ url: 'https://nsw-db.example', apiKey: 'nsw-db-key' });
  });

  it('NSW: falls back to the legacy env pair when the DB row is empty', async () => {
    nextRows = [];
    expect(await getPagerIngest('NSW')).toEqual({ url: 'https://nsw-env.example', apiKey: 'nsw-env-key' });
  });

  it('null / empty state behaves as NSW (pre-097 backfill rows)', async () => {
    nextRows = [];
    expect(await getPagerIngest(null)).toEqual({ url: 'https://nsw-env.example', apiKey: 'nsw-env-key' });
    expect(await getPagerIngest('')).toEqual({ url: 'https://nsw-env.example', apiKey: 'nsw-env-key' });
    expect(await getPagerIngest(undefined)).toEqual({ url: 'https://nsw-env.example', apiKey: 'nsw-env-key' });
  });

  it('QLD: env pair only — the NSW DB row is never consulted', async () => {
    nextRows = [{ pagermon_ingest_url: 'https://nsw-db.example', pagermon_ingest_api_key: 'nsw-db-key' }];
    expect(await getPagerIngest('QLD')).toEqual({ url: 'https://qld.example', apiKey: 'qld-key' });
    expect(fakePool.query).not.toHaveBeenCalled();
  });

  it('state matching is case/whitespace tolerant', async () => {
    expect(await getPagerIngest(' qld ')).toEqual({ url: 'https://qld.example', apiKey: 'qld-key' });
  });

  it('a state with no configured pair gets nulls (relay 503s, never cross-feeds)', async () => {
    expect(await getPagerIngest('VIC')).toEqual({ url: null, apiKey: null });
    expect(await getPagerIngest('XX')).toEqual({ url: null, apiKey: null });
  });

  it('NSW: DB down → env fallback, never throws', async () => {
    poolAvailable = false;
    expect(await getPagerIngest('NSW')).toEqual({ url: 'https://nsw-env.example', apiKey: 'nsw-env-key' });
  });
});
