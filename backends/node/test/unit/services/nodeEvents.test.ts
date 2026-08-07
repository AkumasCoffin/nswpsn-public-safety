/**
 * services/nodeEvents.ts — logical-call grouping unit tests (mocked writer
 * pool; verifies the SQL sequencing + parameters, not a live Postgres).
 *
 * Covers:
 *   - new group: logical_call_id = own row id, hourly_sys logical_calls +1
 *   - existing group: logical_call_id = found id, hourly_sys logical_calls +0
 *   - pager: message_hash = sha256(trimmed message), logical_pages 1/0
 *   - received_at clock sanity clamp (now±48h)
 *   - fire-safe contract: DB failure never throws, ROLLBACK is attempted
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createHash } from 'node:crypto';

const clientQuery = vi.fn();
const clientRelease = vi.fn();
const connectMock = vi.fn(async () => ({ query: clientQuery, release: clientRelease }));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => null),
  getWriterPool: vi.fn(async () => ({ connect: connectMock })),
  closePool: vi.fn(async () => undefined),
}));

import { recordRadioEvent, recordPagerEvent, safeInt } from '../../../src/services/nodeEvents.js';

/** Route mocked queries by SQL substring. */
function armQueries(opts: { foundRadio?: string | null; foundPager?: string | null; insertId?: string }) {
  clientQuery.mockImplementation(async (sql: string) => {
    if (sql.includes('SELECT logical_call_id FROM node_radio_events')) {
      return {
        rows: opts.foundRadio ? [{ logical_call_id: opts.foundRadio }] : [],
      };
    }
    if (sql.includes('SELECT logical_id FROM node_pager_events')) {
      return { rows: opts.foundPager ? [{ logical_id: opts.foundPager }] : [] };
    }
    if (sql.includes('RETURNING id')) {
      return { rows: [{ id: opts.insertId ?? '101' }] };
    }
    return { rows: [] };
  });
}

function callWith(sqlFragment: string): unknown[] | undefined {
  const call = clientQuery.mock.calls.find(
    (args) => typeof args[0] === 'string' && (args[0] as string).includes(sqlFragment),
  );
  return call?.[1] as unknown[] | undefined;
}

const baseRadio = {
  nodeId: 'node-aaaa',
  receivedAt: new Date(),
  system: 4,
  talkgroup: 12345,
  sourceUnit: 777,
  frequency: 420_662_500,
  siteRfss: 1,
  siteId: 12,
  siteNac: 0x2f4,
  siteSource: 'event',
  talkgroupLabel: 'PolAir Sydney',
  systemLabel: 'NSW PSN',
  audioBytes: 90_000,
};

describe('recordRadioEvent grouping', () => {
  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
  });

  it('starts a new group: logical_call_id = own id, logical_calls +1', async () => {
    armQueries({ foundRadio: null, insertId: '101' });
    await recordRadioEvent({ ...baseRadio });

    const upd = callWith('SET logical_call_id');
    expect(upd).toEqual(['101', '101']);

    const sys = callWith('node_radio_hourly_sys');
    // [receivedAt, system, talkgroup, site_rfss, site_id, logicalIncrement]
    expect(sys?.[1]).toBe(4);
    expect(sys?.[2]).toBe(12345);
    expect(sys?.[3]).toBe(1);
    expect(sys?.[4]).toBe(12);
    expect(sys?.[5]).toBe(1); // new group → logical_calls +1

    expect(callWith('BEGIN')).toBeUndefined(); // BEGIN carries no params
    expect(clientQuery.mock.calls.some((a) => a[0] === 'COMMIT')).toBe(true);
    expect(clientRelease).toHaveBeenCalled();
  });

  it('joins an existing group: logical_call_id = found id, logical_calls +0', async () => {
    armQueries({ foundRadio: '55', insertId: '102' });
    await recordRadioEvent({ ...baseRadio });

    expect(callWith('SET logical_call_id')).toEqual(['55', '102']);
    const sys = callWith('node_radio_hourly_sys');
    expect(sys?.[5]).toBe(0); // joined existing group → no logical increment
  });

  it('encodes unknown site as -1 in the hourly sys bucket', async () => {
    armQueries({ foundRadio: null });
    await recordRadioEvent({ ...baseRadio, siteRfss: null, siteId: null });
    const sys = callWith('node_radio_hourly_sys');
    expect(sys?.[3]).toBe(-1);
    expect(sys?.[4]).toBe(-1);
  });

  it('clamps a wildly wrong node clock to now', async () => {
    armQueries({ foundRadio: null });
    await recordRadioEvent({
      ...baseRadio,
      receivedAt: new Date('1999-01-01T00:00:00Z'),
    });
    const ins = callWith('INSERT INTO node_radio_events');
    const receivedAt = new Date(ins?.[1] as string).getTime();
    expect(Math.abs(receivedAt - Date.now())).toBeLessThan(60_000);
  });

  it('never throws when the DB fails, and rolls back', async () => {
    clientQuery.mockImplementation(async (sql: string) => {
      if (sql === 'ROLLBACK') return { rows: [] };
      throw new Error('boom');
    });
    await expect(recordRadioEvent({ ...baseRadio })).resolves.toBeUndefined();
    expect(clientQuery.mock.calls.some((a) => a[0] === 'ROLLBACK')).toBe(true);
    expect(clientRelease).toHaveBeenCalled();
  });
});

describe('recordPagerEvent grouping', () => {
  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
  });

  const basePager = {
    nodeId: 'node-bbbb',
    receivedAt: new Date(),
    capcode: '123456',
    function: 3,
    freqMhz: 148.825,
    message: '  MFS: RESPOND structure fire  ',
  };

  it('hashes the trimmed message and starts a new group', async () => {
    armQueries({ foundPager: null, insertId: '201' });
    await recordPagerEvent({ ...basePager });

    const expectedHash = createHash('sha256')
      .update('MFS: RESPOND structure fire')
      .digest('hex');
    const find = callWith('SELECT logical_id FROM node_pager_events');
    expect(find?.[0]).toBe('123456');
    expect(find?.[1]).toBe(expectedHash);

    expect(callWith('SET logical_id')).toEqual(['201', '201']);
    const hourly = callWith('node_pager_hourly');
    // [receivedAt, nodeId, capcode, logicalIncrement]
    expect(hourly?.[2]).toBe('123456');
    expect(hourly?.[3]).toBe(1);
  });

  it('joins an existing group without a logical_pages increment', async () => {
    armQueries({ foundPager: '77', insertId: '202' });
    await recordPagerEvent({ ...basePager });
    expect(callWith('SET logical_id')).toEqual(['77', '202']);
    expect(callWith('node_pager_hourly')?.[3]).toBe(0);
  });
});

describe('safeInt', () => {
  it('parses ints and rejects garbage', () => {
    expect(safeInt('42')).toBe(42);
    expect(safeInt(7.9)).toBe(7);
    expect(safeInt('')).toBeNull();
    expect(safeInt('abc')).toBeNull();
    expect(safeInt(undefined)).toBeNull();
    expect(safeInt(null)).toBeNull();
    expect(safeInt(Infinity)).toBeNull();
  });
});
