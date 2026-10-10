/**
 * Named API keys: only a hash is stored, lookup is by prefix, the compare is
 * constant-time, and revoked / expired keys stop working.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createHash } from 'node:crypto';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  getWriterPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));

import {
  createApiKey,
  resolveApiKey,
  lookupPrefix,
  apiKeyRequestOk,
  revokeApiKey,
  _clearApiKeyCache,
  type ApiKeyRow,
} from '../../../src/services/auth/apiKeys.js';

function rowFor(plaintext: string, over: Partial<ApiKeyRow> = {}): ApiKeyRow {
  return {
    id: 'k1',
    user_id: 'u1',
    name: 'test',
    prefix: lookupPrefix(plaintext) ?? '',
    key_hash: createHash('sha256').update(plaintext).digest('hex'),
    scopes: ['read'],
    rate_limit_per_min: 3,
    created_at: '2026-10-11T00:00:00Z',
    last_used_at: null,
    expires_at: null,
    revoked_at: null,
    ...over,
  };
}

beforeEach(() => {
  queryMock.mockReset();
  _clearApiKeyCache();
});

describe('createApiKey', () => {
  it('stores the hash and prefix, never the plaintext', async () => {
    queryMock.mockImplementation((_sql: string, params: unknown[]) =>
      Promise.resolve({ rows: [rowFor('x', { prefix: params[2] as string, key_hash: params[3] as string })] }),
    );
    const { plaintext, row } = await createApiKey({ userId: 'u1', name: 'cli' });
    expect(plaintext).toMatch(/^ak_[A-Za-z0-9]{10}_[A-Za-z0-9_-]{32}$/);
    const params = queryMock.mock.calls[0]![1] as unknown[];
    expect(params).not.toContain(plaintext);
    expect(params[3]).toBe(createHash('sha256').update(plaintext).digest('hex'));
    expect(row.prefix).toBe(lookupPrefix(plaintext));
  });
});

describe('resolveApiKey', () => {
  it('resolves the right key by prefix and rejects a wrong secret with the same prefix', async () => {
    const good = 'ak_abcdefghij_' + 'A'.repeat(32);
    queryMock.mockResolvedValue({ rows: [rowFor(good)] });
    expect((await resolveApiKey(good)).ok).toBe(true);
    const bad = 'ak_abcdefghij_' + 'B'.repeat(32);
    const r = await resolveApiKey(bad);
    expect(r).toEqual({ ok: false, reason: 'bad_key' });
    // Same prefix → served from cache: one query for both.
    expect(queryMock).toHaveBeenCalledTimes(1);
  });

  it('rejects things that are not keys without touching the database', async () => {
    expect(await resolveApiKey('bt1.1.2.3')).toEqual({ ok: false, reason: 'bad_key' });
    expect(await resolveApiKey('ak_short_x')).toEqual({ ok: false, reason: 'bad_key' });
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('refuses revoked and expired keys', async () => {
    const k = 'ak_abcdefghij_' + 'A'.repeat(32);
    queryMock.mockResolvedValueOnce({ rows: [rowFor(k, { revoked_at: '2026-10-10T00:00:00Z' })] });
    expect(await resolveApiKey(k)).toEqual({ ok: false, reason: 'revoked' });
    _clearApiKeyCache();
    queryMock.mockResolvedValueOnce({ rows: [rowFor(k, { expires_at: '2020-01-01T00:00:00Z' })] });
    expect(await resolveApiKey(k)).toEqual({ ok: false, reason: 'expired' });
  });

  it('reports unavailable, not bad, when there is no database', async () => {
    const { getPool } = await import('../../../src/db/pool.js');
    vi.mocked(getPool).mockResolvedValueOnce(null as never);
    expect(await resolveApiKey('ak_abcdefghij_' + 'A'.repeat(32))).toEqual({ ok: false, reason: 'unavailable' });
  });

  it('revoke drops the cached resolution', async () => {
    const k = 'ak_abcdefghij_' + 'A'.repeat(32);
    queryMock.mockResolvedValueOnce({ rows: [rowFor(k)] });
    expect((await resolveApiKey(k)).ok).toBe(true);
    queryMock.mockResolvedValueOnce({ rowCount: 1 });
    expect(await revokeApiKey('abcdefghij')).toBe(true);
    queryMock.mockResolvedValueOnce({ rows: [rowFor(k, { revoked_at: 'now' })] });
    expect(await resolveApiKey(k)).toEqual({ ok: false, reason: 'revoked' });
  });
});

describe('per-key rate limit', () => {
  it('allows the row limit per minute and refuses the next', () => {
    const row = rowFor('ak_abcdefghij_' + 'A'.repeat(32));
    const t0 = 1_800_000_000_000;
    expect(apiKeyRequestOk(row, t0)).toBe(true);
    expect(apiKeyRequestOk(row, t0 + 1)).toBe(true);
    expect(apiKeyRequestOk(row, t0 + 2)).toBe(true);
    expect(apiKeyRequestOk(row, t0 + 3)).toBe(false);
    expect(apiKeyRequestOk(row, t0 + 61_000)).toBe(true);
  });
});
