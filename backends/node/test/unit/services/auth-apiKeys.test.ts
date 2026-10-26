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
  reissueApiKey,
  deleteApiKey,
  forgetApiKeyPrefix,
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
    requests_total: 0,
    reissued_at: null,
    deleted_at: null,
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
    // The row comes back with the refusal: the secret matched, so the gate
    // counts the attempt against the key (apiKeyUsage.ts).
    expect(await resolveApiKey(k)).toMatchObject({ ok: false, reason: 'revoked', row: { id: 'k1' } });
    _clearApiKeyCache();
    queryMock.mockResolvedValueOnce({ rows: [rowFor(k, { expires_at: '2020-01-01T00:00:00Z' })] });
    expect(await resolveApiKey(k)).toMatchObject({ ok: false, reason: 'expired', row: { id: 'k1' } });
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
    expect(await resolveApiKey(k)).toMatchObject({ ok: false, reason: 'revoked' });
  });

  it('re-issue: the old plaintext stops resolving once its prefix is forgotten, the new one resolves', async () => {
    const old = 'ak_abcdefghij_' + 'A'.repeat(32);
    queryMock.mockResolvedValueOnce({ rows: [rowFor(old)] });
    expect((await resolveApiKey(old)).ok).toBe(true);

    let minted: { prefix: string; hash: string } | null = null;
    queryMock.mockImplementation((sql: string, params: unknown[]) => {
      if (sql.startsWith('SELECT prefix')) return Promise.resolve({ rows: [{ prefix: 'abcdefghij' }] });
      if (sql.includes('SET prefix')) {
        minted = { prefix: params[1] as string, hash: params[2] as string };
        return Promise.resolve({ rows: [{ ...rowFor(old), prefix: minted.prefix, key_hash: minted.hash, reissued_at: 'now' }] });
      }
      return Promise.resolve({ rows: [] });
    });
    const out = await reissueApiKey('k1', { query: queryMock } as never);
    expect(out).not.toBeNull();
    expect(out!.oldPrefix).toBe('abcdefghij');
    expect(out!.row.id).toBe('k1');
    expect(lookupPrefix(out!.plaintext)).toBe(minted!.prefix);
    expect(minted!.hash).toBe(createHash('sha256').update(out!.plaintext).digest('hex'));

    // Until the caller forgets the old prefix the cache still answers for it —
    // that ordering is the route's job (forget only after COMMIT).
    expect((await resolveApiKey(old)).ok).toBe(true);
    forgetApiKeyPrefix(out!.oldPrefix);
    queryMock.mockReset();
    queryMock.mockResolvedValue({ rows: [] }); // the old prefix no longer exists in the table
    expect(await resolveApiKey(old)).toEqual({ ok: false, reason: 'bad_key' });

    queryMock.mockResolvedValueOnce({ rows: [{ ...rowFor(old), prefix: minted!.prefix, key_hash: minted!.hash }] });
    expect((await resolveApiKey(out!.plaintext)).ok).toBe(true);
  });

  it('re-issue refuses a key that is not live: the UPDATE matches nothing', async () => {
    queryMock.mockImplementation((sql: string) =>
      Promise.resolve({ rows: sql.startsWith('SELECT prefix') ? [{ prefix: 'abcdefghij' }] : [] }),
    );
    expect(await reissueApiKey('k1', { query: queryMock } as never)).toBeNull();
  });

  it('delete revokes and hides in one owner-scoped UPDATE, and drops the cache', async () => {
    const k = 'ak_abcdefghij_' + 'A'.repeat(32);
    queryMock.mockResolvedValueOnce({ rows: [rowFor(k)] });
    expect((await resolveApiKey(k)).ok).toBe(true);
    queryMock.mockResolvedValueOnce({ rowCount: 1 });
    expect(await deleteApiKey('abcdefghij', 'u1')).toBe(true);
    const [sql, params] = queryMock.mock.calls[1]!;
    expect(sql).toContain('revoked_at = COALESCE(revoked_at, now()), deleted_at = now()');
    expect(params).toEqual(['abcdefghij', 'u1']);
    queryMock.mockResolvedValueOnce({ rows: [rowFor(k, { revoked_at: 'now', deleted_at: 'now' })] });
    expect(await resolveApiKey(k)).toMatchObject({ ok: false, reason: 'revoked' });
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

/**
 * The resolution cache is keyed by a caller-supplied prefix and the gate
 * resolves before any rate limit runs, so an unauthenticated client sending a
 * new random prefix per request used to add a permanent entry each time —
 * every request a 403, the growth visible nowhere but the heap, until the
 * process was OOM-killed.
 */
describe('resolve cache bounds', () => {
  it('stays bounded under a flood of distinct unknown prefixes', async () => {
    const { _apiKeyCacheSize } = await import('../../../src/services/auth/apiKeys.js');
    queryMock.mockResolvedValue({ rows: [] });
    for (let i = 0; i < 5_000; i += 1) {
      const prefix = String(i).padStart(10, '0');
      expect(await resolveApiKey(`ak_${prefix}_` + 'A'.repeat(32))).toEqual({
        ok: false,
        reason: 'bad_key',
      });
    }
    expect(_apiKeyCacheSize()).toBeLessThanOrEqual(1_000);
  });

  it('expires an entry on a later write rather than holding it for good', async () => {
    const { _apiKeyCacheSize } = await import('../../../src/services/auth/apiKeys.js');
    const t0 = 1_800_000_000_000;
    queryMock.mockResolvedValue({ rows: [] });
    await resolveApiKey('ak_aaaaaaaaaa_' + 'A'.repeat(32), t0);
    expect(_apiKeyCacheSize()).toBe(1);
    // A minute later the first entry is past its TTL, so the second write
    // sweeps it instead of leaving it to be pushed out by the cap.
    await resolveApiKey('ak_bbbbbbbbbb_' + 'A'.repeat(32), t0 + 61_000);
    expect(_apiKeyCacheSize()).toBe(1);
  });

  it('still answers a repeat miss from cache rather than re-querying', async () => {
    const k = 'ak_cccccccccc_' + 'A'.repeat(32);
    queryMock.mockResolvedValue({ rows: [] });
    await resolveApiKey(k);
    await resolveApiKey(k);
    expect(queryMock).toHaveBeenCalledTimes(1);
  });
});
