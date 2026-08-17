/**
 * Profile tags: vocabulary, bulk fetch and the automatic awards.
 *
 * The rules worth pinning are the two that are easy to get subtly wrong:
 * og_contributor must stop being earnable the moment the Wire goes public,
 * and first_contributor must survive being attempted by two people at once
 * (the DB holds a partial unique index; the code has to treat the violation
 * as a normal answer rather than an error).
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

let queries: Array<{ sql: string; params?: unknown[] }> = [];
let nextRows: unknown[] = [];
let throwCode: string | null = null;

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    queries.push({ sql, ...(params ? { params } : {}) });
    if (throwCode) {
      const e = new Error('constraint') as Error & { code?: string };
      e.code = throwCode;
      throw e;
    }
    return { rows: nextRows, rowCount: nextRows.length };
  }),
};

let wirePublicValue = false;
// LOG_LEVEL/NODE_ENV are here because lib/log.ts is imported transitively and
// pino throws on an undefined level — a stub that only covers the field under
// test takes the whole suite down at import time.
vi.mock('../../../src/config.js', () => ({
  config: {
    get WIRE_PUBLIC() { return wirePublicValue ? 'true' : 'false'; },
    LOG_LEVEL: 'silent',
    NODE_ENV: 'test',
  },
}));

const { USER_TAGS, isKnownTag, shapeTag, tagMap, grantTag, awardPostingTags } =
  await import('../../../src/services/userTags.js');

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const pool = fakePool as any;

beforeEach(() => {
  queries = []; nextRows = []; throwCode = null; wirePublicValue = false;
  fakePool.query.mockClear();
});

describe('vocabulary', () => {
  it('knows only the defined tags', () => {
    expect(isKnownTag('og_contributor')).toBe(true);
    expect(isKnownTag('first_contributor')).toBe(true);
    expect(isKnownTag('admin')).toBe(false);
    expect(isKnownTag('')).toBe(false);
    expect(isKnownTag(undefined)).toBe(false);
  });

  it('uses Font Awesome icons, not emoji', () => {
    for (const t of USER_TAGS) {
      expect(t.icon).toMatch(/^fa-/);
      // A stray emoji in the icon field would render as a broken glyph next to
      // every username that holds the badge.
      expect(t.icon).not.toMatch(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u);
    }
  });

  it('shapes a tag for the client, and refuses unknown keys', () => {
    expect(shapeTag('og_contributor')).toMatchObject({ key: 'og_contributor', label: 'OG Contributor' });
    expect(shapeTag('nope')).toBeNull();
  });
});

describe('tagMap', () => {
  it('groups by user and drops keys no longer in the vocabulary', async () => {
    nextRows = [
      { user_id: 'u1', tag: 'og_contributor' },
      { user_id: 'u1', tag: 'retired_badge' },   // vocabulary shrank
      { user_id: 'u2', tag: 'verified' },
    ];
    const m = await tagMap(pool, ['u1', 'u2']);
    expect(m.get('u1')).toHaveLength(1);
    expect(m.get('u1')?.[0]).toMatchObject({ key: 'og_contributor' });
    expect(m.get('u2')?.[0]).toMatchObject({ key: 'verified' });
  });

  it('does not query at all for an empty id list', async () => {
    const m = await tagMap(pool, [null, undefined]);
    expect(m.size).toBe(0);
    expect(fakePool.query).not.toHaveBeenCalled();
  });

  it('returns empty rather than throwing when the query fails', async () => {
    throwCode = '42P01';
    await expect(tagMap(pool, ['u1'])).resolves.toBeInstanceOf(Map);
  });
});

describe('grantTag', () => {
  it('rejects tags outside the vocabulary without touching the database', async () => {
    const r = await grantTag(pool, 'u1', 'made_up', 'admin');
    expect(r).toEqual({ ok: false, reason: 'unknown tag' });
    expect(fakePool.query).not.toHaveBeenCalled();
  });

  it('reports the single-holder conflict in words, not as a failure', async () => {
    throwCode = '23505';
    const r = await grantTag(pool, 'u2', 'first_contributor', 'admin');
    expect(r.ok).toBe(false);
    // Must name the badge and say what to do — this surfaces straight to staff.
    expect((r as { reason: string }).reason).toMatch(/First Contributor/);
    expect((r as { reason: string }).reason).toMatch(/already held/i);
  });
});

describe('awardPostingTags', () => {
  it('grants OG while the Wire is private', async () => {
    wirePublicValue = false;
    await awardPostingTags(pool, 'u1');
    const granted = queries.filter((q) => q.sql.includes('INSERT INTO user_tags'))
      .map((q) => (q.params as string[])[1]);
    expect(granted).toContain('og_contributor');
    expect(granted).toContain('first_contributor');
  });

  it('does NOT grant OG once the Wire is public', async () => {
    wirePublicValue = true;
    await awardPostingTags(pool, 'u1');
    const granted = queries.filter((q) => q.sql.includes('INSERT INTO user_tags'))
      .map((q) => (q.params as string[])[1]);
    // The badge exists to mark the pre-launch window. After launch it must be
    // unearnable, or it means nothing.
    expect(granted).not.toContain('og_contributor');
    expect(granted).toContain('first_contributor');
  });

  it('records the system as the grantor, not a user', async () => {
    await awardPostingTags(pool, 'u1');
    const first = queries.find((q) => q.sql.includes('INSERT INTO user_tags'));
    expect((first?.params as string[])[2]).toBe('system');
  });

  it('is a no-op without a user id', async () => {
    await awardPostingTags(pool, '');
    expect(fakePool.query).not.toHaveBeenCalled();
  });

  it('never throws, even when the database is unhappy', async () => {
    throwCode = '23505';
    await expect(awardPostingTags(pool, 'u1')).resolves.toBeUndefined();
  });
});
