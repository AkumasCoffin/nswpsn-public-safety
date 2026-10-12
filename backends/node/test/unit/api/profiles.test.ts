/**
 * Contributor profiles.
 *
 * Pinned guarantees:
 *   - the literal /api/profiles/map-prefs and /api/profiles/watermark routes
 *     are reachable, i.e. the param route above them doesn't swallow them
 *   - an avatar or watermark key has to be the caller's own: claiming one
 *     showed someone else's picture as yours, and clearing it deleted theirs
 *   - the cleared object is only deleted once nothing points at it
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
let calls: Call[] = [];
let resultQueue: Array<{ rows: unknown[]; rowCount?: number }> = [];

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return resultQueue.shift() ?? { rows: [], rowCount: 0 };
  }),
};
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => fakePool),
}));

vi.mock('../../../src/services/auth/supabaseJwt.js', () => ({
  requireSupabaseJwt: async (
    c: { get: (k: string) => unknown; json: (b: unknown, s?: number) => unknown },
    next: () => Promise<void>,
  ) => (c.get('userId') ? next() : c.json({ error: 'unauthorized' }, 401)),
}));
vi.mock('../../../src/services/auth/roles.js', () => ({
  invalidateUserRolesCache: vi.fn(),
}));

// Keys something else already points at (the real query lives in
// services/wire.ts and is covered by its own test).
let inUseKeys: string[] = [];
const deleted: string[] = [];
vi.mock('../../../src/services/wire.js', () => ({
  avatarUrl: (k: string | null) => (k ? `https://r2.example/${k}` : null),
  createImageUploadUrl: async () => ({ uploadURL: 'u', key: 'wire/avatars/new.webp', publicUrl: 'p' }),
  r2Configured: () => true,
  readR2ObjectBytes: async () => Buffer.from([0x89, 0x50]),
  deleteUnreferencedR2Object: async (_pool: unknown, key: string) => { deleted.push(key); },
  r2KeysInUse: async (_pool: unknown, keys: string[]) => keys.filter((k) => inUseKeys.includes(k)),
}));
vi.mock('../../../src/services/userTags.js', () => ({ tagsFor: async () => [] }));
vi.mock('../../../src/services/staffNotify.js', () => ({ notifyStaff: vi.fn() }));
vi.mock('../../../src/services/wireComments.js', () => ({ notify: vi.fn(async () => {}) }));

const { profilesRouter } = await import('../../../src/api/profiles.js');

function makeApp(userId: string | null = 'user-1') {
  const app = new Hono();
  app.use('*', async (c, next) => {
    if (userId) c.set('userId', userId);
    await next();
  });
  app.route('/', profilesRouter);
  return app;
}

const UUID = '0b5f9a3c-1f2d-4e8a-9b6c-7d8e9f0a1b2c';

beforeEach(() => {
  calls = []; resultQueue = []; inUseKeys = []; deleted.length = 0;
});

describe('route order', () => {
  // GET /api/profiles/:userId used to match these first and answer 200 with
  // the profile of a user called "map-prefs" / "watermark" — which read to the
  // client as "nothing saved" and "your watermark isn't a PNG".
  it('serves the caller\'s own map prefs, not a profile named map-prefs', async () => {
    resultQueue = [{ rows: [{ map_prefs: { agencies: ['rfs'] } }], rowCount: 1 }];
    const res = await makeApp().request('/api/profiles/map-prefs');
    expect(res.status).toBe(200);
    const j = (await res.json()) as Record<string, unknown>;
    expect(j['prefs']).toEqual({ agencies: ['rfs'] });
    expect(j['profile']).toBeUndefined();
    expect(calls[0]?.sql).toContain('SELECT map_prefs');
  });

  it('serves the caller\'s own watermark bytes as a PNG', async () => {
    resultQueue = [{ rows: [{ watermark_key: 'wire/watermarks/mine.png' }], rowCount: 1 }];
    const res = await makeApp().request('/api/profiles/watermark');
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toBe('image/png');
  });

  it('still serves a profile by user id', async () => {
    resultQueue = [{ rows: [{ user_id: UUID, display_name: 'Tester' }], rowCount: 1 }];
    const res = await makeApp().request(`/api/profiles/${UUID}`);
    expect(res.status).toBe(200);
    expect(((await res.json()) as { profile: { display_name: string } }).profile.display_name).toBe('Tester');
  });
});

describe('avatar + watermark ownership', () => {
  it('refuses an avatar key that belongs to another account', async () => {
    // Avatar keys are public — they are part of avatar_url on every profile
    // read — so the prefix check alone said nothing about whose key it is.
    inUseKeys = ['wire/avatars/victim.webp'];
    const res = await makeApp().request('/api/profiles', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ avatar_key: 'wire/avatars/victim.webp' }),
    });
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toMatch(/someone else/);
    expect(calls.some((c) => c.sql.includes('INSERT INTO user_profiles'))).toBe(false);
  });

  it('accepts the caller\'s own key and clears it only after the write', async () => {
    resultQueue = [
      { rows: [{ avatar_key: 'wire/avatars/mine.webp' }], rowCount: 1 }, // prev key
      { rows: [{ user_id: 'user-1' }], rowCount: 1 },                    // upsert
    ];
    const res = await makeApp().request('/api/profiles', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clear_avatar: true }),
    });
    expect(res.status).toBe(200);
    // Deleted through the reference check, and only after the row stopped
    // pointing at it — a check run before the UPDATE always sees itself.
    expect(deleted).toEqual(['wire/avatars/mine.webp']);
    expect(calls[1]?.sql).toContain('INSERT INTO user_profiles');
  });

  it('refuses a watermark key that belongs to another account', async () => {
    inUseKeys = ['wire/watermarks/victim.png'];
    const res = await makeApp().request('/api/profiles/watermark', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key: 'wire/watermarks/victim.png' }),
    });
    expect(res.status).toBe(400);
    expect(deleted).toEqual([]);
  });
});
