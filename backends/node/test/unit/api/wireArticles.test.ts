/**
 * Article create/edit/feed behaviour in api/wire.ts.
 *
 * Pinned guarantees:
 *   - ?mine=1&deleted=1 without a signed-in caller cannot list soft-deleted
 *     posts (the feed falls back to the PUBLIC branch, where the deleted
 *     filter would otherwise still apply)
 *   - publishing through PUT follows the same rule as creating: the approval
 *     toggle and wire:manager count, and a submission for review alerts staff
 *   - an edit carries a video's processing state and derived fields across
 *     instead of re-queueing the clip for another encode
 *   - storage keys from the body have to be the author's own
 *
 * The fake pool answers by SQL pattern rather than a fixed queue: these
 * handlers issue a different number of queries depending on what the body
 * carries, and a queue would pin an ordering the test doesn't care about.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
let calls: Call[] = [];
let txCalls: Call[] = [];

// Per-test state the fake pool answers from.
let prevArticle: Record<string, unknown> | null = null;
let oldMedia: Array<Record<string, unknown>> = [];
let approvalSetting: string | null = null; // app_settings.wire_approval_required
let feedRows: Array<Record<string, unknown>> = [];

function answer(sql: string): { rows: unknown[]; rowCount: number } {
  const rows = (r: unknown[]) => ({ rows: r, rowCount: r.length });
  if (/FROM wire_media WHERE parent_type/.test(sql)) return rows(oldMedia);
  if (/FROM app_settings/.test(sql)) {
    return rows(approvalSetting === null ? [] : [{ value: approvalSetting }]);
  }
  if (/FROM articles a WHERE/.test(sql)) return rows(feedRows);
  if (/FROM articles WHERE id = \$1/.test(sql)) return rows(prevArticle ? [prevArticle] : []);
  return rows([]);
}

const fakeClient = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    txCalls.push({ sql, ...(params ? { params } : {}) });
    return answer(sql);
  }),
  release: vi.fn(),
};
const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return answer(sql);
  }),
  connect: vi.fn(async () => fakeClient),
};
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => fakePool),
}));

let moderatorMock = false;
let adminMock = false;
vi.mock('../../../src/services/auth/roles.js', () => ({
  canFeedMedia: vi.fn(async () => true),
  canModerateWire: vi.fn(async () => moderatorMock),
  canManageUsers: vi.fn(async () => adminMock),
  isOwner: vi.fn(async () => false),
  requireRole: (fn: (uid: string) => Promise<boolean>) =>
    async (c: { get: (k: string) => unknown; json: (b: unknown, s?: number) => unknown }, next: () => Promise<void>) => {
      const uid = c.get('userId');
      if (typeof uid !== 'string' || !(await fn(uid))) return c.json({ error: 'forbidden' }, 403);
      await next();
    },
}));

vi.mock('../../../src/services/wireSettings.js', () => ({
  wirePublic: vi.fn(async () => true),
  autoTagsEnabled: vi.fn(async () => false),
  setWireSetting: vi.fn(async () => {}),
}));
vi.mock('../../../src/services/callsigns.js', () => ({
  rememberCallsigns: vi.fn(async () => {}),
  normaliseCallsign: (v: unknown) => (typeof v === 'string' ? v.toUpperCase() : ''),
}));
vi.mock('../../../src/services/userTags.js', () => ({
  awardPostingTags: vi.fn(async () => {}),
  tagMap: vi.fn(async () => new Map()),
}));
vi.mock('../../../src/services/wireComments.js', () => ({
  avatarMap: vi.fn(async () => new Map()),
  displayNameMap: vi.fn(async () => new Map()),
}));

const notified: Array<Record<string, unknown>> = [];
vi.mock('../../../src/services/staffNotify.js', () => ({
  notifyStaff: (_pool: unknown, input: Record<string, unknown>) => { notified.push(input); },
}));

let ffmpegOn = true;
vi.mock('../../../src/services/videoTranscode.js', () => ({
  ffmpegAvailable: () => ffmpegOn,
}));

// Real services/wire.ts apart from the three external-storage calls: its pure
// helpers (shapeMedia, slugify, r2PublicUrl…) are what the handlers expect.
let inUseKeys: string[] = [];
const deletedKeys: string[] = [];
vi.mock('../../../src/services/wire.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/wire.js')>();
  return {
    ...actual,
    deleteCfImage: vi.fn(async () => {}),
    deleteUnreferencedR2Object: async (_pool: unknown, key: string) => { deletedKeys.push(key); },
    r2KeysInUse: async (_pool: unknown, keys: string[]) => keys.filter((k) => inUseKeys.includes(k)),
  };
});

const { wireRouter } = await import('../../../src/api/wire.js');

function makeApp(userId: string | null = 'author-1') {
  const app = new Hono();
  app.use('*', async (c, next) => {
    if (userId) { c.set('userId', userId); c.set('userName', 'Tester'); }
    await next();
  });
  app.route('/', wireRouter);
  return app;
}

const PUBLISH = {
  title: 'Something happened',
  status: 'published',
  rights_affirmed: true,
  region: 'NSW — Penrith — Emu Plains',
};

function put(body: Record<string, unknown>, userId = 'author-1') {
  return makeApp(userId).request('/api/wire/articles/a1', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

const articleUpdate = () => txCalls.find((c) => /UPDATE articles SET/.test(c.sql));
const mediaInserts = () => txCalls.filter((c) => /INSERT INTO wire_media/.test(c.sql));

beforeEach(() => {
  calls = []; txCalls = []; notified.length = 0; deletedKeys.length = 0;
  prevArticle = { author_id: 'author-1', status: 'draft', published_at: null, taken_down_at: null };
  oldMedia = []; approvalSetting = null; feedRows = [];
  moderatorMock = false; adminMock = false; ffmpegOn = true; inUseKeys = [];
});

describe('feed: ?mine=1&deleted=1', () => {
  it('cannot list soft-deleted posts without a signed-in caller', async () => {
    const res = await makeApp(null).request('/api/wire/articles?mine=1&deleted=1');
    expect(res.status).toBe(200);
    const sql = calls.find((c) => /FROM articles a WHERE/.test(c.sql))!.sql;
    // Public branch, so deleted posts must be excluded, not selected.
    expect(sql).toContain('a.deleted_at IS NULL');
    expect(sql).not.toContain('a.deleted_at IS NOT NULL');
    expect(sql).toContain("status = 'published'");
  });

  it('still lists the signed-in author\'s own deletions', async () => {
    await makeApp('author-1').request('/api/wire/articles?mine=1&deleted=1');
    const sql = calls.find((c) => /FROM articles a WHERE/.test(c.sql))!.sql;
    expect(sql).toContain('a.deleted_at IS NOT NULL');
    expect(sql).toContain('author_id = $1');
  });
});

describe('PUT publish status', () => {
  it('publishes a contributor\'s draft outright when approval is switched off', async () => {
    approvalSetting = 'false';
    const res = await put(PUBLISH);
    expect(res.status).toBe(200);
    expect(articleUpdate()?.params?.[4]).toBe('published');
    expect(notified).toHaveLength(0);
  });

  it('publishes a wire:manager\'s own draft without review', async () => {
    moderatorMock = true;
    approvalSetting = 'true';
    await put(PUBLISH);
    expect(articleUpdate()?.params?.[4]).toBe('published');
  });

  it('sends a contributor\'s draft for review AND tells staff', async () => {
    approvalSetting = 'true';
    const res = await put(PUBLISH);
    expect(res.status).toBe(200);
    expect(articleUpdate()?.params?.[4]).toBe('pending');
    // Without this the submission waited for someone to open the staff queue.
    expect(notified).toHaveLength(1);
    expect(notified[0]?.['kind']).toBe('wire_approval');
    expect(notified[0]?.['ref']).toBe('article:a1');
  });

  it('does not re-alert staff when the post was already pending', async () => {
    approvalSetting = 'true';
    prevArticle = { author_id: 'author-1', status: 'pending', published_at: null, taken_down_at: null };
    await put(PUBLISH);
    expect(articleUpdate()?.params?.[4]).toBe('pending');
    expect(notified).toHaveLength(0);
  });
});

describe('PUT media carry-over', () => {
  const VIDEO = 'wire/videos/clip.mp4';

  it('keeps a transcoded video done, with its poster and dimensions', async () => {
    // The composer sends back the key and little else; everything the server
    // derived used to be dropped, so every typo fix re-encoded the clip (and
    // burned the watermark in again) and briefly lost its poster.
    oldMedia = [{
      id: 'm1', parent_type: 'article', parent_id: 'a1', kind: 'video',
      cf_image_id: null, r2_key: VIDEO, poster_cf_image_id: null,
      poster_r2_key: 'wire/img/poster.jpg', duration_seconds: 12, is_cover: false,
      unit: null, sort_order: 0, width: 1920, height: 1080, bytes: 10,
      hash: null, process_state: 'done',
    }];
    await put({ ...PUBLISH, media: [{ kind: 'video', r2_key: VIDEO }] });
    const ins = mediaInserts()[0]!;
    expect(ins.params?.[4]).toBe(VIDEO);
    expect(ins.params?.[6]).toBe('wire/img/poster.jpg'); // poster_r2_key
    expect(ins.params?.[7]).toBe(12);                    // duration_seconds
    expect(ins.params?.[11]).toBe(1920);                 // width
    expect(ins.params?.[12]).toBe(1080);                 // height
    expect(ins.params?.[15]).toBe('done');               // process_state
    // Nothing was dropped, so nothing is deleted from storage.
    expect(deletedKeys).toEqual([]);
  });

  it('re-queues a clip that was mid-transcode, rather than leaving it claimed', async () => {
    // The worker holding it will find its row gone and discard its output, so
    // the clip has to be picked up again.
    oldMedia = [{
      id: 'm1', parent_type: 'article', parent_id: 'a1', kind: 'video',
      cf_image_id: null, r2_key: VIDEO, poster_cf_image_id: null, poster_r2_key: null,
      duration_seconds: null, is_cover: false, unit: null, sort_order: 0,
      width: null, height: null, bytes: null, hash: null, process_state: 'processing',
    }];
    await put({ ...PUBLISH, media: [{ kind: 'video', r2_key: VIDEO }] });
    expect(mediaInserts()[0]!.params?.[15]).toBe('pending');
  });

  it('still queues a newly attached clip', async () => {
    await put({ ...PUBLISH, media: [{ kind: 'video', r2_key: VIDEO }] });
    expect(mediaInserts()[0]!.params?.[15]).toBe('pending');
  });
});

describe('PUT media ownership', () => {
  it('refuses a key that belongs to someone else', async () => {
    inUseKeys = ['wire/img/victim.webp'];
    const res = await put({ ...PUBLISH, media: [{ kind: 'image', r2_key: 'wire/img/victim.webp' }] });
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toMatch(/someone else/);
    expect(txCalls).toHaveLength(0);
  });

  it('refuses an avatar or watermark key dressed up as post media', async () => {
    const res = await put({ ...PUBLISH, media: [{ kind: 'image', r2_key: 'wire/avatars/victim.webp' }] });
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toMatch(/not uploaded/);
  });

  it('lets an admin keep the author\'s existing keys', async () => {
    adminMock = true;
    inUseKeys = ['wire/img/theirs.webp'];
    oldMedia = [{
      id: 'm1', parent_type: 'article', parent_id: 'a1', kind: 'image',
      cf_image_id: null, r2_key: 'wire/img/theirs.webp', poster_cf_image_id: null,
      poster_r2_key: null, duration_seconds: null, is_cover: true, unit: null,
      sort_order: 0, width: null, height: null, bytes: null, hash: null, process_state: null,
    }];
    const res = await put(
      { ...PUBLISH, media: [{ kind: 'image', r2_key: 'wire/img/theirs.webp' }] },
      'staff-1',
    );
    expect(res.status).toBe(200);
    expect(deletedKeys).toEqual([]);
  });
});
