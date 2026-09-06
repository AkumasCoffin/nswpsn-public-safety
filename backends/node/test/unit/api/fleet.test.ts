/**
 * Fleet API (The Wire's vehicle directory).
 *
 * Pinned guarantees:
 *   - radio IDs must be exactly 7 digits; lists are deduped and capped
 *   - a contributor's post lands 'pending' while approval is on; a
 *     moderator's publishes instantly
 *   - pending vehicles are invisible to the public but visible to the author
 *   - only the author or an admin can edit/delete; a replaced or deleted
 *     photo's R2 object is cleaned up
 *   - views dedupe through wire_views (parent_type 'fleet') and never count
 *     the author's own
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
let calls: Call[] = [];
let resultQueue: Array<{ rows: unknown[]; rowCount?: number }> = [];
let nextResult: { rows: unknown[]; rowCount?: number } = { rows: [], rowCount: 0 };

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, params });
    const q = resultQueue.shift();
    return q ?? nextResult;
  }),
};
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => fakePool),
}));

let canFeedMock = true;
let moderatorMock = false;
let adminMock = false;
vi.mock('../../../src/services/auth/roles.js', () => ({
  canFeedMedia: vi.fn(async () => canFeedMock),
  canModerateWire: vi.fn(async () => moderatorMock),
  canManageUsers: vi.fn(async () => adminMock),
  requireRole: (fn: (uid: string) => Promise<boolean>) =>
    async (c: { get: (k: string) => unknown; json: (b: unknown, s?: number) => unknown }, next: () => Promise<void>) => {
      const uid = c.get('userId');
      if (typeof uid !== 'string' || !(await fn(uid))) return c.json({ error: 'forbidden' }, 403);
      await next();
    },
}));

let wirePublicMock = true;
vi.mock('../../../src/services/wireSettings.js', () => ({
  wirePublic: vi.fn(async () => wirePublicMock),
}));

const deleteR2 = vi.fn(async () => {});
vi.mock('../../../src/services/wire.js', () => ({
  r2PublicUrl: (k: string) => `https://r2.example/${k}`,
  deleteR2Object: (k: string) => deleteR2(k),
  viewerHash: () => 'vh-1',
  normaliseLicense: (v: unknown) => (v === 'display' || v === 'public' ? v : 'credit'),
  licenseLabel: (v: string) => (v === 'public' ? 'Public domain' : v === 'display' ? 'All rights reserved' : 'Credit required'),
}));

const { fleetRouter } = await import('../../../src/api/fleet.js');

function makeApp(userId: string | null = 'user-1') {
  const app = new Hono();
  app.use('*', async (c, next) => {
    if (userId) { c.set('userId', userId); c.set('userName', 'Tester'); }
    await next();
  });
  app.route('/', fleetRouter);
  return app;
}

const row = (over: Record<string, unknown> = {}) => ({
  id: 'v1', author_id: 'user-1', author_name: 'Tester',
  callsign: 'P 251', state: 'NSW', lga: 'Penrith', suburb: null,
  agency: 'Fire and Rescue NSW (FRNSW)', agency_category: 'fire',
  station: '251 Cardiff', cad_code: null, aerial_id: null,
  vehicle_type: 'Pumper Class 2', registration: 'ABC123',
  make: 'Scania', model: 'P320', cab_chassis: 'Varley Group',
  production_year: 2020, crew_capacity: 4,
  radio_ids: { cab: ['1234567'], mobile: [] }, specs: { water_tank_l: 2000, cafs: true },
  license: 'credit', credit: null, rights_affirmed: true, watermark: false,
  image_key: 'wire/img1.webp', views: 3, status: 'published', review_note: null,
  created_at: new Date('2026-09-01T00:00:00Z'), updated_at: new Date('2026-09-01T00:00:00Z'),
  ...over,
});

const goodBody = {
  callsign: 'P 251', state: 'NSW', lga: 'Penrith', agency: 'Fire and Rescue NSW (FRNSW)',
  agency_category: 'fire', rights_affirmed: true,
  radio_ids: { cab: ['1234567'], mobile: ['7654321'] },
  specs: { water_tank_l: 2000, cafs: true },
};

beforeEach(() => {
  calls = []; resultQueue = []; nextResult = { rows: [], rowCount: 0 };
  canFeedMock = true; moderatorMock = false; adminMock = false; wirePublicMock = true;
  deleteR2.mockClear();
});

describe('fleet list', () => {
  it('shapes rows and applies state + q filters', async () => {
    resultQueue = [{ rows: [row()], rowCount: 1 }];
    const res = await makeApp().request('/api/wire/fleet?state=nsw&q=251');
    expect(res.status).toBe(200);
    const j = await res.json();
    expect(j.vehicles).toHaveLength(1);
    expect(j.vehicles[0].callsign).toBe('P 251');
    expect(j.vehicles[0].image_url).toBe('https://r2.example/wire/img1.webp');
    expect(j.vehicles[0].image_key).toBeUndefined(); // list never leaks keys
    const sql = calls[0]!.sql;
    expect(sql).toContain('state = $');
    expect(sql).toContain('callsign ILIKE');
    expect(calls[0]!.params).toContain('NSW'); // lowercased input normalised
  });

  it('answers visible:false when the Wire is gated for this caller', async () => {
    wirePublicMock = false; canFeedMock = false; moderatorMock = false;
    const res = await makeApp(null).request('/api/wire/fleet');
    expect(await res.json()).toEqual({ vehicles: [], visible: false });
  });
});

describe('fleet create', () => {
  it('rejects a missing callsign and a malformed radio ID', async () => {
    const app = makeApp();
    let res = await app.request('/api/wire/fleet', {
      method: 'POST', body: JSON.stringify({ ...goodBody, callsign: '' }),
      headers: { 'Content-Type': 'application/json' },
    });
    expect(res.status).toBe(400);
    res = await app.request('/api/wire/fleet', {
      method: 'POST',
      body: JSON.stringify({ ...goodBody, radio_ids: { cab: ['12345'], mobile: [] } }),
      headers: { 'Content-Type': 'application/json' },
    });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toMatch(/7 digits/);
    // ...and publishing without the rights affirmation
    res = await app.request('/api/wire/fleet', {
      method: 'POST',
      body: JSON.stringify({ ...goodBody, rights_affirmed: false }),
      headers: { 'Content-Type': 'application/json' },
    });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toMatch(/rights/);
  });

  it("lands pending for a contributor while approval is on, published for a moderator", async () => {
    // approvalRequired SELECT, the byline display-name lookup, then the INSERT
    resultQueue = [{ rows: [], rowCount: 0 }, { rows: [], rowCount: 0 }, { rows: [{ id: 'new1' }], rowCount: 1 }];
    let res = await makeApp().request('/api/wire/fleet', {
      method: 'POST', body: JSON.stringify(goodBody), headers: { 'Content-Type': 'application/json' },
    });
    expect(res.status).toBe(201);
    expect((await res.json()).status).toBe('pending');

    moderatorMock = true;
    resultQueue = [{ rows: [], rowCount: 0 }, { rows: [], rowCount: 0 }, { rows: [{ id: 'new2' }], rowCount: 1 }];
    res = await makeApp().request('/api/wire/fleet', {
      method: 'POST', body: JSON.stringify(goodBody), headers: { 'Content-Type': 'application/json' },
    });
    expect((await res.json()).status).toBe('published');
  });

  it('dedupes radio IDs and keeps the two lists separate', async () => {
    resultQueue = [{ rows: [], rowCount: 0 }, { rows: [], rowCount: 0 }, { rows: [{ id: 'new1' }], rowCount: 1 }];
    await makeApp().request('/api/wire/fleet', {
      method: 'POST',
      body: JSON.stringify({ ...goodBody, radio_ids: { cab: ['1234567', '1234567'], mobile: ['7654321'] } }),
      headers: { 'Content-Type': 'application/json' },
    });
    const insert = calls.find((c) => c.sql.includes('INSERT INTO fleet_vehicles'))!;
    const radio = JSON.parse(insert.params![20] as string);
    expect(radio).toEqual({ cab: ['1234567'], mobile: ['7654321'] });
  });
});

describe('fleet detail', () => {
  it('hides a pending vehicle from the public but shows it to its author', async () => {
    nextResult = { rows: [row({ status: 'pending' })], rowCount: 1 };
    let res = await makeApp('someone-else').request('/api/wire/fleet/v1');
    expect(res.status).toBe(404);
    res = await makeApp('user-1').request('/api/wire/fleet/v1');
    expect(res.status).toBe(200);
    expect((await res.json()).vehicle.image_key).toBe('wire/img1.webp'); // author gets keys
  });
});

describe('fleet edit/delete', () => {
  it('forbids a third party and lets the author update', async () => {
    nextResult = { rows: [row()], rowCount: 1 };
    let res = await makeApp('someone-else').request('/api/wire/fleet/v1', {
      method: 'PUT', body: JSON.stringify(goodBody), headers: { 'Content-Type': 'application/json' },
    });
    expect(res.status).toBe(403);

    resultQueue = [
      { rows: [{ author_id: 'user-1', image_key: 'wire/old.webp', status: 'published' }], rowCount: 1 },
      { rows: [], rowCount: 1 }, // UPDATE
    ];
    res = await makeApp('user-1').request('/api/wire/fleet/v1', {
      method: 'PUT',
      body: JSON.stringify({ ...goodBody, image_key: 'wire/new.webp' }),
      headers: { 'Content-Type': 'application/json' },
    });
    expect(res.status).toBe(200);
    expect(deleteR2).toHaveBeenCalledWith('wire/old.webp'); // replaced photo cleaned up
  });

  it('deletes the row and its R2 object for the author', async () => {
    resultQueue = [
      { rows: [{ author_id: 'user-1', image_key: 'wire/img1.webp' }], rowCount: 1 },
      { rows: [], rowCount: 1 },
    ];
    const res = await makeApp('user-1').request('/api/wire/fleet/v1', { method: 'DELETE' });
    expect(res.status).toBe(200);
    expect(deleteR2).toHaveBeenCalledWith('wire/img1.webp');
  });
});

describe('fleet moderation', () => {
  it('remove requires the moderator role', async () => {
    let res = await makeApp().request('/api/wire/fleet/v1/remove', { method: 'POST' });
    expect(res.status).toBe(403);
    moderatorMock = true;
    nextResult = { rows: [{ id: 'v1' }], rowCount: 1 };
    res = await makeApp().request('/api/wire/fleet/v1/remove', { method: 'POST' });
    expect(res.status).toBe(200);
  });

  it('approve publishes a pending vehicle; a second review 404s', async () => {
    moderatorMock = true;
    resultQueue = [{ rows: [{ id: 'v1' }], rowCount: 1 }];
    let res = await makeApp().request('/api/wire/fleet/v1/approve', { method: 'POST', body: '{}' });
    expect((await res.json()).status).toBe('published');
    resultQueue = [{ rows: [], rowCount: 0 }];
    res = await makeApp().request('/api/wire/fleet/v1/approve', { method: 'POST', body: '{}' });
    expect(res.status).toBe(404);
  });
});

describe('fleet views', () => {
  it("never counts the author's own view", async () => {
    nextResult = { rows: [{ author_id: 'user-1', views: '3' }], rowCount: 1 };
    const res = await makeApp('user-1').request('/api/wire/fleet/v1/view', { method: 'POST' });
    expect(await res.json()).toEqual({ views: 3, self: true });
  });

  it('counts a first view and dedupes the second', async () => {
    resultQueue = [
      { rows: [{ author_id: 'other', views: '3' }], rowCount: 1 },
      { rows: [{ parent_id: 'v1' }], rowCount: 1 }, // wire_views insert landed
      { rows: [{ views: '4' }], rowCount: 1 },
    ];
    let res = await makeApp('user-1').request('/api/wire/fleet/v1/view', { method: 'POST' });
    expect((await res.json()).views).toBe(4);

    resultQueue = [
      { rows: [{ author_id: 'other', views: '4' }], rowCount: 1 },
      { rows: [], rowCount: 0 }, // dedup conflict
      { rows: [{ views: '4' }], rowCount: 1 },
    ];
    res = await makeApp('user-1').request('/api/wire/fleet/v1/view', { method: 'POST' });
    expect((await res.json()).deduped).toBe(true);
  });
});
