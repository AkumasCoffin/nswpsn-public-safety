/**
 * Incident image upload/delete routes + the on-disk storage helpers.
 *
 * Uploads stream to a real temp directory (UPLOADS_DIR is pointed there)
 * so the streaming/rename/unlink paths are genuinely exercised; the DB is
 * a fake pool driven by a per-query result queue.
 */
import { describe, it, expect, beforeEach, afterAll, vi } from 'vitest';
import { Hono } from 'hono';
import { mkdtempSync, existsSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

const UPLOADS = mkdtempSync(path.join(tmpdir(), 'nswpsn-uploads-'));
process.env['UPLOADS_DIR'] = UPLOADS;

type Call = { sql: string; params?: unknown[] };
const calls: Call[] = [];
const txCalls: Call[] = [];
let resultQueue: Array<{ rows: unknown[]; rowCount?: number }> = [];
let getPoolReturn: 'pool' | 'null' = 'pool';

const fakeClient = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    txCalls.push({ sql, ...(params ? { params } : {}) });
    return resultQueue.shift() ?? { rows: [], rowCount: 0 };
  }),
  release: vi.fn(),
};

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return resultQueue.shift() ?? { rows: [], rowCount: 0 };
  }),
  connect: vi.fn(async () => fakeClient),
};

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => (getPoolReturn === 'pool' ? fakePool : null)),
}));

const canManageUsersMock = vi.fn(async () => false);
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return {
    ...actual,
    canEditIncidents: vi.fn(async () => true),
    canManageUsers: (...a: unknown[]) => canManageUsersMock(...(a as [])),
  };
});

const { incidentsRouter } = await import('../../../src/api/incidents.js');
const images = await import('../../../src/services/incidentImages.js');

const JPEG = Buffer.concat([Buffer.from([0xff, 0xd8, 0xff, 0xe0]), Buffer.alloc(64, 7)]);
const PNG = Buffer.concat([
  Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
  Buffer.alloc(64, 3),
]);

function makeApp(userId = 'editor-1') {
  const app = new Hono();
  app.use('*', async (c, next) => {
    c.set('userId' as never, userId as never);
    c.set('userName' as never, 'Test Editor' as never);
    await next();
  });
  app.route('/', incidentsRouter);
  return app;
}

function upload(app: Hono, id: string, body: Buffer, contentType = 'image/jpeg') {
  return app.request(`/api/incidents/${id}/images`, {
    method: 'POST',
    headers: { 'Content-Type': contentType },
    body: body as unknown as BodyInit,
  });
}

/** Files currently stored for an incident (excluding .part leftovers). */
function storedFiles(id: string): string[] {
  const dir = path.join(UPLOADS, 'incident-images', id);
  return existsSync(dir) ? readdirSync(dir) : [];
}

beforeEach(() => {
  calls.length = 0;
  txCalls.length = 0;
  resultQueue = [];
  getPoolReturn = 'pool';
  fakePool.query.mockClear();
  fakeClient.query.mockClear();
  fakeClient.release.mockClear();
  canManageUsersMock.mockReset();
  canManageUsersMock.mockResolvedValue(false);
});

afterAll(() => {
  rmSync(UPLOADS, { recursive: true, force: true });
});

describe('storage helpers', () => {
  it('maps content types to extensions and rejects unsupported ones', () => {
    expect(images.extForContentType('image/jpeg')).toBe('jpg');
    expect(images.extForContentType('image/PNG')).toBe('png');
    expect(images.extForContentType('image/webp')).toBe('webp');
    // HEIC is excluded on purpose — Cloudflare can't transform it.
    expect(images.extForContentType('image/heic')).toBeNull();
    expect(images.normaliseContentType('image/jpeg; charset=binary')).toBe('image/jpeg');
    expect(images.normaliseContentType('application/pdf')).toBeNull();
  });

  it('sniffs real signatures', () => {
    expect(images.sniffImageType(JPEG)).toBe('image/jpeg');
    expect(images.sniffImageType(PNG)).toBe('image/png');
    expect(images.sniffImageType(Buffer.from('GIF89a-----------'))).toBe('image/gif');
    expect(
      images.sniffImageType(Buffer.concat([Buffer.from('RIFF'), Buffer.alloc(4), Buffer.from('WEBP')])),
    ).toBe('image/webp');
    expect(images.sniffImageType(Buffer.from('<?php echo 1; ?>'))).toBeNull();
  });

  it('rejects unsafe id segments (path traversal)', () => {
    expect(images.isSafeIdSegment('a1b2-c3d4')).toBe(true);
    expect(images.isSafeIdSegment('../../etc')).toBe(false);
    expect(images.isSafeIdSegment('foo/bar')).toBe(false);
    expect(images.isSafeIdSegment('')).toBe(false);
  });

  it('deleteIncidentImageFile refuses to escape the incident directory', async () => {
    const outside = path.join(UPLOADS, 'keepme.txt');
    writeFileSync(outside, 'x');
    await images.deleteIncidentImageFile('inc-x', '/uploads/incident-images/inc-x/../../keepme.txt');
    expect(existsSync(outside)).toBe(true);
    rmSync(outside, { force: true });
  });
});

describe('POST /api/incidents/:id/images', () => {
  it('streams the file to disk and appends the JSONB entry', async () => {
    resultQueue = [
      { rows: [{ images: [] }], rowCount: 1 },  // pre-check
      { rows: [] },                             // BEGIN
      { rows: [{ images: [] }], rowCount: 1 },  // SELECT ... FOR UPDATE
      { rows: [] },                             // UPDATE
      { rows: [] },                             // COMMIT
    ];
    const res = await upload(makeApp(), 'inc-1', JPEG);
    expect(res.status).toBe(201);
    const body = (await res.json()) as { image: Record<string, unknown> };
    expect(body.image['content_type']).toBe('image/jpeg');
    expect(body.image['size']).toBe(JPEG.length);
    expect(body.image['uploaded_by']).toBe('editor-1');
    expect(body.image['uploaded_by_name']).toBe('Test Editor');
    expect(String(body.image['file'])).toMatch(
      /^\/uploads\/incident-images\/inc-1\/[0-9a-f-]{36}\.jpg$/,
    );

    // File landed, and no .part leftover.
    const files = storedFiles('inc-1');
    expect(files).toHaveLength(1);
    expect(files[0]).toMatch(/\.jpg$/);

    const update = txCalls.find((c) => c.sql.includes('UPDATE incidents SET images'));
    const stored = JSON.parse(String(update?.params?.[0])) as unknown[];
    expect(stored).toHaveLength(1);
    expect(txCalls.at(-1)?.sql).toBe('COMMIT');
  });

  it('409s at the 4-image cap without writing a file', async () => {
    const four = [1, 2, 3, 4].map((n) => ({ id: `i${n}`, file: `/uploads/x/${n}.jpg` }));
    resultQueue = [{ rows: [{ images: four }], rowCount: 1 }];
    const res = await upload(makeApp(), 'inc-cap', JPEG);
    expect(res.status).toBe(409);
    expect(((await res.json()) as { error: string }).error).toContain('4 photos');
    expect(storedFiles('inc-cap')).toHaveLength(0);
  });

  it('409s and unlinks when a concurrent upload filled the last slot', async () => {
    const four = [1, 2, 3, 4].map((n) => ({ id: `i${n}`, file: `/uploads/x/${n}.jpg` }));
    resultQueue = [
      { rows: [{ images: [] }], rowCount: 1 },     // pre-check saw room
      { rows: [] },                                // BEGIN
      { rows: [{ images: four }], rowCount: 1 },   // lock sees it full
    ];
    const res = await upload(makeApp(), 'inc-race', JPEG);
    expect(res.status).toBe(409);
    // The streamed file must not survive the rejected transaction.
    expect(storedFiles('inc-race')).toHaveLength(0);
    expect(txCalls.some((c) => c.sql === 'ROLLBACK')).toBe(true);
  });

  it('415s when the bytes do not match the declared type', async () => {
    resultQueue = [{ rows: [{ images: [] }], rowCount: 1 }];
    const res = await upload(makeApp(), 'inc-bad', Buffer.from('<?php system($_GET[0]); ?>'));
    expect(res.status).toBe(415);
    expect(storedFiles('inc-bad')).toHaveLength(0);
  });

  it('415s on an unsupported declared type (HEIC) before touching the DB', async () => {
    const res = await upload(makeApp(), 'inc-heic', JPEG, 'image/heic');
    expect(res.status).toBe(415);
    expect(calls).toHaveLength(0);
  });

  it('404s for an unknown or soft-deleted incident', async () => {
    resultQueue = [{ rows: [], rowCount: 0 }];
    const res = await upload(makeApp(), 'ghost', JPEG);
    expect(res.status).toBe(404);
  });

  it('404s an id that could escape the uploads directory', async () => {
    const res = await upload(makeApp(), '..%2F..%2Fetc', JPEG);
    expect(res.status).toBe(404);
    expect(calls).toHaveLength(0);
  });
});

describe('DELETE /api/incidents/:id/images/:imageId', () => {
  const IMG_ID = '11111111-2222-3333-4444-555555555555';
  const entry = (uploader: string | null) => ({
    id: IMG_ID,
    file: `/uploads/incident-images/inc-d/${IMG_ID}.jpg`,
    size: 10,
    content_type: 'image/jpeg',
    uploaded_by: uploader,
    uploaded_by_name: 'Someone',
    uploaded_at: '2026-01-01T00:00:00.000Z',
  });

  it('lets the uploader remove their own photo', async () => {
    resultQueue = [
      { rows: [] },                                          // BEGIN
      { rows: [{ images: [entry('editor-1')] }], rowCount: 1 },
      { rows: [] },                                          // UPDATE
      { rows: [] },                                          // COMMIT
    ];
    const res = await makeApp().request(`/api/incidents/inc-d/images/${IMG_ID}`, {
      method: 'DELETE',
    });
    expect(res.status).toBe(200);
    const update = txCalls.find((c) => c.sql.includes('UPDATE incidents SET images'));
    expect(JSON.parse(String(update?.params?.[0]))).toEqual([]);
  });

  it('403s a different editor', async () => {
    resultQueue = [
      { rows: [] },
      { rows: [{ images: [entry('someone-else')] }], rowCount: 1 },
    ];
    const res = await makeApp().request(`/api/incidents/inc-d/images/${IMG_ID}`, {
      method: 'DELETE',
    });
    expect(res.status).toBe(403);
    expect(((await res.json()) as { error: string }).error).toContain('uploaded this photo');
    expect(txCalls.some((c) => c.sql.includes('UPDATE incidents SET images'))).toBe(false);
  });

  it('lets an admin moderate any photo', async () => {
    canManageUsersMock.mockResolvedValue(true);
    resultQueue = [
      { rows: [] },
      { rows: [{ images: [entry('someone-else')] }], rowCount: 1 },
      { rows: [] },
      { rows: [] },
    ];
    const res = await makeApp().request(`/api/incidents/inc-d/images/${IMG_ID}`, {
      method: 'DELETE',
    });
    expect(res.status).toBe(200);
  });

  it('404s an unknown image id', async () => {
    resultQueue = [
      { rows: [] },
      { rows: [{ images: [entry('editor-1')] }], rowCount: 1 },
    ];
    const res = await makeApp().request(
      '/api/incidents/inc-d/images/99999999-8888-7777-6666-555555555555',
      { method: 'DELETE' },
    );
    expect(res.status).toBe(404);
  });
});
