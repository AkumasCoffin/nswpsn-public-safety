/**
 * Heartbeat route tests.
 *
 * Drives the route directly via Hono's request handler; the underlying
 * activityMode singleton is real but harmless — setActivityMode on the
 * poller is a no-op when the poller isn't running, so calling the route
 * just exercises the in-memory map + JSON response shape.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { heartbeatRouter } from '../../../src/api/heartbeat.js';
import { _resetForTests } from '../../../src/services/activityMode.js';

function makeApp() {
  const app = new Hono();
  app.route('/', heartbeatRouter);
  return app;
}

describe('GET /api/heartbeat', () => {
  beforeEach(() => _resetForTests());

  it('returns idle mode with no sessions', async () => {
    const app = makeApp();
    const res = await app.request('/api/heartbeat?action=ping&page_id=&data_page=false');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['mode']).toBe('idle');
    expect(body['total_viewers']).toBe(0);
    expect(body['data_viewers']).toBe(0);
    expect(body['next_collection_seconds']).toBe(300);
  });

  it('opens a data-page session and reports active mode', async () => {
    const app = makeApp();
    const res = await app.request(
      '/api/heartbeat?action=open&page_id=tab-123&page_type=live&data_page=true',
    );
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['mode']).toBe('active');
    expect(body['total_viewers']).toBe(1);
    expect(body['data_viewers']).toBe(1);
    expect(body['is_data_page']).toBe(true);
    expect(body['page_type']).toBe('live');
    expect(body['interval']).toBe(60);
  });

  it('close removes the session', async () => {
    const app = makeApp();
    await app.request(
      '/api/heartbeat?action=open&page_id=t&page_type=live&data_page=true',
    );
    const res = await app.request('/api/heartbeat?action=close&page_id=t');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['total_viewers']).toBe(0);
    expect(body['mode']).toBe('idle');
  });

  it('accepts both GET and POST', async () => {
    const app = makeApp();
    const post = await app.request(
      '/api/heartbeat?action=open&page_id=p&data_page=true',
      { method: 'POST' },
    );
    expect(post.status).toBe(200);
    const get = await app.request('/api/heartbeat?action=ping&page_id=p&data_page=true');
    expect(get.status).toBe(200);
  });

  it('coerces unknown action to ping', async () => {
    const app = makeApp();
    const res = await app.request(
      '/api/heartbeat?action=garbage&page_id=p&data_page=true',
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['total_viewers']).toBe(1);
  });

  it('parses data_page in any truthy variant', async () => {
    const app = makeApp();
    for (const v of ['true', '1', 'yes', 'TRUE']) {
      _resetForTests();
      const res = await app.request(
        `/api/heartbeat?action=open&page_id=p&data_page=${v}`,
      );
      const body = (await res.json()) as Record<string, unknown>;
      expect(body['is_data_page']).toBe(true);
    }
  });
});
