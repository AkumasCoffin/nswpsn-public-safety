/**
 * POST /api/session/token: mints only for first-party pages, POST only, and
 * within an address's budget.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

import { sessionRouter, _resetSessionMintLimit } from '../../../src/api/session.js';
import { verifyBrowserToken } from '../../../src/services/auth/browserToken.js';
import { config } from '../../../src/config.js';

function makeApp() {
  const app = new Hono();
  app.route('/', sessionRouter);
  return app;
}

const UA = 'Mozilla/5.0 session-test';
const IP = '203.0.113.9';
const base = { 'User-Agent': UA, 'CF-Connecting-IP': IP };

beforeEach(() => _resetSessionMintLimit());

describe('POST /api/session/token', () => {
  it('mints for a first-party page, bound to that client', async () => {
    const res = await makeApp().request('/api/session/token', {
      method: 'POST',
      headers: { ...base, Origin: 'https://nswpsn.forcequit.xyz', 'Sec-Fetch-Site': 'same-site' },
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('cache-control')).toBe('no-store');
    const body = (await res.json()) as { token: string; expiresIn: number };
    expect(body.expiresIn).toBe(900);
    expect(verifyBrowserToken(body.token, { ip: IP, ua: UA })).toBe('ok');
    expect(verifyBrowserToken(body.token, { ip: '198.51.100.1', ua: UA })).toBe('mismatch');
  });

  it('accepts a Referer when a browser sends no Origin', async () => {
    const res = await makeApp().request('/api/session/token', {
      method: 'POST',
      headers: { ...base, Referer: 'https://nswpsn.forcequit.xyz/map' },
    });
    expect(res.status).toBe(200);
  });

  it('refuses a typed URL / curl (no Origin, no Referer) and other sites', async () => {
    let res = await makeApp().request('/api/session/token', { method: 'POST', headers: base });
    expect(res.status).toBe(403);
    expect(await res.json()).toMatchObject({ error: 'first_party_only' });
    res = await makeApp().request('/api/session/token', {
      method: 'POST',
      headers: { ...base, Origin: 'https://radio.forcequit.xyz' },
    });
    expect(res.status).toBe(403);
    res = await makeApp().request('/api/session/token', {
      method: 'POST',
      headers: { ...base, Origin: 'https://nswpsn.forcequit.xyz', 'Sec-Fetch-Site': 'cross-site' },
    });
    expect(res.status).toBe(403);
  });

  it('is POST only', async () => {
    const res = await makeApp().request('/api/session/token', {
      headers: { ...base, Origin: 'https://nswpsn.forcequit.xyz' },
    });
    expect(res.status).toBe(404);
  });

  it('rate-limits an address past its mint budget', async () => {
    const app = makeApp();
    const opts = { method: 'POST', headers: { ...base, Origin: 'https://nswpsn.forcequit.xyz' } };
    for (let i = 0; i < config.SESSION_MINT_PER_10MIN; i++) {
      expect((await app.request('/api/session/token', opts)).status).toBe(200);
    }
    const res = await app.request('/api/session/token', opts);
    expect(res.status).toBe(429);
    // Another address is unaffected.
    const other = await app.request('/api/session/token', {
      ...opts,
      headers: { ...opts.headers, 'CF-Connecting-IP': '198.51.100.1' },
    });
    expect(other.status).toBe(200);
  });
});
