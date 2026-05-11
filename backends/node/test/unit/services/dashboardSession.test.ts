/**
 * dashboardSession unit tests.
 *
 * Covers the cookie HMAC roundtrip, tampering rejection, expiry, and
 * the in-memory session map. We don't exercise putSession's DB write-
 * through here — that's tested via the dashboard router tests where we
 * mock the bot DB pool. Here we focus on pure crypto / pure helpers.
 */
import { describe, it, expect, beforeEach } from 'vitest';

// Set the secret BEFORE importing the module — getSessionSecret() reads
// process.env at call time so we just need it set before any test runs.
process.env['DASHBOARD_SESSION_SECRET'] = 'test-secret-for-vitest';
process.env['DASHBOARD_ADMIN_IDS'] = '111,222';

const mod = await import('../../../src/services/dashboardSession.js');

describe('dashboardSession crypto', () => {
  it('roundtrips a payload through makeCookie/parseCookie', () => {
    const secret = mod.getSessionSecret();
    expect(secret).not.toBeNull();
    const payload = { sid: 'abc', exp: 9_999_999_999 };
    const cookie = mod.makeCookie(payload, secret!);
    expect(cookie).toContain('.');
    const back = mod.parseCookie<typeof payload>(cookie, secret!);
    expect(back).toEqual(payload);
  });

  it('rejects a tampered payload', () => {
    const secret = mod.getSessionSecret()!;
    const cookie = mod.makeCookie({ sid: 'abc', exp: 9_999_999_999 }, secret);
    const [_p, sig] = cookie.split('.');
    // Mutate the payload but reuse the original signature -> mismatch.
    const tampered = `${Buffer.from('{"sid":"evil","exp":9999999999}').toString('base64url')}.${sig}`;
    const back = mod.parseCookie(tampered, secret);
    expect(back).toBeNull();
  });

  it('rejects a malformed cookie', () => {
    const secret = mod.getSessionSecret()!;
    expect(mod.parseCookie('not-a-cookie', secret)).toBeNull();
    expect(mod.parseCookie('', secret)).toBeNull();
    expect(mod.parseCookie(null, secret)).toBeNull();
  });

  it('returns null when DASHBOARD_SESSION_SECRET is unset', () => {
    const orig = process.env['DASHBOARD_SESSION_SECRET'];
    delete process.env['DASHBOARD_SESSION_SECRET'];
    try {
      expect(mod.getSessionSecret()).toBeNull();
    } finally {
      process.env['DASHBOARD_SESSION_SECRET'] = orig;
    }
  });
});

describe('parseCookieHeader', () => {
  it('parses a multi-cookie header', () => {
    const out = mod.parseCookieHeader('a=1; b=hello; c=%2Fpath');
    expect(out).toEqual({ a: '1', b: 'hello', c: '/path' });
  });

  it('handles empty / missing input', () => {
    expect(mod.parseCookieHeader(undefined)).toEqual({});
    expect(mod.parseCookieHeader('')).toEqual({});
  });
});

describe('buildSetCookie / buildClearCookie', () => {
  it('emits SameSite=None; Secure when secure=true', () => {
    const out = mod.buildSetCookie('foo', 'bar', { maxAge: 60, secure: true, domain: '.x.test' });
    expect(out).toContain('foo=bar');
    expect(out).toContain('Max-Age=60');
    expect(out).toContain('SameSite=None');
    expect(out).toContain('Secure');
    expect(out).toContain('Domain=.x.test');
    expect(out).toContain('HttpOnly');
  });

  it('emits SameSite=Lax (no Secure) when secure=false', () => {
    const out = mod.buildSetCookie('foo', 'bar', { maxAge: 60, secure: false });
    expect(out).toContain('SameSite=Lax');
    expect(out).not.toContain('Secure');
  });

  it('clear-cookie has Max-Age=0', () => {
    const out = mod.buildClearCookie('foo', true);
    expect(out).toContain('Max-Age=0');
    expect(out).toContain('foo=;');
  });
});

describe('admin checks', () => {
  it('isAdmin returns true for ids in DASHBOARD_ADMIN_IDS', () => {
    expect(
      mod.isAdmin({
        uid: '111',
        username: '',
        avatar: null,
        access_token: '',
        token_type: 'Bearer',
        refresh_token: null,
        guilds: [],
        gfresh: 0,
        iat: 0,
        exp: 0,
      }),
    ).toBe(true);
  });

  it('isAdmin returns false for non-admin uid', () => {
    expect(
      mod.isAdmin({
        uid: '999',
        username: '',
        avatar: null,
        access_token: '',
        token_type: 'Bearer',
        refresh_token: null,
        guilds: [],
        gfresh: 0,
        iat: 0,
        exp: 0,
      }),
    ).toBe(false);
  });

  it('isAdmin returns false for null/undefined session', () => {
    expect(mod.isAdmin(null)).toBe(false);
    expect(mod.isAdmin(undefined)).toBe(false);
  });
});

describe('isSecureRequest', () => {
  it('honours X-Forwarded-Proto', () => {
    expect(mod.isSecureRequest('https', 'http://x.test/y')).toBe(true);
    expect(mod.isSecureRequest('http', 'https://x.test/y')).toBe(false);
  });
  it('falls back to URL scheme when no XFP', () => {
    expect(mod.isSecureRequest(undefined, 'https://x.test/y')).toBe(true);
    expect(mod.isSecureRequest(null, 'http://x.test/y')).toBe(false);
  });
});

describe('newSid', () => {
  it('generates a unique base64url string', () => {
    const a = mod.newSid();
    const b = mod.newSid();
    expect(a).not.toBe(b);
    expect(a).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(a.length).toBeGreaterThan(20);
  });
});

describe('loadSession', () => {
  beforeEach(() => {
    mod._resetSessionsForTests();
  });

  it('returns null without a cookie', async () => {
    expect(await mod.loadSession(undefined)).toBeNull();
    expect(await mod.loadSession('')).toBeNull();
  });

  it('returns null when expiry is in the past', async () => {
    const secret = mod.getSessionSecret()!;
    const cookie = mod.makeCookie({ sid: 'sid-x', exp: 1 }, secret);
    expect(await mod.loadSession(`nswpsn_dash_sess=${cookie}`)).toBeNull();
  });

  it('returns null when sid not in session map', async () => {
    const secret = mod.getSessionSecret()!;
    const cookie = mod.makeCookie(
      { sid: 'unknown-sid', exp: Math.floor(Date.now() / 1000) + 600 },
      secret,
    );
    expect(await mod.loadSession(`nswpsn_dash_sess=${cookie}`)).toBeNull();
  });

  it('returns the session when cookie + map are valid', async () => {
    const secret = mod.getSessionSecret()!;
    const sid = 'live-sid';
    const exp = Math.floor(Date.now() / 1000) + 600;
    mod._getSessionsForTests().set(sid, {
      uid: '42',
      username: 'tester',
      avatar: null,
      access_token: 'at',
      token_type: 'Bearer',
      refresh_token: null,
      guilds: [],
      gfresh: 0,
      iat: 0,
      exp,
    });
    const cookie = mod.makeCookie({ sid, exp }, secret);
    const back = await mod.loadSession(`nswpsn_dash_sess=${cookie}`);
    expect(back).not.toBeNull();
    expect(back?.uid).toBe('42');
    expect(back?._sid).toBe(sid);
    expect(back?.last_seen).toBeGreaterThan(0);
  });
});
