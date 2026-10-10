/**
 * Browser session tokens: the property that matters is that a token is
 * worthless off the client it was minted for — a copy fails on address or
 * User-Agent, a wait fails on expiry, a byte change fails on signature.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

import {
  mintBrowserToken,
  verifyBrowserToken,
  _resetBrowserTokenWarn,
} from '../../../src/services/auth/browserToken.js';
import { ipScope } from '../../../src/services/auth/clientIp.js';
import { log } from '../../../src/lib/log.js';

const me = { ip: '203.0.113.9', ua: 'Mozilla/5.0 test' };

describe('browser session token', () => {
  beforeEach(() => {
    _resetBrowserTokenWarn();
    vi.mocked(log.warn).mockClear();
  });

  it('round-trips for the client it was minted for', () => {
    const { token, expiresIn } = mintBrowserToken(me);
    expect(token.startsWith('bt1.')).toBe(true);
    expect(expiresIn).toBe(900);
    expect(verifyBrowserToken(token, me)).toBe('ok');
  });

  it('expires', () => {
    const t0 = 1_800_000_000_000;
    const { token } = mintBrowserToken(me, t0);
    expect(verifyBrowserToken(token, me, t0 + 899_000)).toBe('ok');
    expect(verifyBrowserToken(token, me, t0 + 900_000)).toBe('expired');
  });

  it('fails from another address, but tolerates the same /24', () => {
    const { token } = mintBrowserToken(me);
    expect(verifyBrowserToken(token, { ...me, ip: '198.51.100.7' })).toBe('mismatch');
    expect(verifyBrowserToken(token, { ...me, ip: '203.0.113.200' })).toBe('ok');
  });

  it('binds v6 at /48 so a rotating interface id does not re-mint', () => {
    const v6 = { ip: '2001:db8:abcd:1::1', ua: me.ua };
    const { token } = mintBrowserToken(v6);
    expect(verifyBrowserToken(token, { ...v6, ip: '2001:db8:abcd:ffff::9' })).toBe('ok');
    expect(verifyBrowserToken(token, { ...v6, ip: '2001:db8:ffff:1::1' })).toBe('mismatch');
    expect(ipScope('2001:DB8:abcd:1::1')).toBe('2001:db8:abcd::/48');
    expect(ipScope('203.0.113.9')).toBe('203.0.113.0/24');
  });

  it('fails from another browser', () => {
    const { token } = mintBrowserToken(me);
    expect(verifyBrowserToken(token, { ...me, ua: 'curl/8.0' })).toBe('mismatch');
  });

  it('fails when any part is altered', () => {
    const { token } = mintBrowserToken(me);
    const [p, exp, nonce, sig] = token.split('.') as [string, string, string, string];
    expect(verifyBrowserToken(`${p}.${Number(exp) + 100}.${nonce}.${sig}`, me)).toBe('mismatch');
    expect(verifyBrowserToken(`${p}.${exp}.${nonce}x.${sig}`, me)).toBe('mismatch');
    expect(verifyBrowserToken(`${p}.${exp}.${nonce}.${sig.slice(0, -1)}A`, me)).toBe('mismatch');
    expect(verifyBrowserToken('bt1.garbage', me)).toBe('bad');
    expect(verifyBrowserToken('not-a-token', me)).toBe('bad');
  });

  it('warns once when the secret is derived from the site key', () => {
    mintBrowserToken(me);
    mintBrowserToken(me);
    expect(vi.mocked(log.warn)).toHaveBeenCalledTimes(1);
  });
});
