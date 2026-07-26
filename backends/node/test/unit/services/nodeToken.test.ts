import { describe, it, expect } from 'vitest';
import { createHash } from 'node:crypto';
import { mintNodeToken, logPrefix } from '../../../src/services/auth/nodeToken.js';

describe('mintNodeToken', () => {
  it('produces a random npsn_ + 40 hex token, hash-only + prefix', () => {
    const a = mintNodeToken();
    // Format the installer regexes expect stays unchanged.
    expect(a.token).toMatch(/^npsn_[0-9a-f]{40}$/);
    // Stored hash = sha256(token); prefix = 'npsn_' + 16 hex lookup key.
    expect(a.tokenHash).toBe(createHash('sha256').update(a.token).digest('hex'));
    expect(a.tokenPrefix).toBe(a.token.slice(0, 21));
    expect(logPrefix(a.token)).toBe(a.token.slice(0, 12));
  });

  it('is random per call (not derivable)', () => {
    const a = mintNodeToken();
    const b = mintNodeToken();
    expect(a.token).not.toBe(b.token);
    expect(a.tokenHash).not.toBe(b.tokenHash);
  });
});
