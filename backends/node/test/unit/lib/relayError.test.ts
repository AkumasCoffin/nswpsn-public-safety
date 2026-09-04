/**
 * Why a relay failed, in a few words.
 *
 * Four transcript relays failed at once and each printed a thirty-line undici
 * stack — `at dispatch`, `at cors2`, `at Socket.onHttpSocketEnd` — that said
 * only that a fetch was made from a route handler. The one useful fact was
 * buried under `caused by`, and the most useful fact of all was not there:
 * whether WE gave up or the far end did. These pin the two shapes that
 * actually occur, so a log line can answer that without the stack.
 */
import { describe, it, expect } from 'vitest';
import { describeRelayError } from '../../../src/lib/relayError.js';

describe('describeRelayError', () => {
  it('names our own deadline as a timeout, not as an upstream failure', () => {
    // AbortSignal.timeout raises a DOMException named TimeoutError. The
    // distinction is the whole point: a timeout means the relay was still
    // waiting, which is a different problem from the socket dying.
    const err = Object.assign(new Error('The operation was aborted due to timeout'), {
      name: 'TimeoutError',
    });
    expect(describeRelayError(err)).toBe('timeout');
  });

  it('treats a plain abort the same way', () => {
    // Node has reported this as both over the versions; neither is upstream's
    // fault, and the reader does not care which spelling arrived.
    expect(describeRelayError(Object.assign(new Error('aborted'), { name: 'AbortError' })))
      .toBe('timeout');
  });

  it('digs the real reason out from under "fetch failed"', () => {
    // The message that mattered on the night. `fetch failed` on its own names
    // no cause at all, which is why the stack was being read instead.
    const err = Object.assign(new TypeError('fetch failed'), {
      cause: new Error('other side closed'),
    });
    expect(describeRelayError(err)).toBe('other side closed');
  });

  it('reads a cause that arrives as a bare string', () => {
    const err = Object.assign(new TypeError('fetch failed'), { cause: 'ECONNREFUSED' });
    expect(describeRelayError(err)).toBe('ECONNREFUSED');
  });

  it('falls back to the error itself rather than to "unknown"', () => {
    // An unexpected error stays legible instead of being flattened into a
    // word that would send the next reader back to the stack anyway.
    expect(describeRelayError(new Error('relay refused the key'))).toBe('relay refused the key');
    expect(describeRelayError('just a string')).toBe('just a string');
  });
});
