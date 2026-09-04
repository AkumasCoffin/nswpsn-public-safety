/**
 * What went wrong on an outbound relay, in a few words.
 *
 * Four transcript relays failed at 02:16 and each one printed a full undici
 * stack — `at dispatch`, `at cors2`, `at Socket.onHttpSocketEnd` — thirty
 * lines that say only that a fetch was made from a route handler. The one
 * fact worth having was buried in `caused by`, and the one fact that was not
 * there at all is whether we gave up or the far end did.
 *
 * The two shapes that actually occur:
 *
 *   - AbortSignal.timeout fires → a DOMException named 'TimeoutError'. Ours.
 *   - the socket dies → TypeError 'fetch failed' with the real reason on
 *     `.cause` (SocketError 'other side closed', ECONNREFUSED, and so on).
 *
 * Anything else falls through to its own message, so an unexpected error is
 * still legible rather than flattened into 'unknown'.
 */
export function describeRelayError(err: unknown): string {
  if (err instanceof Error) {
    // A timeout is OUR deadline, not the upstream's failure, and the two want
    // different responses from whoever reads the log.
    if (err.name === 'TimeoutError' || err.name === 'AbortError') return 'timeout';
    const cause = (err as { cause?: unknown }).cause;
    if (cause instanceof Error && cause.message) return cause.message;
    if (typeof cause === 'string' && cause) return cause;
    if (err.message) return err.message;
  }
  return String(err);
}
