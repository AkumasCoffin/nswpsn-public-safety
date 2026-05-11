/**
 * Exponential backoff steps for source pollers.
 *
 * Mirrors the Python schedule (line 3126 of external_api_proxy.py):
 *   [300, 900, 1800, 3600] seconds for the 1st..4th consecutive failure.
 *
 * Returns the *next* delay in seconds given the current consecutive
 * failure count. Caller resets `failureCount` on success.
 */
const STEPS_SECS = [300, 900, 1800, 3600] as const;

export function backoffSeconds(failureCount: number): number {
  if (failureCount <= 0) return 0;
  const idx = Math.min(failureCount - 1, STEPS_SECS.length - 1);
  return STEPS_SECS[idx] ?? STEPS_SECS[STEPS_SECS.length - 1] ?? 3600;
}
