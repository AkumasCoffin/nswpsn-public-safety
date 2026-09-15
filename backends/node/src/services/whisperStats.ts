/**
 * Durable whisper throughput counters (whisper_hourly, migration 098).
 *
 * The router's own stats are in-memory and reset on restart; this is the
 * persistent record the staff dashboard graphs. One row per (hour, backend),
 * incremented per ATTEMPT — a call that fails on the PC and succeeds on the
 * VM writes a failure row for 'pc' and a success row for 'vm'; a call no
 * backend could take writes to the reserved backend name 'none'.
 *
 * Recording is fire-and-forget: transcription volume is a few calls a
 * minute, so a direct upsert per attempt is cheap, and a stats failure must
 * never fail (or even delay) the transcription itself.
 */
import { getWriterPool } from '../db/pool.js';
import { log } from '../lib/log.js';

/** `ms` is the elapsed time of a SUCCESSFUL attempt; pass null on failure —
 *  failed-attempt latency (mostly timeouts) would poison the average. */
export function recordWhisperAttempt(backend: string, ok: boolean, ms: number | null): void {
  void (async () => {
    try {
      const pool = await getWriterPool();
      if (!pool) return; // no DB configured — stats are best-effort
      await pool.query(
        `INSERT INTO whisper_hourly (hour, backend, requests, failures, total_ms)
         VALUES (date_trunc('hour', now()), $1, 1, $2, $3)
         ON CONFLICT (hour, backend) DO UPDATE SET
           requests = whisper_hourly.requests + 1,
           failures = whisper_hourly.failures + EXCLUDED.failures,
           total_ms = whisper_hourly.total_ms + EXCLUDED.total_ms`,
        [backend, ok ? 0 : 1, ok && ms != null && Number.isFinite(ms) ? Math.round(ms) : 0],
      );
    } catch (err) {
      // Never let stats interfere with transcription; surface at debug only.
      log.debug({ err, backend }, 'whisper stats upsert failed');
    }
  })();
}
