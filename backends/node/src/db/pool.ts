/**
 * Postgres connection pool.
 *
 * Replaces backends/db.py's ThreadedConnectionPool wrapper.
 *
 * W1 only stub-exports a getPool() factory because /api/health and
 * /api/config don't touch the DB. As soon as W2 starts (LiveStore +
 * ArchiveWriter), this gets wired up properly with `pg.Pool`.
 *
 * Design notes for the real implementation (coming in W2):
 *   - max=20 (matches Python pool max). min unset (lazy connect).
 *   - statement_timeout default = '30s' applied via session-init query
 *     so individual handlers don't need to remember it. Per-query
 *     overrides via SET LOCAL when they need it tighter.
 *   - On client release, run a no-op query to detect dead connections
 *     before returning to pool — pg's default doesn't.
 *   - Single shared instance; no per-request creation.
 *
 * Why we're not using an ORM: the Python codebase showed us the value
 * of seeing the exact SQL we ship. ORMs hide query shape and we paid
 * for that today. Raw `pg` for everything.
 */
import type { Pool } from 'pg';
import { config } from '../config.js';

let _pool: Pool | null = null;

/**
 * Lazy-initialise the pool on first use. Returns null if DATABASE_URL
 * isn't configured — callers that genuinely need the DB should throw,
 * but health/config endpoints can degrade gracefully.
 */
export async function getPool(): Promise<Pool | null> {
  if (_pool) return _pool;
  if (!config.DATABASE_URL) return null;

  // Dynamic import so the `pg` module isn't loaded at startup if no
  // DATABASE_URL is set (e.g. local dev hitting only health/config).
  const { Pool: PgPool } = await import('pg');
  _pool = new PgPool({
    connectionString: config.DATABASE_URL,
    max: 20,
    // Each new connection gets a default statement_timeout. Individual
    // queries can override with SET LOCAL inside a transaction.
    statement_timeout: 30_000,
    // Cap how long a connection sits idle before pg recycles it. Keeps
    // long-lived processes from accumulating zombie sessions.
    idleTimeoutMillis: 60_000,
  });

  // Surface unexpected pool errors via the structured logger. Without
  // this, pg crashes the process on idle-client errors.
  _pool.on('error', (err) => {
    // eslint-disable-next-line no-console
    console.error('pg pool error:', err);
  });

  return _pool;
}

/**
 * Graceful shutdown. Called from src/lib/shutdown.ts when the process
 * receives SIGTERM/SIGINT. Drains in-flight queries before closing.
 */
export async function closePool(): Promise<void> {
  if (_pool) {
    await _pool.end();
    _pool = null;
  }
}
