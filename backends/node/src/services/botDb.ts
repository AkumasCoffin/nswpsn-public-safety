/**
 * Third Postgres pool — keyed off BOT_DATA_DATABASE_URL.
 *
 * The dashboard uses a SEPARATE Postgres from the main archive
 * (DATABASE_URL) and from rdio-scanner (RDIO_DATABASE_URL). This pool
 * mirrors python's `_bot_db_conn()` at external_api_proxy.py:16575-16625.
 *
 * We can't reuse the main pool helper at src/db/pool.ts because it
 * reads config.DATABASE_URL — the dashboard needs its own DSN. The bot
 * data lives wherever the discord-bot writes (alert_presets,
 * guild_mute_state, channel_mute_state, preset_fire_log,
 * pending_bot_actions, dash_sessions, dashboard_users).
 *
 * BOT_DATA_DATABASE_URL is read directly from process.env (NOT via
 * config.ts) per the task brief — config.ts is owned by the consolidator
 * and the var is listed in the final report so they can wire it into the
 * Zod schema. Reading from env directly here keeps the dashboard router
 * deployable independently of that config edit.
 */
import type { Pool } from 'pg';
import { log } from '../lib/log.js';

let _pool: Pool | null = null;
let _initAttempted = false;

/**
 * Whether the BOT_DATA_DATABASE_URL env var is set. Mirrors python's
 * _dash_enabled() at line 16571. Routes that need bot DB but don't have
 * a DSN return 503 so it's clear the feature is just unconfigured.
 */
export function isBotDbConfigured(): boolean {
  return Boolean(process.env['BOT_DATA_DATABASE_URL']);
}

/**
 * Lazy-initialise the bot pool. Returns null when BOT_DATA_DATABASE_URL
 * is unset — caller should 503. Mirrors python's ThreadedConnectionPool(1, 10).
 */
export async function getBotDbPool(): Promise<Pool | null> {
  if (_pool) return _pool;
  const dsn = process.env['BOT_DATA_DATABASE_URL'];
  if (!dsn) {
    if (!_initAttempted) {
      _initAttempted = true;
      log.warn('BOT_DATA_DATABASE_URL not set; dashboard endpoints will 503');
    }
    return null;
  }
  const { Pool: PgPool } = await import('pg');
  _pool = new PgPool({
    connectionString: dsn,
    // python uses min=1 max=10 — match.
    max: 10,
    statement_timeout: 30_000,
    idleTimeoutMillis: 60_000,
  });
  _pool.on('error', (err) => {
    log.error({ err }, 'bot pg pool error');
  });
  _initAttempted = true;
  return _pool;
}

/** Close the pool — for graceful shutdown. */
export async function closeBotDbPool(): Promise<void> {
  if (_pool) {
    await _pool.end();
    _pool = null;
  }
  _initAttempted = false;
}

/**
 * Test-only helper. Lets vitest swap in a fake pool without going
 * through the BOT_DATA_DATABASE_URL env var.
 */
export function _setBotDbPoolForTests(p: Pool | null): void {
  _pool = p;
  _initAttempted = true;
}
