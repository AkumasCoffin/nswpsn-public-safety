/**
 * Migration runner.
 *
 * Reads `src/db/migrations/*.sql` in lexical order, runs each one
 * exactly once, records the filename in `schema_migrations`. Idempotent
 * on restart — already-applied files are skipped.
 *
 * Conventions:
 *   - Filenames: `NNN_short_name.sql` (e.g. `001_init.sql`). Numeric
 *     prefix gives total order; never rename or reorder once applied.
 *   - Each file is wrapped in a transaction unless it begins with the
 *     literal first line `-- noTransaction` (some DDL like CREATE INDEX
 *     CONCURRENTLY can't run inside a transaction block).
 *   - Migrations should be designed to be safe to re-run. We don't
 *     re-run them, but if someone manually clears schema_migrations and
 *     reapplies, things shouldn't break.
 *
 * Run with `npx tsx src/db/migrate.ts` or programmatically from the
 * server entrypoint before binding the port.
 */
import { readFile, readdir } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { getPool } from './pool.js';
import { log } from '../lib/log.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const MIGRATIONS_DIR = join(__dirname, 'migrations');

async function ensureMigrationsTable(): Promise<void> {
  const pool = await getPool();
  if (!pool) throw new Error('DATABASE_URL not configured');
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
}

async function appliedSet(): Promise<Set<string>> {
  const pool = await getPool();
  if (!pool) throw new Error('DATABASE_URL not configured');
  const res = await pool.query<{ filename: string }>(
    'SELECT filename FROM schema_migrations',
  );
  return new Set(res.rows.map((r) => r.filename));
}

export async function runMigrations(): Promise<{
  applied: string[];
  skipped: string[];
}> {
  const pool = await getPool();
  if (!pool) {
    log.warn('runMigrations: DATABASE_URL not set, skipping');
    return { applied: [], skipped: [] };
  }

  await ensureMigrationsTable();
  const already = await appliedSet();

  let files: string[];
  try {
    files = (await readdir(MIGRATIONS_DIR)).filter((f) => f.endsWith('.sql'));
  } catch (err) {
    log.error({ err, dir: MIGRATIONS_DIR }, 'migrations dir unreadable');
    throw err;
  }
  files.sort(); // lexical = numeric for NNN_*.sql

  const applied: string[] = [];
  const skipped: string[] = [];

  for (const f of files) {
    if (already.has(f)) {
      skipped.push(f);
      continue;
    }
    const sql = await readFile(join(MIGRATIONS_DIR, f), 'utf8');
    const noTx = /^\s*--\s*noTransaction/m.test(sql.split('\n')[0] ?? '');

    log.info({ file: f, noTx }, 'applying migration');
    const client = await pool.connect();
    try {
      // Migrations may build large indexes / scan multi-million-row
      // tables. The pool's default 30s statement_timeout from
      // db/pool.ts will kill them. Override here to "unlimited" so the
      // migration runner doesn't fight its own infrastructure. We set
      // session-level (not LOCAL) so it covers the noTx path too;
      // tx-mode SET sticks for the BEGIN/COMMIT span only because the
      // client is released immediately after.
      await client.query('SET statement_timeout = 0');
      if (!noTx) await client.query('BEGIN');
      await client.query(sql);
      await client.query(
        'INSERT INTO schema_migrations (filename) VALUES ($1)',
        [f],
      );
      if (!noTx) await client.query('COMMIT');
      applied.push(f);
    } catch (err) {
      if (!noTx) {
        try {
          await client.query('ROLLBACK');
        } catch {
          /* ignore */
        }
      }
      log.error({ err, file: f }, 'migration failed');
      throw err;
    } finally {
      client.release();
    }
  }

  log.info({ applied, skipped }, 'migrations complete');
  return { applied, skipped };
}

// Allow running directly: `npx tsx src/db/migrate.ts`
const isMain =
  import.meta.url === `file://${process.argv[1]?.replace(/\\/g, '/')}` ||
  process.argv[1]?.endsWith('migrate.ts') ||
  process.argv[1]?.endsWith('migrate.js');
if (isMain) {
  void runMigrations()
    .then(() => process.exit(0))
    .catch((err) => {
      log.fatal({ err }, 'migration runner failed');
      process.exit(1);
    });
}
