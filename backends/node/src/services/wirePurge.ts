/**
 * Purge sweep for Wire posts past their deletion recovery window.
 *
 * Deleting an article or fleet vehicle only sets `deleted_at` (the post is
 * hidden at once but recoverable from the profile). This sweep finishes the
 * job after RECOVERY_DAYS: children and rows go in a transaction, then the
 * storage objects — Cloudflare Images by id, R2 objects only once nothing
 * references them any more (image dedup can share one object across posts).
 *
 * Batched and hourly: deletion volume is tiny, and a missed tick just means
 * a post lives a little past its window, never that one dies early.
 */
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { deleteCfImage, deleteR2Object } from './wire.js';

export const RECOVERY_DAYS = 5;
const SWEEP_INTERVAL_MS = 60 * 60 * 1000;
const BATCH = 25;

let timer: NodeJS.Timeout | null = null;

async function mediaRefsFor(pool: Pool, parentType: string, parentId: string): Promise<{ cfIds: string[]; r2Keys: string[] }> {
  const r = await pool.query<{ cf_image_id: string | null; poster_cf_image_id: string | null; r2_key: string | null; poster_r2_key: string | null }>(
    'SELECT cf_image_id, poster_cf_image_id, r2_key, poster_r2_key FROM wire_media WHERE parent_type=$1 AND parent_id=$2',
    [parentType, parentId],
  );
  const cfIds: string[] = [];
  const r2Keys: string[] = [];
  for (const row of r.rows) {
    if (row.cf_image_id) cfIds.push(row.cf_image_id);
    if (row.poster_cf_image_id) cfIds.push(row.poster_cf_image_id);
    if (row.r2_key) r2Keys.push(row.r2_key);
    if (row.poster_r2_key) r2Keys.push(row.poster_r2_key);
  }
  return { cfIds, r2Keys };
}

/** Delete an R2 object only when no wire_media row still references it. */
async function safeDeleteR2(pool: Pool, key: string): Promise<void> {
  if (!key) return;
  const r = await pool.query('SELECT 1 FROM wire_media WHERE r2_key = $1 OR poster_r2_key = $1 LIMIT 1', [key]);
  if (r.rowCount === 0) await deleteR2Object(key);
}

async function purgeArticles(pool: Pool): Promise<number> {
  const due = await pool.query<{ id: string }>(
    `SELECT id FROM articles
      WHERE deleted_at IS NOT NULL AND deleted_at < now() - make_interval(days => $1)
      LIMIT $2`,
    [RECOVERY_DAYS, BATCH],
  );
  for (const row of due.rows) {
    const { cfIds, r2Keys } = await mediaRefsFor(pool, 'article', row.id);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`DELETE FROM wire_media WHERE parent_type='article' AND parent_id=$1`, [row.id]);
      await client.query('DELETE FROM articles WHERE id = $1', [row.id]);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
    for (const iid of cfIds) await deleteCfImage(iid);
    for (const k of r2Keys) await safeDeleteR2(pool, k);
  }
  return due.rowCount ?? 0;
}

async function purgeFleet(pool: Pool): Promise<number> {
  const due = await pool.query<{ id: string; images: { key?: string }[] | null }>(
    `SELECT id, images FROM fleet_vehicles
      WHERE deleted_at IS NOT NULL AND deleted_at < now() - make_interval(days => $1)
      LIMIT $2`,
    [RECOVERY_DAYS, BATCH],
  );
  for (const row of due.rows) {
    await pool.query('DELETE FROM fleet_vehicles WHERE id = $1', [row.id]);
    for (const im of Array.isArray(row.images) ? row.images : []) {
      if (im.key) await deleteR2Object(im.key);
    }
  }
  return due.rowCount ?? 0;
}

export async function purgeDeletedWirePosts(): Promise<void> {
  const pool = await getPool();
  if (!pool) return;
  try {
    const a = await purgeArticles(pool);
    const f = await purgeFleet(pool);
    if (a || f) log.info({ articles: a, fleet: f }, 'wirePurge: purged posts past their recovery window');
  } catch (err) {
    log.warn({ err }, 'wirePurge: sweep failed (will retry next tick)');
  }
}

export function startWirePurge(intervalMs: number = SWEEP_INTERVAL_MS): void {
  if (timer) return;
  void purgeDeletedWirePosts();
  timer = setInterval(() => void purgeDeletedWirePosts(), intervalMs);
  timer.unref?.();
}
