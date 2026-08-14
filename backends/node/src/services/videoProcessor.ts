/**
 * Background worker that normalises uploaded Wire videos.
 *
 * Videos are PUT straight from the browser to R2 (presigned, bypassing this
 * origin's 50Mbps uplink), so they land raw: whatever bitrate the phone shot at,
 * full device metadata, no burned-in watermark, and a poster that only exists if
 * the uploader's browser could decode the codec. This worker pulls each new
 * clip back down, runs one ffmpeg pass (see services/videoTranscode.ts), and
 * swaps the result in.
 *
 * Loop shape copies nodeEventsPruner.ts (module timer + re-entrancy guard +
 * start/stop, wired in index.ts). Job state lives on the wire_media row rather
 * than in a queue table — nothing in this backend persists in-flight work, so a
 * restart mid-transcode simply leaves the row 'pending' and the next sweep
 * redoes it. That is the whole durability story.
 *
 * Failure is deliberately non-destructive: the ORIGINAL upload is only deleted
 * after the DB commit that points at the new object, so the worst outcome of a
 * transcode bug is an un-normalised, unwatermarked — but perfectly playable —
 * clip, never lost footage.
 */
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { getWriterPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import {
  deleteR2Object, downloadR2ToFile, newPosterKey, newVideoKey,
  r2Configured, uploadFileToR2,
} from './wire.js';
import { ffmpegAvailable, transcodeVideo } from './videoTranscode.js';

const POLL_INTERVAL_MS = 30_000;
/** Give up after this many tries; the raw upload stays in place and playable. */
const MAX_ATTEMPTS = 3;
/** Per tick. One at a time — ffmpeg competes with the API for CPU on this box. */
const MAX_PER_TICK = 3;

let timer: NodeJS.Timeout | null = null;
let tickRunning = false;

interface PendingRow {
  id: string;
  r2_key: string | null;
  poster_r2_key: string | null;
  parent_type: string;
  parent_id: string;
}

/**
 * Watermark text for a media row: the author's display name, but only when the
 * post opted in. Returns null for "no watermark".
 */
async function watermarkFor(row: PendingRow): Promise<string | null> {
  const pool = await getWriterPool();
  if (!pool) return null;
  const table = row.parent_type === 'article' ? 'articles' : 'media_posts';
  const r = await pool.query(
    `SELECT watermark, author_name FROM ${table} WHERE id = $1`,
    [row.parent_id],
  );
  const p = r.rows[0] as { watermark?: boolean; author_name?: string | null } | undefined;
  if (!p?.watermark) return null;
  const name = (p.author_name ?? '').trim();
  return name || null;
}

/** Record a failed attempt. After MAX_ATTEMPTS the row is parked as 'failed'. */
async function recordFailure(id: string, err: unknown): Promise<void> {
  const pool = await getWriterPool();
  if (!pool) return;
  const message = String((err as Error)?.message ?? err).slice(0, 500);
  await pool.query(
    `UPDATE wire_media
        SET process_attempts = process_attempts + 1,
            process_error    = $2,
            process_state    = CASE WHEN process_attempts + 1 >= $3 THEN 'failed' ELSE 'pending' END
      WHERE id = $1`,
    [id, message, MAX_ATTEMPTS],
  );
}

/**
 * Process one row end to end. Returns true if it was transcoded.
 *
 * The new object is written under a FRESH key rather than overwriting the old
 * one: that makes the swap atomic from a reader's point of view (the DB either
 * points at the raw file or the finished one) and stops any CDN cache serving a
 * half-written object.
 */
async function processRow(row: PendingRow): Promise<boolean> {
  if (!row.r2_key) {
    // Nothing to fetch — don't retry forever.
    await recordFailure(row.id, new Error('missing r2_key'));
    return false;
  }
  const dir = await mkdtemp(join(tmpdir(), 'wire-vid-'));
  const input = join(dir, 'in.mp4');
  const output = join(dir, 'out.mp4');
  const posterPath = join(dir, 'poster.jpg');
  const oldKey = row.r2_key;
  const oldPosterKey = row.poster_r2_key;

  try {
    if (!(await downloadR2ToFile(oldKey, input))) {
      throw new Error('R2 download failed');
    }

    const watermarkText = await watermarkFor(row);
    const meta = await transcodeVideo({ input, output, poster: posterPath, watermarkText });

    const newKey = newVideoKey();
    if (!(await uploadFileToR2(output, newKey, 'video/mp4'))) {
      throw new Error('R2 upload failed');
    }

    // Poster is best-effort: if it didn't come out, keep whatever the row had.
    let newPosterKey_: string | null = null;
    const posterKey = newPosterKey();
    if (await uploadFileToR2(posterPath, posterKey, 'image/jpeg')) {
      newPosterKey_ = posterKey;
    }

    const pool = await getWriterPool();
    if (!pool) throw new Error('no writer pool');
    await pool.query(
      `UPDATE wire_media
          SET r2_key           = $2,
              poster_r2_key    = COALESCE($3, poster_r2_key),
              width            = COALESCE($4, width),
              height           = COALESCE($5, height),
              duration_seconds = COALESCE($6, duration_seconds),
              process_state    = 'done',
              process_error    = NULL,
              processed_at     = now()
        WHERE id = $1`,
      [row.id, newKey, newPosterKey_, meta.width, meta.height, meta.durationSeconds],
    );

    // Only now is the raw upload redundant. Before the commit above, deleting it
    // would risk a row pointing at nothing.
    await deleteR2Object(oldKey);
    if (newPosterKey_ && oldPosterKey && oldPosterKey !== newPosterKey_) {
      await deleteR2Object(oldPosterKey);
    }

    log.info(
      { id: row.id, width: meta.width, height: meta.height, watermarked: meta.watermarked },
      'video: transcoded',
    );
    return true;
  } catch (err) {
    log.warn({ err: (err as Error)?.message, id: row.id }, 'video: transcode failed');
    await recordFailure(row.id, err);
    return false;
  } finally {
    await rm(dir, { recursive: true, force: true }).catch(() => {});
  }
}

/**
 * One sweep. Claims rows one at a time with FOR UPDATE SKIP LOCKED so a second
 * process (should one ever exist) can't pick up the same clip.
 */
export async function processPendingVideosOnce(): Promise<number> {
  if (!ffmpegAvailable() || !r2Configured()) return 0;
  const pool = await getWriterPool();
  if (!pool) return 0;

  let done = 0;
  for (let i = 0; i < MAX_PER_TICK; i++) {
    let row: PendingRow | undefined;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const r = await client.query(
        `SELECT id, r2_key, poster_r2_key, parent_type, parent_id
           FROM wire_media
          WHERE kind = 'video' AND process_state = 'pending'
            AND process_attempts < $1
          ORDER BY created_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED`,
        [MAX_ATTEMPTS],
      );
      row = r.rows[0] as PendingRow | undefined;
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      log.warn({ err: (err as Error)?.message }, 'video: claim failed');
      return done;
    } finally {
      client.release();
    }

    if (!row) break;
    if (await processRow(row)) done++;
  }
  return done;
}

/** Start the poller. Idempotent. */
export function startVideoProcessor(intervalMs: number = POLL_INTERVAL_MS): void {
  if (timer) return;
  if (!ffmpegAvailable()) {
    log.info('video processor: ffmpeg unavailable — uploads will be served as-is');
    return;
  }
  // First sweep shortly after boot so anything left 'pending' by a restart
  // (including one that killed a transcode mid-run) is picked straight back up.
  setTimeout(() => void tick(), 15_000).unref?.();
  timer = setInterval(() => void tick(), intervalMs);
  timer.unref?.();
}

async function tick(): Promise<void> {
  if (tickRunning) return;
  tickRunning = true;
  try {
    await processPendingVideosOnce();
  } catch (err) {
    log.warn({ err: (err as Error)?.message }, 'video processor: sweep failed');
  } finally {
    tickRunning = false;
  }
}

export function stopVideoProcessor(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
  tickRunning = false;
}
