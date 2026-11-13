/**
 * Once a day, fill gaps in the stored aircraft history from adsb.lol's
 * globe_history archive (the import itself: adsbGlobeHistoryImport.ts).
 *
 * The archive only exists per UTC day, published as a GitHub release about
 * 14:30 AEDT the next day, so there is nothing to fetch more often. This
 * checks hourly whether a day in the last LOOKBACK_DAYS has been released and
 * not yet imported, and imports the newest such day — one per check, so after
 * an outage the backlog is caught up a day an hour rather than all at once.
 *
 * Each import downloads the whole release (~3.7 GB, the world's traffic for
 * the day), which is the cost of using it: ~110 GB a month. Set
 * ADSB_HISTORY_IMPORT=false to turn it off; it is also off with ADSB_DISABLED,
 * which means "don't use the public aggregators".
 */
import { Worker } from 'node:worker_threads';
import { config } from '../config.js';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { DATA_RETENTION_DAYS } from '../lib/retention.js';
import { fetchJson } from '../sources/shared/http.js';
import {
  DAY_MS,
  GLOBE_HISTORY_DONE_SEQ,
  GLOBE_HISTORY_SEQ,
  type GlobeImportResult,
} from './adsbGlobeHistoryImport.js';

const CHECK_EVERY_MS = 60 * 60_000;
/** First check after boot: out of the way of startup and the first polls. */
const FIRST_CHECK_MS = 10 * 60_000;
/** How far back a missed day is still worth a 3.7 GB download. */
const LOOKBACK_DAYS = 7;
/** A day whose import failed is not tried again for this long. */
const RETRY_AFTER_FAIL_MS = 6 * 60 * 60_000;
/** Generous: a slow download of the whole release plus the scan. */
const IMPORT_TIMEOUT_MS = 90 * 60_000;

const TAG_RE = /^v(\d{4})\.(\d{2})\.(\d{2})-planes-readsb-prod-(\d+)$/;
const ASSET_RE = /\.tar(?:\.[a-z]{2})?$/;

export interface GhRelease {
  tag_name: string;
  assets: Array<{ name: string; browser_download_url: string }>;
}

/** UTC day starts from yesterday back LOOKBACK_DAYS, newest first, inside retention. */
export function candidateDays(nowMs: number, retentionDays: number = DATA_RETENTION_DAYS): number[] {
  const today = Math.floor(nowMs / DAY_MS) * DAY_MS;
  const oldest = nowMs - retentionDays * DAY_MS;
  const out: number[] = [];
  for (let d = 1; d <= LOOKBACK_DAYS; d++) {
    const start = today - d * DAY_MS;
    if (start >= oldest) out.push(start);
  }
  return out;
}

/** The download URLs of a day's prod release, parts in order; null if not out. */
export function releaseAssets(releases: readonly GhRelease[], dayStartMs: number): string[] | null {
  let best: { n: number; urls: string[] } | null = null;
  for (const r of releases) {
    const m = TAG_RE.exec(r.tag_name);
    if (!m) continue;
    if (Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])) !== dayStartMs) continue;
    const urls = r.assets
      .filter((a) => ASSET_RE.test(a.name))
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((a) => a.browser_download_url);
    const n = Number(m[4]);
    // A re-run of the same day is published as -1, -2…: take the latest.
    if (urls.length > 0 && (!best || n > best.n)) best = { n, urls };
  }
  return best?.urls ?? null;
}

async function listReleases(year: number): Promise<GhRelease[]> {
  return fetchJson<GhRelease[]>(
    `https://api.github.com/repos/adsblol/globe_history_${year}/releases?per_page=30`,
    { timeoutMs: 20_000, headers: { Accept: 'application/vnd.github+json' } },
  );
}

async function importedDays(days: readonly number[]): Promise<Set<number>> {
  const pool = await getPool();
  if (!pool) return new Set(days); // nowhere to write: treat as done
  // Done: the marker row, written after the last hour. Days imported before
  // the marker had its own seq have none, so a day whose LAST hour holds
  // imported rows also counts: a run cut short never reached that hour.
  const r = await pool.query<{ day: Date }>(
    `SELECT d.day
       FROM unnest($1::timestamptz[]) AS d(day)
      WHERE EXISTS (SELECT 1 FROM adsb_track_chunks c
                     WHERE c.minute = d.day AND c.seq = $2)
         OR EXISTS (SELECT 1 FROM adsb_track_chunks c
                     WHERE c.seq = $3
                       AND c.minute >= d.day + interval '23 hours'
                       AND c.minute <  d.day + interval '24 hours')`,
    [days.map((d) => new Date(d).toISOString()), GLOBE_HISTORY_DONE_SEQ, GLOBE_HISTORY_SEQ],
  );
  return new Set(r.rows.map((x) => x.day.getTime()));
}

let timer: NodeJS.Timeout | null = null;
let worker: Worker | null = null;
let running = false;
const failedAt = new Map<number, number>();

function runWorker(dayStartMs: number, assetUrls: string[]): Promise<GlobeImportResult> {
  return new Promise((resolve, reject) => {
    const w = new Worker(new URL('./adsbGlobeHistoryWorker.js', import.meta.url), {
      workerData: { dayStartMs, assetUrls },
      resourceLimits: { maxOldGenerationSizeMb: 1536 },
    });
    worker = w;
    const kill = setTimeout(() => void w.terminate(), IMPORT_TIMEOUT_MS);
    let settled = false;
    w.on('message', (msg: { ok: boolean; result?: GlobeImportResult; error?: string }) => {
      settled = true;
      if (msg.ok && msg.result) resolve(msg.result);
      else reject(new Error(msg.error ?? 'import failed'));
    });
    w.on('error', (err) => {
      settled = true;
      reject(err);
    });
    w.on('exit', (code) => {
      clearTimeout(kill);
      worker = null;
      if (!settled) reject(new Error(`import worker exited with code ${code}`));
    });
  });
}

async function checkOnce(nowMs: number = Date.now()): Promise<void> {
  const days = candidateDays(nowMs);
  if (days.length === 0) return;
  const done = await importedDays(days);
  const pending = days.filter(
    (d) => !done.has(d) && nowMs - (failedAt.get(d) ?? -Infinity) >= RETRY_AFTER_FAIL_MS,
  );
  if (pending.length === 0) return;

  const years = Array.from(new Set(pending.map((d) => new Date(d).getUTCFullYear())));
  const releases = (await Promise.all(years.map((y) => listReleases(y).catch(() => [])))).flat();
  for (const day of pending) {
    const urls = releaseAssets(releases, day);
    if (!urls) continue;
    const dayIso = new Date(day).toISOString().slice(0, 10);
    log.info({ day: dayIso, parts: urls.length }, 'adsb globe history: importing');
    const t0 = Date.now();
    try {
      const r = await runWorker(day, urls);
      failedAt.delete(day);
      log.info({ day: dayIso, ...r, secs: Math.round((Date.now() - t0) / 1000) }, 'adsb globe history: imported');
    } catch (err) {
      failedAt.set(day, Date.now());
      log.warn({ day: dayIso, err: (err as Error).message }, 'adsb globe history: import failed');
    }
    return; // one day per check
  }
}

export function startAdsbGlobeHistory(): void {
  if (timer || !config.ADSB_HISTORY_IMPORT || config.ADSB_DISABLED) return;
  const tick = (): void => {
    if (running) return;
    running = true;
    checkOnce()
      .catch((err) => log.warn({ err: (err as Error).message }, 'adsb globe history: check failed'))
      .finally(() => {
        running = false;
      });
  };
  const loop = (): void => {
    tick();
    timer = setTimeout(loop, CHECK_EVERY_MS);
    timer.unref?.();
  };
  timer = setTimeout(loop, FIRST_CHECK_MS);
  timer.unref?.();
}

export async function stopAdsbGlobeHistory(): Promise<void> {
  if (timer) clearTimeout(timer);
  timer = null;
  if (worker) await worker.terminate();
}
