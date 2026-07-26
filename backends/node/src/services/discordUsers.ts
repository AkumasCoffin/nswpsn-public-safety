/**
 * Resolve Discord user IDs (snowflakes) → display names via the Discord API, so
 * the admin user panel can show a name even for accounts that only supplied a
 * Discord ID (no OAuth-linked username in metadata).
 *
 * Names are cached in memory (they rarely change) and resolved LAZILY in the
 * background: `getCachedDiscordName()` returns immediately from cache (or null),
 * and any unknown id passed to `queueDiscordResolve()` is fetched by a single
 * throttled worker so the NEXT page load shows it. This keeps /api/users fast and
 * stays well under Discord's rate limits. Needs DISCORD_BOT_TOKEN — a no-op
 * without it. `GET /users/{id}` with a bot token works for ANY user id (no shared
 * guild required).
 */
import { config } from '../config.js';
import { log } from '../lib/log.js';

interface Entry {
  name: string | null;
  at: number;
}

const cache = new Map<string, Entry>();
const queue: string[] = [];
const queued = new Set<string>();
let workerRunning = false;

const TTL_MS = 24 * 60 * 60 * 1000; // re-resolve a known name once a day
const NEG_TTL_MS = 6 * 60 * 60 * 1000; // retry an unknown/failed id less often
const THROTTLE_MS = 300; // ~3 req/s — safely under Discord's limits
const SNOWFLAKE = /^\d{5,25}$/;

/** Cached Discord display name for an id, or null if not (yet) resolved. */
export function getCachedDiscordName(id: string | null | undefined): string | null {
  if (!id) return null;
  return cache.get(id)?.name ?? null;
}

/** Queue an id for background resolution if it's not cached (or is stale). */
export function queueDiscordResolve(id: string | null | undefined): void {
  if (!id || !SNOWFLAKE.test(id) || !config.DISCORD_BOT_TOKEN) return;
  const e = cache.get(id);
  if (e) {
    const fresh = e.name ? Date.now() - e.at < TTL_MS : Date.now() - e.at < NEG_TTL_MS;
    if (fresh) return;
  }
  if (queued.has(id)) return;
  queued.add(id);
  queue.push(id);
  startWorker();
}

function startWorker(): void {
  if (workerRunning) return;
  workerRunning = true;
  void (async () => {
    try {
      while (queue.length > 0) {
        const id = queue.shift()!;
        queued.delete(id);
        await resolveOne(id);
        await sleep(THROTTLE_MS);
      }
    } finally {
      workerRunning = false;
      if (queue.length > 0) startWorker(); // drain anything enqueued meanwhile
    }
  })();
}

async function resolveOne(id: string): Promise<void> {
  const token = config.DISCORD_BOT_TOKEN;
  if (!token) return;
  try {
    const res = await fetch(`https://discord.com/api/v10/users/${id}`, {
      headers: { Authorization: `Bot ${token}` },
    });
    if (res.status === 429) {
      // Rate limited — back off per retry_after and requeue.
      const body = (await res.json().catch(() => ({}))) as { retry_after?: number };
      const retry = Math.max(1000, Math.round((body.retry_after ?? 1) * 1000));
      log.warn(`discord: rate limited resolving user, backing off ${retry}ms`);
      await sleep(retry);
      queued.add(id);
      queue.push(id);
      return;
    }
    if (!res.ok) {
      // 404 unknown id / 401 bad token / other — negative-cache to avoid hammering.
      cache.set(id, { name: null, at: Date.now() });
      if (res.status === 401) log.warn('discord: 401 resolving user (check DISCORD_BOT_TOKEN)');
      return;
    }
    const u = (await res.json()) as { username?: string; global_name?: string | null };
    const name = (u.global_name || u.username || '').trim() || null;
    cache.set(id, { name, at: Date.now() });
  } catch (err) {
    cache.set(id, { name: null, at: Date.now() });
    log.warn({ err }, `discord: failed to resolve user ${id}`);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
