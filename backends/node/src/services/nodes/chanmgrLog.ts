// Persistence for the automatic channel manager's audit log.
//
// The radio agent keeps its last 50 decisions (auto-stops, probe verdicts,
// restores/releases) in a ring that rides every status frame. This module
// folds that ring into node_chanmgr_log so the history survives agent
// restarts and shows for offline nodes, and serves it back to the staff
// node page. The ring is tiny and repeats itself every 15s, so ingest is
// gated on a per-node high-water mark and writes land ON CONFLICT DO NOTHING.
import { getPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';

/** Same cap as the agent's ring — the table mirrors it per node. */
const LOG_CAP = 50;

/** One manager decision, as shipped in status.channelManager.log. */
export interface ChanMgrLogEntry {
  atMs: number;
  channel: string;
  kind: string;
  text: string;
}

/** Newest at_ms already persisted, per node — skips the no-news frames.
 *  In-memory only: after a backend restart the first frame re-offers the whole
 *  ring and the PK conflict rule makes that a cheap no-op. */
const highWater = new Map<string, number>();

/** Exported for tests. */
export function parseRing(channelManager: unknown): ChanMgrLogEntry[] {
  if (!channelManager || typeof channelManager !== 'object') return [];
  const raw = (channelManager as { log?: unknown }).log;
  if (!Array.isArray(raw)) return [];
  const out: ChanMgrLogEntry[] = [];
  for (const e of raw) {
    if (!e || typeof e !== 'object') continue;
    const { atMs, channel, kind, text } = e as Record<string, unknown>;
    if (typeof atMs !== 'number' || !Number.isFinite(atMs)) continue;
    if (typeof channel !== 'string' || typeof kind !== 'string') continue;
    out.push({
      atMs: Math.trunc(atMs),
      channel: channel.slice(0, 200),
      kind: kind.slice(0, 40),
      text: typeof text === 'string' ? text.slice(0, 500) : '',
    });
  }
  return out;
}

/**
 * Folds a status frame's channel-manager ring into the table. Fire-and-forget
 * from the WS status path — never throws.
 */
export async function ingestChanMgrLog(nodeId: string, channelManager: unknown): Promise<void> {
  try {
    const ring = parseRing(channelManager);
    if (ring.length === 0) return;
    const seen = highWater.get(nodeId) ?? 0;
    const fresh = ring.filter((e) => e.atMs > seen);
    if (fresh.length === 0) return;
    const pool = await getPool();
    if (!pool) return;

    const values: string[] = [];
    const params: unknown[] = [nodeId];
    fresh.forEach((e) => {
      const base = params.length;
      params.push(e.atMs, e.channel, e.kind, e.text);
      values.push(`($1, $${base + 1}, $${base + 2}, $${base + 3}, $${base + 4})`);
    });
    await pool.query(
      `INSERT INTO node_chanmgr_log (node_id, at_ms, channel, kind, text)
       VALUES ${values.join(',')}
       ON CONFLICT DO NOTHING`,
      params,
    );
    // Trim to the cap only on frames that actually inserted — the common
    // frame (no news) never touches the table at all.
    await pool.query(
      `DELETE FROM node_chanmgr_log
        WHERE node_id = $1
          AND at_ms < COALESCE((
            SELECT MIN(at_ms) FROM (
              SELECT at_ms FROM node_chanmgr_log
               WHERE node_id = $1 ORDER BY at_ms DESC LIMIT ${LOG_CAP}
            ) newest
          ), 0)`,
      [nodeId],
    );
    highWater.set(nodeId, Math.max(...fresh.map((e) => e.atMs)));
  } catch (err) {
    log.warn({ err, nodeId }, 'chanmgr log ingest failed');
  }
}

/** The newest entries for one node, newest first. */
export async function listChanMgrLog(nodeId: string): Promise<ChanMgrLogEntry[]> {
  const pool = await getPool();
  if (!pool) return [];
  const r = await pool.query<{ at_ms: string; channel: string; kind: string; text: string }>(
    `SELECT at_ms, channel, kind, text FROM node_chanmgr_log
      WHERE node_id = $1 ORDER BY at_ms DESC LIMIT ${LOG_CAP}`,
    [nodeId],
  );
  return r.rows.map((row) => ({
    atMs: Number(row.at_ms),
    channel: row.channel,
    kind: row.kind,
    text: row.text,
  }));
}

/** Test hook: clears the per-node high-water marks. */
export function _resetChanMgrLogCache(): void {
  highWater.clear();
}
