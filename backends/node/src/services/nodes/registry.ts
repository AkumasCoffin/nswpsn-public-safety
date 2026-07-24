/**
 * Node registry — DB access for the `nodes` + `node_call_stats` tables.
 *
 * Pure persistence. Live/ephemeral status (online, queue depth, spectrum)
 * lives in hub.ts and is merged on top by the API layer.
 */
import { getPool } from '../../db/pool.js';

export interface NodeRow {
  id: string;
  kind: string;
  user_id: string;
  install_id: string;
  name: string;
  enabled: boolean;
  config_override: Record<string, unknown>;
  config_version: string | null;
  agent_version: string | null;
  sdrtrunk_version: string | null;
  rdio_version: string | null;
  os: string | null;
  arch: string | null;
  last_seen_at: string | null;
  notes: string | null;
  created_at: string;
}

export interface HelloMeta {
  agentVersion?: string | null;
  sdrtrunkVersion?: string | null;
  rdioVersion?: string | null;
  os?: string | null;
  arch?: string | null;
  hostname?: string | null;
}

const NODE_COLS = `id, kind, user_id, install_id, name, enabled, config_override,
  config_version, agent_version, sdrtrunk_version, rdio_version, os, arch,
  last_seen_at, notes, created_at`;

/**
 * Create-or-update a node from an agent's hello. Keyed by (user_id,
 * install_id): the first hello inserts, later ones refresh versions + seen
 * time. This is the "auto-link on start" step. A brand-new row gets a
 * default name derived from the hostname (editable later by staff).
 */
export async function upsertNodeOnHello(
  userId: string,
  installId: string,
  meta: HelloMeta,
): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;
  const defaultName =
    (meta.hostname && meta.hostname.trim()) ||
    `radio-${installId.slice(0, 8)}`;
  const res = await pool.query<NodeRow>(
    `INSERT INTO nodes
       (user_id, install_id, kind, name, agent_version, sdrtrunk_version,
        rdio_version, os, arch, last_seen_at)
     VALUES ($1, $2, 'radio', $3, $4, $5, $6, $7, $8, now())
     ON CONFLICT (user_id, install_id) DO UPDATE
       SET agent_version    = COALESCE(EXCLUDED.agent_version, nodes.agent_version),
           sdrtrunk_version = COALESCE(EXCLUDED.sdrtrunk_version, nodes.sdrtrunk_version),
           rdio_version     = COALESCE(EXCLUDED.rdio_version, nodes.rdio_version),
           os               = COALESCE(EXCLUDED.os, nodes.os),
           arch             = COALESCE(EXCLUDED.arch, nodes.arch),
           last_seen_at     = now()
     RETURNING ${NODE_COLS}`,
    [
      userId,
      installId,
      defaultName,
      meta.agentVersion ?? null,
      meta.sdrtrunkVersion ?? null,
      meta.rdioVersion ?? null,
      meta.os ?? null,
      meta.arch ?? null,
    ],
  );
  return res.rows[0] ?? null;
}

export async function getNodeByInstall(
  userId: string,
  installId: string,
): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;
  const res = await pool.query<NodeRow>(
    `SELECT ${NODE_COLS} FROM nodes WHERE user_id = $1 AND install_id = $2`,
    [userId, installId],
  );
  return res.rows[0] ?? null;
}

export async function getNode(id: string): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;
  const res = await pool.query<NodeRow>(
    `SELECT ${NODE_COLS} FROM nodes WHERE id = $1`,
    [id],
  );
  return res.rows[0] ?? null;
}

export async function listNodes(): Promise<NodeRow[]> {
  const pool = await getPool();
  if (!pool) return [];
  const res = await pool.query<NodeRow>(
    `SELECT ${NODE_COLS} FROM nodes ORDER BY created_at ASC`,
  );
  return res.rows;
}

export async function listNodesForUser(userId: string): Promise<NodeRow[]> {
  const pool = await getPool();
  if (!pool) return [];
  const res = await pool.query<NodeRow>(
    `SELECT ${NODE_COLS} FROM nodes WHERE user_id = $1 ORDER BY created_at ASC`,
    [userId],
  );
  return res.rows;
}

export interface NodePatch {
  name?: string;
  enabled?: boolean;
  config_override?: Record<string, unknown>;
  config_version?: string | null;
  notes?: string;
}

export async function updateNode(
  id: string,
  patch: NodePatch,
): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;
  const sets: string[] = [];
  const vals: unknown[] = [];
  let i = 1;
  if (patch.name !== undefined) { sets.push(`name = $${i++}`); vals.push(patch.name); }
  if (patch.enabled !== undefined) { sets.push(`enabled = $${i++}`); vals.push(patch.enabled); }
  if (patch.config_override !== undefined) {
    sets.push(`config_override = $${i++}`);
    vals.push(JSON.stringify(patch.config_override));
  }
  if (patch.config_version !== undefined) {
    sets.push(`config_version = $${i++}`);
    vals.push(patch.config_version);
  }
  if (patch.notes !== undefined) { sets.push(`notes = $${i++}`); vals.push(patch.notes); }
  if (sets.length === 0) return getNode(id);
  vals.push(id);
  const res = await pool.query<NodeRow>(
    `UPDATE nodes SET ${sets.join(', ')} WHERE id = $${i} RETURNING ${NODE_COLS}`,
    vals,
  );
  return res.rows[0] ?? null;
}

export async function touchNodeSeen(id: string): Promise<void> {
  const pool = await getPool();
  if (!pool) return;
  await pool.query('UPDATE nodes SET last_seen_at = now() WHERE id = $1', [id]);
}

export async function deleteNode(id: string): Promise<boolean> {
  const pool = await getPool();
  if (!pool) return false;
  const res = await pool.query('DELETE FROM nodes WHERE id = $1', [id]);
  return (res.rowCount ?? 0) > 0;
}

/** Increment today's call rollup for a node (relay path). */
export async function bumpNodeCallStat(id: string, bytes: number): Promise<void> {
  const pool = await getPool();
  if (!pool) return;
  await pool.query(
    `INSERT INTO node_call_stats (node_id, day, calls, bytes)
     VALUES ($1, CURRENT_DATE, 1, $2)
     ON CONFLICT (node_id, day) DO UPDATE
       SET calls = node_call_stats.calls + 1,
           bytes = node_call_stats.bytes + EXCLUDED.bytes`,
    [id, Math.max(0, Math.floor(bytes))],
  );
}

export interface NodeStatDay { day: string; calls: number; bytes: number; }

export async function getNodeStats(
  id: string,
  days: number,
): Promise<NodeStatDay[]> {
  const pool = await getPool();
  if (!pool) return [];
  const res = await pool.query<NodeStatDay>(
    `SELECT to_char(day, 'YYYY-MM-DD') AS day, calls, bytes
       FROM node_call_stats
      WHERE node_id = $1 AND day >= CURRENT_DATE - ($2::int - 1)
      ORDER BY day ASC`,
    [id, Math.max(1, days)],
  );
  return res.rows;
}
