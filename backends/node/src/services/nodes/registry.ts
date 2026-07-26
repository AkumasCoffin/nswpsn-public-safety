/**
 * Node registry — DB access for the `nodes` + `node_call_stats` tables.
 *
 * Pure persistence. Live/ephemeral status (online, queue depth, spectrum)
 * lives in hub.ts and is merged on top by the API layer.
 */
import { getPool } from '../../db/pool.js';
import { randomUUID } from 'node:crypto';

/** Feeder node types. Only 'radio' has a working agent today; 'pager'/'adsb'
 *  are provisionable now so their future agents slot in. */
export const NODE_KINDS = ['radio', 'pager', 'adsb'] as const;
export type NodeKind = (typeof NODE_KINDS)[number];
export function isNodeKind(v: unknown): v is NodeKind {
  return typeof v === 'string' && (NODE_KINDS as readonly string[]).includes(v);
}

/** Auto-generated node name: `{kind}-{userslug}-{short-uuid}` (e.g.
 *  radio-akumascoffin-a3f9c2d1). Unique + self-describing so operators don't
 *  have to name each node. */
export function autoNodeName(kind: string, username: string | null): string {
  const slug =
    (username || 'user')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 24) || 'user';
  return `${kind}-${slug}-${randomUUID().slice(0, 8)}`;
}

export interface NodeRow {
  id: string;
  kind: string;
  user_id: string;
  // NULL until the node's agent first connects (TOFU-bound then).
  install_id: string | null;
  name: string;
  enabled: boolean;
  feed_enabled: boolean;
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
  // Lookup prefix of this node's token (for UI display / logs). The hash is
  // never selected into API paths.
  token_prefix: string | null;
  // Exact antenna location (used for coverage calculation + channel tuning).
  // Optional; null = unset. Visible to the operator + staff only, never public.
  lat: number | null;
  lon: number | null;
}

export interface HelloMeta {
  agentVersion?: string | null;
  sdrtrunkVersion?: string | null;
  rdioVersion?: string | null;
  os?: string | null;
  arch?: string | null;
  hostname?: string | null;
}

const NODE_COLS = `id, kind, user_id, install_id, name, enabled, feed_enabled, config_override,
  config_version, agent_version, sdrtrunk_version, rdio_version, os, arch,
  last_seen_at, notes, created_at, token_prefix, lat, lon`;

/** Max distinct installs (nodes) one contributor may register. `install_id` is
 *  an attacker-chosen header, so without a cap a single token could create
 *  unbounded rows. A real volunteer runs a handful of machines. */
export const MAX_NODES_PER_USER = 25;

/** Count a user's existing node rows (for the per-user cap check). */
export async function countNodesForUser(userId: string): Promise<number> {
  const pool = await getPool();
  if (!pool) return 0;
  const res = await pool.query<{ n: string }>(
    'SELECT COUNT(*)::text AS n FROM nodes WHERE user_id = $1',
    [userId],
  );
  return Number(res.rows[0]?.n ?? 0);
}

// Clamp an agent-supplied metadata string so a hostile hello can't store an
// oversized blob (the frame cap is 1 MB; DB columns don't need all of it).
function clampMeta(s: string | undefined | null, max: number): string | null {
  if (s == null) return null;
  const t = String(s).slice(0, max);
  return t.length ? t : null;
}

/**
 * Create a pre-created node (name + type) with its own token. `install_id` is
 * NULL until the node's agent first connects and binds it (TOFU). Returns the
 * new row.
 */
export async function createNode(
  userId: string,
  name: string,
  kind: string,
  tokenHash: string,
  tokenPrefix: string,
): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;
  const cleanName = clampMeta(name, 120) || `${kind}-node`;
  const res = await pool.query<NodeRow>(
    `INSERT INTO nodes (user_id, kind, name, token_hash, token_prefix)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING ${NODE_COLS}`,
    [userId, kind, cleanName, tokenHash, tokenPrefix],
  );
  return res.rows[0] ?? null;
}

export type BindResult = 'bound' | 'match' | 'mismatch';

/**
 * TOFU-bind a machine to a node on first connect. If the node has no install_id
 * yet, set it and return 'bound'; if it already equals installId, 'match'; if a
 * DIFFERENT machine already bound this node's token, 'mismatch' (reject — a
 * copied token). Rotation resets the binding (see rotateNodeToken).
 */
export async function bindInstallId(nodeId: string, installId: string): Promise<BindResult> {
  const pool = await getPool();
  if (!pool) return 'mismatch';
  // Atomic claim: only sets install_id when it's currently NULL.
  const claim = await pool.query(
    `UPDATE nodes SET install_id = $2 WHERE id = $1 AND install_id IS NULL`,
    [nodeId, installId],
  );
  if ((claim.rowCount ?? 0) > 0) return 'bound';
  const cur = await pool.query<{ install_id: string | null }>(
    `SELECT install_id FROM nodes WHERE id = $1`,
    [nodeId],
  );
  return cur.rows[0]?.install_id === installId ? 'match' : 'mismatch';
}

/**
 * Rotate a node's token: store the new hash/prefix and CLEAR the TOFU binding
 * (install_id → NULL) so a re-provisioned/replaced machine can bind afresh.
 */
export async function rotateNodeToken(
  nodeId: string,
  tokenHash: string,
  tokenPrefix: string,
): Promise<boolean> {
  const pool = await getPool();
  if (!pool) return false;
  const res = await pool.query(
    `UPDATE nodes
       SET token_hash = $2, token_prefix = $3, token_rotated_at = now(), install_id = NULL
     WHERE id = $1`,
    [nodeId, tokenHash, tokenPrefix],
  );
  return (res.rowCount ?? 0) > 0;
}

/**
 * Refresh an EXISTING node's versions/os/arch/seen-time from a hello. No
 * auto-create — the node is pre-created and resolved by its token.
 */
export async function refreshNodeOnHello(nodeId: string, meta: HelloMeta): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;
  const res = await pool.query<NodeRow>(
    `UPDATE nodes SET
       agent_version    = COALESCE($2, agent_version),
       sdrtrunk_version = COALESCE($3, sdrtrunk_version),
       rdio_version     = COALESCE($4, rdio_version),
       os               = COALESCE($5, os),
       arch             = COALESCE($6, arch),
       last_seen_at     = now()
     WHERE id = $1
     RETURNING ${NODE_COLS}`,
    [
      nodeId,
      clampMeta(meta.agentVersion, 40),
      clampMeta(meta.sdrtrunkVersion, 40),
      clampMeta(meta.rdioVersion, 40),
      clampMeta(meta.os, 40),
      clampMeta(meta.arch, 20),
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
  // No `name` — node names are always auto-generated and never renamable.
  enabled?: boolean;
  feed_enabled?: boolean;
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
  if (patch.enabled !== undefined) { sets.push(`enabled = $${i++}`); vals.push(patch.enabled); }
  if (patch.feed_enabled !== undefined) { sets.push(`feed_enabled = $${i++}`); vals.push(patch.feed_enabled); }
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

/**
 * Set (or clear, with nulls) a node's exact antenna location — used for
 * coverage calculation and channel tuning. Optional; staff/owner-visible only.
 */
export async function setNodeLocation(
  id: string,
  lat: number | null,
  lon: number | null,
): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;
  const res = await pool.query<NodeRow>(
    `UPDATE nodes SET lat = $2, lon = $3 WHERE id = $1 RETURNING ${NODE_COLS}`,
    [id, lat, lon],
  );
  return res.rows[0] ?? null;
}

/**
 * Set a pager node's single-SDR primary frequency preference, merged into the
 * JSONB config_override (so it persists across restarts/updates and other
 * override keys are untouched). Guarded to kind='pager'. Returns the updated row.
 */
export async function setPagerPrimary(
  id: string,
  primary: 'NSWRFS' | 'FRNSW',
): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;
  const res = await pool.query<NodeRow>(
    `UPDATE nodes
       SET config_override = COALESCE(config_override, '{}'::jsonb)
                             || jsonb_build_object('pagerPrimary', $2::text)
     WHERE id = $1 AND kind = 'pager'
     RETURNING ${NODE_COLS}`,
    [id, primary],
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
