/**
 * Node registry — DB access for the `nodes` + `node_call_stats` tables.
 *
 * Pure persistence. Live/ephemeral status (online, queue depth, spectrum)
 * lives in hub.ts and is merged on top by the API layer.
 */
import { getPool } from '../../db/pool.js';
import { randomUUID } from 'node:crypto';
import { notifyStaff } from '../staffNotify.js';

/** Feeder node types. Each has its own Go agent under feeder-nodes/. */
export const NODE_KINDS = ['radio', 'pager', 'adsb'] as const;
export type NodeKind = (typeof NODE_KINDS)[number];
export function isNodeKind(v: unknown): v is NodeKind {
  return typeof v === 'string' && (NODE_KINDS as readonly string[]).includes(v);
}

/** Feeder role names, one per node kind. */
export type FeederRole = 'feeder:radio' | 'feeder:pager' | 'feeder:adsb';

/**
 * The contributor role that gates a node KIND — the single definition.
 *
 * Three places ask this question and they MUST agree: the create/installer
 * gate (api/feeder.ts), the node-token gate every agent request passes
 * through (services/auth/nodeToken.ts), and the periodic WS auth
 * revalidation sweep (api/node-ws.ts). When the sweep hardcoded
 * feeder:radio, healthy pager nodes were admitted at the upgrade gate and
 * then cut ~60s later with close 4003 'role revoked', forever. Adding a kind
 * must therefore never mean editing three copies — hence this function lives
 * here (registry has no import cycle with any of its callers).
 */
export function roleForKind(kind: string): FeederRole {
  if (kind === 'pager') return 'feeder:pager';
  if (kind === 'adsb') return 'feeder:adsb';
  return 'feeder:radio';
}

/** Every feeder role, for allowlists that accept any contributor. */
export const FEEDER_ROLES: readonly FeederRole[] = [
  'feeder:radio',
  'feeder:pager',
  'feeder:adsb',
];

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
  // Required NSW RFS zone (coarse area) the node covers. Null only on legacy rows
  // created before zones existed; the API/UI require it going forward.
  // RADIO nodes only — pager nodes record state + lga instead.
  zone: string | null;
  // Australian state the node operates in (AU_STATES). Selects which state's
  // Pagermon the relay forwards into and which frequency plan the node gets.
  // Backfilled 'NSW' by migration 097; required on pager-node create.
  state: string | null;
  // Coarse locality (ABS LGA name, same vocabulary as the boundaries table).
  // Pager nodes only; display/attribution, not validated against the DB.
  lga: string | null;
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
  last_seen_at, notes, created_at, token_prefix, lat, lon, zone, state, lga`;

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
  loc: {
    zone: string | null;
    state: string | null;
    lga: string | null;
    /** Exact antenna position. Required at creation for adsb nodes (their
     *  decoder needs it to report range); null for the other kinds, which set
     *  it later via PUT /api/feeder/nodes/:id/location. */
    lat?: number | null;
    lon?: number | null;
  },
): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;
  const cleanName = clampMeta(name, 120) || `${kind}-node`;
  const res = await pool.query<NodeRow>(
    `INSERT INTO nodes (user_id, kind, name, token_hash, token_prefix, zone, state, lga, lat, lon)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
     RETURNING ${NODE_COLS}`,
    [
      userId, kind, cleanName, tokenHash, tokenPrefix,
      loc.zone, loc.state, loc.lga, loc.lat ?? null, loc.lon ?? null,
    ],
  );
  const row = res.rows[0] ?? null;
  if (row) {
    // Emitted here rather than in the two routes that call this, so neither
    // path can be missed. The node NAME is a slug of the owner's username
    // (see autoNodeName above), which is exactly what makes it useful here:
    // these land in a private staff channel, not anywhere public.
    const where = [loc.lga, loc.state, loc.zone].filter(Boolean).join(' \u00b7 ');
    notifyStaff(pool, {
      kind: 'new_node',
      event: 'new',
      ref: String(row.id ?? ''),
      title: `${kind} node`,
      subtitle: where || null,
      fields: [
        { name: 'Name', value: cleanName },
        { name: 'Kind', value: kind },
        { name: 'State', value: loc.state },
        { name: 'LGA', value: loc.lga },
        { name: 'Zone', value: loc.zone },
        { name: 'Key prefix', value: tokenPrefix },
      ],
    });
  }
  return row;
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
  let claim;
  try {
    claim = await pool.query(
      `UPDATE nodes SET install_id = $2 WHERE id = $1 AND install_id IS NULL`,
      [nodeId, installId],
    );
  } catch (e: unknown) {
    // 23505 on nodes_user_id_install_id_key: this machine is already TOFU-bound
    // to a DIFFERENT node row of the SAME user (the unique key is per-user, so
    // only own-account rows can conflict). That means this exact agent install
    // previously enrolled as another node (re-provisioned / re-created node) —
    // migrate the binding to the row this token belongs to. The copied-token
    // case (a different machine presenting a bound node's token) never lands
    // here: it fails the NULL-guarded claim without a constraint error and
    // falls through to the mismatch check below.
    if ((e as { code?: string })?.code === '23505') {
      const moved = await pool.query(
        `WITH freed AS (
           UPDATE nodes SET install_id = NULL
           WHERE install_id = $2
             AND user_id = (SELECT user_id FROM nodes WHERE id = $1)
             AND id <> $1
           RETURNING id
         )
         UPDATE nodes SET install_id = $2
         WHERE id = $1 AND install_id IS NULL AND EXISTS (SELECT 1 FROM freed)`,
        [nodeId, installId],
      );
      if ((moved.rowCount ?? 0) > 0) return 'bound';
      return 'mismatch';
    }
    throw e;
  }
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
 * Set a node's location plus the OPTIONAL exact antenna pin (lat/lon — null =
 * area only). Radio nodes set the RFS `zone`; pager nodes set `state` + `lga`.
 * A field left undefined keeps its stored value (so the pager path never
 * clears zone and vice versa). Staff/owner-visible only.
 */
export async function setNodeLocation(
  id: string,
  loc: {
    zone?: string | null;
    state?: string | null;
    lga?: string | null;
    lat?: number | null;
    lon?: number | null;
  },
): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;
  const res = await pool.query<NodeRow>(
    `UPDATE nodes SET
       zone  = CASE WHEN $2::boolean  THEN $3  ELSE zone  END,
       state = CASE WHEN $4::boolean  THEN $5  ELSE state END,
       lga   = CASE WHEN $6::boolean  THEN $7  ELSE lga   END,
       lat   = CASE WHEN $8::boolean  THEN $9  ELSE lat   END,
       lon   = CASE WHEN $10::boolean THEN $11 ELSE lon   END
     WHERE id = $1 RETURNING ${NODE_COLS}`,
    [
      id,
      loc.zone !== undefined, loc.zone ?? null,
      loc.state !== undefined, loc.state ?? null,
      loc.lga !== undefined, loc.lga ?? null,
      loc.lat !== undefined, loc.lat ?? null,
      loc.lon !== undefined, loc.lon ?? null,
    ],
  );
  return res.rows[0] ?? null;
}

/**
 * Set a pager node's single-SDR primary frequency preference, merged into the
 * JSONB config_override (so it persists across restarts/updates and other
 * override keys are untouched). Guarded to kind='pager'. Returns the updated
 * row. Label validity (against the node's STATE's plan) is the API layer's job.
 */
export async function setPagerPrimary(
  id: string,
  primary: string,
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

/**
 * Set (or clear) a pager node's tuner-gain / ppm overrides, merged into the
 * JSONB config_override so they persist across restarts/updates and don't touch
 * other keys (e.g. pagerPrimary). Pass a value to set it, `null` to clear it,
 * or `undefined` to leave that key untouched. Guarded to kind='pager'.
 */
export async function setPagerTuning(
  id: string,
  patch: { gain?: string | null; ppm?: number | null },
): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;

  const setObj: Record<string, unknown> = {};
  const removeKeys: string[] = [];
  if (patch.gain !== undefined) {
    if (patch.gain === null || patch.gain === '') removeKeys.push('pagerGain');
    else setObj['pagerGain'] = patch.gain;
  }
  if (patch.ppm !== undefined) {
    if (patch.ppm === null) removeKeys.push('pagerPpm');
    else setObj['pagerPpm'] = patch.ppm;
  }

  // (base - removeKeys[]) || setObj: remove cleared keys, then merge set keys.
  const res = await pool.query<NodeRow>(
    `UPDATE nodes
       SET config_override = (COALESCE(config_override, '{}'::jsonb) - $2::text[]) || $3::jsonb
     WHERE id = $1 AND kind = 'pager'
     RETURNING ${NODE_COLS}`,
    [id, removeKeys, JSON.stringify(setObj)],
  );
  return res.rows[0] ?? null;
}

/**
 * Set (or clear) an ADS-B node's gain / ppm overrides, merged into the JSONB
 * config_override so they persist across restarts/updates without touching
 * other keys. Pass a value to set it, `null` to clear it, or `undefined` to
 * leave that key untouched. Guarded to kind='adsb'.
 *
 * Gain and ppm are the ONLY tunables: everything else about the decoder is
 * fixed policy, and the antenna position comes from the node's own lat/lon
 * columns rather than an override.
 */
export async function setAdsbTuning(
  id: string,
  patch: { gain?: string | null; ppm?: number | null },
): Promise<NodeRow | null> {
  const pool = await getPool();
  if (!pool) return null;

  const setObj: Record<string, unknown> = {};
  const removeKeys: string[] = [];
  if (patch.gain !== undefined) {
    if (patch.gain === null || patch.gain === '') removeKeys.push('adsbGain');
    else setObj['adsbGain'] = patch.gain;
  }
  if (patch.ppm !== undefined) {
    if (patch.ppm === null) removeKeys.push('adsbPpm');
    else setObj['adsbPpm'] = patch.ppm;
  }

  const res = await pool.query<NodeRow>(
    `UPDATE nodes
       SET config_override = (COALESCE(config_override, '{}'::jsonb) - $2::text[]) || $3::jsonb
     WHERE id = $1 AND kind = 'adsb'
     RETURNING ${NODE_COLS}`,
    [id, removeKeys, JSON.stringify(setObj)],
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
