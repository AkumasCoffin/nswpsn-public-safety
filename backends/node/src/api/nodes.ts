/**
 * Staff feeder-node management (owner|dev only).
 *
 * The DB-persisted node registry (registry.ts) plus live/ephemeral status
 * from the in-memory hub (hub.ts) merged on top. Every route is gated with
 * requireRole(canManageNodes) — the public NSWPSN_API_KEY alone can't reach
 * these, only a logged-in owner or dev.
 *
 *   GET    /api/nodes
 *   GET    /api/nodes/:id
 *   PATCH  /api/nodes/:id
 *   POST   /api/nodes/:id/cmd
 *   GET    /api/nodes/:id/config
 *   GET    /api/nodes/:id/stats
 *   POST   /api/nodes/users/:userId/rotate-feeder-token
 *   DELETE /api/nodes/:id
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { log } from '../lib/log.js';
import { requireRole, canManageNodes, isOwner } from '../services/auth/roles.js';
import {
  listNodes,
  getNode,
  updateNode,
  deleteNode,
  getNodeStats,
  type NodeRow,
  type NodePatch,
} from '../services/nodes/registry.js';
import { hub } from '../services/nodes/hub.js';
import { ConfigOverrideSchema } from '../services/nodes/configSchema.js';
import { buildConfigPayload } from '../services/nodes/configMerge.js';
import { pushConfigToNode, pushConfigToAllNodes } from '../services/nodes/configPush.js';
import {
  getGlobalConfig,
  saveGlobalConfig,
  GlobalConfigSchema,
  getAutoUpdate,
  setAutoUpdate,
} from '../services/nodes/globalConfig.js';
import {
  feederTokensConfigured,
  rotateFeederToken,
} from '../services/auth/nodeToken.js';

export const nodesRouter = new Hono();

/**
 * Map a DB NodeRow to a clean camelCase JSON shape, merging the live
 * hub status (online / last status frame / when it arrived) on top.
 */
function toApi(node: NodeRow) {
  const live = hub.liveStatus(node.id);
  return {
    id: node.id,
    kind: node.kind,
    userId: node.user_id,
    installId: node.install_id,
    name: node.name,
    enabled: node.enabled,
    feedEnabled: node.feed_enabled,
    configOverride: node.config_override,
    configVersion: node.config_version,
    agentVersion: node.agent_version,
    sdrtrunkVersion: node.sdrtrunk_version,
    rdioVersion: node.rdio_version,
    os: node.os,
    arch: node.arch,
    lastSeenAt: node.last_seen_at,
    notes: node.notes,
    createdAt: node.created_at,
    online: live.online,
    status: live.status,
    lastStatusAt: live.lastStatusAt,
  };
}

const PatchSchema = z.object({
  name: z.string().max(120).optional(),
  enabled: z.boolean().optional(),
  // Whether decoded calls are forwarded to the central rdio. Off by default so
  // an operator can verify config + reception before feeding the live system.
  feed_enabled: z.boolean().optional(),
  // Validated against the same schema configMerge consumes so staff can't
  // persist an override that would later break the config build.
  config_override: ConfigOverrideSchema.optional(),
  notes: z.string().max(4000).optional(),
});

// ---------------------------------------------------------------------------
// GET /api/nodes
// ---------------------------------------------------------------------------
nodesRouter.get('/api/nodes', requireRole(canManageNodes), async (c) => {
  try {
    const nodes = await listNodes();
    return c.json({ nodes: nodes.map(toApi) });
  } catch (err) {
    log.error({ err }, 'Error listing nodes');
    return c.json({ error: 'Failed to list nodes' }, 500);
  }
});

// ---------------------------------------------------------------------------
// Global feeder config (synced to ALL nodes). Registered BEFORE /:id so the
// literal path wins over the :id param route.
//   GET /api/nodes/global-config  — current global config + version
//   PUT /api/nodes/global-config  — replace it, then fan out to every node
// ---------------------------------------------------------------------------
nodesRouter.get('/api/nodes/global-config', requireRole(canManageNodes), async (c) => {
  try {
    const config = await getGlobalConfig();
    return c.json({ config });
  } catch (err) {
    log.error({ err }, 'Error fetching global feeder config');
    return c.json({ error: 'Failed to fetch global config' }, 500);
  }
});

nodesRouter.put('/api/nodes/global-config', requireRole(canManageNodes), async (c) => {
  try {
    const body = await c.req.json().catch(() => null);
    const parsed = GlobalConfigSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: 'invalid global config', issues: parsed.error.issues }, 400);
    }
    const config = await saveGlobalConfig(parsed.data, c.get('userId') ?? null);
    // Fan the new config out to every online node so the fleet re-syncs.
    const fanout = await pushConfigToAllNodes();
    return c.json({ config, fanout });
  } catch (err) {
    log.error({ err }, 'Error saving global feeder config');
    return c.json({ error: 'Failed to save global config' }, 500);
  }
});

// ---------------------------------------------------------------------------
// Global auto-update switch. Registered BEFORE /:id so the literal path wins
// over the :id param route.
//   GET /api/nodes/auto-update  — current state
//   PUT /api/nodes/auto-update  — set it (nodes read it via the manifest and
//                                 pause AUTOMATIC self-updates while off)
//   POST /api/nodes/update-all  — force an update on every online node NOW,
//                                 regardless of the auto-update flag.
// ---------------------------------------------------------------------------
nodesRouter.get('/api/nodes/auto-update', requireRole(canManageNodes), async (c) => {
  try {
    return c.json({ enabled: await getAutoUpdate() });
  } catch (err) {
    log.error({ err }, 'Error fetching auto-update flag');
    return c.json({ error: 'Failed to fetch auto-update flag' }, 500);
  }
});

const AutoUpdateSchema = z.object({ enabled: z.boolean() });

nodesRouter.put('/api/nodes/auto-update', requireRole(canManageNodes), async (c) => {
  try {
    const body = await c.req.json().catch(() => null);
    const parsed = AutoUpdateSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: 'invalid body', details: parsed.error.issues }, 400);
    }
    await setAutoUpdate(parsed.data.enabled);
    return c.json({ enabled: parsed.data.enabled });
  } catch (err) {
    log.error({ err }, 'Error setting auto-update flag');
    return c.json({ error: 'Failed to set auto-update flag' }, 500);
  }
});

nodesRouter.post('/api/nodes/update-all', requireRole(canManageNodes), async (c) => {
  try {
    const nodes = await listNodes();
    // Only nodes that are both enabled AND have a live agent connection can be
    // told to update. Manual updates ALWAYS trigger, ignoring the auto-update
    // flag (that flag only gates the agent's own automatic passes).
    const online = nodes.filter((n) => n.enabled && hub.isOnline(n.id));
    let triggered = 0;
    await Promise.all(
      online.map(async (n) => {
        const r = await hub.sendCmd(n.id, 'update');
        if (r.ok) triggered += 1;
      }),
    );
    return c.json({ triggered, total: online.length });
  } catch (err) {
    log.error({ err }, 'Error triggering update-all');
    return c.json({ error: 'Failed to trigger update-all' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/nodes/:id
// ---------------------------------------------------------------------------
nodesRouter.get('/api/nodes/:id', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    return c.json(toApi(node));
  } catch (err) {
    log.error({ err, id }, 'Error fetching node');
    return c.json({ error: 'Failed to fetch node' }, 500);
  }
});

// ---------------------------------------------------------------------------
// PATCH /api/nodes/:id
// ---------------------------------------------------------------------------
nodesRouter.patch('/api/nodes/:id', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const body = (await c.req.json().catch(() => ({}))) as unknown;
    const parsed = PatchSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: 'invalid body', details: parsed.error.issues }, 400);
    }
    const patch: NodePatch = {
      ...parsed.data,
      config_override: parsed.data.config_override as
        | Record<string, unknown>
        | undefined,
    };
    const updated = await updateNode(id, patch);
    if (!updated) return c.json({ error: 'node not found' }, 404);
    // Disabling a node force-drops any live agent connection so it can't
    // keep feeding while marked disabled.
    if (patch.enabled === false) {
      hub.forceDisconnectAgent(id, 'node disabled');
    }
    // Config changed → push the freshly-merged payload to the live agent so
    // it applies without waiting for the next hello. Best-effort: offline /
    // preset-unavailable just means the agent picks it up at next hello.
    if (patch.config_override !== undefined && hub.isOnline(id)) {
      try {
        await pushConfigToNode(id);
      } catch (err) {
        log.warn({ err, id }, 'config push after PATCH failed');
      }
    }
    return c.json(toApi(updated));
  } catch (err) {
    log.error({ err, id }, 'Error updating node');
    return c.json({ error: 'Failed to update node' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/nodes/:id/cmd  — send a command to a live agent.
// ---------------------------------------------------------------------------
nodesRouter.post('/api/nodes/:id/cmd', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const body = (await c.req.json().catch(() => ({}))) as {
      action?: string;
      args?: unknown;
    };
    const action = body.action;
    if (typeof action !== 'string' || !action) {
      return c.json({ error: 'action is required' }, 400);
    }
    if (!hub.isOnline(id)) {
      return c.json({ error: 'node offline' }, 409);
    }
    const r = await hub.sendCmd(id, action, body.args);
    return c.json(r, r.ok ? 200 : 502);
  } catch (err) {
    log.error({ err, id }, 'Error sending node command');
    return c.json({ error: 'Failed to send command' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/nodes/:id/config
// ---------------------------------------------------------------------------
nodesRouter.get('/api/nodes/:id/config', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    // Full merged preview: base presets + this node's override, exactly what
    // a push would send. `appliedVersion` is the version the agent last ACKed
    // (nodes.config_version), so staff can see when a push is pending.
    let payload;
    try {
      payload = await buildConfigPayload(node);
    } catch (err) {
      log.warn({ err, id }, 'config preview: presets unavailable');
      return c.json({ error: 'presets unavailable' }, 503);
    }
    return c.json({
      configOverride: node.config_override,
      appliedVersion: node.config_version,
      payload,
    });
  } catch (err) {
    log.error({ err, id }, 'Error fetching node config');
    return c.json({ error: 'Failed to fetch node config' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/nodes/:id/push-config — force a config push to the live agent.
// 409 if the node is offline; 503 if the presets can't be loaded.
// ---------------------------------------------------------------------------
nodesRouter.post('/api/nodes/:id/push-config', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    if (!hub.isOnline(id)) return c.json({ error: 'node offline' }, 409);
    const r = await pushConfigToNode(id);
    if (!r.sent) {
      if (r.reason === 'presets_unavailable') {
        return c.json({ error: 'presets unavailable' }, 503);
      }
      if (r.reason === 'offline') return c.json({ error: 'node offline' }, 409);
      return c.json({ error: 'push failed' }, 502);
    }
    return c.json({ ok: true, configVersion: r.configVersion });
  } catch (err) {
    log.error({ err, id }, 'Error pushing node config');
    return c.json({ error: 'Failed to push config' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/nodes/:id/stats
// ---------------------------------------------------------------------------
nodesRouter.get('/api/nodes/:id/stats', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const raw = parseInt(c.req.query('days') ?? '30', 10);
    const days = Number.isFinite(raw) ? Math.min(365, Math.max(1, raw)) : 30;
    const stats = await getNodeStats(id, days);
    return c.json({ stats });
  } catch (err) {
    log.error({ err, id }, 'Error fetching node stats');
    return c.json({ error: 'Failed to fetch node stats' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/nodes/users/:userId/rotate-feeder-token
// Rotates a contributor's feeder token. The new plaintext is NOT returned
// to staff — only the owner reveals their own token via /api/feeder/me.
// ---------------------------------------------------------------------------
nodesRouter.post(
  '/api/nodes/users/:userId/rotate-feeder-token',
  requireRole(canManageNodes),
  async (c) => {
    if (!feederTokensConfigured()) {
      return c.json({ error: 'feeder tokens not configured' }, 503);
    }
    const userId = c.req.param('userId');
    try {
      await rotateFeederToken(userId);
      return c.json({ ok: true });
    } catch (err) {
      log.error({ err, userId }, 'Error rotating feeder token');
      return c.json({ error: 'Failed to rotate feeder token' }, 500);
    }
  },
);

// ---------------------------------------------------------------------------
// DELETE /api/nodes/:id — owner-only (destructive; matches DELETE /users/:id).
// ---------------------------------------------------------------------------
nodesRouter.delete('/api/nodes/:id', requireRole(isOwner), async (c) => {
  const id = c.req.param('id');
  try {
    hub.forceDisconnectAgent(id);
    const ok = await deleteNode(id);
    if (!ok) return c.json({ error: 'node not found' }, 404);
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err, id }, 'Error deleting node');
    return c.json({ error: 'Failed to delete node' }, 500);
  }
});
