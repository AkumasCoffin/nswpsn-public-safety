/**
 * Staff feeder-node management.
 *
 * The DB-persisted node registry (registry.ts) plus live/ephemeral status
 * from the in-memory hub (hub.ts) merged on top. Read/write split:
 *   - GET reads (list/detail/config/stats/global-config/auto-update) are gated
 *     with requireRole(canViewNodeData) — owner|dev|node_monitor (view-only).
 *   - Every mutating route stays requireRole(canManageNodes) — owner|dev only;
 *     DELETE /:id is owner-only (isOwner).
 * The public NSWPSN_API_KEY alone can't reach any of these — only a logged-in
 * user with the right role can.
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
import { requireRole, canManageNodes, canViewNodeData, isOwner } from '../services/auth/roles.js';
import {
  listNodes,
  getNode,
  createNode,
  rotateNodeToken,
  setPagerPrimary,
  setPagerTuning,
  setAdsbTuning,
  setNodeLocation,
  countNodesForUser,
  MAX_NODES_PER_USER,
  isNodeKind,
  autoNodeName,
  updateNode,
  deleteNode,
  getNodeStats,
  type NodeRow,
  type NodePatch,
} from '../services/nodes/registry.js';
import { hub } from '../services/nodes/hub.js';
import { listChanMgrLog } from '../services/nodes/chanmgrLog.js';
import { handleSurveyCommand, listSurveys } from '../services/siteSurvey.js';
import { allTaggedSites, lgaRingDepths } from '../services/grnCandidates.js';
import { nodeUptimeMany } from '../services/nodes/nodeUptime.js';
import { liveCallWindow } from '../services/nodeCallWindow.js';
import { isAgentCommandAction } from '../services/nodes/protocol.js';
import { getUsernameMap, getUsername } from './users.js';
import { ConfigOverrideSchema } from '../services/nodes/configSchema.js';
import {
  buildConfigPayload,
  pagerPrimaryOf,
  pagerGainOf,
  pagerPpmOf,
  pagerPrimaryOptionsFor,
  adsbGainOf,
  adsbPpmOf,
} from '../services/nodes/configMerge.js';
import { AU_STATES } from '../lib/stateMask.js';
import { isValidZone } from '../services/nodes/rfsZones.js';
import { pushConfigToNode, pushConfigToAllNodes } from '../services/nodes/configPush.js';
import {
  getGlobalConfig,
  saveGlobalConfig,
  GlobalConfigPatchSchema,
  getAutoUpdate,
  setAutoUpdate,
} from '../services/nodes/globalConfig.js';
import { clearAdsbNodeState } from '../services/nodes/adsbNodeStore.js';
import { mintNodeToken, _clearNodeTokenCache } from '../services/auth/nodeToken.js';

export const nodesRouter = new Hono();

/**
 * Map a DB NodeRow to a clean camelCase JSON shape, merging the live
 * hub status (online / last status frame / when it arrived) on top.
 */
function toApi(node: NodeRow, usernames?: Map<string, string>) {
  const live = hub.liveStatus(node.id);
  return {
    id: node.id,
    kind: node.kind,
    userId: node.user_id,
    ownerUsername: usernames?.get(node.user_id) ?? null,
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
    localIp: node.local_ip,
    lastSeenAt: node.last_seen_at,
    notes: node.notes,
    createdAt: node.created_at,
    tokenPrefix: node.token_prefix,
    lat: node.lat,
    lon: node.lon,
    zone: node.zone,
    state: node.state,
    lga: node.lga,
    suburb: node.suburb,
    online: live.online,
    status: live.status,
    lastStatusAt: live.lastStatusAt,
    // Rolling 10-min relayed count — radio calls forwarded / pager messages
    // forwarded to Pagermon. Used by the staff activity chips.
    messagesLast10m: hub.uploadsInWindow(node.id),
    // Pager single-SDR primary frequency preference (persisted).
    pagerPrimary: node.kind === 'pager' ? pagerPrimaryOf(node) : null,
    // The valid primary choices for THIS node's state ({label, mhz}[]) so the
    // staff UI renders the right options without duplicating the plan table.
    pagerPrimaryOptions: node.kind === 'pager' ? pagerPrimaryOptionsFor(node.state) : null,
    // Pager tuner overrides (persisted); null when unset (agent uses defaults).
    pagerGain: node.kind === 'pager' ? pagerGainOf(node) ?? null : null,
    pagerPpm: node.kind === 'pager' ? pagerPpmOf(node) ?? null : null,
    // Self-update in progress (agent swapping/re-execing) — shown as "updating"
    // instead of offline during the brief disconnect.
    updating: hub.isUpdating(node.id),
    /** Which phase: 'checking' | 'fetching' | 'installing' (null = not updating). */
    updateStage: hub.updatingStage(node.id),
    // Pager: reader labels currently decoding (e.g. ['NSWRFS','FRNSW']).
    pagerDecoding: node.kind === 'pager' ? hub.pagerDecoding(node.id) : null,
    // ADS-B tuner overrides (persisted); null when unset. gain 'auto' means
    // agent-managed adaptive gain, not hardware AGC.
    adsbGain: node.kind === 'adsb' ? adsbGainOf(node) ?? null : null,
    adsbPpm: node.kind === 'adsb' ? adsbPpmOf(node) ?? null : null,
  };
}

const PatchSchema = z.object({
  // Node names are always auto-generated ({kind}-{user}-{uuid}); no rename.
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
nodesRouter.get('/api/nodes', requireRole(canViewNodeData), async (c) => {
  try {
    const [nodes, usernames] = await Promise.all([listNodes(), getUsernameMap()]);
    // One query for the whole page rather than one per card.
    const uptime = await nodeUptimeMany(nodes.map((n) => n.id), '7d');
    return c.json({
      nodes: nodes.map((n) => {
        const u = uptime.get(n.id);
        return {
          ...toApi(n, usernames),
          uptimePct: u?.pct ?? null,
          uptimeRunMs: u?.currentRunMs ?? null,
          uptimeWindow: '7d',
        };
      }),
    });
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
nodesRouter.get('/api/nodes/global-config', requireRole(canViewNodeData), async (c) => {
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
    // PATCH-style: the rdio side (agencies/rdioGroups/rdioTags) and the sdrtrunk
    // side (sdrtrunkConfig) are imported independently, so a body may carry only
    // one of them and must leave the other untouched. Fields present replace;
    // fields absent are kept from the current stored config.
    const parsed = GlobalConfigPatchSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: 'invalid global config', issues: parsed.error.issues }, 400);
    }
    const current = await getGlobalConfig();
    const merged = {
      agencies: parsed.data.agencies ?? current.agencies,
      rdioGroups: parsed.data.rdioGroups ?? current.rdioGroups,
      rdioTags: parsed.data.rdioTags ?? current.rdioTags,
      sdrtrunkConfig: parsed.data.sdrtrunkConfig ?? current.sdrtrunkConfig,
      defaults: parsed.data.defaults ?? current.defaults,
      colorPalette: parsed.data.colorPalette ?? current.colorPalette ?? [],
    };
    const config = await saveGlobalConfig(merged, c.get('userId') ?? null);
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
nodesRouter.get('/api/nodes/auto-update', requireRole(canViewNodeData), async (c) => {
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
    // Any node with a live agent connection can be told to update. Manual
    // updates ALWAYS trigger, ignoring the auto-update flag (that flag only
    // gates the agent's own automatic passes).
    //
    // Deliberately NOT filtered on n.enabled: that flag means CAPTURE off —
    // the decoder is paused, the agent is connected and updatable. The old
    // filter predates that meaning and silently froze paused nodes out of
    // updates ("Update triggered on 4 of 4" with five agents online — the
    // fifth had capture off and was never even counted).
    const online = nodes.filter((n) => hub.isOnline(n.id));
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
nodesRouter.get('/api/nodes/:id', requireRole(canViewNodeData), async (c) => {
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
// GET /api/nodes/:id/chanmgr-log — the automatic channel manager's audit log
// (auto-stops, each retest with its verdict, restores). Last 50, newest first;
// persisted server-side so it survives agent restarts and offline nodes.
// ---------------------------------------------------------------------------
nodesRouter.get('/api/nodes/:id/chanmgr-log', requireRole(canViewNodeData), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    return c.json({ entries: await listChanMgrLog(id) });
  } catch (err) {
    log.error({ err, id }, 'Error fetching chanmgr log');
    return c.json({ error: 'Failed to fetch log' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/nodes/:id/grn-sites — the GRN dataset as channel candidates for
// ONE node: every site with a usable control frequency, ranked by how near it
// is (straight-line from the node's pin when it has one, otherwise by how many
// council borders away the site sits). The parsing is the survey's own, so a
// site added by hand here gets exactly the frequency a survey would have
// tested. Sites whose control channel the dataset does not state are returned
// too, flagged — staff can see they exist and why they cannot be added.
// ---------------------------------------------------------------------------
nodesRouter.get('/api/nodes/:id/grn-sites', requireRole(canViewNodeData), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    const sites = await allTaggedSites();
    const depths = node.lga
      ? await lgaRingDepths(node.state ?? 'NSW', node.lga, 4)
      : new Map<string, number>();
    const hasPin = typeof node.lat === 'number' && typeof node.lon === 'number';

    const rows = sites.map((s) => ({
      name: s.name,
      grnKey: s.grnKey,
      lga: s.lga,
      system: s.system,
      mhz: s.mhz,
      altMhz: s.altMhz,
      // Why a site cannot be added, in the dataset's own words.
      note: s.mhz === null
        ? (s.rawCc ? `control channel not usable: "${s.rawCc.slice(0, 60)}"` : 'no control channel listed')
        : null,
      km: hasPin && s.lat !== null && s.lon !== null
        ? Math.round(haversineKm(node.lat as number, node.lon as number, s.lat, s.lon) * 10) / 10
        : null,
      ring: s.lga !== null && depths.has(s.lga) ? depths.get(s.lga)! : null,
    }));

    rows.sort((a, b) => {
      if (a.km !== null && b.km !== null) return a.km - b.km;
      if (a.km !== null) return -1;
      if (b.km !== null) return 1;
      return (a.ring ?? 99) - (b.ring ?? 99) || a.name.localeCompare(b.name);
    });
    return c.json({ sites: rows, nodeLga: node.lga, located: hasPin });
  } catch (err) {
    log.error({ err, id }, 'Error listing GRN sites for node');
    return c.json({ error: 'Failed to list sites' }, 500);
  }
});

/** Great-circle distance in km. Display-only ranking, so the spherical
 *  approximation is ample. */
function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371;
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(a)));
}

// ---------------------------------------------------------------------------
// GET /api/nodes/:id/site-surveys — the node's RF survey history: every run
// with every site tested (passes, failures, skips). Staff view tier.
// ---------------------------------------------------------------------------
nodesRouter.get('/api/nodes/:id/site-surveys', requireRole(canViewNodeData), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    return c.json({ surveys: await listSurveys(id) });
  } catch (err) {
    log.error({ err, id }, 'Error fetching site surveys');
    return c.json({ error: 'Failed to fetch surveys' }, 500);
  }
});

// ---------------------------------------------------------------------------
// PUT /api/nodes/:id/location — staff edit of a node's location. The owner
// path (feeder.ts) stays; this exists so staff can fix a missing/wrong LGA
// before running a survey without waiting on the owner.
// ---------------------------------------------------------------------------
const StaffAreaLocationSchema = z.object({
  state: z.enum(AU_STATES as unknown as [string, ...string[]]),
  lga: z.string().trim().min(1).max(120),
  suburb: z.string().trim().max(120).optional(),
  // The pin is OPTIONAL here on purpose: staff fixing a node's council area
  // must not silently wipe the antenna position its owner set. Omit the keys
  // and setNodeLocation leaves them alone; send null to clear them.
  lat: z.number().min(-90).max(90).nullable().optional(),
  lon: z.number().min(-180).max(180).nullable().optional(),
});
const StaffAdsbLocationSchema = z.object({
  lat: z.number().min(-90).max(90),
  lon: z.number().min(-180).max(180),
});
nodesRouter.put('/api/nodes/:id/location', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    const body = await c.req.json().catch(() => ({}));
    const parsed =
      node.kind === 'adsb'
        ? StaffAdsbLocationSchema.safeParse(body)
        : StaffAreaLocationSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: 'invalid location', details: parsed.error.issues }, 400);
    }
    const updated = await setNodeLocation(id, {
      ...parsed.data,
      ...(node.kind !== 'adsb' ? { suburb: (parsed.data as { suburb?: string }).suburb ?? null } : {}),
    });
    if (node.kind !== 'radio') await pushConfigToNode(id).catch(() => undefined);
    return c.json({ node: updated ? toApi(updated) : null });
  } catch (err) {
    log.error({ err, id }, 'Error setting node location (staff)');
    return c.json({ error: 'Failed to set location' }, 500);
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
    // enabled (capture on/off), feed_enabled (upload on/off), and config_override
    // all reach the live agent via a config push — NOT by dropping the socket.
    // The agent stays connected; enabled=false stops capture, feed=false disables
    // the rdio downstream. Best-effort: offline just means it applies at next hello.
    const pushRelevant =
      patch.enabled !== undefined ||
      patch.feed_enabled !== undefined ||
      patch.config_override !== undefined;
    if (pushRelevant && hub.isOnline(id)) {
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
// PUT /api/nodes/:id/pager-primary — staff set a pager node's single-SDR
// primary frequency. Valid labels come from the node's STATE's plan (NSW:
// NSWRFS default / FRNSW; QLD: QFES). Persisted + pushed live.
// ---------------------------------------------------------------------------
const PagerPrimarySchema = z.object({ primary: z.string().min(1).max(32) });
nodesRouter.put('/api/nodes/:id/pager-primary', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    if (node.kind !== 'pager') return c.json({ error: 'not a pager node' }, 400);
    const parsed = PagerPrimarySchema.safeParse(await c.req.json().catch(() => ({})));
    if (!parsed.success) return c.json({ error: 'invalid primary' }, 400);
    if (!pagerPrimaryOptionsFor(node.state).some((f) => f.label === parsed.data.primary)) {
      return c.json({ error: `invalid primary for state ${node.state ?? 'NSW'}` }, 400);
    }
    const updated = await setPagerPrimary(id, parsed.data.primary);
    if (!updated) return c.json({ error: 'update failed' }, 500);
    await pushConfigToNode(id).catch(() => {}); // apply live if online
    return c.json(toApi(updated));
  } catch (err) {
    log.error({ err, id }, 'Error setting pager primary');
    return c.json({ error: 'Failed to set primary frequency' }, 500);
  }
});

// ---------------------------------------------------------------------------
// PUT /api/nodes/:id/pager-tuning — staff set a pager node's tuner-gain / ppm
// overrides (applied to all readers). Persisted + pushed live. Each field is
// optional: send a value to set it, null to clear (revert to agent default),
// or omit to leave it unchanged. gain: 0–60 dB or "auto" (hardware AGC);
// ppm: -200..200. Both are stored as config_override keys, so they survive
// restarts/updates the same way the primary frequency does.
// ---------------------------------------------------------------------------
const PagerTuningSchema = z
  .object({
    gain: z.union([z.literal('auto'), z.number().min(0).max(60), z.null()]).optional(),
    ppm: z.union([z.number().int().min(-200).max(200), z.null()]).optional(),
  })
  .refine((v) => v.gain !== undefined || v.ppm !== undefined, {
    message: 'provide gain and/or ppm',
  });
nodesRouter.put('/api/nodes/:id/pager-tuning', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    if (node.kind !== 'pager') return c.json({ error: 'not a pager node' }, 400);
    const parsed = PagerTuningSchema.safeParse(await c.req.json().catch(() => ({})));
    if (!parsed.success) return c.json({ error: 'invalid tuning' }, 400);

    const patch: { gain?: string | null; ppm?: number | null } = {};
    if (parsed.data.gain !== undefined) {
      patch.gain = parsed.data.gain === null ? null : String(parsed.data.gain);
    }
    if (parsed.data.ppm !== undefined) patch.ppm = parsed.data.ppm;

    const updated = await setPagerTuning(id, patch);
    if (!updated) return c.json({ error: 'update failed' }, 500);
    await pushConfigToNode(id).catch(() => {}); // apply live if online
    return c.json(toApi(updated));
  } catch (err) {
    log.error({ err, id }, 'Error setting pager tuning');
    return c.json({ error: 'Failed to set tuner overrides' }, 500);
  }
});

// ---------------------------------------------------------------------------
// PUT /api/nodes/:id/adsb-tuning — staff set an ADS-B node's gain / ppm.
// Persisted + pushed live. Send a value to set, null to clear (revert to the
// decoder's default), or omit to leave unchanged.
//
// gain accepts "auto" — agent-managed adaptive gain, the recommended setting —
// or a fixed 0-49.6 dB. "auto" is NOT the dongle's hardware AGC, which decodes
// 1090 poorly; the agent runs an autogain loop off the decoder's own
// statistics, because the right gain depends on each site's antenna, cabling
// and RF neighbours and no single number suits a national fleet.
//
// Antenna position is deliberately NOT tunable here: it comes from the node's
// own lat/lon, set with the map pin at creation.
// ---------------------------------------------------------------------------
const AdsbTuningSchema = z
  .object({
    // 49.6 dB is the top step on an R820T/R820T2, the tuner in essentially
    // every RTL dongle; values above it are silently clamped by librtlsdr.
    gain: z.union([z.literal('auto'), z.number().min(0).max(49.6), z.null()]).optional(),
    // Tighter than the pager route's +/-200: a 1090 MHz dongle needing more
    // than 100 ppm of correction is faulty, not miscalibrated.
    ppm: z.union([z.number().int().min(-100).max(100), z.null()]).optional(),
  })
  .refine((v) => v.gain !== undefined || v.ppm !== undefined, {
    message: 'provide gain and/or ppm',
  });
nodesRouter.put('/api/nodes/:id/adsb-tuning', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    if (node.kind !== 'adsb') return c.json({ error: 'not an adsb node' }, 400);
    const parsed = AdsbTuningSchema.safeParse(await c.req.json().catch(() => ({})));
    if (!parsed.success) return c.json({ error: 'invalid tuning' }, 400);

    const patch: { gain?: string | null; ppm?: number | null } = {};
    if (parsed.data.gain !== undefined) {
      patch.gain = parsed.data.gain === null ? null : String(parsed.data.gain);
    }
    if (parsed.data.ppm !== undefined) patch.ppm = parsed.data.ppm;

    const updated = await setAdsbTuning(id, patch);
    if (!updated) return c.json({ error: 'update failed' }, 500);
    await pushConfigToNode(id).catch(() => {}); // apply live if online
    return c.json(toApi(updated));
  } catch (err) {
    log.error({ err, id }, 'Error setting adsb tuning');
    return c.json({ error: 'Failed to set tuner overrides' }, 500);
  }
});

// ---------------------------------------------------------------------------
// PUT /api/nodes/:id/state — staff move a pager node to another Australian
// state (+ its LGA tag). The state selects both the Pagermon the relay
// forwards into and the frequency plan, so the config is re-pushed live; the
// antenna pin is left untouched. A stale pagerPrimary from the old state needs
// no clearing — pagerPrimaryOf falls back to the new state's default.
// ---------------------------------------------------------------------------
const NodeStateSchema = z.object({
  state: z.enum(AU_STATES as unknown as [string, ...string[]]),
  lga: z.string().trim().min(1).max(120),
});
nodesRouter.put('/api/nodes/:id/state', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const node = await getNode(id);
    if (!node) return c.json({ error: 'node not found' }, 404);
    if (node.kind !== 'pager') return c.json({ error: 'not a pager node' }, 400);
    const parsed = NodeStateSchema.safeParse(await c.req.json().catch(() => ({})));
    if (!parsed.success) return c.json({ error: 'invalid state/lga' }, 400);
    const updated = await setNodeLocation(id, { state: parsed.data.state, lga: parsed.data.lga });
    if (!updated) return c.json({ error: 'update failed' }, 500);
    await pushConfigToNode(id).catch(() => {}); // retune live if online
    return c.json(toApi(updated));
  } catch (err) {
    log.error({ err, id }, 'Error setting node state');
    return c.json({ error: 'Failed to set node state' }, 500);
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
    if (!isAgentCommandAction(action)) {
      return c.json({ error: 'unknown action' }, 400);
    }
    if (!hub.isOnline(id)) {
      return c.json({ error: 'node offline' }, 409);
    }
    // Site surveys are orchestrated server-side: candidates come from the
    // node's LGA neighbourhood, never from the browser.
    const sv = await handleSurveyCommand(id, action, body.args, (c.get('userId') as string | undefined) ?? null);
    if (sv) return c.json(sv, sv.ok ? 200 : 502);
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
nodesRouter.get('/api/nodes/:id/config', requireRole(canViewNodeData), async (c) => {
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
nodesRouter.get('/api/nodes/:id/stats', requireRole(canViewNodeData), async (c) => {
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
// POST /api/nodes — staff-create a node for a user (name + kind). Mints the
// node's own token, returned ONCE (bake it into the installer now).
// ---------------------------------------------------------------------------
const CreateSchema = z
  .object({
    userId: z.string().min(1),
    kind: z.string().refine(isNodeKind, 'invalid node kind'),
    // Radio and pager nodes both give state + lga (+ optional suburb) — the
    // same per-kind rule as the owner-facing create in feeder.ts. A radio
    // node's LGA is what its RF site survey searches from, so a node created
    // without one cannot be surveyed until someone sets it.
    zone: z.string().min(1).refine(isValidZone, 'unknown zone').optional(),
    state: z.enum(AU_STATES as unknown as [string, ...string[]]).optional(),
    lga: z.string().trim().min(1).max(120).optional(),
    suburb: z.string().trim().max(120).optional(),
  })
  .superRefine((v, ctx) => {
    if (v.kind === 'adsb') return; // located by its pin alone
    if (!v.state) ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['state'], message: 'state required' });
    if (!v.lga) ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['lga'], message: 'lga required' });
  });
nodesRouter.post('/api/nodes', requireRole(canManageNodes), async (c) => {
  try {
    const parsed = CreateSchema.safeParse(await c.req.json().catch(() => ({})));
    if (!parsed.success) {
      return c.json({ error: 'invalid body', details: parsed.error.issues }, 400);
    }
    const { userId, kind, zone, state, lga, suburb } = parsed.data;
    if ((await countNodesForUser(userId)) >= MAX_NODES_PER_USER) {
      return c.json({ error: 'node limit reached for this user' }, 429);
    }
    const name = autoNodeName(kind, await getUsername(userId));
    const { token, tokenHash, tokenPrefix } = mintNodeToken();
    const node = await createNode(userId, name, kind, tokenHash, tokenPrefix, {
      // zone is legacy and radio-only: accepted so an older caller still
      // works, never required, and never the thing anything reads first.
      zone: kind === 'radio' ? zone ?? null : null,
      state: state ?? null,
      lga: lga ?? null,
      suburb: suburb ?? null,
    });
    if (!node) return c.json({ error: 'registry unavailable' }, 503);
    c.header('Cache-Control', 'no-store');
    return c.json({ node: toApi(node), token });
  } catch (err) {
    log.error({ err }, 'Error creating node');
    return c.json({ error: 'Failed to create node' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/nodes/:id/rotate-token — new per-node token, returned ONCE.
// Invalidates the old token, drops the live agent, and resets TOFU binding so
// a re-provisioned machine can re-bind.
// ---------------------------------------------------------------------------
nodesRouter.post('/api/nodes/:id/rotate-token', requireRole(canManageNodes), async (c) => {
  const id = c.req.param('id');
  try {
    const { token, tokenHash, tokenPrefix } = mintNodeToken();
    const ok = await rotateNodeToken(id, tokenHash, tokenPrefix);
    if (!ok) return c.json({ error: 'node not found' }, 404);
    _clearNodeTokenCache();
    hub.forceDisconnectAgent(id, 'token rotated');
    c.header('Cache-Control', 'no-store');
    return c.json({ token });
  } catch (err) {
    log.error({ err, id }, 'Error rotating node token');
    return c.json({ error: 'Failed to rotate token' }, 500);
  }
});

// ---------------------------------------------------------------------------
// DELETE /api/nodes/:id — owner-only (destructive; matches DELETE /users/:id).
// Hard-revokes the node's token (no auto-link recreates it).
// ---------------------------------------------------------------------------
nodesRouter.delete('/api/nodes/:id', requireRole(isOwner), async (c) => {
  const id = c.req.param('id');
  try {
    hub.forceDisconnectAgent(id);
    hub.clearNode(id);
    liveCallWindow.dropNode(id);
    // Live snapshot, 8 hours of traces, issue log. Without this a deleted
    // receiver stays on the map for up to two minutes and keeps its coverage
    // picture for the rest of the window.
    clearAdsbNodeState(id);
    const ok = await deleteNode(id);
    if (!ok) return c.json({ error: 'node not found' }, 404);
    _clearNodeTokenCache();
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err, id }, 'Error deleting node');
    return c.json({ error: 'Failed to delete node' }, 500);
  }
});
