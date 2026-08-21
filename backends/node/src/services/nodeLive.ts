/**
 * Fleet-wide LIVE radio state — the shaping shared by the two ways staff can
 * read it:
 *
 *   GET  /api/node-data/live        one-shot snapshot (also the WS fallback)
 *   WS   subscribeLive              pushed the instant an agent reports
 *
 * Both call shapeNodeLive() so a row looks identical whichever path delivered
 * it; the frontend renders one shape and never has to reconcile two.
 *
 * The source is the WS hub's in-memory status (what each agent last reported),
 * NOT Postgres: node_radio_events is a history of calls that have already
 * ended and carries no control-channel state at all, so it physically cannot
 * answer "what is decoding right now".
 */
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { talkgroupCatalog } from './talkgroupCatalog.js';

/** States that mean "traffic in progress" — mirrors the vce panel's own set. */
const LIVE_CALL_STATES = new Set(['CALL', 'ACTIVE', 'ENCRYPTED']);

export interface LiveNodeSlice {
  node: string;
  nodeName: string | null;
  lastStatusAt: string | null;
  channels: Array<Record<string, unknown>>;
  calls: Array<Record<string, unknown>>;
}

/**
 * Node id → { name, kind }, cached ~60s. The hub only knows ids, and a raw
 * uuid per row is unreadable; `kind` is what keeps pager nodes out of a RADIO
 * view (they have no channels or decode and only inflated "N nodes online").
 */
let _nodeCache: { at: number; map: Map<string, { name: string | null; kind: string }> } | null =
  null;

export async function liveNodeNames(
  pool?: Pool | null,
): Promise<Map<string, { name: string | null; kind: string }>> {
  if (_nodeCache && Date.now() - _nodeCache.at < 60_000) return _nodeCache.map;
  const map = new Map<string, { name: string | null; kind: string }>();
  try {
    const p = pool ?? (await getPool());
    if (p) {
      const res = await p.query<{ id: string; name: string | null; kind: string | null }>(
        'SELECT id, name, kind FROM nodes',
      );
      for (const r of res.rows) map.set(r.id, { name: r.name, kind: r.kind ?? 'radio' });
    }
  } catch (e) {
    log.warn({ err: e }, 'nodeLive: failed to load node names');
  }
  _nodeCache = { at: Date.now(), map };
  return map;
}

const asRows = (v: unknown): Array<Record<string, unknown>> =>
  Array.isArray(v) ? (v as Array<Record<string, unknown>>) : [];

/**
 * Shape one node's last-reported status into the Live view's rows. Returns
 * null when the node isn't a radio node or has reported nothing yet — callers
 * treat that as "nothing to show for this node", not an error.
 */
export async function shapeNodeLive(
  nodeId: string,
  status: unknown,
  lastStatusAt: number | null,
): Promise<LiveNodeSlice | null> {
  const nodes = await liveNodeNames();
  const meta = nodes.get(nodeId);
  // Unknown ids are treated as radio: a node that registered since the cache
  // was filled must not vanish from Live for up to a minute.
  if (meta && meta.kind !== 'radio') return null;
  const st = (status ?? null) as Record<string, unknown> | null;
  if (!st) return null;

  const tg = await talkgroupCatalog();
  const nodeName = meta?.name ?? null;
  const channels: Array<Record<string, unknown>> = [];
  const calls: Array<Record<string, unknown>> = [];

  for (const ch of asRows(st['channels'])) {
    const id = Number(ch['to']);
    channels.push({
      node: nodeId,
      nodeName,
      name: ch['name'] ?? null,
      system: ch['system'] ?? null,
      site: ch['site'] ?? null,
      state: ch['state'] ?? null,
      control: ch['control'] === true,
      processing: ch['processing'] === true,
      frequency: ch['frequency'] ?? null,
      // vce reports these per channel; they are the decode-health pair the
      // Live view leads with.
      syncPercent: ch['syncPercent'] ?? null,
      signalDbfs: ch['signalDbfs'] ?? null,
      timeslot: ch['timeslot'] ?? null,
      talkgroup: Number.isInteger(id) ? id : null,
      talkgroupLabel: Number.isInteger(id) ? tg.labels.get(id) ?? null : null,
      agency: Number.isInteger(id) ? tg.agencies.get(id) ?? null : null,
    });
  }

  for (const ac of asRows(st['activeCalls'])) {
    // vce reports every processing channel here, including the control
    // channels, which arrive with no talkgroup and showed up as a wall of
    // duplicate "TG 0" rows mirroring the channel table above. Only states
    // that represent traffic count as a call.
    const state = String(ac['state'] ?? '').toUpperCase();
    if (!LIVE_CALL_STATES.has(state)) continue;
    // vce reports a granted traffic channel as active BETWEEN calls, with no
    // target and no source. Those are not calls — they showed as "TG 0" rows
    // at 0% decode. Require some call identity, matching the Nodes tab's Now
    // Playing guard.
    if (ac['to'] == null && ac['from'] == null && !ac['toAlias'] && !ac['fromAlias']) continue;
    const id = Number(ac['to']);
    calls.push({
      node: nodeId,
      nodeName,
      name: ac['name'] ?? null,
      system: ac['system'] ?? null,
      // Traffic channels often carry no site of their own; fall back to the
      // channel name, which is the site's name in this deployment.
      site: ac['site'] ?? ac['name'] ?? null,
      state: ac['state'] ?? null,
      // Explicit flag so the frontend's hide-encrypted filter doesn't have to
      // re-derive it from a free-text state string.
      encrypted: state === 'ENCRYPTED',
      frequency: ac['frequency'] ?? null,
      timeslot: ac['timeslot'] ?? null,
      from: ac['from'] ?? null,
      fromAlias: ac['fromAlias'] ?? null,
      talkerAlias: ac['talkerAlias'] ?? null,
      to: ac['to'] ?? null,
      toAlias: ac['toAlias'] ?? null,
      talkgroup: Number.isInteger(id) ? id : null,
      talkgroupLabel: Number.isInteger(id) ? tg.labels.get(id) ?? null : null,
      agency: Number.isInteger(id) ? tg.agencies.get(id) ?? null : null,
      color: Number.isInteger(id) ? tg.colors.get(id) ?? null : null,
      syncPercent: ac['syncPercent'] ?? null,
      signalDbfs: ac['signalDbfs'] ?? null,
    });
  }

  return {
    node: nodeId,
    nodeName,
    lastStatusAt: lastStatusAt !== null ? new Date(lastStatusAt).toISOString() : null,
    channels,
    calls,
  };
}

/**
 * Control channels first, then weakest decode: a site that is struggling is
 * what an operator watching this page needs to see, not the healthy majority.
 * Applied to the merged fleet list, so it lives here rather than per node.
 */
export function sortLiveChannels(channels: Array<Record<string, unknown>>): void {
  channels.sort((x, y) => {
    const c = Number(y['control'] === true) - Number(x['control'] === true);
    if (c !== 0) return c;
    return Number(x['syncPercent'] ?? 0) - Number(y['syncPercent'] ?? 0);
  });
}
