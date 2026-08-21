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
 * Reduce a channel name to the SITE it belongs to, so a call on a traffic
 * channel can be matched to that site's control channel.
 *
 * vce reports decode quality per channel, but a call's channel is the traffic
 * channel it was granted ("T-Knights Hill"), while `channels[]` carries the
 * control channels ("Knights Hill"). An exact-name lookup therefore never
 * matched and every call showed no decode at all.
 *
 * Strips a leading traffic marker — "T-", "T ", "TC-" — then removes
 * punctuation and case. The delimiter is required, so a site legitimately
 * starting with T ("Tumut") is left alone. This is a naming CONVENTION, not a
 * guarantee: when it doesn't match, the caller shows a dash, which is honest.
 * The figure it finds is the site's control-channel decode, i.e. "how well are
 * we hearing this site", which is what the column means.
 */
/**
 * Display form of a channel name used as a SITE label: drops the traffic
 * marker but keeps the original wording and case, so "T-Knights Hill" reads
 * as "Knights Hill". The raw form is a decoder detail; the operator is looking
 * for the site.
 */
function prettySiteName(name: string): string {
  return name.trim().replace(/^t[c]?[-_ ]+/i, '') || name.trim();
}

function normaliseChannelName(name: string): string {
  return name
    .trim()
    .toLowerCase()
    .replace(/^t[c]?[-_ ]+/, '')
    .replace(/[^a-z0-9]/g, '');
}

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

  // Per-channel facts a CALL needs but is never told. vce's buildActiveCalls
  // emits only state/from/to/aliases/timeslot/frequency — no site, no
  // syncPercent, no signalDbfs — so those columns had nothing to render. A
  // call is carried by exactly one channel, so the channel supplies them.
  //
  // Keyed on the CHANNEL's name, which a call refers to as `channelName`
  // (not `name` — that field does not exist on a call, which is what made an
  // earlier attempt at this silently resolve nothing).
  const chFacts = new Map<string, { sync: unknown; signal: unknown; site: unknown }>();
  for (const ch of asRows(st['channels'])) {
    const name = ch['name'];
    if (typeof name === 'string' && name) {
      const facts = {
        sync: ch['syncPercent'],
        signal: ch['signalDbfs'],
        site: ch['site'],
      };
      chFacts.set(name, facts);
      // Also under the normalised key, so a call on a TRAFFIC channel can find
      // its site's CONTROL channel — see normaliseChannelName.
      const norm = normaliseChannelName(name);
      if (norm && !chFacts.has(norm)) chFacts.set(norm, facts);
    }
  }
  /**
   * Decode health for a call is the call's OWN measurement — vce reports it
   * per traffic channel (ChannelProcessingManager.supportsControlChannelQuality
   * covers TRAFFIC channels, so the monitor is attached to voice calls too).
   *
   * The carrying channel is NOT used as a substitute. A call is weak precisely
   * when its traffic channel is weak while the site's control channel is fine,
   * so borrowing the control figure would paint over the one case the column
   * exists to reveal. Null stays null and renders as a dash — an honest "not
   * reported" rather than a healthy-looking number that isn't about this call.
   *
   * 0 is still treated as unmeasured: a call in progress cannot be decoding at
   * 0%, so a literal zero is a defaulted field.
   */
  const ownQuality = (v: unknown): unknown =>
    v === null || v === undefined || v === 0 ? null : v;

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
    // vce calls this `channelName`; `name` is kept only as a defensive
    // fallback for any build that reports it the other way.
    const chNameRaw = ac['channelName'] ?? ac['name'];
    const chName = typeof chNameRaw === 'string' ? chNameRaw : null;
    // Exact name first, then the site-normalised form so a call on a traffic
    // channel ("T-Knights Hill") resolves to its site's control channel
    // ("Knights Hill"). Looking up only the raw name is what left every call
    // with no decode.
    const facts = chName
      ? chFacts.get(chName) ?? chFacts.get(normaliseChannelName(chName))
      : undefined;
    calls.push({
      node: nodeId,
      nodeName,
      name: chName,
      system: ac['system'] ?? null,
      // A call carries no site of its own — take the carrying channel's, and
      // fall back to the channel NAME, which is the site's name in this
      // deployment. Previously this read ac['name'], which is never set, so
      // every call reported no site at all.
      site: ac['site'] ?? facts?.site ?? (chName ? prettySiteName(chName) : null),
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
      // The call's own decode, never the control channel's (see ownQuality).
      syncPercent: ownQuality(ac['syncPercent']),
      signalDbfs: ownQuality(ac['signalDbfs']),
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
 * STABLE ordering: node, then channel name. Deliberately not by decode health.
 *
 * Sorting by decode put the worst site first, which reads well in a snapshot
 * and terribly in a live view — the figures move every report, so at the 1s
 * cadence rows swapped places constantly and the table was unreadable. A row's
 * position must mean something fixed (which site it is) so the eye can track
 * it; decode is conveyed by the colour of the number instead, which draws
 * attention without moving anything.
 */
export function sortLiveChannels(channels: Array<Record<string, unknown>>): void {
  const key = (r: Record<string, unknown>): string =>
    `${String(r['nodeName'] ?? r['node'] ?? '')} ${String(r['name'] ?? '')}`;
  channels.sort((x, y) => key(x).localeCompare(key(y)));
}
