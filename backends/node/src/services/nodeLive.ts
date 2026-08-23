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
import { isLiveCallRow, type WindowCall } from './nodeCallWindow.js';

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

/**
 * The talkgroup id a call is TO.
 *
 * Usually just the number, but a PATCH call reports the whole patch as its
 * target — "P:10128 [10120, 10125]", the supergroup followed by its members.
 * Number() gives NaN on that, so every patched call arrived with no talkgroup,
 * which rendered as a bare dash with no label, agency or colour: on air and
 * apparently unidentifiable. The supergroup is the talkgroup being spoken on,
 * so that is the one to resolve.
 */
function talkgroupIdOf(to: unknown): number {
  return parsePatchTo(to).id;
}

/**
 * Split a live call's TO field into the talkgroup spoken on and, when it is a
 * patch, the talkgroups patched into it.
 *
 * vce renders a patched target with PatchGroup.toString(): "P:" and the
 * supergroup, then the patched TALKGROUPS in brackets, then — only if there
 * are any — the patched RADIOS in a second pair of brackets:
 *
 *     P:10128 [10120, 10125]            supergroup + two patched talkgroups
 *     P:10128 [10120] [1234567]         …and a patched radio as well
 *
 * Only the FIRST bracket group is talkgroups. Reading both would put 7-digit
 * radio ids in a talkgroup list, where they would resolve to no label and,
 * worse, could group two unrelated calls together.
 *
 * This is the one place the automatic patch membership is available live —
 * the activity feed reports a patched transmission as its supergroup alone —
 * so it is kept rather than discarded.
 */
export function parsePatchTo(to: unknown): { id: number; patched: number[] } {
  const none = { id: NaN, patched: [] as number[] };
  if (to === null || to === undefined) return none;
  const raw = String(to).trim();
  const direct = Number(raw);
  if (Number.isInteger(direct)) return { id: direct, patched: [] };
  const head = /^P:\s*(\d+)/i.exec(raw);
  if (!head) return none;
  const id = Number(head[1]);
  const members = /\[([^\]]*)\]/.exec(raw);
  const patched: number[] = [];
  if (members) {
    for (const part of (members[1] ?? '').split(',')) {
      const n = Number(part.trim());
      if (Number.isInteger(n) && n > 0 && n !== id && !patched.includes(n)) patched.push(n);
    }
  }
  return { id, patched };
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
 *
 * `windowCalls` is the node's rolling call window (nodeCallWindow). When given,
 * it REPLACES `status.activeCalls` as the source of call rows: it holds the
 * same calls plus any still inside their grant age-out, and carries the timing
 * the raw frame has no room for. Omitting it falls back to shaping the bare
 * frame, which is what the unit tests exercise and what any future caller
 * without a window gets.
 */
export async function shapeNodeLive(
  nodeId: string,
  status: unknown,
  lastStatusAt: number | null,
  windowCalls?: readonly WindowCall[] | null,
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

  /** Timing a raw frame cannot carry — supplied by the call window. */
  interface CallTiming {
    ended: boolean;
    endedAt: string | null;
    startedAt: string | null;
    lastHeardAt: string | null;
    durationMs: number | null;
  }
  const NO_TIMING: CallTiming = {
    ended: false,
    endedAt: null,
    startedAt: null,
    lastHeardAt: null,
    durationMs: null,
  };

  const buildCall = (ac: Record<string, unknown>, timing: CallTiming): Record<string, unknown> => {
    const state = String(ac['state'] ?? '').toUpperCase();
    const { id, patched } = parsePatchTo(ac['to']);
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
    return {
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
      // An over-the-air patch, as reported for THIS call. Null for an ordinary
      // call. `talkgroups` is the supergroup first — which is the talkgroup
      // being spoken on — then the members patched into it, each carrying its
      // own label and colour so the row can render them like any talkgroup.
      // A patch whose members vce did not name is still a patch: the flag is
      // the supergroup form of the target, not the presence of a member list.
      patch:
        /^P:/i.test(String(ac['to'] ?? '').trim()) && Number.isInteger(id)
          ? {
              kind: 'automatic' as const,
              label: null,
              talkgroups: [id, ...patched].map((t) => ({
                talkgroup: t,
                // Short name: these sit several-to-a-chip on one row.
                label: tg.shortLabels?.get(t) ?? tg.labels.get(t) ?? null,
                color: tg.colors.get(t) ?? null,
              })),
            }
          : null,
      // The call's own decode, never the control channel's (see ownQuality).
      syncPercent: ownQuality(ac['syncPercent']),
      signalDbfs: ownQuality(ac['signalDbfs']),
      ...timing,
    };
  };

  if (windowCalls) {
    // The window already applied the state + identity guard at ingest, so its
    // rows are shaped verbatim. An ended call keeps its last reported values —
    // that is the point of holding it.
    for (const wc of windowCalls) {
      calls.push(
        buildCall(wc.raw, {
          ended: wc.endedAt !== null,
          endedAt: wc.endedAt !== null ? new Date(wc.endedAt).toISOString() : null,
          startedAt: new Date(wc.firstSeenAt).toISOString(),
          lastHeardAt: new Date(wc.lastSeenAt).toISOString(),
          // For a call still up this is how long it has been OBSERVED, not
          // wall-clock elapsed. Deliberate: every timestamp here comes from the
          // window's own clock, and reaching for Date.now() to make a live call
          // tick would mix two clocks — which in testing produced a duration of
          // roughly three years. At the 1s live cadence the two differ by less
          // than a second anyway, and if a node stops reporting the figure
          // correctly stops growing instead of inventing airtime.
          durationMs: Math.max(0, (wc.endedAt ?? wc.lastSeenAt) - wc.firstSeenAt),
        }),
      );
    }
  } else {
    for (const ac of asRows(st['activeCalls'])) {
      if (!isLiveCallRow(ac)) continue;
      calls.push(buildCall(ac, NO_TIMING));
    }
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
    `${String(r['nodeName'] ?? r['node'] ?? '')}\u0000${String(r['name'] ?? '')}`;
  channels.sort((x, y) => key(x).localeCompare(key(y)));
}
