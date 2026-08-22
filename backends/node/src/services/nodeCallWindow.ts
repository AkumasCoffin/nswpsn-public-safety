/**
 * A short ROLLING MEMORY of the calls each node has reported, so the Live view
 * can show a call for a moment after it ends and can show one it never caught
 * in the act.
 *
 * Why this has to exist at all: nothing anywhere else in the chain remembers a
 * call. vce rebuilds `activeCalls` from live decoder state on every request and
 * filters out the TEARDOWN/FADE rows, the agent rebuilds its slice from that
 * response, and the hub stores only the last frame. So a call was on screen if
 * and only if it happened to be mid-transmission at the instant of a poll —
 * which is why finished calls vanished mid-read and short ones never appeared.
 *
 * vce's own Now Playing panel does not have this problem because it holds a
 * finished traffic-channel row for 5s (sdrtrunk.nowPlaying.trafficChannelHangMs).
 * That hang is invisible over the control API — the row is retained with state
 * TEARDOWN and buildActiveCalls filters TEARDOWN out — so we reproduce it here
 * rather than changing vce, and default to the same 5s.
 *
 * Deliberately dependency-free and clock-injected: this is the piece with all
 * the timing edges, so it has to be testable without timers or a database.
 */

/** States that mean a call is up. Shared with the shaper so ingest and the
 *  legacy (no-window) shaping path can never disagree about what a call is. */
export const LIVE_CALL_STATES = new Set(['CALL', 'ACTIVE', 'ENCRYPTED']);

/**
 * vce reports every processing channel in `activeCalls`, including control
 * channels (no talkgroup — they arrived as a wall of duplicate "TG 0" rows) and
 * granted traffic channels sitting idle BETWEEN calls (no target, no source).
 * A row has to be in a call state AND carry some identity to count.
 */
export function isLiveCallRow(ac: Record<string, unknown>): boolean {
  const state = String(ac['state'] ?? '').toUpperCase();
  if (!LIVE_CALL_STATES.has(state)) return false;
  return !(ac['to'] == null && ac['from'] == null && !ac['toAlias'] && !ac['fromAlias']);
}

/**
 * Call event types that carry AUDIO, by vce's `DecodeEventType` enum NAME
 * (EventBuffer emits `getEventType().name()`, not the display label).
 *
 * Matching the CALL_ prefix and subtracting the signalling members is far more
 * robust than listing the ten audio-bearing ones: a new protocol adding
 * CALL_SOMETHING_NEW should show up, whereas a new signalling type is rare and
 * would merely add a harmless zero-duration row.
 */
const CALL_EVENT_PREFIX = /^CALL(_|$)/;
const NON_AUDIO_CALL_EVENTS = new Set([
  'CALL_ALERT', // a page, not a conversation
  'CALL_DETECT', // seen on the control channel, never tuned
  'CALL_DO_NOT_MONITOR',
  'CALL_END', // the terminator of a call already counted
  'CALL_IN_PROGRESS',
  'CALL_NO_TUNER', // grant we could not follow — no audio was ever decoded
  'CALL_TIMEOUT',
  'CALL_UNIQUE_ID',
]);

export interface WindowCall {
  key: string;
  /** The last `activeCalls` row seen for this call, or a row synthesised from a
   *  decode event. Shaped downstream exactly like a live row. */
  raw: Record<string, unknown>;
  firstSeenAt: number;
  lastSeenAt: number;
  /** null while the call is up. */
  endedAt: number | null;
  /** True when this call was never observed live and was recovered from the
   *  event log — it is a record of a call, not a sighting of one. */
  fromEvent: boolean;
}

export interface CallWindowConfig {
  /** How long an ended call stays visible. Matches vce's Now Playing hang. */
  hangMs: number;
  /** Force-end a call we have not seen in a frame for this long. Covers a node
   *  that stops reporting (or reports degraded) while a call is up: without it
   *  the row would sit there indefinitely claiming to be live. */
  staleActiveMs: number;
  maxActivePerNode: number;
  maxEndedPerNode: number;
  maxEventKeysPerNode: number;
  eventKeyTtlMs: number;
  now: () => number;
}

interface StatusLike {
  activeCalls?: unknown;
  events?: unknown;
  components?: Record<string, string>;
}

interface NodeWindow {
  active: Map<string, WindowCall>;
  /** Oldest first. */
  ended: WindowCall[];
  /** Event dedupe key → when we first accepted it. */
  seenEvents: Map<string, number>;
}

const asRows = (v: unknown): Array<Record<string, unknown>> =>
  Array.isArray(v) ? (v.filter((r) => !!r && typeof r === 'object') as Array<Record<string, unknown>>) : [];

const str = (v: unknown): string | null => {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
};

const num = (v: unknown): number | null => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

/**
 * The identity of a call SLOT: channel + timeslot + talkgroup.
 *
 * `from` is deliberately NOT part of this. The calling radio is frequently
 * unresolved when a grant is first seen and populates a frame or two later —
 * keying on it would split one call into two rows and leave the first hanging
 * on screen as a phantom. Instead `from` is metadata with a transition rule
 * (see reconcileFrom), which is what actually distinguishes back-to-back calls
 * on the same talkgroup.
 */
function slotKey(ac: Record<string, unknown>): string {
  const ch = str(ac['channelName']) ?? str(ac['name']) ?? '?';
  const ts = str(ac['timeslot']) ?? '';
  const to = str(ac['to']) ?? str(ac['toAlias']) ?? '?';
  return `${ch}|${ts}|${to}`;
}

/** Who a call is TO — the anchor for matching a decode event against a call we
 *  already tracked. Channel is not usable for this: EventBuffer sets `channel`
 *  to the descriptor text (a frequency or logical channel number), not the
 *  configured channel name activeCalls reports, so the two never compare
 *  equal. */
function targetOf(r: Record<string, unknown>): string | null {
  return str(r['to']) ?? str(r['toAlias']);
}

export class LiveCallWindow {
  private cfg: CallWindowConfig;
  private nodes = new Map<string, NodeWindow>();

  constructor(cfg: Partial<CallWindowConfig> = {}) {
    const envHang = Number(process.env['NODE_LIVE_CALL_HANG_MS']);
    this.cfg = {
      hangMs: Number.isFinite(envHang) && envHang > 0 ? envHang : 5_000,
      staleActiveMs: 30_000,
      maxActivePerNode: 50,
      maxEndedPerNode: 50,
      maxEventKeysPerNode: 200,
      eventKeyTtlMs: 300_000,
      now: () => Date.now(),
      ...cfg,
    };
  }

  private win(nodeId: string): NodeWindow {
    let w = this.nodes.get(nodeId);
    if (!w) {
      w = { active: new Map(), ended: [], seenEvents: new Map() };
      this.nodes.set(nodeId, w);
    }
    return w;
  }

  /**
   * Fold one status frame into the window. Call EXACTLY once per frame received
   * — this is the only place observation state advances, which is what lets
   * reads be free of side effects (a REST poll of a stale status must not keep
   * a call alive).
   */
  observe(nodeId: string, status: StatusLike | null | undefined, atMs?: number): void {
    const at = atMs ?? this.cfg.now();
    const w = this.win(nodeId);

    // An agent that cannot reach sdrtrunk sends EMPTY channels/activeCalls/
    // events and downgrades this component. Empty-because-degraded and
    // empty-because-quiet are otherwise indistinguishable in the frame, and
    // treating the former as "every call ended" is what made one 4s control-API
    // timeout blank the whole view. Trust the flag and hold what we have; the
    // staleness sweep below still stops rows lasting forever.
    const degraded = status?.components?.['sdrtrunk'] === 'unreachable';

    if (!degraded) {
      const seen = new Set<string>();
      for (const ac of asRows(status?.activeCalls)) {
        if (!isLiveCallRow(ac)) continue;
        const key = slotKey(ac);
        seen.add(key);
        const cur = w.active.get(key);
        if (cur) {
          this.advance(w, cur, ac, at);
          continue;
        }
        const revived = this.revive(w, key, ac, at);
        if (revived) continue;
        this.start(w, key, ac, at, false);
      }
      for (const tc of [...w.active.values()]) {
        if (!seen.has(tc.key)) this.retire(w, tc, at);
      }
    }

    for (const tc of [...w.active.values()]) {
      if (at - tc.lastSeenAt > this.cfg.staleActiveMs) this.retire(w, tc, at);
    }

    this.hydrateFromEvents(w, status, at);
    this.prune(w, at);
  }

  /** A frame row matched a call we are already tracking. */
  private advance(w: NodeWindow, cur: WindowCall, ac: Record<string, unknown>, at: number): void {
    const had = str(cur.raw['from']);
    const now = str(ac['from']);
    if (had && now && had !== now) {
      // Same slot, different caller: the previous over finished and a new one
      // started between frames. Close the old call so it gets its own row and
      // its own hang, rather than silently rewriting who was speaking.
      this.retire(w, cur, at);
      this.start(w, cur.key, ac, at, false);
      return;
    }
    // Never downgrade a resolved caller back to unknown — aliases and radio ids
    // resolve mid-call and can drop out of a later frame.
    if (!now && had) {
      cur.raw = { ...ac, from: cur.raw['from'], fromAlias: ac['fromAlias'] ?? cur.raw['fromAlias'] };
    } else {
      cur.raw = ac;
    }
    cur.lastSeenAt = at;
    cur.endedAt = null;
  }

  /** A call that ended within the hang window came back on the same slot —
   *  the same conversation, briefly missed, not a new one. */
  private revive(w: NodeWindow, key: string, ac: Record<string, unknown>, at: number): boolean {
    const from = str(ac['from']);
    const i = w.ended.findIndex((e) => {
      if (e.key !== key || e.fromEvent || e.endedAt === null) return false;
      if (at - e.endedAt > this.cfg.hangMs) return false;
      const had = str(e.raw['from']);
      return !had || !from || had === from;
    });
    if (i < 0) return false;
    const rev = w.ended.splice(i, 1)[0]!;
    rev.raw = ac;
    rev.lastSeenAt = at;
    rev.endedAt = null;
    w.active.set(key, rev);
    return true;
  }

  private start(
    w: NodeWindow,
    key: string,
    raw: Record<string, unknown>,
    at: number,
    fromEvent: boolean,
  ): void {
    w.active.set(key, { key, raw, firstSeenAt: at, lastSeenAt: at, endedAt: null, fromEvent });
    if (w.active.size > this.cfg.maxActivePerNode) {
      let oldest: WindowCall | null = null;
      for (const c of w.active.values()) if (!oldest || c.lastSeenAt < oldest.lastSeenAt) oldest = c;
      if (oldest) w.active.delete(oldest.key);
    }
  }

  private retire(w: NodeWindow, tc: WindowCall, at: number): void {
    w.active.delete(tc.key);
    tc.endedAt = at;
    w.ended.push(tc);
    if (w.ended.length > this.cfg.maxEndedPerNode) w.ended.splice(0, w.ended.length - this.cfg.maxEndedPerNode);
  }

  /**
   * Recover calls from vce's decode-event log.
   *
   * This is the ONLY way a call shorter than the poll interval can ever be
   * shown: it was over before any frame sampled it, so no amount of polling
   * faster would have caught it, but vce logged it with real start and end
   * times. The events array rides along in every status frame and was, until
   * now, stored and never read.
   *
   * It is re-sent whole (the last 20, no cursor) on every frame, so everything
   * here has to be idempotent — hence the dedupe set.
   */
  private hydrateFromEvents(w: NodeWindow, status: StatusLike | null | undefined, at: number): void {
    for (const ev of asRows(status?.events)) {
      const type = String(ev['type'] ?? '').toUpperCase();
      if (!CALL_EVENT_PREFIX.test(type) || NON_AUDIO_CALL_EVENTS.has(type)) continue;

      const timeStart = num(ev['timeStart']);
      const timeEnd = num(ev['timeEnd']);
      // timeEnd is 0 while the call is still in progress — that call is being
      // tracked from activeCalls, so there is nothing to recover.
      if (!timeStart || !timeEnd || timeEnd <= 0 || timeStart <= 0) continue;
      // Zero-length events are signalling vce stamped with one instant, not
      // audio anyone could have heard. They were rendering as calls of no
      // duration, which says nothing and just adds rows.
      if (timeEnd <= timeStart) continue;
      if (timeEnd < at - this.cfg.hangMs) continue; // already older than the hang
      if (timeEnd > at + 60_000) continue; // node clock far ahead — do not trust
      if (!isLiveCallRow({ state: 'CALL', ...ev })) continue;

      const dedupe = `${timeStart}|${type}|${str(ev['from']) ?? ''}|${str(ev['to']) ?? ''}|${str(ev['timeslot']) ?? ''}`;
      if (w.seenEvents.has(dedupe)) continue;
      w.seenEvents.set(dedupe, at);

      // Marked seen BEFORE the overlap test on purpose: an event for a call we
      // already tracked must not be re-tested twenty times a second for the
      // rest of its life in the buffer.
      if (this.alreadyTracked(w, ev, timeStart, timeEnd, at)) continue;

      w.ended.push({
        key: `ev:${dedupe}`,
        raw: {
          state: type.includes('ENCRYPTED') ? 'ENCRYPTED' : 'CALL',
          // No channel name: EventBuffer's `channel` is a descriptor, not the
          // configured name, so claiming it as one would attach the call to a
          // site that does not exist.
          channelName: null,
          from: ev['from'] ?? null,
          fromAlias: ev['fromAlias'] ?? null,
          to: ev['to'] ?? null,
          talkgroup: ev['to'] ?? null,
          toAlias: ev['toAlias'] ?? null,
          talkerAlias: null,
          timeslot: ev['timeslot'] ?? null,
          frequency: null,
          syncPercent: null,
          signalDbfs: null,
        },
        firstSeenAt: timeStart,
        lastSeenAt: timeEnd,
        endedAt: timeEnd,
        fromEvent: true,
      });
      if (w.ended.length > this.cfg.maxEndedPerNode) {
        w.ended.splice(0, w.ended.length - this.cfg.maxEndedPerNode);
      }
    }

    for (const [k, seenAt] of [...w.seenEvents]) {
      if (at - seenAt > this.cfg.eventKeyTtlMs) w.seenEvents.delete(k);
    }
    if (w.seenEvents.size > this.cfg.maxEventKeysPerNode) {
      const drop = w.seenEvents.size - this.cfg.maxEventKeysPerNode;
      let i = 0;
      for (const k of w.seenEvents.keys()) {
        if (i++ >= drop) break;
        w.seenEvents.delete(k);
      }
    }
  }

  /**
   * Did we already see this call live? Anchored on the TALKGROUP plus a time
   * overlap, because a talkgroup carries one conversation at a time — the same
   * target at the same moment is the same call, however many sites heard it.
   *
   * The caller and timeslot only ever REFUTE a match, never require one. vce
   * routinely logs a call event with no source at all, and the timeslot is
   * frequently present on one side and absent on the other; demanding they be
   * equal meant almost nothing matched, so live calls were being duplicated by
   * a "recovered" copy of themselves — which is exactly what this test exists
   * to prevent. Two different callers on one talkgroup at overlapping times are
   * still told apart, since then both values are present and differ.
   */
  private alreadyTracked(
    w: NodeWindow,
    ev: Record<string, unknown>,
    timeStart: number,
    timeEnd: number,
    at: number,
  ): boolean {
    const to = targetOf(ev);
    if (!to) return false;
    const from = str(ev['from']);
    const ts = str(ev['timeslot']);
    const SLACK = 2_000;
    const sameCall = (c: WindowCall): boolean => {
      if (targetOf(c.raw) !== to) return false;
      const cFrom = str(c.raw['from']);
      if (from && cFrom && from !== cFrom) return false;
      const cTs = str(c.raw['timeslot']);
      if (ts && cTs && ts !== cTs) return false;
      return c.firstSeenAt <= timeEnd + SLACK && (c.endedAt ?? at) >= timeStart - SLACK;
    };
    for (const c of w.active.values()) if (sameCall(c)) return true;
    return w.ended.some(sameCall);
  }

  private prune(w: NodeWindow, at: number): boolean {
    const before = w.ended.length;
    w.ended = w.ended.filter((e) => e.endedAt === null || at - e.endedAt <= this.cfg.hangMs);
    return w.ended.length !== before;
  }

  /**
   * What the Live view should show for this node: calls that are up, plus ones
   * that ended within the hang window. Live first, then most recently ended.
   */
  callsFor(nodeId: string): WindowCall[] {
    const w = this.nodes.get(nodeId);
    if (!w) return [];
    this.prune(w, this.cfg.now());
    const ended = [...w.ended].sort((a, b) => (b.endedAt ?? 0) - (a.endedAt ?? 0));
    return [...w.active.values(), ...ended];
  }

  /**
   * Expire ended rows and force-end stale ones, reporting which nodes changed.
   *
   * Needed because expiry is otherwise only driven by incoming frames: a node
   * on the 15s heartbeat (or one that has gone quiet) would leave its last
   * ended rows on screen until it next spoke.
   */
  sweepExpired(): string[] {
    const at = this.cfg.now();
    const changed: string[] = [];
    for (const [nodeId, w] of this.nodes) {
      let dirty = false;
      for (const tc of [...w.active.values()]) {
        if (at - tc.lastSeenAt > this.cfg.staleActiveMs) {
          this.retire(w, tc, at);
          dirty = true;
        }
      }
      if (this.prune(w, at)) dirty = true;
      if (dirty) changed.push(nodeId);
    }
    return changed;
  }

  dropNode(nodeId: string): void {
    this.nodes.delete(nodeId);
  }

  clear(): void {
    this.nodes.clear();
  }
}

export const liveCallWindow = new LiveCallWindow();
