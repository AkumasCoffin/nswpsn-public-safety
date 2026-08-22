/**
 * A short ROLLING MEMORY of the calls each node has reported, so a call does
 * not blink out of the Live view because one frame happened to miss it.
 *
 * Why this has to exist at all: nothing anywhere else in the chain remembers a
 * call. vce rebuilds `activeCalls` from live decoder state on every request and
 * filters out the TEARDOWN/FADE rows, the agent rebuilds its slice from that
 * response, and the hub stores only the last frame. So a call was on screen if
 * and only if it happened to be mid-transmission at the instant of a poll.
 *
 * vce's own web Live page does not have this problem because it is not a list
 * of calls at all: it keeps a stable row per CHANNEL that changes state, and a
 * traffic grant ages out 1s after the call ends (NowPlayingPreference
 * DEFAULT_TRAFFIC_GRANT_AGE_OUT_MILLISECONDS) at which point the row goes IDLE
 * rather than disappearing. With its "Active only" filter on — the mode we
 * match — an idle row is simply hidden, so a finished call stays up for that
 * 1s and then goes.
 *
 * So the hang here is that same 1s grant age-out, and nothing more: it exists
 * to stop a call flickering out when a single frame misses it (vce drops rows
 * from a poll on an index race, and a degraded frame reports none at all), not
 * to keep finished calls on screen.
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

export interface WindowCall {
  key: string;
  /** The last `activeCalls` row seen for this call. */
  raw: Record<string, unknown>;
  firstSeenAt: number;
  lastSeenAt: number;
  /** null while the call is up. */
  endedAt: number | null;
}

export interface CallWindowConfig {
  /** Grace after a call stops being reported. vce's traffic-grant age-out. */
  hangMs: number;
  /** Force-end a call we have not seen in a frame for this long. Covers a node
   *  that stops reporting (or reports degraded) while a call is up: without it
   *  the row would sit there indefinitely claiming to be live. */
  staleActiveMs: number;
  maxActivePerNode: number;
  maxEndedPerNode: number;
  now: () => number;
}

interface StatusLike {
  activeCalls?: unknown;
  components?: Record<string, string>;
}

interface NodeWindow {
  active: Map<string, WindowCall>;
  /** Oldest first. */
  ended: WindowCall[];
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

export class LiveCallWindow {
  private cfg: CallWindowConfig;
  private nodes = new Map<string, NodeWindow>();

  constructor(cfg: Partial<CallWindowConfig> = {}) {
    const envHang = Number(process.env['NODE_LIVE_CALL_HANG_MS']);
    this.cfg = {
      hangMs: Number.isFinite(envHang) && envHang > 0 ? envHang : 1_000,
      staleActiveMs: 30_000,
      maxActivePerNode: 50,
      maxEndedPerNode: 50,
      now: () => Date.now(),
      ...cfg,
    };
  }

  private win(nodeId: string): NodeWindow {
    let w = this.nodes.get(nodeId);
    if (!w) {
      w = { active: new Map(), ended: [] };
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
        this.start(w, key, ac, at);
      }
      for (const tc of [...w.active.values()]) {
        if (!seen.has(tc.key)) this.retire(w, tc, at);
      }
    }

    for (const tc of [...w.active.values()]) {
      if (at - tc.lastSeenAt > this.cfg.staleActiveMs) this.retire(w, tc, at);
    }

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
      this.start(w, cur.key, ac, at);
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
      if (e.key !== key || e.endedAt === null) return false;
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
  ): void {
    w.active.set(key, { key, raw, firstSeenAt: at, lastSeenAt: at, endedAt: null });
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
