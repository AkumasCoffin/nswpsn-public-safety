/**
 * In-memory hub of live node-agent + staff WebSocket connections.
 *
 * Routes:
 *   - agent status/event  → all staff subscribed to that node
 *   - agent binary frames → all staff subscribed to that node (spectrum)
 *   - staff cmd/spectrum  → the node's agent
 *   - backend cmd         → the agent, awaiting a correlated cmdResult
 *
 * Ephemeral only: nothing here is persisted (registry.ts owns the DB). A
 * process restart drops all sockets; agents reconnect, staff re-subscribe.
 */
import type { WebSocket } from 'ws';
import { randomUUID } from 'node:crypto';
import { log } from '../../lib/log.js';
import { envelope, type StatusData } from './protocol.js';

interface AgentConn {
  ws: WebSocket;
  nodeId: string;
  userId: string;
  installId: string;
  lastStatus: StatusData | null;
  lastStatusAt: number | null;
  connectedAt: number;
}

interface StaffConn {
  ws: WebSocket;
  userId: string;
}

interface PendingCmd {
  resolve: (v: { ok: boolean; message?: string; data?: unknown }) => void;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * Turns one node's raw status into the Live view's rows. Supplied by the WS
 * route (see setLiveShaper) rather than imported here, so the hub keeps its
 * "connections only, no DB" shape. Returning null means "nothing to show for
 * this node" (a pager node, or one that hasn't reported yet).
 */
type LiveShaper = (
  nodeId: string,
  status: StatusData | null,
  lastStatusAt: number | null,
) => Promise<unknown | null>;

const CMD_TIMEOUT_MS = 15_000;

const UPLOAD_WINDOW_MS = 10 * 60 * 1000; // rolling "calls in the last 10 min"

// How many recent pager messages to keep per node so opening the staff drawer
// shows history immediately (in-memory, ephemeral — a live convenience buffer).
const PAGER_BUFFER = 50;

/** One decoded pager page kept in the per-node ring buffer / streamed to staff. */
export interface PagerMessageView {
  address: string;
  message: string;
  source: string; // reader label the page was heard on (e.g. NSWRFS / FRNSW)
  freqMhz: number | null;
  at: number; // epoch ms received
  /** Set when the message was FILTERED (not forwarded to Pagermon) — the reason,
   *  e.g. "blocked capcode". Shown dimmed in the drawer so staff can see what's
   *  being dropped. Absent for normal forwarded pages. */
  filtered?: string;
}

class NodeHub {
  private agents = new Map<string, AgentConn>();
  private staff = new Map<string, Set<StaffConn>>();
  // Staff watching the FLEET-wide Live view. Separate from `staff` (which is
  // keyed per node, for the drawer): a Live viewer wants every radio node at
  // once and has no single nodeId to key on.
  private liveStaff = new Set<StaffConn>();
  // Shapes one node's status into Live rows. Injected by the WS route so the
  // hub stays free of DB/config imports (it is pure connection plumbing).
  private liveShaper: LiveShaper | null = null;
  private pending = new Map<string, PendingCmd>();
  // Per-node timestamps of calls actually forwarded to central rdio (fed by the
  // relay). In-memory + ephemeral: a rolling live signal, not durable stats
  // (node_call_stats owns the persisted per-day totals).
  private uploads = new Map<string, number[]>();
  // Per-node ring buffer of recent decoded pager messages (newest last), so the
  // staff drawer can show history the moment it subscribes.
  private recentPager = new Map<string, PagerMessageView[]>();
  // Per-node "self-update in progress until" epoch-ms. Set when the agent signals
  // it's about to swap+re-exec; while set (and until it reconnects) the node is
  // reported as UPDATING rather than offline, so the brief disconnect during an
  // update doesn't flash as "offline".
  private updatingUntil = new Map<string, number>();

  /** Mark a node as updating for the next `ms` (agent about to swap+re-exec). */
  markUpdating(nodeId: string, ms = 120_000): void {
    this.updatingUntil.set(nodeId, Date.now() + ms);
    log.info({ nodeId }, 'node self-update in progress (marked updating)');
  }

  /** Whether a node is mid self-update (signalled + within the window). */
  isUpdating(nodeId: string): boolean {
    const t = this.updatingUntil.get(nodeId);
    if (t == null) return false;
    if (Date.now() >= t) {
      this.updatingUntil.delete(nodeId);
      return false;
    }
    return true;
  }

  /** For a PAGER node, the reader labels currently decoding (e.g. ['NSWRFS','FRNSW']),
   *  derived from the live status components. Empty when offline / none running. */
  pagerDecoding(nodeId: string): string[] {
    const comps = this.agents.get(nodeId)?.lastStatus?.components;
    if (!comps) return [];
    return Object.entries(comps)
      .filter(([k, v]) => k.startsWith('reader') && String(v).toLowerCase().includes('run'))
      .map(([k]) => k.replace(/^reader:?/, '') || k);
  }

  /** The recent decoded-pager buffer for a node (newest last), for the owner's
   *  "recent messages" view on the feeder page (which is REST, not on the staff
   *  WS). Empty when nothing buffered. */
  recentPagerMessages(nodeId: string): PagerMessageView[] {
    return this.recentPager.get(nodeId) ?? [];
  }

  /** Record a decoded pager message: keep it in the ring + push live to staff. */
  recordPagerMessage(nodeId: string, msg: PagerMessageView): void {
    const arr = this.recentPager.get(nodeId) ?? [];
    arr.push(msg);
    if (arr.length > PAGER_BUFFER) arr.splice(0, arr.length - PAGER_BUFFER);
    this.recentPager.set(nodeId, arr);
    this.broadcastToStaff(nodeId, 'pagerMessage', { nodeId, message: msg });
  }

  /** Drop all ephemeral per-node state (upload window + pager message buffer).
   *  Call when a node is deleted so nothing lingers for a gone node. */
  clearNode(nodeId: string): void {
    this.uploads.delete(nodeId);
    this.recentPager.delete(nodeId);
    this.updatingUntil.delete(nodeId);
  }

  /** Record one call successfully forwarded for a node (called by the relay). */
  recordUpload(nodeId: string): void {
    const now = Date.now();
    const arr = this.uploads.get(nodeId) ?? [];
    arr.push(now);
    // Prune anything outside the window so the array can't grow unbounded.
    const cutoff = now - UPLOAD_WINDOW_MS;
    let i = 0;
    while (i < arr.length && arr[i]! < cutoff) i++;
    this.uploads.set(nodeId, i > 0 ? arr.slice(i) : arr);
  }

  /** Count calls forwarded for a node within the last `windowMs` (default 10 min). */
  uploadsInWindow(nodeId: string, windowMs: number = UPLOAD_WINDOW_MS): number {
    const arr = this.uploads.get(nodeId);
    if (!arr || arr.length === 0) return 0;
    const cutoff = Date.now() - windowMs;
    let count = 0;
    for (let i = arr.length - 1; i >= 0 && arr[i]! >= cutoff; i--) count++;
    return count;
  }

  // ── agents ───────────────────────────────────────────────────────────
  registerAgent(
    nodeId: string,
    ws: WebSocket,
    userId: string,
    installId: string,
  ): void {
    const existing = this.agents.get(nodeId);
    if (existing && existing.ws !== ws) {
      // One connection per node — replace the old one.
      try {
        existing.ws.close(4000, 'replaced by new connection');
      } catch {
        /* ignore */
      }
    }
    this.agents.set(nodeId, {
      ws,
      nodeId,
      userId,
      installId,
      lastStatus: null,
      lastStatusAt: null,
      connectedAt: Date.now(),
    });
    // A fresh connection means any in-flight self-update finished (the agent
    // re-execed and reconnected), so clear the updating marker.
    this.updatingUntil.delete(nodeId);
    log.info({ nodeId, installId }, 'node agent connected');
    this.broadcastToStaff(nodeId, 'nodePresence', { nodeId, online: true });
  }

  unregisterAgent(nodeId: string, ws: WebSocket): void {
    const cur = this.agents.get(nodeId);
    if (cur && cur.ws === ws) {
      this.agents.delete(nodeId);
      log.info({ nodeId }, 'node agent disconnected');
      this.broadcastToStaff(nodeId, 'nodePresence', { nodeId, online: false });
      // Live is "what is decoding RIGHT NOW" — a node that dropped has no rows
      // any more. Without this its last frame would sit there looking live.
      this.broadcastLive('liveNodeOffline', { nodeId });
    }
  }

  isOnline(nodeId: string): boolean {
    return this.agents.has(nodeId);
  }

  /** Snapshot of connected agents, for periodic auth (role/enabled) revalidation. */
  agentList(): Array<{ nodeId: string; userId: string; installId: string }> {
    return Array.from(this.agents.values()).map((a) => ({
      nodeId: a.nodeId,
      userId: a.userId,
      installId: a.installId,
    }));
  }

  liveStatus(nodeId: string): {
    online: boolean;
    status: StatusData | null;
    lastStatusAt: number | null;
  } {
    const a = this.agents.get(nodeId);
    return {
      online: !!a,
      status: a?.lastStatus ?? null,
      lastStatusAt: a?.lastStatusAt ?? null,
    };
  }

  recordStatus(nodeId: string, status: StatusData): void {
    const a = this.agents.get(nodeId);
    if (!a) return;
    a.lastStatus = status;
    a.lastStatusAt = Date.now();
    this.broadcastToStaff(nodeId, 'nodeStatus', { nodeId, status });
    // Fleet Live viewers get the same report reshaped. This is what makes the
    // Live tab as current as the vce panel: it repaints when the agent speaks,
    // instead of on a poll timer that is stale by up to its whole interval.
    this.pushLive(nodeId, status, a.lastStatusAt);
  }

  /** Shape + fan one node's status out to fleet-Live subscribers. */
  private pushLive(nodeId: string, status: StatusData | null, at: number | null): void {
    if (this.liveStaff.size === 0 || !this.liveShaper) return;
    void this.liveShaper(nodeId, status, at)
      .then((slice) => {
        if (!slice) return; // pager node / nothing reported
        this.broadcastLive('liveNode', slice);
      })
      .catch((err) => log.debug({ err, nodeId }, 'hub: live shape failed'));
  }

  relayEvent(nodeId: string, data: unknown): void {
    this.broadcastToStaff(nodeId, 'event', { nodeId, event: data });
  }

  relayBinary(nodeId: string, buf: Buffer): void {
    const subs = this.staff.get(nodeId);
    if (!subs) return;
    for (const s of subs) {
      if (s.ws.readyState === s.ws.OPEN) {
        // Backpressure: a fast agent flooding spectrum frames into a slow staff
        // socket would otherwise grow this process's send buffer without bound.
        // Spectrum frames are disposable, so drop rather than buffer.
        if (wsOverBuffered(s.ws)) continue;
        try {
          s.ws.send(buf, { binary: true });
        } catch {
          /* ignore */
        }
      }
    }
  }

  resolveCmd(id: string, result: { ok: boolean; message?: string; data?: unknown }): void {
    const p = this.pending.get(id);
    if (!p) return;
    clearTimeout(p.timer);
    this.pending.delete(id);
    p.resolve(result);
  }

  /**
   * Send a command to a node's agent and await its cmdResult. Rejects
   * (resolves ok:false) if the node is offline or the agent doesn't answer
   * within the timeout.
   */
  sendCmd(
    nodeId: string,
    action: string,
    args?: unknown,
  ): Promise<{ ok: boolean; message?: string; data?: unknown }> {
    const a = this.agents.get(nodeId);
    if (!a || a.ws.readyState !== a.ws.OPEN) {
      return Promise.resolve({ ok: false, message: 'node offline' });
    }
    const id = randomUUID();
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        resolve({ ok: false, message: 'command timed out' });
      }, CMD_TIMEOUT_MS);
      this.pending.set(id, { resolve, timer });
      try {
        a.ws.send(envelope('cmd', { action, args }, id));
      } catch (err) {
        clearTimeout(timer);
        this.pending.delete(id);
        resolve({ ok: false, message: (err as Error).message });
      }
    });
  }

  /** Fire-and-forget message to a node's agent (e.g. config push, spectrum). */
  sendToAgent(nodeId: string, t: string, data?: unknown, id?: string): boolean {
    const a = this.agents.get(nodeId);
    if (!a || a.ws.readyState !== a.ws.OPEN) return false;
    try {
      a.ws.send(envelope(t, data, id));
      return true;
    } catch {
      return false;
    }
  }

  /** Close a node's agent connection (called when staff disables the node). */
  forceDisconnectAgent(nodeId: string, reason = 'node disabled'): void {
    const a = this.agents.get(nodeId);
    if (!a) return;
    try {
      a.ws.send(envelope('disabled', { reason }));
      a.ws.close(4003, reason);
    } catch {
      /* ignore */
    }
  }

  // ── staff ────────────────────────────────────────────────────────────
  subscribeStaff(nodeId: string, ws: WebSocket, userId: string): void {
    let set = this.staff.get(nodeId);
    if (!set) {
      set = new Set();
      this.staff.set(nodeId, set);
    }
    // Drop any prior entry for this socket first, so a re-subscribe (reconnect or
    // UI re-entry) can't leave two StaffConn entries that double-deliver every
    // status/event/spectrum frame to the same viewer.
    for (const s of set) {
      if (s.ws === ws) set.delete(s);
    }
    set.add({ ws, userId });
    // Replay the recent pager message buffer so the drawer shows history at once.
    const recent = this.recentPager.get(nodeId);
    if (recent && recent.length && ws.readyState === ws.OPEN) {
      try {
        ws.send(envelope('pagerHistory', { nodeId, messages: recent }));
      } catch {
        /* ignore */
      }
    }
  }

  unsubscribeStaff(ws: WebSocket): void {
    for (const [nodeId, set] of this.staff) {
      for (const s of set) {
        if (s.ws === ws) set.delete(s);
      }
      if (set.size === 0) this.staff.delete(nodeId);
    }
  }

  // ── staff: fleet-wide Live view ──────────────────────────────────────
  /** Install the status→Live-rows shaper (called once, at route setup). */
  setLiveShaper(fn: LiveShaper): void {
    this.liveShaper = fn;
  }

  subscribeLiveStaff(ws: WebSocket, userId: string): void {
    // Same de-dupe as subscribeStaff: a re-subscribe (reconnect, or the user
    // leaving and re-entering the view) must not leave two entries that
    // double-deliver every frame to one viewer.
    for (const s of this.liveStaff) {
      if (s.ws === ws) this.liveStaff.delete(s);
    }
    this.liveStaff.add({ ws, userId });
  }

  unsubscribeLiveStaff(ws: WebSocket): void {
    for (const s of this.liveStaff) {
      if (s.ws === ws) this.liveStaff.delete(s);
    }
  }

  /** Every online node's current Live slice — the first paint on subscribe. */
  async liveSnapshot(): Promise<unknown[]> {
    if (!this.liveShaper) return [];
    const out: unknown[] = [];
    for (const a of this.agents.values()) {
      try {
        const slice = await this.liveShaper(a.nodeId, a.lastStatus, a.lastStatusAt);
        if (slice) out.push(slice);
      } catch (err) {
        log.debug({ err, nodeId: a.nodeId }, 'hub: live snapshot shape failed');
      }
    }
    return out;
  }

  private broadcastLive(t: string, data: unknown): void {
    const msg = envelope(t, data);
    for (const s of this.liveStaff) {
      if (s.ws.readyState === s.ws.OPEN) {
        if (wsOverBuffered(s.ws)) continue;
        try {
          s.ws.send(msg);
        } catch {
          /* ignore */
        }
      }
    }
  }

  private broadcastToStaff(nodeId: string, t: string, data: unknown): void {
    const subs = this.staff.get(nodeId);
    if (!subs) return;
    const msg = envelope(t, data);
    for (const s of subs) {
      if (s.ws.readyState === s.ws.OPEN) {
        if (wsOverBuffered(s.ws)) continue;
        try {
          s.ws.send(msg);
        } catch {
          /* ignore */
        }
      }
    }
  }
}

/** Skip sending when a socket's outbound buffer is already backed up, so a slow
 *  or stalled staff client can't force unbounded memory growth in this process. */
const MAX_WS_BUFFER_BYTES = 4 * 1024 * 1024;
function wsOverBuffered(ws: { bufferedAmount?: number }): boolean {
  return (ws.bufferedAmount ?? 0) > MAX_WS_BUFFER_BYTES;
}

export const hub = new NodeHub();
