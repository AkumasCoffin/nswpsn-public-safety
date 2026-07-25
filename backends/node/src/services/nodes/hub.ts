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

const CMD_TIMEOUT_MS = 15_000;

class NodeHub {
  private agents = new Map<string, AgentConn>();
  private staff = new Map<string, Set<StaffConn>>();
  private pending = new Map<string, PendingCmd>();

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
    log.info({ nodeId, installId }, 'node agent connected');
    this.broadcastToStaff(nodeId, 'nodePresence', { nodeId, online: true });
  }

  unregisterAgent(nodeId: string, ws: WebSocket): void {
    const cur = this.agents.get(nodeId);
    if (cur && cur.ws === ws) {
      this.agents.delete(nodeId);
      log.info({ nodeId }, 'node agent disconnected');
      this.broadcastToStaff(nodeId, 'nodePresence', { nodeId, online: false });
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
  }

  relayEvent(nodeId: string, data: unknown): void {
    this.broadcastToStaff(nodeId, 'event', { nodeId, event: data });
  }

  relayBinary(nodeId: string, buf: Buffer): void {
    const subs = this.staff.get(nodeId);
    if (!subs) return;
    for (const s of subs) {
      if (s.ws.readyState === s.ws.OPEN) {
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
  }

  unsubscribeStaff(ws: WebSocket): void {
    for (const [nodeId, set] of this.staff) {
      for (const s of set) {
        if (s.ws === ws) set.delete(s);
      }
      if (set.size === 0) this.staff.delete(nodeId);
    }
  }

  private broadcastToStaff(nodeId: string, t: string, data: unknown): void {
    const subs = this.staff.get(nodeId);
    if (!subs) return;
    const msg = envelope(t, data);
    for (const s of subs) {
      if (s.ws.readyState === s.ws.OPEN) {
        try {
          s.ws.send(msg);
        } catch {
          /* ignore */
        }
      }
    }
  }
}

export const hub = new NodeHub();
