/**
 * Node WebSocket endpoints, attached directly to the Node http server's
 * `upgrade` event (not routed through Hono — binary spectrum passthrough and
 * per-connection auth/heartbeat are cleaner on raw `ws`).
 *
 *   /api/node-ws/agent  — a feeder node's agent. Auth: X-Node-Token +
 *                         X-Node-Install headers (a Go client can set them).
 *   /api/node-ws/staff  — a staff browser. Auth: first message
 *                         {t:'auth', token:<supabase jwt>}; must be owner|dev.
 *                         (Browsers can't set headers on a WS handshake.)
 */
import type { Server } from 'node:http';
import type { Socket } from 'node:net';
import { WebSocketServer, type WebSocket } from 'ws';
import { log } from '../lib/log.js';
import { resolveFeederToken } from '../services/auth/nodeToken.js';
import { verifySupabaseToken } from '../services/auth/supabaseJwt.js';
import { hasRole } from '../services/auth/roles.js';
import { hub } from '../services/nodes/hub.js';
import {
  upsertNodeOnHello,
  touchNodeSeen,
  updateNode,
  type HelloMeta,
} from '../services/nodes/registry.js';
import {
  parseEnvelope,
  envelope,
  PROTOCOL_VERSION,
  type HelloData,
  type StatusData,
} from '../services/nodes/protocol.js';
import { buildConfigPayload } from '../services/nodes/configMerge.js';

const AGENT_PATH = '/api/node-ws/agent';
const STAFF_PATH = '/api/node-ws/staff';
const MAX_PAYLOAD = 1 * 1024 * 1024; // 1MB — status/spectrum frames are far smaller
const HEARTBEAT_MS = 30_000; // Cloudflare Tunnel kills idle WS ~100s
const STAFF_AUTH_GRACE_MS = 5_000;

interface Alive extends WebSocket {
  isAlive?: boolean;
}

function rejectUpgrade(socket: Socket, code: number, msg: string): void {
  socket.write(`HTTP/1.1 ${code} ${msg}\r\nConnection: close\r\n\r\n`);
  socket.destroy();
}

export function attachNodeWebSockets(server: Server): void {
  const agentWss = new WebSocketServer({ noServer: true, maxPayload: MAX_PAYLOAD });
  const staffWss = new WebSocketServer({ noServer: true, maxPayload: MAX_PAYLOAD });

  server.on('upgrade', (req, socket, head) => {
    let pathname = '';
    try {
      pathname = new URL(req.url ?? '', 'http://localhost').pathname;
    } catch {
      rejectUpgrade(socket as Socket, 400, 'Bad Request');
      return;
    }

    if (pathname === AGENT_PATH) {
      void handleAgentUpgrade(agentWss, req, socket as Socket, head);
    } else if (pathname === STAFF_PATH) {
      staffWss.handleUpgrade(req, socket as Socket, head, (ws) => {
        staffWss.emit('connection', ws, req);
      });
    } else {
      rejectUpgrade(socket as Socket, 404, 'Not Found');
    }
  });

  agentWss.on('connection', (ws: Alive, ctx: AgentCtx) => {
    setupAgentConnection(ws, ctx);
  });
  staffWss.on('connection', (ws: Alive) => {
    setupStaffConnection(ws);
  });

  // Single heartbeat sweep for both servers.
  const interval = setInterval(() => {
    for (const wss of [agentWss, staffWss]) {
      for (const client of wss.clients) {
        const c = client as Alive;
        if (c.isAlive === false) {
          c.terminate();
          continue;
        }
        c.isAlive = false;
        try {
          c.ping();
        } catch {
          /* ignore */
        }
      }
    }
  }, HEARTBEAT_MS);
  interval.unref?.();

  log.info('node WebSocket endpoints attached (agent + staff)');
}

// ── agent ────────────────────────────────────────────────────────────────
interface AgentCtx {
  userId: string;
  installId: string;
  nodeId: string;
}

async function handleAgentUpgrade(
  wss: WebSocketServer,
  req: import('node:http').IncomingMessage,
  socket: Socket,
  head: Buffer,
): Promise<void> {
  const token = (req.headers['x-node-token'] as string | undefined) ?? '';
  const installId = (req.headers['x-node-install'] as string | undefined) ?? '';
  if (!installId) {
    rejectUpgrade(socket, 400, 'Missing Install Id');
    return;
  }
  const resolved = await resolveFeederToken(token);
  if (!resolved.ok) {
    rejectUpgrade(socket, resolved.reason === 'no_role' ? 403 : 401, 'Unauthorized');
    return;
  }
  // Create/refresh the node row now so we have its id (auto-link on start).
  const node = await upsertNodeOnHello(resolved.userId, installId, {});
  if (!node) {
    rejectUpgrade(socket, 503, 'Registry Unavailable');
    return;
  }
  if (!node.enabled) {
    rejectUpgrade(socket, 403, 'Node Disabled');
    return;
  }
  const ctx: AgentCtx = { userId: resolved.userId, installId, nodeId: node.id };
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, ctx);
  });
}

function setupAgentConnection(ws: Alive, ctx: AgentCtx): void {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  hub.registerAgent(ctx.nodeId, ws, ctx.userId, ctx.installId);

  ws.on('message', (raw: Buffer, isBinary: boolean) => {
    if (isBinary) {
      hub.relayBinary(ctx.nodeId, raw);
      return;
    }
    const msg = parseEnvelope(raw.toString('utf8'));
    if (!msg) return;
    void handleAgentMessage(ws, ctx, msg.t, msg.id, msg.data);
  });

  ws.on('close', () => hub.unregisterAgent(ctx.nodeId, ws));
  ws.on('error', (err) => log.debug({ err, nodeId: ctx.nodeId }, 'agent ws error'));
}

async function handleAgentMessage(
  ws: Alive,
  ctx: AgentCtx,
  t: string,
  id: string | undefined,
  data: unknown,
): Promise<void> {
  switch (t) {
    case 'hello': {
      const h = (data ?? {}) as HelloData;
      const meta: HelloMeta = {
        agentVersion: h.agentVersion ?? null,
        sdrtrunkVersion: h.sdrtrunkVersion ?? null,
        rdioVersion: h.rdioVersion ?? null,
        os: h.os ?? null,
        arch: h.arch ?? null,
        hostname: h.hostname ?? null,
      };
      const node = await upsertNodeOnHello(ctx.userId, ctx.installId, meta);
      ws.send(
        envelope('helloAck', {
          ok: true,
          serverProtocolVersion: PROTOCOL_VERSION,
          configVersion: node?.config_version ?? null,
        }),
      );
      // If the agent's applied config drifts from what we'd build now (or it
      // reports nothing applied), push the current merged config right after
      // the ack. Defensive: any preset/build failure just skips the push and
      // the agent runs on whatever it last applied.
      if (node) {
        try {
          const payload = buildConfigPayload(node);
          const applied = h.appliedConfigVersion ?? null;
          if (applied !== payload.configVersion) {
            hub.sendToAgent(ctx.nodeId, 'configPush', payload);
            log.info(
              { nodeId: ctx.nodeId, applied, target: payload.configVersion },
              'hello: pushed config (version mismatch)',
            );
          }
        } catch (err) {
          log.debug({ err, nodeId: ctx.nodeId }, 'hello: config push skipped');
        }
      }
      return;
    }
    case 'status': {
      hub.recordStatus(ctx.nodeId, (data ?? {}) as StatusData);
      void touchNodeSeen(ctx.nodeId);
      return;
    }
    case 'event': {
      hub.relayEvent(ctx.nodeId, data);
      return;
    }
    case 'configApplied': {
      const cv = (data as { configVersion?: string } | undefined)?.configVersion ?? null;
      await updateNode(ctx.nodeId, { config_version: cv });
      return;
    }
    case 'configError': {
      hub.relayEvent(ctx.nodeId, { kind: 'configError', ...(data as object) });
      return;
    }
    case 'cmdResult': {
      if (id) {
        hub.resolveCmd(id, (data ?? { ok: false }) as { ok: boolean; message?: string });
      }
      return;
    }
    default:
      log.debug({ t, nodeId: ctx.nodeId }, 'unknown agent message');
  }
}

// ── staff ────────────────────────────────────────────────────────────────
interface StaffState {
  authed: boolean;
  userId: string | null;
}

function setupStaffConnection(ws: Alive): void {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  const state: StaffState = { authed: false, userId: null };

  // Must authenticate within the grace window or get dropped.
  const authTimer = setTimeout(() => {
    if (!state.authed) {
      try { ws.close(4001, 'auth timeout'); } catch { /* ignore */ }
    }
  }, STAFF_AUTH_GRACE_MS);
  authTimer.unref?.();

  ws.on('message', (raw: Buffer, isBinary: boolean) => {
    if (isBinary) return; // staff never sends binary
    const msg = parseEnvelope(raw.toString('utf8'));
    if (!msg) return;
    void handleStaffMessage(ws, state, msg.t, msg.data, authTimer);
  });

  ws.on('close', () => {
    clearTimeout(authTimer);
    hub.unsubscribeStaff(ws);
  });
  ws.on('error', (err) => log.debug({ err }, 'staff ws error'));
}

async function handleStaffMessage(
  ws: Alive,
  state: StaffState,
  t: string,
  data: unknown,
  authTimer: ReturnType<typeof setTimeout>,
): Promise<void> {
  if (t === 'auth') {
    const token = (data as { token?: string } | undefined)?.token ?? '';
    const verified = await verifySupabaseToken(token);
    if (!verified || !(await hasRole(verified.userId, ['owner', 'dev']))) {
      ws.send(envelope('authError', { message: 'forbidden' }));
      try { ws.close(4003, 'forbidden'); } catch { /* ignore */ }
      return;
    }
    state.authed = true;
    state.userId = verified.userId;
    clearTimeout(authTimer);
    ws.send(envelope('authOk', { userId: verified.userId }));
    return;
  }

  if (!state.authed) {
    ws.send(envelope('authError', { message: 'not authenticated' }));
    return;
  }

  switch (t) {
    case 'subscribeNode': {
      const nodeId = (data as { nodeId?: string } | undefined)?.nodeId;
      if (!nodeId) return;
      hub.subscribeStaff(nodeId, ws, state.userId!);
      const live = hub.liveStatus(nodeId);
      ws.send(envelope('nodeStatus', { nodeId, status: live.status, online: live.online }));
      return;
    }
    case 'unsubscribeNode': {
      hub.unsubscribeStaff(ws);
      return;
    }
    case 'cmd': {
      const d = (data ?? {}) as { nodeId?: string; action?: string; args?: unknown; id?: string };
      if (!d.nodeId || !d.action) return;
      log.info({ nodeId: d.nodeId, action: d.action, by: state.userId }, 'staff node command');
      const result = await hub.sendCmd(d.nodeId, d.action, d.args);
      ws.send(envelope('cmdResult', { nodeId: d.nodeId, reqId: d.id, ...result }));
      return;
    }
    case 'spectrumStart':
    case 'spectrumStop': {
      const d = (data ?? {}) as { nodeId?: string };
      if (!d.nodeId) return;
      hub.sendToAgent(d.nodeId, t, data);
      return;
    }
    default:
      log.debug({ t }, 'unknown staff message');
  }
}
