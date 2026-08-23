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
import { resolveNodeToken } from '../services/auth/nodeToken.js';
import { verifySupabaseToken } from '../services/auth/supabaseJwt.js';
import { hasRole } from '../services/auth/roles.js';
import { hub } from '../services/nodes/hub.js';
import {
  refreshNodeOnHello,
  bindInstallId,
  touchNodeSeen,
  updateNode,
  getNode,
  type HelloMeta,
} from '../services/nodes/registry.js';
import {
  parseEnvelope,
  envelope,
  PROTOCOL_VERSION,
  isAgentCommandAction,
  type HelloData,
  type StatusData,
} from '../services/nodes/protocol.js';
import { buildConfigPayload } from '../services/nodes/configMerge.js';
import { shapeNodeLive } from '../services/nodeLive.js';
import { liveCallWindow } from '../services/nodeCallWindow.js';

// The hub is pure connection plumbing and must not import config/DB itself, so
// the Live row shaper is handed to it from here (module load = route setup).
// Calls come from the rolling window rather than the raw frame — see
// nodeCallWindow for why the frame alone can't answer "what is on air".
hub.setLiveShaper((nodeId, status, at) =>
  shapeNodeLive(nodeId, status, at, liveCallWindow.callsFor(nodeId)),
);

// Live rows outlive the frame that produced them, so their expiry needs its own
// heartbeat: without this the last ended call of a quiet node would sit on
// screen until that node next reported (up to 15s on the slow cadence).
// sweepExpired only names nodes that actually changed, and refreshLive is a
// no-op when nobody is watching.
const LIVE_EXPIRY_SWEEP_MS = 2_000;
const liveExpirySweep = setInterval(() => {
  for (const nodeId of liveCallWindow.sweepExpired()) hub.refreshLive(nodeId);
}, LIVE_EXPIRY_SWEEP_MS);
liveExpirySweep.unref?.();

const AGENT_PATH = '/api/node-ws/agent';
const STAFF_PATH = '/api/node-ws/staff';
const MAX_PAYLOAD = 1 * 1024 * 1024; // 1MB — status/spectrum frames are far smaller
const HEARTBEAT_MS = 30_000; // Cloudflare Tunnel kills idle WS ~100s
const AUTH_REVALIDATE_MS = 60_000; // re-check role + enabled on connected agents
const STAFF_AUTH_GRACE_MS = 5_000;

interface Alive extends WebSocket {
  isAlive?: boolean;
}

function rejectUpgrade(socket: Socket, code: number, msg: string): void {
  try {
    if (!socket.destroyed) {
      socket.write(`HTTP/1.1 ${code} ${msg}\r\nConnection: close\r\n\r\n`);
    }
  } catch {
    // best effort — the socket may already be gone
  }
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
      // Never let a failed hello (DB error, constraint violation, ...) escape as
      // an unhandledRejection — that takes down the whole API process.
      handleAgentUpgrade(agentWss, req, socket as Socket, head).catch((e) => {
        log.error({ err: e }, 'agent WS upgrade failed');
        rejectUpgrade(socket as Socket, 500, 'Internal Error');
      });
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

  // Periodic auth revalidation. Role (radio_contributor) + node.enabled are
  // checked at upgrade, but a role revocation or a disable on an ALREADY-connected
  // agent otherwise wouldn't take effect until the socket happened to drop. Sweep
  // connected agents and force-disconnect any that no longer pass; reconnect is
  // then rejected at the upgrade check.
  const authSweep = setInterval(() => {
    void (async () => {
      for (const a of hub.agentList()) {
        try {
          if (!(await hasRole(a.userId, ['feeder:radio']))) {
            hub.forceDisconnectAgent(a.nodeId, 'role revoked');
            continue;
          }
          // A DELETED node (hard revoke) must be cut off; a DISABLED node stays
          // connected (enabled now controls capture via config, not the socket).
          const node = await getNode(a.nodeId);
          if (!node) {
            hub.forceDisconnectAgent(a.nodeId, 'node deleted');
          }
        } catch (err) {
          log.warn({ err, nodeId: a.nodeId }, 'agent auth revalidation failed (transient)');
        }
      }
    })();
  }, AUTH_REVALIDATE_MS);
  authSweep.unref?.();

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
  // Diagnostic: enough to compare the presented token/install against what a
  // working curl sends, without logging the full secret.
  const tokenPrefix = token ? token.slice(0, 14) : '(none)';
  const tokenLen = token.length;
  if (!installId) {
    log.warn({ tokenPrefix, tokenLen }, 'agent WS reject: missing install id');
    rejectUpgrade(socket, 400, 'Missing Install Id');
    return;
  }
  // install_id is an attacker-chosen header that keys a DB row; require the
  // agent's generated shape (lowercase hex/UUID-ish, bounded) so it can't be
  // used to spray arbitrary/oversized keys.
  if (!/^[A-Za-z0-9._-]{8,64}$/.test(installId)) {
    log.warn({ tokenPrefix, tokenLen }, 'agent WS reject: malformed install id');
    rejectUpgrade(socket, 400, 'Invalid Install Id');
    return;
  }
  // Per-node token → the pre-created node it belongs to (no auto-create).
  const resolved = await resolveNodeToken(token);
  if (!resolved.ok) {
    log.warn(
      { tokenPrefix, tokenLen, installId, reason: resolved.reason },
      'agent WS reject: token resolve failed',
    );
    rejectUpgrade(socket, resolved.reason === 'no_role' ? 403 : 401, 'Unauthorized');
    return;
  }
  // TOFU: bind this machine to the node on first connect; reject a DIFFERENT
  // machine presenting the same node token (a copied credential).
  const bind = await bindInstallId(resolved.nodeId, installId);
  if (bind === 'mismatch') {
    log.warn(
      { tokenPrefix, installId, nodeId: resolved.nodeId },
      'agent WS reject: install id does not match the node this token is bound to',
    );
    rejectUpgrade(socket, 401, 'Install Mismatch');
    return;
  }
  // enabled is NOT a connection gate anymore — a disabled node stays connected
  // and simply stops capturing (driven via the pushed config).
  log.info(
    { tokenPrefix, installId, userId: resolved.userId, nodeId: resolved.nodeId, kind: resolved.kind, bind },
    'agent WS accepted',
  );
  const ctx: AgentCtx = { userId: resolved.userId, installId, nodeId: resolved.nodeId };
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

  ws.on('close', () => {
    // Drop the call window with the connection: the client removes the node's
    // whole slice on liveNodeOffline, so holding ended calls across a
    // disconnect would only resurrect them if it reconnected inside the hang.
    liveCallWindow.dropNode(ctx.nodeId);
    hub.unregisterAgent(ctx.nodeId, ws);
  });
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
      const node = await refreshNodeOnHello(ctx.nodeId, meta);
      // The agent declares its kind; warn (don't reject) on a mismatch with the
      // type the node was created as, so a wrong-agent install is visible.
      if (node && h.kind && h.kind !== node.kind) {
        log.warn({ nodeId: ctx.nodeId, nodeKind: node.kind, agentKind: h.kind }, 'hello: node kind mismatch');
      }
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
          const payload = await buildConfigPayload(node);
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
      const st = (data ?? {}) as StatusData;
      // Fold the frame into the call window BEFORE the hub fans it out:
      // recordStatus triggers the reshape, and the shaper reads the window, so
      // observing afterwards would broadcast the previous frame's calls.
      // Synchronous by design — no await may separate these two.
      liveCallWindow.observe(ctx.nodeId, st, Date.now());
      hub.recordStatus(ctx.nodeId, st);
      void touchNodeSeen(ctx.nodeId);
      return;
    }
    case 'event': {
      // An agent about to self-update signals it here so the node shows
      // "updating" (not "offline") across the swap + re-exec disconnect.
      if ((data as { kind?: string } | undefined)?.kind === 'updating') {
        hub.markUpdating(ctx.nodeId);
      }
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
  // True only for owner|dev — gates the WRITE paths on this socket (cmd + live
  // stream toggles). A view-only node_monitor authenticates (to read live
  // status) but cannot mutate, mirroring the REST canViewNodeData/canManageNodes
  // split. Without this a node_monitor could send 'cmd' over the WS and bypass
  // the REST 403.
  canManage: boolean;
}

function setupStaffConnection(ws: Alive): void {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  const state: StaffState = { authed: false, userId: null, canManage: false };

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
    hub.unsubscribeLiveStaff(ws);
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
    // Viewer auth: owner|feeder:manager|feeder:monitor may connect to READ live
    // status. feeder:monitor is view-only — canManage gates the write paths.
    if (!verified || !(await hasRole(verified.userId, ['owner', 'feeder:manager', 'feeder:monitor']))) {
      ws.send(envelope('authError', { message: 'forbidden' }));
      try { ws.close(4003, 'forbidden'); } catch { /* ignore */ }
      return;
    }
    state.authed = true;
    state.userId = verified.userId;
    state.canManage = await hasRole(verified.userId, ['owner', 'feeder:manager']);
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
    // Fleet-wide Live view. Unlike subscribeNode this has no nodeId: the
    // viewer wants every radio node at once. Read-only, so the same viewer
    // roles that may subscribeNode may subscribe here (canManage not needed).
    case 'subscribeLive': {
      hub.subscribeLiveStaff(ws, state.userId!);
      // First paint: everything currently online, in one frame. Per-node
      // `liveNode` updates follow as agents report.
      const nodes = await hub.liveSnapshot();
      if (ws.readyState === ws.OPEN) ws.send(envelope('liveSnapshot', { nodes }));
      return;
    }
    case 'unsubscribeLive': {
      hub.unsubscribeLiveStaff(ws);
      return;
    }
    case 'cmd': {
      const d = (data ?? {}) as { nodeId?: string; action?: string; args?: unknown; id?: string };
      if (!d.nodeId || !d.action) return;
      // Write path: only owner|dev. A view-only node_monitor is rejected here
      // (it can subscribe to live status but not command the agent).
      if (!state.canManage) {
        log.warn({ nodeId: d.nodeId, action: d.action, by: state.userId }, 'staff cmd rejected: view-only role');
        ws.send(envelope('cmdResult', { nodeId: d.nodeId, reqId: d.id, ok: false, error: 'forbidden' }));
        return;
      }
      if (!isAgentCommandAction(d.action)) {
        log.warn({ nodeId: d.nodeId, action: d.action, by: state.userId }, 'staff cmd rejected: unknown action');
        ws.send(envelope('cmdResult', { nodeId: d.nodeId, reqId: d.id, ok: false, error: 'unknown action' }));
        return;
      }
      log.info({ nodeId: d.nodeId, action: d.action, by: state.userId }, 'staff node command');
      const result = await hub.sendCmd(d.nodeId, d.action, d.args);
      ws.send(envelope('cmdResult', { nodeId: d.nodeId, reqId: d.id, ...result }));
      return;
    }
    case 'spectrumStart':
    case 'spectrumStop':
    case 'audioStart':
    case 'audioStop': {
      // Live stream control (spectrum for radio, audio monitor for pager):
      // forwarded straight to the agent, which streams binary frames back that
      // the hub relays to this staff subscriber. Not in the 'cmd' allowlist by
      // design — these are transient stream toggles, not durable node commands.
      const d = (data ?? {}) as { nodeId?: string };
      if (!d.nodeId) return;
      // Still a WRITE, though: it makes the node start work and stream data
      // back, and the agent shares one write mutex between a stream and its
      // status heartbeat. A view-only feeder:monitor was able to start
      // spectrum or audio on any node while being refused every 'cmd' two
      // cases up — same gate as those.
      if (!state.canManage) {
        log.warn({ nodeId: d.nodeId, action: t, by: state.userId }, 'staff stream rejected: view-only role');
        return;
      }
      hub.sendToAgent(d.nodeId, t, data);
      return;
    }
    default:
      log.debug({ t }, 'unknown staff message');
  }
}
