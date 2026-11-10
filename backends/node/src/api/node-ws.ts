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
import { decodeJwt } from 'jose';
import { WebSocketServer, type WebSocket } from 'ws';
import { log } from '../lib/log.js';
import { resolveNodeToken } from '../services/auth/nodeToken.js';
import { verifySupabaseToken } from '../services/auth/supabaseJwt.js';
import { hasRole } from '../services/auth/roles.js';
import { hub } from '../services/nodes/hub.js';
import { markNodeSeen } from '../services/nodes/nodeUptime.js';
import {
  refreshNodeOnHello,
  bindInstallId,
  touchNodeSeenThrottled,
  updateNode,
  getNode,
  roleForKind,
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
import { ingestChanMgrLog } from '../services/nodes/chanmgrLog.js';
import { maybeStartInstallSurvey, handleSurveyCommand, noteSurveyStatus } from '../services/siteSurvey.js';

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

/**
 * Normalise an agent-supplied metadata string, dropping NUL.
 *
 * Postgres rejects U+0000 in a text value with 22021, so a hello whose
 * agentVersion was a single NUL byte failed the registry UPDATE — and because
 * these handlers are started fire-and-forget, that rejection reached
 * index.ts's unhandledRejection handler and exited the whole API. The agent
 * re-sends hello on every reconnect, so any enrolled contributor could have
 * held the process down. The .catch on the handler stops it being fatal; this
 * stops it failing at all. Length clamping stays in the registry.
 */
function sanitizeAgentText(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const t = String(v).replace(/\u0000/g, '');
  return t.length ? t : null;
}

/**
 * Which staff sockets hold an audio tap open on which node.
 *
 * Only an explicit audioStop ever cleared the agent's audioLabel, and the
 * close handler below unsubscribed the socket without telling the agent
 * anything — so a staff member who closed the tab instead of the drawer left
 * the node teeing rtl_fm PCM up the WebSocket indefinitely (~14.7 KB/s, about
 * 1.3 GB/day on a volunteer's uplink) for frames the hub then drops because
 * nobody is subscribed, and every config Apply re-armed the tee. Ref-counted
 * per node so one listener leaving cannot cut another's audio.
 */
const audioListeners = new Map<string, Set<Alive>>();

function addAudioListener(nodeId: string, ws: Alive): void {
  let set = audioListeners.get(nodeId);
  if (!set) {
    set = new Set();
    audioListeners.set(nodeId, set);
  }
  set.add(ws);
}

/** Drop this socket's tap on one node; stop the agent when it was the last. */
function dropAudioListener(nodeId: string, ws: Alive): void {
  const set = audioListeners.get(nodeId);
  if (!set || !set.delete(ws)) return;
  if (set.size > 0) return;
  audioListeners.delete(nodeId);
  hub.sendToAgent(nodeId, 'audioStop', { nodeId });
  log.info({ nodeId }, 'pager audio stopped (no listeners left)');
}

/** Drop every tap a socket held — its close, or an unsubscribe. */
function dropAllAudioListeners(ws: Alive): void {
  for (const nodeId of [...audioListeners.keys()]) dropAudioListener(nodeId, ws);
}

/**
 * Authenticated staff sockets, so the sweep below can re-check what the first
 * auth frame decided. Agent sockets have always been revalidated; staff were
 * not, so a feeder:manager whose role was revoked — or whose Supabase session
 * expired — kept cmd and spectrum/audio write access for as long as the socket
 * lived, and the heartbeat ping keeps one alive indefinitely.
 */
interface StaffSession {
  ws: Alive;
  state: StaffState;
}
const staffSessions = new Set<StaffSession>();

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

// The two servers, kept module-scope so shutdown can reach their clients.
// Null until attach, which is also the state in tests that never attach.
let agentWssRef: WebSocketServer | null = null;
let staffWssRef: WebSocketServer | null = null;

/** How long a client gets to answer the close handshake before we drop it. */
const SHUTDOWN_CLOSE_GRACE_MS = 500;

/**
 * Close every agent and staff WebSocket, for shutdown.
 *
 * An upgraded WebSocket is an accepted connection, so server.close() waits for
 * it — and in production an agent is always connected, so the await in
 * index.ts's shutdown() never returned. pm2 SIGKILLed at the kill-timeout and
 * every drain after that await was skipped. 1001 "going away" is what tells an
 * agent or a browser this was a restart rather than a fault, so it reconnects
 * instead of backing off; the sweep then drops anything that did not answer,
 * because we are on a timeout and a silent peer must not hold the port.
 */
export function closeNodeWebSockets(): void {
  const servers = [agentWssRef, staffWssRef].filter((w): w is WebSocketServer => w !== null);
  if (servers.length === 0) return;
  for (const wss of servers) {
    for (const client of wss.clients) {
      try {
        client.close(1001, 'server shutting down');
      } catch {
        /* already gone */
      }
    }
  }
  const sweep = setTimeout(() => {
    for (const wss of servers) {
      for (const client of wss.clients) {
        try {
          client.terminate();
        } catch {
          /* already gone */
        }
      }
    }
  }, SHUTDOWN_CLOSE_GRACE_MS);
  sweep.unref?.();
}

export function attachNodeWebSockets(server: Server): void {
  const agentWss = new WebSocketServer({ noServer: true, maxPayload: MAX_PAYLOAD });
  const staffWss = new WebSocketServer({ noServer: true, maxPayload: MAX_PAYLOAD });
  agentWssRef = agentWss;
  staffWssRef = staffWss;

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
          // A DELETED node (hard revoke) must be cut off; a DISABLED node stays
          // connected (enabled now controls capture via config, not the socket).
          const node = await getNode(a.nodeId);
          if (!node) {
            hub.forceDisconnectAgent(a.nodeId, 'node deleted');
            continue;
          }
          // Kind-aware, via the shared roleForKind (registry.ts) so this can
          // never drift from the upgrade gate. This used to hardcode
          // feeder:radio for every kind, so a healthy pager node was revoked
          // at every sweep — connect, helloAck ok, cut ~60s later, repeat.
          if (!(await hasRole(a.userId, [roleForKind(node.kind)]))) {
            hub.forceDisconnectAgent(a.nodeId, 'role revoked');
          }
        } catch (err) {
          log.warn({ err, nodeId: a.nodeId }, 'agent auth revalidation failed (transient)');
        }
      }
      // Staff sockets get the same treatment. Their role was checked once, at
      // the auth frame, and never again — so a revocation or a signed-out
      // session left full write access in place for the life of the socket.
      // The JWT's own expiry is the other half: a page that stops refreshing
      // its token should not keep commanding nodes on the strength of one it
      // presented hours ago.
      const nowSecs = Math.floor(Date.now() / 1000);
      for (const s of [...staffSessions]) {
        if (!s.state.authed || !s.state.userId) continue;
        if (s.state.exp !== null && s.state.exp <= nowSecs) {
          log.info({ by: s.state.userId }, 'staff ws closed: session expired');
          try { s.ws.close(4001, 'session expired'); } catch { /* ignore */ }
          continue;
        }
        try {
          if (!(await hasRole(s.state.userId, ['owner', 'feeder:manager', 'feeder:monitor']))) {
            log.info({ by: s.state.userId }, 'staff ws closed: role revoked');
            try { s.ws.close(4003, 'forbidden'); } catch { /* ignore */ }
            continue;
          }
          // Still a viewer, but perhaps no longer a manager — drop the write
          // paths without cutting a read-only view that is still allowed.
          const canManage = await hasRole(s.state.userId, ['owner', 'feeder:manager']);
          if (s.state.canManage && !canManage) {
            log.info({ by: s.state.userId }, 'staff ws demoted to view-only');
          }
          s.state.canManage = canManage;
        } catch (err) {
          // Same policy as the agent loop above: a transient role-lookup
          // failure must not close a legitimate session.
          log.warn({ err, by: s.state.userId }, 'staff auth revalidation failed (transient)');
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
    // 503 for 'unavailable' so the agent backs off and retries instead of
    // treating the socket as permanently refused.
    rejectUpgrade(
      socket,
      resolved.reason === 'no_role' ? 403 : resolved.reason === 'unavailable' ? 503 : 401,
      resolved.reason === 'unavailable' ? 'Registry Unavailable' : 'Unauthorized',
    );
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
    // A rejection here used to have nowhere to go: index.ts exits the process
    // on any unhandledRejection, so one Postgres blip on the per-frame
    // last-seen write — or a single malformed hello — took the whole API down
    // and dropped every agent and staff socket with it. A failed frame is
    // worth a log line, not a restart.
    void handleAgentMessage(ws, ctx, msg.t, msg.id, msg.data).catch((err) =>
      log.warn({ err, nodeId: ctx.nodeId, t: msg.t }, 'agent message handler failed'),
    );
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
      // Every field here is agent-supplied and lands in a text column, so it
      // goes through sanitizeAgentText — see there for the NUL case.
      const meta: HelloMeta = {
        agentVersion: sanitizeAgentText(h.agentVersion),
        sdrtrunkVersion: sanitizeAgentText(h.sdrtrunkVersion),
        rdioVersion: sanitizeAgentText(h.rdioVersion),
        os: sanitizeAgentText(h.os),
        arch: sanitizeAgentText(h.arch),
        hostname: sanitizeAgentText(h.hostname),
        localIp: typeof h.localIp === 'string' ? sanitizeAgentText(h.localIp.trim()) : null,
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
        // First-install site survey: a radio node with zero channels, an LGA,
        // and no survey history gets one automatic run. All guards (and the
        // once-ever rule) live in the service; fire-and-forget.
        void maybeStartInstallSurvey(node);
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
      // Persist the channel manager's decision ring (fire-and-forget): the
      // agent repeats it every frame, the ingest high-water mark makes the
      // no-news case free.
      void ingestChanMgrLog(ctx.nodeId, st.channelManager);
      // A survey lives in the agent's memory: if it stops saying it has one,
      // it has lost it (a restart), and the record must not outlive that.
      noteSurveyStatus(ctx.nodeId, (st as { siteSurvey?: { running?: boolean } }).siteSurvey?.running === true);
      // Throttled variant: it swallows its own failures, and the column's
      // resolution is "recently" — a write per frame (every 15s per node, 1s
      // while anyone watches Live) was both pointless load and the likeliest
      // place for a DB blip to land.
      void touchNodeSeenThrottled(ctx.nodeId);
      markNodeSeen(ctx.nodeId);
      return;
    }
    case 'event': {
      // Update-stage reports: agents announce checking -> fetching ->
      // installing as an update progresses (and "done" when a check ends
      // quiet), so the fleet page can say what is actually happening instead
      // of flashing "offline" / "decoder down". Older agents send no stage,
      // only the pre-swap signal — treat that as installing.
      if ((data as { kind?: string } | undefined)?.kind === 'updating') {
        const stage = String((data as { stage?: string }).stage ?? 'installing');
        if (stage === 'done') {
          hub.clearUpdating(ctx.nodeId);
        } else {
          // Windows are stage-sized: a check is seconds, a component fetch
          // can be minutes (the sdrtrunk runtime is hundreds of MB), an
          // install spans the restart + decoder relaunch.
          const ms = stage === 'checking' ? 30_000 : stage === 'fetching' ? 600_000 : 180_000;
          hub.markUpdating(ctx.nodeId, stage, ms);
        }
      }
      hub.relayEvent(ctx.nodeId, data);
      return;
    }
    case 'configApplied': {
      const cv = sanitizeAgentText((data as { configVersion?: string } | undefined)?.configVersion);
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
  /** The presented JWT's `exp` (unix secs), so the sweep can retire a socket
   *  whose session has run out. Null when the token carried no exp. */
  exp: number | null;
}

/** The `exp` claim of an ALREADY-VERIFIED token, or null if it carries none. */
function readTokenExp(token: string): number | null {
  try {
    const exp = decodeJwt(token).exp;
    return typeof exp === 'number' && Number.isFinite(exp) ? exp : null;
  } catch {
    return null;
  }
}

function setupStaffConnection(ws: Alive): void {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  const state: StaffState = { authed: false, userId: null, canManage: false, exp: null };
  const session: StaffSession = { ws, state };

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
    // Same reasoning as the agent handler: a DB error on the auth or survey
    // query must not reach index.ts's unhandledRejection handler and exit.
    void handleStaffMessage(ws, session, msg.t, msg.data, authTimer).catch((err) =>
      log.warn({ err, t: msg.t, by: state.userId }, 'staff message handler failed'),
    );
  });

  ws.on('close', () => {
    clearTimeout(authTimer);
    staffSessions.delete(session);
    // Before dropping the subscriptions: a tap this socket started is still
    // running on the node, and nothing else would ever tell it to stop.
    dropAllAudioListeners(ws);
    hub.unsubscribeStaff(ws);
    hub.unsubscribeLiveStaff(ws);
  });
  ws.on('error', (err) => log.debug({ err }, 'staff ws error'));
}

async function handleStaffMessage(
  ws: Alive,
  session: StaffSession,
  t: string,
  data: unknown,
  authTimer: ReturnType<typeof setTimeout>,
): Promise<void> {
  const state = session.state;
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
    // verifySupabaseToken has already checked the signature (and jose rejects
    // an expired token), so the exp read back here is the verified one — kept
    // so the sweep can retire this socket when the session runs out rather
    // than letting a heartbeat hold it open past its token's life.
    state.exp = readTokenExp(token);
    staffSessions.add(session);
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
      // unsubscribeStaff drops this socket from EVERY node, so the taps go
      // with it — otherwise closing the drawer left the node still streaming.
      dropAllAudioListeners(ws);
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
      // Site surveys are built server-side (the browser says how far to look,
      // never which frequencies to tune), so they go through the same handler
      // the REST command route uses rather than being forwarded as-is.
      const survey = await handleSurveyCommand(d.nodeId, d.action, d.args, state.userId ?? null);
      if (survey) {
        ws.send(envelope('cmdResult', { nodeId: d.nodeId, reqId: d.id, ...survey }));
        return;
      }
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
      // Audio is ref-counted (see audioListeners): the agent keeps the tap
      // running until it is told to stop, so the stop has to be the LAST
      // listener leaving, not the first — otherwise one staff member closing
      // the drawer cut the audio out from under another.
      if (t === 'audioStart') {
        addAudioListener(d.nodeId, ws);
      } else if (t === 'audioStop') {
        dropAudioListener(d.nodeId, ws);
        return;
      }
      hub.sendToAgent(d.nodeId, t, data);
      return;
    }
    default:
      log.debug({ t }, 'unknown staff message');
  }
}
