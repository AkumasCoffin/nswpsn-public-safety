/**
 * WebSocket message protocol between node agents / staff browsers and the
 * backend hub. Text frames are JSON envelopes; binary frames are spectrum
 * data relayed verbatim (agent → hub → staff).
 *
 * Envelope: { t, id?, data? }. `id` correlates a cmd with its cmdResult.
 */
export const PROTOCOL_VERSION = 1;

export interface Envelope {
  t: string;
  id?: string;
  data?: unknown;
}

// ── agent → backend ────────────────────────────────────────────────────
export interface HelloData {
  protocolVersion?: number;
  agentVersion?: string;
  sdrtrunkVersion?: string;
  rdioVersion?: string;
  os?: string;
  arch?: string;
  hostname?: string;
  appliedConfigVersion?: string | null;
  // Node type the agent believes it is (radio/pager/adsb); compared to node.kind.
  kind?: string;
}

export interface StatusData {
  tuners?: unknown[];
  channels?: unknown[];
  components?: Record<string, string>;
  queueDepth?: number;
  cpuPct?: number;
  memMB?: number;
  diskFreeMB?: number;
  configVersion?: string | null;
  activeCalls?: unknown[];
  events?: unknown[];
  // Node readiness (null on older node builds).
  calibrated?: boolean | null;
  jmbeInstalled?: boolean | null;
}

export interface EventData {
  kind: string; // callStart | crash | updateStep | configError | ...
  [k: string]: unknown;
}

// ── backend → agent ────────────────────────────────────────────────────
export interface HelloAckData {
  ok: boolean;
  configVersion?: string | null;
  updateAvailable?: boolean;
  serverProtocolVersion: number;
}

export type AgentCommandAction =
  | 'restartComponent'
  | 'rebootAgent'
  | 'update'
  | 'startChannel'
  | 'stopChannel'
  | 'tunerSet'
  | 'pushConfig'
  | 'rescanSdr';

/** Runtime allowlist of commands staff may forward to an agent. Even though the
 *  cmd path is owner|dev-gated, validating the action here (rather than relaying
 *  any arbitrary string) is defense-in-depth against a compromised staff token or
 *  a future lower-privilege caller. */
export const AGENT_COMMAND_ACTIONS: ReadonlySet<AgentCommandAction> = new Set([
  'restartComponent',
  'rebootAgent',
  'update',
  'startChannel',
  'stopChannel',
  'tunerSet',
  'pushConfig',
  'rescanSdr',
]);

export function isAgentCommandAction(a: unknown): a is AgentCommandAction {
  return typeof a === 'string' && AGENT_COMMAND_ACTIONS.has(a as AgentCommandAction);
}

export function parseEnvelope(raw: string): Envelope | null {
  try {
    const obj = JSON.parse(raw) as unknown;
    if (
      obj &&
      typeof obj === 'object' &&
      typeof (obj as Record<string, unknown>)['t'] === 'string'
    ) {
      return obj as Envelope;
    }
  } catch {
    /* fall through */
  }
  return null;
}

export function envelope(t: string, data?: unknown, id?: string): string {
  const e: Envelope = { t };
  if (id !== undefined) e.id = id;
  if (data !== undefined) e.data = data;
  return JSON.stringify(e);
}
