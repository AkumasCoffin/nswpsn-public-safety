/**
 * GET /api/whisper/status — which transcription server is doing the work.
 *
 * There are two faster-whisper instances: one on a VM that is always up, and
 * one on a PC that is only available while nobody is using it. rdio's
 * transcripts plugin takes a single base URL, so it points at whisper_router
 * (D:\working-dir\faster-whisper-server\whisper_router.py) and the router
 * decides which backend each call goes to.
 *
 * That makes the router the only thing that knows the answer, and this route
 * exists so the staff panel can ask it. It is a read-through: no state is kept
 * here beyond a few seconds of cache.
 *
 * NOT CONFIGURED IS NOT AN ERROR. A deployment without WHISPER_ROUTER_URL set
 * answers 200 with configured:false, so the panel can say "not set up" instead
 * of showing a failure that looks like the router is down.
 */
import { Hono } from 'hono';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { describeRelayError } from '../lib/relayError.js';
import { requireRole, canViewNodeData } from '../services/auth/roles.js';

export const whisperRouter = new Hono();

/**
 * Short, because this is a UI poll and a slow router should read as a problem
 * rather than as a hung panel. Nothing here is on the transcription path — a
 * timeout costs a status card, not a transcript.
 */
const STATUS_TIMEOUT_MS = 4_000;

/**
 * Long enough that several open staff tabs cost the router one request, short
 * enough that "the PC just came up" shows within a refresh. The panel polls on
 * the same cadence as the rest of the Nodes tab.
 */
const CACHE_MS = 5_000;

interface RouterBackend {
  name: string;
  url: string;
  priority: number;
  healthy: boolean;
  draining: boolean;
  inFlight: number;
  requests: number;
  failures: number;
  avgMs: number | null;
  lastOkAt: number | null;
  lastErrorAt: number | null;
  lastError: string | null;
  stateSince: number;
}

interface RouterStatus {
  ok: boolean;
  generatedAt: number;
  current: string | null;
  backends: RouterBackend[];
}

let _cache: { at: number; body: Record<string, unknown> } | null = null;

/** The router speaks epoch seconds; the panel wants what every other date on
 *  the page is. */
function iso(v: number | null | undefined): string | null {
  return typeof v === 'number' && Number.isFinite(v)
    ? new Date(v * 1000).toISOString()
    : null;
}

whisperRouter.get(
  '/api/whisper/status',
  requireRole(canViewNodeData),
  async (c) => {
    const base = config.WHISPER_ROUTER_URL;
    if (!base) {
      return c.json({ configured: false, reachable: false, backends: [] });
    }

    if (_cache && Date.now() - _cache.at < CACHE_MS) return c.json(_cache.body);

    let body: Record<string, unknown>;
    try {
      const res = await fetch(`${base.replace(/\/$/, '')}/status`, {
        headers: config.WHISPER_ROUTER_TOKEN
          ? { 'x-router-token': config.WHISPER_ROUTER_TOKEN }
          : {},
        signal: AbortSignal.timeout(STATUS_TIMEOUT_MS),
      });
      if (!res.ok) {
        // A 401 here means the token is wrong, which is a configuration
        // problem worth naming rather than showing as "router down".
        return c.json({
          configured: true,
          reachable: false,
          error: res.status === 401 ? 'router rejected our token' : `router HTTP ${res.status}`,
          backends: [],
        });
      }
      const s = (await res.json()) as RouterStatus;
      body = {
        configured: true,
        reachable: true,
        // The backend that would take the NEXT call. Null means every one of
        // them is down or draining and transcripts are being refused outright.
        current: s.current ?? null,
        anyAvailable: Boolean(s.ok),
        generatedAt: iso(s.generatedAt),
        backends: (s.backends ?? []).map((b) => ({
          name: b.name,
          url: b.url,
          priority: b.priority,
          healthy: Boolean(b.healthy),
          // Draining is a healthy server being deliberately emptied before a
          // stop, not a failure — the panel has to say so or an idle PC
          // shutting down looks like an outage.
          draining: Boolean(b.draining),
          inFlight: Number(b.inFlight) || 0,
          requests: Number(b.requests) || 0,
          failures: Number(b.failures) || 0,
          avgMs: b.avgMs ?? null,
          lastOkAt: iso(b.lastOkAt),
          lastErrorAt: iso(b.lastErrorAt),
          lastError: b.lastError ?? null,
          stateSince: iso(b.stateSince),
        })),
      };
    } catch (err) {
      log.warn({ cause: describeRelayError(err) }, 'whisper router status unreachable');
      return c.json({
        configured: true,
        reachable: false,
        error: describeRelayError(err),
        backends: [],
      });
    }

    // Only a good answer is cached. Caching a failure would keep the panel
    // showing "down" for seconds after the router came back.
    _cache = { at: Date.now(), body };
    return c.json(body);
  },
);
