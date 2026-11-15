/**
 * A radio node's own rdio listener page on its owner's network.
 *
 * The agent reports where it serves the page (`localListen` in its status
 * frame). Only the node's OWNER gets the link (`GET /api/feeder/me`): staff are
 * not on that network, so it is taken off the frame before the hub stores it,
 * which keeps it out of every staff view of the raw status.
 */
import { isIP } from 'node:net';

export interface LocalListen {
  port: number;
  ip: string;
}

/** Validate the agent's report. Anything off is dropped, not repaired: the
 *  values end up in an href on the owner's page. */
export function parseLocalListen(raw: unknown): LocalListen | null {
  if (!raw || typeof raw !== 'object') return null;
  const r = raw as Record<string, unknown>;
  const port = r.port;
  if (typeof port !== 'number' || !Number.isInteger(port) || port < 1 || port > 65535) return null;
  const ip = typeof r.ip === 'string' ? r.ip.trim() : '';
  if (!isIP(ip)) return null;
  // The agent also sends its hostname; it is not used. A <host>.local link
  // only works where the network resolves .local names, and the address
  // always does.
  return { port, ip };
}

/** The owner-facing link, by address. */
export function localListenLinks(l: LocalListen): { url: string } {
  const addr = isIP(l.ip) === 6 ? `[${l.ip}]` : l.ip;
  return { url: `http://${addr}:${l.port}` };
}
