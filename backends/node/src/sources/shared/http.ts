/**
 * Thin HTTP wrapper around undici for source pollers.
 *
 * Provides:
 *   - Per-call timeout (AbortController) — every fetch must time out
 *   - Reasonable default User-Agent so feeds don't 403 us as a bot
 *   - Helpers for JSON / text / XML response decoding
 *   - Throws structured errors so callers can branch on retryability
 *
 * No global retry: that's the poller's job (see services/poller.ts);
 * a fetcher returning a thrown error trips the source's failure counter
 * and the poller schedules the next attempt with backoff.
 */
import { fetch, type RequestInit } from 'undici';

const DEFAULT_UA =
  'Mozilla/5.0 (compatible; NswPsnBackend/2.4-node; +https://forcequit.xyz)';

/**
 * A path segment opaque enough to be a credential rather than a route.
 *
 * FIRMS puts its MAP_KEY in the path, so "drop the query" is not enough on its
 * own. A route or a slug is words — letters with separators between them —
 * where a credential mixes in digits or runs on without a break, so either of
 * those is taken as enough. Deliberately generous in that direction: masking a
 * harmless segment costs an error message some precision, where leaking a key
 * costs the key.
 */
function looksLikeSecret(segment: string): boolean {
  if (segment.length < 16) return false;
  if (!/^[A-Za-z0-9_.~+-]+$/.test(segment)) return false;
  if (/\d/.test(segment)) return true;
  return Math.max(...segment.split(/[-_.~+]/).map((p) => p.length)) >= 16;
}

/**
 * The URL as it may safely appear in an error message.
 *
 * These messages do not stay server-side. A source's last error is published
 * on /api/status, and the FIRMS snapshot carries it in source_status[].error —
 * which Cloudflare then caches. Two sources carry their credential in the
 * request URL (FIRMS's MAP_KEY in the path, Pagermon's ?apikey=), so the
 * message gets origin + path with the query dropped and any credential-shaped
 * segment masked. The URL as requested stays on HttpError.fullUrl for logs.
 */
export function redactUrl(raw: string): string {
  const maskPath = (p: string): string =>
    p.split('/').map((seg) => (looksLikeSecret(seg) ? '***' : seg)).join('/');
  try {
    const u = new URL(raw);
    return `${u.origin}${maskPath(u.pathname)}${u.search ? '?***' : ''}`;
  } catch {
    // Not an absolute URL. A bare path (endeavour throws with one) has no host
    // to leak and reads usefully, so redact it the same way; for anything else
    // we cannot tell where a secret might be hiding, so we say nothing.
    if (!raw.startsWith('/')) return '(url)';
    const [path, query] = raw.split('?', 2);
    return `${maskPath(path ?? '')}${query === undefined ? '' : '?***'}`;
  }
}

export class HttpError extends Error {
  override readonly name = 'HttpError';
  /** Safe to publish: origin + path, query dropped, opaque segments masked. */
  readonly url: string;
  /**
   * The URL as requested, credential included — for the server log only.
   * Non-enumerable so nothing that spreads or JSON-stringifies the error can
   * carry it into a response body.
   */
  declare readonly fullUrl: string;

  constructor(
    message: string,
    readonly status: number | null,
    url: string,
  ) {
    super(message);
    this.url = redactUrl(url);
    Object.defineProperty(this, 'fullUrl', { value: url, enumerable: false });
  }
}

export interface FetchOptions {
  /** Hard timeout in ms. Defaults to 15s. */
  timeoutMs?: number;
  /** Extra headers (User-Agent and Accept have sane defaults). */
  headers?: Record<string, string>;
  /** Method, default GET. */
  method?: string;
  /** Body, for POSTs. */
  body?: string;
  /** Treat non-2xx as success (return raw response anyway). Default false. */
  allow_non_2xx?: boolean;
}

async function doFetch(
  url: string,
  opts: FetchOptions = {},
): Promise<{ status: number; text: string; headers: Headers }> {
  const ac = new AbortController();
  const timeoutMs = opts.timeoutMs ?? 15_000;
  const t = setTimeout(() => ac.abort(), timeoutMs);
  // Every message below is built from this, never from `url` — see redactUrl.
  const safeUrl = redactUrl(url);
  try {
    const init: RequestInit = {
      method: opts.method ?? 'GET',
      headers: {
        'User-Agent': DEFAULT_UA,
        Accept: 'application/json, application/xml, text/xml, text/*;q=0.9',
        ...opts.headers,
      },
      signal: ac.signal,
    };
    if (opts.body !== undefined) init.body = opts.body;
    const res = await fetch(url, init);
    const body = await res.text();
    if (!res.ok && !opts.allow_non_2xx) {
      throw new HttpError(
        `HTTP ${res.status} for ${safeUrl}`,
        res.status,
        url,
      );
    }
    // Pass headers through as a vanilla Headers-shaped object so
    // callers don't need to import undici types.
    const headers = new Headers();
    for (const [k, v] of res.headers as unknown as Iterable<[string, string]>) {
      headers.set(k, v);
    }
    return { status: res.status, text: body, headers };
  } catch (err) {
    if (err instanceof HttpError) throw err;
    if ((err as Error).name === 'AbortError') {
      throw new HttpError(`timeout after ${timeoutMs}ms`, null, url);
    }
    // undici wraps the real cause (DNS fail, ECONNREFUSED, TLS error, etc.)
    // in `err.cause`; the top-level message is just "fetch failed". Walk
    // the chain so health checks show ENOTFOUND / EAI_AGAIN / ECONNRESET
    // instead of a useless tautology.
    const e = err as Error & { cause?: unknown };
    const parts: string[] = [];
    let cause: unknown = e;
    for (let i = 0; i < 4 && cause; i += 1) {
      const c = cause as { code?: string; message?: string; cause?: unknown };
      const bit = [c.code, c.message].filter(Boolean).join(' ');
      if (bit && !parts.includes(bit)) parts.push(bit);
      cause = c.cause;
    }
    const detail = parts.join(' | ') || 'unknown';
    throw new HttpError(`fetch failed: ${detail}`, null, url);
  } finally {
    clearTimeout(t);
  }
}

/**
 * Binary fetch for protobuf feeds (GTFS-realtime etc.) — doFetch's
 * text() decode corrupts binary payloads, so this is a parallel path.
 */
export async function fetchBuffer(
  url: string,
  opts: FetchOptions = {},
): Promise<Uint8Array> {
  const ac = new AbortController();
  const timeoutMs = opts.timeoutMs ?? 15_000;
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: opts.method ?? 'GET',
      headers: {
        'User-Agent': DEFAULT_UA,
        Accept: 'application/x-google-protobuf, application/octet-stream, */*',
        ...opts.headers,
      },
      signal: ac.signal,
    });
    if (!res.ok && !opts.allow_non_2xx) {
      // Cancel the body before bailing. doFetch always reads it, but this path
      // threw without touching it, which leaves undici holding the response
      // (and its socket) until GC gets round to it.
      await res.body?.cancel().catch(() => {});
      throw new HttpError(`HTTP ${res.status} for ${redactUrl(url)}`, res.status, url);
    }
    return new Uint8Array(await res.arrayBuffer());
  } catch (err) {
    if (err instanceof HttpError) throw err;
    if ((err as Error).name === 'AbortError') {
      throw new HttpError(`timeout after ${timeoutMs}ms`, null, url);
    }
    throw new HttpError(
      `fetch failed: ${(err as Error).message}`,
      null,
      url,
    );
  } finally {
    clearTimeout(t);
  }
}

export async function fetchJson<T = unknown>(
  url: string,
  opts: FetchOptions = {},
): Promise<T> {
  const r = await doFetch(url, opts);
  try {
    return JSON.parse(r.text) as T;
  } catch (err) {
    throw new HttpError(
      `JSON parse failed: ${(err as Error).message}`,
      r.status,
      url,
    );
  }
}

export async function fetchText(
  url: string,
  opts: FetchOptions = {},
): Promise<string> {
  const r = await doFetch(url, opts);
  return r.text;
}

/** Raw — caller decides what to do with non-2xx and headers. */
export async function fetchRaw(
  url: string,
  opts: FetchOptions = {},
): Promise<{ status: number; text: string; headers: Headers }> {
  return doFetch(url, opts);
}
