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

export class HttpError extends Error {
  override readonly name = 'HttpError';
  constructor(
    message: string,
    readonly status: number | null,
    readonly url: string,
  ) {
    super(message);
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
        `HTTP ${res.status} for ${url}`,
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
