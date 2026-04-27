/**
 * Central Watch Playwright browser worker.
 *
 * Mirrors python `_cw_browser_worker` (external_api_proxy.py:7789-8092).
 *
 * Owns a single headless Chromium instance whose page session has solved
 * the Vercel Security Checkpoint at https://centralwatch.watchtowers.io/au.
 * Once the page is "inside" the checkpoint, page-context fetch() carries
 * the Vercel cookies — so JSON + image bytes can be retrieved from
 * same-origin endpoints without re-solving the challenge per request.
 *
 * Differences from python:
 *   - Async/await instead of greenlet+queue. Concurrent fetchJson /
 *     fetchImage calls are serialised with an in-process mutex so we
 *     don't trip Playwright's "page is busy" footgun.
 *   - Periodic challenge refresh every 15 min and CDP cache clear every
 *     5 min run as their own setInterval, not as the queue's idle path.
 *
 * Boot tolerance: if Playwright is missing or chromium fails to launch,
 * `init()` resolves with `ready === false`. Callers must check `isReady()`
 * before using the worker; everything degrades gracefully (the route
 * keeps serving the placeholder SVG, the JSON refresh loop simply skips).
 *
 * Disable kill switch: setting CENTRALWATCH_DISABLED=true in env makes
 * `init()` skip the launch entirely.
 */
import { log } from '../lib/log.js';

// Lazy/optional Playwright import — keep it out of the static graph so a
// deploy that hasn't run `npx playwright install chromium` still boots.
// The `unknown` casts are deliberate: we don't want to take a hard type
// dep on @types/playwright when the runtime dep is itself optional.
type PwModule = {
  chromium: {
    launch: (opts: Record<string, unknown>) => Promise<unknown>;
  };
};

interface BatchImageInput {
  id: string;
  url: string;
}

interface BatchImageResultJs {
  id: string | null;
  ok: boolean;
  contentType?: string;
  size?: number;
  data?: string; // data: URL
  status?: number;
  retryAfter?: number | null;
  type?: string;
  error?: string;
}

export interface BatchImageResult {
  id: string | null;
  ok: boolean;
  bytes?: Buffer;
  contentType?: string;
  size?: number;
  status?: number;
  retryAfter?: number | null;
  error?: string;
}

const VERCEL_LANDING_URL = 'https://centralwatch.watchtowers.io/au';
const CHALLENGE_REFRESH_INTERVAL_MS = 15 * 60 * 1000; // 15 min
const CDP_CACHE_CLEAR_INTERVAL_MS = 5 * 60 * 1000; // 5 min

class CentralwatchBrowser {
  private pw: unknown = null;
  private browser: unknown = null;
  private context: unknown = null;
  private page: unknown = null;
  private ready = false;
  private initStarted = false;
  private initPromise: Promise<void> | null = null;
  private mutex: Promise<unknown> = Promise.resolve();
  private challengeRefreshTimer: NodeJS.Timeout | null = null;
  private cdpClearTimer: NodeJS.Timeout | null = null;
  private shuttingDown = false;

  isReady(): boolean {
    return this.ready && !this.shuttingDown;
  }

  isDisabled(): boolean {
    return process.env['CENTRALWATCH_DISABLED'] === 'true';
  }

  async init(): Promise<void> {
    if (this.initPromise) return this.initPromise;
    if (this.isDisabled()) {
      log.warn('centralwatch browser disabled via CENTRALWATCH_DISABLED=true');
      return;
    }
    this.initStarted = true;
    this.initPromise = this.doInit().catch((err) => {
      log.warn(
        { err: (err as Error).message },
        'centralwatch browser init failed — degrading to cache-only',
      );
      this.ready = false;
    });
    return this.initPromise;
  }

  private async doInit(): Promise<void> {
    let pw: PwModule;
    try {
      // Dynamic import so a missing playwright dep doesn't break boot.
      // We funnel through a string variable so TS doesn't try to resolve
      // the module at compile time — playwright is an optional runtime
      // dep, not present in node_modules during typecheck.
      const moduleId = 'playwright';
      const mod = (await import(moduleId)) as unknown as PwModule;
      pw = mod;
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'playwright not installed — centralwatch image proxy will serve placeholder only',
      );
      this.ready = false;
      return;
    }

    log.info('centralwatch: launching headless chromium...');

    const browser = (await pw.chromium.launch({
      headless: true,
      args: [
        '--disable-blink-features=AutomationControlled',
        '--no-sandbox',
        '--disable-cache',
        '--disk-cache-size=0',
        '--disable-background-networking',
        '--disable-backing-store-limit',
        '--aggressive-cache-discard',
      ],
    })) as {
      newContext: (opts: Record<string, unknown>) => Promise<unknown>;
      close: () => Promise<void>;
    };

    const context = (await browser.newContext({
      userAgent:
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      viewport: { width: 1920, height: 1080 },
      locale: 'en-AU',
      timezoneId: 'Australia/Sydney',
    })) as {
      newPage: () => Promise<unknown>;
      cookies: (url: string) => Promise<Array<{ name: string }>>;
      close: () => Promise<void>;
      newCDPSession: (page: unknown) => Promise<{
        send: (cmd: string) => Promise<void>;
        detach: () => Promise<void>;
      }>;
    };

    const page = (await context.newPage()) as {
      addInitScript: (script: string) => Promise<void>;
      goto: (url: string, opts: { timeout: number }) => Promise<unknown>;
      waitForFunction: (
        fn: string,
        arg: unknown,
        opts: { timeout: number },
      ) => Promise<unknown>;
      waitForTimeout: (ms: number) => Promise<void>;
      title: () => Promise<string>;
      evaluate: <T>(fn: string | ((...args: unknown[]) => T), arg?: unknown) => Promise<T>;
      close: () => Promise<void>;
    };

    await page.addInitScript(
      'Object.defineProperty(navigator, "webdriver", {get: () => undefined})',
    );

    try {
      await page.goto(VERCEL_LANDING_URL, { timeout: 45000 });
      try {
        await page.waitForFunction(
          '() => !document.title.includes("Vercel") && !document.title.includes("Security")',
          undefined,
          { timeout: 30000 },
        );
        log.info('centralwatch: vercel challenge solved');
      } catch {
        const title = await page.title();
        log.warn({ title }, 'centralwatch: vercel challenge timeout');
      }
      await page.waitForTimeout(2000);
      const cookies = await context.cookies('https://centralwatch.watchtowers.io');
      log.info(
        { cookieCount: cookies.length },
        'centralwatch: browser ready',
      );
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'centralwatch: failed to load landing page — degrading',
      );
      try {
        await page.close();
      } catch {
        // ignore
      }
      try {
        await context.close();
      } catch {
        // ignore
      }
      try {
        await browser.close();
      } catch {
        // ignore
      }
      this.ready = false;
      return;
    }

    this.pw = pw;
    this.browser = browser;
    this.context = context;
    this.page = page;
    this.ready = true;

    // Start the periodic maintenance timers. They're best-effort — any
    // failure is logged and the next interval tries again.
    this.challengeRefreshTimer = setInterval(
      () => void this.refreshChallenge(),
      CHALLENGE_REFRESH_INTERVAL_MS,
    );
    this.cdpClearTimer = setInterval(
      () => void this.clearBrowserCache(),
      CDP_CACHE_CLEAR_INTERVAL_MS,
    );
  }

  /**
   * Serialise calls into the Page so we never have two concurrent
   * `evaluate()` invocations stepping on each other. Python's queue gives
   * us this for free; on Node we need an explicit mutex.
   */
  private async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const prev = this.mutex;
    let release!: () => void;
    const next = new Promise<void>((res) => {
      release = res;
    });
    this.mutex = prev.then(() => next);
    try {
      await prev;
      return await fn();
    } finally {
      release();
    }
  }

  async fetchJson(url: string, timeoutMs = 20000): Promise<unknown | null> {
    if (!this.isReady()) return null;
    return this.runExclusive(async () => {
      const page = this.page as {
        evaluate: <T>(fn: string, arg: unknown) => Promise<T>;
      };
      try {
        const result = await page.evaluate<{
          ok: boolean;
          status?: number;
          data?: unknown;
          error?: string;
        }>(
          `async ([url, timeout]) => {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), timeout);
            try {
              const resp = await fetch(url, { signal: controller.signal });
              clearTimeout(timer);
              if (!resp.ok) return { ok: false, status: resp.status };
              const data = await resp.json();
              return { ok: true, status: resp.status, data: data };
            } catch (e) {
              clearTimeout(timer);
              return { ok: false, error: String(e) };
            }
          }`,
          [url, timeoutMs],
        );
        if (result && result.ok) return result.data ?? null;
        log.warn(
          { url, status: result?.status, err: result?.error },
          'centralwatch browser fetchJson failed',
        );
        return null;
      } catch (err) {
        log.warn(
          { err: (err as Error).message, url },
          'centralwatch browser fetchJson threw',
        );
        return null;
      }
    });
  }

  async fetchImage(
    url: string,
    timeoutMs = 15000,
  ): Promise<{ bytes: Buffer; contentType: string } | null> {
    if (!this.isReady()) return null;
    return this.runExclusive(async () => {
      const page = this.page as {
        evaluate: <T>(fn: string, arg: unknown) => Promise<T>;
      };
      try {
        const result = await page.evaluate<{
          ok: boolean;
          status?: number;
          contentType?: string;
          size?: number;
          data?: string;
          error?: string;
        }>(
          `async ([url, timeout]) => {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), timeout);
            try {
              const resp = await fetch(url, { signal: controller.signal });
              clearTimeout(timer);
              if (!resp.ok) return { ok: false, status: resp.status };
              const blob = await resp.blob();
              if (!blob.type.startsWith('image/')) return { ok: false, status: resp.status, type: blob.type };
              const buf = await blob.arrayBuffer();
              const bytes = new Uint8Array(buf);
              let binary = '';
              for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
              const dataUrl = 'data:' + blob.type + ';base64,' + btoa(binary);
              return { ok: true, status: resp.status, contentType: blob.type, size: blob.size, data: dataUrl };
            } catch (e) {
              clearTimeout(timer);
              return { ok: false, error: String(e) };
            }
          }`,
          [url, timeoutMs],
        );
        if (result && result.ok && result.data) {
          const dataUrl = result.data;
          const commaIdx = dataUrl.indexOf(',');
          if (commaIdx < 0) return null;
          const b64 = dataUrl.slice(commaIdx + 1);
          const bytes = Buffer.from(b64, 'base64');
          return {
            bytes,
            contentType: result.contentType ?? 'image/jpeg',
          };
        }
        return null;
      } catch (err) {
        log.warn(
          { err: (err as Error).message, url },
          'centralwatch browser fetchImage threw',
        );
        return null;
      }
    });
  }

  /**
   * Batch fetch via DOM <img> elements.
   *
   * Mirrors python `_browser_dom_batch_fetch_images` (Phase 1 of the
   * `_continuous_cw_image_worker` two-phase strategy). The point of this
   * path is the request profile: an `<img>` element load sends
   * `Sec-Fetch-Dest: image` while a page-context `fetch()` sends
   * `Sec-Fetch-Dest: empty`. Most rate limiters / WAFs treat them
   * differently, and the CW site itself loads images this way — so this
   * profile cannot be rate-limited without breaking the site.
   *
   * Per-image flow (inside page.evaluate):
   *   1. Append <img crossOrigin="anonymous" src=...> to the document.
   *   2. Wait for load/error, 15s timeout, all images race in parallel
   *      via Promise.allSettled.
   *   3. On load: paint to a hidden <canvas> and toDataURL('image/jpeg')
   *      to recover bytes back across the page boundary.
   *   4. Clean up the <img> (and the shared canvas at the end) so DOM
   *      state doesn't leak between calls.
   *
   * Failures don't throw — each image returns `{id, ok: false, error}`
   * matching the python pattern. Status codes aren't available here
   * (the browser only exposes load/error events for cross-origin
   * <img>), so callers that need HTTP status must use fetchImagesBatch.
   */
  async fetchImagesBatchViaDom(
    images: BatchImageInput[],
    timeoutMs = 60000,
  ): Promise<BatchImageResult[]> {
    if (!this.isReady() || images.length === 0) return [];
    return this.runExclusive(async () => {
      const page = this.page as {
        evaluate: <T>(fn: string, arg: unknown) => Promise<T>;
      };
      const imageList = images.map((i) => [i.id, i.url]);
      try {
        const results = await page.evaluate<BatchImageResultJs[]>(
          `async (imageList) => {
            const PER_IMAGE_TIMEOUT = 15000;
            // Shared hidden canvas — created once, cleaned up at end.
            const canvas = document.createElement('canvas');
            canvas.style.display = 'none';
            document.body.appendChild(canvas);
            const ctx = canvas.getContext('2d');
            const settled = await Promise.allSettled(imageList.map(([id, url]) => {
              return new Promise((resolve) => {
                const img = document.createElement('img');
                img.crossOrigin = 'anonymous';
                img.style.display = 'none';
                let done = false;
                const finish = (result) => {
                  if (done) return;
                  done = true;
                  try { img.remove(); } catch (e) { /* ignore */ }
                  resolve(result);
                };
                const timer = setTimeout(() => {
                  finish({ id, ok: false, error: 'timeout' });
                }, PER_IMAGE_TIMEOUT);
                img.addEventListener('load', () => {
                  clearTimeout(timer);
                  try {
                    canvas.width = img.naturalWidth || img.width;
                    canvas.height = img.naturalHeight || img.height;
                    if (!canvas.width || !canvas.height) {
                      finish({ id, ok: false, error: 'zero-size image' });
                      return;
                    }
                    ctx.drawImage(img, 0, 0);
                    // toDataURL is sync and survives canvas re-use better
                    // than toBlob's callback when images race.
                    const dataUrl = canvas.toDataURL('image/jpeg', 0.92);
                    finish({ id, ok: true, contentType: 'image/jpeg', data: dataUrl });
                  } catch (e) {
                    finish({ id, ok: false, error: 'canvas-tainted: ' + String(e) });
                  }
                });
                img.addEventListener('error', () => {
                  clearTimeout(timer);
                  finish({ id, ok: false, error: 'load-error' });
                });
                document.body.appendChild(img);
                img.src = url;
              });
            }));
            try { canvas.remove(); } catch (e) { /* ignore */ }
            return settled.map((r, i) => {
              if (r.status === 'fulfilled') return r.value;
              return { id: imageList[i] ? imageList[i][0] : null, ok: false, error: String(r.reason) };
            });
          }`,
          imageList,
        );
        const out: BatchImageResult[] = [];
        for (const r of results || []) {
          if (r && r.ok && r.data) {
            const commaIdx = r.data.indexOf(',');
            if (commaIdx < 0) {
              out.push({ id: r.id ?? null, ok: false });
              continue;
            }
            const b64 = r.data.slice(commaIdx + 1);
            const bytes = Buffer.from(b64, 'base64');
            const result: BatchImageResult = {
              id: r.id ?? null,
              ok: true,
              bytes,
              contentType: r.contentType ?? 'image/jpeg',
            };
            if (r.size !== undefined) result.size = r.size;
            out.push(result);
          } else {
            const result: BatchImageResult = {
              id: r?.id ?? null,
              ok: false,
            };
            if (r?.error !== undefined) result.error = r.error;
            out.push(result);
          }
        }
        // timeoutMs reserved for a future page.evaluate-level guard.
        void timeoutMs;
        return out;
      } catch (err) {
        log.warn(
          { err: (err as Error).message, count: images.length },
          'centralwatch browser DOM batch fetch threw',
        );
        return [];
      }
    });
  }

  /**
   * Batch fetch via fetch() — gives us HTTP status codes per item so
   * caller can implement retry/backoff. Mirrors python `batch_images`.
   */
  async fetchImagesBatch(
    images: BatchImageInput[],
    timeoutMs = 60000,
  ): Promise<BatchImageResult[]> {
    if (!this.isReady() || images.length === 0) return [];
    return this.runExclusive(async () => {
      const page = this.page as {
        evaluate: <T>(fn: string, arg: unknown) => Promise<T>;
      };
      const imageList = images.map((i) => [i.id, i.url]);
      try {
        const results = await page.evaluate<BatchImageResultJs[]>(
          `async (imageList) => {
            const TIMEOUT = 15000;
            const results = await Promise.allSettled(imageList.map(async ([id, url]) => {
              const controller = new AbortController();
              const timer = setTimeout(() => controller.abort(), TIMEOUT);
              try {
                const resp = await fetch(url, { signal: controller.signal });
                clearTimeout(timer);
                if (!resp.ok) {
                  const ra = resp.headers.get('Retry-After');
                  return { id, ok: false, status: resp.status, retryAfter: ra ? (parseInt(ra) || null) : null };
                }
                const blob = await resp.blob();
                if (!blob.type.startsWith('image/')) return { id, ok: false, type: blob.type };
                const buf = await blob.arrayBuffer();
                const bytes = new Uint8Array(buf);
                let binary = '';
                for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
                const dataUrl = 'data:' + blob.type + ';base64,' + btoa(binary);
                return { id, ok: true, contentType: blob.type, size: blob.size, data: dataUrl };
              } catch (e) {
                clearTimeout(timer);
                return { id, ok: false, error: String(e) };
              }
            }));
            return results.map(r => r.status === 'fulfilled' ? r.value : { id: null, ok: false, error: String(r.reason) });
          }`,
          imageList,
        );
        const out: BatchImageResult[] = [];
        for (const r of results || []) {
          if (r && r.ok && r.data) {
            const commaIdx = r.data.indexOf(',');
            if (commaIdx < 0) {
              out.push({ id: r.id ?? null, ok: false });
              continue;
            }
            const b64 = r.data.slice(commaIdx + 1);
            const bytes = Buffer.from(b64, 'base64');
            const result: BatchImageResult = {
              id: r.id ?? null,
              ok: true,
              bytes,
              contentType: r.contentType ?? 'image/jpeg',
            };
            if (r.size !== undefined) result.size = r.size;
            out.push(result);
          } else {
            const result: BatchImageResult = {
              id: r?.id ?? null,
              ok: false,
            };
            if (r?.status !== undefined) result.status = r.status;
            if (r?.retryAfter !== undefined && r.retryAfter !== null)
              result.retryAfter = r.retryAfter;
            if (r?.error !== undefined) result.error = r.error;
            out.push(result);
          }
        }
        // Hint the timeout to make eslint-noUnusedLocals happy. Future
        // expansion may push timeoutMs into the page-side script.
        void timeoutMs;
        return out;
      } catch (err) {
        log.warn(
          { err: (err as Error).message, count: images.length },
          'centralwatch browser batch fetch threw',
        );
        return [];
      }
    });
  }

  private async refreshChallenge(): Promise<void> {
    if (!this.ready || this.shuttingDown) return;
    await this.runExclusive(async () => {
      const page = this.page as {
        goto: (url: string, opts: { timeout: number }) => Promise<unknown>;
        waitForFunction: (
          fn: string,
          arg: unknown,
          opts: { timeout: number },
        ) => Promise<unknown>;
        waitForTimeout: (ms: number) => Promise<void>;
      };
      try {
        await page.goto(VERCEL_LANDING_URL, { timeout: 45000 });
        await page.waitForFunction(
          '() => !document.title.includes("Vercel")',
          undefined,
          { timeout: 30000 },
        );
        await page.waitForTimeout(2000);
        log.info('centralwatch: browser session refreshed');
      } catch (err) {
        log.warn(
          { err: (err as Error).message },
          'centralwatch: browser session refresh failed',
        );
      }
    });
  }

  private async clearBrowserCache(): Promise<void> {
    if (!this.ready || this.shuttingDown) return;
    await this.runExclusive(async () => {
      try {
        const context = this.context as {
          newCDPSession: (page: unknown) => Promise<{
            send: (cmd: string) => Promise<void>;
            detach: () => Promise<void>;
          }>;
        };
        const cdp = await context.newCDPSession(this.page);
        await cdp.send('Network.clearBrowserCache');
        await cdp.detach();
      } catch {
        // best-effort
      }
    });
  }

  async shutdown(): Promise<void> {
    this.shuttingDown = true;
    this.ready = false;
    if (this.challengeRefreshTimer) {
      clearInterval(this.challengeRefreshTimer);
      this.challengeRefreshTimer = null;
    }
    if (this.cdpClearTimer) {
      clearInterval(this.cdpClearTimer);
      this.cdpClearTimer = null;
    }
    try {
      const page = this.page as { close?: () => Promise<void> } | null;
      if (page?.close) await page.close();
    } catch {
      // ignore
    }
    try {
      const context = this.context as { close?: () => Promise<void> } | null;
      if (context?.close) await context.close();
    } catch {
      // ignore
    }
    try {
      const browser = this.browser as { close?: () => Promise<void> } | null;
      if (browser?.close) await browser.close();
    } catch {
      // ignore
    }
    this.page = null;
    this.context = null;
    this.browser = null;
    this.pw = null;
    log.info('centralwatch: browser shut down');
  }

  /** Test hook — force the ready flag without launching a real browser. */
  _testSetReady(ready: boolean): void {
    this.ready = ready;
  }

  /** Test hook — replace the underlying page with a stub. */
  _testSetPage(page: unknown): void {
    this.page = page;
  }
}

export const centralwatchBrowser = new CentralwatchBrowser();
export type CentralwatchBrowserType = typeof centralwatchBrowser;
