/**
 * MarineTraffic Playwright browser worker.
 *
 * MarineTraffic's `getData/get_data_json_4/...` JSON endpoint 403s any
 * cold-cache server-side request because Cloudflare gates it on a session
 * established via the SPA. The endpoint *does* respond when called from a
 * tab that's already loaded marinetraffic.com/en/ais/home/...; that page
 * solves the Cloudflare challenge and seeds session cookies which the JSON
 * endpoint requires.
 *
 * This worker keeps a headless Chromium tab parked on the AIS map page
 * (centred on Sydney by default — the URL only sets initial centre, the
 * data fetches accept arbitrary tile coords) and exposes a `fetchJson`
 * method that runs `fetch(url, { credentials: 'include' })` inside the
 * page so the request carries the established cookies.
 *
 * Disable kill switch: `MARINETRAFFIC_DISABLED=true` skips the launch.
 *
 * Boot tolerance: if Playwright is missing or chromium fails to launch,
 * `init()` resolves with `ready === false`. `/api/marinetraffic/vessels`
 * responds with 503 when the worker isn't ready.
 */
import { log } from '../lib/log.js';

type PwModule = {
  chromium: {
    launch: (opts: Record<string, unknown>) => Promise<unknown>;
  };
};

// Map page that the live SPA uses (centred on NSW). We mirror the same
// centerx/centery/zoom triple the working browser session uses so the
// Cloudflare WAF sets cookies that match this tile region.
const MAP_LANDING_URL =
  'https://www.marinetraffic.com/en/ais/home/centerx:151.6/centery:-33.2/zoom:10';
// Pre-warm tile — same as the default in api/marinetraffic.ts. Hitting this
// URL via page.goto causes the browser to solve any per-URL WAF challenge,
// matching the centralwatch pattern. Subsequent in-page fetch() calls reuse
// the resulting cf_clearance cookie.
const PREWARM_DATA_URL =
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:472/Y:306/station:0';

const SESSION_REFRESH_INTERVAL_MS = 30 * 60 * 1000; // 30 min
// Cloudflare's JS challenge often completes within a few seconds but can
// take longer on a headless box. Keep polling for cf_clearance up to this.
const CF_CLEARANCE_TIMEOUT_MS = 25_000;

class MarinetrafficBrowser {
  private browser: unknown = null;
  private context: unknown = null;
  private page: unknown = null;
  private ready = false;
  private initPromise: Promise<void> | null = null;
  private mutex: Promise<unknown> = Promise.resolve();
  private refreshTimer: NodeJS.Timeout | null = null;
  private shuttingDown = false;

  isReady(): boolean {
    return this.ready && !this.shuttingDown;
  }

  isDisabled(): boolean {
    return process.env['MARINETRAFFIC_DISABLED'] === 'true';
  }

  async init(): Promise<void> {
    if (this.initPromise) return this.initPromise;
    if (this.isDisabled()) {
      log.warn('marinetraffic browser disabled via MARINETRAFFIC_DISABLED=true');
      return;
    }
    this.initPromise = this.doInit().catch((err) => {
      log.warn(
        { err: (err as Error).message },
        'marinetraffic browser init failed — degrading',
      );
      this.ready = false;
    });
    return this.initPromise;
  }

  private async doInit(): Promise<void> {
    let pw: PwModule;
    try {
      const moduleId = 'playwright';
      const mod = (await import(moduleId)) as unknown as PwModule;
      pw = mod;
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'playwright not installed — marinetraffic proxy will return 503',
      );
      this.ready = false;
      return;
    }

    log.info('marinetraffic: launching headless chromium...');

    const browser = (await pw.chromium.launch({
      headless: true,
      args: [
        '--disable-blink-features=AutomationControlled',
        '--no-sandbox',
        '--disable-cache',
        '--disk-cache-size=0',
        '--disable-background-networking',
      ],
    })) as {
      newContext: (opts: Record<string, unknown>) => Promise<unknown>;
      close: () => Promise<void>;
    };

    const context = (await browser.newContext({
      userAgent:
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      viewport: { width: 1280, height: 800 },
      locale: 'en-AU',
      timezoneId: 'Australia/Sydney',
    })) as {
      newPage: () => Promise<unknown>;
      cookies: (url: string) => Promise<Array<{ name: string }>>;
      close: () => Promise<void>;
    };

    const page = (await context.newPage()) as {
      addInitScript: (script: string) => Promise<void>;
      goto: (url: string, opts: { timeout: number; waitUntil?: string }) => Promise<unknown>;
      waitForTimeout: (ms: number) => Promise<void>;
      evaluate: <T>(fn: string) => Promise<T>;
      close: () => Promise<void>;
    };

    await page.addInitScript(
      'Object.defineProperty(navigator, "webdriver", {get: () => undefined})',
    );

    try {
      // 1. Land on the map page so Cloudflare drops a session cookie + cf_clearance.
      await page.goto(MAP_LANDING_URL, { timeout: 45000, waitUntil: 'domcontentloaded' });
      // Poll until cf_clearance shows up (the WAF challenge solved cookie).
      const ok = await this.waitForCfClearance(context, CF_CLEARANCE_TIMEOUT_MS);
      if (!ok) {
        log.warn('marinetraffic: cf_clearance never appeared on map page (continuing anyway)');
      }
      // 2. Pre-warm the data URL by navigating to it directly. The map page's
      //    SPA does this internally; doing it here causes the browser to
      //    solve any per-URL WAF challenge for /getData/* once, so that
      //    later fetch() calls inside the page reuse the clearance.
      try {
        await page.goto(PREWARM_DATA_URL, { timeout: 30000, waitUntil: 'domcontentloaded' });
        await page.waitForTimeout(1500);
        log.info('marinetraffic: data endpoint prewarmed');
      } catch (err) {
        log.warn(
          { err: (err as Error).message },
          'marinetraffic: data prewarm failed (will retry on first fetch)',
        );
      }
      // 3. Return to the map page so subsequent fetch()s have a sensible Referer.
      try {
        await page.goto(MAP_LANDING_URL, { timeout: 30000, waitUntil: 'domcontentloaded' });
        await page.waitForTimeout(1500);
      } catch { /* best-effort */ }
      const cookies = await context.cookies('https://www.marinetraffic.com');
      const hasClearance = cookies.some((c) => c.name === 'cf_clearance');
      log.info(
        { cookieCount: cookies.length, cfClearance: hasClearance },
        'marinetraffic: browser ready',
      );
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'marinetraffic: failed to load map page — degrading',
      );
      try { await page.close(); } catch { /* ignore */ }
      try { await context.close(); } catch { /* ignore */ }
      try { await browser.close(); } catch { /* ignore */ }
      this.ready = false;
      return;
    }

    this.browser = browser;
    this.context = context;
    this.page = page;
    this.ready = true;

    this.refreshTimer = setInterval(
      () => void this.refreshSession(),
      SESSION_REFRESH_INTERVAL_MS,
    );
  }

  /** Poll the context cookies until cf_clearance shows up or the timeout hits. */
  private async waitForCfClearance(
    context: { cookies: (url: string) => Promise<Array<{ name: string }>> },
    timeoutMs: number,
  ): Promise<boolean> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const cookies = await context.cookies('https://www.marinetraffic.com');
      if (cookies.some((c) => c.name === 'cf_clearance')) return true;
      await new Promise((res) => setTimeout(res, 500));
    }
    return false;
  }

  private async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const prev = this.mutex;
    let release!: () => void;
    const next = new Promise<void>((res) => { release = res; });
    this.mutex = prev.then(() => next);
    try {
      await prev;
      return await fn();
    } finally {
      release();
    }
  }

  /**
   * Run fetch() inside the page so cookies + correct origin are applied.
   * Returns parsed JSON, or null on any failure (logged).
   */
  async fetchJson(url: string, timeoutMs = 15000): Promise<unknown | null> {
    if (!this.isReady()) return null;
    return this.runExclusive(async () => {
      const page = this.page as { evaluate: <T>(fn: string) => Promise<T> };
      const script = `(async () => {
        const url = ${JSON.stringify(url)};
        const timeout = ${JSON.stringify(timeoutMs)};
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeout);
        try {
          const resp = await fetch(url, {
            signal: controller.signal,
            credentials: 'include',
            headers: {
              'Accept': 'application/json, text/javascript, */*; q=0.01',
              'X-Requested-With': 'XMLHttpRequest',
              'Vessel-Image': '00d4d80f7afac6c031bd0eed7e6d9a5cabf3'
            }
          });
          clearTimeout(timer);
          const ct = resp.headers.get('content-type') || '';
          if (!resp.ok) {
            const bodyText = (await resp.text()).slice(0, 300);
            return { ok: false, status: resp.status, contentType: ct, bodyPreview: bodyText };
          }
          const text = await resp.text();
          try {
            return { ok: true, status: resp.status, data: JSON.parse(text) };
          } catch (e) {
            return { ok: false, status: resp.status, contentType: ct, bodyPreview: text.slice(0, 300), error: 'json-parse-failed' };
          }
        } catch (e) {
          clearTimeout(timer);
          return { ok: false, error: String(e && e.message ? e.message : e) };
        }
      })()`;
      try {
        const result = await page.evaluate<{
          ok: boolean;
          status?: number;
          contentType?: string;
          bodyPreview?: string;
          data?: unknown;
          error?: string;
        }>(script);
        if (result && result.ok) return result.data ?? null;
        log.warn(
          {
            url,
            status: result?.status,
            contentType: result?.contentType,
            bodyPreview: result?.bodyPreview,
            err: result?.error,
          },
          'marinetraffic browser fetchJson failed',
        );
        return null;
      } catch (err) {
        log.warn(
          { err: (err as Error).message, url },
          'marinetraffic browser fetchJson threw',
        );
        return null;
      }
    });
  }

  private async refreshSession(): Promise<void> {
    if (!this.ready || this.shuttingDown) return;
    await this.runExclusive(async () => {
      const page = this.page as {
        goto: (url: string, opts: { timeout: number; waitUntil?: string }) => Promise<unknown>;
        waitForTimeout: (ms: number) => Promise<void>;
      };
      try {
        await page.goto(MAP_LANDING_URL, { timeout: 45000, waitUntil: 'domcontentloaded' });
        await page.waitForTimeout(3000);
        log.info('marinetraffic: session refreshed');
      } catch (err) {
        log.warn(
          { err: (err as Error).message },
          'marinetraffic: session refresh failed',
        );
      }
    });
  }

  async shutdown(): Promise<void> {
    this.shuttingDown = true;
    this.ready = false;
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = null;
    }
    try {
      const page = this.page as { close?: () => Promise<void> } | null;
      if (page?.close) await page.close();
    } catch { /* ignore */ }
    try {
      const context = this.context as { close?: () => Promise<void> } | null;
      if (context?.close) await context.close();
    } catch { /* ignore */ }
    try {
      const browser = this.browser as { close?: () => Promise<void> } | null;
      if (browser?.close) await browser.close();
    } catch { /* ignore */ }
    this.page = null;
    this.context = null;
    this.browser = null;
    log.info('marinetraffic: browser shut down');
  }
}

export const marinetrafficBrowser = new MarinetrafficBrowser();
export type MarinetrafficBrowserType = typeof marinetrafficBrowser;
