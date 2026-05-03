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
// the resulting cf_clearance cookie. Using z:2 so the warm-up exercises a
// broad-coverage tile that returns many vessels in one shot.
const PREWARM_DATA_URL =
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:2/Y:2/station:0';

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
    // Prefer playwright-extra + stealth plugin (patches navigator.webdriver,
    // WebGL fingerprint, navigator.plugins, chrome.runtime, iframe
    // contentWindow detection, etc. — the standard Cloudflare-bypass stack
    // for headless Chromium). Fall back to plain playwright if the optional
    // deps aren't installed so a deploy without them can still boot.
    let pw: PwModule;
    let stealthEnabled = false;
    try {
      const peId = 'playwright-extra';
      const peMod = (await import(peId)) as unknown as PwModule & {
        chromium: { use: (plugin: unknown) => void };
      };
      try {
        const stealthId = 'puppeteer-extra-plugin-stealth';
        const stealthMod = (await import(stealthId)) as { default: () => unknown };
        peMod.chromium.use(stealthMod.default());
        stealthEnabled = true;
      } catch (stealthErr) {
        log.warn(
          { err: (stealthErr as Error).message },
          'marinetraffic: playwright-extra found but stealth plugin missing — running without it',
        );
      }
      pw = peMod;
    } catch {
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
    }

    log.info({ stealthEnabled }, 'marinetraffic: launching headless chromium...');

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
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
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
   * Fetch JSON via page navigation rather than in-page XHR.
   *
   * MarineTraffic's Cloudflare config rejects XHRs (Sec-Fetch-Dest: empty)
   * but accepts a normal page navigation (Sec-Fetch-Dest: document). Plus
   * their app server stops returning JSON if you keep hitting the data URL
   * repeatedly without ever loading the map page — it serves its own 404
   * page instead. So before each data fetch we re-warm the session by
   * navigating to the map page first, then to the data URL.
   *
   * Returns the parsed JSON or null on any failure (logged).
   */
  async fetchJson(url: string, timeoutMs = 25000): Promise<unknown | null> {
    if (!this.isReady()) return null;
    return this.runExclusive(async () => {
      const page = this.page as {
        goto: (url: string, opts: { timeout: number; waitUntil?: string }) => Promise<{
          ok: () => boolean;
          status: () => number;
          text: () => Promise<string>;
        } | null>;
        waitForTimeout: (ms: number) => Promise<void>;
      };
      try {
        // Step 1: warm the session by visiting the map page. This refreshes
        // the cf_clearance + app session so the data URL doesn't return
        // MarineTraffic's own 404 page (which it does when you keep hitting
        // the JSON endpoint repeatedly without intervening map activity).
        await page.goto(MAP_LANDING_URL, {
          timeout: timeoutMs,
          waitUntil: 'domcontentloaded',
        });
        await page.waitForTimeout(800);

        // Step 2: navigate to the data URL — same as pasting it in the
        // address bar. Chromium renders through its built-in JSON viewer
        // and Playwright's Response.text() gives us the raw body.
        const response = await page.goto(url, {
          timeout: timeoutMs,
          waitUntil: 'domcontentloaded',
        });
        if (!response) {
          log.warn({ url }, 'marinetraffic browser navigation: no response');
          return null;
        }
        const status = response.status();
        const text = await response.text();
        if (!response.ok()) {
          log.warn(
            { url, status, bodyPreview: text.slice(0, 300) },
            'marinetraffic browser navigation: non-2xx',
          );
          return null;
        }
        try {
          return JSON.parse(text);
        } catch (err) {
          log.warn(
            { url, status, bodyPreview: text.slice(0, 300), err: (err as Error).message },
            'marinetraffic browser navigation: response not JSON',
          );
          return null;
        }
      } catch (err) {
        log.warn(
          { err: (err as Error).message, url },
          'marinetraffic browser navigation threw',
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
