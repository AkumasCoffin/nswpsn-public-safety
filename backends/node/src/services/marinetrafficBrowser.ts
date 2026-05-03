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

// Default map landing URL (centred on NSW, zoom:10). Used by the
// session-init path and as a fallback when fetchJson can't parse a
// tile URL. For per-tile fetches we synthesise a tile-specific
// landing URL via tileLandingUrl() below — MarineTraffic only serves
// data tiles inside the SPA's current viewport at the tile's zoom,
// so warming the SPA with a generic URL fails for most z:11 tiles
// and any z:9/z:10 tile outside the central viewport window.
const MAP_LANDING_URL =
  'https://www.marinetraffic.com/en/ais/home/centerx:151.6/centery:-33.2/zoom:10';

// Pull z/X/Y out of a MarineTraffic tile data URL. Returns null if the
// URL doesn't match the expected /z:N/X:N/Y:N/ pattern.
function parseTileCoords(url: string): { z: number; x: number; y: number } | null {
  const m = url.match(/\/z:(\d+)\/X:(\d+)\/Y:(\d+)\//);
  if (!m) return null;
  return {
    z: parseInt(m[1] as string, 10),
    x: parseInt(m[2] as string, 10),
    y: parseInt(m[3] as string, 10),
  };
}

// Convert a MarineTraffic tile coord to its centre lng/lat. MT's tile
// scheme uses 2^(z-1) tiles per axis (verified empirically — e.g.
// z:10 X:472 Y:306 sits at lng~152, lat~-33.5 which is Sydney).
function tileLandingUrl(z: number, x: number, y: number): string {
  const n = Math.pow(2, z - 1);
  const lng = ((x + 0.5) / n) * 360 - 180;
  const lat =
    (Math.atan(Math.sinh(Math.PI * (1 - (2 * (y + 0.5)) / n))) * 180) /
    Math.PI;
  return (
    'https://www.marinetraffic.com/en/ais/home' +
    `/centerx:${lng.toFixed(4)}/centery:${lat.toFixed(4)}/zoom:${z}`
  );
}
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
      // Step 1: warm the session by visiting the map page at the
      // tile's own centre + zoom. MarineTraffic only serves a data
      // tile when its z/X/Y intersects the SPA's current viewport,
      // so a one-size-fits-all warmup URL silently 404s for any
      // tile outside that view. Per-tile warmup also refreshes the
      // cf_clearance + app session, which MT requires between
      // repeated direct hits to the JSON endpoint.
      const coords = parseTileCoords(url);
      const landingUrl = coords
        ? tileLandingUrl(coords.z, coords.x, coords.y)
        : MAP_LANDING_URL;
      // Two attempts: most tiles succeed on the first try with an 800 ms
      // post-warmup wait, but ~5-10% are flaky (the SPA hasn't finished
      // registering its viewport with MT's tile-serving logic by the
      // time we navigate to the data URL). A second attempt with a
      // longer wait catches almost all of those.
      let lastWarn: Record<string, unknown> | null = null;
      for (let attempt = 1; attempt <= 2; attempt++) {
        const warmWaitMs = attempt === 1 ? 800 : 2000;
        try {
          await page.goto(landingUrl, {
            timeout: timeoutMs,
            waitUntil: 'domcontentloaded',
          });
          await page.waitForTimeout(warmWaitMs);

          // Step 2: navigate to the data URL — same as pasting it in
          // the address bar. Chromium renders through its built-in
          // JSON viewer and Playwright's Response.text() gives us the
          // raw body.
          const response = await page.goto(url, {
            timeout: timeoutMs,
            waitUntil: 'domcontentloaded',
          });
          if (!response) {
            lastWarn = { url, attempt, reason: 'no response' };
            continue;
          }
          const status = response.status();
          const text = await response.text();
          if (!response.ok()) {
            lastWarn = { url, attempt, status, bodyPreview: text.slice(0, 300) };
            continue;
          }
          try {
            return JSON.parse(text);
          } catch (err) {
            lastWarn = {
              url,
              attempt,
              status,
              bodyPreview: text.slice(0, 300),
              err: (err as Error).message,
              reason: 'invalid JSON',
            };
            continue;
          }
        } catch (err) {
          lastWarn = { url, attempt, err: (err as Error).message, reason: 'goto threw' };
        }
      }
      if (lastWarn) {
        log.warn(lastWarn, 'marinetraffic browser navigation: failed both attempts');
      }
      return null;
    });
  }

  /**
   * Navigate the worker page to a MarineTraffic vessel detail URL and
   * extract the structured vessel JSON. MT's SPA fetches that JSON
   * over XHR after the page loads, so the most reliable extractor is
   * just to listen on the page's response stream and grab the first
   * JSON body that looks like a vessel record. Falls back to DOM
   * scraping if the XHR never fires.
   */
  async fetchVesselDetail(shipId: string, timeoutMs = 25000): Promise<unknown | null> {
    if (!this.isReady()) return null;
    return this.runExclusive(async () => {
      type PwResponse = {
        url: () => string;
        headers: () => Record<string, string>;
        text: () => Promise<string>;
      };
      const page = this.page as {
        goto: (
          url: string,
          opts: { timeout: number; waitUntil?: string },
        ) => Promise<unknown>;
        waitForTimeout: (ms: number) => Promise<void>;
        evaluate: <T>(fn: () => T) => Promise<T>;
        on: (event: 'response', handler: (resp: PwResponse) => void) => void;
        off: (event: 'response', handler: (resp: PwResponse) => void) => void;
      };
      // The real MT vessel detail page — the SPA navigates here when
      // you click a vessel marker on their map. The page itself is
      // mostly an HTML shell; the structured JSON is fetched via XHR
      // after navigation, which we capture below by listening on the
      // page's response stream.
      const url = `https://www.marinetraffic.com/en/ais/details/ships/shipid:${encodeURIComponent(shipId)}`;
      let captured: unknown = null;
      let resolveCaptured: ((v: unknown) => void) | null = null;
      const capturedP = new Promise<unknown>((resolve) => {
        resolveCaptured = resolve;
      });
      const onResponse = (resp: PwResponse) => {
        if (captured !== null) return;
        try {
          const respUrl = resp.url();
          // MT's vessel-detail XHR usually hits one of these path
          // fragments. Filtering reduces the body parse work.
          if (
            !/\/(asset|vessel|ship|details|getAsset|getVessel)/i.test(respUrl)
          ) {
            return;
          }
          const ct = (resp.headers()['content-type'] || '').toLowerCase();
          if (!ct.includes('json')) return;
          // Parse asynchronously and check for vessel-shaped fields.
          void resp.text().then((text) => {
            if (captured !== null) return;
            try {
              const json = JSON.parse(text);
              if (
                json &&
                typeof json === 'object' &&
                ((json as Record<string, unknown>).shipId ||
                  (json as Record<string, unknown>).SHIP_ID ||
                  (json as Record<string, unknown>).mmsi ||
                  (json as Record<string, unknown>).MMSI ||
                  (json as Record<string, unknown>).imo ||
                  (json as Record<string, unknown>).IMO)
              ) {
                captured = json;
                if (resolveCaptured) resolveCaptured(json);
              }
            } catch {
              /* not JSON or not parseable */
            }
          });
        } catch {
          /* ignore */
        }
      };
      page.on('response', onResponse);
      try {
        await page.goto(url, {
          timeout: timeoutMs,
          waitUntil: 'domcontentloaded',
        });
        // Wait for the XHR (capped at 3 s — MT's hydration is normally
        // sub-second, longer means it's not coming).
        await Promise.race([
          capturedP,
          new Promise((resolve) => setTimeout(resolve, 3000)),
        ]);
        if (captured !== null) {
          return captured;
        }
        // Fallback: DOM extraction (kept for older page shapes).
        await page.waitForTimeout(500);
        // The evaluate callback runs inside the page (browser context),
        // not Node, so DOM globals exist at runtime. We cast through
        // `any` to keep the Node TS compiler from complaining about
        // missing `window` / `document` types.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const evalFn = (() => {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const g: any = globalThis;
          const w = g;
          const doc = g.document;
          if (!doc) return null;
          // 1. Direct window globals known to be set by MT's pages.
          for (const key of [
            'assetData',
            'vesselData',
            'shipData',
            'asset_detail',
          ]) {
            const v = w[key];
            if (v && typeof v === 'object') return v;
          }
          // 2. Nested in MT's main app namespace.
          const mt = w.MT;
          if (mt && typeof mt === 'object') {
            for (const key of ['vessel', 'asset', 'ship', 'data']) {
              const v = mt[key];
              if (v && typeof v === 'object') return v;
            }
          }
          // 3. JSON-LD blocks.
          const lds = doc.querySelectorAll(
            'script[type="application/ld+json"]',
          );
          for (let i = 0; i < lds.length; i++) {
            const s = lds[i];
            try {
              const j = JSON.parse(s.textContent || '');
              const t = j && j['@type'];
              if (t === 'Ship' || t === 'Vehicle' || t === 'Boat') return j;
            } catch {
              /* ignore */
            }
          }
          // 4. <script id="..."> with a JSON body.
          for (const id of [
            'vessel-data',
            'js-vessel-info-data',
            'asset-data',
            '__NEXT_DATA__',
          ]) {
            const el = doc.getElementById(id);
            if (el && el.textContent) {
              try {
                return JSON.parse(el.textContent);
              } catch {
                /* ignore */
              }
            }
          }
          // 5. Inline <script> regex fallback for old-style pages.
          const scripts = doc.querySelectorAll('script:not([src])');
          for (let i = 0; i < scripts.length; i++) {
            const s = scripts[i];
            const t = s.textContent || '';
            const patterns = [
              /window\.assetData\s*=\s*(\{[\s\S]*?\});/,
              /var\s+assetData\s*=\s*(\{[\s\S]*?\});/,
              /window\.vesselData\s*=\s*(\{[\s\S]*?\});/,
              /var\s+assetDetail\s*=\s*(\{[\s\S]*?\});/,
            ];
            for (const re of patterns) {
              const m = t.match(re);
              if (m) {
                try {
                  return JSON.parse(m[1]);
                } catch {
                  /* ignore */
                }
              }
            }
          }
          return null;
        }) as () => unknown;
        const data = await page.evaluate<unknown>(evalFn);
        if (data == null) {
          log.warn({ url }, 'marinetraffic vessel detail: no JSON found in page');
        }
        return data;
      } catch (err) {
        log.warn(
          { err: (err as Error).message, url },
          'marinetraffic vessel detail: navigation threw',
        );
        return null;
      } finally {
        page.off('response', onResponse);
      }
    });
  }

  /**
   * Fetch arbitrary binary content (images) via the browser context's
   * HTTP client. Returns null on any failure. Used for proxying
   * vessel-image WebPs through our origin so they don't get blocked
   * by Cross-Origin-Resource-Policy / Opaque Response Blocking on
   * the front-end.
   */
  async fetchBinary(
    url: string,
    timeoutMs = 25000,
  ): Promise<{ bytes: Uint8Array<ArrayBuffer>; contentType: string; status: number } | null> {
    if (!this.isReady()) return null;
    return this.runExclusive(async () => {
      const ctx = this.context as {
        request: {
          get: (
            url: string,
            opts: { headers?: Record<string, string>; timeout?: number },
          ) => Promise<{
            ok: () => boolean;
            status: () => number;
            headers: () => Record<string, string>;
            body: () => Promise<Buffer>;
            text: () => Promise<string>;
          }>;
        };
      };
      try {
        const res = await ctx.request.get(url, {
          timeout: timeoutMs,
          headers: {
            Accept: 'image/webp,image/avif,image/png,image/*,*/*;q=0.8',
            Referer: 'https://www.marinetraffic.com/',
          },
        });
        const status = res.status();
        const headers = res.headers();
        const contentType = headers['content-type'] || '';
        const empty = new Uint8Array(new ArrayBuffer(0));
        if (!res.ok()) {
          return { bytes: empty, contentType, status };
        }
        // Refuse to forward non-image bodies — MT returns an HTML 404
        // page for missing vessel images, and we want the caller to
        // see a real 404 rather than that HTML.
        if (!contentType.startsWith('image/')) {
          return { bytes: empty, contentType, status: 404 };
        }
        const buf = await res.body();
        // Copy into a freshly-allocated ArrayBuffer-backed Uint8Array
        // so the type is `Uint8Array<ArrayBuffer>` (Hono needs that
        // exact shape on c.body, and Buffer's underlying may be a
        // SharedArrayBuffer in some Node configs).
        const fresh = new Uint8Array(new ArrayBuffer(buf.byteLength));
        fresh.set(buf);
        return { bytes: fresh, contentType, status };
      } catch (err) {
        log.warn(
          { err: (err as Error).message, url },
          'marinetraffic binary fetch threw',
        );
        return null;
      }
    });
  }

  /**
   * Fetch a MarineTraffic JSON endpoint via the browser context's HTTP
   * client (not via page.goto). The context shares cookies + the
   * cf_clearance the page worker has solved, so MT lets the request
   * through, but we send proper XHR-style headers so endpoints that do
   * content negotiation (e.g. /en/vessels/{id}/general) return JSON
   * instead of HTML.
   *
   * Returns the parsed JSON on success, null on any failure (logged).
   */
  async fetchJsonViaContext(url: string, timeoutMs = 25000): Promise<unknown | null> {
    if (!this.isReady()) return null;
    return this.runExclusive(async () => {
      const ctx = this.context as {
        request: {
          get: (
            url: string,
            opts: { headers?: Record<string, string>; timeout?: number },
          ) => Promise<{
            ok: () => boolean;
            status: () => number;
            text: () => Promise<string>;
          }>;
        };
      };
      try {
        const res = await ctx.request.get(url, {
          timeout: timeoutMs,
          headers: {
            Accept: 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            Referer: 'https://www.marinetraffic.com/',
          },
        });
        const status = res.status();
        const text = await res.text();
        if (!res.ok()) {
          log.warn(
            { url, status, bodyPreview: text.slice(0, 200) },
            'marinetraffic context fetch: non-2xx',
          );
          return null;
        }
        try {
          return JSON.parse(text);
        } catch {
          log.warn(
            { url, status, bodyPreview: text.slice(0, 200) },
            'marinetraffic context fetch: response not JSON',
          );
          return null;
        }
      } catch (err) {
        log.warn(
          { err: (err as Error).message, url },
          'marinetraffic context fetch threw',
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
