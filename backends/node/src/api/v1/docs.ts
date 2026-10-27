/**
 * The API documentation page.
 *
 *   GET /api/docs            the page (assets/docs/index.html)
 *   GET /api/docs/docs.js    its script — asks for the key, fetches the
 *                            description with it, starts the viewer
 *   GET /api/docs/scalar.js  the viewer (assets/scalar/, vendored Scalar)
 *
 * All three are public (services/auth/apiKey.ts): the page holds nothing. What
 * a visitor sees is decided by /api/v1/openapi.json (openapi.ts), which takes
 * the key the page sends and answers with the operations that key can read.
 *
 * The page's Content-Security-Policy allows this origin and nothing else, so
 * the viewer cannot fetch fonts, send telemetry or route "Try it" through a
 * third-party proxy even if a future version defaults to it (docs.js turns
 * all of those off as well; the CSP is the backstop). Inline styles are
 * allowed because the viewer injects its own.
 */
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Hono } from 'hono';
import type { Context } from 'hono';
import { log } from '../../lib/log.js';

/** The vendored viewer's version — the ?v= that makes its URL immutable. See assets/scalar/README.md. */
export const SCALAR_VERSION = '1.76.0';

const HERE = path.dirname(fileURLToPath(import.meta.url));
// dist/api/v1 → dist/assets (copied on build) first, then src/api/v1 →
// <backend>/assets (dev via tsx); the node-updates.ts lookup, one level deeper.
const ASSET_DIRS = [path.resolve(HERE, '../../assets'), path.resolve(HERE, '../../../assets')];

function readAsset(rel: string): Buffer | null {
  for (const dir of ASSET_DIRS) {
    try {
      return readFileSync(path.join(dir, rel));
    } catch {
      // next candidate
    }
  }
  return null;
}

const CSP = [
  "default-src 'none'",
  "script-src 'self'",
  "style-src 'self' 'unsafe-inline'",
  "img-src 'self' data: blob:",
  "font-src 'self' data:",
  "connect-src 'self'",
  "worker-src 'self' blob:",
  "base-uri 'none'",
  "form-action 'none'",
  "frame-ancestors 'none'",
].join('; ');

interface Assets {
  page: string;
  script: Buffer;
  scalar: Buffer;
  scalarBr: Buffer | null;
}
let assets: Assets | null | undefined;

/** Read once, on first use; null when the files are missing (a broken build). */
function load(): Assets | null {
  if (assets !== undefined) return assets;
  const page = readAsset('docs/index.html');
  const script = readAsset('docs/docs.js');
  const scalar = readAsset('scalar/standalone.js');
  if (!page || !script || !scalar) {
    log.error('api docs: assets/docs or assets/scalar missing; /api/docs will 503');
    assets = null;
    return null;
  }
  const docsVersion = createHash('sha256').update(script).digest('hex').slice(0, 12);
  assets = {
    page: page
      .toString('utf8')
      .replaceAll('__DOCS_VERSION__', docsVersion)
      .replaceAll('__SCALAR_VERSION__', SCALAR_VERSION),
    script,
    scalar,
    scalarBr: readAsset('scalar/standalone.js.br'),
  };
  return assets;
}

const IMMUTABLE = 'public, max-age=31536000, immutable';

function unavailable(c: Context): Response {
  return c.text('The API documentation is not available on this server.', 503);
}

export const docsRouter = new Hono();

docsRouter.get('/api/docs', (c) => {
  const a = load();
  if (!a) return unavailable(c);
  return c.html(a.page, 200, {
    'Content-Security-Policy': CSP,
    'Cache-Control': 'no-cache',
    'Referrer-Policy': 'no-referrer',
    'X-Content-Type-Options': 'nosniff',
  });
});

docsRouter.get('/api/docs/docs.js', (c) => {
  const a = load();
  if (!a) return unavailable(c);
  return c.body(new Uint8Array(a.script), 200, {
    'Content-Type': 'text/javascript; charset=utf-8',
    'Cache-Control': IMMUTABLE,
    'X-Content-Type-Options': 'nosniff',
  });
});

// 4.9 MB raw, about 1 MB brotli'd. Sent pre-compressed when the browser takes
// br (hono/compress leaves a response that already has Content-Encoding
// alone), which every current browser does; a client without br gets the raw
// file, gzipped per request by hono/compress like any other response.
docsRouter.get('/api/docs/scalar.js', (c) => {
  const a = load();
  if (!a) return unavailable(c);
  const headers: Record<string, string> = {
    'Content-Type': 'text/javascript; charset=utf-8',
    'Cache-Control': IMMUTABLE,
    'X-Content-Type-Options': 'nosniff',
    Vary: 'Accept-Encoding',
  };
  const acceptsBr = /\bbr\b/.test(c.req.header('Accept-Encoding') ?? '');
  if (a.scalarBr && acceptsBr) {
    return c.body(new Uint8Array(a.scalarBr), 200, { ...headers, 'Content-Encoding': 'br' });
  }
  return c.body(new Uint8Array(a.scalar), 200, headers);
});

/** Test seam: forget the cached files. */
export function _resetDocsAssets(): void {
  assets = undefined;
}
