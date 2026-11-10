/**
 * "Is this request coming from one of our own pages?"
 *
 * One definition, used by CORS (which origins may read responses) and by the
 * browser-token gate (which requests may mint and present a session token).
 * Keeping them identical matters: a page that CORS lets in but the token gate
 * refuses would load and then fail every fetch.
 *
 * Deliberately NARROW — exact first-party origins, not *.forcequit.xyz. The
 * sibling subdomains host third-party self-hosted apps (radio, pager) and a
 * compromise there must not be a pivot into this API.
 */
const FIRST_PARTY_ORIGIN_RE =
  /^https?:\/\/(localhost(:\d+)?|127\.0\.0\.1(:\d+)?|nswpsn\.forcequit\.xyz|forcequit\.xyz|www\.forcequit\.xyz|nswpsn\.org|www\.nswpsn\.org)$/i;

// Operator-extensible without a code change: CORS_ALLOWED_ORIGINS is a
// comma-separated list of EXACT origins (e.g. "https://foo.example") that are
// additionally first-party. Empty/whitespace entries are ignored.
const EXTRA_ORIGINS = new Set(
  (process.env['CORS_ALLOWED_ORIGINS'] ?? '')
    .split(',')
    .map((o) => o.trim())
    .filter((o) => o.length > 0),
);

export function isFirstPartyOrigin(origin: string | undefined | null): boolean {
  if (!origin) return false;
  return FIRST_PARTY_ORIGIN_RE.test(origin) || EXTRA_ORIGINS.has(origin);
}

function originOf(url: string | undefined): string | null {
  if (!url) return null;
  try {
    return new URL(url).origin;
  } catch {
    return null;
  }
}

/**
 * A browser on one of our pages identifies itself two ways, and both are
 * read: `Origin` (always present on a cross-origin fetch, which every call
 * from nswpsn.forcequit.xyz to api.forcequit.xyz is) and, failing that, the
 * `Referer`'s origin. `Sec-Fetch-Site`, when the browser sends it, must not
 * say cross-site.
 *
 * None of this is proof — a script can set any header — which is why the
 * token the mint hands out is short-lived and bound to the caller as well.
 * What this does is make the lazy cases fail: a typed URL, a curl with no
 * headers, a page on someone else's site.
 */
export function requestIsFirstParty(c: {
  req: { header: (k: string) => string | undefined };
}): boolean {
  const site = c.req.header('sec-fetch-site');
  if (site && site.toLowerCase() === 'cross-site') return false;
  const origin = c.req.header('origin');
  if (origin) return isFirstPartyOrigin(origin);
  return isFirstPartyOrigin(originOf(c.req.header('referer')));
}
