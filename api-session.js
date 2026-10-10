// ======== BROWSER SESSION TOKEN ========
// The credential a page sends on /api calls. Replaces the old shared API key
// that /api/config used to hand out: that key worked from anywhere, this
// token works only from this site, only for this browser, and only for a
// short while. The backend mints it (POST /api/session/token) for requests
// that come from one of our pages and binds it to the caller's address and
// User-Agent; it is refreshed here before it expires, and once more if a
// request ever comes back 401 for it.
//
// Pages keep their own apiFetch/userHeaders helpers; they read the token from
// here instead of a global API_KEY. Logged-in users still send their Supabase
// JWT exactly as before — that path is untouched.
//
// Load order: include after config.js (when a page has one) so API_BASE_URL is
// known; the value is read lazily at first use, not at load.
(function () {
  const STORE_KEY = 'ausapi_session';
  // Re-mint when less than this fraction of the lifetime remains, so a token
  // never expires mid-burst.
  const REFRESH_AT = 0.2;

  let cached = null; // { token, expiresAt }
  let inflight = null;

  function apiBase() {
    if (typeof API_BASE_URL !== 'undefined' && API_BASE_URL) return API_BASE_URL;
    if (typeof PROXY_BASE !== 'undefined' && PROXY_BASE) return PROXY_BASE;
    if (typeof API_BASE !== 'undefined' && API_BASE) return API_BASE;
    return 'https://api.forcequit.xyz';
  }

  function load() {
    if (cached) return cached;
    try {
      const raw = sessionStorage.getItem(STORE_KEY);
      if (raw) {
        const v = JSON.parse(raw);
        if (v && typeof v.token === 'string' && typeof v.expiresAt === 'number') cached = v;
      }
    } catch (e) { /* storage unavailable: memory only */ }
    return cached;
  }

  function save(v) {
    cached = v;
    try { sessionStorage.setItem(STORE_KEY, JSON.stringify(v)); } catch (e) { /* ignore */ }
  }

  function fresh(v) {
    if (!v) return false;
    const ttlLeft = v.expiresAt - Date.now();
    return ttlLeft > (v.ttlMs || 0) * REFRESH_AT;
  }

  async function mint() {
    if (inflight) return inflight;
    inflight = (async () => {
      const res = await fetch(`${apiBase()}/api/session/token`, { method: 'POST', cache: 'no-store' });
      if (!res.ok) throw new Error(`session token: HTTP ${res.status}`);
      const j = await res.json();
      if (!j || typeof j.token !== 'string') throw new Error('session token: bad response');
      const ttlMs = (Number(j.expiresIn) || 900) * 1000;
      const v = { token: j.token, expiresAt: Date.now() + ttlMs, ttlMs };
      save(v);
      return v.token;
    })().finally(() => { inflight = null; });
    return inflight;
  }

  // The current token, minting or refreshing as needed. Rejects when the
  // backend will not issue one (offline, rate-limited, not first-party) — the
  // caller's existing error handling sees that as it saw a missing key.
  async function token() {
    const v = load();
    if (fresh(v)) return v.token;
    return mint();
  }

  // Forget the token so the next call mints. Used after a 401.
  function invalidate() {
    cached = null;
    try { sessionStorage.removeItem(STORE_KEY); } catch (e) { /* ignore */ }
  }

  async function headers(extra) {
    const t = await token();
    return Object.assign({}, extra || {}, { Authorization: `Bearer ${t}` });
  }

  // fetch() with the token attached, retried once with a fresh token when the
  // backend says the token is the problem.
  async function apiFetch(url, options) {
    const opts = Object.assign({}, options || {});
    opts.headers = await headers(opts.headers);
    let res = await fetch(url, opts);
    if (res.status === 401) {
      invalidate();
      opts.headers = await headers(options && options.headers);
      res = await fetch(url, opts);
    }
    return res;
  }

  window.AusApi = { token, headers, fetch: apiFetch, invalidate };
})();
