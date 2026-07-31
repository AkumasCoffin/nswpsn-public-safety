/**
 * Node self-update manifest.
 *
 *   GET /api/node-updates/manifest  — node-authenticated (X-Node-Token). The
 *       agent polls this on start / every 6h / on cmd{action:'update'} to see
 *       if a newer agent / sdrtrunk / rdio build is available.
 *   GET /api/nodes/versions         — staff (owner|dev). Same manifest plus a
 *       note, so the Nodes tab can show the current published versions.
 *
 * The manifest itself is a static JSON file (assets/node-versions.json),
 * copied to dist/assets on build. The '/api/node-updates/' prefix is exempt
 * from the site NSWPSN_API_KEY gate (see services/auth/apiKey.ts) because the
 * node authenticates with its feeder token, exactly like /api/node-ingest/.
 */
import { Hono } from 'hono';
import { readFileSync, existsSync, openSync, fstatSync, closeSync } from 'node:fs';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { requireRole, canManageNodes } from '../services/auth/roles.js';
import { resolveNodeToken } from '../services/auth/nodeToken.js';
import { getAutoUpdate } from '../services/nodes/globalConfig.js';

export const nodeUpdatesRouter = new Hono();

const HERE = path.dirname(fileURLToPath(import.meta.url));
// Candidates, in order: dist/assets (copied on build, prod) then the source
// assets dir (dev via tsx, where this module runs from src/api).
const MANIFEST_CANDIDATES = [
  path.resolve(HERE, '../assets/node-versions.json'), // dist/api → dist/assets
  path.resolve(HERE, '../../assets/node-versions.json'), // src/api → <backend>/assets
];

// Resolve a relative NODE_DOWNLOADS_DIR against the backend module dir, NOT
// process.cwd(): pm2 keeps whatever cwd it was first started with (often the
// webroot, not backends/node), so a cwd-relative resolve would miss the
// downloads dir and leave every computed sha256 empty — which the agent reads
// as "no update available". HERE is <backend>/dist/api (prod) or <backend>/
// src/api (dev); '../..' is <backend> in both, so the default '../../downloads'
// lands on <webroot>/downloads regardless of cwd.
const BACKEND_DIR = path.resolve(HERE, '../..');
const DOWNLOADS_DIR = path.isAbsolute(config.NODE_DOWNLOADS_DIR)
  ? config.NODE_DOWNLOADS_DIR
  : path.resolve(BACKEND_DIR, config.NODE_DOWNLOADS_DIR);

interface Manifest {
  [component: string]:
    | { version: string; urls: Record<string, string>; sha256: Record<string, string> }
    | string;
}

let baseCache: Manifest | null = null;
function loadBaseManifest(): Manifest {
  if (baseCache) return baseCache;
  for (const p of MANIFEST_CANDIDATES) {
    if (existsSync(p)) {
      baseCache = JSON.parse(readFileSync(p, 'utf8')) as Manifest;
      return baseCache;
    }
  }
  throw new Error(`node-versions.json not found in: ${MANIFEST_CANDIDATES.join(', ')}`);
}

// sha256 cache keyed by file path, invalidated when the file's mtime/size
// changes (i.e. a new artifact was placed) so a big zip is hashed only once.
const shaCache = new Map<string, { mtimeMs: number; size: number; hash: string }>();
function sha256OfDownload(filename: string): string {
  const p = path.join(DOWNLOADS_DIR, filename);
  let fd: number;
  try {
    fd = openSync(p, 'r');
  } catch {
    return ''; // not present → empty sha means "no update available"
  }
  try {
    // Stat AND read through the SAME file descriptor so the file can't be
    // swapped between the check and the read (no TOCTOU): the cache key and the
    // hash always describe the exact same bytes.
    const st = fstatSync(fd);
    const cached = shaCache.get(p);
    if (cached && cached.mtimeMs === st.mtimeMs && cached.size === st.size) return cached.hash;
    const hash = createHash('sha256').update(readFileSync(fd)).digest('hex');
    shaCache.set(p, { mtimeMs: st.mtimeMs, size: st.size, hash });
    return hash;
  } finally {
    closeSync(fd);
  }
}

/**
 * The served manifest: the committed versions + urls, with sha256 computed
 * from whatever artifacts are actually present in the downloads dir. So an
 * operator just drops files in <webroot>/downloads — no hand-hashing, and the
 * agent only fetches an artifact whose sha256 is non-empty (present + hashed).
 */
function loadManifest(): Manifest {
  const base = loadBaseManifest();
  const out: Manifest = JSON.parse(JSON.stringify(base)) as Manifest;
  for (const key of Object.keys(out)) {
    const comp = out[key];
    if (!comp || typeof comp === 'string' || !comp.urls) continue;
    comp.sha256 = comp.sha256 ?? {};
    for (const [platform, url] of Object.entries(comp.urls)) {
      const filename = url.split('/').pop() ?? '';
      if (!filename) continue;
      // Only override with a computed hash when the artifact is actually
      // hosted locally (present in the downloads dir). For remotely-hosted
      // artifacts (e.g. an rdio binary served straight off GitHub) there's no
      // local file, so KEEP the sha256 committed in node-versions.json rather
      // than wiping it to '' — otherwise the agent would treat it as "skip".
      const local = sha256OfDownload(filename);
      if (local) comp.sha256[platform] = local;
    }
  }
  return out;
}

/**
 * Tailor the manifest to a node's kind. The agent's self-update code always
 * updates the component named "agent", so for a PAGER node we REMAP the
 * `pager-agent` entry onto `agent` (the pager binary) and drop the radio-only
 * `sdrtrunk`/`rdio` components it doesn't run. Radio nodes get the manifest
 * unchanged minus the internal `pager-agent` entry.
 */
function manifestForKind(full: Manifest, kind: string): Manifest {
  if (kind === 'pager') {
    const out: Manifest = {};
    const pa = full['pager-agent'];
    if (pa) out['agent'] = pa; // pager binary served as component "agent"
    return out;
  }
  const out: Manifest = {};
  for (const [k, v] of Object.entries(full)) {
    if (k === 'pager-agent') continue;
    out[k] = v;
  }
  return out;
}

// ---------------------------------------------------------------------------
// GET /api/node-updates/manifest — node-token authenticated.
// ---------------------------------------------------------------------------
nodeUpdatesRouter.get('/api/node-updates/manifest', async (c) => {
  const token = c.req.header('X-Node-Token');
  if (!token) return c.json({ error: 'missing node token' }, 401);
  const r = await resolveNodeToken(token);
  if (!r.ok) {
    if (r.reason === 'no_role') return c.json({ error: 'contributor role removed' }, 403);
    return c.json({ error: 'unauthorized' }, 401);
  }
  try {
    // Fold the global auto-update switch into the manifest so the agent can
    // decide whether an AUTOMATIC (startup / 6h) update pass may apply; manual
    // update commands always apply regardless of this flag. A missing field is
    // treated as enabled on the agent side.
    const autoUpdate = await getAutoUpdate();
    return c.json({ ...manifestForKind(loadManifest(), r.kind), autoUpdate } as object);
  } catch (err) {
    log.error({ err }, 'Error loading node update manifest');
    return c.json({ error: 'manifest unavailable' }, 503);
  }
});

// ---------------------------------------------------------------------------
// GET /api/nodes/versions — staff (owner|dev).
// ---------------------------------------------------------------------------
nodeUpdatesRouter.get('/api/nodes/versions', requireRole(canManageNodes), (c) => {
  try {
    return c.json({
      manifest: loadManifest(),
      note: 'Published component versions. Nodes pull these from /api/node-updates/manifest.',
    });
  } catch (err) {
    log.error({ err }, 'Error loading node update manifest (staff)');
    return c.json({ error: 'manifest unavailable' }, 503);
  }
});
