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
import { readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { log } from '../lib/log.js';
import { requireRole, canManageNodes } from '../services/auth/roles.js';
import { resolveFeederToken } from '../services/auth/nodeToken.js';

export const nodeUpdatesRouter = new Hono();

const HERE = path.dirname(fileURLToPath(import.meta.url));
// Candidates, in order: dist/assets (copied on build, prod) then the source
// assets dir (dev via tsx, where this module runs from src/api).
const MANIFEST_CANDIDATES = [
  path.resolve(HERE, '../assets/node-versions.json'), // dist/api → dist/assets
  path.resolve(HERE, '../../assets/node-versions.json'), // src/api → <backend>/assets
];

let manifestCache: unknown = null;

function loadManifest(): unknown {
  if (manifestCache) return manifestCache;
  for (const p of MANIFEST_CANDIDATES) {
    if (existsSync(p)) {
      manifestCache = JSON.parse(readFileSync(p, 'utf8')) as unknown;
      return manifestCache;
    }
  }
  throw new Error(`node-versions.json not found in: ${MANIFEST_CANDIDATES.join(', ')}`);
}

// ---------------------------------------------------------------------------
// GET /api/node-updates/manifest — node-token authenticated.
// ---------------------------------------------------------------------------
nodeUpdatesRouter.get('/api/node-updates/manifest', async (c) => {
  const token = c.req.header('X-Node-Token');
  if (!token) return c.json({ error: 'missing node token' }, 401);
  const r = await resolveFeederToken(token);
  if (!r.ok) {
    if (r.reason === 'no_role') return c.json({ error: 'contributor role removed' }, 403);
    if (r.reason === 'unconfigured') return c.json({ error: 'feeder not configured' }, 503);
    return c.json({ error: 'unauthorized' }, 401);
  }
  try {
    return c.json(loadManifest() as object);
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
