/**
 * "The Wire" — news & media API.
 *
 * Two entities share one shape: media_posts (photo/video sets, `caption`) and
 * articles (Markdown `body` + `slug`/`excerpt`/draft lifecycle). Both carry the
 * same tagging (units via per-item wire_media, agencies, pin|region location, a
 * single optional linked incident) and view tracking. Storage is external:
 * images in Cloudflare Images, videos in R2 (phase 1.5) — see services/wire.ts.
 *
 * Auth model (post-moderation):
 *   - GET (feed/detail) — public, key-gated like every other read.
 *   - POST create / upload-url — requireRole(canFeedMedia)  (owner|media_feeder)
 *   - PUT/DELETE — author, or an admin override (canManageUsers)
 *   - POST :id/remove — requireRole(canModerateWire)        (owner|team_member)
 *   - POST :id/view — public
 */
import { Hono } from 'hono';
import type { Pool, PoolClient } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { config } from '../config.js';
import {
  requireRole,
  canFeedMedia,
  canModerateWire,
  canManageUsers,
  isOwner,
} from '../services/auth/roles.js';
import { rememberCallsigns, normaliseCallsign } from '../services/callsigns.js';
import {
  createImageUploadUrl,
  deleteCfImage,
  r2Configured,
  createVideoUploadUrl,
  deleteR2Object,
  r2PublicUrl,
  MAX_VIDEO_BYTES,
  normaliseLicense,
  licenseLabel,
  slugify,
  viewerHash,
  shapeMedia,
  type WireMediaRow,
} from '../services/wire.js';

export const wireRouter = new Hono();

const DB_UNAVAILABLE = { error: 'database unavailable' } as const;

interface EntityCfg {
  table: 'media_posts' | 'articles';
  parentType: 'media_post' | 'article';
  maxImages: number;
  maxVideos: number;
}
const MEDIA: EntityCfg = { table: 'media_posts', parentType: 'media_post', maxImages: 6, maxVideos: 2 };
const ARTICLE: EntityCfg = { table: 'articles', parentType: 'article', maxImages: 4, maxVideos: 2 };

// ---- tiny context helpers (mirrors incidents.ts) ---------------------------
function currentUserId(c: { get: (k: string) => unknown }): string | undefined {
  const v = c.get('userId');
  return typeof v === 'string' && v.length > 0 ? v : undefined;
}
function currentUserName(c: { get: (k: string) => unknown }): string | null {
  const v = c.get('userName');
  return typeof v === 'string' && v ? v : null;
}
function clientIp(c: { req: { header: (k: string) => string | undefined } }): string {
  const fwd = c.req.header('cf-connecting-ip') || c.req.header('x-forwarded-for') || '';
  return fwd.split(',')[0]?.trim() || 'unknown';
}

/** Whether contributor posts require approval before publishing (staff toggle).
 * Defaults true (pre-moderation). */
async function wireApprovalRequired(pool: Pool): Promise<boolean> {
  try {
    const r = await pool.query<{ value: string }>(`SELECT value FROM app_settings WHERE key = 'wire_approval_required'`);
    return r.rowCount === 0 ? true : r.rows[0]!.value !== 'false';
  } catch {
    return true;
  }
}

// Soft launch: while WIRE_PUBLIC !== 'true', only the owner may read the feed —
// this enforces the "coming soon for everyone except owner" gate server-side
// (the frontend banner alone is cosmetic). Flip WIRE_PUBLIC=true at launch.
async function wireReadable(c: { get: (k: string) => unknown }): Promise<boolean> {
  if (config.WIRE_PUBLIC === 'true') return true;
  const uid = currentUserId(c);
  return !!(uid && (await isOwner(uid)));
}

// ---- validation ------------------------------------------------------------

function cleanAgencies(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const a of raw) {
    if (typeof a !== 'string') continue;
    const s = a.trim().toLowerCase().replace(/[^a-z0-9-]/g, '').slice(0, 40);
    if (!s || seen.has(s)) continue;
    seen.add(s);
    out.push(s);
    if (out.length >= 20) break;
  }
  return out;
}

interface LocationFields {
  location_type: 'pin' | 'region';
  region: string | null;
  lat: number | null;
  lng: number | null;
}
function cleanLocation(data: Record<string, unknown>): LocationFields {
  const type = data['location_type'] === 'region' ? 'region' : 'pin';
  const toNum = (v: unknown): number | null => {
    const n = typeof v === 'number' ? v : typeof v === 'string' && v.trim() ? Number(v) : NaN;
    return Number.isFinite(n) ? n : null;
  };
  const region = type === 'region' && typeof data['region'] === 'string' ? (data['region'] as string).trim().slice(0, 120) || null : null;
  return { location_type: type, region, lat: toNum(data['lat']), lng: toNum(data['lng']) };
}

interface IncidentRef { source: string | null; source_id: string; title: string | null; }
/** Parse the linked-incident from a payload: prefer the {source,source_id,title}
 * logs-feed object; fall back to a legacy bare incident_id (a user_incident). */
function parseIncident(data: Record<string, unknown>): IncidentRef | null {
  const inc = data['incident'];
  if (inc && typeof inc === 'object') {
    const o = inc as Record<string, unknown>;
    const source_id = typeof o['source_id'] === 'string' ? o['source_id'].slice(0, 200) : '';
    if (source_id) {
      return {
        source: typeof o['source'] === 'string' ? o['source'].slice(0, 60) : null,
        source_id,
        title: typeof o['title'] === 'string' ? o['title'].slice(0, 300) : null,
      };
    }
  }
  if (typeof data['incident_id'] === 'string' && data['incident_id']) {
    return { source: 'user_incident', source_id: data['incident_id'], title: null };
  }
  return null;
}

interface MediaItem {
  kind: 'image' | 'video';
  cf_image_id: string | null;
  r2_key: string | null;
  poster_cf_image_id: string | null;
  poster_r2_key: string | null;
  hash: string | null;
  duration_seconds: number | null;
  is_cover: boolean;
  unit: string | null;
  width: number | null;
  height: number | null;
  bytes: number | null;
}

/** Validate + normalise the media array. Returns items in display order plus
 * the distinct units to remember, or an { error } string for a 400. */
function validateMedia(raw: unknown, cfg: EntityCfg): { items: MediaItem[]; units: string[] } | { error: string } {
  const arr = Array.isArray(raw) ? raw : [];
  const images: MediaItem[] = [];
  const videos: MediaItem[] = [];
  const str = (v: unknown): string | null => (typeof v === 'string' && v.trim() ? v.trim() : null);
  const int = (v: unknown): number | null => {
    const n = typeof v === 'number' ? Math.trunc(v) : NaN;
    return Number.isFinite(n) ? n : null;
  };
  for (const m of arr) {
    if (!m || typeof m !== 'object') continue;
    const o = m as Record<string, unknown>;
    const kind = o['kind'] === 'video' ? 'video' : 'image';
    const item: MediaItem = {
      kind,
      cf_image_id: str(o['cf_image_id']),
      r2_key: str(o['r2_key']),
      poster_cf_image_id: str(o['poster_cf_image_id']),
      poster_r2_key: str(o['poster_r2_key']),
      hash: typeof o['hash'] === 'string' ? o['hash'].slice(0, 128) : null,
      duration_seconds: int(o['duration_seconds']),
      is_cover: o['is_cover'] === true,
      unit: normaliseCallsign(o['unit']) || null,
      width: int(o['width']),
      height: int(o['height']),
      bytes: int(o['bytes']),
    };
    if (kind === 'image') {
      if (!item.r2_key) return { error: 'each image needs an r2_key' };
      images.push(item);
    } else {
      if (!item.r2_key) return { error: 'each video needs an r2_key' };
      item.is_cover = false; // videos are never the cover
      videos.push(item);
    }
  }
  if (images.length > cfg.maxImages) return { error: `at most ${cfg.maxImages} images` };
  if (videos.length > cfg.maxVideos) return { error: `at most ${cfg.maxVideos} videos` };

  // Exactly one cover among images (articles need it; harmless for posts).
  if (images.length > 0) {
    let coverIdx = images.findIndex((i) => i.is_cover);
    if (coverIdx < 0) coverIdx = 0;
    images.forEach((i, idx) => (i.is_cover = idx === coverIdx));
  }

  const items = [...images, ...videos];
  const units = [...new Set(items.map((i) => i.unit).filter((u): u is string => !!u))];
  return { items, units };
}

async function insertMediaRows(client: PoolClient, cfg: EntityCfg, parentId: string, items: MediaItem[]): Promise<void> {
  for (let i = 0; i < items.length; i++) {
    const m = items[i]!;
    await client.query(
      `INSERT INTO wire_media
         (parent_type, parent_id, kind, cf_image_id, r2_key, poster_cf_image_id, poster_r2_key,
          duration_seconds, is_cover, unit, sort_order, width, height, bytes, hash)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
      [cfg.parentType, parentId, m.kind, m.cf_image_id, m.r2_key, m.poster_cf_image_id, m.poster_r2_key,
        m.duration_seconds, m.is_cover, m.unit, i, m.width, m.height, m.bytes, m.hash],
    );
  }
}

/** Fetch wire_media for a set of parents, grouped by parent_id (avoids N+1). */
async function fetchMediaFor(pool: Pool, parentType: string, ids: string[]): Promise<Map<string, WireMediaRow[]>> {
  const map = new Map<string, WireMediaRow[]>();
  if (ids.length === 0) return map;
  const r = await pool.query<WireMediaRow>(
    `SELECT * FROM wire_media WHERE parent_type = $1 AND parent_id = ANY($2::text[]) ORDER BY sort_order`,
    [parentType, ids],
  );
  for (const row of r.rows) {
    const list = map.get(row.parent_id) ?? [];
    list.push(row);
    map.set(row.parent_id, list);
  }
  return map;
}

function deriveUnits(media: WireMediaRow[]): string[] {
  return [...new Set(media.map((m) => m.unit).filter((u): u is string => !!u))];
}

// ---- response shaping ------------------------------------------------------

function isoOrNull(v: unknown): string | null {
  if (v instanceof Date) return v.toISOString();
  return typeof v === 'string' ? v : null;
}

/* eslint-disable @typescript-eslint/no-explicit-any */
function shapeMediaPost(row: any, media: WireMediaRow[]): Record<string, unknown> {
  const shaped = media.map(shapeMedia);
  const cover = shaped.find((m) => m['is_cover']) ?? shaped[0] ?? null;
  return {
    id: row.id,
    kind: 'media_post',
    title: row.title,
    caption: row.caption,
    location: { type: row.location_type, region: row.region, lat: row.lat, lng: row.lng },
    agencies: row.agencies ?? [],
    incident_id: row.incident_id,
    incident: row.incident || (row.incident_id ? { source: 'user_incident', source_id: row.incident_id, title: null } : null),
    units: deriveUnits(media),
    license: row.license || 'credit',
    license_label: licenseLabel(row.license || 'credit'),
    credit: row.credit || null,
    views: Number(row.views) || 0,
    status: row.status,
    review_note: row.review_note ?? null,
    author: { id: row.author_id, name: row.author_name },
    media: shaped,
    cover,
    created_at: isoOrNull(row.created_at),
    updated_at: isoOrNull(row.updated_at),
  };
}

function shapeArticle(row: any, media: WireMediaRow[]): Record<string, unknown> {
  const shaped = media.map(shapeMedia);
  const cover = shaped.find((m) => m['is_cover']) ?? shaped.find((m) => m['kind'] === 'image') ?? null;
  return {
    id: row.id,
    kind: 'article',
    title: row.title,
    slug: row.slug,
    excerpt: row.excerpt,
    body: row.body,
    location: { type: row.location_type, region: row.region, lat: row.lat, lng: row.lng },
    agencies: row.agencies ?? [],
    incident_id: row.incident_id,
    incident: row.incident || (row.incident_id ? { source: 'user_incident', source_id: row.incident_id, title: null } : null),
    units: deriveUnits(media),
    license: row.license || 'credit',
    license_label: licenseLabel(row.license || 'credit'),
    credit: row.credit || null,
    views: Number(row.views) || 0,
    status: row.status,
    review_note: row.review_note ?? null,
    author: { id: row.author_id, name: row.author_name },
    media: shaped,
    cover,
    published_at: isoOrNull(row.published_at),
    created_at: isoOrNull(row.created_at),
    updated_at: isoOrNull(row.updated_at),
  };
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ---- slug uniqueness -------------------------------------------------------
async function uniqueSlug(pool: Pool, title: string, excludeId?: string): Promise<string> {
  const base = slugify(title);
  for (let attempt = 0; attempt < 50; attempt++) {
    const candidate = attempt === 0 ? base : `${base}-${attempt + 1}`;
    const r = await pool.query<{ id: string }>('SELECT id FROM articles WHERE slug = $1', [candidate]);
    if (r.rowCount === 0 || (excludeId && r.rows[0]?.id === excludeId)) return candidate;
  }
  return `${base}-${Date.now().toString(36)}`;
}

// ===========================================================================
// UPLOAD URLS
// ===========================================================================

wireRouter.post('/api/wire/upload-url', requireRole(canFeedMedia), async (c) => {
  // Images now go to R2 (same bucket as video). The browser optimises the
  // photo (downscale + WebP, which also strips GPS) and PUTs it to uploadURL.
  if (!r2Configured()) return c.json({ error: 'image uploads not configured' }, 503);
  const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
  const hash = typeof body['hash'] === 'string' ? body['hash'].slice(0, 128) : null;
  // De-dup: if this exact image was uploaded before, reuse its R2 object rather
  // than uploading again. The client skips the PUT when `duplicate` is set.
  if (hash) {
    const pool = await getPool();
    if (pool) {
      // Only reuse an object from an already-PUBLISHED, non-taken-down post, so
      // dedup can't resurrect a removed/taken-down image or reveal whether an
      // image exists in someone else's draft/pending post.
      const dup = await pool.query<{ r2_key: string }>(
        `SELECT wm.r2_key FROM wire_media wm
           JOIN media_posts mp ON wm.parent_type='media_post' AND wm.parent_id = mp.id
          WHERE wm.hash=$1 AND wm.r2_key IS NOT NULL AND mp.status='published' AND mp.taken_down_at IS NULL
         UNION
         SELECT wm.r2_key FROM wire_media wm
           JOIN articles a ON wm.parent_type='article' AND wm.parent_id = a.id
          WHERE wm.hash=$1 AND wm.r2_key IS NOT NULL AND a.status='published' AND a.taken_down_at IS NULL
         LIMIT 1`, [hash]);
      const existing = dup.rows[0]?.r2_key;
      if (existing) return c.json({ duplicate: true, key: existing, publicUrl: r2PublicUrl(existing), hash });
    }
  }
  const up = await createImageUploadUrl();
  if (!up) return c.json({ error: 'could not create upload url' }, 503);
  return c.json({ uploadURL: up.uploadURL, key: up.key, publicUrl: up.publicUrl, hash });
});

wireRouter.post('/api/wire/video-upload-url', requireRole(canFeedMedia), async (c) => {
  if (!r2Configured()) return c.json({ error: 'video uploads not configured' }, 503);
  const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
  const size = Number(body['size']) || 0;
  if (size <= 0) return c.json({ error: 'size (bytes) is required' }, 400);
  if (size > MAX_VIDEO_BYTES) return c.json({ error: 'video too large (50MB max)' }, 413);
  const up = await createVideoUploadUrl(size);
  if (!up) return c.json({ error: 'could not create upload url' }, 503);
  // Browser must PUT with Content-Type: video/mp4 (it's part of the signature).
  return c.json({ ...up, maxBytes: MAX_VIDEO_BYTES });
});

// ===========================================================================
// MEDIA POSTS
// ===========================================================================

wireRouter.get('/api/wire/media', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  if (!(await wireReadable(c))) return c.json({ posts: [] });
  try {
    const url = new URL(c.req.url);
    const mine = url.searchParams.get('mine') === '1';
    const uid = currentUserId(c);
    const agency = url.searchParams.get('agency');
    const unit = normaliseCallsign(url.searchParams.get('unit') || '') || null;
    const region = url.searchParams.get('region');
    const q = (url.searchParams.get('q') || '').trim();
    const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') ?? 48) || 48));
    const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

    const vals: unknown[] = [];
    const where: string[] = [];
    if (mine && uid) { vals.push(uid); where.push(`author_id = $${vals.length}`); }
    else {
      where.push(`status = 'published' AND taken_down_at IS NULL`);
      // Public "posts by this contributor" (their published posts only).
      const author = new URL(c.req.url).searchParams.get('author');
      if (author) { vals.push(author); where.push(`author_id = $${vals.length}`); }
    }
    if (q) { vals.push(`%${q}%`); where.push(`(title ILIKE $${vals.length} OR caption ILIKE $${vals.length})`); }
    if (agency) { vals.push(JSON.stringify([agency])); where.push(`agencies @> $${vals.length}::jsonb`); }
    if (region) { vals.push(region); where.push(`region = $${vals.length}`); }
    if (unit) {
      vals.push(unit);
      where.push(`EXISTS (SELECT 1 FROM wire_media wm WHERE wm.parent_type='media_post' AND wm.parent_id = media_posts.id AND wm.unit = $${vals.length})`);
    }
    vals.push(limit, offset);
    const r = await pool.query(
      `SELECT * FROM media_posts WHERE ${where.join(' AND ')}
        ORDER BY created_at DESC LIMIT $${vals.length - 1} OFFSET $${vals.length}`,
      vals,
    );
    const ids = r.rows.map((row) => row.id);
    const mediaMap = await fetchMediaFor(pool, 'media_post', ids);
    const posts = r.rows.map((row) => shapeMediaPost(row, mediaMap.get(row.id) ?? []));
    return c.json({ posts });
  } catch (err) {
    log.error({ err }, 'wire: list media failed');
    return c.json({ error: 'failed to list media posts' }, 500);
  }
});

wireRouter.get('/api/wire/media/:id', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  if (!(await wireReadable(c))) return c.json({ error: 'not found' }, 404);
  const id = c.req.param('id');
  try {
    const r = await pool.query('SELECT * FROM media_posts WHERE id = $1', [id]);
    if (r.rowCount === 0) return c.json({ error: 'not found' }, 404);
    const row = r.rows[0];
    const uid = currentUserId(c);
    const isAuthor = uid && row.author_id === uid;
    const isAdmin = !!(uid && (await canManageUsers(uid)));
    if (row.taken_down_at && !isAuthor && !isAdmin) {
      return c.json({ tombstone: { type: 'media_post', taken_down_at: isoOrNull(row.taken_down_at) } });
    }
    if (row.status !== 'published' && !isAuthor && !isAdmin) {
      return c.json({ error: 'not found' }, 404);
    }
    const mediaMap = await fetchMediaFor(pool, 'media_post', [id]);
    return c.json({ post: shapeMediaPost(row, mediaMap.get(id) ?? []) });
  } catch (err) {
    log.error({ err, id }, 'wire: get media failed');
    return c.json({ error: 'failed to fetch media post' }, 500);
  }
});

// Canonical public site origin (the static frontend behind Cloudflare). Used
// to build the canonical share URL that the OG tags point back to.
const SITE_BASE = 'https://nswpsn.forcequit.xyz';

// Public Open Graph metadata for link unfurls. The Cloudflare Worker on
// nswpsn.forcequit.xyz/wire* calls this to inject per-post OG/Twitter tags for
// social crawlers (Discord, X, Facebook…), which don't run JS and so never see
// the client-rendered detail. Only PUBLISHED, non-taken-down posts, and only
// once the Wire itself is public — during soft launch everything stays private,
// so embeds stay dark too. No API key required (crawlers can't send one), but
// nothing sensitive is exposed: title, a short description, the cover image, and
// the canonical URL of already-public content.
wireRouter.get('/api/wire/og/:type/:key', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  if (config.WIRE_PUBLIC !== 'true') return c.json({ error: 'not found' }, 404);
  const type = c.req.param('type');
  const key = c.req.param('key');
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let row: any = null;
    let parentType: 'media_post' | 'article';
    let canonical = '';
    if (type === 'media' || type === 'post') {
      const r = await pool.query('SELECT * FROM media_posts WHERE id = $1', [key]);
      row = r.rows[0];
      parentType = 'media_post';
      if (row) canonical = `${SITE_BASE}/wire?tab=media&post=${encodeURIComponent(row.id)}`;
    } else if (type === 'article' || type === 'articles') {
      const r = await pool.query('SELECT * FROM articles WHERE slug = $1 OR id = $1', [key]);
      row = r.rows[0];
      parentType = 'article';
      if (row) canonical = `${SITE_BASE}/wire?tab=articles&article=${encodeURIComponent(row.slug)}`;
    } else {
      return c.json({ error: 'not found' }, 404);
    }
    if (!row || row.status !== 'published' || row.taken_down_at) {
      return c.json({ error: 'not found' }, 404);
    }
    const mediaMap = await fetchMediaFor(pool, parentType, [row.id]);
    const shaped = (mediaMap.get(row.id) ?? []).map(shapeMedia);
    const cover =
      shaped.find((m) => m['is_cover']) ??
      shaped.find((m) => m['kind'] === 'image') ??
      shaped[0] ??
      null;
    const image = cover ? (cover['url'] || cover['poster_url'] || null) : null;
    const rawDesc = (parentType === 'article' ? row.excerpt : row.caption) || '';
    const description = String(rawDesc).replace(/\s+/g, ' ').trim().slice(0, 200);
    return c.json({
      og: {
        type: parentType === 'article' ? 'article' : 'website',
        title: row.title || 'The Wire',
        description:
          description ||
          'Independent photos & video from NSW emergency-services contributors.',
        image: image || null,
        url: canonical,
        site_name: 'NSWPSN — The Wire',
        author: row.author_name || null,
      },
    });
  } catch (err) {
    log.error({ err, type, key }, 'wire: og lookup failed');
    return c.json({ error: 'not found' }, 404);
  }
});

wireRouter.post('/api/wire/media', requireRole(canFeedMedia), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const title = typeof data['title'] === 'string' ? data['title'].trim().slice(0, 300) : '';
    if (!title) return c.json({ error: 'title is required' }, 400);
    const caption = typeof data['caption'] === 'string' ? data['caption'].slice(0, 4000) : '';
    const loc = cleanLocation(data);
    const agencies = cleanAgencies(data['agencies']);
    const incident = parseIncident(data);
    const incidentId = incident?.source_id ?? null;
    const incidentJson = incident ? JSON.stringify(incident) : null;
    const license = normaliseLicense(data['license']);
    const credit = typeof data['credit'] === 'string' ? data['credit'].trim().slice(0, 200) || null : null;
    if (data['rights_affirmed'] !== true) return c.json({ error: 'you must confirm you own or have the rights to publish this' }, 400);
    const mv = validateMedia(data['media'], MEDIA);
    if ('error' in mv) return c.json({ error: mv.error }, 400);

    const authorId = currentUserId(c)!;
    const authorName = currentUserName(c);
    // Approval mode (staff toggle): when required, non-moderators' posts are
    // 'pending' until approved; moderators always publish instantly. When the
    // toggle is off, everyone publishes instantly.
    const status = (!(await wireApprovalRequired(pool)) || (await canModerateWire(authorId))) ? 'published' : 'pending';
    const client = await pool.connect();
    let newId: string;
    try {
      await client.query('BEGIN');
      const ins = await client.query<{ id: string }>(
        `INSERT INTO media_posts (author_id, author_name, title, caption, location_type, region, lat, lng, agencies, incident_id, license, credit, rights_affirmed, status, incident)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,true,$13,$14::jsonb) RETURNING id`,
        [authorId, authorName, title, caption, loc.location_type, loc.region, loc.lat, loc.lng, JSON.stringify(agencies), incidentId, license, credit, status, incidentJson],
      );
      newId = ins.rows[0]!.id;
      await insertMediaRows(client, MEDIA, newId, mv.items);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
    await rememberCallsigns(pool, mv.units);
    return c.json({ id: newId, success: true, status }, 201);
  } catch (err) {
    log.error({ err }, 'wire: create media failed');
    return c.json({ error: 'failed to create media post' }, 500);
  }
});

wireRouter.put('/api/wire/media/:id', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  const uid = currentUserId(c);
  if (!uid) return c.json({ error: 'authentication required' }, 401);
  try {
    const existing = await pool.query<{ author_id: string; taken_down_at: unknown; status: string }>('SELECT author_id, taken_down_at, status FROM media_posts WHERE id = $1', [id]);
    if (existing.rowCount === 0) return c.json({ error: 'not found' }, 404);
    if (existing.rows[0]!.author_id !== uid && !(await canManageUsers(uid))) {
      return c.json({ error: 'forbidden' }, 403);
    }
    if (existing.rows[0]!.taken_down_at) return c.json({ error: 'this content was removed following a rights complaint and cannot be edited' }, 409);
    // Editing a rejected post resends it for review. Otherwise status is unchanged.
    const statusSet = existing.rows[0]!.status === 'rejected' ? `, status='pending'` : '';
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const title = typeof data['title'] === 'string' ? data['title'].trim().slice(0, 300) : '';
    if (!title) return c.json({ error: 'title is required' }, 400);
    const caption = typeof data['caption'] === 'string' ? data['caption'].slice(0, 4000) : '';
    const loc = cleanLocation(data);
    const agencies = cleanAgencies(data['agencies']);
    const incident = parseIncident(data);
    const incidentId = incident?.source_id ?? null;
    const incidentJson = incident ? JSON.stringify(incident) : null;
    const license = normaliseLicense(data['license']);
    const credit = typeof data['credit'] === 'string' ? data['credit'].trim().slice(0, 200) || null : null;
    const mv = validateMedia(data['media'], MEDIA);
    if ('error' in mv) return c.json({ error: mv.error }, 400);

    const oldImageIds = await imageIdsFor(pool, 'media_post', id);
    const oldR2Keys = await r2KeysFor(pool, 'media_post', id);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `UPDATE media_posts SET title=$1, caption=$2, location_type=$3, region=$4, lat=$5, lng=$6,
           agencies=$7::jsonb, incident_id=$8, license=$9, credit=$10, incident=$12::jsonb, updated_at=now()${statusSet} WHERE id=$11`,
        [title, caption, loc.location_type, loc.region, loc.lat, loc.lng, JSON.stringify(agencies), incidentId, license, credit, id, incidentJson],
      );
      await client.query('DELETE FROM wire_media WHERE parent_type=$1 AND parent_id=$2', ['media_post', id]);
      await insertMediaRows(client, MEDIA, id, mv.items);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
    await rememberCallsigns(pool, mv.units);
    // Best-effort: delete CF images no longer referenced.
    const keptIds = new Set(mv.items.flatMap((m) => [m.cf_image_id, m.poster_cf_image_id]).filter(Boolean) as string[]);
    for (const old of oldImageIds) if (!keptIds.has(old)) await deleteCfImage(old);
    const keptKeys = new Set(mv.items.flatMap((m) => [m.r2_key, m.poster_r2_key]).filter(Boolean) as string[]);
    for (const old of oldR2Keys) if (!keptKeys.has(old)) await safeDeleteR2(pool, old);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'wire: update media failed');
    return c.json({ error: 'failed to update media post' }, 500);
  }
});

wireRouter.delete('/api/wire/media/:id', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  const uid = currentUserId(c);
  if (!uid) return c.json({ error: 'authentication required' }, 401);
  try {
    const existing = await pool.query<{ author_id: string }>('SELECT author_id FROM media_posts WHERE id = $1', [id]);
    if (existing.rowCount === 0) return c.json({ error: 'not found' }, 404);
    if (existing.rows[0]!.author_id !== uid && !(await canManageUsers(uid))) {
      return c.json({ error: 'forbidden' }, 403);
    }
    const imgIds = await imageIdsFor(pool, 'media_post', id);
    const r2keys = await r2KeysFor(pool, 'media_post', id);
    await pool.query('DELETE FROM wire_media WHERE parent_type=$1 AND parent_id=$2', ['media_post', id]);
    await pool.query('DELETE FROM media_posts WHERE id = $1', [id]);
    for (const iid of imgIds) await deleteCfImage(iid);
    for (const k of r2keys) await safeDeleteR2(pool, k);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'wire: delete media failed');
    return c.json({ error: 'failed to delete media post' }, 500);
  }
});

wireRouter.post('/api/wire/media/:id/remove', requireRole(canModerateWire), async (c) => {
  return softRemove(c, MEDIA);
});

wireRouter.post('/api/wire/media/:id/view', async (c) => {
  return countView(c, 'media_post', 'media_posts');
});

// ===========================================================================
// ARTICLES
// ===========================================================================

wireRouter.get('/api/wire/articles', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  if (!(await wireReadable(c))) return c.json({ articles: [] });
  try {
    const url = new URL(c.req.url);
    const mine = url.searchParams.get('mine') === '1';
    const uid = currentUserId(c);
    const agency = url.searchParams.get('agency');
    const region = url.searchParams.get('region');
    const q = (url.searchParams.get('q') || '').trim();
    const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') ?? 48) || 48));
    const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

    const vals: unknown[] = [];
    const where: string[] = [];
    if (mine && uid) { vals.push(uid); where.push(`author_id = $${vals.length}`); }
    else {
      where.push(`status = 'published' AND taken_down_at IS NULL`);
      // Public "posts by this contributor" (their published posts only).
      const author = new URL(c.req.url).searchParams.get('author');
      if (author) { vals.push(author); where.push(`author_id = $${vals.length}`); }
    }
    if (q) { vals.push(`%${q}%`); where.push(`(title ILIKE $${vals.length} OR excerpt ILIKE $${vals.length} OR body ILIKE $${vals.length})`); }
    if (agency) { vals.push(JSON.stringify([agency])); where.push(`agencies @> $${vals.length}::jsonb`); }
    if (region) { vals.push(region); where.push(`region = $${vals.length}`); }
    vals.push(limit, offset);
    const order = mine ? 'updated_at DESC' : 'published_at DESC NULLS LAST';
    const r = await pool.query(
      `SELECT * FROM articles WHERE ${where.join(' AND ')} ORDER BY ${order} LIMIT $${vals.length - 1} OFFSET $${vals.length}`,
      vals,
    );
    const ids = r.rows.map((row) => row.id);
    const mediaMap = await fetchMediaFor(pool, 'article', ids);
    const articles = r.rows.map((row) => shapeArticle(row, mediaMap.get(row.id) ?? []));
    return c.json({ articles });
  } catch (err) {
    log.error({ err }, 'wire: list articles failed');
    return c.json({ error: 'failed to list articles' }, 500);
  }
});

wireRouter.get('/api/wire/articles/:slug', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  if (!(await wireReadable(c))) return c.json({ error: 'not found' }, 404);
  const slug = c.req.param('slug');
  try {
    // Accept a slug (public URLs) OR an id (the compose edit link uses the id).
    const r = await pool.query('SELECT * FROM articles WHERE slug = $1 OR id = $1', [slug]);
    if (r.rowCount === 0) return c.json({ error: 'not found' }, 404);
    const row = r.rows[0];
    const uid = currentUserId(c);
    const isAuthor = uid && row.author_id === uid;
    const isAdmin = !!(uid && (await canManageUsers(uid)));
    if (row.taken_down_at && !isAuthor && !isAdmin) {
      return c.json({ tombstone: { type: 'article', taken_down_at: isoOrNull(row.taken_down_at) } });
    }
    if (row.status !== 'published' && !isAuthor && !isAdmin) {
      return c.json({ error: 'not found' }, 404);
    }
    const mediaMap = await fetchMediaFor(pool, 'article', [row.id]);
    return c.json({ article: shapeArticle(row, mediaMap.get(row.id) ?? []) });
  } catch (err) {
    log.error({ err, slug }, 'wire: get article failed');
    return c.json({ error: 'failed to fetch article' }, 500);
  }
});

wireRouter.post('/api/wire/articles', requireRole(canFeedMedia), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const title = typeof data['title'] === 'string' ? data['title'].trim().slice(0, 300) : '';
    if (!title) return c.json({ error: 'title is required' }, 400);
    const excerpt = typeof data['excerpt'] === 'string' ? data['excerpt'].slice(0, 600) : '';
    const body = typeof data['body'] === 'string' ? data['body'].slice(0, 100_000) : '';
    // Pre-moderation: "publish" from a moderator goes live; from anyone else it
    // becomes 'pending' review. Drafts stay private to the author.
    const wantPublish = data['status'] === 'published';
    const authorId = currentUserId(c)!;
    const status = wantPublish ? ((!(await wireApprovalRequired(pool)) || (await canModerateWire(authorId))) ? 'published' : 'pending') : 'draft';
    const loc = cleanLocation(data);
    const agencies = cleanAgencies(data['agencies']);
    const incident = parseIncident(data);
    const incidentId = incident?.source_id ?? null;
    const incidentJson = incident ? JSON.stringify(incident) : null;
    const license = normaliseLicense(data['license']);
    const credit = typeof data['credit'] === 'string' ? data['credit'].trim().slice(0, 200) || null : null;
    if (wantPublish && data['rights_affirmed'] !== true) return c.json({ error: 'you must confirm you own or have the rights to publish this' }, 400);
    const mv = validateMedia(data['media'], ARTICLE);
    if ('error' in mv) return c.json({ error: mv.error }, 400);
    const slug = await uniqueSlug(pool, title);

    const authorName = currentUserName(c);
    const rightsAffirmed = data['rights_affirmed'] === true;
    const client = await pool.connect();
    let newId: string;
    try {
      await client.query('BEGIN');
      const ins = await client.query<{ id: string }>(
        `INSERT INTO articles (author_id, author_name, title, slug, excerpt, body, status, published_at,
           location_type, region, lat, lng, agencies, incident_id, license, credit, rights_affirmed, incident)
         VALUES ($1,$2,$3,$4,$5,$6,$7,${status === 'published' ? 'now()' : 'NULL'},$8,$9,$10,$11,$12::jsonb,$13,$14,$15,$16,$17::jsonb) RETURNING id`,
        [authorId, authorName, title, slug, excerpt, body, status, loc.location_type, loc.region, loc.lat, loc.lng, JSON.stringify(agencies), incidentId, license, credit, rightsAffirmed, incidentJson],
      );
      newId = ins.rows[0]!.id;
      await insertMediaRows(client, ARTICLE, newId, mv.items);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
    await rememberCallsigns(pool, mv.units);
    return c.json({ id: newId, slug, success: true, status }, 201);
  } catch (err) {
    log.error({ err }, 'wire: create article failed');
    return c.json({ error: 'failed to create article' }, 500);
  }
});

wireRouter.put('/api/wire/articles/:id', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  const uid = currentUserId(c);
  if (!uid) return c.json({ error: 'authentication required' }, 401);
  try {
    const existing = await pool.query<{ author_id: string; status: string; published_at: unknown; taken_down_at: unknown }>(
      'SELECT author_id, status, published_at, taken_down_at FROM articles WHERE id = $1', [id]);
    if (existing.rowCount === 0) return c.json({ error: 'not found' }, 404);
    const prev = existing.rows[0]!;
    const isAdmin = await canManageUsers(uid);
    if (prev.author_id !== uid && !isAdmin) return c.json({ error: 'forbidden' }, 403);
    if (prev.taken_down_at) return c.json({ error: 'this content was removed following a rights complaint and cannot be edited' }, 409);
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const title = typeof data['title'] === 'string' ? data['title'].trim().slice(0, 300) : '';
    if (!title) return c.json({ error: 'title is required' }, 400);
    const excerpt = typeof data['excerpt'] === 'string' ? data['excerpt'].slice(0, 600) : '';
    const body = typeof data['body'] === 'string' ? data['body'].slice(0, 100_000) : '';
    // Pre-moderation: a moderator's "publish" goes live; a contributor's
    // "publish" becomes 'pending' review (unless the article is already public,
    // in which case their edits stay live). Drafts stay drafts.
    let status: string;
    if (data['status'] === 'published') status = isAdmin ? 'published' : (prev.status === 'published' ? 'published' : 'pending');
    else status = 'draft';
    // A soft-removed article can't be re-published by its author — only a
    // moderator (canModerateWire ⊇ canManageUsers) may restore it.
    if (prev.status === 'removed' && !isAdmin) status = 'removed';
    const loc = cleanLocation(data);
    const agencies = cleanAgencies(data['agencies']);
    const incident = parseIncident(data);
    const incidentId = incident?.source_id ?? null;
    const incidentJson = incident ? JSON.stringify(incident) : null;
    const license = normaliseLicense(data['license']);
    const credit = typeof data['credit'] === 'string' ? data['credit'].trim().slice(0, 200) || null : null;
    if (data['status'] === 'published' && data['rights_affirmed'] !== true) return c.json({ error: 'you must confirm you own or have the rights to publish this' }, 400);
    const mv = validateMedia(data['media'], ARTICLE);
    if ('error' in mv) return c.json({ error: mv.error }, 400);
    const slug = await uniqueSlug(pool, title, id);
    // First publish stamps published_at; keep the original on re-publish.
    const setPublished = status === 'published' && !prev.published_at ? ', published_at = now()' : '';
    const affirmSet = data['rights_affirmed'] === true ? ', rights_affirmed = true' : '';

    const oldImageIds = await imageIdsFor(pool, 'article', id);
    const oldR2Keys = await r2KeysFor(pool, 'article', id);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `UPDATE articles SET title=$1, slug=$2, excerpt=$3, body=$4, status=$5, location_type=$6, region=$7,
           lat=$8, lng=$9, agencies=$10::jsonb, incident_id=$11, license=$12, credit=$13, incident=$15::jsonb, updated_at=now()${setPublished}${affirmSet} WHERE id=$14`,
        [title, slug, excerpt, body, status, loc.location_type, loc.region, loc.lat, loc.lng, JSON.stringify(agencies), incidentId, license, credit, id, incidentJson],
      );
      await client.query('DELETE FROM wire_media WHERE parent_type=$1 AND parent_id=$2', ['article', id]);
      await insertMediaRows(client, ARTICLE, id, mv.items);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
    await rememberCallsigns(pool, mv.units);
    const keptIds = new Set(mv.items.flatMap((m) => [m.cf_image_id, m.poster_cf_image_id]).filter(Boolean) as string[]);
    for (const old of oldImageIds) if (!keptIds.has(old)) await deleteCfImage(old);
    const keptKeys = new Set(mv.items.flatMap((m) => [m.r2_key, m.poster_r2_key]).filter(Boolean) as string[]);
    for (const old of oldR2Keys) if (!keptKeys.has(old)) await safeDeleteR2(pool, old);
    return c.json({ success: true, slug });
  } catch (err) {
    log.error({ err, id }, 'wire: update article failed');
    return c.json({ error: 'failed to update article' }, 500);
  }
});

wireRouter.delete('/api/wire/articles/:id', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  const uid = currentUserId(c);
  if (!uid) return c.json({ error: 'authentication required' }, 401);
  try {
    const existing = await pool.query<{ author_id: string }>('SELECT author_id FROM articles WHERE id = $1', [id]);
    if (existing.rowCount === 0) return c.json({ error: 'not found' }, 404);
    if (existing.rows[0]!.author_id !== uid && !(await canManageUsers(uid))) return c.json({ error: 'forbidden' }, 403);
    const imgIds = await imageIdsFor(pool, 'article', id);
    const r2keys = await r2KeysFor(pool, 'article', id);
    await pool.query('DELETE FROM wire_media WHERE parent_type=$1 AND parent_id=$2', ['article', id]);
    await pool.query('DELETE FROM articles WHERE id = $1', [id]);
    for (const iid of imgIds) await deleteCfImage(iid);
    for (const k of r2keys) await safeDeleteR2(pool, k);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'wire: delete article failed');
    return c.json({ error: 'failed to delete article' }, 500);
  }
});

wireRouter.post('/api/wire/articles/:id/remove', requireRole(canModerateWire), async (c) => {
  return softRemove(c, ARTICLE);
});

wireRouter.post('/api/wire/articles/:id/view', async (c) => {
  return countView(c, 'article', 'articles');
});

// ===========================================================================
// PENDING REVIEW (pre-moderation queue)
// ===========================================================================

wireRouter.get('/api/wire/pending', requireRole(canModerateWire), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  try {
    const [m, a] = await Promise.all([
      pool.query(`SELECT * FROM media_posts WHERE status='pending' ORDER BY created_at DESC LIMIT 100`),
      pool.query(`SELECT * FROM articles WHERE status='pending' ORDER BY created_at DESC LIMIT 100`),
    ]);
    const mMedia = await fetchMediaFor(pool, 'media_post', m.rows.map((r) => r.id));
    const aMedia = await fetchMediaFor(pool, 'article', a.rows.map((r) => r.id));
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const items: any[] = [
      ...m.rows.map((r) => shapeMediaPost(r, mMedia.get(r.id) ?? [])),
      ...a.rows.map((r) => shapeArticle(r, aMedia.get(r.id) ?? [])),
    ].sort((x, y) => new Date(String(y['created_at'] || 0)).getTime() - new Date(String(x['created_at'] || 0)).getTime());
    return c.json({ items, pendingCount: items.length });
  } catch (err) {
    log.error({ err }, 'wire: pending list failed');
    return c.json({ error: 'failed to list pending' }, 500);
  }
});

wireRouter.post('/api/wire/media/:id/approve', requireRole(canModerateWire), (c) => reviewPost(c, MEDIA, 'approve'));
wireRouter.post('/api/wire/media/:id/reject', requireRole(canModerateWire), (c) => reviewPost(c, MEDIA, 'reject'));
wireRouter.post('/api/wire/articles/:id/approve', requireRole(canModerateWire), (c) => reviewPost(c, ARTICLE, 'approve'));
wireRouter.post('/api/wire/articles/:id/reject', requireRole(canModerateWire), (c) => reviewPost(c, ARTICLE, 'reject'));

// Approval-mode toggle. Read allowed for contributors (so compose can label the
// button correctly); only moderators may change it.
wireRouter.get('/api/wire/settings', requireRole(canFeedMedia), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  return c.json({ approval_required: await wireApprovalRequired(pool) });
});

wireRouter.put('/api/wire/settings', requireRole(canModerateWire), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  try {
    const d = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const val = d['approval_required'] === false ? 'false' : 'true';
    await pool.query(
      `INSERT INTO app_settings (key, value, updated_by, updated_at) VALUES ('wire_approval_required', $1, $2, now())
       ON CONFLICT (key) DO UPDATE SET value = $1, updated_by = $2, updated_at = now()`,
      [val, currentUserId(c) ?? null],
    );
    return c.json({ success: true, approval_required: val !== 'false' });
  } catch (err) {
    log.error({ err }, 'wire: settings update failed');
    return c.json({ error: 'failed to update settings' }, 500);
  }
});

// ===========================================================================
// shared mutating helpers
// ===========================================================================

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function reviewPost(c: any, cfg: EntityCfg, action: 'approve' | 'reject') {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  const note = await noteFromBody(c);
  const newStatus = action === 'approve' ? 'published' : 'rejected';
  // Approving an article stamps published_at on first publish.
  const publishSet = action === 'approve' && cfg.parentType === 'article' ? ', published_at = COALESCE(published_at, now())' : '';
  try {
    const r = await pool.query(
      `UPDATE ${cfg.table} SET status=$1${publishSet}, reviewed_by=$2, reviewed_by_name=$3, reviewed_at=now(), review_note=$4, updated_at=now()
       WHERE id=$5 AND status='pending' RETURNING id`,
      [newStatus, currentUserId(c) ?? null, currentUserName(c), note, id],
    );
    if (r.rowCount === 0) return c.json({ error: 'not found or already reviewed' }, 404);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id, table: cfg.table, action }, 'wire: review failed');
    return c.json({ error: 'failed to review' }, 500);
  }
}

async function imageIdsFor(pool: Pool, parentType: string, parentId: string): Promise<string[]> {
  const r = await pool.query<{ cf_image_id: string | null; poster_cf_image_id: string | null }>(
    'SELECT cf_image_id, poster_cf_image_id FROM wire_media WHERE parent_type=$1 AND parent_id=$2',
    [parentType, parentId],
  );
  const ids: string[] = [];
  for (const row of r.rows) {
    if (row.cf_image_id) ids.push(row.cf_image_id);
    if (row.poster_cf_image_id) ids.push(row.poster_cf_image_id);
  }
  return ids;
}

/** Delete an R2 object only if no wire_media row still references it — dedup can
 * share one object across posts. Call AFTER the owning rows are deleted. */
async function safeDeleteR2(pool: Pool, key: string): Promise<void> {
  if (!key) return;
  const r = await pool.query('SELECT 1 FROM wire_media WHERE r2_key = $1 OR poster_r2_key = $1 LIMIT 1', [key]);
  if (r.rowCount === 0) await deleteR2Object(key);
}

async function r2KeysFor(pool: Pool, parentType: string, parentId: string): Promise<string[]> {
  const r = await pool.query<{ r2_key: string | null; poster_r2_key: string | null }>(
    'SELECT r2_key, poster_r2_key FROM wire_media WHERE parent_type=$1 AND parent_id=$2',
    [parentType, parentId],
  );
  const keys: string[] = [];
  for (const row of r.rows) {
    if (row.r2_key) keys.push(row.r2_key);
    if (row.poster_r2_key) keys.push(row.poster_r2_key);
  }
  return keys;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function softRemove(c: any, cfg: EntityCfg) {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
    const r = await pool.query(
      `UPDATE ${cfg.table} SET status='removed', removed_by=$1, removed_by_name=$2, removed_at=now(), updated_at=now()
       WHERE id=$3 AND status <> 'removed' RETURNING id`,
      [currentUserId(c) ?? null, currentUserName(c), id],
    );
    if (r.rowCount === 0) return c.json({ error: 'not found' }, 404);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id, table: cfg.table }, 'wire: remove failed');
    return c.json({ error: 'failed to remove' }, 500);
  }
}

// ===========================================================================
// TAKEDOWNS (DMCA-style notice-and-takedown)
// ===========================================================================

const TARGET_TABLE: Record<string, string> = { media_post: 'media_posts', article: 'articles' };

// Public intake: anyone may file a notice (key-gated like other public routes,
// no login required — a rights holder isn't a site user).
wireRouter.post('/api/wire/takedown', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  try {
    const d = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const targetType = d['target_type'] === 'article' ? 'article' : d['target_type'] === 'media_post' ? 'media_post' : null;
    const targetId = typeof d['target_id'] === 'string' ? d['target_id'] : '';
    const name = typeof d['reporter_name'] === 'string' ? d['reporter_name'].trim().slice(0, 200) : '';
    const email = typeof d['reporter_email'] === 'string' ? d['reporter_email'].trim().slice(0, 200) : '';
    const complaint = typeof d['complaint'] === 'string' ? d['complaint'].trim().slice(0, 5000) : '';
    const org = typeof d['reporter_org'] === 'string' ? d['reporter_org'].trim().slice(0, 200) || null : null;
    let originalUrl = typeof d['original_url'] === 'string' ? d['original_url'].trim().slice(0, 500) || null : null;
    // Only http(s) — the staff review UI renders this as a clickable link, so a
    // javascript:/data: scheme (survives HTML-escaping) must never be stored.
    if (originalUrl && !/^https?:\/\//i.test(originalUrl)) originalUrl = null;
    if (!targetType || !targetId) return c.json({ error: 'target_type and target_id are required' }, 400);
    if (!name || !email || !complaint) return c.json({ error: 'name, email and a description are required' }, 400);
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return c.json({ error: 'a valid email is required' }, 400);
    // The two sworn statements a valid notice must carry.
    if (d['good_faith'] !== true || d['accuracy'] !== true) {
      return c.json({ error: 'both good-faith and accuracy statements must be confirmed' }, 400);
    }
    // Denormalise the target title for the review queue (best-effort).
    let title: string | null = null;
    try {
      const t = await pool.query<{ title: string }>(`SELECT title FROM ${TARGET_TABLE[targetType]} WHERE id = $1`, [targetId]);
      title = t.rows[0]?.title ?? null;
    } catch { /* leave null */ }
    await pool.query(
      `INSERT INTO wire_takedowns (target_type, target_id, target_title, reporter_name, reporter_email, reporter_org, complaint, original_url, good_faith, accuracy)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true,true)`,
      [targetType, targetId, title, name, email, org, complaint, originalUrl],
    );
    log.warn({ targetType, targetId }, 'wire: takedown notice filed');
    return c.json({ success: true });
  } catch (err) {
    log.error({ err }, 'wire: takedown intake failed');
    return c.json({ error: 'failed to file notice' }, 500);
  }
});

wireRouter.get('/api/wire/takedowns', requireRole(canModerateWire), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  try {
    const status = new URL(c.req.url).searchParams.get('status') || 'pending';
    const r = await pool.query(
      `SELECT * FROM wire_takedowns WHERE status = $1 ORDER BY created_at DESC LIMIT 200`, [status]);
    const pc = await pool.query<{ n: string }>(`SELECT COUNT(*) AS n FROM wire_takedowns WHERE status = 'pending'`);
    const takedowns = r.rows.map((row) => ({
      ...row,
      created_at: isoOrNull(row.created_at),
      reviewed_at: isoOrNull(row.reviewed_at),
    }));
    return c.json({ takedowns, pendingCount: Number(pc.rows[0]?.n ?? 0) });
  } catch (err) {
    log.error({ err }, 'wire: list takedowns failed');
    return c.json({ error: 'failed to list takedowns' }, 500);
  }
});

wireRouter.post('/api/wire/takedowns/:id/uphold', requireRole(canModerateWire), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  const note = await noteFromBody(c);
  try {
    const t = await pool.query<{ target_type: string; target_id: string; status: string }>(
      'SELECT target_type, target_id, status FROM wire_takedowns WHERE id = $1', [id]);
    if (t.rowCount === 0) return c.json({ error: 'not found' }, 404);
    const row = t.rows[0]!;
    if (row.status !== 'pending') return c.json({ error: 'already reviewed' }, 409);
    const table = TARGET_TABLE[row.target_type];
    if (!table) return c.json({ error: 'bad target' }, 400);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // Soft-remove + tombstone the target (retained for the audit trail). Claim
      // the notice atomically so a double-submit can't re-remove / re-stamp.
      const claim = await client.query(
        `UPDATE wire_takedowns SET status='upheld', action_note=$1, reviewed_by=$2, reviewed_by_name=$3, reviewed_at=now()
         WHERE id=$4 AND status='pending' RETURNING id`,
        [note, currentUserId(c) ?? null, currentUserName(c), id],
      );
      if (claim.rowCount === 0) { await client.query('ROLLBACK'); return c.json({ error: 'already reviewed' }, 409); }
      await client.query(
        `UPDATE ${table} SET status='removed', taken_down_at=now(), removed_by=$1, removed_by_name=$2, removed_at=now(), updated_at=now() WHERE id=$3`,
        [currentUserId(c) ?? null, currentUserName(c), row.target_id],
      );
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
    // DMCA purge: delete the target's media rows and their R2/Cloudflare objects
    // so the infringing bytes are actually gone (and can't be re-linked via the
    // hash-dedup path). Best-effort — the tombstone stands regardless.
    try {
      const imgIds = await imageIdsFor(pool, row.target_type, row.target_id);
      const r2keys = await r2KeysFor(pool, row.target_type, row.target_id);
      await pool.query('DELETE FROM wire_media WHERE parent_type=$1 AND parent_id=$2', [row.target_type, row.target_id]);
      for (const iid of imgIds) await deleteCfImage(iid);
      for (const k of r2keys) await safeDeleteR2(pool, k);
    } catch (e) {
      log.warn({ e, id }, 'wire: takedown media purge failed (bytes may remain)');
    }
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'wire: uphold takedown failed');
    return c.json({ error: 'failed to uphold' }, 500);
  }
});

wireRouter.post('/api/wire/takedowns/:id/reject', requireRole(canModerateWire), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  const note = await noteFromBody(c);
  try {
    const r = await pool.query(
      `UPDATE wire_takedowns SET status='rejected', action_note=$1, reviewed_by=$2, reviewed_by_name=$3, reviewed_at=now()
       WHERE id=$4 AND status='pending' RETURNING id`,
      [note, currentUserId(c) ?? null, currentUserName(c), id],
    );
    if (r.rowCount === 0) return c.json({ error: 'not found or already reviewed' }, 404);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'wire: reject takedown failed');
    return c.json({ error: 'failed to reject' }, 500);
  }
});

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function noteFromBody(c: any): Promise<string | null> {
  const d = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
  return typeof d['note'] === 'string' ? d['note'].trim().slice(0, 2000) || null : null;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function countView(c: any, parentType: string, table: string) {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
    // The author's own views don't count — only other users. When a logged-in
    // user hits this, skip counting if they're the author.
    const uid = currentUserId(c);
    if (uid) {
      const a = await pool.query<{ author_id: string; views: string }>(`SELECT author_id, views FROM ${table} WHERE id = $1`, [id]);
      if (a.rowCount === 0) return c.json({ error: 'not found' }, 404);
      if (a.rows[0]!.author_id === uid) return c.json({ views: Number(a.rows[0]!.views) || 0, self: true });
    }
    const hash = viewerHash(clientIp(c), c.req.header('user-agent') || '');
    const ins = await pool.query(
      `INSERT INTO wire_views (parent_type, parent_id, viewer_hash, day)
       VALUES ($1,$2,$3, CURRENT_DATE) ON CONFLICT DO NOTHING RETURNING parent_id`,
      [parentType, id, hash],
    );
    if ((ins.rowCount ?? 0) > 0) {
      const upd = await pool.query<{ views: string }>(
        `UPDATE ${table} SET views = views + 1 WHERE id = $1 RETURNING views`, [id]);
      return c.json({ views: Number(upd.rows[0]?.views ?? 0) });
    }
    const cur = await pool.query<{ views: string }>(`SELECT views FROM ${table} WHERE id = $1`, [id]);
    return c.json({ views: Number(cur.rows[0]?.views ?? 0), deduped: true });
  } catch (err) {
    log.error({ err, id, parentType }, 'wire: view count failed');
    return c.json({ error: 'failed to record view' }, 500);
  }
}
