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
import {
  requireRole,
  canFeedMedia,
  canModerateWire,
  canManageUsers,
} from '../services/auth/roles.js';
import { rememberCallsigns, normaliseCallsign } from '../services/callsigns.js';
import {
  createImageDirectUploadUrl,
  cfImagesConfigured,
  deleteCfImage,
  imageVariantUrl,
  r2Configured,
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

interface MediaItem {
  kind: 'image' | 'video';
  cf_image_id: string | null;
  r2_key: string | null;
  poster_cf_image_id: string | null;
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
      duration_seconds: int(o['duration_seconds']),
      is_cover: o['is_cover'] === true,
      unit: normaliseCallsign(o['unit']) || null,
      width: int(o['width']),
      height: int(o['height']),
      bytes: int(o['bytes']),
    };
    if (kind === 'image') {
      if (!item.cf_image_id) return { error: 'each image needs a cf_image_id' };
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
         (parent_type, parent_id, kind, cf_image_id, r2_key, poster_cf_image_id,
          duration_seconds, is_cover, unit, sort_order, width, height, bytes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
      [cfg.parentType, parentId, m.kind, m.cf_image_id, m.r2_key, m.poster_cf_image_id,
        m.duration_seconds, m.is_cover, m.unit, i, m.width, m.height, m.bytes],
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
    units: deriveUnits(media),
    views: Number(row.views) || 0,
    status: row.status,
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
    units: deriveUnits(media),
    views: Number(row.views) || 0,
    status: row.status,
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
  if (!cfImagesConfigured()) return c.json({ error: 'image uploads not configured' }, 503);
  const up = await createImageDirectUploadUrl({ feature: 'wire', by: currentUserId(c) ?? '' });
  if (!up) return c.json({ error: 'could not create upload url' }, 503);
  // deliveryUrl lets compose show an instant preview before the post is saved
  // (the CF direct_upload response itself carries no delivery URL).
  return c.json({
    id: up.id,
    uploadURL: up.uploadURL,
    deliveryUrl: imageVariantUrl(up.id, 'public'),
    thumbUrl: imageVariantUrl(up.id, 'thumb'),
  });
});

wireRouter.post('/api/wire/video-upload-url', requireRole(canFeedMedia), async (c) => {
  // Phase 1.5: R2 presigned PUT. Until R2 + the aws-sdk wiring land, report
  // unavailable so the compose UI can hide/disable the video slots.
  if (!r2Configured()) return c.json({ error: 'video uploads not yet available' }, 503);
  return c.json({ error: 'video uploads not yet available' }, 503);
});

// ===========================================================================
// MEDIA POSTS
// ===========================================================================

wireRouter.get('/api/wire/media', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  try {
    const url = new URL(c.req.url);
    const mine = url.searchParams.get('mine') === '1';
    const uid = currentUserId(c);
    const agency = url.searchParams.get('agency');
    const unit = normaliseCallsign(url.searchParams.get('unit') || '') || null;
    const region = url.searchParams.get('region');
    const limit = Math.max(1, Math.min(60, Number(url.searchParams.get('limit') ?? 24) || 24));
    const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

    const vals: unknown[] = [];
    const where: string[] = [];
    if (mine && uid) { vals.push(uid); where.push(`author_id = $${vals.length}`); }
    else { where.push(`status = 'published'`); }
    if (agency) { vals.push(JSON.stringify([agency])); where.push(`agencies @> $${vals.length}::jsonb`); }
    if (region) { vals.push(region); where.push(`region = $${vals.length}`); }
    vals.push(limit, offset);
    const r = await pool.query(
      `SELECT * FROM media_posts WHERE ${where.join(' AND ')}
        ORDER BY created_at DESC LIMIT $${vals.length - 1} OFFSET $${vals.length}`,
      vals,
    );
    const ids = r.rows.map((row) => row.id);
    const mediaMap = await fetchMediaFor(pool, 'media_post', ids);
    let posts = r.rows.map((row) => shapeMediaPost(row, mediaMap.get(row.id) ?? []));
    if (unit) posts = posts.filter((p) => (p['units'] as string[]).includes(unit));
    return c.json({ posts });
  } catch (err) {
    log.error({ err }, 'wire: list media failed');
    return c.json({ error: 'failed to list media posts' }, 500);
  }
});

wireRouter.get('/api/wire/media/:id', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
    const r = await pool.query('SELECT * FROM media_posts WHERE id = $1', [id]);
    if (r.rowCount === 0) return c.json({ error: 'not found' }, 404);
    const row = r.rows[0];
    const uid = currentUserId(c);
    const isAuthor = uid && row.author_id === uid;
    if (row.status !== 'published' && !isAuthor && !(uid && (await canManageUsers(uid)))) {
      return c.json({ error: 'not found' }, 404);
    }
    const mediaMap = await fetchMediaFor(pool, 'media_post', [id]);
    return c.json({ post: shapeMediaPost(row, mediaMap.get(id) ?? []) });
  } catch (err) {
    log.error({ err, id }, 'wire: get media failed');
    return c.json({ error: 'failed to fetch media post' }, 500);
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
    const incidentId = typeof data['incident_id'] === 'string' && data['incident_id'] ? data['incident_id'] : null;
    const mv = validateMedia(data['media'], MEDIA);
    if ('error' in mv) return c.json({ error: mv.error }, 400);

    const authorId = currentUserId(c)!;
    const authorName = currentUserName(c);
    const client = await pool.connect();
    let newId: string;
    try {
      await client.query('BEGIN');
      const ins = await client.query<{ id: string }>(
        `INSERT INTO media_posts (author_id, author_name, title, caption, location_type, region, lat, lng, agencies, incident_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10) RETURNING id`,
        [authorId, authorName, title, caption, loc.location_type, loc.region, loc.lat, loc.lng, JSON.stringify(agencies), incidentId],
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
    return c.json({ id: newId, success: true }, 201);
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
    const existing = await pool.query<{ author_id: string }>('SELECT author_id FROM media_posts WHERE id = $1', [id]);
    if (existing.rowCount === 0) return c.json({ error: 'not found' }, 404);
    if (existing.rows[0]!.author_id !== uid && !(await canManageUsers(uid))) {
      return c.json({ error: 'forbidden' }, 403);
    }
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const title = typeof data['title'] === 'string' ? data['title'].trim().slice(0, 300) : '';
    if (!title) return c.json({ error: 'title is required' }, 400);
    const caption = typeof data['caption'] === 'string' ? data['caption'].slice(0, 4000) : '';
    const loc = cleanLocation(data);
    const agencies = cleanAgencies(data['agencies']);
    const incidentId = typeof data['incident_id'] === 'string' && data['incident_id'] ? data['incident_id'] : null;
    const mv = validateMedia(data['media'], MEDIA);
    if ('error' in mv) return c.json({ error: mv.error }, 400);

    const oldImageIds = await imageIdsFor(pool, 'media_post', id);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `UPDATE media_posts SET title=$1, caption=$2, location_type=$3, region=$4, lat=$5, lng=$6,
           agencies=$7::jsonb, incident_id=$8, updated_at=now() WHERE id=$9`,
        [title, caption, loc.location_type, loc.region, loc.lat, loc.lng, JSON.stringify(agencies), incidentId, id],
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
    const keptIds = new Set(mv.items.map((m) => m.cf_image_id).filter(Boolean) as string[]);
    for (const old of oldImageIds) if (!keptIds.has(old)) await deleteCfImage(old);
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
    await pool.query('DELETE FROM wire_media WHERE parent_type=$1 AND parent_id=$2', ['media_post', id]);
    await pool.query('DELETE FROM media_posts WHERE id = $1', [id]);
    for (const iid of imgIds) await deleteCfImage(iid);
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
  try {
    const url = new URL(c.req.url);
    const mine = url.searchParams.get('mine') === '1';
    const uid = currentUserId(c);
    const agency = url.searchParams.get('agency');
    const region = url.searchParams.get('region');
    const limit = Math.max(1, Math.min(60, Number(url.searchParams.get('limit') ?? 24) || 24));
    const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

    const vals: unknown[] = [];
    const where: string[] = [];
    if (mine && uid) { vals.push(uid); where.push(`author_id = $${vals.length}`); }
    else { where.push(`status = 'published'`); }
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
  const slug = c.req.param('slug');
  try {
    const r = await pool.query('SELECT * FROM articles WHERE slug = $1', [slug]);
    if (r.rowCount === 0) return c.json({ error: 'not found' }, 404);
    const row = r.rows[0];
    const uid = currentUserId(c);
    const isAuthor = uid && row.author_id === uid;
    if (row.status !== 'published' && !isAuthor && !(uid && (await canManageUsers(uid)))) {
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
    const status = data['status'] === 'published' ? 'published' : 'draft';
    const loc = cleanLocation(data);
    const agencies = cleanAgencies(data['agencies']);
    const incidentId = typeof data['incident_id'] === 'string' && data['incident_id'] ? data['incident_id'] : null;
    const mv = validateMedia(data['media'], ARTICLE);
    if ('error' in mv) return c.json({ error: mv.error }, 400);
    const slug = await uniqueSlug(pool, title);

    const authorId = currentUserId(c)!;
    const authorName = currentUserName(c);
    const client = await pool.connect();
    let newId: string;
    try {
      await client.query('BEGIN');
      const ins = await client.query<{ id: string }>(
        `INSERT INTO articles (author_id, author_name, title, slug, excerpt, body, status, published_at,
           location_type, region, lat, lng, agencies, incident_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,${status === 'published' ? 'now()' : 'NULL'},$8,$9,$10,$11,$12::jsonb,$13) RETURNING id`,
        [authorId, authorName, title, slug, excerpt, body, status, loc.location_type, loc.region, loc.lat, loc.lng, JSON.stringify(agencies), incidentId],
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
    return c.json({ id: newId, slug, success: true }, 201);
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
    const existing = await pool.query<{ author_id: string; status: string; published_at: unknown }>(
      'SELECT author_id, status, published_at FROM articles WHERE id = $1', [id]);
    if (existing.rowCount === 0) return c.json({ error: 'not found' }, 404);
    const prev = existing.rows[0]!;
    if (prev.author_id !== uid && !(await canManageUsers(uid))) return c.json({ error: 'forbidden' }, 403);
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const title = typeof data['title'] === 'string' ? data['title'].trim().slice(0, 300) : '';
    if (!title) return c.json({ error: 'title is required' }, 400);
    const excerpt = typeof data['excerpt'] === 'string' ? data['excerpt'].slice(0, 600) : '';
    const body = typeof data['body'] === 'string' ? data['body'].slice(0, 100_000) : '';
    const status = data['status'] === 'published' ? 'published' : (prev.status === 'removed' ? 'removed' : 'draft');
    const loc = cleanLocation(data);
    const agencies = cleanAgencies(data['agencies']);
    const incidentId = typeof data['incident_id'] === 'string' && data['incident_id'] ? data['incident_id'] : null;
    const mv = validateMedia(data['media'], ARTICLE);
    if ('error' in mv) return c.json({ error: mv.error }, 400);
    const slug = await uniqueSlug(pool, title, id);
    // First publish stamps published_at; keep the original on re-publish.
    const setPublished = status === 'published' && !prev.published_at ? ', published_at = now()' : '';

    const oldImageIds = await imageIdsFor(pool, 'article', id);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `UPDATE articles SET title=$1, slug=$2, excerpt=$3, body=$4, status=$5, location_type=$6, region=$7,
           lat=$8, lng=$9, agencies=$10::jsonb, incident_id=$11, updated_at=now()${setPublished} WHERE id=$12`,
        [title, slug, excerpt, body, status, loc.location_type, loc.region, loc.lat, loc.lng, JSON.stringify(agencies), incidentId, id],
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
    const keptIds = new Set(mv.items.map((m) => m.cf_image_id).filter(Boolean) as string[]);
    for (const old of oldImageIds) if (!keptIds.has(old)) await deleteCfImage(old);
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
    await pool.query('DELETE FROM wire_media WHERE parent_type=$1 AND parent_id=$2', ['article', id]);
    await pool.query('DELETE FROM articles WHERE id = $1', [id]);
    for (const iid of imgIds) await deleteCfImage(iid);
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
// shared mutating helpers
// ===========================================================================

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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function countView(c: any, parentType: string, table: string) {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
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
