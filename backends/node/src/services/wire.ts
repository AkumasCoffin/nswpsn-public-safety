/**
 * "The Wire" service helpers: Cloudflare Images direct-upload plumbing,
 * media-URL assembly, slug + view-hash utilities. The CRUD SQL lives in
 * api/wire.ts (mirroring how api/incidents.ts keeps its queries inline);
 * this module owns the external-storage + pure-function pieces so they're
 * unit-testable and reusable.
 */
import { fetch, FormData } from 'undici';
import { createHash } from 'node:crypto';
import { config } from '../config.js';
import { log } from '../lib/log.js';

// ---- Cloudflare Images -----------------------------------------------------

export function cfImagesConfigured(): boolean {
  return Boolean(
    config.CF_IMAGES_ACCOUNT_ID && config.CF_IMAGES_API_TOKEN && config.CF_IMAGES_HASH,
  );
}

interface DirectUpload {
  id: string;
  uploadURL: string;
}

/**
 * Mint a one-time Cloudflare Images direct-upload URL. The browser POSTs the
 * file straight to `uploadURL` (multipart, field name `file`) — the bytes
 * never touch this origin, which keeps the 50Mbps uplink and the hono
 * bodyLimit RAM gotcha out of the picture. We keep the returned `id` and later
 * serve named variants of it. Returns null when Images isn't configured or the
 * CF API errors (caller maps that to a 503).
 */
export async function createImageDirectUploadUrl(meta?: Record<string, string>): Promise<DirectUpload | null> {
  if (!cfImagesConfigured()) return null;
  const url = `https://api.cloudflare.com/client/v4/accounts/${config.CF_IMAGES_ACCOUNT_ID}/images/v2/direct_upload`;
  try {
    const form = new FormData();
    // Serve only named variants (which strip EXIF/GPS); never expose the
    // metadata-bearing original.
    form.set('requireSignedURLs', 'false');
    if (meta) form.set('metadata', JSON.stringify(meta));
    const res = await fetch(url, {
      method: 'POST',
      headers: { Authorization: `Bearer ${config.CF_IMAGES_API_TOKEN}` },
      body: form,
    });
    const json = (await res.json()) as {
      success?: boolean;
      result?: { id?: string; uploadURL?: string };
      errors?: unknown;
    };
    if (!res.ok || !json.success || !json.result?.id || !json.result?.uploadURL) {
      log.warn({ status: res.status, errors: json.errors }, 'wire: CF direct_upload failed');
      return null;
    }
    return { id: json.result.id, uploadURL: json.result.uploadURL };
  } catch (err) {
    log.warn({ err }, 'wire: CF direct_upload request threw');
    return null;
  }
}

/**
 * Best-effort delete of a Cloudflare image (called when a post/asset is
 * removed so orphaned images don't accrue storage cost). Never throws.
 */
export async function deleteCfImage(id: string): Promise<void> {
  if (!id || !cfImagesConfigured()) return;
  const url = `https://api.cloudflare.com/client/v4/accounts/${config.CF_IMAGES_ACCOUNT_ID}/images/v1/${encodeURIComponent(id)}`;
  try {
    await fetch(url, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${config.CF_IMAGES_API_TOKEN}` },
    });
  } catch (err) {
    log.warn({ err, id }, 'wire: CF image delete failed (orphan left)');
  }
}

/**
 * Delivery URL for a named Cloudflare Images variant. `public` always exists;
 * create `thumb` / `feed` variants in the CF dashboard to get smaller cuts.
 */
export function imageVariantUrl(id: string, variant = 'public'): string {
  return `https://imagedelivery.net/${config.CF_IMAGES_HASH}/${id}/${variant}`;
}

// ---- R2 video (phase 1.5) --------------------------------------------------

export function r2Configured(): boolean {
  return Boolean(
    config.R2_ACCESS_KEY_ID &&
      config.R2_SECRET_ACCESS_KEY &&
      config.R2_BUCKET &&
      config.R2_ENDPOINT &&
      config.R2_PUBLIC_BASE,
  );
}

/** Public URL for a stored R2 object key. */
export function r2PublicUrl(key: string): string {
  const base = (config.R2_PUBLIC_BASE ?? '').replace(/\/$/, '');
  return `${base}/${key.split('/').map(encodeURIComponent).join('/')}`;
}

// ---- Pure helpers ----------------------------------------------------------

/** URL-safe slug base from a title. */
export function slugify(title: string): string {
  return (title || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[̀-ͯ]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80) || 'article';
}

/** Stable per-day per-viewer hash for view de-duplication. */
export function viewerHash(ip: string, userAgent: string): string {
  return createHash('sha256')
    .update(`${ip}|${userAgent}|${config.VIEW_HASH_SALT}`)
    .digest('hex')
    .slice(0, 32);
}

// ---- Media row -> response shaping -----------------------------------------

export interface WireMediaRow {
  id: string;
  parent_type: string;
  parent_id: string;
  kind: string;
  cf_image_id: string | null;
  r2_key: string | null;
  poster_cf_image_id: string | null;
  duration_seconds: number | null;
  is_cover: boolean;
  unit: string | null;
  sort_order: number;
  width: number | null;
  height: number | null;
  bytes: number | null;
}

/** Attach delivery URLs so the frontend never constructs storage URLs itself. */
export function shapeMedia(row: WireMediaRow): Record<string, unknown> {
  const out: Record<string, unknown> = {
    id: row.id,
    kind: row.kind,
    is_cover: row.is_cover,
    unit: row.unit,
    sort_order: row.sort_order,
    width: row.width,
    height: row.height,
    duration_seconds: row.duration_seconds,
    // Storage ids are already derivable from the delivery URL below, so
    // exposing them is not a leak — and it lets the compose editor round-trip
    // existing media on edit without re-uploading.
    cf_image_id: row.cf_image_id,
    r2_key: row.r2_key,
    poster_cf_image_id: row.poster_cf_image_id,
  };
  if (row.kind === 'image' && row.cf_image_id) {
    out['thumb_url'] = imageVariantUrl(row.cf_image_id, 'thumb');
    out['feed_url'] = imageVariantUrl(row.cf_image_id, 'feed');
    out['url'] = imageVariantUrl(row.cf_image_id, 'public');
  } else if (row.kind === 'video' && row.r2_key) {
    out['url'] = r2PublicUrl(row.r2_key);
    if (row.poster_cf_image_id) out['poster_url'] = imageVariantUrl(row.poster_cf_image_id, 'public');
  }
  return out;
}
