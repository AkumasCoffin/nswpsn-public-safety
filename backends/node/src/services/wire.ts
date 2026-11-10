/**
 * "The Wire" service helpers: Cloudflare Images direct-upload plumbing,
 * media-URL assembly, slug + view-hash utilities. The CRUD SQL lives in
 * api/wire.ts (mirroring how api/incidents.ts keeps its queries inline);
 * this module owns the external-storage + pure-function pieces so they're
 * unit-testable and reusable.
 */
import { fetch, FormData } from 'undici';
import { createHash, randomUUID } from 'node:crypto';
import type { Pool } from 'pg';
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

// og:image: serve the plain stored image URL. It's a clean https URL (no query
// params, no /cdn-cgi/ path) that returns 200 image/webp to every fetcher, and
// Discord / Facebook / X all support WebP. Earlier attempts used a Cloudflare
// image-transform URL to hand crawlers a JPEG, but the comma-laden /cdn-cgi/
// URL (and its nested-https full-URL form) tripped up Discord's image proxy —
// the plain URL avoids that entirely.
//
// So this strips a transform prefix rather than assuming there isn't one:
// shapeMedia hands out /cdn-cgi/ URLs for video posters (and could for more
// later), and a crawler must never be given one. Undoing it here keeps that
// guarantee in the one place that actually depends on it.
export function ogImageUrl(rawUrl: string | null): string | null {
  if (!rawUrl) return null;
  return rawUrl.replace(/\/cdn-cgi\/image\/[^/]+\//, '/');
}

// ---- R2 video --------------------------------------------------------------

export const MAX_VIDEO_BYTES = 50 * 1024 * 1024; // 50 MB per clip

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

/**
 * Cloudflare Image Transformations URL for an R2 object.
 *
 * Everything in R2 is stored ONCE at full size; the CDN cuts the smaller
 * variants on demand at /cdn-cgi/image/<opts>/<key>. Same mechanism the map
 * already uses for incident photos (services/incidentImages.ts), just pointed
 * at the media host instead of the webroot.
 *
 * Worth being precise about the failure mode: if Transformations were ever
 * turned off for the zone, the resizer path 404s rather than serving the
 * original — hence CF_TRANSFORMS_DISABLED, which puts every consumer straight
 * back on the untransformed URL.
 *
 * `format=auto` is where most of the saving comes from: the stored file is
 * WebP, but a browser that accepts AVIF gets AVIF.
 */
export function r2TransformUrl(
  key: string,
  opts: { width: number; quality?: number },
): string {
  const base = (config.R2_PUBLIC_BASE ?? '').replace(/\/$/, '');
  const path = key.split('/').map(encodeURIComponent).join('/');
  if (config.CF_TRANSFORMS_DISABLED) return `${base}/${path}`;
  // fit=scale-down is the default, but state it: a small source must never be
  // upscaled into a LARGER file than the original.
  const params = `width=${opts.width},quality=${opts.quality ?? 82},format=auto,fit=scale-down`;
  return `${base}/cdn-cgi/image/${params}/${path}`;
}

/**
 * Rendered widths, doubled-ish for retina. Kept here rather than at each call
 * site so the set of distinct transformations stays small — Cloudflare bills
 * per UNIQUE (url + options) pair, so every extra width is a new billable
 * variant of every image.
 */
export const IMG_WIDTHS = {
  /** 76px gallery strip + staff list thumbs. */
  thumb: 160,
  /** ~270-400px feed/profile cards. */
  feed: 640,
  /** ~28px avatar chips beside usernames and in pickers. */
  avatar: 80,
  /** 116px profile-page header portrait. */
  avatarLarge: 256,
  /** Video poster behind the detail player. */
  poster: 1280,
} as const;

/**
 * Avatar URL for a stored R2 key, sized for where it's rendered — or a Discord
 * CDN URL passed straight through, since only objects on our own zone can be
 * transformed.
 */
export function avatarUrl(
  avatarKey: string | null | undefined,
  discordUrl: string | null | undefined,
  size: 'chip' | 'large' = 'chip',
): string | null {
  if (avatarKey) {
    const width = size === 'large' ? IMG_WIDTHS.avatarLarge : IMG_WIDTHS.avatar;
    return r2TransformUrl(avatarKey, { width, quality: 85 });
  }
  return discordUrl ?? null;
}

// The S3 SDK is heavy and only needed when video is actually used, so it's
// dynamically imported (same lazy pattern as pg in db/pool.ts).
async function r2Client() {
  const { S3Client } = await import('@aws-sdk/client-s3');
  return new S3Client({
    region: 'auto',
    endpoint: config.R2_ENDPOINT,
    forcePathStyle: true,
    credentials: {
      accessKeyId: config.R2_ACCESS_KEY_ID as string,
      secretAccessKey: config.R2_SECRET_ACCESS_KEY as string,
    },
  });
}

/**
 * Presign a browser->R2 PUT for an optimised image (WebP). Content-Type is NOT
 * signed, so the browser's blob type (image/webp, or image/jpeg on old
 * browsers) is accepted and stored as-is. The browser downscales + re-encodes
 * before upload, which also strips EXIF/GPS. Returns null when R2 isn't
 * configured or signing fails.
 */
export async function createImageUploadUrl(prefix = 'wire/img', ext = 'webp'): Promise<{ uploadURL: string; key: string; publicUrl: string } | null> {
  if (!r2Configured()) return null;
  try {
    const { PutObjectCommand } = await import('@aws-sdk/client-s3');
    const { getSignedUrl } = await import('@aws-sdk/s3-request-presigner');
    const s3 = await r2Client();
    const key = `${prefix.replace(/\/+$/, '')}/${randomUUID()}.${ext}`;
    const uploadURL = await getSignedUrl(s3, new PutObjectCommand({ Bucket: config.R2_BUCKET as string, Key: key }), { expiresIn: 600 });
    return { uploadURL, key, publicUrl: r2PublicUrl(key) };
  } catch (err) {
    log.warn({ err }, 'wire: R2 image presign failed');
    return null;
  }
}

/**
 * Presign a browser->R2 PUT for a video (<=50MB MP4). The browser PUTs the file
 * straight to R2 with `Content-Type: video/mp4` (which is signed, so it must
 * match) — the bytes never touch this origin, bypassing the 50Mbps uplink.
 * Returns null when R2 isn't configured or signing fails.
 */
export async function createVideoUploadUrl(contentLength?: number): Promise<{ uploadURL: string; key: string; publicUrl: string } | null> {
  if (!r2Configured()) return null;
  try {
    const { PutObjectCommand } = await import('@aws-sdk/client-s3');
    const { getSignedUrl } = await import('@aws-sdk/s3-request-presigner');
    const s3 = await r2Client();
    const key = `wire/videos/${randomUUID()}.mp4`;
    // Sign the exact Content-Length so the browser can't upload more than the
    // declared (server-capped) size — the PUT's Content-Length must match.
    const cmd = new PutObjectCommand({
      Bucket: config.R2_BUCKET as string, Key: key, ContentType: 'video/mp4',
      ...(contentLength && contentLength > 0 ? { ContentLength: contentLength } : {}),
    });
    const uploadURL = await getSignedUrl(s3, cmd, { expiresIn: 600, signableHeaders: contentLength ? new Set(['content-length', 'content-type', 'host']) : undefined });
    return { uploadURL, key, publicUrl: r2PublicUrl(key) };
  } catch (err) {
    log.warn({ err }, 'wire: R2 presign failed');
    return null;
  }
}

/**
 * Read a SMALL R2 object fully into memory (watermark PNGs). Capped so a
 * mis-uploaded huge file can't balloon the API's RSS -- anything over the
 * cap answers null and the caller 404s.
 */
export async function readR2ObjectBytes(key: string, maxBytes = 2_000_000): Promise<Buffer | null> {
  if (!key || !r2Configured()) return null;
  try {
    const { GetObjectCommand } = await import('@aws-sdk/client-s3');
    const s3 = await r2Client();
    const res = await s3.send(new GetObjectCommand({ Bucket: config.R2_BUCKET as string, Key: key }));
    if (res.ContentLength && res.ContentLength > maxBytes) return null;
    const body = res.Body as { transformToByteArray?: () => Promise<Uint8Array> } | undefined;
    if (!body?.transformToByteArray) return null;
    const bytes = await body.transformToByteArray();
    if (bytes.byteLength > maxBytes) return null;
    return Buffer.from(bytes);
  } catch (err) {
    log.warn({ err, key }, 'wire: R2 object read failed');
    return null;
  }
}

/** Best-effort delete of a stored R2 video (called when a post/asset is
 * removed so orphaned objects don't accrue storage). Never throws. */
export async function deleteR2Object(key: string): Promise<void> {
  if (!key || !r2Configured()) return;
  try {
    const { DeleteObjectCommand } = await import('@aws-sdk/client-s3');
    const s3 = await r2Client();
    await s3.send(new DeleteObjectCommand({ Bucket: config.R2_BUCKET as string, Key: key }));
  } catch (err) {
    log.warn({ err, key }, 'wire: R2 delete failed (orphan left)');
  }
}

// ---- Key ownership ---------------------------------------------------------

/** The prefixes our own presigners mint under. A key the caller is adding for
 *  the first time has to be one of ours — not an avatar, not a watermark, not
 *  an arbitrary path in the shared bucket. */
export const R2_IMAGE_PREFIX = 'wire/img/';
export const R2_VIDEO_PREFIX = 'wire/videos/';

/**
 * Which of `keys` something still points at.
 *
 * Object keys reach us from the client — the composer sends back the key it
 * uploaded to, and an edit re-sends the keys the post already has — and the key
 * itself says nothing about who uploaded it. Both questions that matters for
 * are answered by what references it:
 *
 *  - "may this caller save this key?" — pass their id as `exceptOwnerId`, and
 *    anything that comes back is someone else's object;
 *  - "is this object safe to delete?" — pass null, and anything that comes back
 *    is still in use by a row that would be left pointing at nothing.
 *
 * Every place a key is stored is covered: article media (whose owner is the
 * parent article's author), fleet photos, and profile avatars + watermarks. A
 * wire_media row whose parent article has gone counts as foreign — a key we
 * can't attribute is not a key that's free.
 */
export async function r2KeysInUse(
  pool: Pool,
  keys: readonly string[],
  exceptOwnerId: string | null = null,
): Promise<string[]> {
  const wanted = [...new Set(keys.filter((k) => !!k))];
  if (wanted.length === 0) return [];
  const r = await pool.query<{ key: string }>(
    `WITH k(key) AS (SELECT unnest($1::text[]))
     SELECT DISTINCT k.key FROM k
      WHERE EXISTS (SELECT 1 FROM wire_media wm
                      LEFT JOIN articles a ON a.id = wm.parent_id
                     WHERE (wm.r2_key = k.key OR wm.poster_r2_key = k.key)
                       AND ($2::text IS NULL OR a.author_id IS NULL OR a.author_id <> $2))
         OR EXISTS (SELECT 1 FROM fleet_vehicles fv
                     WHERE fv.images @> jsonb_build_array(jsonb_build_object('key', k.key))
                       AND ($2::text IS NULL OR fv.author_id <> $2))
         OR EXISTS (SELECT 1 FROM user_profiles up
                     WHERE (up.avatar_key = k.key OR up.watermark_key = k.key)
                       AND ($2::text IS NULL OR up.user_id <> $2))`,
    [wanted, exceptOwnerId],
  );
  return r.rows.map((row) => row.key);
}

/** Delete a stored object only once nothing points at it any more. Call AFTER
 *  the rows that referenced it are written, so this sees the new state. A
 *  failed check leaves the object in place: an orphan costs storage, deleting
 *  something still in use costs someone their media. */
export async function deleteUnreferencedR2Object(pool: Pool, key: string): Promise<void> {
  if (!key) return;
  try {
    if ((await r2KeysInUse(pool, [key])).length > 0) return;
  } catch (err) {
    log.warn({ err, key }, 'wire: R2 reference check failed (object kept)');
    return;
  }
  await deleteR2Object(key);
}

/**
 * Stream an R2 object to a local file.
 *
 * Streamed, never buffered — a 50MB clip must not become 50MB of RSS on a box
 * that's also serving the API (same discipline as services/incidentImages.ts,
 * which streams uploads to disk rather than holding request bodies in memory).
 * Returns false if R2 isn't configured or the object can't be read.
 */
export async function downloadR2ToFile(key: string, destPath: string): Promise<boolean> {
  if (!key || !r2Configured()) return false;
  try {
    const { GetObjectCommand } = await import('@aws-sdk/client-s3');
    const { createWriteStream } = await import('node:fs');
    const { pipeline } = await import('node:stream/promises');
    const s3 = await r2Client();
    const res = await s3.send(new GetObjectCommand({ Bucket: config.R2_BUCKET as string, Key: key }));
    const body = res.Body as NodeJS.ReadableStream | undefined;
    if (!body) return false;
    await pipeline(body, createWriteStream(destPath));
    return true;
  } catch (err) {
    log.warn({ err, key }, 'wire: R2 download failed');
    return false;
  }
}

/**
 * Upload a local file to R2 under `key`. Streamed from disk for the same reason
 * as above. Returns false on any failure — callers treat that as "leave the
 * original in place" rather than losing content.
 */
export async function uploadFileToR2(
  filePath: string,
  key: string,
  contentType: string,
): Promise<boolean> {
  if (!key || !r2Configured()) return false;
  try {
    const { PutObjectCommand } = await import('@aws-sdk/client-s3');
    const { createReadStream, statSync } = await import('node:fs');
    const s3 = await r2Client();
    await s3.send(new PutObjectCommand({
      Bucket: config.R2_BUCKET as string,
      Key: key,
      Body: createReadStream(filePath),
      // R2 needs an explicit length for a stream body.
      ContentLength: statSync(filePath).size,
      ContentType: contentType,
    }));
    return true;
  } catch (err) {
    log.warn({ err, key }, 'wire: R2 upload failed');
    return false;
  }
}

/** A fresh R2 key alongside an existing one, e.g. the transcoded output. */
export function newVideoKey(): string {
  return `wire/videos/${randomUUID()}.mp4`;
}
export function newPosterKey(): string {
  return `wire/img/${randomUUID()}.jpg`;
}

// ---- Licensing -------------------------------------------------------------

/** The offered license set (code -> human label). Plain-language rather than
 * CC jargon. Validated in app so the set can grow without a migration.
 * 'credit' is the default. */
export const WIRE_LICENSES: Record<string, string> = {
  credit: 'Credit required',
  display: 'All rights reserved',
  public: 'Public domain',
};

export function normaliseLicense(v: unknown): string {
  return typeof v === 'string' && Object.prototype.hasOwnProperty.call(WIRE_LICENSES, v) ? v : 'credit';
}

export function licenseLabel(code: string): string {
  return WIRE_LICENSES[code] ?? WIRE_LICENSES['credit']!;
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
  thumb_r2_key?: string | null;
  poster_cf_image_id: string | null;
  poster_r2_key: string | null;
  hash?: string | null;
  duration_seconds: number | null;
  is_cover: boolean;
  unit: string | null;
  sort_order: number;
  width: number | null;
  height: number | null;
  bytes: number | null;
  process_state?: string | null;
}

/** Attach delivery URLs so the frontend never constructs storage URLs itself. */
/**
 * Shape a wire_media row for the API. Storage keys + the content hash are only
 * emitted when `includeKeys` is true — i.e. to the author/admin, so the compose
 * editor can round-trip existing media on edit without re-uploading. Public
 * reads omit them: they're not needed to render, and the hash in particular is
 * internal (used for dedup) and shouldn't be broadcast.
 */
export function shapeMedia(row: WireMediaRow, includeKeys = false): Record<string, unknown> {
  const out: Record<string, unknown> = {
    id: row.id,
    kind: row.kind,
    is_cover: row.is_cover,
    unit: row.unit,
    sort_order: row.sort_order,
    width: row.width,
    height: row.height,
    duration_seconds: row.duration_seconds,
  };
  if (includeKeys) {
    out['cf_image_id'] = row.cf_image_id;
    out['r2_key'] = row.r2_key;
    out['poster_r2_key'] = row.poster_r2_key;
    out['poster_cf_image_id'] = row.poster_cf_image_id;
    out['hash'] = row.hash ?? null;
  }
  if (row.kind === 'image') {
    // New images live in R2 (single optimised WebP). Legacy Cloudflare-Images
    // rows fall back to variant URLs.
    if (row.r2_key) {
      // `url` stays the stored original: it's already capped at 1600px WebP by
      // the composer, so a transform at that size would cost a billable variant
      // to save nothing. The smaller cuts are where the bandwidth actually is.
      out['url'] = r2PublicUrl(row.r2_key);
      out['feed_url'] = r2TransformUrl(row.r2_key, { width: IMG_WIDTHS.feed });
      out['thumb_url'] = r2TransformUrl(row.r2_key, { width: IMG_WIDTHS.thumb, quality: 80 });
    } else if (row.cf_image_id) {
      out['url'] = imageVariantUrl(row.cf_image_id, 'public');
      out['feed_url'] = imageVariantUrl(row.cf_image_id, 'feed');
      out['thumb_url'] = imageVariantUrl(row.cf_image_id, 'thumb');
    }
  } else if (row.kind === 'video' && row.r2_key) {
    // Still queued for the ffmpeg pass: the file IS playable (it's the raw
    // upload) but it's un-normalised and unwatermarked, and its poster may not
    // exist yet — so the UI shows a placeholder rather than the wrong thing.
    if (row.process_state === 'pending') out['processing'] = true;
    out['url'] = r2PublicUrl(row.r2_key);
    if (row.poster_r2_key) {
      // ffmpeg cuts the poster at the video's own resolution (up to 1080p), so
      // unlike photos it genuinely needs scaling down for both consumers.
      out['poster_url'] = r2TransformUrl(row.poster_r2_key, { width: IMG_WIDTHS.poster });
      out['poster_feed_url'] = r2TransformUrl(row.poster_r2_key, { width: IMG_WIDTHS.feed });
    } else if (row.poster_cf_image_id) {
      out['poster_url'] = imageVariantUrl(row.poster_cf_image_id, 'public');
    }
  }
  return out;
}
