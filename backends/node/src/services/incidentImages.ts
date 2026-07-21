/**
 * On-disk storage for incident photos.
 *
 * The repo root doubles as the Apache webroot for nswpsn.forcequit.xyz, so
 * files written under <repo>/uploads/incident-images/<incidentId>/ are
 * publicly served at
 *   https://nswpsn.forcequit.xyz/uploads/incident-images/<id>/<img>.jpg
 * and resized on demand by Cloudflare Image Transformations via
 *   /cdn-cgi/image/width=320,quality=78,format=auto/uploads/...
 *
 * Uploads are streamed straight to disk (never fully buffered): a 50MB
 * body held in RAM per concurrent upload would be a trivial way to OOM
 * the box, and @hono/node-server buffers whole bodies by default.
 */
import { createWriteStream } from 'node:fs';
import { mkdir, rm, unlink, rename, readdir, stat } from 'node:fs/promises';
import { once } from 'node:events';
import path from 'node:path';
import { Readable } from 'node:stream';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { stripMetadataToFile } from './imageMetadata.js';

/** Hard per-image ceiling. Mirrored by the editor's client-side check. */
export const MAX_IMAGE_BYTES = 50 * 1024 * 1024;
/** Hard per-incident ceiling, enforced inside the DB transaction. */
export const MAX_IMAGES_PER_INCIDENT = 4;

/** Public URL prefix (also the on-disk subdirectory under UPLOADS_DIR). */
const IMAGE_SUBDIR = 'incident-images';

export interface IncidentImage {
  id: string;
  /** Root-relative public path, e.g. /uploads/incident-images/<inc>/<id>.jpg */
  file: string;
  size: number;
  content_type: string;
  uploaded_by: string | null;
  uploaded_by_name: string | null;
  uploaded_at: string;
}

/**
 * Accepted upload types. HEIC is deliberately absent: Cloudflare's
 * transformations can't resize it, so an iPhone HEIC would be served as a
 * 5MB original to every viewer. The editor's file input declares this same
 * list, which makes iOS transcode HEIC to JPEG before upload.
 */
const CONTENT_TYPE_EXT: Record<string, string> = {
  'image/jpeg': 'jpg',
  'image/png': 'png',
  'image/webp': 'webp',
  'image/gif': 'gif',
};

export function extForContentType(contentType: string): string | null {
  return CONTENT_TYPE_EXT[contentType.toLowerCase().split(';')[0]!.trim()] ?? null;
}

export function normaliseContentType(raw: string | undefined | null): string | null {
  if (!raw) return null;
  const ct = raw.toLowerCase().split(';')[0]!.trim();
  return ct in CONTENT_TYPE_EXT ? ct : null;
}

/**
 * Identify an image from its leading bytes. A caller-declared Content-Type
 * is untrusted — without this, `Content-Type: image/png` on a .js payload
 * would land an executable-looking file in the webroot.
 */
export function sniffImageType(header: Buffer): string | null {
  if (header.length >= 3 && header[0] === 0xff && header[1] === 0xd8 && header[2] === 0xff) {
    return 'image/jpeg';
  }
  if (
    header.length >= 8 &&
    header[0] === 0x89 && header[1] === 0x50 && header[2] === 0x4e && header[3] === 0x47 &&
    header[4] === 0x0d && header[5] === 0x0a && header[6] === 0x1a && header[7] === 0x0a
  ) {
    return 'image/png';
  }
  if (header.length >= 6 && header.subarray(0, 6).toString('latin1').match(/^GIF8[79]a$/)) {
    return 'image/gif';
  }
  if (
    header.length >= 12 &&
    header.subarray(0, 4).toString('latin1') === 'RIFF' &&
    header.subarray(8, 12).toString('latin1') === 'WEBP'
  ) {
    return 'image/webp';
  }
  return null;
}

/**
 * Incident ids reach us from the client (POST /api/incidents accepts a
 * caller-supplied id for the RFS/pager stub flow), so they can't be
 * interpolated into a filesystem path unchecked — `../../` would escape
 * the uploads root. UUIDs and the deterministic stub ids both satisfy this.
 */
export function isSafeIdSegment(id: string): boolean {
  // No '.' — UUIDs and the deterministic RFS/pager stub ids don't contain
  // one, and allowing it would let an editor-chosen incident id create a
  // directory like `evil.php/` inside the webroot.
  return /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(id);
}

/** Absolute path of the uploads root (resolved from the backend's cwd). */
export function uploadsRoot(): string {
  return path.resolve(config.UPLOADS_DIR);
}

function incidentDir(incidentId: string): string {
  return path.join(uploadsRoot(), IMAGE_SUBDIR, incidentId);
}

/** Root-relative public URL path for an image file. */
export function publicPathFor(incidentId: string, fileName: string): string {
  return `/${path.posix.join('uploads', IMAGE_SUBDIR, incidentId, fileName)}`;
}

export class ImageTooLargeError extends Error {
  constructor() {
    super('Image exceeds the maximum size');
    this.name = 'ImageTooLargeError';
  }
}
export class ImageTypeMismatchError extends Error {
  constructor(public readonly detected: string | null) {
    super('Image content does not match its declared type');
    this.name = 'ImageTypeMismatchError';
  }
}

/**
 * Stream an upload to disk, enforcing the size ceiling, verifying the
 * magic bytes as the first chunks arrive, and stripping EXIF/XMP/comment
 * metadata before the file becomes publicly reachable.
 *
 * The upload lands in `<id>.<ext>.part`; the metadata-stripped rewrite is
 * what gets renamed into place, so the raw bytes (with any GPS in them)
 * are never served. Any failure removes both temporaries before
 * rethrowing.
 */
export async function saveIncidentImageStream(
  incidentId: string,
  imageId: string,
  contentType: string,
  body: ReadableStream<Uint8Array>,
): Promise<{ size: number; fileName: string; publicPath: string }> {
  const ext = extForContentType(contentType);
  if (!ext) throw new ImageTypeMismatchError(null);
  // Self-guard: callers pass these from route params. Every sibling helper
  // validates them; do so here too so a future call site can't write
  // outside the uploads tree. The route already rejects bad ids, so this
  // is unreachable defense-in-depth.
  if (!isSafeIdSegment(incidentId) || !isSafeIdSegment(imageId)) {
    throw new Error('incidentImages: unsafe id segment');
  }

  const dir = incidentDir(incidentId);
  await mkdir(dir, { recursive: true });

  const fileName = `${imageId}.${ext}`;
  const finalPath = path.join(dir, fileName);
  const partPath = `${finalPath}.part`;
  const strippedPath = `${finalPath}.stripped`;

  const out = createWriteStream(partPath);
  let size = 0;
  let header: Buffer = Buffer.alloc(0);
  let sniffed = false;

  try {
    for await (const chunk of Readable.fromWeb(body as Parameters<typeof Readable.fromWeb>[0])) {
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
      size += buf.length;
      // Belt-and-braces beyond hono's bodyLimit: that trusts Content-Length,
      // this counts what actually arrives.
      if (size > MAX_IMAGE_BYTES) throw new ImageTooLargeError();

      if (!sniffed) {
        header = header.length ? Buffer.concat([header, buf]) : buf;
        // 12 bytes covers the longest signature we check (RIFF....WEBP).
        if (header.length >= 12) {
          const detected = sniffImageType(header);
          if (detected !== contentType) throw new ImageTypeMismatchError(detected);
          sniffed = true;
        }
      }

      if (!out.write(buf)) await once(out, 'drain');
    }

    // Tiny payload that never reached the 12-byte sniff threshold.
    if (!sniffed) {
      const detected = sniffImageType(header);
      if (detected !== contentType) throw new ImageTypeMismatchError(detected);
    }

    await new Promise<void>((resolve, reject) => {
      out.end((err?: NodeJS.ErrnoException | null) => (err ? reject(err) : resolve()));
    });

    // Scrub EXIF/GPS before the file is reachable. Throws (and so rejects
    // the upload) if the image doesn't parse — we never publish bytes we
    // couldn't scrub.
    const strippedSize = await stripMetadataToFile(partPath, strippedPath, contentType);
    await rename(strippedPath, finalPath);
    await unlink(partPath).catch(() => undefined);
    return { size: strippedSize, fileName, publicPath: publicPathFor(incidentId, fileName) };
  } catch (err) {
    out.destroy();
    await unlink(partPath).catch(() => undefined);
    await unlink(strippedPath).catch(() => undefined);
    throw err;
  }
}

/**
 * Remove one stored image. `file` is the stored root-relative path; it is
 * re-derived from the incident id + basename rather than trusted, so a
 * tampered DB row can't delete outside the uploads tree.
 */
export async function deleteIncidentImageFile(incidentId: string, file: string): Promise<void> {
  if (!isSafeIdSegment(incidentId)) return;
  const base = path.posix.basename(file || '');
  if (!base || base.includes('/') || base.includes('\\') || base.includes('..')) return;
  await unlink(path.join(incidentDir(incidentId), base)).catch(() => undefined);
}

/** Drop an incident's whole image directory (retention purge). */
export async function removeIncidentImageDir(incidentId: string): Promise<void> {
  if (!isSafeIdSegment(incidentId)) return;
  try {
    await rm(incidentDir(incidentId), { recursive: true, force: true });
  } catch (err) {
    log.warn({ err, incidentId }, 'incident images: directory removal failed');
  }
}

/**
 * Sweep abandoned `.part`/`.stripped` temp files older than one hour. A
 * process kill mid-upload leaves these behind; they're never servable
 * (the .htaccess denies them) but would otherwise leak disk forever.
 * Called from the periodic cleanup job.
 */
export async function sweepStaleUploadParts(maxAgeMs = 3_600_000): Promise<number> {
  const base = path.join(uploadsRoot(), IMAGE_SUBDIR);
  const cutoff = Date.now() - maxAgeMs;
  let removed = 0;
  let dirs: string[];
  try {
    dirs = await readdir(base);
  } catch {
    return 0; // nothing uploaded yet
  }
  for (const dir of dirs) {
    if (!isSafeIdSegment(dir)) continue;
    const full = path.join(base, dir);
    let files: string[];
    try {
      files = await readdir(full);
    } catch {
      continue;
    }
    for (const f of files) {
      if (!f.endsWith('.part') && !f.endsWith('.stripped')) continue;
      const p = path.join(full, f);
      try {
        const st = await stat(p);
        if (st.mtimeMs < cutoff) {
          await unlink(p);
          removed += 1;
        }
      } catch {
        /* raced with another sweep / delete — ignore */
      }
    }
  }
  if (removed > 0) log.info({ removed }, 'incident images: swept stale upload temp files');
  return removed;
}

/** Coerce a stored JSONB value into a typed image list. */
export function parseIncidentImages(raw: unknown): IncidentImage[] {
  let value = raw;
  if (typeof value === 'string') {
    try {
      value = JSON.parse(value);
    } catch {
      return [];
    }
  }
  if (!Array.isArray(value)) return [];
  return value.filter(
    (v): v is IncidentImage =>
      !!v && typeof v === 'object' &&
      typeof (v as IncidentImage).id === 'string' &&
      typeof (v as IncidentImage).file === 'string',
  );
}
