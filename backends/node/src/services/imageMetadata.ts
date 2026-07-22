/**
 * Metadata stripping for uploaded incident photos.
 *
 * Phone photos carry EXIF, and EXIF carries GPS. Since the original file
 * is publicly reachable at its /uploads/... URL, publishing it verbatim
 * would leak the photographer's exact coordinates (and camera serial,
 * timestamps, etc.) to anyone who downloads it. Cloudflare's resized
 * variants drop metadata, but the original must be clean too.
 *
 * This is a STRUCTURAL rewriter, not a re-encoder:
 *   - no image decoding, so a decompression bomb can't blow up memory
 *   - pixels are bit-identical; no quality loss and no CPU cost
 *   - memory is bounded: we parse only container headers via positional
 *     reads, then stream-copy the byte ranges worth keeping
 *
 * Parsing is FAIL-CLOSED. If a file doesn't match its format's structure
 * we reject the upload rather than silently storing something we
 * couldn't scrub — a loud failure is much better than a quiet GPS leak.
 */
import { createReadStream, createWriteStream } from 'node:fs';
import { open, type FileHandle } from 'node:fs/promises';
import { once } from 'node:events';

export class MetadataStripError extends Error {
  constructor(reason: string) {
    super(`Could not process image: ${reason}`);
    this.name = 'MetadataStripError';
  }
}

/** Output plan: byte ranges copied verbatim, plus any patched bytes. */
type Op =
  | { kind: 'copy'; start: number; end: number }
  | { kind: 'bytes'; data: Buffer };

async function readAt(fh: FileHandle, pos: number, len: number): Promise<Buffer> {
  const buf = Buffer.alloc(len);
  const { bytesRead } = await fh.read(buf, 0, len, pos);
  if (bytesRead < len) throw new MetadataStripError('unexpected end of file');
  return buf;
}

/** Merge adjacent copy ranges so the write pass opens fewer streams. */
function coalesce(ops: Op[]): Op[] {
  const out: Op[] = [];
  for (const op of ops) {
    const prev = out[out.length - 1];
    if (op.kind === 'copy' && prev && prev.kind === 'copy' && prev.end === op.start) {
      prev.end = op.end;
    } else {
      out.push(op);
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// JPEG — drop APP1 (EXIF/XMP, where GPS lives), APP3..APP13 (maker notes,
// IPTC, Ducky) and COM comments. APP0 (JFIF), APP2 (ICC colour profile) and
// APP14 (Adobe colour transform) stay: dropping those changes how the
// image renders.
// ---------------------------------------------------------------------------
const JPEG_DROP = new Set<number>([
  0xe1, // APP1  — EXIF / XMP
  0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xeb, 0xec, // APP3..APP12
  0xed, // APP13 — Photoshop / IPTC
  0xfe, // COM   — free-text comment
]);

async function planJpeg(fh: FileHandle, size: number): Promise<Op[]> {
  const soi = await readAt(fh, 0, 2);
  if (soi[0] !== 0xff || soi[1] !== 0xd8) throw new MetadataStripError('not a JPEG');

  const ops: Op[] = [{ kind: 'copy', start: 0, end: 2 }];
  let pos = 2;

  for (;;) {
    if (pos + 2 > size) throw new MetadataStripError('truncated JPEG');
    const head = await readAt(fh, pos, 2);
    if (head[0] !== 0xff) throw new MetadataStripError('bad JPEG marker');
    const marker = head[1]!;

    // Fill bytes: a run of 0xFF is padding before the real marker.
    if (marker === 0xff) { pos += 1; continue; }
    // Standalone markers carry no payload.
    if (marker === 0x01 || (marker >= 0xd0 && marker <= 0xd7)) {
      ops.push({ kind: 'copy', start: pos, end: pos + 2 });
      pos += 2;
      continue;
    }
    // Start of scan: entropy-coded data runs to EOI; copy the remainder.
    if (marker === 0xda) {
      ops.push({ kind: 'copy', start: pos, end: size });
      break;
    }
    if (marker === 0xd9) { // EOI
      ops.push({ kind: 'copy', start: pos, end: pos + 2 });
      break;
    }

    if (pos + 4 > size) throw new MetadataStripError('truncated JPEG segment');
    const lenBuf = await readAt(fh, pos + 2, 2);
    const segLen = lenBuf.readUInt16BE(0);
    if (segLen < 2) throw new MetadataStripError('bad JPEG segment length');
    const end = pos + 2 + segLen;
    if (end > size) throw new MetadataStripError('JPEG segment overruns file');

    if (!JPEG_DROP.has(marker)) ops.push({ kind: 'copy', start: pos, end });
    pos = end;
  }
  return ops;
}

// ---------------------------------------------------------------------------
// PNG — drop eXIf plus the textual/time chunks. Colour-management chunks
// (gAMA/cHRM/iCCP/sRGB) are kept so rendering is unchanged.
// ---------------------------------------------------------------------------
const PNG_DROP = new Set(['eXIf', 'tEXt', 'zTXt', 'iTXt', 'tIME']);

async function planPng(fh: FileHandle, size: number): Promise<Op[]> {
  const sig = await readAt(fh, 0, 8);
  if (sig.toString('latin1') !== '\x89PNG\r\n\x1a\n') throw new MetadataStripError('not a PNG');

  const ops: Op[] = [{ kind: 'copy', start: 0, end: 8 }];
  let pos = 8;

  while (pos < size) {
    if (pos + 8 > size) throw new MetadataStripError('truncated PNG chunk');
    const head = await readAt(fh, pos, 8);
    const dataLen = head.readUInt32BE(0);
    const type = head.subarray(4, 8).toString('latin1');
    // 12 = 4 length + 4 type + 4 CRC. Guard against a hostile length.
    const total = dataLen + 12;
    if (dataLen > size || pos + total > size) {
      throw new MetadataStripError('PNG chunk overruns file');
    }
    if (!PNG_DROP.has(type)) ops.push({ kind: 'copy', start: pos, end: pos + total });
    pos += total;
    if (type === 'IEND') break;
  }
  return ops;
}

// ---------------------------------------------------------------------------
// WebP — RIFF container. Drop the EXIF/XMP chunks, clear the matching
// flag bits in VP8X, and rewrite the RIFF size field.
// ---------------------------------------------------------------------------
async function planWebp(fh: FileHandle, size: number): Promise<Op[]> {
  const head = await readAt(fh, 0, 12);
  if (head.subarray(0, 4).toString('latin1') !== 'RIFF' ||
      head.subarray(8, 12).toString('latin1') !== 'WEBP') {
    throw new MetadataStripError('not a WebP');
  }

  const body: Op[] = [];
  let pos = 12;
  let kept = 4; // the 'WEBP' fourcc counts toward the RIFF payload size

  while (pos < size) {
    if (pos + 8 > size) throw new MetadataStripError('truncated WebP chunk');
    const ch = await readAt(fh, pos, 8);
    const fourcc = ch.subarray(0, 4).toString('latin1');
    const chunkLen = ch.readUInt32LE(4);
    const padded = chunkLen + (chunkLen % 2); // chunks are even-aligned
    const total = 8 + padded;
    if (chunkLen > size || pos + total > size) {
      throw new MetadataStripError('WebP chunk overruns file');
    }

    if (fourcc === 'EXIF' || fourcc === 'XMP ') {
      // dropped
    } else if (fourcc === 'VP8X' && chunkLen >= 1) {
      // Flags byte: bit3 = EXIF present, bit2 = XMP present. Leaving them
      // set while removing the chunks would make the file self-inconsistent.
      const flags = await readAt(fh, pos + 8, 1);
      const patched = Buffer.from([flags[0]! & 0xf3]);
      body.push({ kind: 'copy', start: pos, end: pos + 8 });
      body.push({ kind: 'bytes', data: patched });
      body.push({ kind: 'copy', start: pos + 9, end: pos + total });
      kept += total;
    } else {
      body.push({ kind: 'copy', start: pos, end: pos + total });
      kept += total;
    }
    pos += total;
  }

  const riff = Buffer.alloc(12);
  riff.write('RIFF', 0, 'latin1');
  riff.writeUInt32LE(kept, 4);
  riff.write('WEBP', 8, 'latin1');
  return [{ kind: 'bytes', data: riff }, ...body];
}

// ---------------------------------------------------------------------------
// GIF — drop Comment, Plain Text and Application extensions. NETSCAPE /
// ANIMEXTS application blocks are kept because they carry the animation
// loop count; XMP arrives as an application block and is dropped.
// ---------------------------------------------------------------------------
const GIF_KEEP_APP = new Set(['NETSCAPE2.0', 'ANIMEXTS1.0']);

/** Walk a GIF sub-block chain, returning the position just past its terminator. */
async function skipSubBlocks(fh: FileHandle, start: number, size: number): Promise<number> {
  let pos = start;
  for (;;) {
    if (pos >= size) throw new MetadataStripError('truncated GIF sub-block');
    const len = (await readAt(fh, pos, 1))[0]!;
    pos += 1;
    if (len === 0) return pos;
    pos += len;
  }
}

async function planGif(fh: FileHandle, size: number): Promise<Op[]> {
  const header = await readAt(fh, 0, 13);
  const magic = header.subarray(0, 6).toString('latin1');
  if (magic !== 'GIF87a' && magic !== 'GIF89a') throw new MetadataStripError('not a GIF');

  let pos = 13;
  const packed = header[10]!;
  if (packed & 0x80) pos += 3 * (1 << ((packed & 0x07) + 1)); // global colour table
  if (pos > size) throw new MetadataStripError('truncated GIF header');
  const ops: Op[] = [{ kind: 'copy', start: 0, end: pos }];

  while (pos < size) {
    const block = (await readAt(fh, pos, 1))[0]!;
    if (block === 0x3b) { // trailer
      ops.push({ kind: 'copy', start: pos, end: pos + 1 });
      break;
    }
    if (block === 0x2c) { // image descriptor
      const desc = await readAt(fh, pos, 10);
      let p = pos + 10;
      const lp = desc[9]!;
      if (lp & 0x80) p += 3 * (1 << ((lp & 0x07) + 1)); // local colour table
      p += 1; // LZW minimum code size
      p = await skipSubBlocks(fh, p, size);
      ops.push({ kind: 'copy', start: pos, end: p });
      pos = p;
      continue;
    }
    if (block === 0x21) { // extension
      const label = (await readAt(fh, pos + 1, 1))[0]!;
      let drop = label === 0xfe || label === 0x01; // comment / plain text
      if (label === 0xff) { // application extension
        const blkLen = (await readAt(fh, pos + 2, 1))[0]!;
        const appId = blkLen >= 11
          ? (await readAt(fh, pos + 3, 11)).toString('latin1')
          : '';
        drop = !GIF_KEEP_APP.has(appId);
      }
      // Sub-blocks start right after the label for every extension type
      // (for an application extension the first one is the 11-byte app id).
      const endPos = await skipSubBlocks(fh, pos + 2, size);
      if (!drop) ops.push({ kind: 'copy', start: pos, end: endPos });
      pos = endPos;
      continue;
    }
    throw new MetadataStripError('unexpected GIF block');
  }
  return ops;
}

/** Execute a plan: stream the kept ranges from src into dest. */
async function applyOps(srcPath: string, destPath: string, ops: Op[]): Promise<number> {
  const out = createWriteStream(destPath);
  let written = 0;
  try {
    for (const op of ops) {
      if (op.kind === 'bytes') {
        if (!out.write(op.data)) await once(out, 'drain');
        written += op.data.length;
        continue;
      }
      if (op.end <= op.start) continue;
      const rs = createReadStream(srcPath, { start: op.start, end: op.end - 1 });
      for await (const chunk of rs) {
        const buf = chunk as Buffer;
        if (!out.write(buf)) await once(out, 'drain');
        written += buf.length;
      }
    }
    await new Promise<void>((resolve, reject) => {
      out.end((err?: NodeJS.ErrnoException | null) => (err ? reject(err) : resolve()));
    });
    return written;
  } catch (err) {
    out.destroy();
    throw err;
  }
}

/**
 * Rewrite `srcPath` into `destPath` without EXIF/XMP/comment metadata.
 * Returns the stripped size. Throws MetadataStripError when the file
 * doesn't parse — callers must reject the upload in that case.
 */
export async function stripMetadataToFile(
  srcPath: string,
  destPath: string,
  contentType: string,
): Promise<number> {
  const fh = await open(srcPath, 'r');
  let ops: Op[];
  try {
    const { size } = await fh.stat();
    if (size <= 0) throw new MetadataStripError('empty file');
    switch (contentType) {
      case 'image/jpeg': ops = await planJpeg(fh, size); break;
      case 'image/png': ops = await planPng(fh, size); break;
      case 'image/webp': ops = await planWebp(fh, size); break;
      case 'image/gif': ops = await planGif(fh, size); break;
      default: throw new MetadataStripError('unsupported type');
    }
  } finally {
    await fh.close();
  }
  const written = await applyOps(srcPath, destPath, coalesce(ops));
  if (written <= 0) throw new MetadataStripError('produced an empty image');
  return written;
}

/** Test seam: plan-only, so unit tests can assert what gets dropped. */
export async function _planForTests(srcPath: string, contentType: string): Promise<Op[]> {
  const fh = await open(srcPath, 'r');
  try {
    const { size } = await fh.stat();
    switch (contentType) {
      case 'image/jpeg': return coalesce(await planJpeg(fh, size));
      case 'image/png': return coalesce(await planPng(fh, size));
      case 'image/webp': return coalesce(await planWebp(fh, size));
      case 'image/gif': return coalesce(await planGif(fh, size));
      default: throw new MetadataStripError('unsupported type');
    }
  } finally {
    await fh.close();
  }
}
