/**
 * Reading files out of a zip held in memory — for the open-data downloads
 * that only come zipped (sources/saRoads.ts). Stored and deflated entries,
 * which is all a published dataset uses; no zip64, no encryption, no
 * spanning. The central directory at the end is what is read, so a local
 * header's sizes (absent when the zip was streamed) never matter.
 */
import { inflateRawSync } from 'node:zlib';

const END_OF_DIRECTORY = 0x06054b50;
const DIRECTORY_ENTRY = 0x02014b50;
const LOCAL_HEADER = 0x04034b50;

/** The entries whose names `want` accepts, by name, uncompressed. */
export function unzip(zip: Uint8Array, want: (name: string) => boolean = () => true): Map<string, Buffer> {
  const buf = Buffer.from(zip.buffer, zip.byteOffset, zip.byteLength);
  // The end-of-directory record: 22 bytes, then a comment of up to 64 KB.
  let end = -1;
  for (let i = buf.length - 22; i >= Math.max(0, buf.length - 22 - 0xffff); i--) {
    if (buf.readUInt32LE(i) === END_OF_DIRECTORY) {
      end = i;
      break;
    }
  }
  if (end < 0) throw new Error('not a zip: no end-of-directory record');
  const count = buf.readUInt16LE(end + 10);
  let at = buf.readUInt32LE(end + 16);
  const out = new Map<string, Buffer>();
  for (let n = 0; n < count; n++) {
    if (buf.readUInt32LE(at) !== DIRECTORY_ENTRY) throw new Error(`zip: bad directory entry ${n}`);
    const method = buf.readUInt16LE(at + 10);
    const packed = buf.readUInt32LE(at + 20);
    const size = buf.readUInt32LE(at + 24);
    const nameLen = buf.readUInt16LE(at + 28);
    const extraLen = buf.readUInt16LE(at + 30);
    const commentLen = buf.readUInt16LE(at + 32);
    const local = buf.readUInt32LE(at + 42);
    const name = buf.toString('utf8', at + 46, at + 46 + nameLen);
    at += 46 + nameLen + extraLen + commentLen;
    if (name.endsWith('/') || !want(name)) continue;
    if (buf.readUInt32LE(local) !== LOCAL_HEADER) throw new Error(`zip: bad local header for ${name}`);
    const start = local + 30 + buf.readUInt16LE(local + 26) + buf.readUInt16LE(local + 28);
    const data = buf.subarray(start, start + packed);
    const body = method === 0 ? Buffer.from(data) : method === 8 ? inflateRawSync(data) : null;
    if (!body) throw new Error(`zip: ${name} uses compression method ${method}`);
    if (body.length !== size) throw new Error(`zip: ${name} came out ${body.length} bytes, not ${size}`);
    out.set(name, body);
  }
  return out;
}
