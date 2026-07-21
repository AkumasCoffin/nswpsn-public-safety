/**
 * Metadata stripping. Each test builds a real container carrying real
 * metadata (including EXIF GPS), strips it, and asserts that the payload
 * survives byte-identical while the metadata is gone.
 */
import { describe, it, expect, afterAll } from 'vitest';
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { stripMetadataToFile, MetadataStripError } from '../../../src/services/imageMetadata.js';

const DIR = mkdtempSync(path.join(tmpdir(), 'nswpsn-meta-'));
afterAll(() => rmSync(DIR, { recursive: true, force: true }));

let seq = 0;
/** Write `buf`, strip it, return the stripped bytes. */
async function strip(buf: Buffer, contentType: string): Promise<Buffer> {
  const src = path.join(DIR, `in-${seq}.bin`);
  const dst = path.join(DIR, `out-${seq++}.bin`);
  writeFileSync(src, buf);
  await stripMetadataToFile(src, dst, contentType);
  return readFileSync(dst);
}

/** A JPEG segment: marker + 2-byte length + payload. */
function seg(marker: number, payload: Buffer): Buffer {
  const len = Buffer.alloc(2);
  len.writeUInt16BE(payload.length + 2, 0);
  return Buffer.concat([Buffer.from([0xff, marker]), len, payload]);
}

// A minimal but genuine EXIF APP1 payload with a GPS IFD pointer, so the
// test is stripping something that really would leak a location.
const EXIF_GPS = Buffer.concat([
  Buffer.from('Exif\0\0', 'latin1'),
  Buffer.from('II*\0\x08\0\0\0', 'latin1'), // little-endian TIFF header
  Buffer.from([0x01, 0x00]),                // 1 IFD entry
  Buffer.from([0x25, 0x88, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00]), // GPSInfo
  Buffer.from([0x00, 0x00, 0x00, 0x00]),
  Buffer.from('GPSDATA-33.8688,151.2093', 'latin1'),
]);

describe('JPEG', () => {
  const scan = Buffer.concat([
    Buffer.from([0xff, 0xda, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3f, 0x00]), // SOS
    Buffer.from([0x12, 0x34, 0x56, 0x78, 0x9a]),                               // entropy data
    Buffer.from([0xff, 0xd9]),                                                 // EOI
  ]);

  it('removes EXIF/GPS, XMP, IPTC and comments but keeps image data', async () => {
    const input = Buffer.concat([
      Buffer.from([0xff, 0xd8]),
      seg(0xe0, Buffer.from('JFIF\0\x01\x02\0\0\x01\0\x01\0\0', 'latin1')), // APP0 — keep
      seg(0xe1, EXIF_GPS),                                                  // APP1 EXIF — drop
      seg(0xe1, Buffer.from('http://ns.adobe.com/xap/1.0/\0<x:xmpmeta/>', 'latin1')), // XMP — drop
      seg(0xe2, Buffer.from('ICC_PROFILE\0fake-profile', 'latin1')),        // APP2 ICC — keep
      seg(0xed, Buffer.from('Photoshop 3.0\0IPTC-NAME', 'latin1')),         // APP13 — drop
      seg(0xfe, Buffer.from('taken by alice', 'latin1')),                   // COM — drop
      seg(0xdb, Buffer.alloc(65, 1)),                                       // DQT — keep
      scan,
    ]);
    const out = await strip(input, 'image/jpeg');

    expect(out.includes(Buffer.from('GPSDATA'))).toBe(false);
    expect(out.includes(Buffer.from('Exif\0\0', 'latin1'))).toBe(false);
    expect(out.includes(Buffer.from('xmpmeta'))).toBe(false);
    expect(out.includes(Buffer.from('IPTC-NAME'))).toBe(false);
    expect(out.includes(Buffer.from('taken by alice'))).toBe(false);

    // Rendering-relevant segments and the scan survive untouched.
    expect(out.includes(Buffer.from('JFIF'))).toBe(true);
    expect(out.includes(Buffer.from('ICC_PROFILE'))).toBe(true);
    expect(out.subarray(0, 2)).toEqual(Buffer.from([0xff, 0xd8]));
    expect(out.includes(scan)).toBe(true);
    expect(out.length).toBeLessThan(input.length);
  });

  it('is a no-op for a JPEG that carries no metadata', async () => {
    const clean = Buffer.concat([
      Buffer.from([0xff, 0xd8]),
      seg(0xdb, Buffer.alloc(65, 1)),
      scan,
    ]);
    expect(await strip(clean, 'image/jpeg')).toEqual(clean);
  });

  it('rejects a truncated or malformed JPEG instead of storing it', async () => {
    // Fail-closed: we never publish bytes we could not scrub.
    await expect(strip(Buffer.from([0xff, 0xd8, 0xff]), 'image/jpeg')).rejects.toThrow(MetadataStripError);
    const badLen = Buffer.concat([
      Buffer.from([0xff, 0xd8]),
      Buffer.from([0xff, 0xe1, 0xff, 0xf0]), // segment length overruns the file
    ]);
    await expect(strip(badLen, 'image/jpeg')).rejects.toThrow(MetadataStripError);
  });
});

describe('PNG', () => {
  const SIG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
  function chunk(type: string, data: Buffer): Buffer {
    const len = Buffer.alloc(4);
    len.writeUInt32BE(data.length, 0);
    return Buffer.concat([len, Buffer.from(type, 'latin1'), data, Buffer.alloc(4)]);
  }

  it('drops eXIf/tEXt/iTXt/tIME and keeps image + colour chunks', async () => {
    const input = Buffer.concat([
      SIG,
      chunk('IHDR', Buffer.alloc(13, 2)),
      chunk('eXIf', Buffer.from('GPSDATA-33.8688', 'latin1')),
      chunk('tEXt', Buffer.from('Author\0alice', 'latin1')),
      chunk('iTXt', Buffer.from('XML:com.adobe.xmp\0\0\0\0\0<x:xmpmeta/>', 'latin1')),
      chunk('tIME', Buffer.alloc(7, 3)),
      chunk('sRGB', Buffer.from([0])),
      chunk('IDAT', Buffer.from('PIXELDATA', 'latin1')),
      chunk('IEND', Buffer.alloc(0)),
    ]);
    const out = await strip(input, 'image/png');

    expect(out.includes(Buffer.from('GPSDATA'))).toBe(false);
    expect(out.includes(Buffer.from('alice'))).toBe(false);
    expect(out.includes(Buffer.from('xmpmeta'))).toBe(false);
    expect(out.includes(Buffer.from('sRGB'))).toBe(true);
    expect(out.includes(Buffer.from('PIXELDATA'))).toBe(true);
    expect(out.subarray(0, 8)).toEqual(SIG);
    expect(out.subarray(out.length - 12).includes(Buffer.from('IEND'))).toBe(true);
  });

  it('rejects a PNG whose chunk length overruns the file', async () => {
    const bad = Buffer.concat([SIG, Buffer.from([0xff, 0xff, 0xff, 0x00]), Buffer.from('IDAT', 'latin1')]);
    await expect(strip(bad, 'image/png')).rejects.toThrow(MetadataStripError);
  });
});

describe('WebP', () => {
  function riff(chunks: Buffer): Buffer {
    const head = Buffer.alloc(12);
    head.write('RIFF', 0, 'latin1');
    head.writeUInt32LE(4 + chunks.length, 4);
    head.write('WEBP', 8, 'latin1');
    return Buffer.concat([head, chunks]);
  }
  function ch(fourcc: string, data: Buffer): Buffer {
    const head = Buffer.alloc(8);
    head.write(fourcc, 0, 'latin1');
    head.writeUInt32LE(data.length, 4);
    const pad = data.length % 2 ? Buffer.alloc(1) : Buffer.alloc(0);
    return Buffer.concat([head, data, pad]);
  }

  it('drops EXIF/XMP chunks, clears the VP8X flags and fixes the RIFF size', async () => {
    // VP8X flags byte with EXIF (bit3) and XMP (bit2) set.
    const vp8x = Buffer.concat([Buffer.from([0x0c]), Buffer.alloc(9, 0)]);
    const input = riff(Buffer.concat([
      ch('VP8X', vp8x),
      ch('VP8 ', Buffer.from('PIXELDATA', 'latin1')),
      ch('EXIF', Buffer.from('GPSDATA-33.8688', 'latin1')),
      ch('XMP ', Buffer.from('<x:xmpmeta/>', 'latin1')),
    ]));
    const out = await strip(input, 'image/webp');

    expect(out.includes(Buffer.from('GPSDATA'))).toBe(false);
    expect(out.includes(Buffer.from('xmpmeta'))).toBe(false);
    expect(out.includes(Buffer.from('PIXELDATA'))).toBe(true);
    // Flags byte no longer advertises metadata that is no longer there.
    expect(out[20]! & 0x0c).toBe(0);
    // RIFF size matches the actual remaining payload.
    expect(out.readUInt32LE(4)).toBe(out.length - 8);
  });
});

describe('GIF', () => {
  it('drops comment/XMP blocks but keeps frames and the loop extension', async () => {
    const input = Buffer.concat([
      Buffer.from('GIF89a', 'latin1'),
      Buffer.from([0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]), // screen descriptor, no GCT
      // Comment extension — dropped.
      Buffer.from([0x21, 0xfe, 0x0b]), Buffer.from('shot by bob', 'latin1'), Buffer.from([0x00]),
      // NETSCAPE loop extension — kept.
      Buffer.from([0x21, 0xff, 0x0b]), Buffer.from('NETSCAPE2.0', 'latin1'),
      Buffer.from([0x03, 0x01, 0x00, 0x00, 0x00]),
      // XMP application extension — dropped.
      Buffer.from([0x21, 0xff, 0x0b]), Buffer.from('XMP DataXMP', 'latin1'),
      Buffer.from([0x04]), Buffer.from('GPSX', 'latin1'), Buffer.from([0x00]),
      // Image frame — kept.
      Buffer.from([0x2c, 0, 0, 0, 0, 0x01, 0x00, 0x01, 0x00, 0x00]),
      Buffer.from([0x02, 0x02]), Buffer.from('AB', 'latin1'), Buffer.from([0x00]),
      Buffer.from([0x3b]),
    ]);
    const out = await strip(input, 'image/gif');

    expect(out.includes(Buffer.from('shot by bob'))).toBe(false);
    expect(out.includes(Buffer.from('GPSX'))).toBe(false);
    expect(out.includes(Buffer.from('NETSCAPE2.0'))).toBe(true);
    expect(out.subarray(0, 6).toString('latin1')).toBe('GIF89a');
    expect(out[out.length - 1]).toBe(0x3b);
  });
});
