/**
 * Read a tar stream one entry at a time, holding at most one entry in memory.
 *
 * Written for adsb.lol's daily globe_history release: a ~3.7 GB tar, split into
 * parts that are simply concatenated, of which we want the few thousand small
 * trace files that touch Australia. Extracting to disk would need the whole
 * archive's space for a minute's work; this streams it, skipping entries the
 * caller doesn't want without buffering them.
 *
 * Handles ustar (name + prefix), GNU long names ('L') and pax `path` records
 * ('x'), which is everything readsb's archive uses. Header checksums are not
 * verified: a corrupt header shows up as a nonsense size, and the caller gets
 * a truncation error instead of data.
 */

const BLOCK = 512;

export interface TarFile {
  name: string;
  data: Buffer;
}

class ByteReader {
  private bufs: Buffer[] = [];
  private len = 0;
  private done = false;

  constructor(private readonly it: AsyncIterator<Uint8Array>) {}

  private async fill(n: number): Promise<void> {
    while (this.len < n && !this.done) {
      const r = await this.it.next();
      if (r.done) {
        this.done = true;
      } else if (r.value.byteLength > 0) {
        const b = Buffer.from(r.value.buffer, r.value.byteOffset, r.value.byteLength);
        this.bufs.push(b);
        this.len += b.length;
      }
    }
  }

  /** Exactly n bytes, or null at a clean end of stream. */
  async read(n: number): Promise<Buffer | null> {
    if (n === 0) return Buffer.alloc(0);
    await this.fill(n);
    if (this.len === 0 && this.done) return null;
    if (this.len < n) throw new Error('tar: stream ended inside an entry');
    const first = this.bufs[0]!;
    if (first.length >= n) {
      const out = first.subarray(0, n);
      if (first.length === n) this.bufs.shift();
      else this.bufs[0] = first.subarray(n);
      this.len -= n;
      return out;
    }
    const parts: Buffer[] = [];
    let need = n;
    while (need > 0) {
      const f = this.bufs[0]!;
      if (f.length <= need) {
        parts.push(f);
        this.bufs.shift();
        need -= f.length;
      } else {
        parts.push(f.subarray(0, need));
        this.bufs[0] = f.subarray(need);
        need = 0;
      }
    }
    this.len -= n;
    return Buffer.concat(parts, n);
  }

  async skip(n: number): Promise<void> {
    while (n > 0) {
      if (this.len === 0) {
        await this.fill(1);
        if (this.len === 0) throw new Error('tar: stream ended inside an entry');
      }
      const f = this.bufs[0]!;
      if (f.length <= n) {
        this.bufs.shift();
        this.len -= f.length;
        n -= f.length;
      } else {
        this.bufs[0] = f.subarray(n);
        this.len -= n;
        n = 0;
      }
    }
  }
}

function cString(b: Buffer): string {
  const z = b.indexOf(0);
  return b.subarray(0, z === -1 ? b.length : z).toString('utf8');
}

function octal(b: Buffer): number {
  if ((b[0]! & 0x80) !== 0) throw new Error('tar: base-256 sizes are not supported');
  const s = cString(b).trim();
  return s === '' ? 0 : Number.parseInt(s, 8);
}

/** The `path` record of a pax header, if it has one. */
function paxPath(b: Buffer): string | null {
  let off = 0;
  while (off < b.length) {
    const sp = b.indexOf(0x20, off);
    if (sp === -1) break;
    const len = Number.parseInt(b.subarray(off, sp).toString('ascii'), 10);
    if (!Number.isFinite(len) || len <= 0) break;
    const rec = b.subarray(sp + 1, off + len - 1).toString('utf8');
    const eq = rec.indexOf('=');
    if (eq !== -1 && rec.slice(0, eq) === 'path') return rec.slice(eq + 1);
    off += len;
  }
  return null;
}

/**
 * Every regular file in the stream whose name `want` accepts, in archive
 * order. Directories, links and unwanted files are skipped unread.
 */
export async function* tarEntries(
  chunks: AsyncIterable<Uint8Array>,
  want: (name: string) => boolean,
): AsyncGenerator<TarFile> {
  const r = new ByteReader(chunks[Symbol.asyncIterator]());
  let nextName: string | null = null;
  for (;;) {
    const h = await r.read(BLOCK);
    if (!h || h.every((x) => x === 0)) return; // end of stream / end-of-archive block
    const size = octal(h.subarray(124, 136));
    const padded = Math.ceil(size / BLOCK) * BLOCK;
    const type = h[156] === 0 ? '0' : String.fromCharCode(h[156]!);

    if (type === 'L' || type === 'x') {
      const body = await r.read(padded);
      if (!body) throw new Error('tar: stream ended inside an entry');
      nextName = type === 'L' ? cString(body.subarray(0, size)) : paxPath(body.subarray(0, size));
      continue;
    }

    let name = nextName;
    nextName = null;
    if (name === null) {
      const base = cString(h.subarray(0, 100));
      const ustar = h.subarray(257, 262).toString('ascii') === 'ustar';
      const prefix = ustar ? cString(h.subarray(345, 500)) : '';
      name = prefix ? `${prefix}/${base}` : base;
    }

    if ((type === '0' || type === '7') && want(name)) {
      const body = await r.read(padded);
      if (!body) throw new Error('tar: stream ended inside an entry');
      yield { name, data: body.subarray(0, size) };
    } else {
      await r.skip(padded);
    }
  }
}
