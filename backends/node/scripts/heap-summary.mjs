#!/usr/bin/env node
/**
 * Summarise a V8 .heapsnapshot from the command line.
 *
 *   node scripts/heap-summary.mjs state/heap-....heapsnapshot [topN]
 *
 * Why this exists: the snapshots this box produces are ~1GB, and the usual
 * route (Chrome DevTools > Memory > Load) needs several GB of RAM to open one
 * plus a copy of the file on your laptop. This reads it on the server and
 * prints the few lines that identify a leak, so the answer travels as text
 * instead of a gigabyte.
 *
 * It reports SHALLOW size and count grouped by constructor, not retained size
 * — retained size needs the dominator tree, which is what DevTools is for. In
 * practice shallow is enough to name a leak: several hundred MB of retained
 * objects shows up as an implausible count for one or two constructors.
 *
 * The file is scanned as bytes, never JSON.parse'd. A 1GB snapshot exceeds
 * V8's maximum string length, so readFile(...,'utf8') throws before parsing
 * begins, and holding the parsed structure would need more memory than the
 * process being diagnosed.
 */
import { createReadStream, statSync } from 'node:fs';
import { open } from 'node:fs/promises';

const file = process.argv[2];
if (!file) {
  console.error('usage: node scripts/heap-summary.mjs <file.heapsnapshot> [topN]');
  process.exit(1);
}
const TOP = Number(process.argv[3]) || 25;

async function head(path, n) {
  const fh = await open(path, 'r');
  try {
    const buf = Buffer.alloc(n);
    const { bytesRead } = await fh.read(buf, 0, n, 0);
    return buf.subarray(0, bytesRead).toString('latin1');
  } finally {
    await fh.close();
  }
}

/** Extract the `"snapshot":{...}` header by brace matching. */
function readHeader(text) {
  const at = text.indexOf('"snapshot"');
  if (at < 0) throw new Error('not a heapsnapshot: no "snapshot" key');
  const start = text.indexOf('{', at + 10);
  let depth = 0;
  for (let i = start; i < text.length; i++) {
    if (text[i] === '{') depth++;
    else if (text[i] === '}' && --depth === 0) return JSON.parse(text.slice(start, i + 1));
  }
  throw new Error('heapsnapshot header truncated — is the file complete?');
}

const NODES_KEY = Buffer.from('"nodes":[');
const STRINGS_KEY = Buffer.from('"strings":[');

/**
 * Single pass over the file as bytes. State persists across chunk boundaries,
 * and no byte is ever examined twice — the first version carried a tail
 * between chunks and double-counted it, which is exactly the sort of bug that
 * yields confident nonsense (780GB "total", a third of the objects missing).
 */
function scan(path, fieldCount, nameIdx, sizeIdx) {
  return new Promise((resolve, reject) => {
    const byName = new Map();
    const strings = [];

    let state = 'seekNodes';
    let match = 0;      // how much of the current key literal matched so far

    let field = 0, num = 0, inNum = false, curName = 0, curSize = 0;
    let inStr = false, esc = false, sBuf = [];

    const rs = createReadStream(path, { highWaterMark: 1 << 22 });

    rs.on('data', (buf) => {
      for (let i = 0; i < buf.length; i++) {
        const b = buf[i];

        if (state === 'seekNodes' || state === 'seekStrings') {
          const key = state === 'seekNodes' ? NODES_KEY : STRINGS_KEY;
          // Restart the match on mismatch; these literals have no self-overlap
          // beyond the leading quote, so a simple reset is correct.
          if (b === key[match]) {
            match++;
            if (match === key.length) {
              state = state === 'seekNodes' ? 'inNodes' : 'inStrings';
              match = 0;
            }
          } else {
            match = b === key[0] ? 1 : 0;
          }
          continue;
        }

        if (state === 'inNodes') {
          if (b >= 0x30 && b <= 0x39) {          // 0-9
            num = num * 10 + (b - 0x30);
            inNum = true;
            continue;
          }
          if (inNum) {
            if (field === nameIdx) curName = num;
            else if (field === sizeIdx) curSize = num;
            num = 0;
            inNum = false;
            if (++field === fieldCount) {
              const e = byName.get(curName);
              if (e) { e.count++; e.self += curSize; }
              else byName.set(curName, { count: 1, self: curSize });
              field = 0;
            }
          }
          if (b === 0x5d) { state = 'seekStrings'; }  // ]
          continue;
        }

        if (state === 'inStrings') {
          if (inStr) {
            const ch = String.fromCharCode(b);
            if (esc) { sBuf.push(ch); esc = false; }
            else if (b === 0x5c) { sBuf.push(ch); esc = true; }   // backslash
            else if (b === 0x22) { strings.push(sBuf.join('')); sBuf = []; inStr = false; }
            else sBuf.push(ch);
            continue;
          }
          if (b === 0x22) { inStr = true; continue; }             // "
          if (b === 0x5d) { state = 'done'; break; }              // ]
          continue;
        }
      }
      if (state === 'done') { rs.destroy(); resolve({ byName, strings }); }
    });

    rs.on('error', reject);
    rs.on('close', () => resolve({ byName, strings }));
  });
}

const mb = (b) => (b / 1048576).toFixed(1);

const header = readHeader(await head(file, 65536));
const fields = header.meta.node_fields;
const nameIdx = fields.indexOf('name');
const sizeIdx = fields.indexOf('self_size');
if (nameIdx < 0 || sizeIdx < 0) throw new Error('unexpected node_fields: ' + fields.join(','));

console.log(`file           ${file}  (${mb(statSync(file).size)} MB)`);
console.log(`node_count     ${(header.node_count ?? 0).toLocaleString()}`);

const { byName, strings } = await scan(file, fields.length, nameIdx, sizeIdx);

let totalSelf = 0, totalCount = 0;
for (const v of byName.values()) { totalSelf += v.self; totalCount += v.count; }

// Sanity check against the header — if these disagree the scan is wrong and
// every number below it is worthless, so say so rather than print them.
if (header.node_count && Math.abs(totalCount - header.node_count) > 1) {
  console.error(`\nWARNING: counted ${totalCount.toLocaleString()} nodes but the header declares ` +
    `${header.node_count.toLocaleString()} — the scan is unreliable, do not trust the table.`);
}

console.log(`objects        ${totalCount.toLocaleString()}`);
console.log(`shallow total  ${mb(totalSelf)} MB\n`);
console.log('constructor                                       count      self MB      %');
console.log('---------------------------------------------------------------------------');
for (const r of [...byName.entries()]
  .map(([idx, v]) => ({ name: strings[idx] ?? `<#${idx}>`, ...v }))
  .sort((a, b) => b.self - a.self)
  .slice(0, TOP)) {
  const nm = (r.name || '(anonymous)').slice(0, 45).padEnd(47);
  console.log(`${nm}${String(r.count).padStart(10)}  ${mb(r.self).padStart(9)}  ${((r.self / totalSelf) * 100).toFixed(1).padStart(5)}`);
}
console.log('\nAn implausible count for one constructor is the usual signature.');
