/**
 * The globe-history gap fill: reading adsb.lol's daily tar, keeping what is
 * Australian, and adding to a stored minute only what it was missing.
 */
import { describe, it, expect } from 'vitest';
import { tarEntries } from '../../../src/lib/tarEntries.js';
import {
  addTrace,
  buildHourRows,
  fillFromHistory,
  mightTouchAustralia,
  traceFileHex,
  NEAR_LIVE_MS,
  type HistoryDay,
} from '../../../src/services/adsbGlobeHistoryImport.js';
import { candidateDays, releaseAssets } from '../../../src/services/adsbGlobeHistory.js';

const DAY = Date.UTC(2026, 10, 9); // 2026-11-09
const MIN = 60_000;

function header(name: string, size: number, type = '0', prefix = ''): Buffer {
  const h = Buffer.alloc(512);
  h.write(name, 0, 'utf8');
  h.write(size.toString(8).padStart(11, '0') + '\0', 124, 'ascii');
  h.write(type, 156, 'ascii');
  h.write('ustar\0', 257, 'ascii');
  h.write('00', 263, 'ascii');
  if (prefix) h.write(prefix, 345, 'utf8');
  return h;
}
const pad = (b: Buffer) => Buffer.concat([b, Buffer.alloc((512 - (b.length % 512)) % 512)]);
function entry(name: string, body: Buffer, type = '0', prefix = ''): Buffer {
  return Buffer.concat([header(name, body.length, type, prefix), pad(body)]);
}
async function* chunked(b: Buffer, size: number): AsyncGenerator<Uint8Array> {
  for (let i = 0; i < b.length; i += size) yield b.subarray(i, i + size);
}
async function collect(b: Buffer, want: (n: string) => boolean, chunk = 333) {
  const out: Array<{ name: string; text: string }> = [];
  for await (const f of tarEntries(chunked(b, chunk), want)) out.push({ name: f.name, text: f.data.toString() });
  return out;
}

describe('tarEntries', () => {
  it('yields wanted files across chunk boundaries and skips the rest', async () => {
    const longName = `./traces/aa/${'x'.repeat(120)}.json`;
    const pax = Buffer.from(`${('path=' + './traces/bb/from_pax.json').length + 4} path=./traces/bb/from_pax.json\n`);
    const tar = Buffer.concat([
      entry('./', Buffer.alloc(0), '5'),
      entry('./heatmap/13.bin.ttf', Buffer.alloc(5000, 7)),
      entry('./traces/3c/trace_full_7c6b3c.json', Buffer.from('{"a":1}')),
      entry('././@LongLink', Buffer.from(longName + '\0'), 'L'),
      entry('ignored', Buffer.from('long')),
      entry('PaxHeader', pax, 'x'),
      entry('ignored', Buffer.from('pax')),
      entry('trace_full_7c0001.json', Buffer.from('prefixed'), '0', './traces/01'),
      Buffer.alloc(1024),
    ]);
    for (const chunk of [1, 333, 512, 100_000]) {
      const files = await collect(tar, (n) => !n.includes('heatmap'), chunk);
      expect(files).toEqual([
        { name: './traces/3c/trace_full_7c6b3c.json', text: '{"a":1}' },
        { name: longName, text: 'long' },
        { name: './traces/bb/from_pax.json', text: 'pax' },
        { name: './traces/01/trace_full_7c0001.json', text: 'prefixed' },
      ]);
    }
  });

  it('reports a stream that ends inside an entry', async () => {
    const tar = entry('./traces/3c/trace_full_7c6b3c.json', Buffer.alloc(2000));
    await expect(collect(tar.subarray(0, 1200), () => true)).rejects.toThrow(/ended inside/);
  });
});

describe('trace parsing', () => {
  it('recognises trace files by name', () => {
    expect(traceFileHex('./traces/3c/trace_full_7c6b3c.json')).toBe('7c6b3c');
    expect(traceFileHex('./traces/3c/trace_full_~2a1b3c.json')).toBe('~2a1b3c');
    expect(traceFileHex('./heatmap/13.bin.ttf')).toBeNull();
    expect(traceFileHex('./traces/3c/trace_recent_7c6b3c.json')).toBeNull();
  });

  it('skips traces that never come near Australia, without parsing them', () => {
    expect(mightTouchAustralia('"trace":[ \n[17.47,22.974600,86.237488,34975,529.0]')).toBe(false);
    expect(mightTouchAustralia('"trace":[ \n[3.1,-33.946100,151.177200,2500,180.0]')).toBe(true);
    expect(mightTouchAustralia('[3.1,-12.400000,130.870000,null]')).toBe(true); // Darwin
    expect(mightTouchAustralia('[3.1,-33.9,18.6,null]')).toBe(false); // Cape Town
  });

  it('keeps Australian points of the day, with callsign carried forward', () => {
    const day: HistoryDay = new Map();
    const n = addTrace(day, '7c6b3c', {
      r: 'VH-VXA',
      t: 'B738',
      timestamp: DAY / 1000,
      trace: [
        [10.5, -33.9, 151.2, 'ground', 0, null, 0, null, { flight: 'QFA1    ' }],
        [70, -33.8, 151.3, 2500, 180, 90, 0, 0, null],
        [80, 1.3, 103.9, 35000, 480, 300, 0, 0, null], // Singapore
        [86_400.5, -33.7, 151.4, 3000, 200, 90, 0, 0, null], // next day
      ],
    }, DAY);
    expect(n).toBe(2);
    expect(day.get(DAY)?.get('7c6b3c')).toEqual({
      c: 'QFA1', r: 'VH-VXA', t: 'B738', points: [[DAY + 10_500, -33.9, 151.2, null]],
    });
    expect(day.get(DAY + MIN)?.get('7c6b3c')?.points).toEqual([[DAY + 70_000, -33.8, 151.3, 2500]]);
    expect(day.get(DAY + MIN)?.get('7c6b3c')?.c).toBe('QFA1');
  });
});

describe('fillFromHistory', () => {
  // A straight line at constant speed, one point per 2 s.
  const line = (from: number, to: number) => {
    const pts: Array<[number, number, number, number | null]> = [];
    for (let t = from; t <= to; t += 2_000) pts.push([DAY + t, -33 - t * 1e-6, 151, 1000]);
    return pts;
  };

  it('adds nothing a stored point already covers', () => {
    const stored = line(0, 58_000).filter((_, i) => i % 5 === 0); // every 10 s
    expect(fillFromHistory(stored, line(0, 58_000))).toEqual([]);
  });

  it('fills a gap, keeping clear of the stored points either side', () => {
    const stored: Array<[number, number, number, number | null]> = [
      [DAY + 0, -33, 151, 1000],
      [DAY + 59_000, -33.059, 151, 1000],
    ];
    const add = fillFromHistory(stored, line(0, 58_000));
    expect(add.length).toBeGreaterThan(0);
    for (const p of add) {
      expect(p[0] - DAY).toBeGreaterThanOrEqual(NEAR_LIVE_MS);
      expect(DAY + 59_000 - p[0]).toBeGreaterThanOrEqual(NEAR_LIVE_MS);
    }
    // The 30 s rule: a straight line still gets a point inside the gap.
    expect(add.some((p) => p[0] - DAY >= 25_000 && p[0] - DAY <= 35_000)).toBe(true);
  });

  it('keeps a turn the stored points cut across', () => {
    const stored: Array<[number, number, number, number | null]> = [
      [DAY + 0, -33, 151, 1000],
      [DAY + 40_000, -33, 151.04, 1000],
    ];
    const history: Array<[number, number, number, number | null]> = [
      [DAY + 20_000, -32.98, 151.02, 1000], // ~2 km off the straight line
    ];
    expect(fillFromHistory(stored, history)).toEqual(history);
  });

  it('reduces a minute nothing was stored for on its own', () => {
    const add = fillFromHistory([], line(0, 58_000));
    expect(add[0]![0]).toBe(DAY);
    expect(add[add.length - 1]![0]).toBe(DAY + 58_000);
    expect(add.length).toBeLessThan(5);
  });
});

describe('buildHourRows', () => {
  it('writes only aircraft-minutes with something to add, attributed to adsb.lol', () => {
    const day: HistoryDay = new Map([
      [DAY, new Map([
        ['7c6b3c', { c: 'QFA1', r: 'VH-VXA', t: 'B738', points: [[DAY + 1_000, -33.9, 151.2, 1000]] }],
        ['7c0001', { c: null, r: null, t: null, points: [[DAY + 30_000, -34.123456789, 150.9, null]] }],
      ])],
    ]) as HistoryDay;
    const stored = [{
      minute: new Date(DAY),
      aircraft: { '7c6b3c': { p: [[2_000, -33.9, 151.2, 1000]] } },
    }];
    const rows = buildHourRows(day, DAY, stored);
    expect(rows).toEqual([{
      minuteMs: DAY,
      points: 1,
      json: { '7c0001': { c: null, r: null, t: null, e: null, s: ['adsb_lol'], p: [[30_000, -34.12346, 150.9, null]] } },
    }]);
  });
});

describe('release lookup', () => {
  it('offers yesterday back a week, newest first, inside retention', () => {
    const now = DAY + 5 * 3_600_000;
    const days = candidateDays(now, 31);
    expect(days[0]).toBe(DAY - 86_400_000);
    expect(days).toHaveLength(7);
    expect(candidateDays(now, 3)).toHaveLength(2);
  });

  it("picks a day's prod release parts in order, latest re-run first", () => {
    const rel = (tag: string, names: string[]) => ({
      tag_name: tag,
      assets: names.map((n) => ({ name: n, browser_download_url: `https://x/${tag}/${n}` })),
    });
    const releases = [
      rel('v2026.11.09-planes-readsb-staging-0', ['s.tar']),
      rel('v2026.11.09-planes-readsb-prod-0', ['p0.tar.ab', 'p0.tar.aa']),
      rel('v2026.11.09-planes-readsb-prod-1', ['p1.tar.ab', 'p1.tar.aa', 'notes.txt']),
      rel('v2026.11.08-planes-readsb-prod-0', ['old.tar']),
    ];
    expect(releaseAssets(releases, DAY)).toEqual([
      'https://x/v2026.11.09-planes-readsb-prod-1/p1.tar.aa',
      'https://x/v2026.11.09-planes-readsb-prod-1/p1.tar.ab',
    ]);
    expect(releaseAssets(releases, DAY + 86_400_000)).toBeNull();
  });
});
