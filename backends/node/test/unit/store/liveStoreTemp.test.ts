/**
 * LiveStore's on-disk temp-file handling, against a real temp directory.
 *
 * Motivated by a production state/ directory holding ~90 orphaned
 * `<source>.json.<pid>.tmp` files — one per source per process death, each a
 * full snapshot. They accumulate because hydrate only ever reads `.json`, so
 * nothing had reason to look at them.
 *
 * The risk in sweeping them is deleting the wrong thing, so that's what these
 * pin down: real snapshots survive, and only the pid-stamped temps go.
 */
import { describe, it, expect, beforeEach, afterAll } from 'vitest';
import { mkdtempSync, writeFileSync, readdirSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { LiveStore } from '../../../src/store/live.js';

const DIR = mkdtempSync(path.join(tmpdir(), 'nswpsn-livestore-'));
afterAll(() => { try { rmSync(DIR, { recursive: true, force: true }); } catch { /* ignore */ } });

const write = (name: string, body: string) => writeFileSync(path.join(DIR, name), body);
const snap = (data: unknown) => JSON.stringify({ ts: 1_700_000_000, data });

beforeEach(() => {
  for (const f of readdirSync(DIR)) rmSync(path.join(DIR, f), { force: true });
});

describe('LiveStore.hydrateFromDisk — orphaned temp sweep', () => {
  it('removes pid-stamped temps and keeps the real snapshots', async () => {
    write('waze.json', snap({ alerts: [1, 2] }));
    write('pager.json', snap({ hits: [] }));
    write('waze.json.10328.tmp', snap({ alerts: ['stale'] }));
    write('waze.json.442512.tmp', snap({ alerts: ['stale'] }));
    write('traffic_cameras.json.87768.tmp', snap({}));

    const store = new LiveStore(DIR);
    const res = await store.hydrateFromDisk();

    expect(res.loaded).toBe(2);
    const left = readdirSync(DIR).sort();
    expect(left).toEqual(['pager.json', 'waze.json']);
    // The live data survived the sweep intact.
    expect(store.get('waze')).toEqual({ ts: 1_700_000_000, data: { alerts: [1, 2] } });
  });

  it('leaves anything that is not a pid-stamped temp alone', async () => {
    write('waze.json', snap({ ok: true }));
    // Near-misses that must survive: no pid, non-numeric pid, and a .json
    // file that merely contains "tmp" in its name.
    write('waze.json.tmp', 'x');
    write('waze.json.abc.tmp', 'x');
    write('tmp_notes.json', snap({ ok: true }));
    write('backup.json.bak', 'x');

    await new LiveStore(DIR).hydrateFromDisk();

    const left = readdirSync(DIR).sort();
    expect(left).toEqual(['backup.json.bak', 'tmp_notes.json', 'waze.json', 'waze.json.abc.tmp', 'waze.json.tmp']);
  });

  it('is a no-op on a clean directory', async () => {
    write('waze.json', snap({ ok: true }));
    const res = await new LiveStore(DIR).hydrateFromDisk();
    expect(res.loaded).toBe(1);
    expect(readdirSync(DIR)).toEqual(['waze.json']);
  });

  it('does not leave a temp behind on a successful persist', async () => {
    const store = new LiveStore(DIR);
    store.set('waze', { alerts: [1] });
    const out = await store.persistDirty();
    expect(out.written).toBe(1);
    expect(out.errors).toBe(0);
    expect(readdirSync(DIR)).toEqual(['waze.json']);
    expect(existsSync(path.join(DIR, `waze.json.${process.pid}.tmp`))).toBe(false);
  });
});
