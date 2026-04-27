/**
 * Unit tests for LiveStore — purely in-memory and disk semantics, no DB.
 *
 * Each test gets its own tmp directory so they don't step on each
 * other or persist anything across runs.
 */
import { describe, it, expect } from 'vitest';
import { mkdtemp, readFile, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { LiveStore } from '../../src/store/live.js';

async function freshStore(): Promise<{ store: LiveStore; dir: string }> {
  const dir = await mkdtemp(join(tmpdir(), 'livestore-test-'));
  return { store: new LiveStore(dir), dir };
}

describe('LiveStore', () => {
  it('round-trips a snapshot in memory', async () => {
    const { store } = await freshStore();
    store.set('rfs', { items: [{ id: 1 }] });
    const snap = store.get('rfs');
    expect(snap).not.toBeNull();
    expect(snap?.data).toEqual({ items: [{ id: 1 }] });
    expect(typeof snap?.ts).toBe('number');
  });

  it('keys() returns all sources', async () => {
    const { store } = await freshStore();
    store.set('a', 1);
    store.set('b', 2);
    expect(store.keys().sort()).toEqual(['a', 'b']);
  });

  it('get returns null for unknown source', async () => {
    const { store } = await freshStore();
    expect(store.get('never-set')).toBeNull();
  });

  it('persists dirty entries to disk and hydrates them back', async () => {
    const { store, dir } = await freshStore();
    store.set('waze', { alerts: ['a'] });
    store.set('rfs', { items: ['b'] });

    const result = await store.persistDirty();
    expect(result.errors).toBe(0);
    expect(result.written).toBe(2);

    const files = (await readdir(dir)).sort();
    expect(files).toEqual(['rfs.json', 'waze.json']);

    // Spin up a fresh store on the same dir; it should hydrate.
    const fresh = new LiveStore(dir);
    const hyd = await fresh.hydrateFromDisk();
    expect(hyd.loaded).toBe(2);
    expect(hyd.failed).toBe(0);
    expect(fresh.get<{ alerts: string[] }>('waze')?.data.alerts).toEqual(['a']);
    expect(fresh.get<{ items: string[] }>('rfs')?.data.items).toEqual(['b']);
  });

  it('persistDirty clears the dirty set so the next call is a no-op', async () => {
    const { store } = await freshStore();
    store.set('a', 1);
    const first = await store.persistDirty();
    expect(first.written).toBe(1);
    const second = await store.persistDirty();
    expect(second.written).toBe(0);
  });

  it('files written are valid JSON with ts and data fields', async () => {
    const { store, dir } = await freshStore();
    store.set('bom', { warnings: 3 });
    await store.persistDirty();
    const raw = await readFile(join(dir, 'bom.json'), 'utf8');
    const parsed = JSON.parse(raw) as { ts: number; data: unknown };
    expect(typeof parsed.ts).toBe('number');
    expect(parsed.data).toEqual({ warnings: 3 });
  });

  it('skips malformed files during hydration without crashing', async () => {
    const { store, dir } = await freshStore();
    // Create a valid + invalid pair.
    store.set('good', 1);
    await store.persistDirty();
    // Drop a bogus file alongside.
    await import('node:fs/promises').then((fs) =>
      fs.writeFile(join(dir, 'bad.json'), 'not json', 'utf8'),
    );
    const fresh = new LiveStore(dir);
    const hyd = await fresh.hydrateFromDisk();
    expect(hyd.loaded).toBe(1);
    expect(hyd.failed).toBe(1);
    expect(fresh.get('good')).not.toBeNull();
    expect(fresh.get('bad')).toBeNull();
  });
});
