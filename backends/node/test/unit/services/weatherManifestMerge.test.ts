/**
 * The manifest merge is serialized and reads the CURRENT manifest.
 *
 * Land, marine, flood and air refresh concurrently and each takes minutes.
 * Each used to read the manifest when it started and write
 * `{...thatStaleBase, itsOwnSection}` when it finished — so whoever finished
 * last erased everyone who finished during its run. Production run, first
 * self-hosted deploy: marine refreshed at 12:28, air (which had read the
 * manifest before that) wrote at 12:30, and marine's section was gone —
 * waves, swell and currents dead on the map with all their grids on disk.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const stateDir = mkdtempSync(join(tmpdir(), 'wx-manifest-'));
vi.mock('../../../src/config.js', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../../../src/config.js')>();
  return { ...mod, config: { ...mod.config, STATE_DIR: stateDir } };
});

const {
  mergeManifest, readManifest, writeManifest,
} = await import('../../../src/services/weatherStore.js');
type WeatherManifest = NonNullable<Awaited<ReturnType<typeof readManifest>>>;

function landManifest(): WeatherManifest {
  return {
    issuedAt: new Date().toISOString(),
    geometry: { west: 112, south: -44, stepDeg: 0.5, cols: 85, rows: 69 },
    timesteps: ['2026-10-06T00:00:00Z'],
    vars: [{ name: 'temperature_2m', scale: 10, unit: '°C', marine: false }],
    nodata: -32768,
  } as WeatherManifest;
}

beforeEach(async () => {
  await writeManifest(landManifest());
});

describe('mergeManifest', () => {
  it('a late writer with a stale base does not erase an earlier merge', async () => {
    // Air reads its base FIRST (as the real source does)...
    const airStaleBase = (await readManifest())!;

    // ...then marine finishes and merges its section...
    await mergeManifest((current) => ({
      ...(current ?? landManifest()),
      marineGeometry: { west: 112, south: -44, stepDeg: 1, cols: 43, rows: 35 },
      marineTimesteps: ['2026-10-06T00:00:00Z'],
      vars: [
        ...(current ?? landManifest()).vars.filter((v) => !v.marine),
        { name: 'wave_height', scale: 100, unit: 'm', marine: true },
      ],
    }));

    // ...and air merges AFTER, the way the production race played out. The
    // mutator receives the CURRENT manifest; building on it instead of the
    // stale base is what preserves marine.
    const final = await mergeManifest((current) => {
      const b = current ?? airStaleBase;
      return {
        ...b,
        airGeometry: { west: 112, south: -44, stepDeg: 1.5, cols: 29, rows: 23 },
        airTimesteps: ['2026-10-06T00:00:00Z'],
        vars: [
          ...b.vars.filter((v) => (v as { air?: boolean }).air !== true),
          { name: 'pm2_5', scale: 10, unit: 'µg/m³', marine: false, air: true } as never,
        ],
      };
    });

    expect(final.marineGeometry).toBeDefined();
    expect(final.airGeometry).toBeDefined();
    expect(final.vars.some((v) => v.name === 'wave_height')).toBe(true);
    expect(final.vars.some((v) => v.name === 'pm2_5')).toBe(true);

    const onDisk = await readManifest();
    expect(onDisk?.marineGeometry).toBeDefined();
    expect(onDisk?.airGeometry).toBeDefined();
  });

  it('concurrent merges serialize: both sections land', async () => {
    // Fired together with no await between them — the queue must order them.
    const [a, b] = await Promise.all([
      mergeManifest((current) => ({
        ...(current ?? landManifest()),
        floodGeometry: { west: 112, south: -44, stepDeg: 1, cols: 43, rows: 35 },
        floodTimesteps: ['2026-10-06'],
      })),
      mergeManifest((current) => ({
        ...(current ?? landManifest()),
        airGeometry: { west: 112, south: -44, stepDeg: 1.5, cols: 29, rows: 23 },
        airTimesteps: ['2026-10-06T00:00:00Z'],
      })),
    ]);
    expect(a).toBeDefined();
    expect(b).toBeDefined();
    const final = await readManifest();
    expect(final?.floodGeometry).toBeDefined();
    expect(final?.airGeometry).toBeDefined();
  });

  it('a throwing mutator does not jam the queue', async () => {
    await expect(mergeManifest(() => { throw new Error('boom'); })).rejects.toThrow('boom');
    const after = await mergeManifest((current) => ({
      ...(current ?? landManifest()),
      issuedAt: '2026-10-06T13:00:00.000Z',
    }));
    expect(after.issuedAt).toBe('2026-10-06T13:00:00.000Z');
  });
});
