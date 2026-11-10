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
  mergeManifest, readManifest, writeManifest, geometryMatches, sectionIsFresh,
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

describe('geometryMatches', () => {
  // Freshness used to be age plus variable coverage, so changing a grid step
  // or bounding box in .env did nothing until the age lapsed. Every such
  // change needed a manual delete of the state directory, and a forgotten one
  // left the old resolution on screen looking like the change had failed.
  const g = { west: 112, south: -44, stepDeg: 0.1, cols: 421, rows: 341 };

  it('accepts the same grid', () => {
    expect(geometryMatches({ ...g }, g)).toBe(true);
  });

  it('rejects a changed step', () => {
    expect(geometryMatches({ ...g, stepDeg: 0.5, cols: 85, rows: 69 }, g)).toBe(false);
  });

  it('rejects a moved or widened box', () => {
    expect(geometryMatches({ ...g, west: 90 }, g)).toBe(false);
    expect(geometryMatches({ ...g, cols: 601 }, g)).toBe(false);
  });

  it('a section that was never stored does not match', () => {
    expect(geometryMatches(undefined, g)).toBe(false);
    expect(geometryMatches(null, g)).toBe(false);
  });

  it('is not fooled by float noise in the step', () => {
    expect(geometryMatches({ ...g, stepDeg: 0.1 + 1e-12 }, g)).toBe(true);
  });
});

describe('sectionIsFresh', () => {
  // Marine, flood and air used to borrow the land grid's issuedAt, so their
  // cadence was an accident of the land grid's. Flood in particular stays on
  // the PUBLIC API: tied to a three-hourly land refresh it would spend the
  // public quota eight times a day for data GloFAS publishes once.
  const HOUR = 60 * 60 * 1000;

  it('a section with no timestamp of its own is stale', () => {
    expect(sectionIsFresh(undefined, 24 * HOUR)).toBe(false);
  });

  it('is fresh inside its own window and stale outside it', () => {
    const twoHoursAgo = new Date(Date.now() - 2 * HOUR).toISOString();
    expect(sectionIsFresh(twoHoursAgo, 3 * HOUR)).toBe(true);
    expect(sectionIsFresh(twoHoursAgo, 1 * HOUR)).toBe(false);
  });

  it('flood on a daily window outlives several land refreshes', () => {
    // Fetched ten hours ago: three land cycles have passed, and it is still
    // not time to spend the public quota again.
    const tenHoursAgo = new Date(Date.now() - 10 * HOUR).toISOString();
    expect(sectionIsFresh(tenHoursAgo, 24 * HOUR)).toBe(true);
    expect(sectionIsFresh(tenHoursAgo, 3 * HOUR)).toBe(false);
  });

  it('an unparseable timestamp is stale, not fresh forever', () => {
    expect(sectionIsFresh('not a date', 24 * HOUR)).toBe(false);
  });
});
