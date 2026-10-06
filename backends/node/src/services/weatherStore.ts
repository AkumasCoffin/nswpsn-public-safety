/**
 * On-disk store for the gridded weather field.
 *
 * WHY NOT LIVESTORE
 * Every other source hands its snapshot to LiveStore, which persists it with
 * `JSON.stringify` (store/live.ts). That is right for a few hundred features
 * and wrong for this: a full grid is 72 timesteps x 6 variables x 5,865 cells,
 * and as JSON numbers that is tens of megabytes re-serialised on every write.
 *
 * So the bulk goes to disk as raw Int16 — one file per (variable, timestep),
 * 11.7 KB each, written once per refresh and then only read. The small JSON
 * manifest describing them IS the LiveStore snapshot, so the thing that needs
 * to survive a restart does, and the thing that would be expensive to serialise
 * never goes near JSON.
 *
 * Layout under {STATE_DIR}/weather/:
 *   manifest.json                     geometry, timesteps, scales, issued-at
 *   g_<var>_<timestamp>.bin           Int16LE, cols*rows values, row-major
 *                                     from the SOUTH-WEST corner
 *   mask-<step>.bin                   land/sea mask (written by weatherMask)
 */
import { mkdir, readFile, rename, writeFile, readdir, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import type { GridGeometry, GridVar } from '../sources/weatherGrid.js';

export interface ManifestVar {
  name: GridVar;
  scale: number;
  unit: string;
  /** Ocean-only. Inland cells are NODATA, which the client must not render. */
  marine: boolean;
  /** Land-only river discharge, on a DAILY axis rather than 3-hourly. */
  flood?: boolean;
  /** Air quality, on its own coarser grid and its own axis. */
  air?: boolean;
}

export interface WeatherManifest {
  issuedAt: string;
  geometry: GridGeometry;
  /** ISO instants, 3-hourly, oldest first. Always UTC — see weatherGridSource. */
  timesteps: string[];
  vars: ManifestVar[];
  nodata: number;
  /**
   * Marine runs on its own coarser grid and its own time axis, so a marine
   * variable must be read against these rather than the land pair above.
   * Absent until the marine source has run at least once.
   */
  marineGeometry?: GridGeometry;
  marineTimesteps?: string[];
  /**
   * Flood shares marine's grid (it takes the land cells marine leaves) but has
   * its own DAILY axis, so it cannot be read against either of the pairs above.
   */
  floodGeometry?: GridGeometry;
  /**
   * When each dependent section was last fetched. They used to borrow the
   * land grid's `issuedAt`, which made their refresh cadence an accident of
   * the land grid's: a section could be refetched every tick, or never.
   */
  marineIssuedAt?: string;
  floodIssuedAt?: string;
  airIssuedAt?: string;
  floodTimesteps?: string[];
  /** Air quality: coarser grid again, 3-hourly like land but its own axis. */
  airGeometry?: GridGeometry;
  airTimesteps?: string[];
}

export function weatherDir(): string {
  return join(config.STATE_DIR, 'weather');
}

/**
 * Filesystem-safe name for one grid.
 *
 * Colons are legal in a POSIX filename and illegal on Windows, and this runs on
 * both — development here, production on Linux. Stripping them (and the
 * punctuation around them) keeps one naming scheme on every platform rather
 * than a path that works until someone runs it on the wrong machine.
 */
export function gridFileName(v: GridVar, timestep: string): string {
  const stamp = timestep.replace(/[-:]/g, '').replace(/\.\d+/, '');
  return `g_${v}_${stamp}.bin`;
}

async function ensureDir(): Promise<string> {
  const dir = weatherDir();
  await mkdir(dir, { recursive: true });
  return dir;
}

/**
 * Atomic write: temp file then rename.
 *
 * Same reasoning as LiveStore's snapshot writer — a reader must only ever see
 * the previous complete file or the new complete file, never a half-written
 * one. A truncated grid would render as a torn field rather than fail cleanly.
 */
export async function writeFileAtomic(path: string, data: Uint8Array | string): Promise<void> {
  const temp = `${path}.tmp`;
  await writeFile(temp, data);
  await rename(temp, path);
}

export async function writeGrid(v: GridVar, timestep: string, values: Int16Array): Promise<void> {
  const dir = await ensureDir();
  // Int16Array over the exact byte range — `.buffer` alone would write the
  // whole underlying ArrayBuffer if this array is ever a view into a larger one.
  const bytes = new Uint8Array(values.buffer, values.byteOffset, values.byteLength);
  await writeFileAtomic(join(dir, gridFileName(v, timestep)), bytes);
}

/** Raw bytes for one grid, or null if that (variable, timestep) was never written. */
export async function readGridBytes(v: GridVar, timestep: string): Promise<Buffer | null> {
  try {
    return await readFile(join(weatherDir(), gridFileName(v, timestep)));
  } catch {
    return null;
  }
}

export async function writeManifest(m: WeatherManifest): Promise<void> {
  const dir = await ensureDir();
  await writeFileAtomic(join(dir, 'manifest.json'), JSON.stringify(m));
}

/**
 * Serialized read-modify-write of the manifest.
 *
 * Land, marine, flood and air refresh CONCURRENTLY (prewarm runs them
 * together), each takes minutes, and each used to read the manifest at the
 * START of its run and write `{...thatStaleBase, itsOwnPart}` at the END —
 * so whoever finished last erased everyone who finished during its run. In
 * production: marine refreshed at 12:28, air (which had read the manifest
 * before that) wrote at 12:30, and the marine section was gone — waves,
 * swell and currents dead on the map with all their grids sitting on disk.
 *
 * Every manifest write goes through here: the mutator receives the manifest
 * AS IT IS NOW, under a queue that lets only one merge run at a time, and
 * returns the next manifest. A mutator must build its own section from its
 * own data and take EVERYTHING ELSE from `current`.
 */
let _mergeQueue: Promise<unknown> = Promise.resolve();

export function mergeManifest(
  mutate: (current: WeatherManifest | null) => WeatherManifest,
): Promise<WeatherManifest> {
  const run = _mergeQueue.then(async () => {
    const next = mutate(await readManifest());
    await writeManifest(next);
    return next;
  });
  // A failed merge must not jam the queue for every later writer.
  _mergeQueue = run.catch(() => {});
  return run;
}

export async function readManifest(): Promise<WeatherManifest | null> {
  try {
    const raw = await readFile(join(weatherDir(), 'manifest.json'), 'utf8');
    const m = JSON.parse(raw) as WeatherManifest;
    // A manifest with no timesteps describes nothing and would have every
    // endpoint 404 in a way that looks like a bug rather than an empty cache.
    if (!Array.isArray(m.timesteps) || m.timesteps.length === 0) return null;
    return m;
  } catch {
    return null;
  }
}

/**
 * Delete grid files no longer referenced by the manifest.
 *
 * Each refresh moves the window forward, so without this the directory grows
 * by a full dataset every day until the disk fills. Driven off the manifest
 * rather than file age: the manifest is the definition of what is live, and
 * anything else is by definition unreachable.
 */
export async function pruneGrids(m: WeatherManifest): Promise<number> {
  const dir = weatherDir();
  let files: string[];
  try {
    files = await readdir(dir);
  } catch {
    return 0;
  }

  const keep = new Set<string>();
  for (const v of m.vars) {
    // Each variable is kept on ITS OWN axis. Pruning marine against the land
    // timesteps would delete every marine grid the moment the two axes differ
    // by so much as an hour.
    // Every non-land grid MUST have a branch here. Falling through to the
    // land axis gives that variable an empty keep-set, so the next land
    // refresh deletes every one of its files and forces a same-day refetch.
    // Marine and flood each had to learn this; air nearly did too.
    const axis = v.flood ? (m.floodTimesteps ?? [])
      : v.marine ? (m.marineTimesteps ?? [])
      : v.air ? (m.airTimesteps ?? [])
      : m.timesteps;
    for (const t of axis) keep.add(gridFileName(v.name, t));
  }

  let removed = 0;
  for (const f of files) {
    if (!f.startsWith('g_') || !f.endsWith('.bin')) continue; // never touch the mask
    if (keep.has(f)) continue;
    try {
      await unlink(join(dir, f));
      removed += 1;
    } catch (err) {
      log.debug({ err: (err as Error).message, file: f }, 'weather: prune failed for one file');
    }
  }
  if (removed > 0) log.info({ removed }, 'weather: pruned grid files no longer in the manifest');
  return removed;
}

/**
 * Is the stored dataset still current?
 *
 * The poller fires a source on startup, so without this check every restart
 * would re-fetch the whole grid and re-spend a day of the Open-Meteo budget.
 * A redeploy during an incident is exactly when that must not happen.
 */
/** Is a section's own timestamp younger than `maxAgeMs`? Absent = stale. */
export function sectionIsFresh(issuedAt: string | undefined, maxAgeMs: number): boolean {
  if (!issuedAt) return false;
  const issued = Date.parse(issuedAt);
  if (!Number.isFinite(issued)) return false;
  return Date.now() - issued < maxAgeMs;
}

/**
 * Does a stored section's grid still match the grid the config asks for?
 *
 * Freshness used to be age plus variable coverage, so changing a step or a
 * bounding box in .env did nothing until the age lapsed — every such change
 * needed a manual delete of the state directory, and one that was forgotten
 * left the old resolution on screen looking like the change had failed.
 */
export function geometryMatches(
  stored: GridGeometry | undefined | null,
  wanted: GridGeometry,
): boolean {
  if (!stored) return false;
  const close = (a: number, b: number) => Math.abs(a - b) < 1e-9;
  return close(stored.west, wanted.west) && close(stored.south, wanted.south)
    && close(stored.stepDeg, wanted.stepDeg)
    && stored.cols === wanted.cols && stored.rows === wanted.rows;
}

export function manifestIsFresh(m: WeatherManifest | null, maxAgeMs: number): boolean {
  if (!m) return false;
  const issued = Date.parse(m.issuedAt);
  if (!Number.isFinite(issued)) return false;
  return Date.now() - issued < maxAgeMs;
}

/**
 * Does the stored manifest still describe the variables the code asks for?
 *
 * THE BUG THIS EXISTS FOR. Freshness used to be age alone. So when the land
 * variable list grew from six to ten and the new build went out, the refresh
 * found a six-variable manifest less than a day old, called it current, and
 * skipped. Four layers were live in the UI with no grids behind them for
 * twenty-four hours — and because a missing grid makes the client bail out
 * silently, those pills left the PREVIOUS layer on screen rather than showing
 * an error. It looked like the layers were broken, not like the cache was.
 *
 * Age is about the data going stale. This is about the SHAPE going stale, and
 * a deploy changes the shape the moment it lands.
 */
export function manifestCovers(
  m: WeatherManifest | null,
  wanted: readonly string[],
  kind: (v: ManifestVar) => boolean,
): boolean {
  if (!m) return false;
  const have = new Set(m.vars.filter(kind).map((v) => v.name as string));
  return wanted.every((w) => have.has(w));
}
