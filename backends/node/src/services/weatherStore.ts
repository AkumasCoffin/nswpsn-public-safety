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
    const axis = v.marine ? (m.marineTimesteps ?? []) : m.timesteps;
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
export function manifestIsFresh(m: WeatherManifest | null, maxAgeMs: number): boolean {
  if (!m) return false;
  const issued = Date.parse(m.issuedAt);
  if (!Number.isFinite(issued)) return false;
  return Date.now() - issued < maxAgeMs;
}
