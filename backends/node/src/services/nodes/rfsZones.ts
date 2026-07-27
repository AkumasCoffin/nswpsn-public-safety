/**
 * NSW RFS zone list — the coarse "zone" a feeder node declares for its location.
 *
 * Source of truth is assets/rfs-zones.json, generated from the repo's RFS zone
 * CSVs by scripts/build-rfs-zones.mjs (grouped by Area Command). Loaded + cached
 * once here; used to (a) serve the picker dropdown via GET /api/feeder/zones and
 * (b) validate a submitted zone name on create / location-save.
 */
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { log } from '../../lib/log.js';

export interface ZoneGroup {
  areaCommand: string;
  zones: { name: string; districts: string }[];
}

const HERE = path.dirname(fileURLToPath(import.meta.url));
// dist/services/nodes → dist/assets (prod build), then src/services/nodes →
// <backend>/assets (dev via tsx). Mirrors node-updates.ts asset resolution.
const CANDIDATES = [
  path.resolve(HERE, '../../assets/rfs-zones.json'),
  path.resolve(HERE, '../../../assets/rfs-zones.json'),
];

let groupsCache: ZoneGroup[] | null = null;
let validCache: Set<string> | null = null;

function load(): ZoneGroup[] {
  if (groupsCache) return groupsCache;
  for (const p of CANDIDATES) {
    try {
      const groups = JSON.parse(readFileSync(p, 'utf8')) as ZoneGroup[];
      groupsCache = groups;
      validCache = new Set(groups.flatMap((g) => g.zones.map((z) => z.name)));
      log.info({ groups: groups.length, zones: validCache.size }, 'loaded RFS zone list');
      return groups;
    } catch {
      /* try next candidate */
    }
  }
  log.error(`rfs-zones.json not found in: ${CANDIDATES.join(', ')}`);
  groupsCache = [];
  validCache = new Set();
  return groupsCache;
}

/** The zone list grouped by Area Command, for the picker dropdown. */
export function getZoneGroups(): ZoneGroup[] {
  return load();
}

/** Whether `name` is a known RFS zone (exact match). */
export function isValidZone(name: string): boolean {
  load();
  return validCache!.has(name);
}
