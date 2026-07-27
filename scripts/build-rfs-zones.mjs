// Build backends/node/assets/rfs-zones.json from the NSW RFS zone CSVs.
//
// Reads data/Extended/nsw-rural-fire-service/zones-<area>.csv (columns:
// "Zone ID,Zone,Districts,Channel") and emits the zone list grouped by Area
// Command, which backs the node location picker's zone dropdown and the
// backend's zone validation. Re-run this whenever the source CSVs change.
//
//   node scripts/build-rfs-zones.mjs
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const csvDir = join(root, 'data', 'Extended', 'nsw-rural-fire-service');
const outPath = join(root, 'backends', 'node', 'assets', 'rfs-zones.json');

// Area Commands in display order; slug matches the zones-<slug>.csv filename.
const AREAS = [
  ['greater-sydney', 'Greater Sydney'],
  ['hunter', 'Hunter'],
  ['north-eastern', 'North Eastern'],
  ['north-western', 'North Western'],
  ['south-eastern', 'South Eastern'],
  ['south-western', 'South Western'],
  ['western', 'Western'],
];

// Minimal CSV line parser: comma-separated, double-quoted fields may contain
// commas ("" escapes a quote). Enough for these simple reference tables.
function parseLine(line) {
  const out = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQ) {
      if (ch === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; } else inQ = false;
      } else cur += ch;
    } else if (ch === '"') inQ = true;
    else if (ch === ',') { out.push(cur); cur = ''; }
    else cur += ch;
  }
  out.push(cur);
  return out.map((s) => s.trim());
}

const groups = [];
for (const [slug, areaCommand] of AREAS) {
  const text = readFileSync(join(csvDir, `zones-${slug}.csv`), 'utf8');
  const lines = text.split(/\r?\n/).filter((l) => l.trim() !== '');
  // Drop the header row ("Zone ID,Zone,Districts,Channel").
  const zones = [];
  for (const line of lines.slice(1)) {
    const [, name, districts] = parseLine(line);
    if (name) zones.push({ name, districts: districts || '' });
  }
  groups.push({ areaCommand, zones });
}

const total = groups.reduce((n, g) => n + g.zones.length, 0);
writeFileSync(outPath, JSON.stringify(groups, null, 2) + '\n');
console.log(`wrote ${outPath}: ${groups.length} area commands, ${total} zones`);
