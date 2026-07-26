#!/usr/bin/env node
/**
 * postbuild copy of src/db/migrations/*.sql → dist/db/migrations/.
 *
 * tsc only emits .ts → .js; .sql files have to be copied manually so
 * the migrate runner can readdir() them at production runtime
 * (`dist/db/migrate.js` resolves migrations relative to its own
 * __dirname).
 *
 * Cross-platform — uses fs.cp instead of `cp -r` so Windows dev
 * environments don't choke on the path separators.
 */
import { promises as fs } from 'node:fs';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..');
const src = path.join(root, 'src', 'db', 'migrations');
const dest = path.join(root, 'dist', 'db', 'migrations');

await fs.mkdir(dest, { recursive: true });
await fs.cp(src, dest, { recursive: true, force: true });

const files = (await fs.readdir(dest)).filter((f) => f.endsWith('.sql'));
console.log(`copied ${files.length} migration file(s) to ${path.relative(root, dest)}`);

// Static assets (e.g. node-versions.json self-update manifest) live outside
// src/ and tsc doesn't emit them; copy assets/ → dist/assets/ so runtime
// reads from import.meta.url resolve in production. api/node-updates.ts also
// falls back to the source assets/ dir, which is what dev (tsx) uses.
const assetsSrc = path.join(root, 'assets');
const assetsDest = path.join(root, 'dist', 'assets');
try {
  await fs.access(assetsSrc);
  await fs.mkdir(assetsDest, { recursive: true });
  await fs.cp(assetsSrc, assetsDest, { recursive: true, force: true });
  const assetFiles = await fs.readdir(assetsDest);
  console.log(`copied ${assetFiles.length} asset file(s) to ${path.relative(root, assetsDest)}`);
} catch {
  console.log('no assets/ dir to copy (skipping)');
}
