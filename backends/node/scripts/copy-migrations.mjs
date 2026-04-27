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
