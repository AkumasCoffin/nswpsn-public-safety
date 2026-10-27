/**
 * Write backends/node/openapi.json — the API description — from the app's own
 * routes (src/api/v1/openapi.ts builds it). Run after changing anything on
 * /api/v1/: a route, its describeRoute() text, its zod schemas, its scope:
 *
 *   npm run openapi
 *
 * and commit the file with the change. test/unit/api/openapi.test.ts fails while
 * the committed file differs from what this would write.
 *
 * Nothing here touches the database or the network: the app is built, never
 * served. src/config.ts refuses to load without its three secrets, so the
 * generator gives it placeholders (as vitest.config.ts does) rather than
 * reading .env — the description must not depend on whose machine made it.
 */
import { writeFileSync } from 'node:fs';

process.env['NSWPSN_API_KEY'] = 'openapi-generator';
process.env['BROWSER_TOKEN_SECRET'] = 'openapi-generator';
process.env['SUPABASE_JWT_SECRET'] = 'openapi-generator-0123456789';
process.env['LOG_LEVEL'] = 'warn';

const { createApp } = await import('../src/server.js');
const { buildSpec, serialiseSpec, SPEC_FILE } = await import('../src/api/v1/openapi.js');

const spec = await buildSpec(createApp());
writeFileSync(SPEC_FILE, serialiseSpec(spec));
const ops = Object.values(spec.paths).reduce((n, item) => n + Object.keys(item).length, 0);
console.log(`wrote ${SPEC_FILE} (${ops} operations)`);
// Some modules start timers when imported; this script is done.
process.exit(0);
