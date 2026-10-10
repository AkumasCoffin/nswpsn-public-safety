/**
 * Revoke a named API key by its prefix (the part between `ak_` and the next
 * underscore; `issue-api-key.ts --list` shows them). Takes effect within a
 * minute — the gate caches resolutions that long.
 *
 * Usage (from backends/node):
 *   npx tsx --env-file=../.env scripts/revoke-api-key.ts --prefix <prefix>
 */
import { closePool } from '../src/db/pool.js';
import { revokeApiKey } from '../src/services/auth/apiKeys.js';

const i = process.argv.indexOf('--prefix');
const prefix = i >= 0 ? process.argv[i + 1] : undefined;

async function main(): Promise<void> {
  if (!prefix) {
    console.error('usage: revoke-api-key.ts --prefix <prefix>');
    process.exitCode = 2;
    return;
  }
  const done = await revokeApiKey(prefix);
  console.log(done ? `revoked ${prefix}` : `no active key with prefix ${prefix}`);
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => closePool());
