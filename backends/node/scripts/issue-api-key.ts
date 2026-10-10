/**
 * Issue a named API key to a user. Owner tool, run on the server until the
 * panel grows a keys page.
 *
 * Usage (from backends/node):
 *   npx tsx --env-file=../.env scripts/issue-api-key.ts --user <supabase uid> --name "<label>" [--rpm 120] [--expires 2027-01-01] [--scopes read]
 *   npx tsx --env-file=../.env scripts/issue-api-key.ts --list [--user <uid>]
 *
 * The plaintext is printed ONCE and is not stored — hand it over and close
 * the terminal. Keys work from scripts and the command line only; the gate
 * refuses them from web pages (see services/auth/apiKeys.ts).
 */
import { closePool } from '../src/db/pool.js';
import { createApiKey, listApiKeys } from '../src/services/auth/apiKeys.js';

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : undefined;
}
const flag = (name: string): boolean => process.argv.includes(`--${name}`);

async function main(): Promise<void> {
  if (flag('list')) {
    const rows = await listApiKeys(arg('user'));
    if (rows.length === 0) {
      console.log('no keys');
      return;
    }
    for (const r of rows) {
      const state = r.revoked_at ? 'REVOKED' : r.expires_at && new Date(r.expires_at) < new Date() ? 'EXPIRED' : 'active';
      console.log(
        `${r.prefix}  ${state.padEnd(7)}  ${r.user_id}  "${r.name}"  ${r.rate_limit_per_min}/min  scopes=${r.scopes.join(',')}  last used ${r.last_used_at ?? 'never'}`,
      );
    }
    return;
  }

  const userId = arg('user');
  const name = arg('name');
  if (!userId || !name) {
    console.error('usage: issue-api-key.ts --user <uid> --name "<label>" [--rpm N] [--expires YYYY-MM-DD] [--scopes a,b]');
    process.exitCode = 2;
    return;
  }
  const rpm = arg('rpm') ? Number(arg('rpm')) : undefined;
  if (rpm !== undefined && !(Number.isInteger(rpm) && rpm > 0)) {
    console.error('--rpm must be a positive integer');
    process.exitCode = 2;
    return;
  }
  const expiresRaw = arg('expires');
  const expiresAt = expiresRaw ? new Date(expiresRaw) : null;
  if (expiresAt && Number.isNaN(expiresAt.getTime())) {
    console.error('--expires must be a date, e.g. 2027-01-01');
    process.exitCode = 2;
    return;
  }
  const scopes = arg('scopes')?.split(',').map((s) => s.trim()).filter(Boolean);

  const { plaintext, row } = await createApiKey({ userId, name, rateLimitPerMin: rpm, expiresAt, scopes });
  console.log('');
  console.log(`API key issued to ${userId} ("${name}") — prefix ${row.prefix}, ${row.rate_limit_per_min}/min${row.expires_at ? `, expires ${row.expires_at}` : ''}`);
  console.log('');
  console.log(`  ${plaintext}`);
  console.log('');
  console.log('This is the only time it is shown. Use it as:');
  console.log('  curl -H "Authorization: Bearer <key>" https://api.forcequit.xyz/api/rfs/incidents');
  console.log('It is refused from web pages by design.');
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => closePool());
