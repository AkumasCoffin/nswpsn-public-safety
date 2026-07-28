// Production dependency audit with a documented allowlist for false-positive
// advisories. Replaces a bare `npm audit --omit=dev --audit-level=high`, which
// cannot ignore a single bogus advisory and so fails CI on issues that are not
// real. We fail on ANY high/critical advisory EXCEPT those explicitly listed
// below (each with a reason + review note). A brand-new high advisory is never
// silently allowed — it must be added here deliberately.
import { execSync } from 'node:child_process';

/**
 * GHSA id -> why it's allowlisted. Keep this list SHORT and revisit it: an
 * entry is a promise that we've verified the finding is not exploitable here.
 */
const ALLOWLIST = {
  // brace-expansion DoS (GHSA-mh99-v99m-4gvg). The advisory's vulnerable range
  // is published as `<=5.0.7`, but brace-expansion never shipped past 4.x, so
  // the range flags EVERY version — including the already-patched ones. Our
  // tree resolves 1.1.16 and 2.1.2, both ABOVE the real fixed versions
  // (1.1.12 / 2.0.2), i.e. not actually vulnerable. npm's only "fix" is a
  // semver-major downgrade of gtfs-realtime-bindings to 0.0.6, which we will
  // not do. Remove this entry once GitHub corrects the advisory range.
  'GHSA-mh99-v99m-4gvg': 'brace-expansion: malformed advisory range flags already-patched versions',
};

const HIGH = new Set(['high', 'critical']);

let raw;
try {
  // npm audit exits non-zero when vulnerabilities exist — capture stdout anyway.
  raw = execSync('npm audit --omit=dev --json', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] });
} catch (err) {
  raw = err.stdout ? String(err.stdout) : '';
}
if (!raw.trim()) {
  console.error('audit-check: no output from `npm audit` — treating as failure.');
  process.exit(1);
}

const data = JSON.parse(raw);
const advisories = new Map(); // GHSA id -> title
for (const vuln of Object.values(data.vulnerabilities ?? {})) {
  for (const via of vuln.via ?? []) {
    if (via && typeof via === 'object' && via.url && HIGH.has(via.severity)) {
      const m = String(via.url).match(/GHSA-[0-9a-z-]+/i);
      advisories.set(m ? m[0] : String(via.url), via.title || '');
    }
  }
}

const offenders = [...advisories.entries()].filter(([id]) => !(id in ALLOWLIST));
const allowed = [...advisories.entries()].filter(([id]) => id in ALLOWLIST);

if (allowed.length) {
  console.log('Allowlisted (documented false-positives), NOT failing the build:');
  for (const [id, title] of allowed) console.log(`  - ${id}: ${title}`);
}

if (offenders.length) {
  console.error('\n::error::High/critical advisories not on the allowlist:');
  for (const [id, title] of offenders) console.error(`  - ${id}: ${title}`);
  process.exit(1);
}

console.log(`\nOK: no un-allowlisted high/critical production advisories (${allowed.length} allowlisted).`);
