// The check whose absence caused the bug this catalog exists to fix.
//
// The alert-type list used to live in nine hand-maintained copies across three
// languages with nothing comparing them. They drifted, the backend started
// rejecting keys the dashboard was offering, and every preset save came back
// bad_request. These tests assert the catalog stays the only definition.

import { describe, it, expect } from 'vitest';
import { readFileSync, existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  ALERT_TYPES,
  ALERT_TYPE_DEFS,
  ALERT_PROVIDERS,
  catalog,
  canonicalAlertType,
  alertTypeDef,
  catalogForClient,
} from '../../../src/services/alertCatalog.js';

/** Repo root — walk up until shared/alert-catalog.json is there. */
function repoRoot(): string {
  let dir = dirname(fileURLToPath(import.meta.url));
  for (let i = 0; i < 8; i += 1) {
    if (existsSync(join(dir, 'shared', 'alert-catalog.json'))) return dir;
    dir = dirname(dir);
  }
  throw new Error('repo root not found');
}
const ROOT = repoRoot();

describe('catalog integrity', () => {
  it('every type names a provider that exists', () => {
    const providers = new Set(ALERT_PROVIDERS.map((p) => p.key));
    for (const t of ALERT_TYPE_DEFS) {
      expect(providers, `${t.key} -> ${t.provider}`).toContain(t.provider);
    }
  });

  it('has no duplicate keys, and no alias shadowing a real key', () => {
    const keys = ALERT_TYPES;
    expect(new Set(keys).size).toBe(keys.length);
    for (const t of ALERT_TYPE_DEFS) {
      for (const a of t.aliases ?? []) {
        expect(keys, `alias ${a} shadows a real key`).not.toContain(a);
      }
    }
  });

  it('every severityScale reference resolves', () => {
    for (const t of ALERT_TYPE_DEFS) {
      if (t.severityScale) {
        expect(Object.keys(catalog.severityScales)).toContain(t.severityScale);
      }
    }
  });

  it('types sharing an upstream feed all carry a discriminator', () => {
    // Otherwise one type silently swallows another's records — which is how
    // Victoria's SES, EMV and ESTA sat ingested but unalertable.
    const byEndpoint = new Map<string, typeof ALERT_TYPE_DEFS>();
    for (const t of ALERT_TYPE_DEFS) {
      if (!t.endpoint) continue;
      const list = byEndpoint.get(t.endpoint) ?? [];
      byEndpoint.set(t.endpoint, [...list, t] as typeof ALERT_TYPE_DEFS);
    }
    for (const [endpoint, group] of byEndpoint) {
      if (group.length < 2) continue;
      for (const t of group) {
        expect(t.discriminator, `${t.key} shares ${endpoint} but has none`).toBeTruthy();
      }
      // ...and they must not all select the same thing.
      const fingerprints = group.map((t) => JSON.stringify(t.discriminator));
      expect(new Set(fingerprints).size, `${endpoint} discriminators collide`).toBe(
        group.length,
      );
    }
  });

  it('every type a bespoke checker does not own has an endpoint', () => {
    for (const t of ALERT_TYPE_DEFS) {
      if (t.extract === 'bespoke') expect(t.endpoint).toBeNull();
      else expect(t.endpoint, `${t.key} has no endpoint`).toBeTruthy();
    }
  });
});

describe('canonical folding', () => {
  it('accepts every canonical key', () => {
    for (const k of ALERT_TYPES) expect(canonicalAlertType(k)).toBe(k);
  });

  it('folds the retired state-named keys to their agency-named form', () => {
    expect(canonicalAlertType('qld_warning')).toBe('qfd_warning');
    expect(canonicalAlertType('wa_warning')).toBe('dfes_warning');
  });

  it('rejects an unknown key rather than guessing', () => {
    expect(canonicalAlertType('not_a_type')).toBeNull();
    expect(alertTypeDef('not_a_type')).toBeUndefined();
  });

  it('resolves an alias to the real definition', () => {
    expect(alertTypeDef('qld_warning')?.key).toBe('qfd_warning');
  });
});

describe('the shape the dashboard renders from', () => {
  it('nests every type under its provider, losing none', () => {
    const client = catalogForClient();
    const flat = client.providers.flatMap((p) => p.types.map((t) => t.key));
    expect(flat.sort()).toEqual([...ALERT_TYPES].sort());
  });
});

describe('no hardcoded alert-type list has come back', () => {
  // Each of these once declared its own copy. A reintroduced literal is the
  // exact failure mode that produced the bad_request, so it fails the build.
  const read = (rel: string) => readFileSync(resolve(ROOT, rel), 'utf8');

  it('dashboard.ts has no ALERT_TYPES literal', () => {
    const src = read('backends/node/src/api/dashboard.ts');
    expect(src).not.toMatch(/const ALERT_TYPES\s*:?[^=]*=\s*\[/);
  });

  it('bot.py reads the catalog instead of declaring a dict', () => {
    const src = read('discord-bot/bot.py');
    expect(src).toMatch(/ALERT_TYPES\s*=\s*alert_catalog\.LABELS/);
    expect(src).not.toMatch(/^ALERT_TYPES\s*=\s*\{/m);
  });

  it('dashboard.html fetches the catalog instead of declaring PROVIDERS', () => {
    const src = read('dashboard.html');
    expect(src).toMatch(/loadAlertCatalog/);
    expect(src).not.toMatch(/const PROVIDERS\s*=\s*\[\s*\n\s*\{\s*key:/);
  });

  it('embeds.py derives its three maps', () => {
    const src = read('discord-bot/embeds.py');
    expect(src).toMatch(/_ALERT_TYPE_LABELS = dict\(alert_catalog\.LABELS\)/);
    expect(src).toMatch(/alert_catalog\.TYPE_DEFS/);
  });

  it('every key the bot could alert on is one the API would accept', () => {
    // Same file, so this can only fail if a consumer starts filtering — but
    // that is precisely the drift worth catching early.
    const py = read('discord-bot/alert_catalog.py');
    expect(py).toMatch(/LABELS: Dict\[str, str\] = \{\s*t\['key'\]: t\['botLabel'\]/);
  });
});
