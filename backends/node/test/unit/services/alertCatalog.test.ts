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
  severityTokens,
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

  it('folds the retired essential_planned into essential_future', () => {
    // The two overlapped almost 1:1 on the wire (planned work spans both
    // feeds; the future feed is all planned), so a guild on both got every
    // outage twice. One type remains; the old key keeps saving and alerting.
    expect(canonicalAlertType('essential_planned')).toBe('essential_future');
    expect(ALERT_TYPES).not.toContain('essential_planned');
  });
});

describe('severity scales', () => {
  it('every scale maps onto its own options or the __below__ sentinel', () => {
    // __below__ ranks a raw value UNDER the whole scale: it is never a
    // selectable floor (not an option), and a record mapped to it fails any
    // configured floor — e.g. RFS "Not Applicable".
    for (const [name, scale] of Object.entries(catalog.severityScales)) {
      const values = new Set(scale.options.map((o) => o.value));
      values.add('__below__');
      for (const [raw, token] of Object.entries(scale.map)) {
        expect(values, `${name}.map[${raw}] -> ${token}`).toContain(token);
      }
      expect(raw_is_lowercased(scale.map)).toBe(true);
      // ...and the sentinel must never leak into the floor dropdowns.
      expect(scale.options.map((o) => o.value)).not.toContain('__below__');
    }
  });

  it('gives every fire agency its own floor, not just RFS', () => {
    // The severity list named only RFS/BOM/traffic_majorevent, so the
    // interstate agencies carried an alertLevel nothing could filter on.
    const fireAgencies = [
      'rfs', 'cfa', 'deeca', 'vicses', 'emv', 'esta',
      'qfd', 'qfd_warning', 'dfes', 'dfes_warning',
      'sa_cfs', 'sa_mfs', 'nt_fire', 'nt_bushfires',
    ];
    for (const key of fireAgencies) {
      expect(alertTypeDef(key)?.severityScale, key).toBe('fire');
      expect(severityTokens('fire')).toEqual(['advice', 'watch_and_act', 'emergency']);
    }
  });

  it('does not offer a floor for types with no severity', () => {
    // traffic_majorevent is here deliberately: its old scale named
    // properties.severity, a field that exists on neither the parsed
    // snapshot nor the raw upstream — the floor never filtered anything.
    for (const k of ['ausgrid', 'endeavour_current', 'user_incident', 'wire_article', 'traffic_majorevent']) {
      expect(alertTypeDef(k)?.severityScale, k).toBeNull();
    }
  });

  it('ranks RFS "Not Applicable" below any floor', () => {
    expect(catalog.severityScales.fire?.map['not applicable']).toBe('__below__');
  });

  it('no retired waze key is still marked sub-type aware', () => {
    const aware = ALERT_TYPE_DEFS.filter((t) => t.subtypeAware).map((t) => t.key);
    expect(aware.filter((k) => k.startsWith('waze'))).toEqual([]);
  });
});

function raw_is_lowercased(map: Record<string, string>): boolean {
  // Lookups lowercase the upstream value, so an uppercase key can never match.
  return Object.keys(map).every((k) => k === k.toLowerCase());
}

describe('the shape the dashboard renders from', () => {
  it('carries the severity scales the filters card builds its dropdowns from', () => {
    const client = catalogForClient();
    expect(Object.keys(client.severityScales)).toContain('fire');
    expect(client.severityScales.fire?.options.map((o) => o.label)).toEqual([
      'Advice', 'Watch & Act', 'Emergency',
    ]);
  });

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

  it('dashboard.html derives its filter lists rather than listing them', () => {
    const src = read('dashboard.html');
    // The literals that made the interstate agencies filter-less.
    expect(src).not.toMatch(/_DASH_SEVERITY_PER_TYPE = \{[^}]*rfs:/);
    expect(src).not.toMatch(/_DASH_SUBTYPE_AWARE_TYPES = new Set\(\[[^\]]*'rfs'/);
    expect(src).toMatch(/_DASH_FILTER_SEVERITY_TYPES\.add/);
  });

  it('bot.py no longer hand-maps raw severity values', () => {
    const src = read('discord-bot/bot.py');
    expect(src).not.toMatch(/^_SEVERITY_RFS_MAP = \{/m);
    expect(src).not.toMatch(/^_SEVERITY_BOM_MAP = \{/m);
    expect(src).toMatch(/alert_catalog\.severity_token/);
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
