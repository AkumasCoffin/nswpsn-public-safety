/**
 * Unified "Agency" model: an agency owns its SDR-Trunk alias + rdio system, and
 * we DERIVE both from it. These tests pin the derivation (esp. the routing-
 * critical broadcastChannel == name == system label unification) and the
 * back-compat build from separate alias + system lists.
 */
import { describe, it, expect } from 'vitest';
import {
  buildAgencies,
  agenciesToAliases,
  agenciesToSystems,
  ALIAS_LIST_NAME,
  type Alias,
} from '../../../src/services/nodes/globalConfig.js';

const aliases: Alias[] = [
  {
    name: 'Rural Fire Service',
    list: 'catch all PSN',
    group: 'RFS',
    color: '-65536',
    iconName: 'fire',
    ids: [
      { type: 'priority', attrs: { priority: '-1' } },
      { type: 'broadcastChannel', attrs: { channel: 'Rural Fire Service' } },
      { type: 'talkgroupRange', attrs: { protocol: 'APCO25', min: '10101', max: '10199' } },
    ],
  },
];
const systems: Record<string, unknown>[] = [
  { id: 1, label: 'Rural Fire Service', led: 'red', talkgroups: [{ id: 10101, label: 'Ops' }], units: [] },
];

describe('agencies model', () => {
  it('builds an agency by merging a system with its alias (matched on broadcastChannel)', () => {
    const [a] = buildAgencies(aliases, systems);
    expect(a.systemId).toBe(1);
    expect(a.name).toBe('Rural Fire Service');
    expect(a.priority).toBe(-1);
    expect(a.color).toBe('-65536');
    expect(a.led).toBe('red');
    expect(a.talkgroups).toHaveLength(1);
    // aliasIds = the alias ids MINUS priority + broadcastChannel (those are derived)
    expect(a.aliasIds).toEqual([
      { type: 'talkgroupRange', attrs: { protocol: 'APCO25', min: '10101', max: '10199' } },
    ]);
  });

  it('derives an alias with a unified broadcastChannel + the auto list', () => {
    const [al] = agenciesToAliases(buildAgencies(aliases, systems));
    expect(al.name).toBe('Rural Fire Service');
    expect(al.list).toBe(ALIAS_LIST_NAME);
    expect(al.ids.find((i) => i.type === 'broadcastChannel')?.attrs['channel']).toBe('Rural Fire Service');
    expect(al.ids.find((i) => i.type === 'priority')?.attrs['priority']).toBe('-1');
    expect(al.ids.some((i) => i.type === 'talkgroupRange')).toBe(true);
  });

  it('derives a system with id/label from systemId/name and no alias-only fields', () => {
    const [s] = agenciesToSystems(buildAgencies(aliases, systems));
    expect(s['id']).toBe(1);
    expect(s['label']).toBe('Rural Fire Service');
    expect(s['led']).toBe('red');
    expect(s['color']).toBeUndefined();
    expect(s['aliasIds']).toBeUndefined();
    expect(Array.isArray(s['talkgroups'])).toBe(true);
  });

  it('a system with no matching alias → agency with no streaming scope (system kept, no alias)', () => {
    const agencies = buildAgencies([], [{ id: 100, label: 'Other', talkgroups: [], units: [] }]);
    expect(agencies).toHaveLength(1);
    expect(agencies[0].aliasIds).toEqual([]);
    expect(agenciesToAliases(agencies)).toHaveLength(0);
    expect(agenciesToSystems(agencies)).toHaveLength(1);
  });

  it('the derived alias broadcastChannel equals the derived system label (routing holds)', () => {
    const agencies = buildAgencies(aliases, systems);
    const bc = agenciesToAliases(agencies)[0].ids.find((i) => i.type === 'broadcastChannel')?.attrs['channel'];
    const label = agenciesToSystems(agencies)[0]['label'];
    expect(bc).toBe(label);
  });
});

import { loadPresets } from '../../../src/services/nodes/configMerge.js';
import { parseAliasesFromXml } from '../../../src/services/nodes/globalConfig.js';

describe('agencies seed from the real preset', () => {
  it('produces agencies with the routing unification intact', () => {
    const presets = loadPresets();
    const aliases = parseAliasesFromXml(presets.playlistXml);
    const systems = (presets.rdio as Record<string, unknown>)['systems'] as Record<string, unknown>[];
    const agencies = buildAgencies(aliases, systems ?? []);
    expect(agencies.length).toBeGreaterThan(0);
    for (const a of agencies) {
      expect(typeof a.systemId).toBe('number');
      expect(a.name.length).toBeGreaterThan(0);
    }
    const outAliases = agenciesToAliases(agencies);
    expect(outAliases.length).toBeGreaterThan(0);
    const labels = new Set(agenciesToSystems(agencies).map((s) => s['label']));
    for (const al of outAliases) {
      const bc = al.ids.find((i) => i.type === 'broadcastChannel')?.attrs['channel'];
      expect(labels.has(bc)).toBe(true);
    }
  });
});

import { GlobalConfigSchema } from '../../../src/services/nodes/globalConfig.js';

describe('seeded agencies pass schema validation (rowToConfig round-trip)', () => {
  it('GlobalConfigSchema accepts the real-preset agencies', () => {
    const presets = loadPresets();
    const aliases = parseAliasesFromXml(presets.playlistXml);
    const systems = (presets.rdio as Record<string, unknown>)['systems'] as Record<string, unknown>[];
    const agencies = buildAgencies(aliases, systems ?? []);
    const parsed = GlobalConfigSchema.safeParse({ agencies, rdioGroups: [], rdioTags: [] });
    if (!parsed.success) {
      // Print the first few failures so we can see WHICH field breaks.
      console.error('VALIDATION FAILURES:\n' + JSON.stringify(parsed.error.issues.slice(0, 10), null, 2));
    }
    expect(parsed.success).toBe(true);
  });
});
