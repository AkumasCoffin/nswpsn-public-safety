/**
 * Unified-talkgroup derivation: ONE talkgroup row drives BOTH programs — the
 * SDR-Trunk alias (deriveAliasesFromTalkgroups) and the rdio talkgroup
 * (agenciesToSystems) are generated from it at push time. These tests pin the
 * field-resolution rules (name/group/colour/priority inheritance), the
 * encrypted-agency carve-out (aliases but no rdio side, no routing), and the
 * configMerge legacy switch that keeps pre-unification imported aliases working.
 */
import { describe, it, expect } from 'vitest';
import {
  AgencySchema,
  deriveAliasesFromTalkgroups,
  agenciesToSystems,
  globalConfigVersion,
  GlobalConfigSchema,
  ALIAS_LIST_NAME,
  type Agency,
  type Talkgroup,
  type TalkgroupDefaults,
  type GlobalConfig,
} from '../../../src/services/nodes/globalConfig.js';
import { buildConfigPayload } from '../../../src/services/nodes/configMerge.js';
import type { NodeRow } from '../../../src/services/nodes/registry.js';

// ── helpers ─────────────────────────────────────────────────────────────────

/** Build a valid Agency through the schema (fills talkgroups/units/aliasIds
 *  defaults) so the tests exercise the same shapes production data has. */
function agency(over: Record<string, unknown> = {}): Agency {
  return AgencySchema.parse({ systemId: 1, name: 'Rural Fire Service', ...over });
}

const idOf = (al: { ids: Array<{ type: string; attrs: Record<string, string> }> }, type: string) =>
  al.ids.find((i) => i.type === type);

describe('deriveAliasesFromTalkgroups', () => {
  it('resolves the alias name aliasName > label > name > String(id), treating whitespace-only as empty', () => {
    const a = agency({
      talkgroups: [
        { id: 1209, aliasName: '1209 AVATN 1', label: 'Aviation', name: 'RFS Aviation' },
        { id: 101, label: 'ME1', name: 'Metro East 1' },
        { id: 5, aliasName: '   ', label: '', name: 'Ops' }, // whitespace/empty fall through
        { id: 6, aliasName: null, label: '  ', name: null }, // nothing usable → the id
      ],
    });
    const out = deriveAliasesFromTalkgroups([a], {});
    expect(out.map((al) => al.name)).toEqual(['1209 AVATN 1', 'ME1', 'Ops', '6']);
  });

  it('inherits priority row > agency > defaults > 100 and emits it as a string attr', () => {
    const withAgencyPrio = agency({
      priority: 9,
      talkgroups: [
        { id: 1 }, // → agency 9
        { id: 2, priority: 3 }, // row wins
        { id: 3, priority: null }, // null = inherit, same as absent
      ],
    });
    const noAgencyPrio = agency({ systemId: 2, name: 'Fire and Rescue', talkgroups: [{ id: 4 }] });
    const defaults: TalkgroupDefaults = { priority: 55 };
    const out = deriveAliasesFromTalkgroups([withAgencyPrio, noAgencyPrio], defaults);
    expect(out.map((al) => idOf(al, 'priority')?.attrs['priority'])).toEqual(['9', '3', '9', '55']);
    // No default either → the SDR-Trunk "normal monitor" 100.
    const fallback = deriveAliasesFromTalkgroups([noAgencyPrio], {});
    expect(idOf(fallback[0]!, 'priority')?.attrs['priority']).toBe('100');
  });

  it('groups under sdrGroupName ?? agency name (trimmed) and routes to the trimmed agency name', () => {
    const historic = agency({
      systemId: 2,
      name: ' Fire and Rescue ',
      sdrGroupName: ' Fire & Rescue NSW ',
      talkgroups: [{ id: 101, label: 'ME1' }],
    });
    const plain = agency({ name: 'Rural Fire Service', talkgroups: [{ id: 10101, label: 'Ops' }] });
    const [h, p] = deriveAliasesFromTalkgroups([historic, plain], {});
    // sdrGroupName exists because the historical alias group ≠ the agency name.
    expect(h!.group).toBe('Fire & Rescue NSW');
    expect(idOf(h!, 'broadcastChannel')?.attrs['channel']).toBe('Fire and Rescue');
    expect(p!.group).toBe('Rural Fire Service');
    expect(idOf(p!, 'broadcastChannel')?.attrs['channel']).toBe('Rural Fire Service');
  });

  it('encrypted agency: talkgroup + priority ids but NO broadcastChannel; global colour; auto list', () => {
    const enc = agency({
      systemId: 12,
      name: 'NSW PF',
      encrypted: true,
      talkgroups: [{ id: 12001, label: 'PF 1' }],
    });
    const [al] = deriveAliasesFromTalkgroups([enc], { color: '-65536' });
    expect(al!.name).toBe('PF 1');
    expect(al!.list).toBe(ALIAS_LIST_NAME); // always 'NSWPSN'
    expect(al!.color).toBe('-65536'); // the one global colour
    expect(idOf(al!, 'broadcastChannel')).toBeUndefined(); // no stream, nothing uploads
    expect(idOf(al!, 'priority')?.attrs['priority']).toBe('100');
    expect(idOf(al!, 'talkgroup')?.attrs).toEqual({ value: '12001', protocol: 'APCO25' });
    // No default colour set → the alias simply has none.
    const [plainAl] = deriveAliasesFromTalkgroups([enc], {});
    expect(plainAl!.color).toBeUndefined();
  });

  it('skips talkgroup rows with a missing or non-integer id', () => {
    const a = agency({ talkgroups: [{ id: 10101, label: 'Ops' }] });
    // Bad rows can only come from unvalidated legacy data — inject them past the schema.
    a.talkgroups = [
      { id: 10101, label: 'Ops' },
      { id: 1.5, label: 'fractional' },
      { label: 'no id at all' },
      { id: '10102', label: 'stringly id' },
    ] as unknown as Talkgroup[];
    const out = deriveAliasesFromTalkgroups([a], {});
    expect(out).toHaveLength(1);
    expect(out[0]!.name).toBe('Ops');
  });
});

describe('agenciesToSystems (unified talkgroup rows)', () => {
  it('an encrypted agency produces NO rdio system', () => {
    const enc = agency({ systemId: 12, name: 'NSW PF', encrypted: true, talkgroups: [{ id: 12001 }] });
    const clear = agency({ systemId: 1, name: 'Rural Fire Service', talkgroups: [{ id: 10101 }] });
    const out = agenciesToSystems([enc, clear]);
    expect(out.map((s) => s['id'])).toEqual([1]);
  });

  it('materialises agency defaultGroupId/defaultTagId onto rows, explicit values winning', () => {
    const a = agency({
      defaultGroupId: 5,
      defaultTagId: 9,
      talkgroups: [
        { id: 1, groupId: null, tagId: null }, // null = inherit
        { id: 2 }, // absent = inherit
        { id: 3, groupId: 7, tagId: 2 }, // explicit overrides
      ],
    });
    const [sys] = agenciesToSystems([a]);
    const rows = sys!['talkgroups'] as Array<Record<string, unknown>>;
    expect(rows.map((r) => [r['groupId'], r['tagId']])).toEqual([
      [5, 9],
      [5, 9],
      [7, 2],
    ]);
  });

  it('strips the SDR-Trunk-only aliasName/priority from the rdio talkgroup doc, keeping passthrough fields', () => {
    const a = agency({
      talkgroups: [
        { id: 10101, label: 'Ops', aliasName: '1209 AVATN 1', priority: 3, led2: 'blue', order: 4 },
      ],
    });
    const [sys] = agenciesToSystems([a]);
    const [row] = sys!['talkgroups'] as Array<Record<string, unknown>>;
    expect(row).not.toHaveProperty('aliasName');
    expect(row).not.toHaveProperty('priority');
    expect(row!['label']).toBe('Ops');
    expect(row!['led2']).toBe('blue'); // unmodelled rdio fields round-trip untouched
    expect(row!['order']).toBe(4);
  });
});

// ── configMerge: the legacy switch + encrypted exclusions ───────────────────
// buildConfigPayload reads presets from disk (loadPresets) and takes the global
// config as a parameter, so no DB/mocking is needed — same pattern as
// configMergeSdrtrunk.test.ts.

function radioNode(overrides: Partial<NodeRow> = {}): NodeRow {
  return {
    id: 'node-1',
    kind: 'radio',
    user_id: 'user-1',
    install_id: null,
    name: 'radio-node',
    enabled: true,
    feed_enabled: true,
    config_override: {},
    config_version: null,
    agent_version: null,
    sdrtrunk_version: null,
    rdio_version: null,
    os: null,
    arch: null,
    last_seen_at: null,
    notes: null,
    created_at: '2026-01-01T00:00:00Z',
    token_prefix: null,
    lat: null,
    lon: null,
    ...overrides,
  } as NodeRow;
}

function globalCfg(over: Partial<GlobalConfig> = {}): GlobalConfig {
  return {
    agencies: [],
    rdioGroups: [],
    rdioTags: [],
    sdrtrunkConfig: { aliasLists: [], aliases: [], streams: [] },
    defaults: {},
    version: 'test',
    updatedAt: null,
    updatedBy: null,
    streamNames: [],
    ...over,
  };
}

describe('buildConfigPayload legacy switch (imported aliases vs derived)', () => {
  it('non-empty sdrtrunkConfig.aliases → imported aliases pushed verbatim (list → primary), NOT derived', async () => {
    const global = globalCfg({
      agencies: [
        agency({ systemId: 2, name: 'Fire and Rescue', talkgroups: [{ id: 101, label: 'ME1' }] }),
      ],
      sdrtrunkConfig: {
        aliasLists: [{ name: 'Z list', family: 'P25' }],
        aliases: [
          {
            name: '101 ME1',
            list: 'catch all PSN',
            group: 'FRNSW',
            ids: [{ type: 'talkgroup', attrs: { protocol: 'APCO25', value: '101' } }],
          },
          {
            name: '102 ME2',
            list: 'catch all PSN',
            group: 'FRNSW',
            ids: [{ type: 'talkgroup', attrs: { protocol: 'APCO25', value: '102' } }],
          },
        ],
        streams: [{ name: 'Fire and Rescue', systemId: 2, enabled: true }],
      },
    });
    const p = await buildConfigPayload(radioNode(), global);
    // The imported list wins over derivation while it is still non-empty.
    expect(p.aliases.map((a) => a.name)).toEqual(['101 ME1', '102 ME2']);
    // Collapsed onto the primary list (most-populated imported list).
    expect(new Set(p.aliases.map((a) => a.list))).toEqual(new Set(['catch all PSN']));
  });

  it('empty sdrtrunkConfig.aliases → aliases derived from the agency talkgroup rows', async () => {
    const global = globalCfg({
      agencies: [
        agency({
          systemId: 2,
          name: 'Fire and Rescue',
          sdrGroupName: 'Fire & Rescue NSW',
          talkgroups: [{ id: 101, label: 'ME1' }, { id: 102, aliasName: '102 ME2' }],
        }),
      ],
      defaults: { color: '-65536' },
    });
    const p = await buildConfigPayload(radioNode(), global);
    expect(p.aliases.map((a) => a.name)).toEqual(['ME1', '102 ME2']);
    expect(new Set(p.aliases.map((a) => a.list))).toEqual(new Set([ALIAS_LIST_NAME]));
    expect(p.aliases[0]!.group).toBe('Fire & Rescue NSW');
    expect(p.aliases[0]!.color).toBe('-65536');
    expect(idOf(p.aliases[0]!, 'broadcastChannel')?.attrs['channel']).toBe('Fire and Rescue');
  });

  it("an encrypted agency's systemId never reaches streamTargets or the generated rdio apiKeys", async () => {
    const agencies = [
      agency({ systemId: 2, name: 'Fire and Rescue', talkgroups: [{ id: 101, label: 'ME1' }] }),
      agency({ systemId: 12, name: 'NSW PF', encrypted: true, talkgroups: [{ id: 12001, label: 'PF 1' }] }),
    ];

    // Derived-streams path (no imported streams → fall back to rdio systems).
    const derived = await buildConfigPayload(radioNode(), globalCfg({ agencies }));
    expect(derived.streamTargets.map((t) => t.systemId)).toEqual([2]);

    // Imported-streams path: even an imported stream for the encrypted system is dropped.
    const imported = await buildConfigPayload(
      radioNode(),
      globalCfg({
        agencies,
        sdrtrunkConfig: {
          aliasLists: [],
          aliases: [],
          streams: [
            { name: 'NSW PF', systemId: 12, enabled: true },
            { name: 'Fire and Rescue', systemId: 2, enabled: true },
          ],
        },
      }),
    );
    expect(imported.streamTargets.map((t) => t.systemId)).toEqual([2]);
    const apiKeys =
      (imported.rdioConfig as { apiKeys?: Array<{ systems: Array<{ id: number }> }> }).apiKeys ?? [];
    expect(apiKeys.length).toBeGreaterThan(0);
    expect(apiKeys.every((k) => k.systems.every((s) => s.id !== 12))).toBe(true);
    // The encrypted agency has no rdio system either.
    const systems = (imported.rdioConfig as { systems?: Array<{ id: number }> }).systems ?? [];
    expect(systems.some((s) => s.id === 12)).toBe(false);
    // But it still gets its decode-label alias.
    expect(imported.aliases.some((a) => a.name === 'PF 1')).toBe(true);
  });
});

describe('globalConfigVersion folds in defaults', () => {
  const base = { agencies: [], rdioGroups: [], rdioTags: [] };

  it('changes when defaults.color changes, stable when defaults are equal', () => {
    const red = globalConfigVersion({ ...base, defaults: { color: '-65536' } });
    const blue = globalConfigVersion({ ...base, defaults: { color: '-16776961' } });
    const redAgain = globalConfigVersion({ ...base, defaults: { color: '-65536' } });
    expect(red).not.toBe(blue); // colour feeds the derived aliases → must re-sync
    expect(red).toBe(redAgain);
  });

  it('treats absent defaults the same as empty defaults', () => {
    expect(globalConfigVersion(base)).toBe(globalConfigVersion({ ...base, defaults: {} }));
  });
});

describe('GlobalConfigSchema defaults key', () => {
  it('defaults to empty and accepts colour + priority', () => {
    expect(GlobalConfigSchema.parse({}).defaults).toEqual({});
    const parsed = GlobalConfigSchema.parse({ defaults: { color: '-65536', priority: -1 } });
    expect(parsed.defaults).toEqual({ color: '-65536', priority: -1 });
  });
});
