/**
 * talkgroupCatalog display resolution. `labels` is what the Data tab shows, and
 * it must prefer a talkgroup's FRIENDLY NAME ("South Western Slopes A") over the
 * short label/alias name ("SWS A"). The alias pass fills anything the unified
 * rows don't cover, and must never overwrite a friendly name.
 *
 * The catalog memoises for ~60s in module state, so each case re-imports the
 * module with its own stubbed config rather than fighting the cache.
 */
import { describe, it, expect, vi } from 'vitest';

type Catalog = Awaited<ReturnType<typeof import('../../../src/services/talkgroupCatalog.js')['talkgroupCatalog']>>;

async function withConfig(cfg: unknown, fail = false): Promise<Catalog> {
  vi.resetModules();
  vi.doMock('../../../src/services/nodes/globalConfig.js', async () => {
    const actual = await vi.importActual<typeof import('../../../src/services/nodes/globalConfig.js')>(
      '../../../src/services/nodes/globalConfig.js',
    );
    return {
      ...actual,
      getGlobalConfig: fail
        ? () => Promise.reject(new Error('db down'))
        : () => Promise.resolve(cfg),
    };
  });
  const mod = await import('../../../src/services/talkgroupCatalog.js');
  return mod.talkgroupCatalog();
}

const cfg = (agencies: unknown[], aliases: unknown[] = []) => ({
  agencies,
  rdioGroups: [],
  rdioTags: [],
  sdrtrunkConfig: { aliasLists: [], aliases, streams: [] },
  defaults: {},
});

describe('talkgroupCatalog display names', () => {
  it('prefers the friendly name over the short label', async () => {
    const cat = await withConfig(
      cfg([
        {
          systemId: 1,
          name: 'Rural Fire Service',
          talkgroups: [{ id: 30015, label: 'SWS A', name: 'South Western Slopes A' }],
          units: [],
        },
      ]),
    );
    expect(cat.labels.get(30015)).toBe('South Western Slopes A');
  });

  it('falls back to the label when a row has no friendly name', async () => {
    const cat = await withConfig(
      cfg([
        {
          systemId: 1,
          name: 'Rural Fire Service',
          talkgroups: [{ id: 30012, label: 'RVRNA A' }, { id: 30013, label: 'X', name: '  ' }],
          units: [],
        },
      ]),
    );
    // Derived aliases name themselves from the label, so the alias pass supplies it.
    expect(cat.labels.get(30012)).toBe('RVRNA A');
    expect(cat.labels.get(30013)).toBe('X');
  });

  it('a legacy imported alias never overwrites a friendly name', async () => {
    const cat = await withConfig(
      cfg(
        [
          {
            systemId: 1,
            name: 'Rural Fire Service',
            talkgroups: [{ id: 30015, label: 'SWS A', name: 'South Western Slopes A' }],
            units: [],
          },
        ],
        // Pre-merge config: the imported list is still populated and pushed verbatim.
        [{ name: '144 SWS A', group: 'RFS', ids: [{ type: 'talkgroup', attrs: { value: '30015' } }] }],
      ),
    );
    expect(cat.labels.get(30015)).toBe('South Western Slopes A');
    // The agency is the AGENCY'S NAME, not the alias group — talkgroups and
    // radios must agree on one name or the filters list the agency twice.
    expect(cat.agencies.get(30015)).toBe('Rural Fire Service');
  });

  it('falls back to the alias group when no unified row owns the talkgroup', async () => {
    const cat = await withConfig(
      cfg(
        [], // legacy config: nothing but the imported alias list
        [{ name: '144 SWS A', group: 'RFS', ids: [{ type: 'talkgroup', attrs: { value: '30015' } }] }],
      ),
    );
    expect(cat.agencies.get(30015)).toBe('RFS');
  });

  it('collects encrypted talkgroups from agencies with the flag', async () => {
    const cat = await withConfig(
      cfg([
        { systemId: 1, name: 'RFS', talkgroups: [{ id: 10030, label: 'A' }], units: [] },
        {
          systemId: 120,
          name: 'NSW PF (ENC)',
          encrypted: true,
          talkgroups: [{ id: 12001 }, { id: 12002 }],
          units: [],
        },
      ]),
    );
    expect([...cat.encrypted].sort()).toEqual([12001, 12002]);
    expect(cat.encrypted.has(10030)).toBe(false);
  });

  it('degrades to empty maps when the config load fails', async () => {
    const cat = await withConfig(null, true);
    expect(cat.labels.size).toBe(0);
    expect(cat.encrypted.size).toBe(0);
  });
});

describe('normaliseAliasColor', () => {
  it('accepts hex and signed Java ints, treating 0 as unset', async () => {
    vi.resetModules();
    const { normaliseAliasColor } = await import('../../../src/services/talkgroupCatalog.js');
    expect(normaliseAliasColor('#ff0000')).toBe('#ff0000');
    expect(normaliseAliasColor('-65536')).toBe('#ff0000');
    expect(normaliseAliasColor('0')).toBeNull();
    expect(normaliseAliasColor('nonsense')).toBeNull();
  });
});

describe('radio display lookups (unit → agency / colour / configured label)', () => {
  const agencies = [
    {
      systemId: 1,
      name: 'Rural Fire Service',
      color: '-1671091',
      talkgroups: [{ id: 30015, label: 'SWS A', name: 'South Western Slopes A' }],
      units: [
        { id: 2000115, label: 'NEWRAD15' },
        { id: 2010046, label: 'Gwandalan Pumper' },
        { id: 2010047, label: '  ' }, // blank label = no configured alias
      ],
    },
    {
      // No explicit colour — falls back to the rdio LED preset.
      systemId: 4,
      name: 'State Emergency Service',
      led: 'yellow',
      talkgroups: [],
      units: [{ id: 2044011, label: 'Armidale 12' }, { id: 2000115, label: 'DUPLICATE' }],
    },
  ];

  it('maps a unit to its agency, colour and configured label', async () => {
    const cat = await withConfig(cfg(agencies));
    expect(cat.unitAgencies.get(2010046)).toBe('Rural Fire Service');
    expect(cat.unitColors.get(2010046)).toBe('#e6804d'); // -1671091
    expect(cat.unitLabels.get(2010046)).toBe('Gwandalan Pumper');
  });

  it('falls back to the LED preset when an agency has no explicit colour', async () => {
    const cat = await withConfig(cfg(agencies));
    expect(cat.unitAgencies.get(2044011)).toBe('State Emergency Service');
    expect(cat.unitColors.get(2044011)).toBe('#eab308'); // yellow
  });

  it('first agency wins on a duplicate unit id', async () => {
    const cat = await withConfig(cfg(agencies));
    expect(cat.unitAgencies.get(2000115)).toBe('Rural Fire Service');
    expect(cat.unitLabels.get(2000115)).toBe('NEWRAD15');
  });

  it('a blank label is not a configured alias', async () => {
    const cat = await withConfig(cfg(agencies));
    expect(cat.unitAgencies.get(2010047)).toBe('Rural Fire Service'); // still owned
    expect(cat.unitLabels.has(2010047)).toBe(false); // but unnamed
  });

  it('the configured label is independent of any OTA alias', async () => {
    // The catalogue knows nothing about OTA aliases — those live per-event in
    // node_radio_events.source_alias / node_radio_aliases. A radio can have
    // both, and this map must never be treated as the OTA.
    const cat = await withConfig(cfg(agencies));
    expect(cat.unitLabels.get(2000115)).toBe('NEWRAD15');
    expect(cat.labels.has(2000115)).toBe(false); // not a talkgroup label either
  });
});

describe('configuredTalkgroups (what the talkgroup list is allowed to show)', () => {
  it('carries ids that are NOT 5 digits, so configured AirBand channels survive', async () => {
    // A P25 talkgroup on this network is always 5 digits, and the list filters
    // on that — but AirBand carries aviation channels as low pseudo-talkgroups.
    // Anything the operator configured must pass regardless of its id, and
    // adding more AirBand channels must keep working with no code change.
    const cat = await withConfig(
      cfg([
        {
          systemId: 99,
          name: 'AirBand',
          talkgroups: [
            { id: 5, label: 'TWR YSNW', name: 'Tower Nowra Airport' },
            { id: 6, label: 'CTR SY', name: 'Sydney Centre' }, // a future one
          ],
          units: [],
        },
        {
          systemId: 1,
          name: 'Rural Fire Service',
          talkgroups: [{ id: 30015, label: 'SWS A', name: 'South Western Slopes A' }],
          units: [],
        },
      ]),
    );
    expect(cat.configuredTalkgroups.has(5)).toBe(true);
    expect(cat.configuredTalkgroups.has(6)).toBe(true);
    expect(cat.configuredTalkgroups.has(30015)).toBe(true);
    // Decode noise is never configured, which is what the list filters on.
    expect(cat.configuredTalkgroups.has(798)).toBe(false);
    expect(cat.configuredTalkgroups.has(23610)).toBe(false);
  });

  it('is empty for a config with no talkgroups (callers must not filter on it)', async () => {
    const cat = await withConfig(cfg([]));
    expect(cat.configuredTalkgroups.size).toBe(0);
  });
});
