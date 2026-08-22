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
    expect(cat.agencies.get(30015)).toBe('RFS'); // group still comes from the alias
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
