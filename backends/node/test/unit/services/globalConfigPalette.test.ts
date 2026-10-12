/**
 * The agency colour-wheel palette, through the global config's persistence.
 *
 * It was validated and merged on the way in and then dropped: there was no
 * column, so saveGlobalConfig never wrote it and rowToConfig never read one,
 * and the schema default ([]) won every time. The PUT answered with an empty
 * palette and the staff UI overwrote its local swatches with it, so a saved
 * swatch vanished on save and on every reload.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const poolQuery = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => ({ query: poolQuery })),
}));
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

import {
  saveGlobalConfig,
  getGlobalConfig,
  GlobalConfigSchema,
} from '../../../src/services/nodes/globalConfig.js';

/** A stored singleton row, as both queries select it. */
function storedRow(over: Record<string, unknown> = {}) {
  return {
    agencies: [],
    sdrtrunk_aliases: [],
    rdio_systems: [],
    rdio_groups: [],
    rdio_tags: [],
    sdrtrunk_config: { aliasLists: [], aliases: [], streams: [] },
    talkgroup_defaults: {},
    color_palette: [],
    version: 'v1',
    updated_at: '2026-10-13T00:00:00.000Z',
    updated_by: 'owner',
    ...over,
  };
}

const input = GlobalConfigSchema.parse({});

beforeEach(() => {
  poolQuery.mockReset();
});

describe('global config colour palette', () => {
  it('writes the palette and reads back what was written', async () => {
    const palette = ['#ff0000', '#00ff00'];
    poolQuery.mockImplementation(async () => ({
      rows: [storedRow({ color_palette: palette })],
    }));

    const saved = await saveGlobalConfig({ ...input, colorPalette: palette }, 'owner');
    expect(saved.colorPalette).toEqual(palette);

    const sql = String(poolQuery.mock.calls[0]?.[0]);
    expect(sql).toContain('color_palette');
    expect(sql).toContain('color_palette      = EXCLUDED.color_palette');
    // Sent as JSON, like every other JSONB column on the row.
    expect(poolQuery.mock.calls[0]?.[1]).toContain(JSON.stringify(palette));
  });

  it('is selected on the read path too, not just written', async () => {
    poolQuery.mockImplementation(async () => ({
      rows: [storedRow({ color_palette: ['#123456'] })],
    }));
    const got = await getGlobalConfig();
    expect(got.colorPalette).toEqual(['#123456']);
    expect(String(poolQuery.mock.calls[0]?.[0])).toContain('color_palette');
  });

  it('keeps the palette out of the version, so a swatch never re-syncs the fleet', async () => {
    // The palette is display-only and never reaches a node. Folding it into
    // globalConfigVersion would push the whole config to every node each time
    // someone picked a colour.
    poolQuery.mockImplementation(async () => ({ rows: [storedRow()] }));
    const plain = await saveGlobalConfig({ ...input, colorPalette: [] }, 'owner');
    const versionWithout = String(poolQuery.mock.calls[0]?.[1]?.[6]);

    poolQuery.mockClear();
    await saveGlobalConfig({ ...input, colorPalette: ['#ff0000'] }, 'owner');
    expect(String(poolQuery.mock.calls[0]?.[1]?.[6])).toBe(versionWithout);
    expect(plain.colorPalette).toEqual([]);
  });

  it('drops a malformed palette without taking the rest of the config with it', async () => {
    // Same treatment as talkgroup_defaults: parsed on its own, so one bad
    // swatch cannot wipe (and then re-persist) every agency.
    poolQuery.mockImplementation(async () => ({
      rows: [storedRow({ color_palette: { not: 'an array' } })],
    }));
    const got = await getGlobalConfig();
    expect(got.colorPalette).toEqual([]);
    expect(got.version).toBe('v1');
  });
});
