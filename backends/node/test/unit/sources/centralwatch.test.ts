/**
 * Centralwatch source unit tests. Mocks node:fs/promises so we can feed
 * in a controlled JSON payload without touching the real backends/data
 * tree.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const stat = vi.fn();
const readFile = vi.fn();

vi.mock('node:fs', () => ({
  promises: {
    stat: (...args: unknown[]) => stat(...args),
    readFile: (...args: unknown[]) => readFile(...args),
  },
}));

const FIXTURE = JSON.stringify({
  lastUpdated: '2026-04-25T16:48:03Z',
  source: 'https://centralwatch.watchtowers.io/au/api/cameras',
  sites: {
    'site-1': {
      name: 'Mt Wereboldera',
      latitude: -35.36,
      longitude: 148.21,
      altitude: 837,
      state: 'NSW',
    },
    'site-2': {
      name: 'Bad Site',
      // missing latitude/longitude — cameras keyed to this site must drop
      latitude: null as unknown as number,
      longitude: null as unknown as number,
      altitude: null,
      state: 'NSW',
    },
  },
  cameras: [
    {
      id: 'cam-1',
      name: 'Main Tower',
      siteId: 'site-1',
      time: '2026-04-25T16:48:03.000+00:00',
    },
    {
      id: 'cam-orphan',
      name: 'Orphan',
      siteId: 'no-such-site',
      time: '2026-04-25T16:48:03Z',
    },
    {
      id: 'cam-bad-site',
      name: 'Bad Site Cam',
      siteId: 'site-2',
      time: '2026-04-25T16:48:03Z',
    },
  ],
});

describe('centralwatch source', () => {
  beforeEach(async () => {
    stat.mockReset();
    readFile.mockReset();
    const mod = await import('../../../src/sources/centralwatch.js');
    mod._resetCentralwatchCacheForTests();
  });

  it('joins cameras with sites — preserves nullish coords (python parity)', async () => {
    stat.mockResolvedValue({ mtimeMs: 1 });
    readFile.mockResolvedValue(FIXTURE);
    const { getCentralwatchCameras } = await import(
      '../../../src/sources/centralwatch.js'
    );
    const cams = await getCentralwatchCameras();
    // Both cameras returned — python emits site.latitude/longitude as
    // null rather than dropping the camera entirely.
    expect(cams.length).toBe(2);
    const good = cams.find((c) => c.id === 'cam-1');
    if (!good) throw new Error('no good cam');
    expect(good.siteName).toBe('Mt Wereboldera');
    expect(good.latitude).toBe(-35.36);
    expect(good.imageUrl).toBe('/api/centralwatch/image/cam-1');
    expect(good.time).toBe('2026-04-25T16:48:03.000Z');
    expect(good.source).toBe('centralwatch');
    const bad = cams.find((c) => c.id === 'cam-bad-site');
    if (!bad) throw new Error('no bad cam');
    expect(bad.latitude).toBeNull();
    expect(bad.longitude).toBeNull();
  });

  it('groups cameras by site for /sites', async () => {
    stat.mockResolvedValue({ mtimeMs: 1 });
    readFile.mockResolvedValue(FIXTURE);
    const { getCentralwatchSites } = await import(
      '../../../src/sources/centralwatch.js'
    );
    const sites = await getCentralwatchSites();
    // Both sites — site-2 has null coords but its camera still surfaces.
    expect(sites.length).toBe(2);
    const s = sites.find((x) => x.siteId === 'site-1');
    if (!s) throw new Error('no site');
    expect(s.cameras.length).toBe(1);
    expect(s.cameras[0]?.id).toBe('cam-1');
  });

  it('returns empty list when JSON file is missing', async () => {
    const err = new Error('not found') as NodeJS.ErrnoException;
    err.code = 'ENOENT';
    stat.mockRejectedValue(err);
    const { getCentralwatchCameras } = await import(
      '../../../src/sources/centralwatch.js'
    );
    expect(await getCentralwatchCameras()).toEqual([]);
  });

  it('caches between calls until the file mtime changes', async () => {
    stat.mockResolvedValue({ mtimeMs: 1 });
    readFile.mockResolvedValueOnce(FIXTURE);
    const mod = await import('../../../src/sources/centralwatch.js');
    await mod.getCentralwatchCameras();
    await mod.getCentralwatchCameras();
    // Two getCentralwatchCameras calls — only one readFile (mtime same).
    // (Note: stat is called twice; readFile only when mtime differs.)
    expect(readFile).toHaveBeenCalledTimes(1);
  });
});
