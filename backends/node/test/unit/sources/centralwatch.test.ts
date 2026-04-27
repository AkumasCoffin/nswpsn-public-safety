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

  it('joins cameras with sites and drops ones without coords', async () => {
    stat.mockResolvedValue({ mtimeMs: 1 });
    readFile.mockResolvedValue(FIXTURE);
    const { getCentralwatchCameras } = await import(
      '../../../src/sources/centralwatch.js'
    );
    const cams = await getCentralwatchCameras();
    expect(cams.length).toBe(1);
    const cam = cams[0];
    if (!cam) throw new Error('no cam');
    expect(cam.id).toBe('cam-1');
    expect(cam.siteName).toBe('Mt Wereboldera');
    expect(cam.latitude).toBe(-35.36);
    expect(cam.imageUrl).toBe('/api/centralwatch/image/cam-1');
    // Timestamp was normalised to Z form (matches python).
    expect(cam.time).toBe('2026-04-25T16:48:03.000Z');
    expect(cam.source).toBe('centralwatch');
  });

  it('groups cameras by site for /sites', async () => {
    stat.mockResolvedValue({ mtimeMs: 1 });
    readFile.mockResolvedValue(FIXTURE);
    const { getCentralwatchSites } = await import(
      '../../../src/sources/centralwatch.js'
    );
    const sites = await getCentralwatchSites();
    expect(sites.length).toBe(1);
    const s = sites[0];
    if (!s) throw new Error('no site');
    expect(s.siteId).toBe('site-1');
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
