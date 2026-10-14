/**
 * Unit tests for the Dorsal Watch shark-report source.
 *
 * The two things worth pinning here are the ones that would quietly cost
 * money or leak data if they regressed: the archive fan-out writes a report
 * exactly once (the parents are append-only, so "once" is the difference
 * between 25 rows a day and 49,000), and the reporter's `account` object
 * never reaches a row.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchJsonMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

// No DATABASE_URL in unit tests, but be explicit: the sidecar seed must be a
// no-op rather than a connection attempt.
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => null),
}));

const NOW = Date.now();
const hoursAgo = (h: number): number => NOW - h * 3_600_000;

function report(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: 1,
    location: 'Mollymook Beach',
    state: 'NSW',
    zone: 'South Coast',
    country: 'Australia',
    coordinate: '-35.333000,150.481033',
    reportTime: hoursAgo(1),
    typeOfEncounter: 'Receiver Tag',
    typeOfShark: 'White Shark',
    sharkLength: null,
    distanceFromShore: 'N/A',
    comment: 'DPIRD advise: tagged White Shark #3101 detected.',
    official: true,
    sharkPhotos: [],
    // Upstream really does send this; it must never survive normalisation.
    account: { id: 1000264344, name: 'Kerianne', email: '-', mobile: '-' },
    ...over,
  };
}

function upstream(rows: Record<string, unknown>[]): unknown {
  return { error: null, responseData: rows };
}

async function freshModule() {
  vi.resetModules();
  return import('../../../src/sources/dorsal.js');
}

describe('dorsal.fetchDorsal', () => {
  beforeEach(() => fetchJsonMock.mockReset());

  it('normalises a report into a GeoJSON feature with lon,lat order', async () => {
    fetchJsonMock.mockResolvedValueOnce(upstream([report()]));
    const { fetchDorsal } = await freshModule();
    const snap = await fetchDorsal();

    expect(snap.type).toBe('FeatureCollection');
    expect(snap.count).toBe(1);
    const f = snap.features[0]!;
    expect(f.geometry.coordinates).toEqual([150.481033, -35.333]);
    expect(f.properties.report_id).toBe('1');
    expect(f.properties.category).toBe('detection');
    expect(f.properties.species).toBe('White Shark');
    expect(f.properties.state).toBe('NSW');
    expect(f.properties.provenance).toBe('Official');
    // The logs page projects data->>'title'; without one every row is "Unknown".
    expect(f.properties.title).toBe('White Shark — Mollymook Beach');
  });

  it('never carries the reporter account through', async () => {
    fetchJsonMock.mockResolvedValueOnce(upstream([report({ official: false })]));
    const { fetchDorsal } = await freshModule();
    const snap = await fetchDorsal();

    const serialised = JSON.stringify(snap);
    expect(serialised).not.toContain('Kerianne');
    expect(serialised).not.toContain('account');
    expect(snap.features[0]!.properties.provenance).toBe('Community report');
  });

  it('drops reports outside the Australian bbox', async () => {
    fetchJsonMock.mockResolvedValueOnce(
      upstream([
        report({ id: 1 }),
        // Point Panic, Oahu — present in the real upstream.
        report({ id: 2, coordinate: '21.289886,-157.863205', state: 'Hawaii' }),
      ]),
    );
    const { fetchDorsal } = await freshModule();
    const snap = await fetchDorsal();
    expect(snap.features.map((f) => f.properties.report_id)).toEqual(['1']);
  });

  it('skips reports with an unusable coordinate, id or timestamp', async () => {
    fetchJsonMock.mockResolvedValueOnce(
      upstream([
        report({ id: 1 }),
        report({ id: 2, coordinate: 'not-a-coordinate' }),
        report({ id: 3, coordinate: '' }),
        report({ id: null }),
        report({ id: 5, reportTime: 0 }),
      ]),
    );
    const { fetchDorsal } = await freshModule();
    const snap = await fetchDorsal();
    expect(snap.features.map((f) => f.properties.report_id)).toEqual(['1']);
  });

  it('treats "N/A" and blank measurements as absent, not zero', async () => {
    fetchJsonMock.mockResolvedValueOnce(
      upstream([
        report({ id: 1, distanceFromShore: 'N/A', sharkLength: '' }),
        report({ id: 2, distanceFromShore: '50.0', sharkLength: '2.5' }),
      ]),
    );
    const { fetchDorsal } = await freshModule();
    const snap = await fetchDorsal();
    const byId = new Map(snap.features.map((f) => [f.properties.report_id, f.properties]));
    expect(byId.get('1')!.distance_from_shore_m).toBeNull();
    expect(byId.get('1')!.shark_length_m).toBeNull();
    expect(byId.get('2')!.distance_from_shore_m).toBe(50);
    expect(byId.get('2')!.shark_length_m).toBe(2.5);
  });

  it('returns newest first and evicts anything past the 14-day window', async () => {
    fetchJsonMock.mockResolvedValueOnce(
      upstream([
        report({ id: 'old', reportTime: hoursAgo(24 * 20) }),
        report({ id: 'mid', reportTime: hoursAgo(48) }),
        report({ id: 'new', reportTime: hoursAgo(1) }),
      ]),
    );
    const { fetchDorsal } = await freshModule();
    const snap = await fetchDorsal();
    expect(snap.features.map((f) => f.properties.report_id)).toEqual(['new', 'mid']);
  });

  it('merges successive polls into the rolling window and lets edits win', async () => {
    const { fetchDorsal } = await freshModule();

    fetchJsonMock.mockResolvedValueOnce(upstream([report({ id: 1 })]));
    await fetchDorsal();

    fetchJsonMock.mockResolvedValueOnce(
      upstream([
        report({ id: 2, reportTime: hoursAgo(0.5) }),
        // Same id re-sent with a corrected species — upstream has an
        // "Updated report" encounter type, so a re-seen id must overwrite.
        report({ id: 1, typeOfShark: 'Bull' }),
      ]),
    );
    const snap = await fetchDorsal();

    expect(snap.count).toBe(2);
    const byId = new Map(snap.features.map((f) => [f.properties.report_id, f.properties]));
    expect(byId.get('1')!.species).toBe('Bull Shark');
  });

  it('throws when upstream reports an error inside its HTTP 200 envelope', async () => {
    // A missing publicKey comes back as {error:{code:1016}} with a 200, so
    // the status code alone proves nothing.
    fetchJsonMock.mockResolvedValueOnce({
      error: { code: 1016, message: 'Public key is empty.' },
      responseData: null,
    });
    const { fetchDorsal } = await freshModule();
    await expect(fetchDorsal()).rejects.toThrow(/Public key is empty/);
  });
});

describe('dorsal.categoryFor', () => {
  it('maps every encounter type observed upstream', async () => {
    const { categoryFor } = await freshModule();
    expect(categoryFor('Receiver Tag')).toBe('detection');
    expect(categoryFor('Tag and Release')).toBe('tagging');
    expect(categoryFor('Caught & Released')).toBe('tagging');
    expect(categoryFor('Bite')).toBe('incident');
    expect(categoryFor('Bump')).toBe('incident');
    expect(categoryFor('Beach Closure')).toBe('closure');
    expect(categoryFor('Water Evacuated')).toBe('closure');
    expect(categoryFor('General Warning')).toBe('advisory');
    expect(categoryFor('Whale Carcass')).toBe('carcass');
    // Drone is its own category; the Westpac helicopter deliberately is not,
    // so a change to one does not silently move the other.
    expect(categoryFor('Drone')).toBe('drone');
    expect(categoryFor('drone')).toBe('drone');
    expect(categoryFor('Westpac Heli')).toBe('sighting');
  });

  it('falls back to sighting for blank and unseen values', async () => {
    const { categoryFor } = await freshModule();
    // Both of these are real: upstream has blank entries and misspells
    // "Aerial Survelliance".
    expect(categoryFor('')).toBe('sighting');
    expect(categoryFor('Aerial Survelliance')).toBe('sighting');
    expect(categoryFor('Something Invented Next Year')).toBe('sighting');
  });

  it('is case and whitespace insensitive', async () => {
    const { categoryFor } = await freshModule();
    expect(categoryFor('  receiver tag ')).toBe('detection');
  });
});

describe('dorsal.speciesFor', () => {
  it('collapses the spellings upstream actually sends', async () => {
    const { speciesFor } = await freshModule();
    for (const s of ['White Shark', 'white shark']) {
      expect(speciesFor(s)).toBe('White Shark');
    }
    for (const s of ['Bull', 'bull shark', 'Bull Shark']) {
      expect(speciesFor(s)).toBe('Bull Shark');
    }
    for (const s of ['Tiger', 'tiger shark', 'tiger sharks', 'Tiger Shark']) {
      expect(speciesFor(s)).toBe('Tiger Shark');
    }
    for (const s of ['bronze whaler', 'Whaler']) {
      expect(speciesFor(s)).toBe('Whaler');
    }
    for (const s of ['hammerhead', 'hammerhead shark', 'Hammerhead']) {
      expect(speciesFor(s)).toBe('Hammerhead');
    }
  });

  it('normalises every "we do not know" spelling to one Unknown', async () => {
    const { speciesFor } = await freshModule();
    for (const s of ['', '  ', 'unknown', 'Unknown', 'unknown sp.', 'other']) {
      expect(speciesFor(s)).toBe('Unknown');
    }
  });

  it('passes an unrecognised species through rather than hiding it', async () => {
    const { speciesFor } = await freshModule();
    expect(speciesFor('Wobbegong')).toBe('Wobbegong');
  });
});

describe('dorsal.dorsalArchiveItems', () => {
  beforeEach(() => fetchJsonMock.mockReset());

  it('writes a report exactly once, however many polls see it', async () => {
    // This ratio IS the design: the archive parents are append-only, so
    // re-emitting the window every poll would be ~49,000 rows/day.
    const { fetchDorsal, dorsalArchiveItems } = await freshModule();
    const at = Math.floor(NOW / 1000);

    fetchJsonMock.mockResolvedValueOnce(
      upstream([report({ id: 1 }), report({ id: 2, reportTime: hoursAgo(2) })]),
    );
    const first = await fetchDorsal();
    expect(dorsalArchiveItems(first, at, 'dorsal')).toHaveLength(2);

    // Same two reports still in the window on the next poll, plus one new.
    fetchJsonMock.mockResolvedValueOnce(
      upstream([report({ id: 3, reportTime: hoursAgo(0.2) })]),
    );
    const second = await fetchDorsal();
    expect(second.count).toBe(3);
    const rows = dorsalArchiveItems(second, at, 'dorsal');
    expect(rows.map((r) => r.source_id)).toEqual(['3']);

    // And a third pass with nothing new writes nothing at all.
    fetchJsonMock.mockResolvedValueOnce(upstream([]));
    expect(dorsalArchiveItems(await fetchDorsal(), at, 'dorsal')).toHaveLength(0);
  });

  it('backfills 7 days on a cold start but reaches no further', async () => {
    const { fetchDorsal, dorsalArchiveItems } = await freshModule();
    fetchJsonMock.mockResolvedValueOnce(
      upstream([
        report({ id: 'recent', reportTime: hoursAgo(1) }),
        report({ id: 'day6', reportTime: hoursAgo(24 * 6) }),
        // Inside the 14-day snapshot window, outside the 7-day archive one.
        report({ id: 'day10', reportTime: hoursAgo(24 * 10) }),
      ]),
    );
    const snap = await fetchDorsal();
    expect(snap.count).toBe(3);

    const rows = dorsalArchiveItems(snap, Math.floor(NOW / 1000), 'dorsal');
    expect(rows.map((r) => r.source_id).sort()).toEqual(['day6', 'recent']);
  });

  it('builds a row the logs page can render and filter', async () => {
    const { fetchDorsal, dorsalArchiveItems } = await freshModule();
    fetchJsonMock.mockResolvedValueOnce(
      upstream([report({ id: 42, typeOfEncounter: 'Bite', typeOfShark: 'bull shark' })]),
    );
    const snap = await fetchDorsal();
    const row = dorsalArchiveItems(snap, 1_700_000_000, 'dorsal')[0]!;

    expect(row.source).toBe('dorsal');
    expect(row.source_id).toBe('42');
    expect(row.fetched_at).toBe(1_700_000_000);
    expect(row.source_timestamp_unix).toBe(Math.floor(hoursAgo(1) / 1000));
    expect(row.lat).toBe(-35.333);
    expect(row.lng).toBe(150.481033);
    expect(row.category).toBe('incident');
    expect(row.subcategory).toBe('Bull Shark');
    expect((row.data as { title: string }).title).toContain('Bull Shark');
    expect(JSON.stringify(row)).not.toContain('Kerianne');
  });

  it('repeats the coordinate inside data so the row resolves a state', async () => {
    // archive_*_latest.state comes from stateForRow(row.data), which reads
    // lat/lng out of the JSON — NOT from the row's own lat/lng columns. Shipped
    // without these and all 181 backfilled rows landed state NULL, so the logs
    // page state filter matched none of them. Assert on stateForRow itself
    // rather than the keys, so the contract is pinned to the real consumer.
    const { fetchDorsal, dorsalArchiveItems } = await freshModule();
    const { stateForRow } = await import('../../../src/lib/stateMask.js');
    fetchJsonMock.mockResolvedValueOnce(upstream([report({ id: 7 })]));
    const row = dorsalArchiveItems(await fetchDorsal(), 1_700_000_000, 'dorsal')[0]!;

    expect(stateForRow(row.data)).toBe('NSW');
  });

  it('returns nothing for a snapshot that is empty or the wrong shape', async () => {
    const { dorsalArchiveItems } = await freshModule();
    const at = Math.floor(NOW / 1000);
    expect(dorsalArchiveItems(null, at, 'dorsal')).toEqual([]);
    expect(dorsalArchiveItems({ nope: true }, at, 'dorsal')).toEqual([]);
    expect(
      dorsalArchiveItems({ type: 'FeatureCollection', features: [], count: 0 }, at, 'dorsal'),
    ).toEqual([]);
  });
});
