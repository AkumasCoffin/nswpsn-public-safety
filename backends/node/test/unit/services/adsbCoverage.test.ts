/**
 * The persisted coverage envelope.
 *
 * Restarting the backend used to clear the coverage map, because the per-node
 * traces lived only in memory. The fix is not to store the tracks — that would
 * write the same aircraft's geometry once per receiver that heard it — but to
 * store what the picture is actually for: the furthest aircraft per bearing,
 * 72 numbers a day per node.
 *
 * Two properties carry it, and both are asserted directly:
 *
 *  - the bearing maths, because a plot whose whole purpose is "which
 *    direction" cannot afford an off-by-one bucket;
 *  - the HYDRATE step, because without it a restart at noon replaces the
 *    morning's envelope with the afternoon's, which is the exact bug this
 *    feature exists to remove.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const queryMock = vi.fn();
let poolAvailable = true;

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve(poolAvailable ? { query: queryMock } : null)),
  closePool: vi.fn(),
}));

const {
  bearingDeg, bucketOf, mergeEnvelopes, emptyEnvelope, foldCoverage,
  flushAdsbCoverage, adsbCoverageFor, clearAdsbCoverage, _resetAdsbCoverage,
  COVERAGE_BUCKETS,
} = await import('../../../src/services/nodes/adsbCoverage.js');

/** Sydney-ish. Chosen so the local day and UTC day differ, which is where day
 *  bucketing usually goes wrong. */
const SITE = { lat: -33.8688, lon: 151.2093 };
const NODE = 'node-cov-1';
/** Midday in Sydney, TODAY. Derived from the clock rather than written down:
 *  the flush drops every day that is not the current one, so a hardcoded date
 *  stops exercising the hydrate path the moment it goes stale — which it did,
 *  silently, the day after it was written. */
const T = Date.parse(
  new Intl.DateTimeFormat('en-CA', { timeZone: 'Australia/Sydney' }).format(new Date())
  + 'T02:00:00Z',
);

/** The Sydney day `back` days before T, as the `day` column holds it. Same
 *  reason as T: "the last 24 hours" is measured against the clock, so a row
 *  dated in the source stops being recent and the test stops testing. */
function dayBefore(back: number): string {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'Australia/Sydney' })
    .format(new Date(T - back * 86_400_000));
}

/** A point `km` away from SITE on `bearing`, by flat approximation — close
 *  enough at these distances to land in a known bucket. */
function at(bearingDegrees: number, km: number) {
  const rad = (bearingDegrees * Math.PI) / 180;
  const dLat = (km * Math.cos(rad)) / 111.19;
  const dLon = (km * Math.sin(rad)) / (111.19 * Math.cos((SITE.lat * Math.PI) / 180));
  return { lat: SITE.lat + dLat, lon: SITE.lon + dLon };
}

beforeEach(() => {
  _resetAdsbCoverage();
  queryMock.mockReset();
  queryMock.mockResolvedValue({ rows: [] });
  poolAvailable = true;
});

describe('bearing and bucketing', () => {
  it('points north, east, south and west', () => {
    expect(bearingDeg(SITE.lat, SITE.lon, SITE.lat + 1, SITE.lon)).toBeCloseTo(0, 1);
    expect(bearingDeg(SITE.lat, SITE.lon, SITE.lat, SITE.lon + 1)).toBeCloseTo(90, 0);
    expect(bearingDeg(SITE.lat, SITE.lon, SITE.lat - 1, SITE.lon)).toBeCloseTo(180, 1);
    expect(bearingDeg(SITE.lat, SITE.lon, SITE.lat, SITE.lon - 1)).toBeCloseTo(270, 0);
  });

  it('buckets 5 degrees to a slice', () => {
    expect(bucketOf(0)).toBe(0);
    expect(bucketOf(4.9)).toBe(0);
    expect(bucketOf(5)).toBe(1);
    expect(bucketOf(90)).toBe(18);
    expect(bucketOf(180)).toBe(36);
    expect(bucketOf(270)).toBe(54);
  });

  it('never runs off the end of the array', () => {
    // 359.9 / 5 is 71.98 — the floor is 71, but a naive round would be 72 and
    // write past the last bucket.
    expect(bucketOf(359.9)).toBe(COVERAGE_BUCKETS - 1);
    expect(bucketOf(360)).toBe(0);
    expect(bucketOf(-5)).toBe(COVERAGE_BUCKETS - 1);
    expect(bucketOf(725)).toBe(1);
  });
});

describe('mergeEnvelopes', () => {
  it('takes the larger of each bearing', () => {
    const a = emptyEnvelope(); a[0] = 100; a[1] = 50;
    const b = emptyEnvelope(); b[0] = 80; b[1] = 90; b[2] = 30;
    mergeEnvelopes(a, b);
    expect(a[0]).toBe(100);
    expect(a[1]).toBe(90);
    expect(a[2]).toBe(30);
  });

  it('keeps a never-heard bearing as null, not zero', () => {
    // The null IS the gap in the fan — the whole diagnostic. Collapsing it to
    // zero would draw a lobe of length nothing instead of a hole.
    const a = emptyEnvelope();
    mergeEnvelopes(a, emptyEnvelope());
    expect(a[10]).toBeNull();
    expect(a.every((v) => v === null)).toBe(true);
  });

  it('ignores junk in a stored row', () => {
    const a = emptyEnvelope(); a[0] = 100;
    mergeEnvelopes(a, ['nope' as unknown as number, Number.NaN, null]);
    expect(a[0]).toBe(100);
    expect(a[1]).toBeNull();
  });
});

describe('a single bad fix must not become coverage', () => {
  // A lone spike reached a hundred kilometres into airspace with no track
  // anywhere near it, and nothing could ever lower it again: the envelope was
  // a running maximum over SINGLE observations, kept forever. ADS-B supplies
  // plenty of candidates — a CPR decode error puts an aircraft tens or
  // hundreds of kilometres from where it actually is, once.
  it('ignores a range seen only once', async () => {
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 60), at(0, 60)], T);  // real
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 400)], T);            // garbage
    await flushAdsbCoverage();
    const buckets = JSON.parse(queryMock.mock.calls.at(-1)![1][2] as string);
    expect(buckets[0]).toBeGreaterThan(55);
    expect(buckets[0]).toBeLessThan(65);
  });

  it('believes a range as soon as it is seen twice', async () => {
    // Corroboration has to be cheap for real traffic: an aircraft in range
    // reports every few seconds, so genuine reach is confirmed almost at once.
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 60), at(0, 60)], T);
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 210)], T);
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 205)], T);
    await flushAdsbCoverage();
    const buckets = JSON.parse(queryMock.mock.calls.at(-1)![1][2] as string);
    expect(buckets[0]).toBeGreaterThan(200);
  });

  it('keeps the runner-up, not the outlier, when both are new', async () => {
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 300), at(0, 80), at(0, 75)], T);
    await flushAdsbCoverage();
    const buckets = JSON.parse(queryMock.mock.calls.at(-1)![1][2] as string);
    expect(buckets[0]).toBeLessThan(100);
  });
});

describe('folding an upload', () => {
  it('records the furthest aircraft on each bearing', async () => {
    // Twice, because one observation is no longer enough — see the
    // outlier test below for why.
    for (let i = 0; i < 2; i += 1) {
      foldCoverage(NODE, SITE.lat, SITE.lon, [
        at(0, 120),     // due north
        at(90, 200),    // due east
        at(90, 150),    // also east, but nearer — must not lower it
      ], T);
    }
    await flushAdsbCoverage();
    const buckets = JSON.parse(queryMock.mock.calls.at(-1)![1][2] as string);
    expect(buckets[0]).toBeGreaterThan(115);
    expect(buckets[0]).toBeLessThan(125);
    expect(buckets[18]).toBeGreaterThan(195);
    expect(buckets[18]).toBeLessThan(205);
    expect(buckets[36]).toBeNull();
  });

  it('returns the furthest aircraft overall, for the range figure', () => {
    const km = foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 50), at(180, 210)], T);
    expect(km).toBeGreaterThan(205);
    expect(km).toBeLessThan(215);
  });

  it('records nothing without an antenna pin', async () => {
    // Nothing to measure a bearing or a distance FROM.
    expect(foldCoverage(NODE, null, null, [at(0, 120)], T)).toBeNull();
    expect(await flushAdsbCoverage()).toBe(0);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('a running receiver still produces ONE row per day', async () => {
    // The whole storage argument. 720 uploads (an hour at 5s) must not be 720
    // rows, or this is the per-position table it was designed not to be.
    for (let i = 0; i < 720; i += 1) {
      foldCoverage(NODE, SITE.lat, SITE.lon, [at(i % 360, 50 + (i % 100))], T);
      foldCoverage(NODE, SITE.lat, SITE.lon, [at(i % 360, 50 + (i % 100))], T);
    }
    expect(await flushAdsbCoverage()).toBe(1);
    const inserts = queryMock.mock.calls.filter((c) => String(c[0]).includes('INSERT INTO'));
    expect(inserts).toHaveLength(1);
  });

  it('keeps separate rows for separate nodes', async () => {
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 100), at(0, 100)], T);
    foldCoverage('other-node', SITE.lat, SITE.lon, [at(0, 100), at(0, 100)], T);
    expect(await flushAdsbCoverage()).toBe(2);
  });
});

describe('hydration — surviving a restart', () => {
  it('merges the stored row before its first write', async () => {
    // THE test. A backend restarted at noon starts with an empty envelope; if
    // it wrote that straight out, the morning's picture would be gone.
    queryMock.mockImplementation((sql: string) => {
      if (String(sql).includes('SELECT buckets')) {
        const morning = emptyEnvelope();
        morning[0] = 180;      // heard 180 km north this morning
        return Promise.resolve({ rows: [{ buckets: morning }] });
      }
      return Promise.resolve({ rows: [] });
    });

    foldCoverage(NODE, SITE.lat, SITE.lon, [at(90, 60), at(90, 60)], T);   // only east, now
    await flushAdsbCoverage();

    const written = JSON.parse(queryMock.mock.calls.at(-1)![1][2] as string);
    expect(written[0]).toBe(180);                 // the morning survived
    expect(written[18]).toBeGreaterThan(55);      // and the afternoon is there
  });

  it('only hydrates once', async () => {
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 100), at(0, 100)], T);
    await flushAdsbCoverage();
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 120)], T);
    await flushAdsbCoverage();
    const selects = queryMock.mock.calls.filter((c) => String(c[0]).includes('SELECT buckets'));
    expect(selects).toHaveLength(1);
  });

  it('keeps accumulating in memory across flushes', async () => {
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 100), at(0, 100)], T);
    await flushAdsbCoverage();
    // A later upload that hears nothing to the north must not erase the north.
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(90, 40), at(90, 40)], T);
    await flushAdsbCoverage();
    const written = JSON.parse(queryMock.mock.calls.at(-1)![1][2] as string);
    expect(written[0]).toBeGreaterThan(95);
    expect(written[18]).toBeGreaterThan(35);
  });

  it('retries rather than dropping the day when a write fails', async () => {
    queryMock.mockRejectedValue(new Error('connection reset'));
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 100), at(0, 100)], T);
    await expect(flushAdsbCoverage()).resolves.toBe(0);

    // The envelope is a running maximum, so a failed pass costs nothing.
    queryMock.mockReset();
    queryMock.mockResolvedValue({ rows: [] });
    expect(await flushAdsbCoverage()).toBe(1);
  });

  it('is a no-op without a database', async () => {
    poolAvailable = false;
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 100), at(0, 100)], T);
    expect(await flushAdsbCoverage()).toBe(0);
  });
});

describe('reading it back', () => {
  it('unions the window and separates the last 24 hours', async () => {
    const old = emptyEnvelope(); old[0] = 200;    // a week ago, heard north
    const today = emptyEnvelope(); today[18] = 90; // today, only east
    queryMock.mockResolvedValue({
      rows: [
        { day: dayBefore(7), buckets: old },
        { day: dayBefore(0), buckets: today },
      ],
    });

    const c = await adsbCoverageFor(NODE, 31, T);
    expect(c!.buckets[0]).toBe(200);
    expect(c!.buckets[18]).toBe(90);
    // The north lobe is in the long envelope but NOT in the recent one — which
    // is precisely how a lobe that has stopped working becomes visible.
    expect(c!.recent[0]).toBeNull();
    expect(c!.recent[18]).toBe(90);
    expect(c!.daysCovered).toBe(2);
    expect(c!.maxKm).toBe(200);
  });

  it('folds in today’s unflushed picture', async () => {
    // Otherwise a freshly-started receiver shows an empty map for a minute.
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 130), at(0, 130)], T);
    const c = await adsbCoverageFor(NODE, 31, T);
    expect(c!.buckets[0]).toBeGreaterThan(125);
    expect(c!.recent[0]).toBeGreaterThan(125);
  });

  it('reports nothing rather than an empty circle for a silent node', async () => {
    const c = await adsbCoverageFor('quiet-node', 31, T);
    expect(c!.maxKm).toBeNull();
    expect(c!.daysCovered).toBe(0);
    expect(c!.buckets.every((v) => v === null)).toBe(true);
  });

  it('returns null without a database', async () => {
    poolAvailable = false;
    expect(await adsbCoverageFor(NODE, 31, T)).toBeNull();
  });
});

describe('deleting a node', () => {
  it('forgets its in-flight envelope', async () => {
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 100), at(0, 100)], T);
    clearAdsbCoverage(NODE);
    expect(await flushAdsbCoverage()).toBe(0);
  });

  it('leaves other nodes alone', async () => {
    foldCoverage(NODE, SITE.lat, SITE.lon, [at(0, 100), at(0, 100)], T);
    foldCoverage('keep-me', SITE.lat, SITE.lon, [at(0, 100)], T);
    clearAdsbCoverage(NODE);
    expect(await flushAdsbCoverage()).toBe(1);
  });
});
