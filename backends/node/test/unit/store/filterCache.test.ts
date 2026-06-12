/**
 * Tests for the in-memory filter facet aggregator. We seed the
 * LiveStore with synthetic snapshots and read the LiveStore-only
 * variant (`getFilterFacetsLive`) which has no DB dependency. The
 * production async path (`getFilterFacets`) goes through the same
 * `buildResponse` shaping plus an archive_*_latest sidecar query —
 * its semantics are covered indirectly by sources/* tests and direct
 * end-to-end tests against the real DB.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { liveStore } from '../../../src/store/live.js';
import {
  _resetFilterCacheForTests,
  canonicalAlertType,
  getFilterFacetsLive,
  type ProviderFacets,
  type TypeFacets,
} from '../../../src/store/filterCache.js';

function clearLive(): void {
  for (const k of liveStore.keys()) liveStore.delete(k);
}

function findProvider(facets: { providers: ProviderFacets[] }, key: string): ProviderFacets | undefined {
  return facets.providers.find((p) => p.key === key);
}

function findType(provider: ProviderFacets | undefined, alertType: string): TypeFacets | undefined {
  return provider?.types.find((t) => t.alert_type === alertType);
}

describe('filterCache', () => {
  beforeEach(() => {
    clearLive();
    _resetFilterCacheForTests();
  });

  it('returns the full provider list when LiveStore is empty', () => {
    const facets = getFilterFacetsLive();
    // 10 providers: rfs, bom, livetraffic, endeavour, ausgrid, essential,
    // waze, pager, user, rdio.
    expect(facets.providers).toHaveLength(10);
    for (const p of facets.providers) {
      expect(p.count).toBe(0);
    }
    expect(facets.date_range.oldest).toBeNull();
    expect(facets.date_range.newest).toBeNull();
  });

  it('counts records from an array snapshot', () => {
    liveStore.set('rfs', [
      { category: 'Bushfire', status: 'Advice' },
      { category: 'Bushfire', status: 'Watch and Act' },
      { category: 'Hazard Reduction', status: 'Advice' },
    ]);
    const facets = getFilterFacetsLive();
    const rfs = findProvider(facets, 'rfs');
    expect(rfs?.count).toBe(3);
    const rfsType = findType(rfs, 'rfs');
    expect(rfsType?.count).toBe(3);

    const bushfire = rfsType?.categories.find((c) => c.value === 'Bushfire');
    expect(bushfire?.count).toBe(2);
    const advice = rfsType?.statuses.find((s) => s.value === 'Advice');
    expect(advice?.count).toBe(2);
  });

  it('drops numeric subcategory values (pager capcode noise)', () => {
    liveStore.set('pager', [
      { subcategory: '1160008', category: 'NSWFR' },
      { subcategory: 'Welfare', category: 'NSWFR' },
    ]);
    const pager = findProvider(getFilterFacetsLive(), 'pager');
    const pagerType = findType(pager, 'pager');
    const subs = pagerType?.subcategories.map((s) => s.value);
    expect(subs).toContain('Welfare');
    expect(subs).not.toContain('1160008');
  });

  it('extracts dimensions from a GeoJSON-style { features: [...] } snapshot', () => {
    liveStore.set('traffic_incident', {
      features: [
        { properties: { category: 'CRASH', severity: 'Major' } },
        { properties: { category: 'CRASH', severity: 'Minor' } },
        { properties: { category: 'HAZARD', severity: 'Minor' } },
      ],
    });
    const lt = findProvider(getFilterFacetsLive(), 'livetraffic');
    const incidents = findType(lt, 'traffic_incident');
    expect(incidents?.count).toBe(3);
    const crash = incidents?.categories.find((c) => c.value === 'CRASH');
    expect(crash?.count).toBe(2);
  });

  it('flattens waze-style { alerts, jams } snapshots into one type bucket', () => {
    liveStore.set('waze_police', {
      alerts: [{ category: 'POLICE' }, { category: 'POLICE' }],
      jams: [],
    });
    const waze = findProvider(getFilterFacetsLive(), 'waze');
    const police = findType(waze, 'waze_police');
    expect(police?.count).toBe(2);
  });

  it('routes JAM-typed waze alert points to waze_jam, not waze_hazard', () => {
    // Bbox-keyed 'waze' snapshot exercises the wazeAlertType classifier.
    // A JAM-typed alert point must count under waze_jam (alongside jam
    // polylines), never inflate the Hazards bucket — mirrors the ingest
    // split so the live facets match the DB-backed catalogue.
    liveStore.set('waze', {
      bboxes: {
        '0': {
          alerts: [
            { uuid: 'a1', type: 'JAM', subtype: 'JAM_HEAVY_TRAFFIC' },
            { uuid: 'a2', type: 'HAZARD', subtype: 'HAZARD_ON_ROAD_POT_HOLE' },
            { uuid: 'a3', type: 'ACCIDENT' },
          ],
          jams: [{ uuid: 'j1' }],
        },
      },
    });
    const waze = findProvider(getFilterFacetsLive(), 'waze');
    // waze_jam = 1 JAM-typed alert point + 1 jam polyline.
    expect(findType(waze, 'waze_jam')?.count).toBe(2);
    // waze_hazard = HAZARD + ACCIDENT only; the JAM alert is excluded.
    expect(findType(waze, 'waze_hazard')?.count).toBe(2);
    const hazardSubs = findType(waze, 'waze_hazard')?.subcategories.map((s) => s.value) ?? [];
    expect(hazardSubs).not.toContain('JAM_HEAVY_TRAFFIC');
  });

  it('case-insensitively merges duplicate values, preserving the dominant casing', () => {
    // Include a second category so the merged dim has ≥2 entries and
    // doesn't get dropped by the trivial-dim filter in buildResponse
    // (a single-value 100%-of-total dim is treated as a constant).
    liveStore.set('rfs', [
      { category: 'Bushfire' },
      { category: 'Bushfire' },
      { category: 'BUSHFIRE' },
      { category: 'Grass Fire' },
    ]);
    const rfsType = findType(findProvider(getFilterFacetsLive(), 'rfs'), 'rfs');
    const merged = rfsType?.categories ?? [];
    // Two distinct buckets after case merge.
    expect(merged).toHaveLength(2);
    const bushfire = merged.find((c) => c.value === 'Bushfire');
    // Dominant casing wins (2× "Bushfire" beats 1× "BUSHFIRE").
    expect(bushfire?.count).toBe(3);
  });

  it('scopes response to a single provider when `source` filter is given', () => {
    liveStore.set('rfs', [{ category: 'Bushfire' }]);
    liveStore.set('waze_police', { alerts: [{ category: 'POLICE' }], jams: [] });
    const facets = getFilterFacetsLive('waze_police');
    expect(facets.providers).toHaveLength(1);
    expect(facets.providers[0]?.key).toBe('waze');
    expect(facets.providers[0]?.types).toHaveLength(1);
    expect(facets.providers[0]?.types[0]?.alert_type).toBe('waze_police');
  });

  it('returns empty providers when source filter doesn\'t match anything', () => {
    const facets = getFilterFacetsLive('nonsense_source_xyz');
    expect(facets.providers).toEqual([]);
  });

  it('exposes liveStore snapshot timestamps as date_range', () => {
    liveStore.set('rfs', [{ category: 'Bushfire' }]);
    const facets = getFilterFacetsLive();
    expect(typeof facets.date_range.oldest_unix).toBe('number');
    expect(typeof facets.date_range.newest_unix).toBe('number');
    expect(facets.date_range.oldest).not.toBeNull();
  });

  it('canonicalAlertType folds legacy source names', () => {
    expect(canonicalAlertType('livetraffic')).toBe('traffic_incident');
    expect(canonicalAlertType('bom_warning')).toBe('bom_land');
    expect(canonicalAlertType('endeavour')).toBe('endeavour_current');
  });

  it('canonicalAlertType returns null for empty input', () => {
    expect(canonicalAlertType(null)).toBeNull();
    expect(canonicalAlertType('')).toBeNull();
  });
});
