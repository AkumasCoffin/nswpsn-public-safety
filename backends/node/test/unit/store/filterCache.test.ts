/**
 * Tests for the in-memory filter facet aggregator. We seed the
 * LiveStore with synthetic snapshots, force a refresh, and assert the
 * computed shape against the contract Python's
 * `_build_filters_response` defines.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { liveStore } from '../../../src/store/live.js';
import {
  _resetFilterCacheForTests,
  canonicalAlertType,
  getFilterFacets,
  refreshFilterCache,
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
    refreshFilterCache();
    const facets = getFilterFacets();
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
    refreshFilterCache();
    const facets = getFilterFacets();
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
    refreshFilterCache();
    const pager = findProvider(getFilterFacets(), 'pager');
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
    refreshFilterCache();
    const lt = findProvider(getFilterFacets(), 'livetraffic');
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
    refreshFilterCache();
    const waze = findProvider(getFilterFacets(), 'waze');
    const police = findType(waze, 'waze_police');
    expect(police?.count).toBe(2);
  });

  it('case-insensitively merges duplicate values, preserving the dominant casing', () => {
    liveStore.set('rfs', [
      { category: 'Bushfire' },
      { category: 'Bushfire' },
      { category: 'BUSHFIRE' },
    ]);
    refreshFilterCache();
    const rfsType = findType(findProvider(getFilterFacets(), 'rfs'), 'rfs');
    const merged = rfsType?.categories ?? [];
    // Only one row — the case variants collapsed.
    expect(merged).toHaveLength(1);
    // Dominant casing wins (2× "Bushfire" beats 1× "BUSHFIRE").
    expect(merged[0]?.value).toBe('Bushfire');
    expect(merged[0]?.count).toBe(3);
  });

  it('scopes response to a single provider when `source` filter is given', () => {
    liveStore.set('rfs', [{ category: 'Bushfire' }]);
    liveStore.set('waze_police', { alerts: [{ category: 'POLICE' }], jams: [] });
    refreshFilterCache();
    const facets = getFilterFacets('waze_police');
    expect(facets.providers).toHaveLength(1);
    expect(facets.providers[0]?.key).toBe('waze');
    expect(facets.providers[0]?.types).toHaveLength(1);
    expect(facets.providers[0]?.types[0]?.alert_type).toBe('waze_police');
  });

  it('returns empty providers when source filter doesn\'t match anything', () => {
    refreshFilterCache();
    const facets = getFilterFacets('nonsense_source_xyz');
    expect(facets.providers).toEqual([]);
  });

  it('exposes liveStore snapshot timestamps as date_range', () => {
    liveStore.set('rfs', [{ category: 'Bushfire' }]);
    refreshFilterCache();
    const facets = getFilterFacets();
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
