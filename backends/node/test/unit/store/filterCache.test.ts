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
    // 13 providers: the four fire agencies (rfs, nt_fire, qfd, vic) plus
    // bom, livetraffic, endeavour, ausgrid, essential, pager, user, rdio,
    // actas. Waze was retired along with its ingest.
    expect(facets.providers).toHaveLength(13);
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
    const facets = getFilterFacetsLive('rfs');
    expect(facets.providers).toHaveLength(1);
    expect(facets.providers[0]?.key).toBe('rfs');
    expect(facets.providers[0]?.types).toHaveLength(1);
    expect(facets.providers[0]?.types[0]?.alert_type).toBe('rfs');
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

  it('drops a subcategory list that only mirrors the status list', () => {
    // ACT Ambulance writes its status into `subcategory` as well as
    // `status`, so the logs page drew the same three values under
    // "Category / Subtype" and under "Status", where picking either did
    // the same thing.
    liveStore.set('act_ambulance', [
      { category: 'Ambulance', subcategory: 'On Scene', status: 'On Scene' },
      { category: 'Ambulance', subcategory: 'On Scene', status: 'On Scene' },
      { category: 'Ambulance', subcategory: 'Units On Route', status: 'Units On Route' },
    ]);
    const actas = findType(findProvider(getFilterFacetsLive(), 'actas'), 'act_ambulance');
    expect(actas?.statuses.map((e) => e.value).sort()).toEqual(['On Scene', 'Units On Route']);
    expect(actas?.subcategories).toEqual([]);
  });

  it('keeps subcategories when they say something the statuses do not', () => {
    // RFS: subcategory is the fire type, status is the control state.
    // Two genuinely different dimensions that must both stay filterable.
    liveStore.set('rfs', [
      { category: 'Advice', subcategory: 'Bush Fire', status: 'Under control' },
      { category: 'Advice', subcategory: 'Grass Fire', status: 'Out of control' },
    ]);
    const rfsType = findType(findProvider(getFilterFacetsLive(), 'rfs'), 'rfs');
    expect(rfsType?.subcategories.map((e) => e.value).sort()).toEqual(['Bush Fire', 'Grass Fire']);
    expect(rfsType?.statuses.map((e) => e.value).sort()).toEqual(['Out of control', 'Under control']);
  });

  it('keeps a subcategory list that overlaps the statuses without matching them', () => {
    // Same length, one value shared — not a mirror, so nothing is dropped.
    liveStore.set('rfs', [
      { subcategory: 'Going', status: 'Going' },
      { subcategory: 'Bush Fire', status: 'Patrolled' },
    ]);
    const rfsType = findType(findProvider(getFilterFacetsLive(), 'rfs'), 'rfs');
    expect(rfsType?.subcategories.length).toBe(2);
  });

  it('gives each fire agency its own provider rather than one "Fires" group', () => {
    // A provider is an organisation and a type is one of its feeds, the
    // same shape Endeavour Energy and the ACT Ambulance Service have.
    liveStore.set('rfs', [{ category: 'Advice', status: 'Under control' }]);
    liveStore.set('nt_fire', [{ status: 'Going' }]);
    liveStore.set('qld_fire', [{ status: 'Going' }]);
    liveStore.set('qld_warning', [{ category: 'Advice' }]);
    liveStore.set('vic_emergency', [{ category: 'Advice' }]);
    const facets = getFilterFacetsLive();

    const named = (key: string) => findProvider(facets, key)?.name;
    expect(named('rfs')).toBe('NSW RFS');
    expect(named('nt_fire')).toBe('NT Fire & Rescue');
    expect(named('qfd')).toBe('QLD Fire Dept');
    expect(named('vic')).toBe('VIC Emergency');
    // The old catch-all is gone, not merely renamed.
    expect(facets.providers.some((p) => p.name === 'Fires')).toBe(false);

    // Queensland keeps two feeds under the one agency; the others have one.
    expect(findProvider(facets, 'qfd')?.types.map((t) => t.name)).toEqual(['Incidents', 'Warnings']);
    expect(findProvider(facets, 'rfs')?.types.map((t) => t.alert_type)).toEqual(['rfs']);
    expect(findProvider(facets, 'vic')?.types.map((t) => t.name)).toEqual(['Events']);
  });

  it('counts each fire agency against its own provider', () => {
    liveStore.set('rfs', [{ status: 'Going' }, { status: 'Going' }]);
    liveStore.set('qld_fire', [{ status: 'Going' }]);
    liveStore.set('qld_warning', [{ status: 'Stay Informed' }]);
    const facets = getFilterFacetsLive();
    expect(findProvider(facets, 'rfs')?.count).toBe(2);
    // Both Queensland feeds roll up to the one agency.
    expect(findProvider(facets, 'qfd')?.count).toBe(2);
    expect(findProvider(facets, 'nt_fire')?.count).toBe(0);
  });

  it('scopes to the agency when filtered by one of its alert types', () => {
    liveStore.set('qld_fire', [{ status: 'Going' }]);
    liveStore.set('rfs', [{ status: 'Going' }]);
    const facets = getFilterFacetsLive('qld_fire');
    expect(facets.providers.map((p) => p.key)).toEqual(['qfd']);
    expect(facets.providers[0]?.types.map((t) => t.alert_type)).toEqual(['qld_fire']);
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
