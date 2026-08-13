import { describe, it, expect } from 'vitest';
import { slugify, viewerHash, shapeMedia, normaliseLicense, licenseLabel, WIRE_LICENSES, type WireMediaRow } from '../../../src/services/wire.js';

function mediaRow(over: Partial<WireMediaRow>): WireMediaRow {
  return {
    id: 'm1', parent_type: 'media_post', parent_id: 'p1', kind: 'image',
    cf_image_id: null, r2_key: null, poster_cf_image_id: null, poster_r2_key: null, duration_seconds: null,
    is_cover: false, unit: null, sort_order: 0, width: null, height: null, bytes: null,
    ...over,
  };
}

describe('wire.slugify', () => {
  it('lowercases, hyphenates and trims', () => {
    expect(slugify('Ridge-line Backburn Holds!')).toBe('ridge-line-backburn-holds');
  });
  it('collapses runs and strips edges', () => {
    expect(slugify('  Hello   World  ')).toBe('hello-world');
  });
  it('falls back for empty/symbol-only input', () => {
    expect(slugify('')).toBe('article');
    expect(slugify('!!!')).toBe('article');
  });
});

describe('wire.viewerHash', () => {
  it('is deterministic for the same inputs', () => {
    expect(viewerHash('1.2.3.4', 'UA')).toBe(viewerHash('1.2.3.4', 'UA'));
  });
  it('differs when the ip or ua changes', () => {
    expect(viewerHash('1.2.3.4', 'UA')).not.toBe(viewerHash('1.2.3.5', 'UA'));
    expect(viewerHash('1.2.3.4', 'UA')).not.toBe(viewerHash('1.2.3.4', 'OTHER'));
  });
});

describe('wire.license', () => {
  it('defaults unknown/blank to credit-required', () => {
    expect(normaliseLicense(undefined)).toBe('credit');
    expect(normaliseLicense('nope')).toBe('credit');
    expect(normaliseLicense('display')).toBe('display');
    expect(normaliseLicense('public')).toBe('public');
  });
  it('labels use the plain-language names', () => {
    expect(WIRE_LICENSES['credit']).toBe('Credit required');
    expect(WIRE_LICENSES['display']).toBe('All rights reserved');
    expect(WIRE_LICENSES['public']).toBe('Public domain');
    expect(licenseLabel('bogus')).toBe('Credit required');
  });
});

describe('wire.shapeMedia', () => {
  it('serves R2 images (single optimised size) and round-trips the key when includeKeys', () => {
    const out = shapeMedia(mediaRow({ kind: 'image', r2_key: 'wire/img/abc.webp', is_cover: true, unit: 'P421' }), true);
    expect(out['kind']).toBe('image');
    expect(out['is_cover']).toBe(true);
    expect(out['unit']).toBe('P421');
    expect(out['r2_key']).toBe('wire/img/abc.webp');
    expect(String(out['url'])).toContain('abc.webp');
    expect(out['thumb_url']).toBe(out['url']);
  });
  it('hides storage keys + hash from public (default) responses', () => {
    const out = shapeMedia(mediaRow({ kind: 'image', r2_key: 'wire/img/abc.webp', hash: 'deadbeef' }));
    // Delivery URL is still present so the image renders...
    expect(String(out['url'])).toContain('abc.webp');
    // ...but the internal storage key, cf id and content hash are NOT exposed.
    expect(out).not.toHaveProperty('r2_key');
    expect(out).not.toHaveProperty('cf_image_id');
    expect(out).not.toHaveProperty('hash');
    expect(out).not.toHaveProperty('poster_r2_key');
  });
  it('falls back to Cloudflare variants for legacy cf-only image rows', () => {
    const out = shapeMedia(mediaRow({ kind: 'image', cf_image_id: 'abc' }));
    expect(String(out['url'])).toContain('/abc/public');
  });
  it('shapes a video with its r2 public url', () => {
    const out = shapeMedia(mediaRow({ kind: 'video', r2_key: 'vids/x.mp4' }), true);
    expect(out['kind']).toBe('video');
    expect(out['r2_key']).toBe('vids/x.mp4');
    // url is present (exact host depends on R2_PUBLIC_BASE env; may be undefined base)
    expect(out).toHaveProperty('url');
  });
});
