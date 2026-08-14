import { describe, it, expect } from 'vitest';
import { slugify, viewerHash, shapeMedia, normaliseLicense, licenseLabel, WIRE_LICENSES, r2TransformUrl, avatarUrl, ogImageUrl, type WireMediaRow } from '../../../src/services/wire.js';

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
  it('serves R2 images and round-trips the key when includeKeys', () => {
    const out = shapeMedia(mediaRow({ kind: 'image', r2_key: 'wire/img/abc.webp', is_cover: true, unit: 'P421' }), true);
    expect(out['kind']).toBe('image');
    expect(out['is_cover']).toBe(true);
    expect(out['unit']).toBe('P421');
    expect(out['r2_key']).toBe('wire/img/abc.webp');
    expect(String(out['url'])).toContain('abc.webp');
  });
  it('cuts smaller variants via Cloudflare, leaving the full size untransformed', () => {
    const out = shapeMedia(mediaRow({ kind: 'image', r2_key: 'wire/img/abc.webp' }));
    // The stored file is already capped at 1600px, so transforming it at full
    // size would buy nothing and cost a billable variant.
    expect(String(out['url'])).not.toContain('/cdn-cgi/');
    expect(String(out['feed_url'])).toContain('/cdn-cgi/image/width=640');
    expect(String(out['thumb_url'])).toContain('/cdn-cgi/image/width=160');
    expect(String(out['thumb_url'])).toContain('abc.webp');
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
    // The video itself is never transformed — only its poster still image is.
    expect(String(out['url'])).not.toContain('/cdn-cgi/');
  });
  it('sizes a video poster for both the player and the card', () => {
    const out = shapeMedia(mediaRow({ kind: 'video', r2_key: 'vids/x.mp4', poster_r2_key: 'wire/img/p.jpg' }));
    expect(String(out['poster_url'])).toContain('/cdn-cgi/image/width=1280');
    expect(String(out['poster_feed_url'])).toContain('/cdn-cgi/image/width=640');
  });
  it('flags a video that is still awaiting the ffmpeg pass', () => {
    expect(shapeMedia(mediaRow({ kind: 'video', r2_key: 'v.mp4', process_state: 'pending' }))['processing']).toBe(true);
    expect(shapeMedia(mediaRow({ kind: 'video', r2_key: 'v.mp4', process_state: 'done' }))).not.toHaveProperty('processing');
    expect(shapeMedia(mediaRow({ kind: 'video', r2_key: 'v.mp4' }))).not.toHaveProperty('processing');
  });
});

describe('wire.r2TransformUrl', () => {
  it('builds a /cdn-cgi/image path that never upscales', () => {
    const u = r2TransformUrl('wire/img/a b.webp', { width: 320, quality: 70 });
    expect(u).toContain('/cdn-cgi/image/width=320,quality=70,format=auto,fit=scale-down/');
    // Key segments stay percent-encoded, as in r2PublicUrl.
    expect(u).toContain('a%20b.webp');
  });
});

describe('wire.avatarUrl', () => {
  it('sizes an own-R2 avatar for where it renders', () => {
    expect(String(avatarUrl('av/a.webp', null))).toContain('width=80');
    expect(String(avatarUrl('av/a.webp', null, 'large'))).toContain('width=256');
  });
  it('passes a Discord CDN url through untouched — it is not on our zone', () => {
    const d = 'https://cdn.discordapp.com/avatars/1/2.png';
    expect(avatarUrl(null, d)).toBe(d);
    expect(avatarUrl(null, null)).toBeNull();
  });
  it('prefers the uploaded avatar over the Discord fallback', () => {
    expect(String(avatarUrl('av/a.webp', 'https://cdn.discordapp.com/x.png'))).toContain('av/a.webp');
  });
});

describe('wire.ogImageUrl', () => {
  // Link-preview crawlers get the RAW object URL, never a transform. Discord's
  // image proxy choked on the comma-laden /cdn-cgi/ form, which is why embeds
  // showed no image at all — the pages keep using transforms, the crawlers don't.
  it('strips a Cloudflare transform prefix', () => {
    expect(ogImageUrl('https://media.forcequit.xyz/cdn-cgi/image/width=640,quality=82,format=auto,fit=scale-down/wire/img/a.webp'))
      .toBe('https://media.forcequit.xyz/wire/img/a.webp');
  });
  it('leaves an already-plain url alone', () => {
    const u = 'https://media.forcequit.xyz/wire/img/a.webp';
    expect(ogImageUrl(u)).toBe(u);
  });
  it('passes through null', () => {
    expect(ogImageUrl(null)).toBeNull();
    expect(ogImageUrl('')).toBeNull();
  });
});
