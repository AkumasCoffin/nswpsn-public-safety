/**
 * Wire edit-log diffing. The rules that matter here are what gets a VALUE
 * recorded (short fields) versus only a flag (prose), and what counts as no
 * change at all — an empty diff is what stops a no-op save appearing in a
 * reader-facing history.
 */
import { describe, it, expect } from 'vitest';
import { diffSnapshots, mediaKeyOf, type EditSnapshot } from '../../../src/services/wireEdits.js';

const base: EditSnapshot = {
  title: 'Ridge-line backburn holds',
  caption: 'Crews held the line overnight.',
  location: { type: 'region', region: 'Blue Mountains' },
  agencies: ['RFS', 'FRNSW'],
  incidentId: 'inc-1',
  license: 'credit',
  credit: null,
  watermark: false,
  coAuthors: [],
  mediaKeys: ['wire/img/a.webp', 'wire/img/b.webp'],
};

describe('diffSnapshots', () => {
  it('reports nothing when nothing changed', () => {
    expect(diffSnapshots(base, { ...base })).toEqual([]);
  });

  it('keeps before/after for short fields', () => {
    const d = diffSnapshots(base, { ...base, title: 'Backburn holds at Mount Vic' });
    expect(d).toHaveLength(1);
    expect(d[0]).toMatchObject({ field: 'title', label: 'Title', from: 'Ridge-line backburn holds', to: 'Backburn holds at Mount Vic' });
  });

  it('flags prose changes WITHOUT storing the text', () => {
    const d = diffSnapshots(base, { ...base, caption: `${base.caption} A second engine arrived at 0400.` });
    expect(d).toHaveLength(1);
    expect(d[0]?.field).toBe('caption');
    expect(d[0]?.detail).toMatch(/expanded by \d+ characters/);
    // The whole point: the body/caption text is never copied into the log.
    expect(d[0]).not.toHaveProperty('from');
    expect(d[0]).not.toHaveProperty('to');
  });

  it('distinguishes added / removed / shortened prose', () => {
    expect(diffSnapshots({ ...base, body: '' }, { ...base, body: 'x' })[0]?.detail).toBe('added');
    expect(diffSnapshots({ ...base, body: 'x' }, { ...base, body: '' })[0]?.detail).toBe('removed');
    expect(diffSnapshots({ ...base, body: 'abcd' }, { ...base, body: 'ab' })[0]?.detail).toBe('shortened by 2 characters');
    expect(diffSnapshots({ ...base, body: 'ab' }, { ...base, body: 'cd' })[0]?.detail).toBe('reworded');
  });

  it('ignores agency reordering but catches a real change', () => {
    expect(diffSnapshots(base, { ...base, agencies: ['FRNSW', 'RFS'] })).toEqual([]);
    const d = diffSnapshots(base, { ...base, agencies: ['RFS'] });
    expect(d[0]).toMatchObject({ field: 'agencies', from: 'FRNSW, RFS', to: 'RFS' });
  });

  it('compares a pin at 4dp, so a metre of drift is not an edit', () => {
    const pin = { ...base, location: { type: 'pin', lat: -33.712345, lng: 150.312345 } };
    expect(diffSnapshots(pin, { ...pin, location: { type: 'pin', lat: -33.7123451, lng: 150.312345 } })).toEqual([]);
    expect(diffSnapshots(pin, { ...pin, location: { type: 'pin', lat: -33.9, lng: 150.312345 } })).toHaveLength(1);
  });

  it('reports media by identity, not count — a swap is a change', () => {
    const d = diffSnapshots(base, { ...base, mediaKeys: ['wire/img/a.webp', 'wire/img/c.webp'] });
    expect(d[0]).toMatchObject({ field: 'media', detail: '1 added, 1 removed' });
  });

  it('calls a pure reorder a reorder', () => {
    const d = diffSnapshots(base, { ...base, mediaKeys: ['wire/img/b.webp', 'wire/img/a.webp'] });
    expect(d[0]).toMatchObject({ field: 'media', detail: 'reordered' });
  });

  it('reads the watermark toggle as on/off', () => {
    expect(diffSnapshots(base, { ...base, watermark: true })[0])
      .toMatchObject({ field: 'watermark', from: 'off', to: 'on' });
  });

  it('names co-authors rather than ids where it can', () => {
    const d = diffSnapshots(base, { ...base, coAuthors: [{ id: 'u2', name: 'M. Doyle' }] });
    expect(d[0]).toMatchObject({ field: 'co_authors', from: null, to: 'M. Doyle' });
  });

  it('clips an absurdly long title instead of storing it whole', () => {
    const d = diffSnapshots(base, { ...base, title: 'x'.repeat(1000) });
    expect(String(d[0]?.to).length).toBeLessThanOrEqual(300);
    expect(String(d[0]?.to).endsWith('…')).toBe(true);
  });

  it('collects several changes in one edit', () => {
    const d = diffSnapshots(base, { ...base, title: 'New', license: 'display', credit: '© M. Doyle' });
    expect(d.map((x) => x.field).sort()).toEqual(['credit', 'license', 'title']);
  });
});

describe('mediaKeyOf', () => {
  it('prefers the R2 key and falls back to a legacy CF image id', () => {
    expect(mediaKeyOf({ r2_key: 'k', cf_image_id: 'c' })).toBe('k');
    expect(mediaKeyOf({ cf_image_id: 'c' })).toBe('c');
    expect(mediaKeyOf({})).toBe('');
  });
});
