// Regression tests for the two ordering bugs CodeQL flagged as
// js/double-escaping and js/incomplete-multi-character-sanitization
// (4 high alerts across waEmergency.stripHtml and traffic.asProse).

import { describe, it, expect } from 'vitest';
import { htmlToProse } from '../../../src/sources/shared/prose.js';

describe('htmlToProse', () => {
  it('strips ordinary markup and keeps words apart', () => {
    expect(htmlToProse('<p>Road closed</p><br>Use detour')).toBe('Road closed Use detour');
    expect(htmlToProse('<ul><li>One</li><li>Two</li></ul>')).toBe('One Two');
  });

  it('decodes ordinary entities', () => {
    expect(htmlToProse('Tom &amp; Jerry &nbsp; say &quot;hi&quot;')).toBe('Tom & Jerry say "hi"');
    expect(htmlToProse('It&#39;s fine')).toBe("It's fine");
  });

  it('does not let doubly-encoded markup become a tag', () => {
    // The bug: `&amp;` was decoded BEFORE `&lt;`/`&gt;`, so `&amp;lt;` became
    // `&lt;` and then `<` — reassembling a tag the stripper had already passed.
    const out = htmlToProse('&amp;lt;script&amp;gt;alert(1)&amp;lt;/script&amp;gt;');
    expect(out).toBe('&lt;script&gt;alert(1)&lt;/script&gt;');
    expect(out).not.toContain('<script>');
  });

  it('strips nested tags that survive a single pass', () => {
    // `<<a>script>` leaves `<script>` behind after one replace.
    const out = htmlToProse('<<a>script>alert(1)<</a>/script>');
    expect(out).not.toContain('<script>');
    expect(out).not.toContain('<');
  });

  it('keeps entity-encoded text that was never markup', () => {
    // `&lt;b&gt;` is someone writing about a tag; it must read as one, and a
    // second strip pass after decoding would silently delete it.
    expect(htmlToProse('Use &lt;b&gt; for bold')).toBe('Use <b> for bold');
  });

  it('handles the non-string and empty cases callers pass', () => {
    expect(htmlToProse(null)).toBe('');
    expect(htmlToProse(undefined)).toBe('');
    expect(htmlToProse('')).toBe('');
    expect(htmlToProse(42)).toBe('42');
    expect(htmlToProse('   spaced   out   ')).toBe('spaced out');
  });

  it('terminates on pathological nesting rather than spinning', () => {
    const nasty = `${'<'.repeat(500)}script${'>'.repeat(500)}`;
    const out = htmlToProse(nasty);
    expect(out).not.toContain('<');
  });
});
