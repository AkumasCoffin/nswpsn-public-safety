/**
 * Tests for keywordMatches — the whole-token keyword matcher behind the
 * ntfy "major incident" detector. The bug it fixes: the old substring
 * match flagged "GSW" inside "Cogswell" / "Kingswood" (false positives).
 */
import { describe, it, expect } from 'vitest';
import { keywordMatches } from '../../../src/services/rdioIncidentAlerts.js';

describe('keywordMatches', () => {
  // text is lowercased by the caller (analyzeKeywords); keywords are
  // lowercased by parseKeywordEnv. Tests pass lowercased inputs to match.
  it('matches a standalone token', () => {
    expect(keywordMatches('gsw to the left leg', 'gsw')).toBe(true);
    expect(keywordMatches('gsw', 'gsw')).toBe(true);
  });

  it('does NOT match inside a longer word (the reported false positives)', () => {
    expect(keywordMatches('responding to cogswell street', 'gsw')).toBe(false);
    expect(keywordMatches('kingswood', 'gsw')).toBe(false);
    expect(keywordMatches('kingswood high school', 'gsw')).toBe(false);
  });

  it('treats digits as part of a token (no glue to alphanumerics)', () => {
    expect(keywordMatches('gsw2 something', 'gsw')).toBe(false);
    expect(keywordMatches('2gsw', 'gsw')).toBe(false);
  });

  it('still matches when flanked by punctuation / slashes / line ends', () => {
    expect(keywordMatches('gsw/stab wound', 'gsw')).toBe(true);
    expect(keywordMatches('patient has a gsw.', 'gsw')).toBe(true);
    expect(keywordMatches('(gsw)', 'gsw')).toBe(true);
    expect(keywordMatches('stab | gsw | cardiac', 'gsw')).toBe(true);
  });

  it('matches multi-word phrases as whole tokens', () => {
    expect(keywordMatches('structure fire on main st', 'structure fire')).toBe(true);
    expect(keywordMatches('structure firefighter', 'structure fire')).toBe(false);
  });

  it('does not match a short abbreviation inside an unrelated word', () => {
    expect(keywordMatches('ambulance en route', 'amb')).toBe(false);
    expect(keywordMatches('amb 33 responding', 'amb')).toBe(true);
    expect(keywordMatches('command post at mva', 'mva')).toBe(true);
    expect(keywordMatches('mvale road', 'mva')).toBe(false);
  });

  it('escapes regex metacharacters in the keyword', () => {
    expect(keywordMatches('code 10-4 received', '10-4')).toBe(true);
    expect(keywordMatches('value is 10-45 now', '10-4')).toBe(false);
    // A keyword with metachars must be matched literally, not as a pattern.
    expect(keywordMatches('the c.o.d was', 'c.o.d')).toBe(true);
    expect(keywordMatches('the cxoxd was', 'c.o.d')).toBe(false);
  });

  it('returns false for an empty keyword', () => {
    expect(keywordMatches('anything', '')).toBe(false);
  });
});
