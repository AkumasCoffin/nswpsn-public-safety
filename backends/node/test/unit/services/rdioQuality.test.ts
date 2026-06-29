/**
 * Tests for the junk-transcript filter (services/rdioQuality.ts) — the
 * URL / garbled / non-English gates that drop Whisper hallucinations
 * before they reach the spell-checker, Gemini, or the rebuilder.
 *
 * The bias under test is conservative: real (if noisy) radio traffic
 * must survive every gate; only clear hallucination artefacts are
 * dropped.
 */
import { describe, it, expect } from 'vitest';
import {
  classifyTranscript,
  hasUrl,
  isGarbled,
  isNonEnglish,
} from '../../../src/services/rdioQuality.js';

describe('hasUrl', () => {
  it('flags explicit schemes and www. prefixes', () => {
    expect(hasUrl('https://example.com/foo')).toBe(true);
    expect(hasUrl('go to www.zeoranger.co.uk now')).toBe(true);
    expect(hasUrl('Subtitles by the Amara.org community')).toBe(true);
  });

  it('flags bare domains with a known TLD', () => {
    expect(hasUrl('visit beyondblue.org.au')).toBe(true);
    expect(hasUrl('thanks for watching mychannel.tv')).toBe(true);
  });

  it('does not flag abbreviations / numbers that contain a dot', () => {
    expect(hasUrl('Pumper 7 on scene, Smith St.')).toBe(false);
    expect(hasUrl('responding Code 2.5 km out')).toBe(false);
    expect(hasUrl('arrived at 14.30')).toBe(false);
    expect(hasUrl('patient is 60s, B.P. stable')).toBe(false);
  });
});

describe('isNonEnglish', () => {
  it('flags non-Latin scripts (Whisper language switches)', () => {
    expect(isNonEnglish('구독과 좋아요 부탁드립니다')).toBe(true); // Korean
    expect(isNonEnglish('请订阅我的频道')).toBe(true); // Chinese
    expect(isNonEnglish('Спасибо за просмотр')).toBe(true); // Russian
    expect(isNonEnglish('ご視聴ありがとうございました')).toBe(true); // Japanese
    expect(isNonEnglish('شكرا للمشاهدة')).toBe(true); // Arabic
  });

  it('flags even a single non-Latin character embedded in English', () => {
    expect(isNonEnglish('Pumper 7 责 on scene')).toBe(true);
  });

  it('keeps plain English radio traffic', () => {
    expect(isNonEnglish('Pumper 7 from FireCom respond Code 3')).toBe(false);
    expect(isNonEnglish("don't, they're, can't — contractions")).toBe(false);
  });
});

describe('isGarbled', () => {
  it('flags long single-character runs', () => {
    expect(isGarbled('aaaaaaah')).toBe(true);
    expect(isGarbled('uhhhhhhhh okay')).toBe(true);
  });

  it('flags a word stuttered 4+ times in a row', () => {
    expect(isGarbled('you you you you you')).toBe(true);
  });

  it('flags a short phrase looping (low distinct-word ratio)', () => {
    expect(isGarbled('thank you thank you thank you thank you')).toBe(true);
  });

  it('keeps short, legitimately repetitive prowords', () => {
    expect(isGarbled('break break')).toBe(false);
    expect(isGarbled('Code 3, Code 3')).toBe(false);
    expect(isGarbled('copy copy copy')).toBe(false); // 3 tokens, under thresholds
  });

  it('keeps normal radio traffic', () => {
    expect(
      isGarbled('Pumper 7 from FireCom respond Code 3 to Smith Street Newtown'),
    ).toBe(false);
    expect(isGarbled('')).toBe(false);
    expect(isGarbled('   ')).toBe(false);
  });
});

describe('classifyTranscript', () => {
  it('returns the matching reason', () => {
    expect(classifyTranscript('see www.example.com')).toBe('url');
    expect(classifyTranscript('请订阅')).toBe('non_english');
    expect(classifyTranscript('you you you you you')).toBe('garbled');
  });

  it('returns null for usable radio traffic', () => {
    expect(
      classifyTranscript('Pumper 7 on scene, investigating, no signs of fire'),
    ).toBeNull();
    expect(classifyTranscript('')).toBeNull();
    expect(classifyTranscript('   ')).toBeNull();
  });

  it('prioritises URL over the other gates', () => {
    // A line that is both a URL and looping resolves to the cheaper,
    // more decisive reason.
    expect(classifyTranscript('www.a.com www.a.com www.a.com www.a.com')).toBe(
      'url',
    );
  });
});
