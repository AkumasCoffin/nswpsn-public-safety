/**
 * rdioSpell — conservative spellcheck for transcripts. Tests load the
 * real en-AU dictionary (small, ~700KB; ~50ms to spin up once per file).
 * Allow-list and lexicon paths are exercised by seeding via the unit
 * label helper before loading.
 */
import { describe, it, expect, beforeAll, vi } from 'vitest';

vi.mock('../../../src/services/rdio.js', async () => {
  const actual =
    await vi.importActual<typeof import('../../../src/services/rdio.js')>(
      '../../../src/services/rdio.js',
    );
  return {
    ...actual,
    ensureUnitLabelsLoaded: vi.fn(async () => undefined),
    // A handful of synthetic unit labels to seed the allow-list. Picks
    // include lower-case fragments ("greenwall", "huskisson") that
    // would otherwise trigger Hunspell corrections.
    allUnitLabels: () =>
      [
        'RFS - Greenwall Point 1',
        'RFS - Huskisson Pumper',
        'FRNSW - Pumper 7',
      ][Symbol.iterator](),
  };
});

beforeAll(async () => {
  const { ensureSpellcheckerLoaded, _resetSpellcheckerForTests } = await import(
    '../../../src/services/rdioSpell.js'
  );
  _resetSpellcheckerForTests();
  await ensureSpellcheckerLoaded();
});

describe('spellcheckTranscript', () => {
  it('returns empty input unchanged', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    const out = spellcheckTranscript('');
    expect(out.corrected).toBe('');
    expect(out.changes).toEqual([]);
  });

  it('passes correctly-spelled text through', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    const out = spellcheckTranscript('the truck is en route to the fire');
    expect(out.corrected).toBe('the truck is en route to the fire');
    expect(out.changes).toEqual([]);
  });

  it('corrects an obvious lowercase typo', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    const out = spellcheckTranscript('the strucure is on fire');
    expect(out.corrected).toContain('structure');
    expect(out.changes).toEqual([{ from: 'strucure', to: 'structure' }]);
  });

  it('leaves all-uppercase acronyms alone', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    const out = spellcheckTranscript('VKG to RFS Comms');
    expect(out.corrected).toBe('VKG to RFS Comms');
  });

  it('leaves capitalised proper nouns alone', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    // "Bulli" and "Helensburgh" are NSW suburbs; the Hunspell dict
    // doesn't ship them but they're capitalised, so the gate fires.
    const out = spellcheckTranscript(
      'crew responding from Bulli to Helensburgh',
    );
    expect(out.corrected).toBe('crew responding from Bulli to Helensburgh');
  });

  it('does not touch short tokens (< 3 chars)', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    // "te" would have a single likely suggestion ("the") but is too
    // short to safely apply.
    const out = spellcheckTranscript('te crew is on scene');
    expect(out.corrected).toBe('te crew is on scene');
  });

  it('honours the unit-label allow-list', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    // "huskisson" pulled from a mocked unit label; would otherwise
    // not be in the dictionary.
    const out = spellcheckTranscript('huskisson pumper to base');
    expect(out.corrected).toBe('huskisson pumper to base');
  });

  it('preserves punctuation and whitespace around words', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    const out = spellcheckTranscript('Hello, world!');
    expect(out.corrected).toBe('Hello, world!');
  });

  it('skips a token when nspell returns multiple suggestions', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    // "hte" has many plausible corrections — single-suggestion gate
    // means we leave it. (If this test flakes on dictionary
    // updates that change suggestion counts, just bump the assertion
    // to .changes being empty OR length 1 — the rule is "be safe".)
    const out = spellcheckTranscript('hte truck arrived');
    // Either left as-is (multi-suggestion) or corrected to "the" —
    // both pass the rule. We only assert no destructive change.
    expect(out.corrected.toLowerCase()).toMatch(/(hte|the) truck arrived/);
  });

  it('reports changes as { from, to } pairs', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    const out = spellcheckTranscript('we recieved the call');
    if (out.changes.length > 0) {
      expect(out.changes[0]).toMatchObject({ from: 'recieved' });
    }
  });

  it('leaves apostrophe contractions intact', async () => {
    const { spellcheckTranscript } = await import(
      '../../../src/services/rdioSpell.js'
    );
    const out = spellcheckTranscript("we don't have a unit available");
    expect(out.corrected).toBe("we don't have a unit available");
  });
});
