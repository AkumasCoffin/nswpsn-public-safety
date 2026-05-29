/**
 * Conservative spell-check for Whisper-transcribed radio audio.
 *
 * Tokenises a transcript into word + non-word runs, runs nspell against
 * the en-AU Hunspell dictionary on the word tokens only, and applies a
 * correction only when ALL of these are true:
 *
 *   - The word isn't already correct per the dictionary.
 *   - The word isn't in our allow-list (unit-label corpus from
 *     `rdio_units.csv` plus the hand-curated `rdio_lexicon.txt`).
 *   - The word isn't all-uppercase (acronym: NSWPF, VKG, AFA, …).
 *   - The word isn't capitalised first letter (proper noun: Bulli,
 *     Helensburgh, Vaucluse, …).
 *   - The word has length ≥ 3 (single letters and short tokens are
 *     unsafe to flip).
 *   - The word has no digits or non-word characters (apart from a
 *     contraction apostrophe).
 *   - nspell returns EXACTLY ONE suggestion.
 *   - Levenshtein distance from the original to the suggestion is ≤ 2.
 *
 * Anything that falls outside those gates is left untouched. Non-word
 * runs (spaces, punctuation) are preserved as-is, so the corrected
 * string has the same shape as the input.
 *
 * `rdio_lexicon.txt` is operator-editable — one word per line, '#'
 * comments allowed — so a false-positive correction can be fixed by
 * appending the original word to that file and restarting api-node.
 */
import { promises as fs } from 'node:fs';
import * as path from 'node:path';
import nspell from 'nspell';
import dictAuPkg from 'dictionary-en-au';
import { log } from '../lib/log.js';
import { ensureUnitLabelsLoaded, allUnitLabels } from './rdio.js';

type SpellInstance = ReturnType<typeof nspell>;

let _spell: SpellInstance | null = null;
const _allowList = new Set<string>();
let _ready = false;
let _loadingPromise: Promise<void> | null = null;

const MAX_EDIT_DISTANCE = 2;
const MIN_WORD_LENGTH = 3;

/** Words to add unconditionally — used at startup before the lexicon file
 *  loads. Kept tiny; the bulk lives in `backends/reference/rdio_lexicon.txt`. */
const BUILT_IN_ALLOW = [
  // Agency / radio core terms that anchor every transcript and would
  // be a nightmare if the file went missing.
  'frnsw', 'nswa', 'nswpf', 'rfs', 'ses', 'nswpsn', 'psn',
  'vkg', 'vka', 'polair', 'firecom', 'firecomms',
  'afa', 'mvc', 'mva', 'mvp', 'hazmat', 'epa',
];

/** Tokenise: split into words + non-word runs. Words include an inner
 *  apostrophe for contractions ("don't", "they're"). */
const TOKEN_RE = /[A-Za-z]+(?:'[A-Za-z]+)?|[^A-Za-z]+/g;

function levenshtein(a: string, b: string): number {
  const m = a.length;
  const n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;
  const prev = new Array<number>(n + 1);
  for (let j = 0; j <= n; j++) prev[j] = j;
  for (let i = 1; i <= m; i++) {
    const curr: number[] = [i];
    for (let j = 1; j <= n; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      curr[j] = Math.min(
        curr[j - 1]! + 1,
        prev[j]! + 1,
        prev[j - 1]! + cost,
      );
    }
    for (let j = 0; j <= n; j++) prev[j] = curr[j]!;
  }
  return prev[n]!;
}

async function loadLexiconFile(): Promise<void> {
  const referenceDir = path.resolve(process.cwd(), '..', 'reference');
  const fp = path.join(referenceDir, 'rdio_lexicon.txt');
  try {
    const text = await fs.readFile(fp, 'utf8');
    let added = 0;
    for (const rawLine of text.split(/\r?\n/)) {
      const line = rawLine.trim();
      if (!line || line.startsWith('#')) continue;
      const lower = line.toLowerCase();
      if (!_allowList.has(lower)) {
        _allowList.add(lower);
        added += 1;
      }
    }
    log.info(
      { path: fp, added, totalAllow: _allowList.size },
      'rdio spell: lexicon loaded',
    );
  } catch (err) {
    const e = err as NodeJS.ErrnoException;
    if (e.code !== 'ENOENT') {
      log.warn({ err: e.message, path: fp }, 'rdio spell: lexicon read failed');
    }
  }
}

async function ensureLoaded(): Promise<void> {
  if (_ready) return;
  if (_loadingPromise) {
    await _loadingPromise;
    return;
  }
  _loadingPromise = (async () => {
    _spell = nspell({
      aff: Buffer.from(dictAuPkg.aff),
      dic: Buffer.from(dictAuPkg.dic),
    });
    for (const w of BUILT_IN_ALLOW) _allowList.add(w);
    // Pull unit-label words into the allow-list so brigade / station
    // names aren't flagged as misspellings.
    await ensureUnitLabelsLoaded();
    for (const label of allUnitLabels()) {
      for (const word of label.toLowerCase().split(/[^A-Za-z]+/)) {
        if (word.length >= 2) _allowList.add(word);
      }
    }
    await loadLexiconFile();
    _ready = true;
    log.info(
      { allowSize: _allowList.size },
      'rdio spell: ready',
    );
  })();
  await _loadingPromise;
}

/** Apply the conservative correction rules to a single word. Returns the
 *  original if any gate trips. Caller has already filtered to word-shaped
 *  tokens (letters + optional contraction). */
function correctWord(word: string): string {
  if (!_spell || !_ready) return word;
  if (word.length < MIN_WORD_LENGTH) return word;
  if (word === word.toUpperCase()) return word; // acronym
  if (word[0]! !== word[0]!.toLowerCase()) return word; // proper noun
  const lower = word.toLowerCase();
  if (_allowList.has(lower)) return word;
  if (_spell.correct(lower)) return word;
  const suggestions = _spell.suggest(lower);
  if (suggestions.length !== 1) return word;
  const top = suggestions[0]!;
  if (top === lower) return word;
  if (levenshtein(lower, top) > MAX_EDIT_DISTANCE) return word;
  // All gates passed — apply correction. Spelling fixes always come
  // back lowercase from nspell; the original was lowercase too (the
  // capital-letter gate above bailed otherwise).
  return top;
}

/** Public API: ensure the spellchecker is loaded before any
 *  `spellcheckTranscript` call. Caller does this once near the top of
 *  the orchestrator (e.g. formatRdioPrompt). */
export async function ensureSpellcheckerLoaded(): Promise<void> {
  await ensureLoaded();
}

/** Spell-check a single transcript string. If the spellchecker isn't
 *  loaded yet, returns the input unchanged. */
export function spellcheckTranscript(text: string): {
  corrected: string;
  changes: Array<{ from: string; to: string }>;
} {
  if (!_ready || !text) return { corrected: text, changes: [] };
  const changes: Array<{ from: string; to: string }> = [];
  const corrected = text.replace(TOKEN_RE, (match) => {
    if (!/^[A-Za-z]/.test(match)) return match; // non-word run
    const fixed = correctWord(match);
    if (fixed !== match) changes.push({ from: match, to: fixed });
    return fixed;
  });
  return { corrected, changes };
}

// ---------------------------------------------------------------------------
// Test seams
// ---------------------------------------------------------------------------

export function _resetSpellcheckerForTests(): void {
  _spell = null;
  _allowList.clear();
  _ready = false;
  _loadingPromise = null;
}
