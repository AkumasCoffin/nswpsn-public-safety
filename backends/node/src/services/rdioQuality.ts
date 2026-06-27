/**
 * Junk-transcript filtering for Whisper-transcribed radio audio.
 *
 * Whisper does not emit "(silence)" when fed dead air, static, or
 * non-speech noise — it *hallucinates*. The three failure modes below
 * are the ones that poison the hourly summary if they reach Gemini:
 *
 *   1. **URLs** — Whisper's training data is riddled with subtitle
 *      credits, so on noise it loves to produce things like
 *      "Subtitles by the Amara.org community", "www.zeoranger.co.uk",
 *      or a bare "https://…". NSW emergency radio never speaks a URL,
 *      so any domain/URL token means the line is a hallucination.
 *
 *   2. **Garbled / looping output** — long single-character runs
 *      ("aaaaaaah"), the same word stuttered many times ("you you you
 *      you you"), or a short phrase looped ("thank you thank you thank
 *      you thank you"). All are classic Whisper artefacts on
 *      silence/noise and carry no operational content.
 *
 *   3. **Non-English** — on noise Whisper frequently switches language
 *      entirely and emits Korean / Chinese / Russian / Arabic / etc.
 *      Any non-Latin script in an NSW English-radio transcript means
 *      the decoder lost the plot. (Latin-script non-English — French,
 *      Welsh — is deliberately NOT flagged: reliably distinguishing it
 *      from English callsigns and NSW place names is error-prone and
 *      the false-positive risk outweighs the rare true positive.)
 *
 * `classifyTranscript` returns the first matching reason or null. It is
 * a pure function with no I/O — wired into formatRdioPrompt() so junk
 * is dropped BEFORE the spell-check and before the call ever reaches
 * Gemini or the rebuilder's inputMap. Conservative by design: a missed
 * junk line is a minor nuisance, but dropping a real transmission loses
 * operational signal, so every gate errs toward keeping the line.
 */

export type JunkReason = 'url' | 'garbled' | 'non_english';

// URL / bare-domain detector. Matches an explicit scheme or www. prefix,
// or a hostname ending in a known TLD (so "Smith St." / "Code 2." don't
// trip it — "st"/"2" aren't TLDs). The TLD list covers the ones Whisper
// actually hallucinates in subtitle credits plus the AU set radio might
// conceivably (but won't) speak.
const URL_RE =
  /(?:https?:\/\/|www\.)\S+|\b[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.(?:com|net|org|gov|edu|io|co|tv|info|biz|me|app|xyz|uk|au)\b/i;

// Any character from a non-Latin script that NSW English radio would
// never contain: Greek, Cyrillic, Hebrew, Arabic, Devanagari, Thai,
// Hiragana/Katakana, CJK Unified, Hangul Jamo + syllables. All ranges
// are in the BMP, so plain \uXXXX (no `u` flag) is sufficient.
const NON_LATIN_RE =
  /[Ͱ-ϿЀ-ӿ֐-׿؀-ۿऀ-ॿ฀-๿ᄀ-ᇿ぀-ヿ一-鿿가-힯]/;

// 6+ of the same letter in a row ("aaaaaaah", "hmmmmmm"). Three is fine
// ("brrr"), six is a stuck decoder.
const CHAR_RUN_RE = /([a-z])\1{5,}/i;

/** True if the line is (or contains) a URL / bare domain. */
export function hasUrl(text: string): boolean {
  return URL_RE.test(text);
}

/** True if the line contains any non-Latin script (treated as non-English). */
export function isNonEnglish(text: string): boolean {
  return NON_LATIN_RE.test(text);
}

/**
 * True if the line looks like garbled / looping Whisper output:
 *   - a run of 6+ identical letters, OR
 *   - the same word stuttered 4+ times in a row, OR
 *   - a longer line (6+ words) whose distinct-word ratio is ≤ 1/3,
 *     i.e. it's a short phrase looping over and over.
 *
 * Short, legitimately repetitive radio ("break break", "Code 3, Code 3")
 * stays under the thresholds and is kept.
 */
export function isGarbled(text: string): boolean {
  const t = text.trim();
  if (!t) return false;
  if (CHAR_RUN_RE.test(t)) return true;

  const tokens = t.toLowerCase().split(/\s+/).filter(Boolean);
  if (tokens.length < 4) return false;

  // Consecutive stutter: same token 4+ times back-to-back.
  let run = 1;
  for (let i = 1; i < tokens.length; i++) {
    if (tokens[i] === tokens[i - 1]) {
      run += 1;
      if (run >= 4) return true;
    } else {
      run = 1;
    }
  }

  // Phrase loop: lots of words but very few distinct ones.
  if (tokens.length >= 6) {
    const distinct = new Set(tokens).size;
    if (distinct / tokens.length <= 1 / 3) return true;
  }
  return false;
}

/**
 * Classify a transcript as junk and return the reason, or null if it
 * looks like usable radio traffic. Order is URL → non-English → garbled
 * (cheapest / most decisive first). Empty input is treated as not-junk
 * — the caller already drops empties separately.
 */
export function classifyTranscript(text: string): JunkReason | null {
  if (!text || !text.trim()) return null;
  if (hasUrl(text)) return 'url';
  if (isNonEnglish(text)) return 'non_english';
  if (isGarbled(text)) return 'garbled';
  return null;
}
