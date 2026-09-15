/**
 * Turn an upstream HTML fragment into plain prose.
 *
 * Several feeds write their longer notes as HTML — WA Emergency ships `<p>`,
 * `<ul>` and anchors; LiveTraffic embeds `<p>`, `<br>` and `&nbsp;`. Consumers
 * render these as text, so the markup has to come out here rather than in each
 * frontend.
 *
 * Two ordering rules matter, and both were wrong in the copies this replaces
 * (CodeQL js/double-escaping + js/incomplete-multi-character-sanitization):
 *
 *   1. Decode entities AFTER stripping tags is not enough on its own — but
 *      decoding `&amp;` BEFORE `&lt;`/`&gt;` is actively wrong. `&amp;lt;`
 *      means a literal `&lt;`, so decoding `&amp;` first yields `&lt;`, which
 *      the next rule then turns into `<`. Doubly-encoded markup reassembled
 *      into real tags the stripper had already run past. `&amp;` goes LAST.
 *
 *   2. A single `replace(/<[^>]*>/g, '')` pass is not a fixpoint: `<<a>script>`
 *      leaves `<script>` behind. Strip until the string stops changing.
 *
 * No consumer renders this with innerHTML today, so neither was exploitable —
 * but both produced wrong text, and the next consumer should not have to know.
 */

/** Entity decodes, in the order they must run. `&amp;` is deliberately last. */
const ENTITIES: Array<[RegExp, string]> = [
  [/&nbsp;/gi, ' '],
  [/&lt;/gi, '<'],
  [/&gt;/gi, '>'],
  [/&quot;/gi, '"'],
  [/&#0*39;/g, "'"],
  [/&apos;/gi, "'"],
  [/&amp;/gi, '&'],
];

/** Remove tags repeatedly until the result stops changing. */
function stripTags(input: string): string {
  let out = input;
  // Bounded so a pathological input cannot spin: each pass removes at least
  // one `<`, and anything still matching after 10 is not worth keeping.
  for (let i = 0; i < 10; i += 1) {
    const next = out.replace(/<[^>]*>/g, '');
    if (next === out) return out;
    out = next;
  }
  return out.replace(/</g, '');
}

/**
 * HTML fragment -> single-line plain text.
 *
 * Block ends and `<br>` become spaces so sentences do not run together; the
 * final whitespace collapse tidies up after them.
 */
export function htmlToProse(value: unknown): string {
  const raw =
    value === null || value === undefined
      ? ''
      : typeof value === 'string'
        ? value
        : String(value);

  let out = raw
    .replace(/<\s*br\s*\/?\s*>/gi, ' ')
    .replace(/<\s*\/\s*(p|li|ul|ol|div|tr|h[1-6])\s*>/gi, ' ');

  out = stripTags(out);

  for (const [pattern, replacement] of ENTITIES) {
    out = out.replace(pattern, replacement);
  }

  // Decoding can expose markup that was entity-encoded in the source. That
  // text is meant to READ as `<b>`, so it is left alone — but a second strip
  // here would silently delete it, which is why this does not run again.
  return out.replace(/\s+/g, ' ').trim();
}
