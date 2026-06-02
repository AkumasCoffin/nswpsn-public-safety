/**
 * Trimmed post-rebuilder polish for the rdio hourly summary. Runs AFTER
 * services/llm.ts → rebuildIncidents() has reattached server-authoritative
 * transcripts + units to the LLM's incident groupings, so the surface area
 * is small:
 *
 *   1. Drop legacy `timeline[]` if it sneaks in (old schema).
 *   2. Drop incidents whose title+summary echoes a worked example from
 *      prompts/rdio_hourly.txt (model-cache regurgitation).
 *   3. Dedup by `member_call_ids` overlap (>=50% of smaller side, or proper
 *      subset). Richer incident wins.
 *
 * Pure function, no I/O. Returns the same `structured` reference (mutated)
 * or passes through unchanged if the input isn't a plain object.
 *
 * Replaces the previous validateStructuredAgainstTranscripts (parity with
 * python's _validate_structured_against_transcripts) — see commit history.
 * That function handled call_id sanity, NSWPF redaction, NATO/units checks,
 * agency-allow-list filter, and agency_stats.NSWPF strip; all of those
 * became unnecessary once the rebuilder made the LLM's emitted transcript
 * text and unit IDs unreachable.
 */
import { log } from '../lib/log.js';

// Fingerprints of worked examples in prompts/rdio_hourly.txt. Gemini
// sometimes regurgitates one of these example incidents verbatim every
// hour — even when staples a real current-hour call_id onto it, the
// title/summary still match. Each entry is a list of lowercase substrings
// that must ALL appear in the incident's combined title + summary blob
// for the incident to be dropped. Keep in sync with the EXAMPLE blocks
// in rdio_hourly.txt; when rewriting an example, ADD the new fingerprint
// and KEEP the old one for a few weeks (the model may still echo the
// previous version from cache).
const EXAMPLE_INCIDENT_FINGERPRINTS: ReadonlyArray<readonly string[]> = [
  // Original Example 2 (pre-2026-05-15).
  ['machinery fire at recycling plant', 'elizabeth street'],
  // Hazmat synthetic (2026-05-15+).
  ['brindwell loop', 'karoneth'],
  // Hazmat synthetic (2026-05-29+, post-rebuilder rewrite).
  ['vesperton loop', 'maracine'],
];

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function isExampleEcho(inc: Record<string, unknown>): boolean {
  const title = typeof inc['title'] === 'string' ? (inc['title'] as string) : '';
  const summary = typeof inc['summary'] === 'string' ? (inc['summary'] as string) : '';
  const blob = `${title}\n${summary}`.toLowerCase();
  for (const sig of EXAMPLE_INCIDENT_FINGERPRINTS) {
    if (sig.every((sub) => blob.includes(sub))) return true;
  }
  return false;
}

/** Read `member_call_ids: number[]` off an incident with light coercion. */
function readMemberCallIds(inc: Record<string, unknown>): Set<number> {
  const out = new Set<number>();
  const raw = inc['member_call_ids'];
  if (!Array.isArray(raw)) return out;
  for (const v of raw) {
    const n = typeof v === 'number' ? v : Number(v);
    if (Number.isFinite(n)) out.add(Math.trunc(n));
  }
  return out;
}

/** Richness used to pick the loser in dedup. Transcripts and units are
 *  server-attached so they count even though the LLM didn't emit them. */
function rebuiltRichness(inc: Record<string, unknown>): number {
  const summary = typeof inc['summary'] === 'string' ? (inc['summary'] as string) : '';
  const mcids = Array.isArray(inc['member_call_ids']) ? inc['member_call_ids'] : [];
  const units = Array.isArray(inc['units']) ? inc['units'] : [];
  return summary.length + mcids.length * 50 + units.length * 20;
}

export function polishStructuredIncidents(structured: unknown): unknown {
  if (!isPlainObject(structured)) return structured;
  const incidents = structured['incidents'];
  if (!Array.isArray(incidents)) return structured;

  let legacyTimelineSeen = 0;
  let droppedExampleEcho = 0;

  // Pass 1: per-incident filters.
  const survivors: Array<Record<string, unknown>> = [];
  for (const inc of incidents) {
    if (!isPlainObject(inc)) continue;
    if ('timeline' in inc) {
      legacyTimelineSeen += 1;
      delete inc['timeline'];
    }
    if (isExampleEcho(inc)) {
      droppedExampleEcho += 1;
      continue;
    }
    survivors.push(inc);
  }

  if (legacyTimelineSeen > 0) {
    log.warn(
      { count: legacyTimelineSeen },
      'rdio polish: legacy timeline[] present; ignored',
    );
  }

  // Pass 2: dedup by member_call_ids overlap.
  const idSets = survivors.map(readMemberCallIds);
  const keep = new Set<number>(survivors.map((_, i) => i));
  let droppedDupes = 0;
  for (let i = 0; i < survivors.length; i++) {
    if (!keep.has(i)) continue;
    for (let j = i + 1; j < survivors.length; j++) {
      if (!keep.has(j)) continue;
      const a = idSets[i]!;
      const b = idSets[j]!;
      if (a.size === 0 || b.size === 0) continue;
      const smaller = Math.min(a.size, b.size);
      let overlap = 0;
      const [iter, other] = a.size <= b.size ? [a, b] : [b, a];
      for (const v of iter) if (other.has(v)) overlap += 1;
      if (overlap === smaller || overlap / smaller >= 0.5) {
        const richI = rebuiltRichness(survivors[i]!);
        const richJ = rebuiltRichness(survivors[j]!);
        const loser = richI >= richJ ? j : i;
        keep.delete(loser);
        droppedDupes += 1;
        if (loser === i) break;
      }
    }
  }
  const out: Array<Record<string, unknown>> = [];
  for (let k = 0; k < survivors.length; k++) {
    if (keep.has(k)) out.push(survivors[k]!);
  }

  const bits: string[] = [];
  if (droppedExampleEcho) bits.push(`${droppedExampleEcho} prompt-example echo(es)`);
  if (droppedDupes) bits.push(`${droppedDupes} duplicate incident(s)`);
  if (bits.length > 0) {
    log.warn({ summary: bits.join(', ') }, 'rdio polish dropped');
  }

  structured['incidents'] = out;
  structured['incident_count'] = out.length;
  return structured;
}
