/**
 * Polish layer that strips hallucinated content from the LLM-emitted
 * `details.structured` block before persistence. Pure function, no I/O.
 *
 * Mirrors python `_validate_structured_against_transcripts` at
 * external_api_proxy.py:14874-15164. See that doc-string for the full
 * spec; the short version is:
 *
 *   1. Drop legacy `timeline[]` (the new schema is `transcripts[]`).
 *   2. Drop transcripts[] rows whose `call_id` isn't in the real hour's
 *      call set (numeric coercion: "123" → 123).
 *   3. Strip NSWPF-themed text from summary/title/transcripts[].text;
 *      drop whole incidents that are exclusively NSWPF-themed.
 *   4. Filter `agencies[]` to {FRNSW, NSWA, RFS, SES}.
 *   5. Merge duplicate incidents via transcripts[].call_id overlap
 *      (>=50% or proper subset of the smaller side).
 *   6. Drop NATO-alphabet-only entries in units[] (Whisper hallucinations).
 *   7. Drop units[] entries not mentioned in any transcripts[].text or
 *      in the rdio unit-label corpus.
 *   8. Strip top-level `agency_stats.NSWPF`.
 *
 * Best-effort cleanup: if the input isn't an object, or `incidents` isn't
 * a list, return the input unchanged (matches python's pass-through).
 */
import { log } from '../lib/log.js';

// Verbatim parity with python line 14858.
const NATO_ALPHABET = new Set([
  'alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf',
  'hotel', 'india', 'juliet', 'juliett', 'kilo', 'lima', 'mike', 'november',
  'oscar', 'papa', 'quebec', 'romeo', 'sierra', 'tango', 'uniform',
  'victor', 'whiskey', 'whisky', 'xray', 'x-ray', 'yankee', 'zulu',
]);

// Verbatim parity with python line 14866.
const NSWPF_BODY =
  '\\b(nswpf|nsw\\s*police|police\\s*(?:officer|car|patrol|unit|vehicle)s?|vkg|vka|highway\\s*patrol|traffic\\s*&?\\s*highway|pursuit|pol(?:air|police)|constable|sergeant|detective)\\b';
const NSWPF_TEST = new RegExp(NSWPF_BODY, 'i');
const NSWPF_REPLACE = new RegExp(NSWPF_BODY, 'gi');

const ALLOWED_AGENCIES = new Set(['FRNSW', 'NSWA', 'RFS', 'SES']);

const DIGIT_WORDS: Record<string, string> = {
  '0': 'zero', '1': 'one', '2': 'two', '3': 'three', '4': 'four',
  '5': 'five', '6': 'six', '7': 'seven', '8': 'eight', '9': 'nine',
};

export interface ValidatorCallRow {
  call_id?: number | string | null;
  id?: number | string | null;
}

export interface ValidatorOptions {
  /**
   * Lower-cased authoritative unit-label corpus, one entry per
   * `_RDIO_UNIT_LABELS.values()`. The validator falls back to an empty
   * set when omitted (callers without enumeration access lose this
   * specific cleanup but everything else works).
   */
  knownLabels?: Iterable<string>;
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function coerceCallId(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number') return Number.isFinite(v) ? Math.trunc(v) : null;
  if (typeof v === 'string') {
    const trimmed = v.trim();
    if (!trimmed) return null;
    const n = Number(trimmed);
    return Number.isFinite(n) ? Math.trunc(n) : null;
  }
  return null;
}

function isNswpfText(s: unknown): boolean {
  if (typeof s !== 'string' || !s) return false;
  return NSWPF_TEST.test(s);
}

function redactNswpf(s: string): string {
  return s.replace(NSWPF_REPLACE, '[redacted]');
}

function isNatoOnly(uid: string): boolean {
  const trimmed = (uid ?? '').trim().toLowerCase();
  if (!trimmed) return false;
  const tokens = trimmed.split(/\s+/);
  if (tokens.length === 0 || tokens.length > 2) return false;
  if (!NATO_ALPHABET.has(tokens[0]!)) return false;
  if (tokens.length === 1) return true;
  // Python: tokens[1].replace('-', '').isdigit()
  const second = tokens[1]!.replace(/-/g, '');
  return second.length > 0 && /^\d+$/.test(second);
}

function isAllDigits(s: string): boolean {
  return s.length > 0 && /^\d+$/.test(s);
}

function mentioned(
  uid: string,
  corpus: string,
  corpusNoSep: string,
  knownLabels: Set<string>,
): boolean {
  const u = (uid ?? '').trim().toLowerCase();
  if (!u) return false;
  if (knownLabels.has(u)) return true;
  if (corpus.includes(u)) return true;
  if (corpusNoSep.includes(u.replace(/-/g, '').replace(/ /g, ''))) return true;
  if (isAllDigits(u)) {
    const spelled = u.split('').map((d) => DIGIT_WORDS[d] ?? d).join(' ');
    if (corpus.includes(spelled)) return true;
  }
  return false;
}

function transcriptCallIds(inc: Record<string, unknown>): Set<number> {
  const set = new Set<number>();
  const trs = inc['transcripts'];
  if (!Array.isArray(trs)) return set;
  for (const t of trs) {
    if (!isPlainObject(t)) continue;
    const cid = coerceCallId(t['call_id']);
    if (cid !== null) set.add(cid);
  }
  return set;
}

function incidentRichness(inc: Record<string, unknown>): number {
  const summary = typeof inc['summary'] === 'string' ? inc['summary'] : '';
  const trs = Array.isArray(inc['transcripts']) ? inc['transcripts'] : [];
  const units = Array.isArray(inc['units']) ? inc['units'] : [];
  return summary.length + trs.length * 50 + units.length * 20;
}

export function validateStructuredAgainstTranscripts(
  structured: unknown,
  calls: ValidatorCallRow[],
  options: ValidatorOptions = {},
): unknown {
  if (!isPlainObject(structured)) return structured;
  const incidents = structured['incidents'];
  if (!Array.isArray(incidents)) return structured;

  // Real call_ids for this hour. Mirrors python lines 14904-14916.
  const knownCallIds = new Set<number>();
  for (const c of calls) {
    if (!isPlainObject(c)) continue;
    let cid = coerceCallId((c as Record<string, unknown>)['call_id']);
    if (cid === null) cid = coerceCallId((c as Record<string, unknown>)['id']);
    if (cid !== null) knownCallIds.add(cid);
  }

  const knownLabels = new Set<string>();
  if (options.knownLabels) {
    for (const lbl of options.knownLabels) {
      if (typeof lbl === 'string' && lbl) knownLabels.add(lbl.toLowerCase());
    }
  }

  let droppedUnits = 0;
  let droppedNato = 0;
  let droppedNswpf = 0;
  let droppedBadCids = 0;
  let droppedNswpfIncidents = 0;
  let legacyTimelineSeen = 0;

  const incidentsIn = incidents.filter(isPlainObject) as Array<
    Record<string, unknown>
  >;

  // Pass 1: per-incident normalisation (legacy timeline drop, transcripts
  // call_id validation, NSWPF strip, allowed-agency filter, whole-incident
  // NSWPF drop). Mirrors python lines 14970-15040.
  const normalised: Array<Record<string, unknown>> = [];
  for (const inc of incidentsIn) {
    if ('timeline' in inc) {
      legacyTimelineSeen += 1;
      delete inc['timeline'];
    }

    const rawTrs = inc['transcripts'];
    const cleanedTrs: Array<Record<string, unknown>> = [];
    if (Array.isArray(rawTrs)) {
      for (const t of rawTrs) {
        if (!isPlainObject(t)) {
          droppedBadCids += 1;
          continue;
        }
        const cid = coerceCallId(t['call_id']);
        if (cid === null || !knownCallIds.has(cid)) {
          droppedBadCids += 1;
          continue;
        }
        t['call_id'] = cid;
        const text = t['text'];
        if (typeof text === 'string' && isNswpfText(text)) {
          droppedNswpf += 1;
          continue;
        }
        cleanedTrs.push(t);
      }
    }
    inc['transcripts'] = cleanedTrs;

    const summaryTxt = typeof inc['summary'] === 'string' ? (inc['summary'] as string) : '';
    const titleTxt = typeof inc['title'] === 'string' ? (inc['title'] as string) : '';
    if (isNswpfText(summaryTxt)) {
      droppedNswpf += 1;
      inc['summary'] = redactNswpf(summaryTxt);
    }
    if (isNswpfText(titleTxt)) {
      droppedNswpf += 1;
      inc['title'] = redactNswpf(titleTxt);
    }

    const agencies = inc['agencies'];
    if (Array.isArray(agencies)) {
      const filtered = agencies.filter(
        (a) => typeof a === 'string' && ALLOWED_AGENCIES.has(a),
      );
      if (filtered.length !== agencies.length) {
        droppedNswpf += agencies.length - filtered.length;
      }
      inc['agencies'] = filtered;
    }

    // Whole-incident NSWPF drop. Python line 15031: surface_blob =
    // ' '.join(str(inc.get(k) or '') for k in ('title', 'summary', 'type'))
    const surfaceBlob = ['title', 'summary', 'type']
      .map((k) => {
        const v = inc[k];
        return v === null || v === undefined ? '' : String(v);
      })
      .join(' ');
    const hasNonNswpfTranscript = cleanedTrs.some(
      (t) => typeof t['text'] === 'string' && !isNswpfText(t['text'] as string),
    );
    if (!hasNonNswpfTranscript && isNswpfText(surfaceBlob)) {
      droppedNswpfIncidents += 1;
      continue;
    }

    normalised.push(inc);
  }

  if (legacyTimelineSeen > 0) {
    log.warn(
      { count: legacyTimelineSeen },
      'rdio validator: legacy timeline[] present; ignored',
    );
  }

  // Pass 2: dedupe by transcripts[].call_id overlap (python 15046-15087).
  const idSets = normalised.map(transcriptCallIds);
  const keep = new Set<number>(normalised.map((_, i) => i));
  let droppedDupes = 0;
  for (let i = 0; i < normalised.length; i++) {
    if (!keep.has(i)) continue;
    for (let j = i + 1; j < normalised.length; j++) {
      if (!keep.has(j)) continue;
      const a = idSets[i]!;
      const b = idSets[j]!;
      if (a.size === 0 || b.size === 0) continue;
      const smaller = Math.min(a.size, b.size);
      let overlap = 0;
      // Iterate the smaller set for the intersection count.
      const [iter, other] = a.size <= b.size ? [a, b] : [b, a];
      for (const v of iter) if (other.has(v)) overlap += 1;
      if (overlap === smaller || overlap / smaller >= 0.5) {
        const richI = incidentRichness(normalised[i]!);
        const richJ = incidentRichness(normalised[j]!);
        const loser = richI >= richJ ? j : i;
        keep.delete(loser);
        droppedDupes += 1;
        if (loser === i) break;
      }
    }
  }
  const deduped: Array<Record<string, unknown>> = [];
  for (let k = 0; k < normalised.length; k++) {
    if (keep.has(k)) deduped.push(normalised[k]!);
  }

  // Pass 3: clean units[] (python 15091-15128). units[] may be strings or
  // legacy dicts; both supported.
  for (const inc of deduped) {
    const trs = Array.isArray(inc['transcripts']) ? inc['transcripts'] : [];
    const corpusParts: string[] = [];
    for (const t of trs) {
      if (isPlainObject(t) && typeof t['text'] === 'string') {
        corpusParts.push((t['text'] as string).toLowerCase());
      }
    }
    const corpus = corpusParts.join(' ');
    const corpusNoSep = corpus
      .replace(/-/g, '')
      .replace(/ /g, '')
      .replace(/\./g, '');

    const units = inc['units'];
    if (!Array.isArray(units)) continue;
    const kept: unknown[] = [];
    for (const u of units) {
      let uid: string;
      let carrier: unknown;
      if (typeof u === 'string') {
        uid = u;
        carrier = u;
      } else if (isPlainObject(u)) {
        if ((u as Record<string, unknown>)['agency'] === 'NSWPF') {
          droppedNswpf += 1;
          continue;
        }
        const rawId = (u as Record<string, unknown>)['id'];
        uid = typeof rawId === 'string' ? rawId : '';
        carrier = u;
      } else {
        droppedUnits += 1;
        continue;
      }
      if (!uid) {
        droppedUnits += 1;
        continue;
      }
      if (isNatoOnly(uid)) {
        droppedNato += 1;
        continue;
      }
      if (mentioned(uid, corpus, corpusNoSep, knownLabels)) {
        kept.push(carrier);
      } else {
        droppedUnits += 1;
      }
    }
    inc['units'] = kept;
  }

  // Top-level agency_stats.NSWPF strip (python 15130-15134).
  const stats = structured['agency_stats'];
  if (isPlainObject(stats) && 'NSWPF' in stats) {
    delete (stats as Record<string, unknown>)['NSWPF'];
    droppedNswpf += 1;
  }

  structured['incidents'] = deduped;

  if (droppedBadCids > 0) {
    log.warn(
      { count: droppedBadCids },
      'rdio validator: dropped transcripts[] rows with unknown call_id',
    );
  }
  const bits: string[] = [];
  if (droppedUnits) bits.push(`${droppedUnits} unit(s) not in transcripts`);
  if (droppedNato) bits.push(`${droppedNato} NATO-alphabet hallucination(s)`);
  if (droppedNswpf) bits.push(`${droppedNswpf} NSWPF ref(s)`);
  if (droppedNswpfIncidents) bits.push(`${droppedNswpfIncidents} NSWPF-only incident(s)`);
  if (droppedDupes) bits.push(`${droppedDupes} duplicate incident(s)`);
  if (bits.length > 0) {
    log.warn({ summary: bits.join(', ') }, 'rdio validator dropped');
  }

  structured['incident_count'] = deduped.length;
  return structured;
}
