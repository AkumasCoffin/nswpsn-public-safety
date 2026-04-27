/**
 * Tests for the structured-output validator that polishes the LLM's
 * `details.structured` block before persistence. Mirrors the python
 * coverage at backends/external_api_proxy.py:14874-15164.
 */
import { describe, it, expect } from 'vitest';
import { validateStructuredAgainstTranscripts } from '../../../src/services/rdioValidator.js';

describe('validateStructuredAgainstTranscripts', () => {
  it('passes through non-object input unchanged', () => {
    expect(validateStructuredAgainstTranscripts(null, [])).toBeNull();
    expect(validateStructuredAgainstTranscripts('hello', [])).toBe('hello');
    expect(validateStructuredAgainstTranscripts(42, [])).toBe(42);
    const arr = [1, 2, 3];
    expect(validateStructuredAgainstTranscripts(arr, [])).toBe(arr);
  });

  it('passes through when incidents is not an array', () => {
    const input = { incidents: 'oops', other: 1 };
    const out = validateStructuredAgainstTranscripts(input, []);
    expect(out).toBe(input);
    expect((out as { other: number }).other).toBe(1);
  });

  it('drops fabricated transcript call_ids and coerces string ids', () => {
    const input = {
      incidents: [
        {
          title: 'Fire on George St',
          summary: 'crews respond',
          transcripts: [
            { call_id: 1, text: 'truck en route' },
            { call_id: '2', text: 'second unit responding' }, // string -> 2
            { call_id: 999, text: 'fabricated' }, // not in known set
            { call_id: null, text: 'no id' },
            'not even a dict',
          ],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 },
      { id: 2 },
    ]) as { incidents: Array<{ transcripts: Array<{ call_id: number }> }> };
    const trs = out.incidents[0]!.transcripts;
    expect(trs).toHaveLength(2);
    expect(trs[0]!.call_id).toBe(1);
    expect(trs[1]!.call_id).toBe(2); // coerced
  });

  it('drops whole incidents that are exclusively NSWPF-themed', () => {
    const input = {
      incidents: [
        {
          title: 'NSW Police pursuit on M1',
          summary: 'Highway Patrol chasing stolen vehicle',
          type: 'pursuit',
          transcripts: [], // no surviving transcripts
        },
        {
          title: 'House fire',
          summary: 'FRNSW responding',
          transcripts: [{ call_id: 1, text: 'truck en route' }],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 },
    ]) as { incidents: Array<{ title: string }>; incident_count: number };
    expect(out.incidents).toHaveLength(1);
    expect(out.incidents[0]!.title).toBe('House fire');
    expect(out.incident_count).toBe(1);
  });

  it('redacts NSWPF text in summary/title when there is non-NSWPF content', () => {
    const input = {
      incidents: [
        {
          title: 'House fire — Highway Patrol assisting',
          summary: 'FRNSW responded; constable on scene with hose',
          transcripts: [{ call_id: 1, text: 'truck en route' }],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 },
    ]) as { incidents: Array<{ title: string; summary: string }> };
    expect(out.incidents[0]!.title).toContain('[redacted]');
    expect(out.incidents[0]!.title).not.toMatch(/highway patrol/i);
    expect(out.incidents[0]!.summary).toContain('[redacted]');
    expect(out.incidents[0]!.summary).not.toMatch(/constable/i);
  });

  it('drops NSWPF-themed transcript rows', () => {
    const input = {
      incidents: [
        {
          title: 'Crash on M5',
          summary: 'multi-vehicle collision',
          transcripts: [
            { call_id: 1, text: 'NSW Police pursuit code 1' }, // dropped
            { call_id: 2, text: 'NSWA on scene treating two patients' },
          ],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 },
      { call_id: 2 },
    ]) as { incidents: Array<{ transcripts: Array<{ call_id: number }> }> };
    expect(out.incidents[0]!.transcripts).toHaveLength(1);
    expect(out.incidents[0]!.transcripts[0]!.call_id).toBe(2);
  });

  it('drops legacy timeline[] field with a log warning', () => {
    const input = {
      incidents: [
        {
          title: 'Fire',
          summary: 's',
          timeline: [{ time: '01:00', event: 'old shape' }],
          transcripts: [{ call_id: 1, text: 'truck en route' }],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 },
    ]) as { incidents: Array<Record<string, unknown>> };
    expect('timeline' in out.incidents[0]!).toBe(false);
    expect(Array.isArray(out.incidents[0]!['transcripts'])).toBe(true);
  });

  it('filters agencies to allowed set and strips top-level NSWPF stats', () => {
    const input = {
      incidents: [
        {
          title: 'Crash',
          summary: 'rescue underway',
          agencies: ['FRNSW', 'NSWPF', 'NSWA', 'CHPP'],
          transcripts: [{ call_id: 1, text: 'rescue tools deploying' }],
        },
      ],
      agency_stats: { FRNSW: 5, NSWPF: 9, NSWA: 3 },
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 },
    ]) as {
      incidents: Array<{ agencies: string[] }>;
      agency_stats: Record<string, number>;
    };
    expect(out.incidents[0]!.agencies).toEqual(['FRNSW', 'NSWA']);
    expect(out.agency_stats).not.toHaveProperty('NSWPF');
    expect(out.agency_stats.FRNSW).toBe(5);
  });

  it('drops NATO-alphabet-only units (Whisper hallucinations)', () => {
    const input = {
      incidents: [
        {
          title: 'Fire',
          summary: 's',
          units: ['alpha', 'bravo 7', 'echo-9', 'HP 77'],
          transcripts: [
            { call_id: 1, text: 'HP 77 on scene' },
          ],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 },
    ]) as { incidents: Array<{ units: unknown[] }> };
    // alpha, bravo 7, echo-9 are NATO-only -> dropped.
    // HP 77 is mentioned in transcript -> kept.
    expect(out.incidents[0]!.units).toEqual(['HP 77']);
  });

  it('keeps units mentioned via spelled digits ("five-one" matches "51")', () => {
    const input = {
      incidents: [
        {
          title: 'Rescue',
          summary: 's',
          units: ['51'],
          transcripts: [
            { call_id: 1, text: 'Rescue five one is approaching the scene' },
          ],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 },
    ]) as { incidents: Array<{ units: unknown[] }> };
    expect(out.incidents[0]!.units).toEqual(['51']);
  });

  it('keeps units mentioned only via the rdio_units.csv label corpus', () => {
    const input = {
      incidents: [
        {
          title: 'Fire',
          summary: 's',
          units: ['Pumper 99'],
          transcripts: [
            { call_id: 1, text: 'unit responding to fire alarm' },
          ],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(
      input,
      [{ call_id: 1 }],
      { knownLabels: ['Pumper 99', 'HP 12'] },
    ) as { incidents: Array<{ units: unknown[] }> };
    expect(out.incidents[0]!.units).toEqual(['Pumper 99']);
  });

  it('drops units that are neither NATO nor mentioned in any way', () => {
    const input = {
      incidents: [
        {
          title: 'Fire',
          summary: 's',
          units: ['ZZ 88'],
          transcripts: [
            { call_id: 1, text: 'unit responding to fire alarm' },
          ],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 },
    ]) as { incidents: Array<{ units: unknown[] }> };
    expect(out.incidents[0]!.units).toEqual([]);
  });

  it('drops NSWPF unit dicts and respects legacy dict shape', () => {
    const input = {
      incidents: [
        {
          title: 'Fire',
          summary: 's',
          units: [
            { id: 'HP 77', agency: 'NSWPF' }, // dropped: police agency
            { id: 'P 12', agency: 'FRNSW' }, // kept if mentioned
          ],
          transcripts: [
            { call_id: 1, text: 'P 12 on scene with hose' },
          ],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 },
    ]) as { incidents: Array<{ units: Array<Record<string, unknown>> }> };
    expect(out.incidents[0]!.units).toHaveLength(1);
    expect(out.incidents[0]!.units[0]!['id']).toBe('P 12');
  });

  it('merges duplicate incidents via >=50% transcript call_id overlap', () => {
    const input = {
      incidents: [
        {
          title: 'Fire (rich)',
          summary: 'Detailed multi-line summary describing the incident',
          units: ['P 1', 'P 2', 'P 3'],
          transcripts: [
            { call_id: 1, text: 'a' },
            { call_id: 2, text: 'b' },
            { call_id: 3, text: 'c' },
            { call_id: 4, text: 'd' },
          ],
        },
        {
          title: 'Fire (poor)',
          summary: 'short',
          transcripts: [
            { call_id: 1, text: 'a' },
            { call_id: 2, text: 'b' },
          ],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 }, { call_id: 2 }, { call_id: 3 }, { call_id: 4 },
    ]) as { incidents: Array<{ title: string }>; incident_count: number };
    // The richer incident should win.
    expect(out.incidents).toHaveLength(1);
    expect(out.incidents[0]!.title).toBe('Fire (rich)');
    expect(out.incident_count).toBe(1);
  });

  it('does not merge incidents with zero call_id overlap', () => {
    const input = {
      incidents: [
        {
          title: 'Fire A',
          summary: 'a',
          transcripts: [{ call_id: 1, text: 'a' }, { call_id: 2, text: 'b' }],
        },
        {
          title: 'Fire B',
          summary: 'b',
          transcripts: [{ call_id: 3, text: 'c' }, { call_id: 4, text: 'd' }],
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, [
      { call_id: 1 }, { call_id: 2 }, { call_id: 3 }, { call_id: 4 },
    ]) as { incidents: unknown[] };
    expect(out.incidents).toHaveLength(2);
  });

  it('does not truncate transcripts[] (LLM controls truncation)', () => {
    const trs = Array.from({ length: 30 }, (_, i) => ({
      call_id: i + 1,
      text: `m${i}`,
    }));
    const calls = Array.from({ length: 30 }, (_, i) => ({ call_id: i + 1 }));
    const input = {
      incidents: [
        {
          title: 'Big one',
          summary: 's',
          transcripts: trs,
          transcripts_truncated: true,
        },
      ],
    };
    const out = validateStructuredAgainstTranscripts(input, calls) as {
      incidents: Array<{
        transcripts: unknown[];
        transcripts_truncated: boolean;
      }>;
    };
    expect(out.incidents[0]!.transcripts).toHaveLength(30);
    expect(out.incidents[0]!.transcripts_truncated).toBe(true);
  });

  it('sets incident_count to the deduped length', () => {
    const input = { incidents: [] };
    const out = validateStructuredAgainstTranscripts(input, []) as {
      incident_count: number;
    };
    expect(out.incident_count).toBe(0);
  });
});
