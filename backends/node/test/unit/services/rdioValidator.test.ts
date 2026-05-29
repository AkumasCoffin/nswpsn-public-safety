/**
 * Tests for polishStructuredIncidents — the trimmed post-rebuilder
 * polish that runs after services/llm.ts → rebuildIncidents().
 *
 * Replaces the test suite that targeted the now-removed
 * validateStructuredAgainstTranscripts (the python parity polish that
 * handled call_id sanity, NSWPF redaction, NATO/units checks, etc).
 */
import { describe, it, expect } from 'vitest';
import { polishStructuredIncidents } from '../../../src/services/rdioValidator.js';

describe('polishStructuredIncidents', () => {
  it('passes through non-object input unchanged', () => {
    expect(polishStructuredIncidents(null)).toBeNull();
    expect(polishStructuredIncidents('hello')).toBe('hello');
    const arr = [1, 2, 3];
    expect(polishStructuredIncidents(arr)).toBe(arr);
  });

  it('passes through when incidents is not an array', () => {
    const input = { incidents: 'oops', other: 1 };
    const out = polishStructuredIncidents(input) as { other: number };
    expect(out.other).toBe(1);
  });

  it('drops legacy timeline[] without touching other fields', () => {
    const input = {
      incidents: [
        {
          title: 'real',
          summary: 'crews respond',
          member_call_ids: [100],
          timeline: [{ time: '03:11', text: 'legacy row' }],
        },
      ],
    };
    const out = polishStructuredIncidents(input) as {
      incidents: Array<Record<string, unknown>>;
    };
    expect(out.incidents).toHaveLength(1);
    expect('timeline' in out.incidents[0]!).toBe(false);
    expect(out.incidents[0]!['title']).toBe('real');
  });

  it('drops incidents that echo a prompt example fingerprint', () => {
    const input = {
      incidents: [
        {
          title: 'Hazmat ammonia leak at Vesperton Loop',
          summary: 'crews from Maracine responded',
          member_call_ids: [735101, 735108],
        },
        {
          title: 'Real fire on King Street',
          summary: 'Pumper 7 attended',
          member_call_ids: [500801],
        },
      ],
    };
    const out = polishStructuredIncidents(input) as {
      incidents: Array<Record<string, unknown>>;
      incident_count: number;
    };
    expect(out.incidents).toHaveLength(1);
    expect(out.incidents[0]!['title']).toBe('Real fire on King Street');
    expect(out.incident_count).toBe(1);
  });

  it('drops the older prompt-example fingerprints too', () => {
    const input = {
      incidents: [
        // Original pre-2026-05-15 example.
        {
          title: 'Machinery fire at recycling plant',
          summary: 'crews respond to Elizabeth Street',
          member_call_ids: [1],
        },
        // 2026-05-15+ Brindwell/Karoneth example.
        {
          title: 'Hazmat fuel spill on Brindwell Loop',
          summary: 'units staged at Karoneth',
          member_call_ids: [2],
        },
        // Real incident.
        {
          title: 'Real bushfire near Lithgow',
          summary: 'RFS attending',
          member_call_ids: [3],
        },
      ],
    };
    const out = polishStructuredIncidents(input) as {
      incidents: Array<Record<string, unknown>>;
    };
    expect(out.incidents).toHaveLength(1);
    expect(out.incidents[0]!['title']).toBe('Real bushfire near Lithgow');
  });

  it('dedups incidents by member_call_ids overlap — proper subset', () => {
    const input = {
      incidents: [
        {
          title: 'fire (full coverage)',
          summary: 'all calls',
          member_call_ids: [100, 101, 102, 103, 104],
        },
        {
          title: 'fire (subset)',
          summary: 'fewer',
          member_call_ids: [102, 103, 104],
        },
      ],
    };
    const out = polishStructuredIncidents(input) as {
      incidents: Array<Record<string, unknown>>;
    };
    expect(out.incidents).toHaveLength(1);
    expect(out.incidents[0]!['title']).toBe('fire (full coverage)');
  });

  it('dedups incidents by member_call_ids overlap — >=50% smaller side', () => {
    const input = {
      incidents: [
        {
          title: 'one',
          summary: 'long summary makes it richer',
          member_call_ids: [100, 101, 102, 103],
        },
        {
          title: 'two',
          summary: 's',
          member_call_ids: [100, 101, 999, 998], // 2/4 = 50% with one
        },
      ],
    };
    const out = polishStructuredIncidents(input) as {
      incidents: Array<Record<string, unknown>>;
    };
    expect(out.incidents).toHaveLength(1);
    expect(out.incidents[0]!['title']).toBe('one');
  });

  it('keeps unrelated incidents (no overlap)', () => {
    const input = {
      incidents: [
        { title: 'a', summary: '...', member_call_ids: [100, 101] },
        { title: 'b', summary: '...', member_call_ids: [200, 201] },
        { title: 'c', summary: '...', member_call_ids: [300, 301] },
      ],
    };
    const out = polishStructuredIncidents(input) as {
      incidents: unknown[];
      incident_count: number;
    };
    expect(out.incidents).toHaveLength(3);
    expect(out.incident_count).toBe(3);
  });

  it('does not crash on incidents without member_call_ids', () => {
    const input = {
      incidents: [
        { title: 'orphan', summary: 'no ids' },
        { title: 'normal', summary: '...', member_call_ids: [100] },
      ],
    };
    const out = polishStructuredIncidents(input) as {
      incidents: unknown[];
      incident_count: number;
    };
    expect(out.incidents).toHaveLength(2);
    expect(out.incident_count).toBe(2);
  });
});
