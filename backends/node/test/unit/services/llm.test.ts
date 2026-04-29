/**
 * Gemini summary helpers — covers the pure functions (parser, prompt
 * format) without firing live HTTP. The end-to-end trigger flow is
 * covered separately via /api/summaries/trigger smoke tests with a
 * mocked Gemini client.
 */
import { describe, it, expect, vi } from 'vitest';

vi.mock('../../../src/services/rdio.js', async () => {
  const actual = await vi.importActual<typeof import('../../../src/services/rdio.js')>(
    '../../../src/services/rdio.js',
  );
  return {
    ...actual,
    resolveLabels: vi.fn(async (s: number, t: number) => ({
      systemLabel: `Sys ${s}`,
      talkgroupLabel: `TG ${t}`,
    })),
    getUnitLabel: () => null,
    ensureUnitLabelsLoaded: vi.fn(async () => undefined),
  };
});

describe('parseSummaryOutput', () => {
  it('parses raw JSON', async () => {
    const { parseSummaryOutput } = await import('../../../src/services/llm.js');
    const out = parseSummaryOutput('{"overview":"hello","incidents":[]}');
    expect(out.overview).toBe('hello');
    expect(out.structured?.['incidents']).toEqual([]);
  });

  it('strips ```json fences', async () => {
    const { parseSummaryOutput } = await import('../../../src/services/llm.js');
    const out = parseSummaryOutput('```json\n{"overview":"fenced"}\n```');
    expect(out.overview).toBe('fenced');
  });

  it('falls back to brace extraction', async () => {
    const { parseSummaryOutput } = await import('../../../src/services/llm.js');
    const out = parseSummaryOutput('garbage {"overview":"braced"} trailing');
    expect(out.overview).toBe('braced');
  });

  it('scrubs the doubled-quote field-name typo (Gemini occasionally emits `""foo":`)', async () => {
    const { parseSummaryOutput } = await import('../../../src/services/llm.js');
    // Two opening quotes, one closing — the actual typo python's
    // `_scrub_llm_typos` was written for. Doubled-on-both-sides isn't
    // a real Gemini output and would defeat any field-name regex.
    const out = parseSummaryOutput('{""overview":"scrubbed","x":1}');
    expect(out.overview).toBe('scrubbed');
  });

  it('uses quiet_hour fallback when overview is missing', async () => {
    const { parseSummaryOutput } = await import('../../../src/services/llm.js');
    const out = parseSummaryOutput('{"quiet_hour": true}');
    expect(out.overview).toContain('Quiet hour');
  });

  it('returns empty overview on total parse failure (avoids raw-JSON leak into embed)', async () => {
    const { parseSummaryOutput } = await import('../../../src/services/llm.js');
    const out = parseSummaryOutput('not json at all');
    expect(out.structured).toBeNull();
    // Empty overview lets the embed fall through to the incidents
    // builder or a placeholder. Returning the raw response here would
    // dump raw LLM text (often half-broken JSON) into the discord
    // embed verbatim.
    expect(out.overview).toBe('');
  });

  it('coerces non-string overview to empty (rejects object/array values)', async () => {
    const { parseSummaryOutput } = await import('../../../src/services/llm.js');
    // Gemini occasionally emits {"overview": {...}} or {"overview": [...]}.
    // Without the type-guard those would render as [object Object] in
    // the embed.
    const obj = parseSummaryOutput('{"overview": {"text":"oops"}, "x":1}');
    expect(obj.overview).toBe('');
    expect(obj.structured).toEqual({ overview: { text: 'oops' }, x: 1 });
    const arr = parseSummaryOutput('{"overview": ["a","b"]}');
    expect(arr.overview).toBe('');
  });
});

describe('formatRdioPrompt', () => {
  it('groups by (system, talkgroup) and tags each line with #call_id', async () => {
    const { formatRdioPrompt } = await import('../../../src/services/llm.js');
    const calls = [
      {
        call_id: 1,
        date_time: new Date('2026-04-25T01:00:00Z'),
        system: 1,
        talkgroup: 50,
        transcript: 'fire reported',
        source: null,
        sources: null,
      },
      {
        call_id: 2,
        date_time: new Date('2026-04-25T01:01:00Z'),
        system: 1,
        talkgroup: 50,
        transcript: 'units en route',
        source: 999,
        sources: null,
      },
      {
        call_id: 3,
        date_time: new Date('2026-04-25T01:02:00Z'),
        system: 2,
        talkgroup: 80,
        transcript: 'all clear',
        source: null,
        sources: null,
      },
    ];
    const { prompt, totalChars } = await formatRdioPrompt(calls, 'test period');
    expect(prompt).toContain('Period: test period');
    expect(prompt).toContain('=== Sys 1 — TG 50 (2 transmissions) ===');
    expect(prompt).toContain('=== Sys 2 — TG 80 (1 transmissions) ===');
    expect(prompt).toContain('#1');
    expect(prompt).toContain('RID:999');
    expect(totalChars).toBe('fire reportedunits en routeall clear'.length);
  });

  it('drops calls with empty transcripts', async () => {
    const { formatRdioPrompt } = await import('../../../src/services/llm.js');
    const calls = [
      {
        call_id: 1,
        date_time: new Date('2026-04-25T01:00:00Z'),
        system: 1,
        talkgroup: 50,
        transcript: '   ',
        source: null,
        sources: null,
      },
    ];
    const { prompt, totalChars } = await formatRdioPrompt(calls, 'p');
    expect(totalChars).toBe(0);
    expect(prompt).not.toContain('===');
  });
});

describe('dedupeCalls', () => {
  type Row = Parameters<
    typeof import('../../../src/services/llm.js').dedupeCalls
  >[0][number];
  const mkRow = (overrides: Partial<Row>): Row => ({
    call_id: 0,
    date_time: new Date('2026-04-29T01:00:00Z'),
    system: 1,
    talkgroup: 50,
    transcript: '',
    source: null,
    sources: null,
    ...overrides,
  });

  it('drops a literal repeat with same talkgroup + RID within window', async () => {
    const { dedupeCalls } = await import('../../../src/services/llm.js');
    const out = dedupeCalls([
      mkRow({
        call_id: 1,
        transcript: 'Pumper 7 on scene',
        source: 999,
        date_time: new Date('2026-04-29T01:00:00Z'),
      }),
      mkRow({
        call_id: 2,
        transcript: 'pumper 7 on scene',
        source: 999,
        date_time: new Date('2026-04-29T01:00:30Z'),
      }),
    ]);
    expect(out.map((r) => r.call_id)).toEqual([1]);
  });

  it('keeps the longer (richer) row when one is a prefix of the other', async () => {
    const { dedupeCalls } = await import('../../../src/services/llm.js');
    const out = dedupeCalls([
      mkRow({
        call_id: 1,
        transcript: 'Pumper 7 on scene',
        source: 999,
        date_time: new Date('2026-04-29T01:00:00Z'),
      }),
      mkRow({
        call_id: 2,
        transcript: 'Pumper 7 on scene, BA crew committing',
        source: 999,
        date_time: new Date('2026-04-29T01:00:30Z'),
      }),
    ]);
    expect(out.map((r) => r.call_id)).toEqual([2]);
    expect(out[0]?.transcript).toContain('BA crew committing');
  });

  it('keeps both rows when RIDs differ', async () => {
    const { dedupeCalls } = await import('../../../src/services/llm.js');
    const out = dedupeCalls([
      mkRow({ call_id: 1, transcript: 'on scene', source: 100 }),
      mkRow({ call_id: 2, transcript: 'on scene', source: 200 }),
    ]);
    expect(out.map((r) => r.call_id)).toEqual([1, 2]);
  });

  it('keeps both rows when talkgroups differ', async () => {
    const { dedupeCalls } = await import('../../../src/services/llm.js');
    const out = dedupeCalls([
      mkRow({ call_id: 1, transcript: 'on scene', talkgroup: 50, source: 999 }),
      mkRow({ call_id: 2, transcript: 'on scene', talkgroup: 60, source: 999 }),
    ]);
    expect(out.map((r) => r.call_id)).toEqual([1, 2]);
  });

  it('keeps both rows when timestamps are >180s apart', async () => {
    const { dedupeCalls } = await import('../../../src/services/llm.js');
    const out = dedupeCalls([
      mkRow({
        call_id: 1,
        transcript: 'on scene',
        source: 999,
        date_time: new Date('2026-04-29T01:00:00Z'),
      }),
      mkRow({
        call_id: 2,
        transcript: 'on scene',
        source: 999,
        date_time: new Date('2026-04-29T01:05:00Z'),
      }),
    ]);
    expect(out.map((r) => r.call_id)).toEqual([1, 2]);
  });

  it('dedups on talkgroup+text alone when one side is missing an RID', async () => {
    const { dedupeCalls } = await import('../../../src/services/llm.js');
    const out = dedupeCalls([
      mkRow({ call_id: 1, transcript: 'on scene', source: 999 }),
      mkRow({ call_id: 2, transcript: 'on scene', source: null }),
    ]);
    expect(out.map((r) => r.call_id)).toEqual([1]);
  });

  it('passes through empty/whitespace transcripts unchanged', async () => {
    const { dedupeCalls } = await import('../../../src/services/llm.js');
    const out = dedupeCalls([
      mkRow({ call_id: 1, transcript: '   ' }),
      mkRow({ call_id: 2, transcript: '' }),
      mkRow({ call_id: 3, transcript: 'real traffic' }),
    ]);
    expect(out.map((r) => r.call_id)).toEqual([1, 2, 3]);
  });

  it('treats punctuation/casing as the same text for prefix matching', async () => {
    const { dedupeCalls } = await import('../../../src/services/llm.js');
    const out = dedupeCalls([
      mkRow({ call_id: 1, transcript: 'Pumper 7, on scene.', source: 999 }),
      mkRow({ call_id: 2, transcript: 'pumper 7 on scene', source: 999 }),
    ]);
    expect(out).toHaveLength(1);
  });
});

describe('localHourStartUtc', () => {
  it('rounds to top-of-hour for whole-hour offset (Australia/Sydney)', async () => {
    const { _testHooks } = await import('../../../src/services/llm.js');
    // 2026-04-29 13:55 AEST is 2026-04-29 03:55 UTC. AEST is UTC+10
    // (April is post-DST in Sydney). Top of local hour = 13:00 AEST =
    // 03:00 UTC.
    const now = new Date('2026-04-29T03:55:00Z');
    const top = _testHooks.localHourStartUtc(now, 'Australia/Sydney');
    expect(top.toISOString()).toBe('2026-04-29T03:00:00.000Z');
  });

  it('rounds to top-of-hour for half-hour offset (Australia/Adelaide)', async () => {
    const { _testHooks } = await import('../../../src/services/llm.js');
    // ACST is UTC+9:30 (April is post-DST in Adelaide). 2026-04-29
    // 13:55 ACST is 04:25 UTC. Top of local hour = 13:00 ACST = 03:30 UTC.
    // A naive UTC-rounding would have produced 04:00 UTC which is
    // 13:30 ACST — half an hour off the local clock.
    const now = new Date('2026-04-29T04:25:00Z');
    const top = _testHooks.localHourStartUtc(now, 'Australia/Adelaide');
    expect(top.toISOString()).toBe('2026-04-29T03:30:00.000Z');
  });

  it('is idempotent when called on a top-of-hour instant', async () => {
    const { _testHooks } = await import('../../../src/services/llm.js');
    const now = new Date('2026-04-29T03:00:00Z'); // = 13:00 AEST
    const top = _testHooks.localHourStartUtc(now, 'Australia/Sydney');
    expect(top.toISOString()).toBe('2026-04-29T03:00:00.000Z');
  });
});

describe('nextFireTimeMs', () => {
  // The function reads system time directly via Date.now()/new Date().
  // Use vi.setSystemTime to make it deterministic, then verify the
  // computed fire-time lands on the next :55:00 in SUMMARY_TZ
  // (Australia/Sydney → UTC+10 in April).
  it('lands on the next :55:00 mid-hour', async () => {
    vi.useFakeTimers();
    try {
      // 13:30:15 AEST = 03:30:15 UTC. Next :55:00 = 13:55:00 AEST = 03:55:00 UTC.
      vi.setSystemTime(new Date('2026-04-29T03:30:15Z'));
      const { _testHooks } = await import('../../../src/services/llm.js');
      const fire = new Date(_testHooks.nextFireTimeMs());
      expect(fire.toISOString()).toBe('2026-04-29T03:55:00.000Z');
    } finally {
      vi.useRealTimers();
    }
  });

  it('skips to the next hour when called exactly at :55:00', async () => {
    vi.useFakeTimers();
    try {
      // 13:55:00 AEST. Next fire should be 14:55:00 AEST (one hour ahead)
      // — the existing one fires now, scheduler arms the next.
      vi.setSystemTime(new Date('2026-04-29T03:55:00Z'));
      const { _testHooks } = await import('../../../src/services/llm.js');
      const fire = new Date(_testHooks.nextFireTimeMs());
      expect(fire.toISOString()).toBe('2026-04-29T04:55:00.000Z');
    } finally {
      vi.useRealTimers();
    }
  });

  it('lands on the next :55:00 just after the previous fire', async () => {
    vi.useFakeTimers();
    try {
      // 13:55:30 — 30s past the previous fire. Next = 14:55:00.
      vi.setSystemTime(new Date('2026-04-29T03:55:30Z'));
      const { _testHooks } = await import('../../../src/services/llm.js');
      const fire = new Date(_testHooks.nextFireTimeMs());
      expect(fire.toISOString()).toBe('2026-04-29T04:55:00.000Z');
    } finally {
      vi.useRealTimers();
    }
  });

  it('strips sub-second drift', async () => {
    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date('2026-04-29T03:30:15.789Z'));
      const { _testHooks } = await import('../../../src/services/llm.js');
      const fire = new Date(_testHooks.nextFireTimeMs());
      // No millisecond residual.
      expect(fire.getUTCMilliseconds()).toBe(0);
      expect(fire.toISOString()).toBe('2026-04-29T03:55:00.000Z');
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('/api/summaries/trigger', () => {
  it('503s when GEMINI_API_KEY is unset', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/summaries/trigger?type=hourly', {
      method: 'POST',
      headers: { 'X-API-Key': 'test-api-key' },
    });
    // Either 503 (no rdio configured) or 503 (no gemini key) — both
    // come from the same handler. Just assert the env-gating works.
    expect(res.status).toBe(503);
  });
});
