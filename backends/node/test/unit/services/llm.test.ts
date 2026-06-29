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

  it('drops junk transcripts (url / garbled / non-english) before grouping', async () => {
    const { formatRdioPrompt } = await import('../../../src/services/llm.js');
    const mk = (call_id: number, transcript: string) => ({
      call_id,
      date_time: new Date('2026-04-25T01:00:00Z'),
      system: 1,
      talkgroup: 50,
      transcript,
      source: null,
      sources: null,
    });
    const calls = [
      mk(1, 'Pumper 7 on scene, no signs of fire'), // keep
      mk(2, 'Subtitles by the Amara.org community'), // url
      mk(3, 'you you you you you'), // garbled
      mk(4, '请订阅我的频道'), // non-english
    ];
    const { prompt, totalChars, inputMap } = await formatRdioPrompt(calls, 'p');
    // Only the real line survives into the prompt and the input map.
    expect(inputMap.size).toBe(1);
    expect(inputMap.has(1)).toBe(true);
    expect(prompt).toContain('Pumper 7 on scene');
    expect(prompt).not.toContain('Amara');
    expect(totalChars).toBe('Pumper 7 on scene, no signs of fire'.length);
  });

  it('emits an inputMap keyed by call_id with verbatim text + uid + context', async () => {
    const { formatRdioPrompt } = await import('../../../src/services/llm.js');
    const calls = [
      {
        call_id: 100,
        date_time: new Date('2026-04-25T01:00:00Z'),
        system: 1,
        talkgroup: 50,
        transcript: 'fire reported',
        source: 2010282,
        sources: null,
      },
      {
        call_id: 101,
        date_time: new Date('2026-04-25T01:01:00Z'),
        system: 2,
        talkgroup: 80,
        transcript: 'all clear',
        source: null,
        sources: null,
      },
    ];
    const { inputMap } = await formatRdioPrompt(calls, 'p');
    expect(inputMap.size).toBe(2);
    const row100 = inputMap.get(100);
    expect(row100?.text).toBe('fire reported');
    expect(row100?.uid).toBe(2010282);
    expect(row100?.context).toBe('Sys 1 — TG 50');
    const row101 = inputMap.get(101);
    expect(row101?.uid).toBeNull();
    expect(row101?.text).toBe('all clear');
  });
});

describe('rebuildIncidents', () => {
  const mkInputMap = (): Map<number, import('../../../src/services/llm.js').RdioInputRow> => {
    const m = new Map<number, import('../../../src/services/llm.js').RdioInputRow>();
    m.set(100, {
      call_id: 100, time: '10:00:01', text: 'truck en route',
      uid: 2010282, unit_label: 'RFS - Greenwall Point 1', context: 'PSN — RFS',
    });
    m.set(101, {
      call_id: 101, time: '10:00:30', text: 'second unit responding',
      uid: 2010282, unit_label: 'RFS - Greenwall Point 1', context: 'PSN — RFS',
    });
    m.set(102, {
      call_id: 102, time: '10:01:00', text: 'arriving on scene',
      uid: 2019977, unit_label: null, context: 'PSN — RFS',
    });
    return m;
  };

  it('rebuilds transcripts + units from inputMap, ignoring LLM-emitted text', async () => {
    const { rebuildIncidents } = await import('../../../src/services/llm.js');
    const input = mkInputMap();
    const structured = {
      incidents: [
        {
          title: 'Fire on George St',
          summary: 'crews respond',
          member_call_ids: [100, 101, 102],
          // LLM emitted these — must be discarded and rebuilt from inputMap.
          transcripts: [
            { call_id: 100, text: 'POLISHED VERSION' },
            { call_id: 101, text: 'ALSO POLISHED' },
          ],
          units: [{ uid: 999, label: 'Fabricated Unit' }],
        },
      ],
    };
    const out = rebuildIncidents(structured, input);
    const incs = out['incidents'] as Array<Record<string, unknown>>;
    expect(incs).toHaveLength(1);
    const trs = incs[0]!['transcripts'] as Array<Record<string, unknown>>;
    expect(trs).toHaveLength(3);
    expect(trs[0]).toEqual({ call_id: 100, time: '10:00:01', text: 'truck en route' });
    expect(trs[2]!['text']).toBe('arriving on scene'); // verbatim, not "ALSO POLISHED"
    // units[] is a deduped string array of friendly labels only. UID
    // 2010282 has a label in the input map; UID 2019977 does not, so it
    // is SKIPPED — bare "UID:<n>" tokens are noise and no longer emitted
    // (see commit "stop showing bare UID tokens in the Units line").
    const units = incs[0]!['units'] as unknown[];
    expect(units).toEqual(['RFS - Greenwall Point 1']);
    expect(out['incident_count']).toBe(1);
  });

  it('drops unknown call_ids and warns', async () => {
    const { rebuildIncidents } = await import('../../../src/services/llm.js');
    const input = mkInputMap();
    const structured = {
      incidents: [
        {
          title: 'mixed',
          member_call_ids: [100, 999, 102, 12345],
        },
      ],
    };
    const out = rebuildIncidents(structured, input);
    const incs = out['incidents'] as Array<Record<string, unknown>>;
    expect(incs[0]!['member_call_ids']).toEqual([100, 102]);
    const trs = incs[0]!['transcripts'] as unknown[];
    expect(trs).toHaveLength(2);
  });

  it('drops incidents that end up empty after filtering', async () => {
    const { rebuildIncidents } = await import('../../../src/services/llm.js');
    const input = mkInputMap();
    const structured = {
      incidents: [
        { title: 'real', member_call_ids: [100] },
        { title: 'fabricated', member_call_ids: [999, 888] },
        { title: 'no ids at all', member_call_ids: [] },
      ],
    };
    const out = rebuildIncidents(structured, input);
    const incs = out['incidents'] as Array<Record<string, unknown>>;
    expect(incs).toHaveLength(1);
    expect(incs[0]!['title']).toBe('real');
    expect(out['incident_count']).toBe(1);
  });

  it('falls back to transcripts[].call_id when member_call_ids is missing (legacy schema)', async () => {
    const { rebuildIncidents } = await import('../../../src/services/llm.js');
    const input = mkInputMap();
    const structured = {
      incidents: [
        {
          title: 'legacy shape',
          // No member_call_ids — read call_ids from transcripts[].
          transcripts: [
            { call_id: 100, text: 'whatever' },
            { call_id: '101', text: 'string id' }, // numeric coerce
            { call_id: 999, text: 'fabricated' },
          ],
        },
      ],
    };
    const out = rebuildIncidents(structured, input);
    const incs = out['incidents'] as Array<Record<string, unknown>>;
    expect(incs[0]!['member_call_ids']).toEqual([100, 101]);
    const trs = incs[0]!['transcripts'] as unknown[];
    expect(trs).toHaveLength(2);
  });

  it('sorts transcripts chronologically by input-map time', async () => {
    const { rebuildIncidents } = await import('../../../src/services/llm.js');
    const input = mkInputMap();
    const structured = {
      incidents: [
        { title: 'reverse order', member_call_ids: [102, 100, 101] },
      ],
    };
    const out = rebuildIncidents(structured, input);
    const incs = out['incidents'] as Array<Record<string, unknown>>;
    const trs = incs[0]!['transcripts'] as Array<Record<string, unknown>>;
    expect(trs.map((t) => t['call_id'])).toEqual([100, 101, 102]);
  });

  it('caps transcripts at 10 when more rows are present, keeps full member_call_ids', async () => {
    const { rebuildIncidents } = await import('../../../src/services/llm.js');
    type Row = import('../../../src/services/llm.js').RdioInputRow;
    const bigMap = new Map<number, Row>();
    // 23 rows. After truncation we expect first 8 + last 2 = 10
    // rendered, but member_call_ids stays as 23.
    for (let i = 0; i < 23; i++) {
      const id = 1000 + i;
      const hh = 10 + Math.floor(i / 60);
      const mm = i % 60;
      bigMap.set(id, {
        call_id: id,
        time: `${hh}:${String(mm).padStart(2, '0')}:00`,
        text: `row ${i}`,
        uid: null,
        unit_label: null,
        context: 'PSN',
      });
    }
    const structured = {
      incidents: [
        {
          title: 'big',
          summary: '...',
          member_call_ids: Array.from(bigMap.keys()),
        },
      ],
    };
    const out = rebuildIncidents(structured, bigMap);
    const incs = out['incidents'] as Array<Record<string, unknown>>;
    expect((incs[0]!['member_call_ids'] as number[]).length).toBe(23);
    const trs = incs[0]!['transcripts'] as Array<Record<string, unknown>>;
    expect(trs).toHaveLength(10);
    // First 8 are rows 0..7 (call_ids 1000..1007); last 2 are rows
    // 21..22 (call_ids 1021..1022).
    expect(trs.map((t) => t['call_id'])).toEqual([
      1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1021, 1022,
    ]);
    expect(incs[0]!['transcripts_truncated']).toBe(true);
    expect(incs[0]!['transcripts_total']).toBe(23);
  });

  it('does not set transcripts_truncated when at or below the cap', async () => {
    const { rebuildIncidents } = await import('../../../src/services/llm.js');
    type Row = import('../../../src/services/llm.js').RdioInputRow;
    const m = new Map<number, Row>();
    for (let i = 0; i < 10; i++) {
      m.set(1000 + i, {
        call_id: 1000 + i,
        time: `10:${String(i).padStart(2, '0')}:00`,
        text: `r${i}`,
        uid: null,
        unit_label: null,
        context: 'PSN',
      });
    }
    const structured = {
      incidents: [
        { title: 'exactly 10', member_call_ids: Array.from(m.keys()) },
      ],
    };
    const out = rebuildIncidents(structured, m);
    const incs = out['incidents'] as Array<Record<string, unknown>>;
    const trs = incs[0]!['transcripts'] as unknown[];
    expect(trs).toHaveLength(10);
    expect(incs[0]!['transcripts_truncated']).toBeUndefined();
    expect(incs[0]!['transcripts_total']).toBeUndefined();
  });

  it('passes through if incidents is not an array', async () => {
    const { rebuildIncidents } = await import('../../../src/services/llm.js');
    const input = mkInputMap();
    const structured = { incidents: 'nope', other: 1 };
    const out = rebuildIncidents(structured, input);
    expect(out['incidents']).toBe('nope');
    expect(out['other']).toBe(1);
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

describe('resolveSummaryWindow (quiet-hour bucketing)', () => {
  // AEST = UTC+10 (no DST). Local HH:55 = UTC (HH-10):55. Tests pick a
  // mid-May 2026 date so the offset is deterministic regardless of DST.
  const TZ = 'Australia/Sydney';

  it('fires a normal 1h window for active hours', async () => {
    const { resolveSummaryWindow } = await import('../../../src/services/llm.js');
    const fire = new Date('2026-05-20T04:55:00.000Z'); // 14:55 local Wed
    const w = resolveSummaryWindow(fire, TZ);
    expect(w).not.toBeNull();
    expect(w!.startUtc.toISOString()).toBe('2026-05-20T04:00:00.000Z');
    expect(w!.endUtc.toISOString()).toBe('2026-05-20T05:00:00.000Z');
  });

  it('fires a merged 2h window at the close of the 22-24 bucket', async () => {
    const { resolveSummaryWindow } = await import('../../../src/services/llm.js');
    const fire = new Date('2026-05-20T13:55:00.000Z'); // 23:55 local Wed
    const w = resolveSummaryWindow(fire, TZ);
    expect(w).not.toBeNull();
    expect(w!.startUtc.toISOString()).toBe('2026-05-20T12:00:00.000Z');
    expect(w!.endUtc.toISOString()).toBe('2026-05-20T14:00:00.000Z');
  });

  it('skips mid-bucket hours', async () => {
    const { resolveSummaryWindow } = await import('../../../src/services/llm.js');
    expect(resolveSummaryWindow(new Date('2026-05-20T12:55:00.000Z'), TZ)).toBeNull(); // 22:55 local
    expect(resolveSummaryWindow(new Date('2026-05-20T14:55:00.000Z'), TZ)).toBeNull(); // 00:55 local
    expect(resolveSummaryWindow(new Date('2026-05-20T18:55:00.000Z'), TZ)).toBeNull(); // 04:55 local
  });

  it('fires merged 3h windows at the close of the 00-03 and 03-06 buckets', async () => {
    const { resolveSummaryWindow } = await import('../../../src/services/llm.js');
    // 02:55 local Thu = 16:55Z Wed
    const w1 = resolveSummaryWindow(
      new Date('2026-05-20T16:55:00.000Z'),
      TZ,
    );
    expect(w1).not.toBeNull();
    expect(w1!.startUtc.toISOString()).toBe('2026-05-20T14:00:00.000Z');
    expect(w1!.endUtc.toISOString()).toBe('2026-05-20T17:00:00.000Z');

    // 05:55 local Thu = 19:55Z Wed
    const w2 = resolveSummaryWindow(
      new Date('2026-05-20T19:55:00.000Z'),
      TZ,
    );
    expect(w2).not.toBeNull();
    expect(w2!.startUtc.toISOString()).toBe('2026-05-20T17:00:00.000Z');
    expect(w2!.endUtc.toISOString()).toBe('2026-05-20T20:00:00.000Z');
  });

  it('fires normally at 06:55 — first non-bucket hour after the merge', async () => {
    const { resolveSummaryWindow } = await import('../../../src/services/llm.js');
    const fire = new Date('2026-05-20T20:55:00.000Z'); // 06:55 local
    const w = resolveSummaryWindow(fire, TZ);
    expect(w).not.toBeNull();
    expect(w!.startUtc.toISOString()).toBe('2026-05-20T20:00:00.000Z');
    expect(w!.endUtc.toISOString()).toBe('2026-05-20T21:00:00.000Z');
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
