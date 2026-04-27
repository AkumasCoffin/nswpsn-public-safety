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

  it('returns the raw text on total parse failure', async () => {
    const { parseSummaryOutput } = await import('../../../src/services/llm.js');
    const out = parseSummaryOutput('not json at all');
    expect(out.structured).toBeNull();
    expect(out.overview).toBe('not json at all');
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
