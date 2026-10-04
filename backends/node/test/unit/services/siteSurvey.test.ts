/**
 * Site-survey orchestration: the backend-owned pass threshold (65), alt-CC
 * folding into frequencies[], the atomic winner append (dedupe + 64 cap),
 * the aborted path that records-but-never-adds, and the start/install guards.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

// ---------------------------------------------------------------------------
// Fakes. The pool dispatches by SQL text so tests don't depend on call order;
// the same handler backs pool.query and the FOR UPDATE client.
// ---------------------------------------------------------------------------

interface Exec { sql: string; params: unknown[] }
let executed: Exec[] = [];
let surveyRow: Record<string, unknown> | null = null;
let configOverride: Record<string, unknown> = {};
let runningSurveyIds: number[] = [];
let priorSurveyIds: number[] = [];

async function dispatch(sql: string, params: unknown[] = []): Promise<{ rows: unknown[]; rowCount: number }> {
  executed.push({ sql, params });
  if (sql.includes('FROM node_site_surveys WHERE id =')) {
    return { rows: surveyRow ? [surveyRow] : [], rowCount: surveyRow ? 1 : 0 };
  }
  if (sql.includes("status = 'running' LIMIT 1")) {
    return { rows: runningSurveyIds.map((id) => ({ id })), rowCount: runningSurveyIds.length };
  }
  if (sql.includes('FROM node_site_surveys WHERE node_id = $1 LIMIT 1')) {
    return { rows: priorSurveyIds.map((id) => ({ id })), rowCount: priorSurveyIds.length };
  }
  if (sql.includes('INSERT INTO node_site_surveys')) {
    return { rows: [{ id: 77 }], rowCount: 1 };
  }
  if (sql.includes('SELECT config_override FROM nodes')) {
    return { rows: [{ config_override: configOverride }], rowCount: 1 };
  }
  if (sql.includes('UPDATE nodes SET config_override')) {
    configOverride = JSON.parse(params[1] as string) as Record<string, unknown>;
    configOverride['channels'] = JSON.parse(params[2] as string);
    return { rows: [], rowCount: 1 };
  }
  return { rows: [], rowCount: 0 };
}

const fakeClient = {
  query: vi.fn(dispatch),
  release: vi.fn(),
};
const fakePool = {
  query: vi.fn(dispatch),
  connect: vi.fn(async () => fakeClient),
};
vi.mock('../../../src/db/pool.js', () => ({ getPool: vi.fn(async () => fakePool) }));

let fakeNode: Record<string, unknown> | null = null;
vi.mock('../../../src/services/nodes/registry.js', () => ({
  getNode: vi.fn(async () => fakeNode),
}));

const isOnline = vi.fn(() => true);
const sendCmd = vi.fn(async () => ({ ok: true }));
vi.mock('../../../src/services/nodes/hub.js', () => ({
  hub: { isOnline: (...a: unknown[]) => isOnline(...(a as [])), sendCmd: (...a: unknown[]) => sendCmd(...(a as [])) },
}));

const pushConfigToNode = vi.fn(async () => undefined);
vi.mock('../../../src/services/nodes/configPush.js', () => ({
  pushConfigToNode: (...a: unknown[]) => pushConfigToNode(...(a as [])),
}));

const notifyStaff = vi.fn();
vi.mock('../../../src/services/staffNotify.js', () => ({
  notifyStaff: (...a: unknown[]) => notifyStaff(...(a as [])),
}));

let fakeCandidates: { candidates: unknown[]; skipped: unknown[] } = { candidates: [], skipped: [] };
vi.mock('../../../src/services/grnCandidates.js', () => ({
  candidatesForNode: vi.fn(async () => fakeCandidates),
}));

const { startSurvey, maybeStartInstallSurvey, ingestSurveyResults, handleSurveyCommand, runningSurveys, SURVEY_PASS_PCT } =
  await import('../../../src/services/siteSurvey.js');

const NODE = 'node-1';

function radioNode(extra: Record<string, unknown> = {}): Record<string, unknown> {
  return { id: NODE, kind: 'radio', name: 'Shed', lga: 'Alpha', state: 'NSW', config_override: {}, ...extra };
}

function result(over: Record<string, unknown> = {}) {
  return {
    grnKey: null, siteName: 'Good Hill', freqHz: 422_375_000, altFreqHz: null,
    outcome: 'measured', medianPct: 80, samples: 20, signalDbfs: -40, isAlt: false,
    ...over,
  };
}

beforeEach(() => {
  executed = [];
  surveyRow = { id: 77, node_id: NODE, status: 'running', trigger: 'manual' };
  configOverride = {};
  runningSurveyIds = [];
  priorSurveyIds = [];
  fakeNode = radioNode();
  fakeCandidates = { candidates: [], skipped: [] };
  vi.clearAllMocks();
});

const channels = () => (configOverride['channels'] as Array<Record<string, unknown>> | undefined) ?? [];

describe('ingestSurveyResults — threshold and append', () => {
  it('65 is the boundary: exactly 65 passes, 64.9 fails', async () => {
    expect(SURVEY_PASS_PCT).toBe(65);
    const out = await ingestSurveyResults(NODE, 77, [
      result({ siteName: 'Edge Pass', freqHz: 420_000_000, medianPct: 65 }),
      result({ siteName: 'Edge Fail', freqHz: 421_000_000, medianPct: 64.9 }),
    ], null);
    expect(out).toMatchObject({ ok: true, added: 1 });
    expect(channels().map((c) => c['name'])).toEqual(['Edge Pass']);
    const verdicts = executed
      .filter((e) => e.sql.includes('INSERT INTO node_site_survey_results'))
      .map((e) => [e.params[1], e.params[5]]);
    expect(verdicts).toEqual([['Edge Pass', 'pass'], ['Edge Fail', 'fail']]);
    expect(pushConfigToNode).toHaveBeenCalledWith(NODE);
    expect(notifyStaff).toHaveBeenCalledOnce();
  });

  it('noLock and unmeasured verdicts record without adding', async () => {
    const out = await ingestSurveyResults(NODE, 77, [
      result({ siteName: 'Silent', outcome: 'noLock', medianPct: null }),
      result({ siteName: 'Starved', freqHz: 423_000_000, outcome: 'unmeasured', medianPct: null }),
    ], null);
    expect(out).toMatchObject({ ok: true, added: 0 });
    expect(channels()).toEqual([]);
    const verdicts = executed
      .filter((e) => e.sql.includes('INSERT INTO node_site_survey_results'))
      .map((e) => e.params[5]);
    expect(verdicts).toEqual(['noLock', 'unmeasured']);
    expect(pushConfigToNode).not.toHaveBeenCalled();
  });

  it('a passing alt CC rides along as frequencies[]; a failing alt does not', async () => {
    await ingestSurveyResults(NODE, 77, [
      result({ siteName: 'With Alt', freqHz: 420_000_000, altFreqHz: 421_000_000 }),
      result({ siteName: 'With Alt', freqHz: 421_000_000, medianPct: 70, isAlt: true }),
      result({ siteName: 'Alt Flopped', freqHz: 424_000_000, altFreqHz: 425_000_000 }),
      result({ siteName: 'Alt Flopped', freqHz: 425_000_000, medianPct: 10, isAlt: true }),
    ], null);
    const byName = new Map(channels().map((c) => [c['name'], c]));
    expect(byName.get('With Alt')?.['frequencies']).toEqual([421_000_000]);
    expect(byName.get('Alt Flopped')?.['frequencies']).toBeUndefined();
    // alt rows never become channels of their own
    expect(channels()).toHaveLength(2);
  });

  it('dedupes against existing channels by frequency and by name', async () => {
    configOverride = { channels: [
      { name: 'Existing', frequency: 420_000_000 },
      { name: 'Good Hill', frequency: 410_000_000 },
    ] };
    const out = await ingestSurveyResults(NODE, 77, [
      result({ siteName: 'Freq Clash', freqHz: 420_000_000 }),
      result({ siteName: 'Good Hill', freqHz: 422_375_000 }), // name clash, different freq
      result({ siteName: 'Fresh', freqHz: 430_000_000 }),
    ], null);
    expect(out.added).toBe(1);
    expect(channels().map((c) => c['name'])).toEqual(['Existing', 'Good Hill', 'Fresh']);
    const demoted = executed.filter((e) => e.sql.includes('SET added = false'));
    expect(demoted.map((e) => [e.params[1], e.params[2]])).toEqual([
      ['Freq Clash', 'already configured'],
      ['Good Hill', 'already configured'],
    ]);
  });

  it('stops at the 64-channel cap and logs the overflow', async () => {
    configOverride = { channels: Array.from({ length: 63 }, (_, i) => ({
      name: `ch${i}`, frequency: 400_000_000 + i * 12_500,
    })) };
    const out = await ingestSurveyResults(NODE, 77, [
      result({ siteName: 'Fits', freqHz: 430_000_000 }),
      result({ siteName: 'Overflow', freqHz: 431_000_000 }),
    ], null);
    expect(out.added).toBe(1);
    expect(channels()).toHaveLength(64);
    const demoted = executed.filter((e) => e.sql.includes('SET added = false'));
    expect(demoted.map((e) => [e.params[1], e.params[2]])).toEqual([
      ['Overflow', 'channel cap (64) reached'],
    ]);
  });

  it('aborted surveys record every row but add nothing and finish failed', async () => {
    const out = await ingestSurveyResults(NODE, 77, [
      result({ siteName: 'Would Pass', medianPct: 95 }),
    ], 'cancelled', true);
    expect(out).toMatchObject({ ok: true, added: 0 });
    expect(channels()).toEqual([]);
    expect(pushConfigToNode).not.toHaveBeenCalled();
    const fin = executed.find((e) => e.sql.includes("UPDATE node_site_surveys SET status = $4"));
    expect(fin?.params[3]).toBe('failed');
    // the measurement is still on the record
    const inserted = executed.filter((e) => e.sql.includes('INSERT INTO node_site_survey_results'));
    expect(inserted).toHaveLength(1);
    expect(inserted[0]!.params[5]).toBe('pass');
  });

  it('refuses results for an unknown or finished survey', async () => {
    surveyRow = null;
    expect(await ingestSurveyResults(NODE, 99, [result()], null)).toMatchObject({ ok: false, error: 'unknown survey' });
    surveyRow = { id: 77, node_id: NODE, status: 'done', trigger: 'manual' };
    expect(await ingestSurveyResults(NODE, 77, [result()], null)).toMatchObject({ ok: false, error: 'survey is done' });
  });
});

describe('startSurvey — guards', () => {
  beforeEach(() => {
    fakeCandidates = { candidates: [{ name: 'Good Hill', grnKey: null, mhz: 422.375, altMhz: null, lga: 'Alpha' }], skipped: [] };
  });

  it('starts, records skips, and sends the candidate list', async () => {
    fakeCandidates.skipped = [{ name: 'Broken CC', grnKey: null, note: 'control channel not parseable: "TBA"' }];
    const out = await startSurvey(NODE, 'manual', 'staff-1', 2);
    expect(out).toMatchObject({ ok: true, surveyId: 77 });
    expect(sendCmd).toHaveBeenCalledWith(NODE, 'surveySites', {
      surveyId: 77,
      candidates: [{ name: 'Good Hill', grnKey: null, mhz: 422.375, altMhz: null }],
    });
    const skipInsert = executed.find((e) => e.sql.includes("'skipped'"));
    expect(skipInsert?.params[1]).toBe('Broken CC');
    const ins = executed.find((e) => e.sql.includes('INSERT INTO node_site_surveys'));
    expect(ins?.params).toEqual([NODE, 'manual', 'staff-1', 2, 1]);
  });

  it('refuses non-radio, offline, LGA-less, busy, and candidate-less nodes', async () => {
    fakeNode = radioNode({ kind: 'pager' });
    expect((await startSurvey(NODE, 'manual', null)).error).toContain('radio-only');
    fakeNode = radioNode();
    isOnline.mockReturnValueOnce(false);
    expect((await startSurvey(NODE, 'manual', null)).error).toContain('offline');
    fakeNode = radioNode({ lga: null });
    expect((await startSurvey(NODE, 'manual', null)).error).toContain('no LGA');
    fakeNode = radioNode();
    runningSurveyIds = [42];
    expect((await startSurvey(NODE, 'manual', null)).error).toContain('already running');
    runningSurveyIds = [];
    fakeCandidates = { candidates: [], skipped: [{ name: 'x', grnKey: null, note: 'n' }] };
    expect((await startSurvey(NODE, 'manual', null)).error).toContain('no testable GRN sites');
    expect(sendCmd).not.toHaveBeenCalled();
  });

  it('marks the survey failed when the agent refuses the command', async () => {
    sendCmd.mockResolvedValueOnce({ ok: false, message: 'survey already running on agent' } as never);
    const out = await startSurvey(NODE, 'manual', null);
    expect(out).toMatchObject({ ok: false, error: 'survey already running on agent' });
    const fail = executed.find((e) => e.sql.includes("SET status = 'failed'"));
    expect(fail?.params).toEqual([77, 'survey already running on agent']);
  });
});

describe('maybeStartInstallSurvey — once-ever guards', () => {
  beforeEach(() => {
    fakeCandidates = { candidates: [{ name: 'Good Hill', grnKey: null, mhz: 422.375, altMhz: null, lga: 'Alpha' }], skipped: [] };
  });

  const node = (over: Record<string, unknown> = {}) => radioNode(over) as never;

  it('fires once for a fresh radio node with an LGA and no channels', async () => {
    await maybeStartInstallSurvey(node());
    expect(sendCmd).toHaveBeenCalledOnce();
    const ins = executed.find((e) => e.sql.includes('INSERT INTO node_site_surveys'));
    expect(ins?.params).toEqual([NODE, 'install', null, 1, 1]);
  });

  it('never fires for non-radio, configured, LGA-less, or previously surveyed nodes', async () => {
    await maybeStartInstallSurvey(node({ kind: 'pager' }));
    await maybeStartInstallSurvey(node({ config_override: { channels: [{ name: 'x' }] } }));
    await maybeStartInstallSurvey(node({ lga: null }));
    priorSurveyIds = [41];
    await maybeStartInstallSurvey(node());
    expect(sendCmd).not.toHaveBeenCalled();
  });
});

describe('handleSurveyCommand — one path for both staff entry points', () => {
  beforeEach(() => {
    fakeCandidates = { candidates: [{ name: 'Good Hill', grnKey: null, mhz: 422.375, altMhz: null, lga: 'Alpha' }], skipped: [] };
  });

  it('ignores anything that is not a survey verb', async () => {
    expect(await handleSurveyCommand(NODE, 'restartComponent', {}, 'staff-1')).toBeNull();
  });

  it('builds the candidate list server-side from the ring count', async () => {
    const out = await handleSurveyCommand(NODE, 'surveySites', { rings: 3 }, 'staff-1');
    expect(out).toMatchObject({ ok: true });
    // The browser said how far to look; the sites came from the backend.
    const ins = executed.find((e) => e.sql.includes('INSERT INTO node_site_surveys'));
    expect(ins?.params[3]).toBe(3);
    expect(sendCmd).toHaveBeenCalledWith(NODE, 'surveySites', {
      surveyId: 77,
      candidates: [{ name: 'Good Hill', grnKey: null, mhz: 422.375, altMhz: null }],
    });
  });

  it('defaults to one ring when the caller sends junk', async () => {
    await handleSurveyCommand(NODE, 'surveySites', { rings: 'lots' }, null);
    const ins = executed.find((e) => e.sql.includes('INSERT INTO node_site_surveys'));
    expect(ins?.params[3]).toBe(1);
  });

  it('cancelling leaves the survey running so the partial report can land', async () => {
    const out = await handleSurveyCommand(NODE, 'surveyCancel', { surveyId: 77 }, 'staff-1');
    expect(out).toMatchObject({ ok: true });
    expect(sendCmd).toHaveBeenCalledWith(NODE, 'surveyCancel', { surveyId: 77 });
    expect(executed.some((e) => e.sql.includes("SET status = 'failed'"))).toBe(false);
  });

  it('a cancel that cannot reach the node closes the survey itself', async () => {
    sendCmd.mockResolvedValueOnce({ ok: false, message: 'node offline' } as never);
    const out = await handleSurveyCommand(NODE, 'surveyCancel', { surveyId: 77 }, 'staff-1');
    expect(out).toMatchObject({ ok: false });
    const fail = executed.find((e) => e.sql.includes("SET status = 'failed'"));
    expect(fail?.params).toEqual([77, NODE, 'cancel could not reach the node']);
  });
});

describe('runningSurveys — what the staff list shows a badge from', () => {
  it('maps the nodes with an open survey to its id', async () => {
    const rows = [{ id: 12, node_id: 'node-1' }, { id: 13, node_id: 'node-9' }];
    const q = fakePool.query as unknown as { mockImplementationOnce: (f: () => unknown) => void };
    q.mockImplementationOnce(async () => ({ rows, rowCount: rows.length }));
    const out = await runningSurveys();
    expect(out.get('node-1')).toBe(12);
    expect(out.get('node-9')).toBe(13);
    expect(out.has('node-nope')).toBe(false);
  });

  it('ignores a survey old enough to have timed out', async () => {
    await runningSurveys();
    const sql = executed[executed.length - 1]!.sql;
    // The same staleness bound the lazy expiry uses, so the badge cannot
    // outlive the survey it is reporting.
    expect(sql).toContain("status = 'running'");
    expect(sql).toContain('started_at >');
  });
});
