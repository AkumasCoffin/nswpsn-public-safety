/**
 * The bootstrap tick of the rdio → ntfy incident notifier.
 *
 * Bootstrap exists to suppress a burst that was already under way when the
 * backend restarted. It must not also suppress the first burst that starts
 * AFTER a restart, which is what it did while the flag was keyed on the first
 * tick that happened to see one.
 *
 * Module state is one-shot per import, so each test re-imports the module
 * with vi.resetModules() to get a fresh "just booted" process.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const rdioQuery = vi.fn();
const poolQuery = vi.fn();
const ntfyFetch = vi.fn(async () => ({ ok: true, status: 200, text: async () => '' }));

vi.mock('undici', () => ({ fetch: ntfyFetch }));
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => ({ query: poolQuery })),
}));
vi.mock('../../../src/services/rdio.js', () => ({
  getRdioPool: vi.fn(async () => ({ query: rdioQuery })),
  isRdioConfigured: () => true,
  resolveLabels: vi.fn(async () => ({ systemLabel: 'GRN', talkgroupLabel: 'Fire Comms' })),
  RDIO_CALLS_FROM: '"rdioScannerCalls"',
  RDIO_TRANSCRIPT: '"transcript"',
}));
vi.mock('../../../src/config.js', () => ({
  config: {
    NTFY_BASE_URL: 'https://ntfy.example',
    NTFY_TOPIC: 'incidents',
    NTFY_TOKEN: '',
    RDIO_ALERT_COOLDOWN_MIN: 20,
    RDIO_ALERT_KEYWORDS: '',
    RDIO_ALERT_URGENT_KEYWORDS: '',
    RDIO_ALERT_POLL_SECS: 60,
    // Off, so the gate under test is the bootstrap flag and nothing else.
    RDIO_ALERT_REQUIRE_KEYWORD: false,
    RDIO_BURST_THRESHOLD: 5,
    RDIO_BURST_WINDOW_MIN: 5,
    RDIO_CALL_URL_BASE: 'https://rdio.example',
    RDIO_INCIDENT_ALERTS_ENABLED: true,
    SUMMARY_TZ: 'Australia/Sydney',
  },
}));

/** One bursting talkgroup, shaped as detectBursts' query returns it. */
const BURST = {
  system: 1,
  talkgroup: 12001,
  n: 9,
  latest_id: 4242,
  calls: [{ id: 4242, transcript: 'structure fire main st', dt: '2026-10-13T02:00:00' }],
  all_text: 'structure fire main st',
};

/** Fresh module state, i.e. a just-restarted backend. */
async function freshModule() {
  vi.resetModules();
  return import('../../../src/services/rdioIncidentAlerts.js');
}

/** detectBursts answers from this queue, one tick at a time. */
function armTicks(ticks: Array<Array<typeof BURST>>) {
  let i = 0;
  rdioQuery.mockImplementation(async () => ({ rows: ticks[i++] ?? [] }));
  poolQuery.mockImplementation(async () => ({ rows: [], rowCount: 0 }));
}

beforeEach(() => {
  rdioQuery.mockReset();
  poolQuery.mockReset();
  ntfyFetch.mockClear();
});

describe('runRdioIncidentAlertsOnce bootstrap', () => {
  it('pushes for the first burst that starts after a quiet restart', async () => {
    // The restart landed in a quiet moment — which is the normal case, since
    // every deploy restarts the backend. Bootstrap had nothing to record, so
    // it must be finished with, not left armed to swallow the next incident
    // and put it on cooldown as well.
    const mod = await freshModule();
    armTicks([[], [BURST]]);

    await mod.runRdioIncidentAlertsOnce();
    expect(ntfyFetch).not.toHaveBeenCalled();

    await mod.runRdioIncidentAlertsOnce();
    expect(ntfyFetch).toHaveBeenCalledTimes(1);
  });

  it('still suppresses a burst already under way at restart', async () => {
    // The reason bootstrap exists: a long job's chatter spans many minutes, so
    // a restart mid-incident would otherwise re-alert something subscribers
    // were already told about.
    const mod = await freshModule();
    armTicks([[BURST]]);

    await mod.runRdioIncidentAlertsOnce();
    expect(ntfyFetch).not.toHaveBeenCalled();
    // Recorded as on-cooldown instead, so the next tick does not treat it as
    // new either.
    const cooldownWrites = poolQuery.mock.calls.filter((c) =>
      String(c[0]).includes('INSERT INTO ntfy_incident_cooldown'),
    );
    expect(cooldownWrites).toHaveLength(1);
    expect(cooldownWrites[0]?.[1]).toEqual([BURST.system, BURST.talkgroup]);
  });
});
