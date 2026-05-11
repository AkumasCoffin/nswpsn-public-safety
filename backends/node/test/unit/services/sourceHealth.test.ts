/**
 * Source-health module tests.
 *
 * Drives the in-memory poller state via the test-only seeders and
 * asserts the snapshot shape + state buckets the admin panel relies on.
 * No timers, no DB — pure functions.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import {
  getSourceHealthSnapshot,
  clearSourceErrors,
} from '../../../src/services/sourceHealth.js';
import {
  _seedSourceStateForTests,
  _resetPollerStateForTests,
} from '../../../src/services/poller.js';
import { registerSource } from '../../../src/services/sourceRegistry.js';

// Snapshot iterates over allSources(), so we register a small set the
// tests can drive directly. We pick names whose thresholds we know
// (rfs/waze/etc) plus a name that falls through to defaults.
beforeEach(() => {
  _resetPollerStateForTests();
  // Re-register on every test so a previous test's state doesn't bleed.
  // (sourceRegistry is module-level state but registerSource overwrites
  // by name, so this is idempotent.)
  registerSource({
    name: 'rfs',
    family: 'rfs',
    intervalActiveMs: 60_000,
    intervalIdleMs: 120_000,
    fetch: async () => ({}),
  });
  registerSource({
    name: 'waze',
    family: 'waze',
    intervalActiveMs: 60_000,
    intervalIdleMs: 120_000,
    fetch: async () => ({}),
  });
  registerSource({
    name: 'unconfigured',
    family: 'misc',
    intervalActiveMs: 60_000,
    intervalIdleMs: 120_000,
    fetch: async () => ({}),
  });
});

describe('getSourceHealthSnapshot', () => {
  it("emits 'unknown' for sources that have never succeeded", () => {
    const rows = getSourceHealthSnapshot();
    const rfs = rows.find((r) => r.name === 'rfs');
    expect(rfs).toBeDefined();
    expect(rfs!.state).toBe('unknown');
    expect(rfs!.last_ok_at).toBeNull();
    expect(rfs!.consec_fails).toBe(0);
  });

  it("emits 'ok' for a recently-successful source", () => {
    const now = Math.floor(Date.now() / 1000);
    _seedSourceStateForTests('rfs', {
      lastSuccessTs: now - 60, // 60s ago, well under rfs soft=600
      lastError: null,
      failureCount: 0,
    });
    const row = getSourceHealthSnapshot().find((r) => r.name === 'rfs');
    expect(row?.state).toBe('ok');
    expect(row?.age_seconds).toBeGreaterThanOrEqual(60);
  });

  it("emits 'degraded' when consec_fails is non-zero", () => {
    const now = Math.floor(Date.now() / 1000);
    _seedSourceStateForTests('rfs', {
      lastSuccessTs: now - 60,
      lastError: 'oh no',
      failureCount: 2,
      lastAttemptTs: now - 5,
    });
    const row = getSourceHealthSnapshot().find((r) => r.name === 'rfs');
    expect(row?.state).toBe('degraded');
    expect(row?.last_error).toBe('oh no');
    // last_error_at is exposed only when lastError is set.
    expect(row?.last_error_at).not.toBeNull();
  });

  it("emits 'degraded' when age exceeds soft but not hard", () => {
    const now = Math.floor(Date.now() / 1000);
    _seedSourceStateForTests('rfs', {
      lastSuccessTs: now - 700, // soft=600, hard=1800
      lastError: null,
      failureCount: 0,
    });
    const row = getSourceHealthSnapshot().find((r) => r.name === 'rfs');
    expect(row?.state).toBe('degraded');
  });

  it("emits 'down' when age exceeds hard threshold", () => {
    const now = Math.floor(Date.now() / 1000);
    _seedSourceStateForTests('rfs', {
      lastSuccessTs: now - 2000, // hard=1800
      lastError: null,
      failureCount: 0,
    });
    const row = getSourceHealthSnapshot().find((r) => r.name === 'rfs');
    expect(row?.state).toBe('down');
  });

  it("emits 'down' when consec_fails >= 5", () => {
    const now = Math.floor(Date.now() / 1000);
    _seedSourceStateForTests('rfs', {
      lastSuccessTs: now - 60,
      lastError: 'still broken',
      failureCount: 5,
      lastAttemptTs: now,
    });
    const row = getSourceHealthSnapshot().find((r) => r.name === 'rfs');
    expect(row?.state).toBe('down');
  });

  it('falls back to default thresholds for unknown source names', () => {
    const now = Math.floor(Date.now() / 1000);
    _seedSourceStateForTests('unconfigured', {
      lastSuccessTs: now - 100, // default soft=300
      lastError: null,
      failureCount: 0,
    });
    const row = getSourceHealthSnapshot().find((r) => r.name === 'unconfigured');
    expect(row?.state).toBe('ok');
    // Label defaults to the source name when no entry in SOURCE_THRESHOLDS.
    expect(row?.label).toBe('unconfigured');
    expect(row?.family).toBe('misc');
  });

  it('exposes per-source labels from the threshold table', () => {
    const row = getSourceHealthSnapshot().find((r) => r.name === 'rfs');
    expect(row?.label).toBe('RFS incidents');
  });
});

describe('clearSourceErrors', () => {
  it('resets every source when called with no arg', () => {
    const now = Math.floor(Date.now() / 1000);
    _seedSourceStateForTests('rfs', {
      lastSuccessTs: now - 60,
      lastError: 'broken',
      failureCount: 3,
    });
    _seedSourceStateForTests('waze', {
      lastSuccessTs: now - 60,
      lastError: 'broken',
      failureCount: 2,
    });
    clearSourceErrors();
    const rows = getSourceHealthSnapshot();
    for (const r of rows) {
      expect(r.consec_fails).toBe(0);
      expect(r.last_error).toBeNull();
    }
  });

  it('resets a single source when name supplied', () => {
    const now = Math.floor(Date.now() / 1000);
    _seedSourceStateForTests('rfs', {
      lastSuccessTs: now - 60,
      lastError: 'broken',
      failureCount: 3,
    });
    _seedSourceStateForTests('waze', {
      lastSuccessTs: now - 60,
      lastError: 'still broken',
      failureCount: 2,
    });
    clearSourceErrors('rfs');
    const rfs = getSourceHealthSnapshot().find((r) => r.name === 'rfs');
    const waze = getSourceHealthSnapshot().find((r) => r.name === 'waze');
    expect(rfs?.consec_fails).toBe(0);
    expect(rfs?.last_error).toBeNull();
    // Waze untouched.
    expect(waze?.consec_fails).toBe(2);
    expect(waze?.last_error).toBe('still broken');
  });

  it('preserves last_ok_at on clear (only resets failure state)', () => {
    const now = Math.floor(Date.now() / 1000);
    _seedSourceStateForTests('rfs', {
      lastSuccessTs: now - 60,
      lastError: 'broken',
      failureCount: 3,
    });
    clearSourceErrors();
    const rfs = getSourceHealthSnapshot().find((r) => r.name === 'rfs');
    expect(rfs?.last_ok_at).not.toBeNull();
  });
});
