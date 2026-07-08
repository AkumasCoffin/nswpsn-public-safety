/**
 * Multi-key ingest auth: WAZE_INGEST_KEY (single) + WAZE_INGEST_KEYS (list)
 * are both accepted, so each feeder can run the userscript with its own key.
 */
import { describe, it, expect, afterEach } from 'vitest';
import { Hono } from 'hono';
import {
  configuredIngestKeys,
  ingestKeysConfigured,
  requireIngestKey,
} from '../../../src/services/auth/ingestKey.js';
import { config } from '../../../src/config.js';

type MutableConfig = { WAZE_INGEST_KEY?: string; WAZE_INGEST_KEYS?: string };
const c = config as unknown as MutableConfig;
const origSingle = c.WAZE_INGEST_KEY;
const origList = c.WAZE_INGEST_KEYS;

afterEach(() => {
  c.WAZE_INGEST_KEY = origSingle;
  c.WAZE_INGEST_KEYS = origList;
});

function appWith() {
  const app = new Hono();
  app.post('/ingest', requireIngestKey, (ctx) => ctx.json({ ok: true }));
  return app;
}

async function post(app: ReturnType<typeof appWith>, key?: string) {
  return app.request('/ingest', {
    method: 'POST',
    headers: key ? { 'X-Ingest-Key': key } : {},
  });
}

describe('configuredIngestKeys', () => {
  it('merges the single key and the list, split on commas/whitespace, de-duped', () => {
    c.WAZE_INGEST_KEY = 'solo';
    c.WAZE_INGEST_KEYS = 'alice, bob\tcarol\nsolo';
    expect(configuredIngestKeys().sort()).toEqual(['alice', 'bob', 'carol', 'solo']);
  });

  it('is empty when nothing is configured', () => {
    c.WAZE_INGEST_KEY = undefined;
    c.WAZE_INGEST_KEYS = undefined;
    expect(configuredIngestKeys()).toEqual([]);
    expect(ingestKeysConfigured()).toBe(false);
  });

  it('works with only the list set (no single key)', () => {
    c.WAZE_INGEST_KEY = undefined;
    c.WAZE_INGEST_KEYS = 'k1,k2';
    expect(configuredIngestKeys()).toEqual(['k1', 'k2']);
    expect(ingestKeysConfigured()).toBe(true);
  });
});

describe('requireIngestKey middleware', () => {
  it('403s when no keys are configured', async () => {
    c.WAZE_INGEST_KEY = undefined;
    c.WAZE_INGEST_KEYS = undefined;
    const res = await post(appWith(), 'anything');
    expect(res.status).toBe(403);
  });

  it('accepts any key in the configured set', async () => {
    c.WAZE_INGEST_KEY = 'primary';
    c.WAZE_INGEST_KEYS = 'alice,bob';
    for (const k of ['primary', 'alice', 'bob']) {
      const res = await post(appWith(), k);
      expect(res.status, `key ${k}`).toBe(200);
    }
  });

  it('401s an unknown key', async () => {
    c.WAZE_INGEST_KEY = 'primary';
    c.WAZE_INGEST_KEYS = 'alice,bob';
    expect((await post(appWith(), 'mallory')).status).toBe(401);
    expect((await post(appWith())).status).toBe(401); // missing header
  });
});
