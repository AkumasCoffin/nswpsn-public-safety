/**
 * The request log's client tag: who is actually calling.
 *
 * The old classifier knew three answers — bot, browser, other — from before
 * the feeder nodes, rdio's pushes and the whisper traffic existed, so by the
 * time all of those were live, most of the log read [other] and the tag
 * answered nothing. These pin the identity rules for each caller, exercised
 * through the real middleware rather than a copied regex.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn(), trace: vi.fn() },
}));

// The tag test needs uncoloured output; NODE_ENV is not 'production' under
// vitest, so strip ANSI rather than fighting the colour gate.
const strip = (v: string) => v.replace(/\x1b\[[0-9;]*m/g, '');

import { createApp } from '../../src/server.js';
import { log } from '../../src/lib/log.js';

/** The last request line the logger emitted, colour removed. */
function lastLine(): string {
  const calls = (log.info as ReturnType<typeof vi.fn>).mock.calls;
  for (let i = calls.length - 1; i >= 0; i--) {
    const m = calls[i]![0];
    if (typeof m === 'string' && m.includes('→')) return strip(m);
  }
  return '';
}

async function request(path: string, headers: Record<string, string> = {}) {
  const app = createApp();
  // A guaranteed-slow-free 404 is fine: 4xx logs at info unconditionally,
  // which makes every classification observable without stubbing routes.
  await app.request(`http://x${path}`, { headers });
  return lastLine();
}

beforeEach(() => {
  (log.info as ReturnType<typeof vi.fn>).mockClear();
});

describe('client classification in the request log', () => {
  it('feeder agents are [node] by their UA, radio and pager alike', async () => {
    // Both Go agents send NSWPSN-NodeAgent/<ver> on every request.
    expect(await request('/api/nope', { 'User-Agent': 'NSWPSN-NodeAgent/0.2.25 (linux; amd64)' }))
      .toContain('[node]');
  });

  it('rdio is identified by PATH, because its sender cannot set headers', async () => {
    // The same limitation that makes scanner-ingest auth by form field.
    expect(await request('/api/scanner-ingest/api/nope', { 'User-Agent': 'Go-http-client/1.1' }))
      .toContain('[rdio]');
  });

  it('transcription traffic is [whisper], also by path', async () => {
    expect(await request('/api/whisper/v1/nope', { 'User-Agent': 'Go-http-client/1.1' }))
      .toContain('[whisper]');
  });

  it('the PC watcher is [whisper-node] via its header, not its browser-shaped UA', async () => {
    // Invoke-RestMethod's UA starts with Mozilla/ like everything on Windows,
    // which had the watcher's 5s status polls masquerading as a person.
    expect(await request('/api/whisper/status', {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0) WindowsPowerShell/5.1',
      'X-Client-Type': 'whisper-node',
    })).toContain('[whisper-node]');
  });

  it('the discord bot keeps both of its spellings', async () => {
    expect(await request('/api/nope', { 'User-Agent': 'AusAwareBot/1.0' })).toContain('[discord-bot]');
    expect(await request('/api/nope', { 'User-Agent': 'curl/8', 'X-Client-Type': 'discord-bot' }))
      .toContain('[discord-bot]');
  });

  it('a plain browser is still a browser, and the unknown stays [other]', async () => {
    expect(await request('/api/nope', { 'User-Agent': 'Mozilla/5.0 Chrome/120' })).toContain('[browser]');
    expect(await request('/api/nope', { 'User-Agent': 'curl/8.4.0' })).toContain('[other]');
  });
});
