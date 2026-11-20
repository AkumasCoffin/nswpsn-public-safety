/**
 * Raw message logs from radio nodes: the timed capture window, the names a
 * file may be stored under, and the on-disk store with its size cap.
 */
import { describe, it, expect, vi, beforeEach, afterAll } from 'vitest';
import { mkdtempSync, rmSync, readdirSync, readFileSync, utimesSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

const stateDir = mkdtempSync(path.join(tmpdir(), 'rawlogs-'));

vi.mock('../../../src/config.js', () => ({
  config: { LOG_LEVEL: 'silent', NODE_ENV: 'test', STATE_DIR: stateDir },
}));

const {
  rawLoggingActive,
  rawLogUploadAllowed,
  rawLoggingUntilOf,
  safeRawLogName,
  rawLogDir,
  rawLogPath,
  saveRawLog,
  listRawLogs,
  pruneRawLogs,
  clearRawLogs,
  RAW_LOG_GRACE_MS,
} = await import('../../../src/services/nodes/rawLogs.js');
import type { NodeRow } from '../../../src/services/nodes/registry.js';

const NODE = '11111111-2222-3333-4444-555555555555';
const node = (override: Record<string, unknown> | null): NodeRow =>
  ({ id: NODE, kind: 'radio', config_override: override }) as unknown as NodeRow;

function body(text: string, chunks = 1): ReadableStream<Uint8Array> {
  const bytes = new TextEncoder().encode(text);
  const size = Math.ceil(bytes.length / chunks);
  return new ReadableStream({
    start(controller) {
      for (let i = 0; i < bytes.length; i += size) controller.enqueue(bytes.slice(i, i + size));
      controller.close();
    },
  });
}

beforeEach(async () => {
  await clearRawLogs(NODE);
});
afterAll(() => {
  rmSync(stateDir, { recursive: true, force: true });
});

describe('the capture window', () => {
  const now = 1_000_000;

  it('is off for a node that never had one', () => {
    expect(rawLoggingUntilOf(node(null))).toBeNull();
    expect(rawLoggingActive(node({}), now)).toBe(false);
    expect(rawLogUploadAllowed(node({}), now)).toBe(false);
  });

  it('is on until its end, and not a moment after', () => {
    expect(rawLoggingActive(node({ rawLoggingUntil: now + 1 }), now)).toBe(true);
    expect(rawLoggingActive(node({ rawLoggingUntil: now }), now)).toBe(false);
  });

  it('still takes uploads for a while after it ends', () => {
    const n = node({ rawLoggingUntil: now - 5 * 60_000 });
    expect(rawLoggingActive(n, now)).toBe(false);
    expect(rawLogUploadAllowed(n, now)).toBe(true);
    expect(rawLogUploadAllowed(node({ rawLoggingUntil: now - RAW_LOG_GRACE_MS - 1 }), now)).toBe(false);
  });

  it('ignores a stored value that is not a number', () => {
    expect(rawLoggingActive(node({ rawLoggingUntil: 'tomorrow' }), now)).toBe(false);
  });
});

describe('stored names', () => {
  it('keeps an ordinary log name and marks it gzipped', () => {
    expect(safeRawLogName('20261120_101500_T-NSWPSN_decoded_messages.log')).toBe(
      '20261120_101500_T-NSWPSN_decoded_messages.log.gz',
    );
    expect(safeRawLogName('a.log.gz')).toBe('a.log.gz');
  });

  it('never yields a path', () => {
    for (const bad of ['../../etc/passwd', '..\\..\\x.log', '/abs/x.log', '.hidden.log', 'a/b.log']) {
      const n = safeRawLogName(bad);
      expect(n === null || (!n.includes('/') && !n.includes('\\') && !n.startsWith('.'))).toBe(true);
    }
    expect(safeRawLogName('')).toBeNull();
    expect(safeRawLogName('...')).toBeNull();
  });

  it('only resolves a path for a name it would have produced', () => {
    expect(rawLogPath(NODE, 'x.log.gz')).toBe(path.join(rawLogDir(NODE)!, 'x.log.gz'));
    expect(rawLogPath(NODE, '../x.log.gz')).toBeNull();
    expect(rawLogPath(NODE, 'x.txt')).toBeNull();
    expect(rawLogPath('not a node id', 'x.log.gz')).toBeNull();
  });
});

describe('the store', () => {
  it('writes an upload under its name and lists it', async () => {
    const r = await saveRawLog(NODE, 'one.log', body('hello raw log', 3));
    expect(r).toEqual({ ok: true, name: 'one.log.gz', bytes: 13 });
    expect(readFileSync(rawLogPath(NODE, 'one.log.gz')!, 'utf8')).toBe('hello raw log');
    const files = await listRawLogs(NODE);
    expect(files.map((f) => [f.name, f.bytes])).toEqual([['one.log.gz', 13]]);
  });

  it('refuses an upload over the limit and leaves nothing behind', async () => {
    const r = await saveRawLog(NODE, 'big.log', body('x'.repeat(100), 4), 50);
    expect(r).toEqual({ ok: false, reason: 'too_large' });
    expect(readdirSync(rawLogDir(NODE)!)).toEqual([]);
  });

  it('refuses an empty body and a bad name', async () => {
    expect(await saveRawLog(NODE, 'empty.log', body(''))).toEqual({ ok: false, reason: 'failed' });
    expect(await saveRawLog(NODE, '', body('x'))).toEqual({ ok: false, reason: 'bad_name' });
    expect(await saveRawLog(NODE, 'x.log', null)).toEqual({ ok: false, reason: 'bad_name' });
  });

  it('drops the oldest files first when the node is over its cap', async () => {
    for (const [i, name] of ['a', 'b', 'c'].entries()) {
      await saveRawLog(NODE, `${name}.log`, body('0123456789'));
      const t = new Date(1_700_000_000_000 + i * 60_000);
      utimesSync(rawLogPath(NODE, `${name}.log.gz`)!, t, t);
    }
    expect(await pruneRawLogs(NODE, 25)).toBe(1);
    expect((await listRawLogs(NODE)).map((f) => f.name)).toEqual(['c.log.gz', 'b.log.gz']);
  });

  it('clears a node and reports how many files it had', async () => {
    await saveRawLog(NODE, 'a.log', body('x'));
    await saveRawLog(NODE, 'b.log', body('y'));
    expect(await clearRawLogs(NODE)).toBe(2);
    expect(await listRawLogs(NODE)).toEqual([]);
  });
});
