/**
 * Raw message logs from radio nodes.
 *
 * Staff can switch a radio node into a timed capture. For that long the node's
 * config tells SDR-Trunk to write its decoded-message logs (every message it
 * receives on the control and traffic channels, data channels included, as
 * text with the raw hex). The agent gzips each finished log file and POSTs it
 * here; the files are kept on disk under STATE_DIR/raw-logs/<nodeId>/ for
 * staff to download and read. Nothing is parsed and nothing goes in the
 * database: this is a diagnostic capture, not a feed.
 *
 * The capture is always timed. SDR-Trunk logs every voice frame too, so a node
 * left logging would fill its own disk and then this one.
 */
import { createWriteStream } from 'node:fs';
import { mkdir, readdir, rename, rm, stat } from 'node:fs/promises';
import path from 'node:path';
import { config } from '../../config.js';
import type { NodeRow } from './registry.js';

/** Longest capture staff can ask for, and the default. */
export const RAW_LOG_MAX_MINUTES = 240;
export const RAW_LOG_DEFAULT_MINUTES = 60;
/** Files finished just before the capture ended are still on their way. */
export const RAW_LOG_GRACE_MS = 60 * 60 * 1000;
/** One uploaded (gzipped) file. */
export const RAW_LOG_MAX_FILE_BYTES = 64 * 1024 * 1024;
/** Everything kept for one node; the oldest files go first. */
export const RAW_LOG_MAX_NODE_BYTES = 2 * 1024 * 1024 * 1024;

/** When the node's capture ends (epoch ms), or null if none was ever set. */
export function rawLoggingUntilOf(node: NodeRow | null | undefined): number | null {
  const co = (node?.config_override ?? {}) as Record<string, unknown>;
  const u = co['rawLoggingUntil'];
  return typeof u === 'number' && Number.isFinite(u) ? u : null;
}

/** Whether the node should be logging right now. */
export function rawLoggingActive(node: NodeRow | null | undefined, now = Date.now()): boolean {
  const until = rawLoggingUntilOf(node);
  return until !== null && until > now;
}

/** Whether an upload from the node is still expected: capturing, or just finished. */
export function rawLogUploadAllowed(node: NodeRow | null | undefined, now = Date.now()): boolean {
  const until = rawLoggingUntilOf(node);
  return until !== null && until + RAW_LOG_GRACE_MS > now;
}

function rootDir(): string {
  return path.resolve(config.STATE_DIR, 'raw-logs');
}

/** The directory holding one node's files. Node ids are UUIDs; anything else is refused. */
export function rawLogDir(nodeId: string): string | null {
  if (!/^[0-9a-fA-F-]{8,64}$/.test(nodeId)) return null;
  return path.join(rootDir(), nodeId);
}

/**
 * The name a file is stored under: the agent's name with anything but plain
 * filename characters replaced, always ending .log.gz. Null when nothing
 * usable is left. The result never contains a path separator or a leading dot.
 */
export function safeRawLogName(name: string): string | null {
  let n = name.replace(/[^A-Za-z0-9._-]+/g, '_').replace(/^[._]+/, '').slice(0, 160);
  n = n.replace(/\.gz$/i, '').replace(/\.log$/i, '');
  if (!n) return null;
  return `${n}.log.gz`;
}

export interface RawLogFile {
  name: string;
  bytes: number;
  /** Epoch ms the upload finished. */
  at: number;
}

/** The node's stored files, newest first. Empty when it has none. */
export async function listRawLogs(nodeId: string): Promise<RawLogFile[]> {
  const dir = rawLogDir(nodeId);
  if (!dir) return [];
  let names: string[];
  try {
    names = await readdir(dir);
  } catch {
    return [];
  }
  const out: RawLogFile[] = [];
  for (const name of names) {
    if (!name.endsWith('.log.gz')) continue;
    try {
      const st = await stat(path.join(dir, name));
      if (st.isFile()) out.push({ name, bytes: st.size, at: Math.round(st.mtimeMs) });
    } catch {
      // Removed between the listing and the stat.
    }
  }
  out.sort((a, b) => b.at - a.at || a.name.localeCompare(b.name));
  return out;
}

/** Count and total size of the node's stored files. */
export async function rawLogTotals(nodeId: string): Promise<{ files: number; bytes: number }> {
  const files = await listRawLogs(nodeId);
  return { files: files.length, bytes: files.reduce((n, f) => n + f.bytes, 0) };
}

/** The path of one stored file, or null when the name is not one of ours. */
export function rawLogPath(nodeId: string, name: string): string | null {
  const dir = rawLogDir(nodeId);
  if (!dir || safeRawLogName(name) !== name) return null;
  return path.join(dir, name);
}

/** Delete the node's oldest files until what is left fits under `maxBytes`. */
export async function pruneRawLogs(nodeId: string, maxBytes = RAW_LOG_MAX_NODE_BYTES): Promise<number> {
  const dir = rawLogDir(nodeId);
  if (!dir) return 0;
  const files = await listRawLogs(nodeId);
  let total = files.reduce((n, f) => n + f.bytes, 0);
  let removed = 0;
  for (let i = files.length - 1; i >= 0 && total > maxBytes; i -= 1) {
    const f = files[i]!;
    await rm(path.join(dir, f.name), { force: true });
    total -= f.bytes;
    removed += 1;
  }
  return removed;
}

/** Delete every stored file for the node. Returns how many there were. */
export async function clearRawLogs(nodeId: string): Promise<number> {
  const dir = rawLogDir(nodeId);
  if (!dir) return 0;
  const files = await listRawLogs(nodeId);
  await rm(dir, { recursive: true, force: true });
  return files.length;
}

export type SaveResult =
  | { ok: true; name: string; bytes: number }
  | { ok: false; reason: 'bad_name' | 'too_large' | 'failed' };

/**
 * Write an uploaded body to the node's directory. The body is streamed to a
 * temporary file and renamed into place when complete, so a broken upload
 * never leaves a half file under a real name. More than `maxBytes` aborts.
 */
export async function saveRawLog(
  nodeId: string,
  name: string,
  body: ReadableStream<Uint8Array> | null,
  maxBytes = RAW_LOG_MAX_FILE_BYTES,
): Promise<SaveResult> {
  const dir = rawLogDir(nodeId);
  const safe = safeRawLogName(name);
  if (!dir || !safe || !body) return { ok: false, reason: 'bad_name' };
  await mkdir(dir, { recursive: true });
  const tmp = path.join(dir, `.${safe}.${process.pid}.${Date.now()}.part`);
  const out = createWriteStream(tmp);
  let bytes = 0;
  let tooLarge = false;
  try {
    const reader = body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > maxBytes) {
        tooLarge = true;
        await reader.cancel().catch(() => {});
        break;
      }
      if (!out.write(value)) await new Promise<void>((resolve) => out.once('drain', resolve));
    }
    await new Promise<void>((resolve, reject) => {
      out.once('error', reject);
      out.end(resolve);
    });
    if (tooLarge || bytes === 0) {
      await rm(tmp, { force: true });
      return { ok: false, reason: tooLarge ? 'too_large' : 'failed' };
    }
    await rename(tmp, path.join(dir, safe));
    await pruneRawLogs(nodeId);
    return { ok: true, name: safe, bytes };
  } catch {
    out.destroy();
    await rm(tmp, { force: true }).catch(() => {});
    return { ok: false, reason: 'failed' };
  }
}
