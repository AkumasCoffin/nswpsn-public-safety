/**
 * Whole-box memory attribution, read from /proc.
 *
 * Context: the VM went from ~6GB to ~10GB and ran out, while this process's
 * JS heap is capped by V8 at ~2GB. Those two facts can't both be explained by
 * the Node heap, so the question "which process is holding the other 2GB+?"
 * has to be answered before anything can be fixed. Node's own
 * process.memoryUsage() cannot see the Playwright Chromium processes, which
 * are separate OS processes and a prime suspect: two browsers are launched at
 * boot and kept alive for the life of the service.
 *
 * Rather than ask someone to catch it in the act with ps, this reads
 * /proc/<pid>/status directly and groups resident memory by process name. At a
 * 5-minute cadence a few hundred small reads is nothing, and it turns an
 * open question into a line in the log.
 *
 * Linux only by design — /proc is where the answer lives, and the production
 * box is Linux. Everywhere else this returns null and the caller skips it.
 */
import { readdir, readFile } from 'node:fs/promises';
import { log } from '../lib/log.js';

export interface ProcGroup {
  name: string;
  rssMB: number;
  count: number;
}

export interface ProcMemorySummary {
  /**
   * Sum of every process's RSS. Deliberately NOT called "total used" — RSS
   * counts shared pages once per process, so Postgres backends each count the
   * whole shared_buffers segment and Chromium's helpers each count their
   * shared mappings. This number is therefore an OVERESTIMATE of real usage
   * and is only useful for comparing one sample against the next. For actual
   * box usage read MemAvailable from /proc/meminfo.
   */
  sumRssMB: number;
  /** Largest groups first. */
  groups: ProcGroup[];
  /** This process specifically, so Node can be told apart from its children. */
  selfRssMB: number;
  /** Real available memory from /proc/meminfo, which has no double-counting. */
  memAvailableMB: number | null;
  memTotalMB: number | null;
}

/** Parse MemTotal/MemAvailable (kB) out of /proc/meminfo. */
export function parseMeminfo(text: string): { totalMB: number | null; availableMB: number | null } {
  let totalMB: number | null = null;
  let availableMB: number | null = null;
  for (const line of text.split('\n')) {
    const m = /^(MemTotal|MemAvailable):\s+(\d+) kB/.exec(line);
    if (!m) continue;
    const v = Math.round(Number(m[2]) / 1024);
    if (m[1] === 'MemTotal') totalMB = v;
    else availableMB = v;
    if (totalMB !== null && availableMB !== null) break;
  }
  return { totalMB, availableMB };
}

/** Parse VmRSS (kB) and Name out of a /proc/<pid>/status blob. Exported for tests. */
export function parseStatus(text: string): { name: string; rssKB: number } | null {
  let name = '';
  let rssKB = -1;
  for (const line of text.split('\n')) {
    if (line.startsWith('Name:')) name = line.slice(5).trim();
    else if (line.startsWith('VmRSS:')) {
      rssKB = parseInt(line.slice(6).trim(), 10);
      // Name always precedes VmRSS in /proc status, so once we have both
      // there's nothing left to read.
      if (name) break;
    }
  }
  if (!name || !Number.isFinite(rssKB) || rssKB < 0) return null;
  return { name, rssKB };
}

/**
 * Resident memory per process name across the whole box. Returns null on
 * non-Linux or if /proc can't be read — never throws, since this only exists
 * to explain a memory problem and must not become one.
 */
export async function procMemorySummary(topN = 6): Promise<ProcMemorySummary | null> {
  if (process.platform !== 'linux') return null;
  try {
    const entries = await readdir('/proc');
    const pids = entries.filter((e) => /^\d+$/.test(e));
    const byName = new Map<string, { rssKB: number; count: number }>();
    let totalKB = 0;
    let selfKB = 0;
    const selfPid = String(process.pid);

    // Sequential-ish but batched: these are tiny virtual files, and a process
    // exiting mid-scan just yields ENOENT, which is expected, not an error.
    await Promise.all(
      pids.map(async (pid) => {
        try {
          const text = await readFile(`/proc/${pid}/status`, 'utf8');
          const parsed = parseStatus(text);
          if (!parsed) return;
          totalKB += parsed.rssKB;
          if (pid === selfPid) selfKB = parsed.rssKB;
          const cur = byName.get(parsed.name) ?? { rssKB: 0, count: 0 };
          cur.rssKB += parsed.rssKB;
          cur.count += 1;
          byName.set(parsed.name, cur);
        } catch {
          /* process vanished, or no permission — skip it */
        }
      }),
    );

    const groups = [...byName.entries()]
      .map(([name, v]) => ({ name, rssMB: Math.round(v.rssKB / 1024), count: v.count }))
      .sort((a, b) => b.rssMB - a.rssMB)
      .slice(0, topN);

    // The honest number, free of RSS double-counting.
    let memAvailableMB: number | null = null;
    let memTotalMB: number | null = null;
    try {
      const info = parseMeminfo(await readFile('/proc/meminfo', 'utf8'));
      memAvailableMB = info.availableMB;
      memTotalMB = info.totalMB;
    } catch {
      /* not fatal — the per-process breakdown is still useful */
    }

    return {
      sumRssMB: Math.round(totalKB / 1024),
      selfRssMB: Math.round(selfKB / 1024),
      groups,
      memAvailableMB,
      memTotalMB,
    };
  } catch (err) {
    log.debug({ err }, 'procMemory: scan failed');
    return null;
  }
}
