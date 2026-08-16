/**
 * /proc parsing for the memory-attribution log.
 *
 * Worth testing despite being small: this runs only on the production box, so
 * a parsing slip wouldn't throw — it would quietly report nothing, and the
 * memory question it exists to answer would stay open through another OOM.
 * The fixtures below are real /proc formats.
 */
import { describe, it, expect } from 'vitest';
import { parseStatus, parseMeminfo } from '../../../src/services/procMemory.js';

const NODE_STATUS = `Name:\tnode
Umask:\t0022
State:\tS (sleeping)
Tgid:\t402851
Ngid:\t0
Pid:\t402851
PPid:\t1
VmPeak:\t 4812345 kB
VmSize:\t 4712345 kB
VmLck:\t       0 kB
VmPin:\t       0 kB
VmHWM:\t 2103456 kB
VmRSS:\t 2098765 kB
RssAnon:\t 2000000 kB
Threads:\t11
`;

const CHROME_STATUS = `Name:\tchrome
Umask:\t0022
State:\tS (sleeping)
Pid:\t403001
VmRSS:\t  512000 kB
Threads:\t27
`;

// Kernel threads have no VmRSS at all.
const KTHREAD_STATUS = `Name:\tkworker/0:1
Umask:\t0000
State:\tI (idle)
Pid:\t42
Threads:\t1
`;

describe('parseStatus', () => {
  it('pulls the name and resident size out of a real status blob', () => {
    expect(parseStatus(NODE_STATUS)).toEqual({ name: 'node', rssKB: 2098765 });
    expect(parseStatus(CHROME_STATUS)).toEqual({ name: 'chrome', rssKB: 512000 });
  });

  it('does not confuse VmRSS with the other Vm* lines above it', () => {
    // VmPeak/VmSize/VmHWM all precede VmRSS and are all larger — picking the
    // wrong one would silently inflate every reading.
    expect(parseStatus(NODE_STATUS)?.rssKB).toBe(2098765);
  });

  it('skips kernel threads, which have no VmRSS', () => {
    expect(parseStatus(KTHREAD_STATUS)).toBeNull();
  });

  it('returns null for junk rather than throwing', () => {
    expect(parseStatus('')).toBeNull();
    expect(parseStatus('nonsense')).toBeNull();
    expect(parseStatus('VmRSS:\t 100 kB')).toBeNull(); // no Name
  });
});

describe('parseMeminfo', () => {
  it('reads MemTotal and MemAvailable in MB', () => {
    const text = `MemTotal:       10240000 kB
MemFree:          512000 kB
MemAvailable:    2048000 kB
Buffers:          100000 kB
`;
    expect(parseMeminfo(text)).toEqual({ totalMB: 10000, availableMB: 2000 });
  });

  it('does not mistake MemFree for MemAvailable', () => {
    const text = `MemTotal:       10240000 kB
MemFree:          512000 kB
`;
    expect(parseMeminfo(text)).toEqual({ totalMB: 10000, availableMB: null });
  });

  it('tolerates a missing or unreadable meminfo', () => {
    expect(parseMeminfo('')).toEqual({ totalMB: null, availableMB: null });
  });
});
