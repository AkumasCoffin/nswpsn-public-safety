/**
 * Worker-thread entry for one day's globe-history import (see
 * adsbGlobeHistoryImport.ts). Posts one message, the result or the error, and
 * exits. Has its own database pool, closed on the way out.
 */
import { parentPort, workerData } from 'node:worker_threads';
import { closePool } from '../db/pool.js';
import { importGlobeHistoryDay } from './adsbGlobeHistoryImport.js';

const { dayStartMs, assetUrls } = workerData as { dayStartMs: number; assetUrls: string[] };
try {
  const result = await importGlobeHistoryDay(dayStartMs, assetUrls);
  parentPort?.postMessage({ ok: true, result });
} catch (err) {
  parentPort?.postMessage({ ok: false, error: (err as Error).message });
} finally {
  await closePool().catch(() => undefined);
}
