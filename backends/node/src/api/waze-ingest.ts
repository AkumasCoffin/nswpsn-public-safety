/**
 * POST /api/waze/ingest — userscript pushes scraped Waze georss here.
 *
 * Replaces the Python `waze_ingest` endpoint. New behaviour vs Python:
 *   - Writes to LiveStore via WazeIngestCache (same in-memory pattern)
 *   - DOES NOT write to data_history. Waze archival was the source of
 *     today's contention; this backend's archive_waze table is also
 *     skipped for now per the migration plan (see W2 commit notes).
 *     If/when we want historical Waze, flip ARCHIVE_WAZE on and the
 *     ingest will queue archive rows here.
 *
 * Auth: X-Ingest-Key matched against WAZE_INGEST_KEY env var.
 */
import { Hono } from 'hono';
import { requireIngestKey } from '../services/auth/ingestKey.js';
import { WazeIngestPayloadSchema } from '../types/waze.js';
import { ingest } from '../store/wazeIngestCache.js';
import { log } from '../lib/log.js';

export const wazeIngestRouter = new Hono();

wazeIngestRouter.post('/api/waze/ingest', requireIngestKey, async (c) => {
  let body: unknown;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: 'bad json' }, 400);
  }
  const parsed = WazeIngestPayloadSchema.safeParse(body);
  if (!parsed.success) {
    return c.json(
      { error: 'bad payload', issues: parsed.error.issues },
      400,
    );
  }
  const result = ingest(parsed.data);
  log.debug(
    {
      bbox: result.bboxKey,
      alerts: result.alerts,
      jams: result.jams,
      regions: result.regions,
    },
    'waze ingest',
  );
  return c.json({
    ok: true,
    regions_cached: result.regions,
    received: { alerts: result.alerts, jams: result.jams },
  });
});
