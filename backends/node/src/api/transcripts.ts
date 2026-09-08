/**
 * /api/rdio/calls/:id — one call by id from the SELF-HOSTED rdio-scanner
 * Postgres (RDIO_DATABASE_URL), joined with the system + talkgroup label
 * cache and the radio-unit label dictionary.
 *
 * /api/rdio/transcripts/search used to live here too. It was REMOVED
 * (2026-09): its browse mode ran an unbounded COUNT(*) + leading-wildcard
 * ILIKE over the whole calls⨝transcripts join every 30 s per open staff
 * tab, which blew the rdio pool's 30 s statement_timeout (500s) and
 * starved its 5 connections (hangs). The staff Transcripts view is now a
 * whisper throughput dashboard (api/whisper.ts /history) that never
 * touches the rdio DB, and the bot's /ts command was retired with it.
 */
import { Hono } from 'hono';
import {
  isRdioConfigured,
  getRdioPool,
  resolveLabels,
  getUnitLabel,
  ensureUnitLabelsLoaded,
  RDIO_CALLS_FROM,
  RDIO_TRANSCRIPT,
} from '../services/rdio.js';
import { config } from '../config.js';
import { log } from '../lib/log.js';

interface RdioCallRow {
  id: number;
  date_time: Date | null;
  system: number | null;
  talkgroup: number | null;
  transcript: string | null;
  source: number | string | null;
  sources: unknown;
}

function extractRadioId(row: RdioCallRow): number | null {
  // Mirror of python's _extract_radio_id at line 14634.
  const src = row.source;
  if (src !== null && src !== undefined && src !== '') {
    const n = Number(src);
    if (Number.isFinite(n) && n > 0) return n;
  }
  let raw = row.sources;
  if (typeof raw === 'string') {
    try {
      raw = JSON.parse(raw);
    } catch {
      return null;
    }
  }
  if (Array.isArray(raw)) {
    for (const item of raw) {
      if (item && typeof item === 'object') {
        const obj = item as Record<string, unknown>;
        const sid = obj['src'] ?? obj['source'];
        if (sid !== undefined && sid !== null && sid !== '') {
          const n = Number(sid);
          if (Number.isFinite(n) && n > 0) return n;
        }
      }
    }
  }
  return null;
}

async function rowToShape(row: RdioCallRow): Promise<Record<string, unknown>> {
  await ensureUnitLabelsLoaded();
  const { systemLabel, talkgroupLabel } = await resolveLabels(
    row.system,
    row.talkgroup,
  );
  const rid = extractRadioId(row);
  const dt = row.date_time
    ? row.date_time.toISOString().replace(/\.\d+Z$/, 'Z')
    : null;
  return {
    id: row.id,
    datetime: dt,
    system: row.system,
    system_label: systemLabel,
    talkgroup: row.talkgroup,
    talkgroup_label: talkgroupLabel,
    transcript: row.transcript,
    radio_id: rid,
    radio_label: rid !== null ? getUnitLabel(rid) : null,
    call_url: `${config.RDIO_CALL_URL_BASE}${row.id}`,
  };
}

export const transcriptsRouter = new Hono();

transcriptsRouter.get('/api/rdio/calls/:callId', async (c) => {
  if (!isRdioConfigured()) {
    return c.json({ error: 'RDIO_DATABASE_URL not configured' }, 503);
  }
  const callIdRaw = c.req.param('callId');
  if (!/^\d+$/.test(callIdRaw)) {
    return c.json({ error: 'callId must be numeric' }, 400);
  }
  const callId = Number.parseInt(callIdRaw, 10);
  try {
    const pool = await getRdioPool();
    if (!pool) {
      return c.json({ error: 'RDIO_DATABASE_URL not configured' }, 503);
    }
    const res = await pool.query<RdioCallRow>(
      'SELECT "id", "dateTime" AS date_time, "system", "talkgroup", ' +
        `${RDIO_TRANSCRIPT} AS transcript, "source", "sources" FROM ${RDIO_CALLS_FROM} ` +
        'WHERE "id" = $1',
      [callId],
    );
    const row = res.rows[0];
    if (!row) {
      return c.json({ error: 'call not found' }, 404);
    }
    return c.json(await rowToShape(row));
  } catch (err) {
    log.error({ err, callId }, '/api/rdio/calls/:id error');
    return c.json({ error: 'failed to load transcripts' }, 500);
  }
});
