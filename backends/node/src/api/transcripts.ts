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
 *
 * Also served at /api/v1/ (server.ts mirrors it), wrapped in `data` there
 * (api/v1/respond.ts). The path keeps rdio-scanner's word, "call"; what it
 * serves is one reception.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute, resolver } from 'hono-openapi';
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
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  ErrorBody,
  GATED_ERRORS,
  NOT_FOUND_ERROR,
  okJson,
  UNAVAILABLE_ERROR,
  type SameShape,
} from './v1/schemas.js';

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

/** One reception, as this route answers it. */
interface ReceptionTranscript {
  id: number;
  datetime: string | null;
  system: number | null;
  system_label: string | null;
  talkgroup: number | null;
  talkgroup_label: string | null;
  transcript: string | null;
  radio_id: number | null;
  radio_label: string | null;
  call_url: string;
}

async function rowToShape(row: RdioCallRow): Promise<ReceptionTranscript> {
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

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_TRANSCRIPTS_HISTORY = scoped({ source: 'rdio', type: 'transcripts', window: 'history' });

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const errorJson = (description: string) => ({
  description,
  content: { 'application/json': { schema: resolver(ErrorBody) } },
});

const RadioReceptionTranscript = z
  .object({
    id: z.number().int().describe("The reception's id in AusAware's radio scanner"),
    datetime: z.string().nullable().describe('When the reception began, ISO 8601 in UTC, to the second'),
    system: z.number().int().nullable().describe("The radio system's id in AusAware's radio scanner"),
    system_label: z.string().nullable().describe("The radio system's name there; null when it has none"),
    talkgroup: z.number().int().nullable().describe("The talkgroup's id"),
    talkgroup_label: z.string().nullable().describe("The talkgroup's name in AusAware's radio scanner; null when it has none"),
    transcript: z
      .string()
      .nullable()
      .describe(
        'The machine transcript as stored: usually plain text, in some cases a JSON document with the text in `text`. An empty string when the audio was transcribed and held nothing usable; null when there is no transcript, or none yet. Speech recognition makes mistakes.',
      ),
    radio_id: z.number().int().nullable().describe("The transmitting radio's unit id, when the reception carries one"),
    radio_label: z
      .string()
      .nullable()
      .describe("AusAware's name for that unit, such as a brigade, station or callsign; null when it has none"),
    call_url: z.string().describe("A link to the reception in AusAware's radio scanner, where it can be played"),
  })
  .meta({ id: 'RadioReceptionTranscript' });
assertShape<SameShape<z.infer<typeof RadioReceptionTranscript>, ReceptionTranscript>>();

const RECEPTIONS_UNAVAILABLE = 'Radio receptions are not available on this server.';

transcriptsRouter.get(
  '/api/rdio/calls/:callId',
  SCOPE_TRANSCRIPTS_HISTORY,
  describeRoute({
    operationId: 'getReceptionTranscript',
    tags: ['Radio'],
    summary: 'One radio reception and its transcript',
    description:
      "One reception recorded by AusAware's own radio scanner, with its time, radio system, talkgroup, transmitting unit and its transcript, made by speech recognition.",
    parameters: [
      {
        in: 'path',
        name: 'callId',
        required: true,
        schema: { type: 'integer', minimum: 0 },
        description: "The reception's id: digits only (as in the receptions a summary lists, member_call_ids)",
      },
    ],
    responses: {
      ...okJson(envelope(RadioReceptionTranscript), 'The reception'),
      ...GATED_ERRORS,
      400: errorJson('callId is not a whole number (validation_failed)'),
      ...NOT_FOUND_ERROR,
      ...UNAVAILABLE_ERROR,
      500: errorJson('The reception could not be read (internal_error)'),
    },
  }),
  async (c) => {
    if (!isRdioConfigured()) {
      return fail(c, 503, 'service_unavailable', RECEPTIONS_UNAVAILABLE, { error: 'RDIO_DATABASE_URL not configured' });
    }
    const callIdRaw = c.req.param('callId');
    if (!/^\d+$/.test(callIdRaw)) {
      return fail(
        c,
        400,
        'validation_failed',
        'callId: must be a whole number',
        { error: 'callId must be numeric' },
        { issues: [{ path: 'callId', message: 'must be a whole number' }] },
      );
    }
    const callId = Number.parseInt(callIdRaw, 10);
    try {
      const pool = await getRdioPool();
      if (!pool) {
        return fail(c, 503, 'service_unavailable', RECEPTIONS_UNAVAILABLE, { error: 'RDIO_DATABASE_URL not configured' });
      }
      const res = await pool.query<RdioCallRow>(
        'SELECT "id", "dateTime" AS date_time, "system", "talkgroup", ' +
          `${RDIO_TRANSCRIPT} AS transcript, "source", "sources" FROM ${RDIO_CALLS_FROM} ` +
          'WHERE "id" = $1',
        [callId],
      );
      const row = res.rows[0];
      if (!row) {
        return fail(c, 404, 'not_found', 'No reception with that id.', { error: 'call not found' });
      }
      return ok(c, await rowToShape(row));
    } catch (err) {
      log.error({ err, callId }, '/api/rdio/calls/:id error');
      return fail(c, 500, 'internal_error', 'The reception could not be read.', { error: 'failed to load transcripts' });
    }
  },
);
