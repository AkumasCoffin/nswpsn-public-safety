/**
 * /api/rdio/transcripts/search and /api/rdio/calls/:id.
 *
 * These read from the SELF-HOSTED rdio-scanner Postgres (RDIO_DATABASE_URL),
 * not our archive DB. Porting them faithfully requires:
 *   - a second pg.Pool keyed off RDIO_DATABASE_URL
 *   - a label cache (system + talkgroup -> friendly names)
 *   - timezone-aware day-bound resolution (SUMMARY_TZ)
 *   - call-URL composition for the rdio-scanner web UI
 *
 * For now both routes return 503 with a clear "not yet ported" note so
 * Apache can pin them to python during the strangler-fig migration. The
 * full port lands alongside the rdio summary generation loop in W8.
 */
import { Hono } from 'hono';

const NOT_PORTED = {
  error: 'rdio transcripts/search not yet ported to node backend',
  message:
    'This endpoint reads from the rdio-scanner Postgres (RDIO_DATABASE_URL) ' +
    'with a label cache + timezone-aware filters. Route to the python service ' +
    'via Apache until W8 lands the rdio integration.',
};

export const transcriptsRouter = new Hono();

transcriptsRouter.get('/api/rdio/transcripts/search', (c) =>
  c.json(NOT_PORTED, 503),
);

transcriptsRouter.get('/api/rdio/calls/:callId', (c) => c.json(NOT_PORTED, 503));
