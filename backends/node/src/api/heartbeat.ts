/**
 * GET/POST /api/heartbeat — page-active tracker.
 *
 * Mirrors Python's handler at external_api_proxy.py:6348-6464. The
 * frontend calls this on page load (`?action=open`), every ~30s as a
 * keepalive (`?action=ping`), and on unload (`?action=close`).
 *
 * Query parameters (all optional except page_id for accurate tracking):
 *   - action     : 'open' | 'ping' | 'close' (default 'ping')
 *   - page_id    : opaque id the frontend generates per browser tab
 *   - page_type  : human label (e.g. 'live', 'map')
 *   - data_page  : 'true'/'1'/'yes' if the page fetches live data; this
 *                  is what flips the poller into active mode
 *
 * Both GET and POST are accepted; the body is unused. Response matches
 * Python's JSON shape so the frontend's existing parser keeps working
 * during cutover.
 */
import { Hono, type Context } from 'hono';
import {
  recordHeartbeat,
  type HeartbeatAction,
} from '../services/activityMode.js';

export const heartbeatRouter = new Hono();

const ACTIVE_INTERVAL_SECS = 60;
const IDLE_INTERVAL_SECS = 300;
// `data_retention_days` is reported back by Python; we don't enforce it
// from the Node side (archive retention is owned by the DB partition
// drop policy) but echo the same default so the frontend's display
// logic doesn't break.
const DATA_RETENTION_DAYS = 7;

function parseAction(raw: string | null): HeartbeatAction {
  const a = (raw ?? 'ping').toLowerCase();
  if (a === 'open' || a === 'close' || a === 'ping') return a;
  return 'ping';
}

function parseDataPage(raw: string | null): boolean {
  const v = (raw ?? 'false').toLowerCase();
  return v === 'true' || v === '1' || v === 'yes';
}

function handle(c: Context): Response {
  const url = new URL(c.req.url);
  const action = parseAction(url.searchParams.get('action'));
  const pageId = url.searchParams.get('page_id') ?? '';
  const pageType = url.searchParams.get('page_type') ?? 'unknown';
  const isDataPage = parseDataPage(url.searchParams.get('data_page'));
  const ip = c.req.header('x-forwarded-for') ?? '';

  const result = recordHeartbeat(pageId, action, {
    pageType,
    isDataPage,
    ip,
  });

  const interval = result.active ? ACTIVE_INTERVAL_SECS : IDLE_INTERVAL_SECS;
  return c.json({
    status: 'ok',
    mode: result.active ? 'active' : 'idle',
    interval,
    total_viewers: result.totalViewers,
    data_viewers: result.dataViewers,
    page_id: pageId,
    page_type: pageType,
    is_data_page: isDataPage,
    next_collection_seconds: interval,
    data_retention_days: DATA_RETENTION_DAYS,
  });
}

heartbeatRouter.get('/api/heartbeat', handle);
heartbeatRouter.post('/api/heartbeat', handle);
