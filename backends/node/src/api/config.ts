/**
 * GET /api/config — public, a liveness probe and a version string.
 *
 * It used to hand out NSWPSN_API_KEY, which is how every page authenticated
 * and also how anyone with a network tab got a key that worked from curl.
 * Pages now ask POST /api/session/token for a short-lived token bound to
 * their own browser instead (api/session.ts). The static key is for
 * server-side callers only — the Discord bot, rdio's transcripts plugin —
 * and never leaves .env.
 */
import { Hono } from 'hono';

export const configRouter = new Hono();

configRouter.get('/api/config', (c) =>
  c.json({
    version: '2.5-node',
    // Where a page gets its credential now. Advisory — the pages know.
    session: '/api/session/token',
  }),
);
