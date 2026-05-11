/**
 * GET /api/config — public, returns the API key the frontend embeds.
 *
 * Bug-for-bug compatible with Python (external_api_proxy.py:10994-11005).
 * The endpoint exists because the API key is needed by every page-load
 * fetch and we don't want it baked into HTML/git. Yes, it's still
 * visible in the network tab — Python's docstring is honest about that
 * being intentional.
 */
import { Hono } from 'hono';
import { config } from '../config.js';

export const configRouter = new Hono();

configRouter.get('/api/config', (c) =>
  c.json({
    apiKey: config.NSWPSN_API_KEY,
    // Bumped from Python's '2.0' to '2.4-node' so a fixture-diff between
    // the two backends shows the source of the response. Once Python
    // is fully retired, drop the suffix back to a clean version string.
    version: '2.4-node',
  }),
);
