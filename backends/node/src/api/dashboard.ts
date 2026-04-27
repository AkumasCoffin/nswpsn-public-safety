/**
 * Discord OAuth dashboard endpoints.
 *
 * Mirrors python external_api_proxy.py:16797-19046 (~30 routes spanning
 * Discord OAuth login/callback/logout, /api/dashboard/me, guild
 * channels/roles/presets/preset-stats, mute-state, admin overview/
 * broadcast/cleanup/bot-actions/sources).
 *
 * Porting them faithfully requires:
 *   - Discord OAuth2 (login redirect, token exchange, refresh tokens)
 *   - JWE/JWT-style session cookie issuance
 *   - Discord API client with the exact retry / rate-limit semantics
 *     python uses (~5 distinct endpoints with bespoke error handling)
 *   - Lots of CRUD on the dashboard_sessions / dashboard_presets /
 *     dashboard_mute_state / dashboard_bot_actions tables
 *
 * That is genuinely a week of work and it touches infrastructure
 * (cookie domain, OAuth callback URL) that's already proven on python.
 * Strangler-fig says: leave it on python. Apache routes /api/dashboard/*
 * to the python service indefinitely.
 *
 * We mount a single catch-all 503 route here so anyone who accidentally
 * lands on the Node backend gets a clear pointer rather than a 404 +
 * "API key required" wall.
 */
import { Hono } from 'hono';

export const dashboardRouter = new Hono();

const NOT_PORTED = {
  error: 'dashboard endpoints not yet ported to node backend',
  message:
    'Discord OAuth + guild preset + admin endpoints under /api/dashboard/* ' +
    'remain on the python service. Apache routes this prefix to python; ' +
    'no migration is planned for W8.',
};

dashboardRouter.all('/api/dashboard/*', (c) => c.json(NOT_PORTED, 503));
