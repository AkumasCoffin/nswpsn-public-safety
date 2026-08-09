/**
 * Agency reference data ("Extended" tables) served from the CSV source.
 *
 *   GET /api/agency/extended        — all agencies { agencies: { <slug>: {...} } }
 *   GET /api/agency/extended/:slug  — one agency  { slug, title, tag, badges, ... }
 *
 * Replaces the committed agency-extended.json static file. Public read (the same
 * data was previously a public static asset).
 */
import { Hono } from 'hono';
import { loadAllAgencyExtended, getAgencyExtended } from '../services/agencyData.js';

export const agencyRouter = new Hono();

agencyRouter.get('/api/agency/extended', (c) => {
  c.header('Cache-Control', 'public, max-age=300');
  return c.json({ agencies: loadAllAgencyExtended() });
});

agencyRouter.get('/api/agency/extended/:slug', (c) => {
  const slug = c.req.param('slug');
  const agency = getAgencyExtended(slug);
  if (!agency) return c.json({ error: 'unknown agency' }, 404);
  c.header('Cache-Control', 'public, max-age=300');
  return c.json({ slug, ...agency });
});
