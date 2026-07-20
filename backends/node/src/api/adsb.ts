/**
 * Live ADS-B aircraft endpoints.
 *
 *   GET /api/adsb/aircraft — merged aircraft positions over NSW from
 *                            adsb.lol / adsb.fi / airplanes.live,
 *                            deduped by ICAO hex. Live-only snapshot;
 *                            deliberately NOT in CACHEABLE_PATHS — a
 *                            30 s max-age would defeat the frontend's
 *                            15 s refresh.
 *
 *   GET /api/adsb/photo/:hex?reg=VH-ABC
 *                          — aircraft photo lookup via the Planespotters
 *                            public API, proxied because they reject
 *                            browser-originated requests (their policy
 *                            wants a server UA with a contact URL, which
 *                            our default UA in sources/shared/http.ts
 *                            carries). Tries the hex first, falls back
 *                            to the registration. Cached 24 h — photos
 *                            for a given airframe essentially never
 *                            change and Planespotters rate-limits.
 */
import { Hono } from 'hono';
import { adsbSnapshot } from '../sources/adsb.js';
import { fetchJson } from '../sources/shared/http.js';
import { SwrCache } from '../services/swrCache.js';
import { log } from '../lib/log.js';

export const adsbRouter = new Hono();

adsbRouter.get('/api/adsb/aircraft', (c) => c.json(adsbSnapshot()));

interface PlanespottersPhoto {
  id?: string;
  thumbnail?: { src?: string };
  thumbnail_large?: { src?: string };
  link?: string;
  photographer?: string;
}
interface PlanespottersResponse {
  photos?: PlanespottersPhoto[];
  error?: string;
}

export interface AdsbPhoto {
  src: string;
  link: string | null;
  photographer: string | null;
}

const PLANESPOTTERS_BASE = 'https://api.planespotters.net/pub/photos';
// Planespotters classifies undici's fetch as a browser request (it sends
// sec-fetch-* headers) and then requires an Origin/Referer identifying
// the embedding site — without this the API answers 403.
const PLANESPOTTERS_HEADERS = { Referer: 'https://nswpsn.forcequit.xyz/' };

// Photos per airframe are effectively static; keep them a full day and
// serve stale for a week rather than re-hitting the rate-limited API.
const photoCache = new SwrCache<AdsbPhoto | null>(2000);
const PHOTO_FRESH_MS = 24 * 3600_000;
const PHOTO_STALE_MS = 7 * 24 * 3600_000;

function firstPhoto(body: PlanespottersResponse): AdsbPhoto | null {
  const p = body.photos?.[0];
  const src = p?.thumbnail_large?.src ?? p?.thumbnail?.src;
  if (!src) return null;
  return {
    src,
    link: p?.link ?? null,
    photographer: p?.photographer ?? null,
  };
}

async function lookupPhoto(hex: string, reg: string): Promise<AdsbPhoto | null> {
  // TIS-B targets ('~'-prefixed) have synthetic ids Planespotters can't
  // know — go straight to the registration for those.
  if (!hex.startsWith('~')) {
    const byHex = await fetchJson<PlanespottersResponse>(
      `${PLANESPOTTERS_BASE}/hex/${encodeURIComponent(hex)}`,
      { timeoutMs: 8_000, headers: PLANESPOTTERS_HEADERS },
    );
    const photo = firstPhoto(byHex);
    if (photo) return photo;
  }
  if (reg) {
    const byReg = await fetchJson<PlanespottersResponse>(
      `${PLANESPOTTERS_BASE}/reg/${encodeURIComponent(reg)}`,
      { timeoutMs: 8_000, headers: PLANESPOTTERS_HEADERS },
    );
    return firstPhoto(byReg);
  }
  return null;
}

adsbRouter.get('/api/adsb/photo/:hex', async (c) => {
  const hex = c.req.param('hex').trim().toLowerCase();
  const reg = (c.req.query('reg') ?? '').trim().toUpperCase();
  if (!/^~?[0-9a-f]{4,8}$/.test(hex)) {
    return c.json({ error: 'invalid hex' }, 400);
  }
  if (reg && !/^[A-Z0-9-]{2,10}$/.test(reg)) {
    return c.json({ error: 'invalid reg' }, 400);
  }
  try {
    const { value } = await photoCache.get(
      `${hex}|${reg}`,
      () => lookupPhoto(hex, reg),
      {
        fresh: PHOTO_FRESH_MS,
        stale: PHOTO_STALE_MS,
        onError: (err) => log.warn({ err, hex }, 'adsb: photo refresh failed'),
      },
    );
    return c.json({ photo: value });
  } catch (err) {
    // Cold-path failure (nothing cached). Not worth a 5xx — the frontend
    // just shows no photo.
    log.warn({ err, hex }, 'adsb: photo lookup failed');
    return c.json({ photo: null });
  }
});
