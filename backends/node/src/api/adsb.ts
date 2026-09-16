/**
 * Live ADS-B aircraft endpoints.
 *
 *   GET /api/adsb/aircraft — merged aircraft positions over NSW from
 *                            adsb.lol / adsb.fi / airplanes.live /
 *                            adsb.one, deduped by ICAO hex. Live-only;
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
import { adsbSnapshot, adsbTrailsSnapshot } from '../sources/adsb.js';
import {
  adsbHistoryAt,
  adsbHistoryOldestMs,
  snapHistoryBucket,
  HISTORY_TRAIL_DEFAULT_MIN,
} from '../services/adsbHistory.js';
import { DATA_RETENTION_DAYS } from '../lib/retention.js';
import { fetchJson } from '../sources/shared/http.js';
import { SwrCache } from '../services/swrCache.js';
import { log } from '../lib/log.js';

export const adsbRouter = new Hono();

adsbRouter.get('/api/adsb/aircraft', (c) => {
  // The history block rides along on the live poll rather than getting its own
  // endpoint: the map already fetches this every 15s, and the date picker
  // needs the window bounds before it can offer a first scrub.
  const now = Date.now();
  return c.json({
    ...adsbSnapshot(),
    history: {
      retentionDays: DATA_RETENTION_DAYS,
      oldestMs: adsbHistoryOldestMs(now),
      newestMs: now,
    },
  });
});

// ---------------------------------------------------------------------------
// GET /api/adsb/history?at=<epoch ms>&trail=<minutes>
//
// Where everything was at an instant, plus the track flown into it. Backed by
// adsb_tracks (migration 103), which stores one row per aircraft per hour.
//
// `at` is SNAPPED to a fixed grid inside the service and echoed back, and the
// response is deliberately not scoped to a viewport. Both choices exist so
// that many people scrubbing independently converge on the same URLs: a
// bbox-scoped or millisecond-exact response would give every viewer a private
// cache entry. The client interpolates within the bucket, so the coarse grid
// costs nothing on screen.
// ---------------------------------------------------------------------------
adsbRouter.get('/api/adsb/history', async (c) => {
  const url = new URL(c.req.url);
  const atRaw = Number(url.searchParams.get('at') ?? '');
  const at = Number.isFinite(atRaw) ? atRaw : Date.now();
  const trail = Number(url.searchParams.get('trail') ?? HISTORY_TRAIL_DEFAULT_MIN);

  const now = Date.now();
  const oldest = adsbHistoryOldestMs(now);
  if (at < oldest) {
    return c.json({ error: 'outside the retention window', oldestMs: oldest }, 400);
  }

  try {
    const data = await adsbHistoryAt(at, trail, now);
    if (!data) return c.json({ error: 'history unavailable' }, 503);

    // A closed bucket can never change, so it is worth caching hard. The most
    // recent one is still filling — the archive flushes once a minute — so it
    // gets the short window instead. Set here rather than left to
    // CACHEABLE_PATHS, whose blanket 30s would throw away most of the sharing
    // the bucketing exists to create (that middleware only fills in a
    // Cache-Control header that is absent).
    const settled = snapHistoryBucket(now) - data.atMs >= 2 * 60_000;
    c.header(
      'Cache-Control',
      settled
        ? 'public, max-age=3600, stale-while-revalidate=86400'
        : 'public, max-age=30, stale-while-revalidate=120',
    );
    return c.json(data);
  } catch (err) {
    log.warn({ err }, '/api/adsb/history failed');
    return c.json({ error: 'history query failed' }, 500);
  }
});

// Position trails, prebuilt once per poll by the source (see
// updateTrails in sources/adsb.ts). Fixed path with no query — listed
// in CACHEABLE_PATHS so the CDN/browser 30s cache absorbs any number
// of clients; the frontend polls it at the same 30s cadence.
adsbRouter.get('/api/adsb/trails', (c) => c.json(adsbTrailsSnapshot()));

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
