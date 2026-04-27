/**
 * End-to-end test for the Waze ingest path:
 *   POST /api/waze/ingest with X-Ingest-Key
 *   GET  /api/waze/{police,hazards,roadwork} → reads from LiveStore
 *
 * Uses Hono's request-handler API directly (no port binding), so this
 * runs in the unit suite without a DB or any external state.
 */
import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import { createApp } from '../../src/server.js';
import { liveStore } from '../../src/store/live.js';

// Matches what vitest.config.ts sets in test.env. The setting MUST
// happen at vitest level (not beforeAll) because src/config.ts parses
// process.env at module load time.
const INGEST_KEY = 'test-ingest-key';

describe('waze ingest end-to-end', () => {
  let app: ReturnType<typeof createApp>;

  beforeAll(() => {
    app = createApp();
  });

  beforeEach(() => {
    // Each test starts with an empty LiveStore so they don't bleed.
    liveStore.delete('waze');
  });

  it('rejects POST without X-Ingest-Key', async () => {
    const res = await app.request('/api/waze/ingest', {
      method: 'POST',
      body: JSON.stringify({
        bbox: { top: 0, bottom: 0, left: 0, right: 0 },
      }),
      headers: { 'Content-Type': 'application/json' },
    });
    expect(res.status).toBe(401);
  });

  it('rejects malformed JSON', async () => {
    const res = await app.request('/api/waze/ingest', {
      method: 'POST',
      body: 'not json',
      headers: {
        'Content-Type': 'application/json',
        'X-Ingest-Key': INGEST_KEY,
      },
    });
    expect(res.status).toBe(400);
  });

  it('accepts a valid payload and exposes it via GET /api/waze/police', async () => {
    const ingestRes = await app.request('/api/waze/ingest', {
      method: 'POST',
      body: JSON.stringify({
        bbox: { top: -33.5, bottom: -34.0, left: 151.0, right: 151.5 },
        alerts: [
          {
            uuid: 'alert-1',
            type: 'POLICE',
            subtype: 'POLICE_VISIBLE',
            location: { x: 151.2, y: -33.8 },
            street: 'George St',
            city: 'Sydney',
          },
          {
            uuid: 'alert-2',
            type: 'HAZARD',
            subtype: 'HAZARD_ON_ROAD_OBJECT',
            location: { x: 151.21, y: -33.81 },
          },
        ],
        jams: [],
      }),
      headers: {
        'Content-Type': 'application/json',
        'X-Ingest-Key': INGEST_KEY,
      },
    });
    expect(ingestRes.status).toBe(200);
    const ingestBody = (await ingestRes.json()) as Record<string, unknown>;
    expect(ingestBody['ok']).toBe(true);
    expect(ingestBody['regions_cached']).toBe(1);

    const policeRes = await app.request('/api/waze/police');
    expect(policeRes.status).toBe(200);
    const police = (await policeRes.json()) as {
      type: string;
      features: Array<{
        properties: { id: string; type: string; wazeType: string };
      }>;
      count: number;
    };
    expect(police.type).toBe('FeatureCollection');
    expect(police.count).toBe(1);
    expect(police.features[0]?.properties.id).toBe('alert-1');
    expect(police.features[0]?.properties.type).toBe('Police');
    expect(police.features[0]?.properties.wazeType).toBe('POLICE');
  });

  it('separates police, hazards, and roadwork by alert type', async () => {
    await app.request('/api/waze/ingest', {
      method: 'POST',
      body: JSON.stringify({
        bbox: { top: 0, bottom: -1, left: 150, right: 151 },
        alerts: [
          { uuid: 'p1', type: 'POLICE', location: { x: 150.5, y: -0.5 } },
          { uuid: 'h1', type: 'HAZARD', location: { x: 150.6, y: -0.6 } },
          {
            uuid: 'r1',
            type: 'CONSTRUCTION',
            location: { x: 150.7, y: -0.7 },
          },
          { uuid: 'a1', type: 'ACCIDENT', location: { x: 150.8, y: -0.8 } },
        ],
      }),
      headers: {
        'Content-Type': 'application/json',
        'X-Ingest-Key': INGEST_KEY,
      },
    });

    const police = (await (
      await app.request('/api/waze/police')
    ).json()) as { count: number };
    const hazards = (await (
      await app.request('/api/waze/hazards')
    ).json()) as { count: number };
    const roadwork = (await (
      await app.request('/api/waze/roadwork')
    ).json()) as { count: number };

    expect(police.count).toBe(1);
    expect(hazards.count).toBe(2); // HAZARD + ACCIDENT
    expect(roadwork.count).toBe(1);
  });

  it('drops alerts without coordinates', async () => {
    await app.request('/api/waze/ingest', {
      method: 'POST',
      body: JSON.stringify({
        bbox: { top: 0, bottom: -1, left: 150, right: 151 },
        alerts: [
          { uuid: 'no-loc', type: 'POLICE' }, // missing coordinates
        ],
      }),
      headers: {
        'Content-Type': 'application/json',
        'X-Ingest-Key': INGEST_KEY,
      },
    });
    const police = (await (
      await app.request('/api/waze/police')
    ).json()) as { count: number };
    expect(police.count).toBe(0);
  });

  it('GET /api/waze/metrics surfaces region count', async () => {
    await app.request('/api/waze/ingest', {
      method: 'POST',
      body: JSON.stringify({
        bbox: { top: 1, bottom: 0, left: 150, right: 151 },
        alerts: [],
      }),
      headers: {
        'Content-Type': 'application/json',
        'X-Ingest-Key': INGEST_KEY,
      },
    });
    const m = (await (
      await app.request('/api/waze/metrics')
    ).json()) as Record<string, unknown>;
    expect(m['regions_cached']).toBe(1);
    expect(typeof m['last_ingest_age_secs']).toBe('number');
  });
});
