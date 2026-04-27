/**
 * Waze ingest payload + alert shapes.
 *
 * Matches what the Tampermonkey userscript posts (see
 * docs/waze-userscript.user.js) and what the Python backend stored in
 * _waze_ingest_cache.
 *
 * The payload is whatever the userscript scraped from
 * waze.com/live-map's georss feed — alerts, jams, users — for a single
 * bbox. We round bbox coordinates to 3dp (~110m) so re-pans of the same
 * region key to the same cache slot.
 */
import { z } from 'zod';

export const BboxSchema = z.object({
  top: z.number(),
  bottom: z.number(),
  left: z.number(),
  right: z.number(),
});

/** Bbox key tuple used for cache lookups, rounded to 3 decimal places. */
export type BboxKey = readonly [number, number, number, number];

/** Round a bbox to 3 decimals and tuple it for use as a Map key. */
export function bboxKey(b: z.infer<typeof BboxSchema>): BboxKey {
  return [
    Math.round(b.top * 1000) / 1000,
    Math.round(b.bottom * 1000) / 1000,
    Math.round(b.left * 1000) / 1000,
    Math.round(b.right * 1000) / 1000,
  ] as const;
}

/** String form of a bbox key for use as a regular Map<string,...>. */
export function bboxKeyStr(b: z.infer<typeof BboxSchema>): string {
  return bboxKey(b).join(',');
}

export const WazeLocationSchema = z
  .object({
    x: z.number().optional(),
    y: z.number().optional(),
    longitude: z.number().optional(),
    latitude: z.number().optional(),
  })
  .passthrough();

export const WazeAlertSchema = z
  .object({
    uuid: z.string().optional(),
    id: z.string().optional(),
    type: z.string().optional(),
    subtype: z.string().optional(),
    location: WazeLocationSchema.optional(),
    lat: z.number().optional(),
    lon: z.number().optional(),
  })
  .passthrough();
export type WazeAlert = z.infer<typeof WazeAlertSchema>;

export const WazeJamSchema = z
  .object({
    uuid: z.string().optional(),
    id: z.string().optional(),
  })
  .passthrough();
export type WazeJam = z.infer<typeof WazeJamSchema>;

export const WazeIngestPayloadSchema = z.object({
  bbox: BboxSchema,
  alerts: z.array(WazeAlertSchema).optional().default([]),
  jams: z.array(WazeJamSchema).optional().default([]),
  users: z.array(z.unknown()).optional().default([]),
});
export type WazeIngestPayload = z.infer<typeof WazeIngestPayloadSchema>;

/** What we store per bbox in the Waze ingest cache. */
export interface WazeBboxSnapshot {
  alerts: WazeAlert[];
  jams: WazeJam[];
  users: unknown[];
  /** Epoch seconds when this bbox snapshot was last received. */
  ts: number;
  /** The bbox itself, useful for eventual heatmap reconstruction. */
  bbox: { top: number; bottom: number; left: number; right: number };
}

/** GeoJSON Feature shape returned by /api/waze/{police,hazards,roadwork}. */
export interface WazeGeoFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: Record<string, unknown>;
}

export interface WazeFeatureCollection {
  type: 'FeatureCollection';
  features: WazeGeoFeature[];
  count: number;
}
