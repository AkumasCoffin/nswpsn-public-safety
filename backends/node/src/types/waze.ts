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

// Match python's permissive coercion: `float(bbox.get('top', 0))`.
// The userscript occasionally posts bbox values as strings (Waze's own
// /api/georss response carries them as strings sometimes), and a strict
// `z.number()` would 400 every such ingest. `z.coerce.number()` accepts
// numbers and numeric strings; `.default(0)` matches python's `or 0`
// fallback when a field is missing entirely.
const bboxCoord = z.coerce.number().catch(0).default(0);
export const BboxSchema = z.object({
  top: bboxCoord,
  bottom: bboxCoord,
  left: bboxCoord,
  right: bboxCoord,
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

// Waze upstream sometimes serialises uuid/id as integers (specifically
// for jam objects). Coerce to string so dedup keys are type-stable
// across the alert + jam collections.
const idLike = z
  .union([z.string(), z.number()])
  .transform((v) => String(v))
  .optional();

export const WazeAlertSchema = z
  .object({
    uuid: idLike,
    id: idLike,
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
    uuid: idLike,
    id: idLike,
  })
  .passthrough();
export type WazeJam = z.infer<typeof WazeJamSchema>;

export const WazeIngestPayloadSchema = z.object({
  // Default {} if missing: python does `payload.get('bbox') or {}` then
  // coerces each field to float with a 0 default — same effective
  // behaviour as missing-bbox-becomes-{0,0,0,0}.
  bbox: BboxSchema.default({ top: 0, bottom: 0, left: 0, right: 0 }),
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
