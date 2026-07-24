/**
 * Zod schema for a node's `config_override` — the small, staff-editable slice
 * of a feeder node's configuration that overrides the base presets when the
 * backend builds the full ConfigPayload (see configMerge.ts).
 *
 * Everything is optional: an empty `{}` override is valid and means "use the
 * presets as-is". The PATCH /api/nodes/:id handler validates the incoming
 * `config_override` against this so staff can't persist arbitrary junk that
 * would later poison the merge.
 */
import { z } from 'zod';

export const ConfigOverrideSchema = z
  .object({
    site: z
      .object({
        // Human label for this node's receive site (shown in the channel plan).
        name: z.string().max(120).optional(),
        // Trunked-system control-channel frequencies, in Hz (e.g. 412987500).
        controlFrequencies: z.array(z.number().int().positive()).optional(),
      })
      .strict()
      .optional(),
    tuner: z
      .object({
        // RTL/SDR gain (dB). rdio/SDR-Trunk take a numeric gain index/value.
        gain: z.number().optional(),
        // Frequency correction, parts-per-million.
        ppm: z.number().optional(),
        // Tuner backend, e.g. "rtl", "airspy". Free-form; the agent maps it.
        type: z.string().max(40).optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

export type ConfigOverride = z.infer<typeof ConfigOverrideSchema>;
