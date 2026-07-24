/**
 * Zod schema for a node's `config_override` — the staff-editable slice of a
 * feeder node's configuration that overrides the base presets when the backend
 * builds the full ConfigPayload (see configMerge.ts).
 *
 * Everything is optional: an empty `{}` override is valid and means "use the
 * presets as-is". The PATCH /api/nodes/:id handler validates the incoming
 * `config_override` against this so staff can't persist arbitrary junk that
 * would later poison the merge.
 */
import { z } from 'zod';

/**
 * Decoder types offered in the channel editor. `p25p2` is the default (NSW PSN
 * is P25 Phase 2); the rest cover conventional/analog and other digital modes a
 * node might also watch. The agent maps these to SDR-Trunk decode_configuration
 * types when rendering the playlist.
 */
export const DECODER_TYPES = ['p25p2', 'p25p1', 'dmr', 'nbfm', 'am'] as const;
export type DecoderType = (typeof DECODER_TYPES)[number];

/**
 * One SDR-Trunk channel on a node. For a trunked P25 system `frequency` is the
 * control-channel frequency and SDR-Trunk follows the trunk; for conventional
 * modes it's the channel frequency. `system`/`site` are free-text labels shown
 * in SDR-Trunk + carried onto decoded calls. `order` sets SDR-Trunk's channel
 * start order; `autoStart` starts the channel on load.
 */
export const ChannelSchema = z
  .object({
    name: z.string().min(1).max(120),
    frequency: z.number().int().positive(), // Hz, e.g. 142658000
    decoder: z.enum(DECODER_TYPES).default('p25p2'),
    system: z.string().max(120).optional(),
    site: z.string().max(120).optional(),
    autoStart: z.boolean().optional(),
    order: z.number().int().min(0).max(9999).optional(),
    // Optional SDR pin: the device serial this channel must use. Absent = let
    // SDR-Trunk pick any available tuner (sourceConfigTuner).
    sdr: z.string().max(80).optional(),
  })
  .strict();
export type Channel = z.infer<typeof ChannelSchema>;

/**
 * Per-SDR tuner settings, keyed by device `serial` so they follow the physical
 * dongle across reboots/re-plugs (a node may have several SDRs, each tuned
 * differently). `sampleRate` is Hz; `gain` is a numeric SDR-Trunk gain value;
 * `autoPpm` lets SDR-Trunk correct PPM from the control channel.
 */
export const TunerSettingsSchema = z
  .object({
    serial: z.string().min(1).max(80),
    label: z.string().max(120).optional(),
    sampleRate: z.number().positive().optional(),
    gain: z.number().optional(),
    ppm: z.number().optional(),
    autoPpm: z.boolean().optional(),
    type: z.string().max(40).optional(),
  })
  .strict();
export type TunerSettings = z.infer<typeof TunerSettingsSchema>;

export const ConfigOverrideSchema = z
  .object({
    // The node's channels (0..N). Replaces the deprecated single-channel `site`
    // form below; when present it wins in the merge.
    channels: z.array(ChannelSchema).max(64).optional(),
    // Per-SDR tuner settings, one entry per device serial.
    tuners: z.array(TunerSettingsSchema).max(16).optional(),
    // DEPRECATED single-channel form. Kept so older overrides still validate and
    // merge (configMerge derives one channel from it when `channels` is absent).
    site: z
      .object({
        name: z.string().max(120).optional(),
        controlFrequencies: z.array(z.number().int().positive()).optional(),
      })
      .strict()
      .optional(),
    // DEPRECATED node-wide tuner (applied to all SDRs). Superseded by `tuners[]`.
    tuner: z
      .object({
        gain: z.number().optional(),
        ppm: z.number().optional(),
        type: z.string().max(40).optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

export type ConfigOverride = z.infer<typeof ConfigOverrideSchema>;
