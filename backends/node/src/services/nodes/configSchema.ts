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
 * Decoder-specific channel settings (see the SDR-Trunk config reference's
 * "decoderConfig JSON contract"). Discriminated at render time by the channel's
 * `decoder`; this is a loose superset of every decoder's fields so a channel can
 * carry whichever ones its decoder uses. ALL fields are optional — the agent's
 * renderer fills SDR-Trunk defaults for anything omitted. Per-decoder use:
 *   - p25p1: modulation, ignoreDataCalls, trafficPoolSize
 *   - p25p2: ignoreDataCalls, trafficPoolSize, autoDetectScramble, scramble{wacn,system,nac}
 *   - dmr:   ignoreDataCalls, ignoreCrc, useCompressedTalkgroups, trafficPoolSize, timeslots[{lcn,downlink,uplink}]
 *   - nbfm:  bandwidth, talkgroup, audioFilter
 *   - am:    bandwidth, talkgroup, squelch, autoTrack
 */
export const BANDWIDTH_VALUES = [
  'BW_3_0',
  'BW_5_0',
  'BW_7_5',
  'BW_8_33',
  'BW_12_5',
  'BW_15_0',
  'BW_25_0',
] as const;

export const DmrTimeslotSchema = z
  .object({
    lcn: z.number().int().min(0).optional(),
    downlink: z.number().int().min(0).optional(), // Hz
    uplink: z.number().int().min(0).optional(), // Hz
  })
  .strict();

export const DecoderConfigSchema = z
  .object({
    // p25p1
    modulation: z.enum(['C4FM', 'CQPSK']).optional(),
    // shared: p25p1 / p25p2 / dmr
    ignoreDataCalls: z.boolean().optional(),
    trafficPoolSize: z.number().int().min(0).max(50).optional(),
    // p25p2
    autoDetectScramble: z.boolean().optional(),
    scramble: z
      .object({
        wacn: z.number().int().min(0).optional(),
        system: z.number().int().min(0).optional(),
        nac: z.number().int().min(0).optional(),
      })
      .strict()
      .optional(),
    // dmr
    ignoreCrc: z.boolean().optional(),
    useCompressedTalkgroups: z.boolean().optional(),
    timeslots: z.array(DmrTimeslotSchema).max(32).optional(),
    // nbfm / am
    bandwidth: z.enum(BANDWIDTH_VALUES).optional(),
    talkgroup: z.number().int().min(1).max(65535).optional(),
    audioFilter: z.boolean().optional(), // nbfm
    // am
    squelch: z.number().int().optional(), // dB, may be negative
    autoTrack: z.boolean().optional(),
  })
  .strict();
export type DecoderConfig = z.infer<typeof DecoderConfigSchema>;

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
    // Decoder-specific settings; the agent renders the matching
    // <decode_configuration> from these (SDR-Trunk defaults fill omitted fields).
    decoderConfig: DecoderConfigSchema.optional(),
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
    gain: z.number().nullable().optional(),
    // Auto/AGC gain flag. When true the agent asks the device for automatic gain
    // and ignores the scalar `gain` above.
    autoGain: z.boolean().optional(),
    // Per-axis gain values for multi-stage devices (e.g. Airspy/HackRF lna/vga/
    // mixer) or `{ auto: true }`. Free-form: keys/units are device-specific, so
    // this is a loose bag the agent forwards to SDR-Trunk verbatim — but bounded
    // (keys + serialized size) so a PATCH can't stuff an unbounded blob into the
    // stored/pushed/hashed config.
    gainParams: z
      .record(z.string(), z.unknown())
      .refine(
        (v) => Object.keys(v).length <= 24 && JSON.stringify(v).length <= 2000,
        'gainParams too large',
      )
      .optional(),
    ppm: z.number().optional(),
    autoPpm: z.boolean().optional(),
    type: z.string().max(40).optional(),
  })
  // Not `.strict()`: the tuner UI evolves faster than this schema (new device
  // axes, capability flags), and a stray unknown key must not 400 the whole
  // config PATCH. `.passthrough()` keeps extras so nothing is silently dropped.
  .passthrough();
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
