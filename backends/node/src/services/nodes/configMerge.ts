/**
 * Build the full `configPush` payload the backend sends a node agent.
 *
 * The backend owns the BASE presets (feeder-nodes/radio-node/presets):
 *   - rdio-scanner.json — the full rdio config document (apiKeys[].key left
 *     empty; the agent fills each with a locally-generated per-system key).
 *   - default.xml       — the SDR-Trunk playlist template. The backend does
 *     NOT parse it here; the agent renders the playlist from its own bundled
 *     copy. We only read it as text so a missing/unreadable preset dir surfaces
 *     as a clear error before we try to push a half-built config.
 *
 * A node's staff-editable `config_override` (site name, control frequencies,
 * tuner gain/ppm — validated by configSchema.ts) is merged on top to produce
 * the `channelPlan`. `streamTargets` is derived from the rdio systems so the
 * agent knows which per-agency local keys to generate. `configVersion` is a
 * sha256 of the canonicalised payload (minus the version field) so the agent
 * can detect drift and re-apply only on change.
 *
 * The presets don't change at runtime, so they're parsed once and cached.
 */
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { config } from '../../config.js';
import { log } from '../../lib/log.js';
import type { NodeRow } from './registry.js';
import {
  ConfigOverrideSchema,
  type ConfigOverride,
  type DecoderType,
  type DecoderConfig,
} from './configSchema.js';
import {
  getGlobalConfig,
  agenciesToAliases,
  agenciesToSystems,
  type Alias,
  type GlobalConfig,
} from './globalConfig.js';

// ── payload contract ───────────────────────────────────────────────────────
export interface StreamTarget {
  systemId: number;
  name: string;
}

/** One SDR-Trunk channel the agent renders into the playlist. */
export interface ChannelPlan {
  name: string;
  frequency: number; // Hz (control-channel freq for trunked P25)
  decoder: DecoderType;
  system?: string;
  site?: string;
  autoStart?: boolean;
  order?: number;
  /** Device serial to pin to; omitted = any available SDR. */
  sdr?: string;
  /** Decoder-specific settings; the agent fills SDR-Trunk defaults for omitted
   *  fields when rendering this channel's <decode_configuration>. */
  decoderConfig?: DecoderConfig;
}

/** Per-SDR tuner settings, keyed by serial ("*" = apply to all SDRs). */
export interface TunerSettings {
  serial: string;
  label?: string;
  sampleRate?: number;
  gain?: number | null;
  autoGain?: boolean;
  gainParams?: Record<string, unknown>;
  ppm?: number;
  autoPpm?: boolean;
  type?: string;
}

export interface ConfigPayload {
  configVersion: string;
  channels: ChannelPlan[];
  tuners: TunerSettings[];
  /** Global SDR-Trunk aliases (synced to all nodes); agent renders them into
   *  the playlist. */
  aliases: Alias[];
  /** The full rdio-scanner.json document (apiKeys[].key empty) with global
   *  systems/groups/tags merged in. */
  rdioConfig: Record<string, unknown>;
  streamTargets: StreamTarget[];
  /** Node on/off: when false the agent stops ALL capture (radio: every SDR-Trunk
   *  channel), staying connected. */
  captureEnabled: boolean;
  /** Feed on/off: when false the agent disables the rdio downstream so rdio keeps
   *  running but stops uploading. */
  feedEnabled: boolean;
}

// The "system" label reported in the channel plan — the radio network these
// nodes receive. (The site is only NAMED after NSW PSN; see project memory.)
const PLAN_SYSTEM = 'NSW PSN';

interface RdioSystem {
  id?: number;
  _id?: number;
  label?: string;
}

interface LoadedPresets {
  rdio: Record<string, unknown>;
  systems: RdioSystem[];
  // Kept only to fail fast if the playlist template is missing; not parsed.
  playlistXml: string;
}

let cache: LoadedPresets | null = null;

/**
 * Read + parse the base presets from NODE_PRESET_DIR, caching the result.
 * Throws if the directory or either file is missing/unreadable so callers
 * can 503 / skip rather than push a broken config.
 */
export function loadPresets(): LoadedPresets {
  if (cache) return cache;
  const dir = path.resolve(config.NODE_PRESET_DIR);
  const rdioText = readFileSync(path.join(dir, 'rdio-scanner.json'), 'utf8');
  const playlistXml = readFileSync(path.join(dir, 'default.xml'), 'utf8');
  const rdio = JSON.parse(rdioText) as Record<string, unknown>;
  const systemsRaw = rdio['systems'];
  const systems: RdioSystem[] = Array.isArray(systemsRaw)
    ? (systemsRaw as RdioSystem[])
    : [];
  cache = { rdio, systems, playlistXml };
  log.info(
    { dir, systems: systems.length },
    'loaded node config presets',
  );
  return cache;
}

/** Test/ops hook — drop the cached presets so the next build re-reads disk. */
export function _clearPresetCache(): void {
  cache = null;
}

/**
 * Canonical JSON: recursively sort object keys so the serialization is stable
 * regardless of insertion order. Arrays keep their order (order is meaningful
 * for systems/streams). Used only for hashing.
 */
function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      out[key] = canonicalize((value as Record<string, unknown>)[key]);
    }
    return out;
  }
  return value;
}

function sha256Hex(s: string): string {
  return createHash('sha256').update(s).digest('hex');
}

/** Parse+validate a stored override; a malformed one degrades to empty. */
function parseOverride(raw: Record<string, unknown> | null | undefined): ConfigOverride {
  const parsed = ConfigOverrideSchema.safeParse(raw ?? {});
  if (parsed.success) return parsed.data;
  log.warn({ issues: parsed.error.issues }, 'node config_override failed schema; ignoring');
  return {};
}

/**
 * Derive the channel list. Prefers the new `channels[]`; otherwise migrates the
 * deprecated single-channel `site` form (first control frequency → one channel)
 * so pre-P5 overrides keep working.
 */
function deriveChannels(override: ConfigOverride, node: NodeRow): ChannelPlan[] {
  if (override.channels && override.channels.length > 0) {
    return override.channels.map((c) => ({ ...c }));
  }
  const freqs = override.site?.controlFrequencies ?? [];
  const first = freqs[0];
  if (first === undefined) return [];
  return [
    {
      name: override.site?.name ?? node.name ?? 'Control',
      frequency: first,
      decoder: 'p25p2',
      system: PLAN_SYSTEM,
      site: override.site?.name,
      autoStart: true,
      order: 1,
    },
  ];
}

/**
 * Derive per-SDR tuner settings. Prefers the new `tuners[]`; otherwise maps the
 * deprecated node-wide `tuner` to a single "*" (all-SDRs) entry.
 */
function deriveTuners(override: ConfigOverride): TunerSettings[] {
  if (override.tuners && override.tuners.length > 0) {
    return override.tuners.map((t) => ({ ...t }));
  }
  const legacy = override.tuner;
  if (legacy && (legacy.gain !== undefined || legacy.ppm !== undefined || legacy.type)) {
    return [{ serial: '*', gain: legacy.gain, ppm: legacy.ppm, type: legacy.type }];
  }
  return [];
}

/**
 * Merge the base presets + the global feeder config + a node's `config_override`
 * into the full ConfigPayload pushed to the agent. The global config (aliases +
 * rdio systems/groups/tags) is fetched from the DB and folded in, so editing it
 * changes every node's configVersion and re-syncs the fleet. Throws (via
 * loadPresets) if presets are unavailable.
 *
 * `global` may be passed in (e.g. when fanning out to many nodes, to fetch it
 * once); otherwise it's read here.
 */
export async function buildConfigPayload(
  node: NodeRow,
  global?: GlobalConfig,
): Promise<ConfigPayload> {
  const presets = loadPresets();
  const globalCfg = global ?? (await getGlobalConfig());
  const override = parseOverride(node.config_override);

  const channels = deriveChannels(override, node);
  const tuners = deriveTuners(override);

  // Agencies are the single source of truth: derive the SDR-Trunk aliases + the
  // rdio systems from them (id/label/name/broadcastChannel all unified).
  const aliases = agenciesToAliases(globalCfg.agencies);
  const derivedSystems = agenciesToSystems(globalCfg.agencies);

  // The rdio document: preset as the base (options/apiKeys/downstreams/etc.),
  // with the fleet-wide systems/groups/tags overlaid from the global config.
  const rdioConfig = structuredClone(presets.rdio) as Record<string, unknown>;
  if (derivedSystems.length > 0) rdioConfig['systems'] = derivedSystems;
  if (globalCfg.rdioGroups.length > 0) rdioConfig['groups'] = globalCfg.rdioGroups;
  if (globalCfg.rdioTags.length > 0) rdioConfig['tags'] = globalCfg.rdioTags;

  // Stream targets follow the (possibly-edited) rdio systems so the agent knows
  // which per-agency local keys to generate.
  const systemsForTargets = (rdioConfig['systems'] as RdioSystem[] | undefined) ?? presets.systems;
  const streamTargets: StreamTarget[] = systemsForTargets
    .map((s): StreamTarget | null => {
      const systemId = s.id ?? s._id;
      if (typeof systemId !== 'number') return null;
      return { systemId, name: (s.label ?? `System ${systemId}`).trim() };
    })
    .filter((t): t is StreamTarget => t !== null);

  // Generate one rdio apiKey per system from the SAME systems list, so a system
  // the operator creates automatically gets its own upload key (1:1). The `key`
  // is left EMPTY here — the agent fills each with the unique local key it mints
  // for that system (keys.EnsureKeys), the same key it injects into the matching
  // SDR-Trunk stream, so the two sides always agree and no key is ever set by
  // hand. This replaces the static preset apiKeys.
  if (streamTargets.length > 0) {
    rdioConfig['apiKeys'] = streamTargets.map((t, i) => ({
      _id: i + 1,
      disabled: false,
      ident: t.name,
      key: '',
      order: i + 1,
      systems: [{ id: t.systemId, talkgroups: '*' }],
    }));
  }

  // captureEnabled/feedEnabled are per-node and part of the hashed payload, so a
  // Node-on/off or Feed-on/off toggle changes configVersion and the agent
  // re-applies (stops/starts capture; enables/disables the rdio downstream).
  const payloadNoVersion = {
    channels,
    tuners,
    aliases,
    rdioConfig,
    streamTargets,
    captureEnabled: node.enabled,
    feedEnabled: node.feed_enabled,
  };
  const configVersion = sha256Hex(JSON.stringify(canonicalize(payloadNoVersion)));

  return { configVersion, ...payloadNoVersion };
}
