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
  agenciesToSystems,
  type Alias,
  type GlobalConfig,
} from './globalConfig.js';

// ── payload contract ───────────────────────────────────────────────────────
export interface StreamTarget {
  systemId: number;
  name: string;
  /** The rdio apiKey for this system, set on BOTH the sdrtrunk stream and the
   *  matching rdio apiKey so a call uploads under the right systemId. Generated
   *  server-side (deterministic per systemId) — non-empty + unique so the local
   *  rdio never rejects the config on a UNIQUE(key) collision. */
  key: string;
}

/** Deterministic, stable rdio apiKey for a system. Not a secret (localhost only)
 *  — its job is to be a non-empty value unique per systemId and identical on the
 *  sdrtrunk stream and the rdio apiKey so uploads route. Stable across re-applies
 *  so the config version doesn't churn. */
function rdioKeyForSystem(systemId: number): string {
  const h = createHash('sha256').update(`nswpsn-rdio-apikey:${systemId}`).digest('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
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

/** One paging frequency a pager node scans, in priority order. The agent runs
 *  one reader per frequency up to the number of SDRs detected (1 SDR → the
 *  first only; 2 → both). */
export interface PagerFreq {
  label: string;
  mhz: number;
}

/** Pager-node config: which frequencies + POCSAG rates to decode. The central
 *  Pagermon URL/key are NEVER here — they stay server-side (the relay forwards). */
export interface PagerConfig {
  frequencies: PagerFreq[];
  protocols: string[];
  /** Optional tuner-gain override for ALL readers: a number-as-string (dB) or
   *  "auto" (hardware AGC). Absent = the agent's built-in default gain. */
  gain?: string;
  /** Optional ppm override applied to ALL readers. Absent = the agent's
   *  per-dongle auto-measured value. */
  ppm?: number;
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
   *  channel; pager: all readers), staying connected. */
  captureEnabled: boolean;
  /** Feed on/off: when false the agent disables the downstream so it keeps
   *  running but stops uploading. */
  feedEnabled: boolean;
  /** Present only for pager nodes — the frequencies/protocols to decode. */
  pager?: PagerConfig;
}

// Fixed pager plan (decided with the operator). NSW RFS + Fire & Rescue NSW.
// The list is sent in PRIORITY ORDER: with a single SDR the agent runs only the
// FIRST frequency; with two SDRs it runs both. Which one is first is a per-node
// preference (config_override.pagerPrimary), default NSWRFS.
const PAGER_FREQUENCIES: PagerFreq[] = [
  { label: 'NSWRFS', mhz: 148.5875 },
  { label: 'FRNSW', mhz: 148.9875 },
];
const PAGER_PROTOCOLS = ['POCSAG512', 'POCSAG1200', 'POCSAG2400'];
export const PAGER_PRIMARY_LABELS = ['NSWRFS', 'FRNSW'] as const;
export type PagerPrimary = (typeof PAGER_PRIMARY_LABELS)[number];

/** The per-node primary (single-SDR) frequency label — read from the persisted
 *  config_override so it survives restarts/updates. Defaults to NSWRFS. */
export function pagerPrimaryOf(node: NodeRow): PagerPrimary {
  const co = (node.config_override ?? {}) as Record<string, unknown>;
  return co['pagerPrimary'] === 'FRNSW' ? 'FRNSW' : 'NSWRFS';
}

/** The per-node tuner-gain override ("auto" or a number-as-string, dB), or
 *  undefined when unset (agent uses its default). Read from config_override so
 *  it persists across restarts/updates alongside the primary frequency. */
export function pagerGainOf(node: NodeRow): string | undefined {
  const co = (node.config_override ?? {}) as Record<string, unknown>;
  const g = co['pagerGain'];
  if (g === 'auto') return 'auto';
  if (typeof g === 'string' && g.trim() !== '' && Number.isFinite(Number(g))) return g;
  return undefined;
}

/** The per-node ppm override, or undefined when unset (agent auto-measures). */
export function pagerPpmOf(node: NodeRow): number | undefined {
  const co = (node.config_override ?? {}) as Record<string, unknown>;
  const p = co['pagerPpm'];
  return typeof p === 'number' && Number.isFinite(p) ? p : undefined;
}

/** Frequencies in priority order for this node: the chosen primary first. */
function pagerFrequencies(node: NodeRow): PagerFreq[] {
  const primary = pagerPrimaryOf(node);
  const first = PAGER_FREQUENCIES.find((f) => f.label === primary);
  const rest = PAGER_FREQUENCIES.filter((f) => f.label !== primary);
  return first ? [first, ...rest] : [...PAGER_FREQUENCIES];
}

/** Build the lean config payload a PAGER node receives. No presets/rdio/SDR-Trunk
 *  doc, no Pagermon key — just capture/feed + the frequency plan, all hashed into
 *  configVersion so a toggle re-syncs the node. */
function buildPagerPayload(node: NodeRow): ConfigPayload {
  const pager: PagerConfig = {
    frequencies: pagerFrequencies(node),
    protocols: PAGER_PROTOCOLS,
  };
  // Only attach overrides when set, so nodes without them keep their previous
  // configVersion (no spurious re-push). Both are hashed in below, so changing
  // either bumps the version and re-syncs the node live.
  const gain = pagerGainOf(node);
  if (gain !== undefined) pager.gain = gain;
  const ppm = pagerPpmOf(node);
  if (ppm !== undefined) pager.ppm = ppm;
  const payloadNoVersion = {
    channels: [] as ChannelPlan[],
    tuners: [] as TunerSettings[],
    aliases: [] as Alias[],
    rdioConfig: {} as Record<string, unknown>,
    streamTargets: [] as StreamTarget[],
    captureEnabled: node.enabled,
    feedEnabled: node.feed_enabled,
    pager,
  };
  const configVersion = sha256Hex(JSON.stringify(canonicalize(payloadNoVersion)));
  return { configVersion, ...payloadNoVersion };
}


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
function deriveChannels(override: ConfigOverride, _node: NodeRow): ChannelPlan[] {
  // A node has exactly the channels configured on its node page — nothing is
  // synthesized. A fresh node starts with zero channels; the operator adds each
  // one (with its control frequency) manually, so no phantom/default channel is
  // ever pushed to sdrtrunk.
  if (override.channels && override.channels.length > 0) {
    return override.channels.map((c) => ({ ...c }));
  }
  return [];
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
  // Pager nodes get a lean, radio-free payload (no presets, no rdio doc).
  if (node.kind === 'pager') return buildPagerPayload(node);

  const presets = loadPresets();
  const globalCfg = global ?? (await getGlobalConfig());
  const override = parseOverride(node.config_override);

  const channels = deriveChannels(override, node);
  const tuners = deriveTuners(override);

  // Agencies are the single source of truth: derive the SDR-Trunk aliases + the
  // rdio systems from them (id/label/name/broadcastChannel all unified). The
  // agency-level aliases carry the streaming ranges + routing; the per-talkgroup
  // aliases give every talkgroup its own label and route (so all talkgroups get
  // imported into sdrtrunk-vce and show individually in the activity view).
  // sdrtrunk-vce aliases come from the IMPORTED sdrtrunk config (not generated
  // from the rdio talkgroups). P25 channels reference a single alias list, so
  // collapse every imported alias into one list — a channel's list then carries
  // all labels + ranges + routes. Primary list = the first imported list, else a
  // default. Empty when no sdrtrunk config has been imported yet.
  const sdr = globalCfg.sdrtrunkConfig ?? { aliasLists: [], aliases: [], streams: [] };
  // Primary list = the one the MOST aliases belong to (the comprehensive list,
  // e.g. NSWPSN with ~1,500 vs the 27-entry range list), falling back to the
  // first declared list, then a default.
  const listCounts = new Map<string, number>();
  for (const a of sdr.aliases) {
    if (a.list) listCounts.set(a.list, (listCounts.get(a.list) ?? 0) + 1);
  }
  const primaryList =
    [...listCounts.entries()].sort((x, y) => y[1] - x[1])[0]?.[0] ??
    sdr.aliasLists[0]?.name ??
    'NSWPSN';
  const aliases = sdr.aliases.map((a) => ({ ...a, list: primaryList }));
  const derivedSystems = agenciesToSystems(globalCfg.agencies);

  // The rdio document: preset as the base (options/apiKeys/downstreams/etc.),
  // with the fleet-wide systems/groups/tags overlaid from the global config.
  const rdioConfig = structuredClone(presets.rdio) as Record<string, unknown>;
  if (derivedSystems.length > 0) rdioConfig['systems'] = derivedSystems;
  if (globalCfg.rdioGroups.length > 0) rdioConfig['groups'] = globalCfg.rdioGroups;
  if (globalCfg.rdioTags.length > 0) rdioConfig['tags'] = globalCfg.rdioTags;

  // Stream targets come from the IMPORTED sdrtrunk streams: each carries the
  // systemId (links a call to its rdio system + apiKey) and the name (= the
  // broadcastChannel the aliases route to). The agent mints one local key per
  // systemId and writes it onto both the stream and the matching rdio apiKey.
  // Fall back to deriving from the rdio systems only when no streams were
  // imported yet (keeps a pre-import / rdio-only config from having zero streams).
  let streamTargets: StreamTarget[];
  if (sdr.streams.length > 0) {
    streamTargets = sdr.streams.map((s) => ({
      systemId: s.systemId,
      name: s.name.trim(),
      key: rdioKeyForSystem(s.systemId),
    }));
  } else {
    const systemsForTargets = (rdioConfig['systems'] as RdioSystem[] | undefined) ?? presets.systems;
    streamTargets = systemsForTargets
      .map((s): StreamTarget | null => {
        const systemId = s.id ?? s._id;
        if (typeof systemId !== 'number') return null;
        return { systemId, name: (s.label ?? `System ${systemId}`).trim(), key: rdioKeyForSystem(systemId) };
      })
      .filter((t): t is StreamTarget => t !== null);
  }
  // De-dup stream targets by systemId (a single systemId must map to ONE rdio
  // apiKey, else the local rdio 500s on UNIQUE(key)/UNIQUE(system id)).
  {
    const seen = new Set<number>();
    streamTargets = streamTargets.filter((t) => (seen.has(t.systemId) ? false : (seen.add(t.systemId), true)));
  }

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
      // Real per-system key (non-empty, unique) — the agent keeps it (it only
      // fills EMPTY keys), so the rdio apiKey and the sdrtrunk stream that
      // uploads to this systemId carry the same key.
      key: t.key,
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
