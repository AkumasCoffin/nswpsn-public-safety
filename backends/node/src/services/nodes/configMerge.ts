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
import { ConfigOverrideSchema, type ConfigOverride } from './configSchema.js';

// ── payload contract (matches p4-config-contract.md) ───────────────────────
export interface StreamTarget {
  systemId: number;
  name: string;
}

export interface TunerPlan {
  gain?: number;
  ppm?: number;
  type?: string;
}

export interface ChannelPlan {
  system: string;
  siteName: string;
  controlFrequencies: number[]; // Hz
  tuner: TunerPlan;
}

export interface ConfigPayload {
  configVersion: string;
  channelPlan: ChannelPlan;
  /** The full rdio-scanner.json document (apiKeys[].key empty). */
  rdioConfig: Record<string, unknown>;
  streamTargets: StreamTarget[];
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
 * Merge the base presets with a node's `config_override` into the full
 * ConfigPayload pushed to the agent. Throws (via loadPresets) if presets
 * are unavailable.
 */
export function buildConfigPayload(node: NodeRow): ConfigPayload {
  const presets = loadPresets();
  const override = parseOverride(node.config_override);

  const channelPlan: ChannelPlan = {
    system: PLAN_SYSTEM,
    // No site name lives in the preset itself, so the node's own display name
    // is the sensible default when staff haven't set one explicitly.
    siteName: override.site?.name ?? node.name ?? 'NSW PSN Site',
    controlFrequencies: override.site?.controlFrequencies ?? [],
    tuner: override.tuner ?? {},
  };

  const streamTargets: StreamTarget[] = presets.systems
    .map((s): StreamTarget | null => {
      const systemId = s.id ?? s._id;
      if (typeof systemId !== 'number') return null;
      return { systemId, name: (s.label ?? `System ${systemId}`).trim() };
    })
    .filter((t): t is StreamTarget => t !== null);

  // The rdio document is served verbatim (apiKeys keys already empty in the
  // preset — the agent fills them). Structured-clone so a caller can't mutate
  // the cached preset.
  const rdioConfig = structuredClone(presets.rdio);

  const payloadNoVersion = { channelPlan, rdioConfig, streamTargets };
  const configVersion = sha256Hex(JSON.stringify(canonicalize(payloadNoVersion)));

  return { configVersion, ...payloadNoVersion };
}
