/**
 * Global feeder configuration — the shared radio config synced to ALL feeder
 * nodes: SDR-Trunk aliases + rdio systems/talkgroups/units/groups/tags. Stored
 * as a singleton row (feeder_global_config, id = 1) and seeded lazily from the
 * on-disk presets the first time it's read while still empty.
 *
 * Per-node config (channels + tuner) stays in nodes.config_override; this is the
 * fleet-wide layer. Editing it bumps `version`, and the caller fans the new
 * config out to every online node (see configPush.pushConfigToAllNodes).
 */
import { z } from 'zod';
import { createHash } from 'node:crypto';
import { getPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';
import { loadPresets } from './configMerge.js';

// ── shapes ─────────────────────────────────────────────────────────────────

/** One `<id>` inside an SDR-Trunk alias, e.g. a talkgroup or priority. `type`
 *  is the SDR-Trunk id type; `attrs` preserves every other attribute verbatim
 *  so the agent can re-emit the element faithfully. */
export const AliasIdSchema = z
  .object({
    type: z.string().max(60),
    attrs: z.record(z.string(), z.string()).default({}),
  })
  .strict();

/** An SDR-Trunk alias: a named entry (with a colour/list/group) mapping one or
 *  more ids to a label. */
export const AliasSchema = z
  .object({
    name: z.string().max(200),
    list: z.string().max(200).optional(),
    group: z.string().max(200).optional(),
    color: z.string().max(40).optional(),
    // Named-icon (surfaced as the alias "Icon" dropdown) + "stream as talkgroup"
    // int. Both round-trip the `<alias>` attributes iconName / stream_talkgroup_alias.
    iconName: z.string().max(200).optional(),
    streamTalkgroupAlias: z.union([z.string().max(40), z.number()]).optional(),
    ids: z.array(AliasIdSchema).max(4096).default([]),
  })
  .strict();
export type Alias = z.infer<typeof AliasSchema>;

// rdio systems/groups/tags are complex documents; validate loosely (preserve
// every field) so the round-trip through the editor never drops data.
const LooseObj = z.record(z.string(), z.unknown());

/** The single SDR-Trunk alias list every generated alias belongs to. Auto-set —
 *  there is one playlist, so operators never pick a list. The channel's
 *  alias_list_name must match this (preset ships it). */
export const ALIAS_LIST_NAME = 'catch all PSN';

/**
 * An Agency: the unified entity that owns its SDR-Trunk alias + stream AND its
 * rdio system + apiKey. Identity is `systemId` + `name`; the name drives the
 * alias name, the stream name, the alias broadcastChannel, and the rdio system
 * label (all unified). The SDR-Trunk alias's talkgroup scope + look come from
 * the alias-* fields; the rdio system's per-talkgroup display + units come from
 * `talkgroups`/`units`. The alias list is NOT here — it's auto-set to
 * ALIAS_LIST_NAME.
 */
export const AgencySchema = z
  .object({
    systemId: z.number().int(),
    name: z.string().max(200),
    // --- SDR-Trunk alias appearance/behaviour ---
    // rdio serialises unset fields as null (e.g. led: null), so every modelled
    // optional field must accept null (`.nullish()` = value | null | undefined) —
    // otherwise ONE null field drops the WHOLE agencies array on read.
    color: z.string().max(40).nullish(),
    iconName: z.string().max(200).nullish(),
    // -1 = do-not-monitor, 1..99 = priority, undefined/100 = normal monitor.
    priority: z.number().int().nullish(),
    streamTalkgroupAlias: z.union([z.string().max(40), z.number()]).nullish(),
    // The alias's identifier scope — talkgroupRange / talkgroup / radio ids that
    // decide which calls stream for this agency (the "streaming ranges" section).
    aliasIds: z.array(AliasIdSchema).max(4096).default([]),
    // --- rdio system fields (id/label derive from systemId/name) ---
    led: z.string().max(40).nullish(),
    autoPopulate: z.boolean().nullish(),
    transcribe: z.boolean().nullish(),
    transcriptionPrompt: z.string().nullish(),
    blacklists: z.string().nullish(),
    delay: z.number().nullish(),
    alert: z.string().max(40).nullish(),
    talkgroups: z.array(LooseObj).max(8192).default([]),
    units: z.array(LooseObj).max(8192).default([]),
  })
  // passthrough so any extra rdio system field we don't model round-trips.
  .passthrough();
export type Agency = z.infer<typeof AgencySchema>;

export const GlobalConfigSchema = z
  .object({
    agencies: z.array(AgencySchema).max(4096).default([]),
    rdioGroups: z.array(LooseObj).max(4096).default([]),
    rdioTags: z.array(LooseObj).max(4096).default([]),
  })
  .strict();
export type GlobalConfigInput = z.infer<typeof GlobalConfigSchema>;

export interface GlobalConfig extends GlobalConfigInput {
  version: string;
  updatedAt: string | null;
  updatedBy: string | null;
  /** DERIVED, read-only: the stream names defined in the preset playlist
   *  (`<stream ... name="X">`). An alias's broadcastChannel must match one of
   *  these exactly to route. Output-only — NOT stored in the DB and NOT part of
   *  the PUT/save schema. */
  streamNames: string[];
}

// ── versioning ───────────────────────────────────────────────────────────────

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

/** sha256 over the content (not the version/metadata) so the version changes iff
 *  the actual config changes. Exposed so configMerge can fold it into a node's
 *  own config version. */
export function globalConfigVersion(c: GlobalConfigInput): string {
  const canon = canonicalize({
    agencies: c.agencies,
    rdioGroups: c.rdioGroups,
    rdioTags: c.rdioTags,
  });
  return createHash('sha256').update(JSON.stringify(canon)).digest('hex');
}

// ── agency <-> alias/system derivation ───────────────────────────────────────
// Agencies are the single source of truth. From each agency we DERIVE the
// SDR-Trunk alias + the rdio system that the rest of the pipeline (configMerge,
// agent) already consumes — so nothing downstream changed.

type AliasId = z.infer<typeof AliasIdSchema>;

/** Build the SDR-Trunk alias for an agency, or null when the agency has no
 *  identifier scope (nothing to match → no alias needed). The list is auto-set,
 *  the broadcastChannel + priority ids are synthesised from the agency. */
function agencyToAlias(a: Agency): Alias | null {
  if (!a.aliasIds || a.aliasIds.length === 0) return null;
  const ids: AliasId[] = [
    { type: 'priority', attrs: { priority: String(a.priority ?? 100) } },
    { type: 'broadcastChannel', attrs: { channel: a.name } },
    ...a.aliasIds,
  ];
  return {
    name: a.name,
    list: ALIAS_LIST_NAME,
    group: a.name,
    // rdio-derived fields can be null; the Alias shape wants string | undefined.
    color: a.color ?? undefined,
    iconName: a.iconName ?? undefined,
    streamTalkgroupAlias: a.streamTalkgroupAlias ?? undefined,
    ids,
  };
}

/** The SDR-Trunk aliases for all agencies (agencies with an identifier scope). */
export function agenciesToAliases(agencies: Agency[]): Alias[] {
  return agencies.map(agencyToAlias).filter((a): a is Alias => a !== null);
}

/** Build the rdio system doc for an agency: id/label from systemId/name, all
 *  other rdio fields (led/talkgroups/units/…) carried through; alias-only fields
 *  stripped. */
function agencyToSystem(a: Agency): Record<string, unknown> {
  const {
    systemId,
    name,
    color: _color,
    iconName: _iconName,
    priority: _priority,
    streamTalkgroupAlias: _sta,
    aliasIds: _aliasIds,
    id: _strayId,
    _id: _strayUnderId,
    label: _strayLabel,
    ...rest
  } = a as Agency & Record<string, unknown>;
  return { ...rest, id: systemId, label: name };
}

/** The rdio systems for all agencies. */
export function agenciesToSystems(agencies: Agency[]): Record<string, unknown>[] {
  return agencies.map(agencyToSystem);
}

/** Build agencies by merging rdio systems with their matching SDR-Trunk alias.
 *  The link is broadcastChannel == system label (both == the agency name, which
 *  we unified). Systems with no alias become agencies with no streaming scope. */
export function buildAgencies(aliases: Alias[], systems: Record<string, unknown>[]): Agency[] {
  const aliasByChannel = new Map<string, Alias>();
  for (const al of aliases) {
    const bc = al.ids.find((id) => id.type === 'broadcastChannel')?.attrs['channel'];
    if (bc) aliasByChannel.set(bc, al);
  }
  return systems.map((s): Agency => {
    const systemId = Number(s['id'] ?? s['_id']);
    const name = String(s['label'] ?? `System ${systemId}`);
    const { id: _id, _id: _underId, label: _label, ...sysRest } = s;
    const agency: Agency = {
      systemId,
      name,
      ...sysRest,
      talkgroups: Array.isArray(s['talkgroups']) ? (s['talkgroups'] as Record<string, unknown>[]) : [],
      units: Array.isArray(s['units']) ? (s['units'] as Record<string, unknown>[]) : [],
      aliasIds: [],
    };
    const alias = aliasByChannel.get(name);
    if (alias) {
      agency.color = alias.color;
      agency.iconName = alias.iconName;
      agency.streamTalkgroupAlias = alias.streamTalkgroupAlias;
      const prio = alias.ids.find((id) => id.type === 'priority')?.attrs['priority'];
      if (prio !== undefined && prio !== '') agency.priority = Number(prio);
      agency.aliasIds = alias.ids.filter(
        (id) => id.type !== 'priority' && id.type !== 'broadcastChannel',
      );
    }
    return agency;
  });
}

// ── alias XML parsing (seed from default.xml) ────────────────────────────────

function decodeEntities(s: string): string {
  return s
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, '&');
}

function parseAttrs(s: string): Record<string, string> {
  const out: Record<string, string> = {};
  const re = /([\w:.-]+)\s*=\s*"([^"]*)"/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(s)) !== null) {
    const k = m[1];
    const v = m[2];
    if (k !== undefined && v !== undefined) out[k] = decodeEntities(v);
  }
  return out;
}

/** Parse the `<alias>` blocks out of an SDR-Trunk playlist (default.xml) into
 *  the structured Alias[] the editor works with. Handles both the paired
 *  `<alias>…</alias>` form (with `<id>` children) and self-closing aliases. */
export function parseAliasesFromXml(xml: string): Alias[] {
  const aliases: Alias[] = [];
  const blockRe = /<alias\b([^>]*?)(?:\/>|>([\s\S]*?)<\/alias>)/g;
  let bm: RegExpExecArray | null;
  while ((bm = blockRe.exec(xml)) !== null) {
    const attrs = parseAttrs(bm[1] ?? '');
    const inner = bm[2] ?? '';
    const ids: Alias['ids'] = [];
    const idRe = /<id\b([^>]*?)\/>/g;
    let im: RegExpExecArray | null;
    while ((im = idRe.exec(inner)) !== null) {
      const a = parseAttrs(im[1] ?? '');
      const type = a['type'] ?? '';
      delete a['type'];
      ids.push({ type, attrs: a });
    }
    aliases.push({
      name: attrs['name'] ?? '',
      list: attrs['list'],
      group: attrs['group'],
      color: attrs['color'],
      iconName: attrs['iconName'],
      streamTalkgroupAlias: attrs['stream_talkgroup_alias'],
      ids,
    });
  }
  return aliases;
}

/** Parse the `name="..."` of every `<stream>` element in an SDR-Trunk playlist
 *  (default.xml). These are the rdio stream names an alias's broadcastChannel
 *  must match exactly. Returns unique names in document order. */
export function parseStreamNamesFromXml(xml: string): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  // Match each `<stream ...>` opening tag ([^>]* stops at the tag's own `>`).
  const re = /<stream\b([^>]*)>/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    const name = parseAttrs(m[1] ?? '')['name'];
    if (name != null && name !== '' && !seen.has(name)) {
      seen.add(name);
      out.push(name);
    }
  }
  return out;
}

/** DERIVED stream names from the on-disk preset playlist. Best-effort: never
 *  throws, returns [] if the presets can't be read. */
function presetStreamNames(): string[] {
  try {
    return parseStreamNamesFromXml(loadPresets().playlistXml);
  } catch (err) {
    log.warn({ err }, 'failed to derive stream names from presets');
    return [];
  }
}

/** The seed config, derived from the on-disk presets. */
function seedFromPresets(): GlobalConfigInput {
  const presets = loadPresets();
  const rdio = presets.rdio as Record<string, unknown>;
  const asArr = (v: unknown): Record<string, unknown>[] =>
    Array.isArray(v) ? (v as Record<string, unknown>[]) : [];
  const aliases = parseAliasesFromXml(presets.playlistXml);
  const systems = asArr(rdio['systems']);
  return {
    agencies: buildAgencies(aliases, systems),
    rdioGroups: asArr(rdio['groups']),
    rdioTags: asArr(rdio['tags']),
  };
}

// ── read / write ─────────────────────────────────────────────────────────────

interface Row {
  agencies: unknown;
  // Legacy columns — kept only for back-compat reads of rows written before the
  // agencies model (derived into agencies on read; cleared on the next save).
  sdrtrunk_aliases: unknown;
  rdio_systems: unknown;
  rdio_groups: unknown;
  rdio_tags: unknown;
  version: string;
  updated_at: string | null;
  updated_by: string | null;
}

function rowToConfig(r: Row): GlobalConfig {
  let agencies: unknown = r.agencies;
  // Back-compat: a row written before the agencies model has an empty `agencies`
  // but populated legacy alias/system columns — derive agencies from them so a
  // pre-reseed config keeps working.
  if (!Array.isArray(agencies) || agencies.length === 0) {
    const legacyAliases = AliasSchema.array().safeParse(r.sdrtrunk_aliases ?? []);
    const legacySystems = Array.isArray(r.rdio_systems)
      ? (r.rdio_systems as Record<string, unknown>[])
      : [];
    if (legacyAliases.success && (legacyAliases.data.length > 0 || legacySystems.length > 0)) {
      agencies = buildAgencies(legacyAliases.data, legacySystems);
    }
  }
  const parsed = GlobalConfigSchema.safeParse({
    agencies: agencies ?? [],
    rdioGroups: r.rdio_groups ?? [],
    rdioTags: r.rdio_tags ?? [],
  });
  const content: GlobalConfigInput = parsed.success
    ? parsed.data
    : { agencies: [], rdioGroups: [], rdioTags: [] };
  return {
    ...content,
    version: r.version || globalConfigVersion(content),
    updatedAt: r.updated_at,
    updatedBy: r.updated_by,
    streamNames: presetStreamNames(),
  };
}

const EMPTY: GlobalConfig = {
  agencies: [],
  rdioGroups: [],
  rdioTags: [],
  version: globalConfigVersion({ agencies: [], rdioGroups: [], rdioTags: [] }),
  updatedAt: null,
  updatedBy: null,
  streamNames: [],
};

/**
 * Read the global config, seeding it from presets on first read while empty.
 * Returns an empty config (never throws) when the DB is unavailable so config
 * builds degrade gracefully.
 */
export async function getGlobalConfig(): Promise<GlobalConfig> {
  const pool = await getPool();
  if (!pool) return { ...EMPTY, streamNames: presetStreamNames() };
  const res = await pool.query<Row>(
    `SELECT agencies, sdrtrunk_aliases, rdio_systems, rdio_groups, rdio_tags, version, updated_at, updated_by
       FROM feeder_global_config WHERE id = 1`,
  );
  const row = res.rows[0];
  // Seed from presets ONLY when the singleton row is absent (a genuine first
  // run). A row that EXISTS but is empty is a deliberate clear by the owner —
  // return it as-is rather than re-seeding, so the fleet config can actually be
  // emptied and a read never silently turns into a write (which also raced /
  // clobbered a concurrent PUT and reverted deliberate clears).
  if (row) {
    return rowToConfig(row);
  }

  // Lazy seed from presets (row absent = first run). Best-effort: if presets are
  // unavailable, fall back to EMPTY.
  try {
    const seed = seedFromPresets();
    const saved = await saveGlobalConfig(seed, 'system:seed');
    log.info({ agencies: seed.agencies.length }, 'seeded global feeder config from presets');
    return saved;
  } catch (err) {
    log.warn({ err }, 'global feeder config seed from presets failed');
    return row ? rowToConfig(row) : EMPTY;
  }
}

/**
 * Global auto-update switch. True (default) = nodes self-update automatically;
 * false = automatic checks are paused (manual updates still work). Stored on the
 * feeder_global_config singleton, independent of the radio config.
 */
export async function getAutoUpdate(): Promise<boolean> {
  const pool = await getPool();
  if (!pool) return true;
  const res = await pool.query<{ auto_update: boolean }>(
    'SELECT auto_update FROM feeder_global_config WHERE id = 1',
  );
  return res.rows[0]?.auto_update ?? true;
}

/** Set the global auto-update switch, upserting the singleton row without
 *  touching the radio config columns. */
export async function setAutoUpdate(enabled: boolean): Promise<void> {
  const pool = await getPool();
  if (!pool) return;
  await pool.query(
    `INSERT INTO feeder_global_config (id, auto_update) VALUES (1, $1)
     ON CONFLICT (id) DO UPDATE SET auto_update = EXCLUDED.auto_update`,
    [enabled],
  );
}

/** Upsert the singleton config with a freshly-computed version. */
export async function saveGlobalConfig(
  input: GlobalConfigInput,
  updatedBy: string | null,
): Promise<GlobalConfig> {
  const pool = await getPool();
  const version = globalConfigVersion(input);
  if (!pool) {
    return { ...input, version, updatedAt: null, updatedBy, streamNames: presetStreamNames() };
  }
  const res = await pool.query<Row>(
    `INSERT INTO feeder_global_config
       (id, agencies, sdrtrunk_aliases, rdio_systems, rdio_groups, rdio_tags, version, updated_at, updated_by)
     VALUES (1, $1, '[]'::jsonb, '[]'::jsonb, $2, $3, $4, now(), $5)
     ON CONFLICT (id) DO UPDATE SET
       agencies         = EXCLUDED.agencies,
       sdrtrunk_aliases = '[]'::jsonb,
       rdio_systems     = '[]'::jsonb,
       rdio_groups      = EXCLUDED.rdio_groups,
       rdio_tags        = EXCLUDED.rdio_tags,
       version          = EXCLUDED.version,
       updated_at       = now(),
       updated_by       = EXCLUDED.updated_by
     RETURNING agencies, sdrtrunk_aliases, rdio_systems, rdio_groups, rdio_tags, version, updated_at, updated_by`,
    [
      JSON.stringify(input.agencies),
      JSON.stringify(input.rdioGroups),
      JSON.stringify(input.rdioTags),
      version,
      updatedBy,
    ],
  );
  const saved = res.rows[0];
  return saved ? rowToConfig(saved) : { ...input, version, updatedAt: null, updatedBy, streamNames: presetStreamNames() };
}
