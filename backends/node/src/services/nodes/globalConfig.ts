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
import { config } from '../../config.js';
import { loadPresets } from './configMerge.js';

// ── shapes ─────────────────────────────────────────────────────────────────

/** An XML attribute name is emitted verbatim into the SDR-Trunk playlist by the
 *  agent and interpolated into staff-panel event handlers, so it MUST be a safe
 *  XML NCName-ish token — never arbitrary text. Constraining it here (the single
 *  choke point all alias ids flow through) closes both the playlist-XML injection
 *  in the agent and the handler-arg XSS in the editor at the source. */
const AttrNameSchema = z
  .string()
  .min(1)
  .max(64)
  .regex(/^[A-Za-z_][A-Za-z0-9_.:-]*$/, 'invalid attribute name');

/** One `<id>` inside an SDR-Trunk alias, e.g. a talkgroup or priority. `type`
 *  is the SDR-Trunk id type; `attrs` preserves every other attribute verbatim
 *  so the agent can re-emit the element faithfully. Keys are constrained to safe
 *  XML attribute names; values are length-capped (they're XML-escaped downstream). */
export const AliasIdSchema = z
  .object({
    type: z.string().max(60),
    attrs: z.record(AttrNameSchema, z.string().max(512)).default({}),
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

/**
 * ONE talkgroup — the single row that drives BOTH programs. The rdio talkgroup
 * and the SDR-Trunk alias are DERIVED from this at push time, so a talkgroup
 * cannot exist on one side only and the two can never drift (they used to be
 * two independently-imported lists, which left ~600 talkgroups streaming audio
 * that rdio then dropped for having no matching talkgroup row).
 *
 * `groupId`/`tagId`/`priority` are null when the row INHERITS the agency
 * default — an explicit value overrides it. That's what lets an agency set its
 * group + tag once instead of on all 623 rows.
 *
 * `aliasName` is the SDR-Trunk alias name kept verbatim, because its leading
 * number is each agency's OWN channel numbering (RFS "1209 AVATN 1", FRNSW
 * "101 ME1", SES "0916 OPS 16", Endeavour "D01 CNTRLE1", Rail none at all) and
 * is NOT derivable from the talkgroup id. Null = the alias is named by `label`.
 *
 * Passthrough keeps the rdio fields we don't model (led, led2, order, _id)
 * round-tripping untouched.
 */
// The staff editor historically saved unset numeric fields as "" (e.g. a
// talkgroup added but never given a group), and that shape exists in stored
// configs — treat empty-string as unset rather than failing the row.
const optionalInt = z.preprocess((v) => (v === '' ? undefined : v), z.number().int().nullish());
const optionalNum = z.preprocess((v) => (v === '' ? undefined : v), z.number().nullish());

export const TalkgroupSchema = z
  .object({
    id: z.number().int(),
    label: z.string().max(200).nullish(),
    name: z.string().max(200).nullish(),
    groupId: optionalInt,
    tagId: optionalInt,
    frequency: optionalNum,
    delay: optionalNum,
    alert: z.string().max(40).nullish(),
    aliasName: z.string().max(200).nullish(),
    priority: optionalInt,
  })
  .passthrough();
export type Talkgroup = z.infer<typeof TalkgroupSchema>;

/** The single SDR-Trunk alias list every generated alias belongs to. Auto-set —
 *  there is one playlist, so operators never pick a list. The channel's
 *  alias_list_name must match this (preset ships it). */
export const ALIAS_LIST_NAME = 'NSWPSN';

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
    // Length-capped free text — this whole config is hashed and fanned out to
    // every node, so bound the fields that would otherwise be unbounded.
    transcriptionPrompt: z.string().max(4000).nullish(),
    blacklists: z.string().max(8000).nullish(),
    delay: z.number().nullish(),
    alert: z.string().max(40).nullish(),
    // Inherited by this agency's talkgroups whenever their own groupId/tagId is
    // null. 17 of 19 systems use a single group and 16 a single tag, so this is
    // the field that stops the same value being typed on every row.
    defaultGroupId: z.number().int().nullish(),
    defaultTagId: z.number().int().nullish(),
    // The SDR-Trunk alias `group` attribute, which is NOT the agency name:
    // FRNSW aliases group under "Fire & Rescue NSW" while the agency, system
    // label and stream are all "Fire and Rescue". Generating it from the name
    // would silently rename every alias in sdrtrunk-vce. Null = use the name.
    sdrGroupName: z.string().max(200).nullish(),
    // An ENCRYPTED agency is SDR-Trunk only: its talkgroups produce aliases (so
    // decode events still get a readable label) but no rdio system, no rdio
    // talkgroups, no stream and no broadcastChannel — there is never any audio
    // to upload. NSW PF (12001-12275) is the one such agency today.
    encrypted: z.boolean().nullish(),
    talkgroups: z.array(TalkgroupSchema).max(8192).default([]),
    units: z.array(LooseObj).max(8192).default([]),
  })
  // passthrough so any extra rdio system field we don't model round-trips.
  .passthrough();
export type Agency = z.infer<typeof AgencySchema>;

/** The sdrtrunk-vce config, imported from the operator's sdrtrunk SQLite and
 *  pushed to sdrtrunk-vce verbatim (NOT derived from the rdio talkgroups). Alias
 *  lists + aliases label/route talkgroups; streams carry the systemId that links
 *  a call to its rdio system. Aliases reuse the SDR-Trunk `AliasSchema` so they
 *  flow straight into the config payload the agent already consumes. */
export const SdrtrunkAliasListSchema = z
  .object({ name: z.string().max(200), family: z.string().max(20).default('P25') })
  .strict();
export const SdrtrunkStreamSchema = z
  .object({
    name: z.string().max(200),
    systemId: z.number().int(),
    enabled: z.boolean().default(true),
  })
  .strict();
export const SdrtrunkConfigSchema = z
  .object({
    aliasLists: z.array(SdrtrunkAliasListSchema).max(256).default([]),
    // The operator's list is ~1,500 aliases; cap generously.
    aliases: z.array(AliasSchema).max(20000).default([]),
    streams: z.array(SdrtrunkStreamSchema).max(4096).default([]),
  })
  .strict();
export type SdrtrunkConfig = z.infer<typeof SdrtrunkConfigSchema>;

const EMPTY_SDRTRUNK: SdrtrunkConfig = { aliasLists: [], aliases: [], streams: [] };

/** Fleet-wide talkgroup defaults, set once instead of per row/agency:
 *  `priority` is the SDR-Trunk monitor priority every agency inherits unless it
 *  overrides (production is uniformly -1 = do-not-monitor — headless nodes
 *  have no local audio to prioritise). Alias COLOUR is deliberately NOT here:
 *  it is per agency (each agency's one LED colour drives its aliases). */
export const TalkgroupDefaultsSchema = z
  .object({
    priority: z.number().int().nullish(),
  })
  .strict();
export type TalkgroupDefaults = z.infer<typeof TalkgroupDefaultsSchema>;

const EMPTY_DEFAULTS: TalkgroupDefaults = {};

export const GlobalConfigSchema = z
  .object({
    agencies: z.array(AgencySchema).max(4096).default([]),
    rdioGroups: z.array(LooseObj).max(4096).default([]),
    rdioTags: z.array(LooseObj).max(4096).default([]),
    sdrtrunkConfig: SdrtrunkConfigSchema.default(EMPTY_SDRTRUNK),
    defaults: TalkgroupDefaultsSchema.default(EMPTY_DEFAULTS),
  })
  .strict();
export type GlobalConfigInput = z.infer<typeof GlobalConfigSchema>;

/** Partial form for PATCH-style saves: each program's config is imported
 *  independently, so a PUT may carry only `agencies` (rdio) or only
 *  `sdrtrunkConfig` and must leave the other side untouched. */
export const GlobalConfigPatchSchema = z
  .object({
    agencies: z.array(AgencySchema).max(4096),
    rdioGroups: z.array(LooseObj).max(4096),
    rdioTags: z.array(LooseObj).max(4096),
    sdrtrunkConfig: SdrtrunkConfigSchema,
    defaults: TalkgroupDefaultsSchema,
  })
  .partial()
  .strict();
export type GlobalConfigPatch = z.infer<typeof GlobalConfigPatchSchema>;

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
export function globalConfigVersion(c: {
  agencies: unknown;
  rdioGroups: unknown;
  rdioTags: unknown;
  sdrtrunkConfig?: unknown;
  defaults?: unknown;
}): string {
  const canon = canonicalize({
    agencies: c.agencies,
    rdioGroups: c.rdioGroups,
    rdioTags: c.rdioTags,
    // Folded in so a sdrtrunk-only import bumps the version and re-syncs the fleet.
    sdrtrunkConfig: c.sdrtrunkConfig ?? EMPTY_SDRTRUNK,
    // Colour/priority feed the derived aliases, so changing them must re-sync too.
    defaults: c.defaults ?? EMPTY_DEFAULTS,
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
    // Route to the agency's stream. The stream name is the TRIMMED agency name
    // (see streamTargets/agenciesToSystems), so the broadcastChannel must be
    // trimmed too or a stray space (e.g. "NPWS ") silently breaks routing.
    { type: 'broadcastChannel', attrs: { channel: a.name.trim() } },
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

/** rdio LED preset name -> SDR-Trunk alias colour (signed ARGB int string).
 *  Hues match the staff editor's LED swatches so the alias colour in vce agrees
 *  with the LED dot the operator picked. Agencies store an explicit `color`
 *  when set via the editor; this covers configs that only carry `led`. */
const LED_ALIAS_ARGB: Record<string, string> = {};
for (const [led, hex] of Object.entries({
  blue: '#2563eb', cyan: '#06b6d4', green: '#22c55e', magenta: '#d946ef',
  orange: '#f97316', red: '#ef4444', white: '#e5e7eb', yellow: '#eab308',
})) {
  const rgb = Number.parseInt(hex.slice(1), 16);
  LED_ALIAS_ARGB[led] = String(((0xff << 24) | rgb) | 0);
}

function ledToAliasColor(led: unknown): string | null {
  if (typeof led !== 'string') return null;
  return LED_ALIAS_ARGB[led.trim().toLowerCase()] ?? null;
}

/**
 * The SDR-Trunk aliases DERIVED from the unified talkgroup rows — one alias per
 * talkgroup, every agency. This replaces pushing the imported
 * `sdrtrunkConfig.aliases` verbatim (configMerge falls back to that legacy list
 * whenever it is still non-empty, so pre-merge configs are untouched).
 *
 * Field resolution, most-specific wins:
 *   name     = aliasName ?? label ?? name ?? String(id)   (aliasName carries the
 *              agency's own channel numbering and is never generated)
 *   group    = agency.sdrGroupName ?? agency.name  (sdrGroupName exists because
 *              the historical alias group is NOT the agency name)
 *   color    = agency.color ?? agency's LED colour (each agency has ONE colour
 *              that drives both its rdio LED and its aliases; there is no
 *              fleet-wide colour)
 *   priority = row.priority ?? agency.priority ?? defaults.priority ?? 100
 *   route    = broadcastChannel -> trimmed agency name, OMITTED for encrypted
 *              agencies (no stream, nothing ever uploads)
 */
export function deriveAliasesFromTalkgroups(
  agencies: Agency[],
  defaults: TalkgroupDefaults,
): Alias[] {
  const out: Alias[] = [];
  for (const a of agencies) {
    const group = (a.sdrGroupName ?? a.name).trim();
    const channel = a.name.trim();
    const color = a.color ?? ledToAliasColor((a as Record<string, unknown>)['led']);
    for (const tg of a.talkgroups) {
      const row = tg as Talkgroup;
      if (typeof row.id !== 'number' || !Number.isInteger(row.id)) continue;
      const name =
        (row.aliasName ?? '').trim() ||
        (row.label ?? '').trim() ||
        (row.name ?? '').trim() ||
        String(row.id);
      const priority = row.priority ?? a.priority ?? defaults.priority ?? 100;
      const ids: AliasId[] = [
        { type: 'priority', attrs: { priority: String(priority) } },
        { type: 'talkgroup', attrs: { value: String(row.id), protocol: 'APCO25' } },
      ];
      if (!a.encrypted) {
        ids.push({ type: 'broadcastChannel', attrs: { channel } });
      }
      out.push({
        name,
        list: ALIAS_LIST_NAME,
        group,
        color: color ?? undefined,
        ids,
      });
    }
  }
  return out;
}

/** Build the rdio system doc for an agency: id/label from systemId/name, all
 *  other rdio fields (led/talkgroups/units/…) carried through; alias-only and
 *  unified-model fields stripped. Talkgroup rows materialise the agency
 *  defaults — rdio wants a concrete groupId/tagId on every row, so the
 *  null-means-inherit convention stops here. */
function agencyToSystem(a: Agency): Record<string, unknown> {
  const {
    systemId,
    name,
    color: _color,
    iconName: _iconName,
    priority: _priority,
    streamTalkgroupAlias: _sta,
    aliasIds: _aliasIds,
    defaultGroupId: _dg,
    defaultTagId: _dt,
    sdrGroupName: _sgn,
    encrypted: _enc,
    talkgroups,
    id: _strayId,
    _id: _strayUnderId,
    label: _strayLabel,
    ...rest
  } = a as Agency & Record<string, unknown>;
  const rdioTalkgroups = (talkgroups as Talkgroup[]).map((tg) => {
    const { aliasName: _an, priority: _pr, groupId, tagId, ...tgRest } = tg;
    return {
      ...tgRest,
      groupId: groupId ?? a.defaultGroupId ?? undefined,
      tagId: tagId ?? a.defaultTagId ?? undefined,
    };
  });
  return { ...rest, talkgroups: rdioTalkgroups, id: systemId, label: name };
}

/**
/**
 * The rdio systems for all agencies. Each agency owns a unique systemId (RFS=1,
 * FRNSW=2, …), so this is normally a 1:1 map. rdio-scanner requires unique system
 * ids and 500s the admin config PUT with a UNIQUE constraint on
 * rdioScannerSystems.id if two ever collide, so a duplicate systemId — which can
 * only come from bad global-config data — is dropped (first wins) and logged
 * rather than allowed to break the whole fleet's rdio apply.
 */
export function agenciesToSystems(agencies: Agency[]): Record<string, unknown>[] {
  const seen = new Set<number>();
  const out: Record<string, unknown>[] = [];
  const dropped: number[] = [];

  for (const agency of agencies) {
    // Encrypted agencies are SDR-Trunk only: aliases label their decode events,
    // but no audio can ever arrive, so no rdio system exists for them.
    if (agency.encrypted) continue;
    const sys = agencyToSystem(agency);
    const id = sys.id;
    if (typeof id !== 'number' || Number.isNaN(id)) {
      dropped.push(NaN);
      continue;
    }
    if (seen.has(id)) {
      dropped.push(id);
      continue;
    }
    seen.add(id);
    out.push(sys);
  }

  if (dropped.length > 0) {
    log.warn(
      { duplicateSystemIds: dropped },
      'agenciesToSystems: dropped agencies with a duplicate/invalid systemId — ' +
        'each agency must have a unique systemId; fix the duplicate in the global config',
    );
  }

  return out;
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
      // Legacy rdio talkgroups have an integer id; rows missing one can't be
      // addressed by the unified model, so they're dropped here (keepValid does
      // the same on read).
      talkgroups: Array.isArray(s['talkgroups'])
        ? (s['talkgroups'] as Record<string, unknown>[]).filter(
            (t): t is Talkgroup => typeof t['id'] === 'number' && Number.isInteger(t['id']),
          )
        : [],
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
    // The sdrtrunk-vce alias lists/streams are imported by the operator, not seeded.
    sdrtrunkConfig: EMPTY_SDRTRUNK,
    defaults: EMPTY_DEFAULTS,
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
  // The sdrtrunk-vce config (alias lists + aliases + streams), imported separately.
  sdrtrunk_config: unknown;
  // Fleet-wide talkgroup defaults (global alias colour + monitor priority).
  talkgroup_defaults: unknown;
  version: string;
  updated_at: string | null;
  updated_by: string | null;
}

// Element-wise parse: keep every item that validates on its own, drop the rest.
// Used to recover a mostly-valid config instead of collapsing the whole document
// to empty when a single element is malformed.
function keepValid<T>(schema: z.ZodType<T>, arr: unknown): T[] {
  if (!Array.isArray(arr)) return [];
  const out: T[] = [];
  for (const el of arr) {
    const r = schema.safeParse(el);
    if (r.success) out.push(r.data);
  }
  return out;
}

function salvageConfig(
  agencies: unknown,
  rdioGroups: unknown,
  rdioTags: unknown,
): GlobalConfigInput {
  return {
    agencies: keepValid(AgencySchema, agencies),
    // rdioGroups/rdioTags are the loose element type used in GlobalConfigSchema.
    rdioGroups: keepValid(LooseObj, rdioGroups),
    rdioTags: keepValid(LooseObj, rdioTags),
    // sdrtrunkConfig/defaults are salvaged separately by the caller (rowToConfig).
    sdrtrunkConfig: EMPTY_SDRTRUNK,
    defaults: EMPTY_DEFAULTS,
  };
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
  // The sdrtrunk-vce config is independent of the rdio (agencies) side; parse it
  // on its own so a malformed alias can't wipe the rdio config and vice-versa.
  const sdrtrunkParsed = SdrtrunkConfigSchema.safeParse(r.sdrtrunk_config ?? EMPTY_SDRTRUNK);
  const sdrtrunkConfig: SdrtrunkConfig = sdrtrunkParsed.success
    ? sdrtrunkParsed.data
    : {
        aliasLists: keepValid(SdrtrunkAliasListSchema, (r.sdrtrunk_config as { aliasLists?: unknown })?.aliasLists),
        aliases: keepValid(AliasSchema, (r.sdrtrunk_config as { aliases?: unknown })?.aliases),
        streams: keepValid(SdrtrunkStreamSchema, (r.sdrtrunk_config as { streams?: unknown })?.streams),
      };
  if (!sdrtrunkParsed.success) {
    log.error(
      { issues: sdrtrunkParsed.error.issues.slice(0, 20), aliases: sdrtrunkConfig.aliases.length },
      'sdrtrunk config failed schema on read; salvaged valid elements',
    );
  }
  // Defaults are tiny and additive — a malformed value falls back to empty
  // rather than dragging the rest of the config down with it.
  const defaultsParsed = TalkgroupDefaultsSchema.safeParse(r.talkgroup_defaults ?? EMPTY_DEFAULTS);
  const defaults: TalkgroupDefaults = defaultsParsed.success ? defaultsParsed.data : EMPTY_DEFAULTS;
  const parsed = GlobalConfigSchema.safeParse({
    agencies: agencies ?? [],
    rdioGroups: r.rdio_groups ?? [],
    rdioTags: r.rdio_tags ?? [],
    sdrtrunkConfig,
    defaults,
  });
  let content: GlobalConfigInput;
  if (parsed.success) {
    content = parsed.data;
  } else {
    // Fail SOFT, not empty: the whole config is all-or-nothing under the schema,
    // so one malformed agency would otherwise wipe (and then re-persist + fan out)
    // every agency/group/tag fleet-wide. Instead salvage the valid elements and
    // drop only the offenders, logging what was rejected so it's visible.
    content = { ...salvageConfig(agencies ?? [], r.rdio_groups ?? [], r.rdio_tags ?? []), sdrtrunkConfig, defaults };
    log.error(
      { issues: parsed.error.issues.slice(0, 20), kept: content.agencies.length },
      'global config failed schema on read; salvaged valid elements (dropped offenders)',
    );
  }
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
  sdrtrunkConfig: EMPTY_SDRTRUNK,
  defaults: EMPTY_DEFAULTS,
  version: globalConfigVersion({ agencies: [], rdioGroups: [], rdioTags: [], sdrtrunkConfig: EMPTY_SDRTRUNK, defaults: EMPTY_DEFAULTS }),
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
    `SELECT agencies, sdrtrunk_aliases, rdio_systems, rdio_groups, rdio_tags, sdrtrunk_config, talkgroup_defaults, version, updated_at, updated_by
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

// ── Pagermon ingest target (SERVER-ONLY secret) ──────────────────────────────
// The central Pagermon the pager-node relay forwards INTO. Stored on the
// singleton row (owner-set in staff) so it's editable without a redeploy, but
// deliberately kept OUT of GlobalConfigSchema / buildConfigPayload — it is never
// fanned out to agents. Falls back to env (config.PAGERMON_INGEST_*).

export interface PagerIngest {
  url: string | null;
  apiKey: string | null;
}

/** Read the Pagermon ingest URL + key: DB row first, then env fallback. Never
 *  throws — returns nulls when the DB is down and env is unset. */
export async function getPagerIngest(): Promise<PagerIngest> {
  const envUrl = config.PAGERMON_INGEST_URL ?? null;
  const envKey = config.PAGERMON_INGEST_API_KEY ?? null;
  const pool = await getPool();
  if (!pool) return { url: envUrl, apiKey: envKey };
  try {
    const res = await pool.query<{ pagermon_ingest_url: string | null; pagermon_ingest_api_key: string | null }>(
      'SELECT pagermon_ingest_url, pagermon_ingest_api_key FROM feeder_global_config WHERE id = 1',
    );
    const row = res.rows[0];
    return {
      url: (row?.pagermon_ingest_url ?? null) || envUrl,
      apiKey: (row?.pagermon_ingest_api_key ?? null) || envKey,
    };
  } catch (err) {
    log.warn({ err }, 'getPagerIngest: DB read failed, using env fallback');
    return { url: envUrl, apiKey: envKey };
  }
}

/** Upsert the Pagermon ingest URL + key on the singleton row, without touching
 *  the radio config columns. A null field clears that column (falls back to env).
 *  Pass `apiKey === undefined` to leave the stored key unchanged. */
export async function setPagerIngest(url: string | null, apiKey?: string | null): Promise<void> {
  const pool = await getPool();
  if (!pool) return;
  if (apiKey === undefined) {
    await pool.query(
      `INSERT INTO feeder_global_config (id, pagermon_ingest_url) VALUES (1, $1)
       ON CONFLICT (id) DO UPDATE SET pagermon_ingest_url = EXCLUDED.pagermon_ingest_url`,
      [url],
    );
    return;
  }
  await pool.query(
    `INSERT INTO feeder_global_config (id, pagermon_ingest_url, pagermon_ingest_api_key)
       VALUES (1, $1, $2)
     ON CONFLICT (id) DO UPDATE SET
       pagermon_ingest_url     = EXCLUDED.pagermon_ingest_url,
       pagermon_ingest_api_key = EXCLUDED.pagermon_ingest_api_key`,
    [url, apiKey],
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
       (id, agencies, sdrtrunk_aliases, rdio_systems, rdio_groups, rdio_tags, sdrtrunk_config, talkgroup_defaults, version, updated_at, updated_by)
     VALUES (1, $1, '[]'::jsonb, '[]'::jsonb, $2, $3, $4, $5, $6, now(), $7)
     ON CONFLICT (id) DO UPDATE SET
       agencies           = EXCLUDED.agencies,
       sdrtrunk_aliases   = '[]'::jsonb,
       rdio_systems       = '[]'::jsonb,
       rdio_groups        = EXCLUDED.rdio_groups,
       rdio_tags          = EXCLUDED.rdio_tags,
       sdrtrunk_config    = EXCLUDED.sdrtrunk_config,
       talkgroup_defaults = EXCLUDED.talkgroup_defaults,
       version            = EXCLUDED.version,
       updated_at         = now(),
       updated_by         = EXCLUDED.updated_by
     RETURNING agencies, sdrtrunk_aliases, rdio_systems, rdio_groups, rdio_tags, sdrtrunk_config, talkgroup_defaults, version, updated_at, updated_by`,
    [
      JSON.stringify(input.agencies),
      JSON.stringify(input.rdioGroups),
      JSON.stringify(input.rdioTags),
      JSON.stringify(input.sdrtrunkConfig ?? EMPTY_SDRTRUNK),
      JSON.stringify(input.defaults ?? EMPTY_DEFAULTS),
      version,
      updatedBy,
    ],
  );
  const saved = res.rows[0];
  return saved ? rowToConfig(saved) : { ...input, version, updatedAt: null, updatedBy, streamNames: presetStreamNames() };
}
