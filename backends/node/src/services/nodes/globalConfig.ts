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
    ids: z.array(AliasIdSchema).max(4096).default([]),
  })
  .strict();
export type Alias = z.infer<typeof AliasSchema>;

// rdio systems/groups/tags are complex documents; validate loosely (preserve
// every field) so the round-trip through the editor never drops data.
const LooseObj = z.record(z.string(), z.unknown());

export const GlobalConfigSchema = z
  .object({
    sdrtrunkAliases: z.array(AliasSchema).max(8192).default([]),
    rdioSystems: z.array(LooseObj).max(4096).default([]),
    rdioGroups: z.array(LooseObj).max(4096).default([]),
    rdioTags: z.array(LooseObj).max(4096).default([]),
  })
  .strict();
export type GlobalConfigInput = z.infer<typeof GlobalConfigSchema>;

export interface GlobalConfig extends GlobalConfigInput {
  version: string;
  updatedAt: string | null;
  updatedBy: string | null;
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
    sdrtrunkAliases: c.sdrtrunkAliases,
    rdioSystems: c.rdioSystems,
    rdioGroups: c.rdioGroups,
    rdioTags: c.rdioTags,
  });
  return createHash('sha256').update(JSON.stringify(canon)).digest('hex');
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
      ids,
    });
  }
  return aliases;
}

/** The seed config, derived from the on-disk presets. */
function seedFromPresets(): GlobalConfigInput {
  const presets = loadPresets();
  const rdio = presets.rdio as Record<string, unknown>;
  const asArr = (v: unknown): Record<string, unknown>[] =>
    Array.isArray(v) ? (v as Record<string, unknown>[]) : [];
  return {
    sdrtrunkAliases: parseAliasesFromXml(presets.playlistXml),
    rdioSystems: asArr(rdio['systems']),
    rdioGroups: asArr(rdio['groups']),
    rdioTags: asArr(rdio['tags']),
  };
}

// ── read / write ─────────────────────────────────────────────────────────────

interface Row {
  sdrtrunk_aliases: unknown;
  rdio_systems: unknown;
  rdio_groups: unknown;
  rdio_tags: unknown;
  version: string;
  updated_at: string | null;
  updated_by: string | null;
}

function rowToConfig(r: Row): GlobalConfig {
  const parsed = GlobalConfigSchema.safeParse({
    sdrtrunkAliases: r.sdrtrunk_aliases ?? [],
    rdioSystems: r.rdio_systems ?? [],
    rdioGroups: r.rdio_groups ?? [],
    rdioTags: r.rdio_tags ?? [],
  });
  const content: GlobalConfigInput = parsed.success
    ? parsed.data
    : { sdrtrunkAliases: [], rdioSystems: [], rdioGroups: [], rdioTags: [] };
  return {
    ...content,
    version: r.version || globalConfigVersion(content),
    updatedAt: r.updated_at,
    updatedBy: r.updated_by,
  };
}

const EMPTY: GlobalConfig = {
  sdrtrunkAliases: [],
  rdioSystems: [],
  rdioGroups: [],
  rdioTags: [],
  version: globalConfigVersion({
    sdrtrunkAliases: [],
    rdioSystems: [],
    rdioGroups: [],
    rdioTags: [],
  }),
  updatedAt: null,
  updatedBy: null,
};

/**
 * Read the global config, seeding it from presets on first read while empty.
 * Returns an empty config (never throws) when the DB is unavailable so config
 * builds degrade gracefully.
 */
export async function getGlobalConfig(): Promise<GlobalConfig> {
  const pool = await getPool();
  if (!pool) return EMPTY;
  const res = await pool.query<Row>(
    `SELECT sdrtrunk_aliases, rdio_systems, rdio_groups, rdio_tags, version, updated_at, updated_by
       FROM feeder_global_config WHERE id = 1`,
  );
  const row = res.rows[0];
  if (
    row &&
    ((Array.isArray(row.sdrtrunk_aliases) && row.sdrtrunk_aliases.length > 0) ||
      (Array.isArray(row.rdio_systems) && row.rdio_systems.length > 0))
  ) {
    return rowToConfig(row);
  }

  // Lazy seed from presets. Best-effort: if presets are unavailable, fall back
  // to whatever the row holds (or EMPTY).
  try {
    const seed = seedFromPresets();
    const saved = await saveGlobalConfig(seed, 'system:seed');
    log.info(
      { aliases: seed.sdrtrunkAliases.length, systems: seed.rdioSystems.length },
      'seeded global feeder config from presets',
    );
    return saved;
  } catch (err) {
    log.warn({ err }, 'global feeder config seed from presets failed');
    return row ? rowToConfig(row) : EMPTY;
  }
}

/** Upsert the singleton config with a freshly-computed version. */
export async function saveGlobalConfig(
  input: GlobalConfigInput,
  updatedBy: string | null,
): Promise<GlobalConfig> {
  const pool = await getPool();
  const version = globalConfigVersion(input);
  if (!pool) {
    return { ...input, version, updatedAt: null, updatedBy };
  }
  const res = await pool.query<Row>(
    `INSERT INTO feeder_global_config
       (id, sdrtrunk_aliases, rdio_systems, rdio_groups, rdio_tags, version, updated_at, updated_by)
     VALUES (1, $1, $2, $3, $4, $5, now(), $6)
     ON CONFLICT (id) DO UPDATE SET
       sdrtrunk_aliases = EXCLUDED.sdrtrunk_aliases,
       rdio_systems     = EXCLUDED.rdio_systems,
       rdio_groups      = EXCLUDED.rdio_groups,
       rdio_tags        = EXCLUDED.rdio_tags,
       version          = EXCLUDED.version,
       updated_at       = now(),
       updated_by       = EXCLUDED.updated_by
     RETURNING sdrtrunk_aliases, rdio_systems, rdio_groups, rdio_tags, version, updated_at, updated_by`,
    [
      JSON.stringify(input.sdrtrunkAliases),
      JSON.stringify(input.rdioSystems),
      JSON.stringify(input.rdioGroups),
      JSON.stringify(input.rdioTags),
      version,
      updatedBy,
    ],
  );
  const saved = res.rows[0];
  return saved ? rowToConfig(saved) : { ...input, version, updatedAt: null, updatedBy };
}
