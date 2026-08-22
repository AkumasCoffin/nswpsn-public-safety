/**
 * Talkgroup id → label / owning agency / display colour, resolved from the
 * imported sdrtrunk-vce alias list (the single source of truth already pushed
 * to nodes). Ingest stores no labels, so every read path resolves them here.
 *
 * This lives in services/ rather than inside the Data-tab router because both
 * the REST routes (api/node-data.ts) and the live WebSocket push
 * (services/nodeLive.ts) need it. Importing it back out of the router would
 * be a cycle, and a second copy would drift.
 */
import { log } from '../lib/log.js';
import { getGlobalConfig, deriveAliasesFromTalkgroups } from './nodes/globalConfig.js';

export interface TalkgroupCatalog {
  labels: Map<number, string>;
  agencies: Map<number, string>;
  colors: Map<number, string>;
  /** Talkgroups of agencies with the encrypted toggle on (SDR-Trunk-only
   *  agencies like NSW PF): every call on them is encrypted regardless of what
   *  any single decode event managed to establish in time. Empty until the
   *  unified-talkgroup merge has run. */
  encrypted: Set<number>;
}

let _cache: { at: number; map: TalkgroupCatalog } | null = null;

/**
 * Alias colour -> a CSS hex string, or null if it isn't one.
 *
 * SDR-Trunk stores an alias colour as a Java Color RGB **integer**, and the
 * agent reads it back with atoiOr(a.Color, 0) — so the value here is normally a
 * numeric string like "-65536", not "#ff0000". Only accepting hex silently
 * dropped every colour.
 *
 * Java's getRGB() packs alpha into the high byte and is usually negative, so
 * the value is coerced to unsigned and masked to the low 24 bits. Output is
 * always a literal #rrggbb: this is interpolated into a style attribute, so
 * nothing from the config is ever passed through verbatim.
 */
export function normaliseAliasColor(raw: string): string | null {
  const v = raw.trim();
  if (/^#(?:[0-9a-f]{3}|[0-9a-f]{6})$/i.test(v)) return v;
  // 0x-prefixed or plain (possibly negative) integer.
  const n = /^-?\d+$/.test(v) ? Number(v) : /^0x[0-9a-f]+$/i.test(v) ? Number(v) : NaN;
  if (!Number.isFinite(n)) return null;
  const rgb = (n >>> 0) & 0xffffff;
  // 0 is SDR-Trunk's "unset" default (atoiOr falls back to 0), not black.
  if (rgb === 0) return null;
  return '#' + rgb.toString(16).padStart(6, '0');
}

/**
 * All three lookups from ONE pass over the alias list, sharing a single ~60s
 * cache: they read the same aliases, so resolving them separately would double
 * the config loads and let the maps drift apart between refreshes.
 *
 * First alias wins for each talkgroup so a duplicate id can't flip a label,
 * agency or colour between requests. Only individual talkgroup matchers are
 * mapped (a range labels a span, not a single id).
 */
export async function talkgroupCatalog(): Promise<TalkgroupCatalog> {
  if (_cache && Date.now() - _cache.at < 60_000) return _cache.map;
  const labels = new Map<number, string>();
  const agencies = new Map<number, string>();
  const colors = new Map<number, string>();
  const encrypted = new Set<number>();
  try {
    const cfg = await getGlobalConfig();
    // Same data-level switch as configMerge: a non-empty imported alias list is
    // the pre-unification config and is read verbatim; once the merge clears it
    // the aliases are derived from the unified talkgroup rows.
    const imported = cfg.sdrtrunkConfig?.aliases ?? [];
    const aliasList =
      imported.length > 0 ? imported : deriveAliasesFromTalkgroups(cfg.agencies ?? [], cfg.defaults ?? {});
    for (const ag of cfg.agencies ?? []) {
      if (!ag.encrypted) continue;
      for (const tg of ag.talkgroups) {
        if (typeof tg.id === 'number' && Number.isInteger(tg.id)) encrypted.add(tg.id);
      }
    }
    for (const a of aliasList) {
      const name = (a.name ?? '').trim();
      if (!name) continue;
      const group = (a.group ?? '').trim();
      const color = (a.color ?? '').trim();
      for (const id of a.ids ?? []) {
        if (id.type === 'talkgroup') {
          const v = Number(id.attrs?.['value']);
          if (!Number.isInteger(v)) continue;
          if (!labels.has(v)) labels.set(v, name);
          if (group && !agencies.has(v)) agencies.set(v, group);
          if (color && !colors.has(v)) {
            const hex = normaliseAliasColor(color);
            if (hex) colors.set(v, hex);
          }
        }
      }
    }
  } catch (e) {
    log.warn({ err: e }, 'talkgroupCatalog: failed to load global config');
  }
  const map = { labels, agencies, colors, encrypted };
  _cache = { at: Date.now(), map };
  return map;
}

export async function talkgroupLabels(): Promise<Map<number, string>> {
  return (await talkgroupCatalog()).labels;
}

/**
 * Talkgroup -> owning agency, taken from the SDR-Trunk alias `group`.
 *
 * An alias already carries the agency as its group (that is what groups are for
 * in a playlist), so this needs no new field anywhere — the mapping has been
 * sitting in the global config all along, just never read back out.
 */
export async function talkgroupAgencies(): Promise<Map<number, string>> {
  return (await talkgroupCatalog()).agencies;
}

export async function talkgroupColors(): Promise<Map<number, string>> {
  return (await talkgroupCatalog()).colors;
}
