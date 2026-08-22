/**
 * One-off migration to the unified talkgroup model: merge the imported
 * SDR-Trunk alias list (sdrtrunkConfig.aliases) INTO the agencies' talkgroup
 * rows, so one row per talkgroup drives both programs from then on. After the
 * merge, sdrtrunkConfig.aliases is CLEARED — that's the data-level switch that
 * flips configMerge from "push imported aliases verbatim" to "derive aliases
 * from the rows" (see buildConfigPayload).
 *
 * Merge rules (operator-agreed):
 *   - union by (agency, talkgroup id); rdio label/name WIN over the alias name,
 *     which is preserved verbatim in `aliasName` (it carries each agency's own
 *     channel numbering and is not derivable).
 *   - alias-only rows become new talkgroups under their broadcastChannel's
 *     agency. Aliases routed to a channel with NO matching agency are reported
 *     and skipped — this tool never creates agencies, with ONE exception:
 *   - unrouted aliases in the NSW PF encrypted range (12001-12275) go to a new
 *     `NSW PF (ENC)` agency with the encrypted toggle on (SDR-Trunk only — no
 *     rdio system, no stream). Unrouted aliases OUTSIDE that range are reported
 *     and skipped.
 *   - blank labels ("", "-") are filled from the RadioReference NSW PSN page
 *     snapshot (scripts/data/rr-nswpsn-labels.json), which itself contains only
 *     real labels (RR's own id-echo placeholders were excluded). Fill applies
 *     only to talkgroups that already exist under the operator's agencies.
 *   - repetition is hoisted: per-agency modal groupId/tagId become
 *     defaultGroupId/defaultTagId (rows matching go null = inherit); the modal
 *     alias group becomes sdrGroupName (null when = agency name); the modal
 *     priority per agency becomes agency.priority; the global modal priority
 *     and colour become defaults.priority / defaults.color.
 *   - operator-supplied unit CSVs (scripts/data/units/<code>.csv, "Unit ID,Tag")
 *     merge into their agency's rdio units by id — the CSV wins on a label
 *     conflict (it is the operator's newer curation). Units are rdio-only.
 *
 * Usage (run from backends/node):
 *   npx tsx scripts/merge-talkgroups.ts --input cfg.json --out merged.json  # offline dry-run
 *   npx tsx --env-file=../.env scripts/merge-talkgroups.ts                  # DB dry-run
 *   npx tsx --env-file=../.env scripts/merge-talkgroups.ts --apply         # write to DB
 *
 * Dry-run prints a full report and (with --out) writes the merged config; it
 * never writes to the DB. --apply saves via saveGlobalConfig, which bumps the
 * config version and lets the normal fan-out re-sync the fleet on next push.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  GlobalConfigSchema,
  type Agency,
  type Alias,
  type GlobalConfigInput,
  type Talkgroup,
  type TalkgroupDefaults,
} from '../src/services/nodes/globalConfig.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// NSW PF encrypted talkgroup range (operator-supplied).
const ENC_MIN = 12001;
const ENC_MAX = 12275;
const ENC_AGENCY_NAME = 'NSW PF (ENC)';

/** Unit CSV file (scripts/data/units/) → the agency name it merges into. */
const UNIT_CSV_AGENCIES: Record<string, string> = {
  'ASNSW.csv': 'Ambulance Service',
  'FRNSW.csv': 'Fire and Rescue',
  'NSWNPWS.csv': 'National Parks and Wildlife Service',
  'NSWRFS.csv': 'Rural Fire Service',
  'NSWSES.csv': 'State Emergency Service',
};

interface MergeReport {
  matched: number;
  aliasOnlyAdded: number;
  rdioOnlyRouted: number;
  encRows: number;
  labelsFilled: number;
  namesFilled: number;
  unitsAdded: number;
  unitsRelabelled: number;
  duplicates: string[];
  unroutableAliases: string[];
  unroutedOutsideEnc: string[];
  hoisted: string[];
  chosen: { color: string | null; priority: number | null };
}

const blank = (s: unknown): boolean => {
  const v = typeof s === 'string' ? s.trim() : '';
  return v === '' || v === '-';
};

/** The talkgroup id + priority + broadcastChannel of an imported alias (all
 *  production aliases carry exactly one talkgroup id; anything else reports). */
function aliasParts(al: Alias): { tg: number | null; priority: number | null; channel: string | null } {
  let tg: number | null = null;
  let priority: number | null = null;
  let channel: string | null = null;
  for (const id of al.ids) {
    if (id.type === 'talkgroup') {
      const v = Number(id.attrs['value']);
      if (Number.isInteger(v)) tg = v;
    } else if (id.type === 'priority') {
      const v = Number(id.attrs['priority']);
      if (Number.isInteger(v)) priority = v;
    } else if (id.type === 'broadcastChannel') {
      const c = (id.attrs['channel'] ?? '').trim();
      if (c) channel = c;
    }
  }
  return { tg, priority, channel };
}

/** Pick a group/tag `_id` for an agency that has no talkgroup rows to infer one
 *  from: exact normalised label match on the agency name, else a UNIQUE
 *  containment match ("AusGrid" for "AusGrid Power"), else null. */
function pickByName(
  list: { _id?: unknown; label?: unknown }[],
  agencyName: string,
): number | null {
  const norm = (s: string): string => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  const target = norm(agencyName);
  const entries = list
    .map((e) => ({ id: Number(e._id), label: norm(String(e.label ?? '')) }))
    .filter((e) => Number.isInteger(e.id) && e.label !== '');
  const exact = entries.filter((e) => e.label === target);
  if (exact.length === 1) return exact[0]!.id;
  const contained = entries.filter(
    (e) => e.label.length >= 4 && (target.includes(e.label) || e.label.includes(target)),
  );
  return contained.length === 1 ? contained[0]!.id : null;
}

function mostCommon<T>(values: T[]): T | null {
  const counts = new Map<T, number>();
  for (const v of values) counts.set(v, (counts.get(v) ?? 0) + 1);
  let best: T | null = null;
  let n = 0;
  for (const [v, c] of counts) if (c > n) { best = v; n = c; }
  return best;
}

export function mergeTalkgroups(cfg: GlobalConfigInput): { merged: GlobalConfigInput; report: MergeReport } {
  const rrFill = JSON.parse(
    readFileSync(path.join(__dirname, 'data', 'rr-nswpsn-labels.json'), 'utf8'),
  ) as Record<string, { label: string; name: string }>;

  const report: MergeReport = {
    matched: 0, aliasOnlyAdded: 0, rdioOnlyRouted: 0, encRows: 0,
    labelsFilled: 0, namesFilled: 0, unitsAdded: 0, unitsRelabelled: 0,
    duplicates: [], unroutableAliases: [], unroutedOutsideEnc: [], hoisted: [],
    chosen: { color: null, priority: null },
  };

  // Deep-copy agencies; index by trimmed name (= what broadcastChannel routes to).
  // Names are trimmed in place — a trailing space (NPWS had one) otherwise
  // splits the trimmed stream/broadcastChannel from the untrimmed rdio label.
  const agencies: Agency[] = structuredClone(cfg.agencies);
  for (const a of agencies) {
    const t = a.name.trim();
    if (t !== a.name) { report.hoisted.push(`${t}: name trimmed (was ${JSON.stringify(a.name)})`); a.name = t; }
  }
  const byChannel = new Map<string, Agency>();
  for (const a of agencies) byChannel.set(a.name.trim(), a);

  const aliases = cfg.sdrtrunkConfig?.aliases ?? [];

  // Per-row priorities collected for hoisting; per-agency alias colours so an
  // agency whose rdio side never set a colour keeps the one its aliases used.
  const rowPriority = new Map<Talkgroup, number>();
  const aliasColourPerAgency = new Map<Agency, string[]>();

  // ── 1. fold every alias into a talkgroup row ──────────────────────────────
  const encRows: Talkgroup[] = [];
  const seenPerAgency = new Map<Agency, Map<number, Talkgroup>>();
  for (const a of agencies) {
    const m = new Map<number, Talkgroup>();
    const kept: Talkgroup[] = [];
    for (const tg of a.talkgroups as Talkgroup[]) {
      if (m.has(tg.id)) {
        // First row wins; the duplicate is DROPPED (it would otherwise derive a
        // second alias and collide on rdio's UNIQUE(systemId, id)).
        report.duplicates.push(
          `rdio ${a.name} tg ${tg.id} duplicated — kept "${m.get(tg.id)?.label ?? ''}", dropped "${tg.label ?? ''}"`,
        );
        continue;
      }
      m.set(tg.id, tg);
      kept.push(tg);
    }
    a.talkgroups = kept;
    seenPerAgency.set(a, m);
  }
  const seenEnc = new Map<number, Talkgroup>();
  const aliasGroupPerAgency = new Map<Agency, string[]>();
  const touchedByAlias = new Set<Talkgroup>();

  for (const al of aliases) {
    const { tg, priority, channel } = aliasParts(al);
    if (tg === null) {
      report.unroutableAliases.push(`alias "${al.name}" has no talkgroup id`);
      continue;
    }
    if (channel === null) {
      // Unrouted: NSW PF encrypted range only.
      if (tg >= ENC_MIN && tg <= ENC_MAX) {
        if (seenEnc.has(tg)) { report.duplicates.push(`ENC tg ${tg} duplicated`); continue; }
        const row: Talkgroup = { id: tg, label: al.name || String(tg) };
        if (priority !== null) rowPriority.set(row, priority);
        seenEnc.set(tg, row);
        encRows.push(row);
        report.encRows++;
      } else {
        report.unroutedOutsideEnc.push(`alias "${al.name}" tg ${tg} unrouted and outside ${ENC_MIN}-${ENC_MAX}`);
      }
      continue;
    }
    const agency = byChannel.get(channel);
    if (!agency) {
      report.unroutableAliases.push(`alias "${al.name}" tg ${tg} routes to unknown agency "${channel}"`);
      continue;
    }
    if (al.group) {
      const g = aliasGroupPerAgency.get(agency) ?? [];
      g.push(al.group.trim());
      aliasGroupPerAgency.set(agency, g);
    }
    if (al.color && String(al.color) !== '0') {
      const c = aliasColourPerAgency.get(agency) ?? [];
      c.push(String(al.color));
      aliasColourPerAgency.set(agency, c);
    }
    const rows = seenPerAgency.get(agency)!;
    const existing = rows.get(tg);
    if (existing) {
      // Both sides: rdio label/name win; the alias name is preserved verbatim.
      report.matched++;
      touchedByAlias.add(existing);
      const aliasNm = (al.name ?? '').trim();
      // "-" is a placeholder, not a name — leave aliasName unset so the alias
      // takes the (real, possibly RR-filled) rdio label.
      if (aliasNm && aliasNm !== '-' && aliasNm !== ((existing.label ?? '').trim() || (existing.name ?? '').trim())) {
        existing.aliasName = aliasNm;
      }
      if (priority !== null) rowPriority.set(existing, priority);
    } else {
      // Alias-only: new row named by the alias (RR fill may improve it below).
      const row: Talkgroup = { id: tg, label: al.name || String(tg) };
      if (priority !== null) rowPriority.set(row, priority);
      rows.set(tg, row);
      touchedByAlias.add(row);
      (agency.talkgroups as Talkgroup[]).push(row);
      report.aliasOnlyAdded++;
    }
  }

  // rdio-only rows (no alias existed): they now gain a derived alias + route.
  for (const a of agencies) {
    for (const tg of a.talkgroups as Talkgroup[]) {
      if (!touchedByAlias.has(tg)) report.rdioOnlyRouted++;
    }
  }

  // ── 1b. cross-agency duplicate talkgroup ids ──────────────────────────────
  // The same talkgroup id under two agencies would derive two aliases matching
  // the same traffic (sdrtrunk double-match) — production has exactly this
  // where "Other" holds a pre-identification leftover of a talkgroup a real
  // agency now owns. The real agency wins; the "Other" copy is dropped. Two
  // REAL agencies sharing an id can't be auto-resolved and is only reported.
  {
    const owner = new Map<number, Agency>();
    for (const a of agencies) for (const tg of a.talkgroups as Talkgroup[]) {
      const prev = owner.get(tg.id);
      if (!prev) { owner.set(tg.id, a); continue; }
      const drop = a.name === 'Other' ? a : prev.name === 'Other' ? prev : null;
      if (drop) {
        const keep = drop === a ? prev : a;
        drop.talkgroups = (drop.talkgroups as Talkgroup[]).filter((t) => t.id !== tg.id);
        owner.set(tg.id, keep);
        report.duplicates.push(`tg ${tg.id} in both ${keep.name} and Other — dropped the Other copy`);
      } else {
        report.duplicates.push(`tg ${tg.id} in TWO real agencies: ${prev.name} and ${a.name} — left as-is, resolve by hand`);
      }
    }
  }

  // ── 2. RadioReference fill for blank labels (existing agencies only) ──────
  for (const a of agencies) {
    for (const tg of a.talkgroups as Talkgroup[]) {
      const rr = rrFill[String(tg.id)];
      if (!rr) continue;
      if (blank(tg.label)) {
        tg.label = rr.label;
        report.labelsFilled++;
      }
      if (blank(tg.name)) {
        tg.name = rr.name;
        report.namesFilled++;
      }
    }
  }

  // ── 3. hoist repetition ───────────────────────────────────────────────────
  // Global priority: the most common per-row priority. Colour is PER AGENCY
  // (each agency's one colour drives its rdio LED and its aliases) — an agency
  // that has no explicit colour inherits the modal colour its aliases used.
  const allPriorities = [...rowPriority.values()];
  const globalPriority = mostCommon(allPriorities);
  const defaults: TalkgroupDefaults = {};
  if (globalPriority !== null) defaults.priority = globalPriority;
  report.chosen = { color: null, priority: globalPriority ?? null };
  for (const a of agencies) {
    if (a.color != null) continue; // explicit agency colour wins
    const modalColour = mostCommon(aliasColourPerAgency.get(a) ?? []);
    if (modalColour !== null) {
      a.color = modalColour;
      report.hoisted.push(`${a.name}: color=${modalColour} from its aliases`);
    }
  }

  for (const a of agencies) {
    if (a.encrypted) continue; // SDR-Trunk only — no rdio group/tag to default
    const tgs = a.talkgroups as Talkgroup[];
    // groupId/tagId → agency default + per-row override. Agencies with no rdio
    // rows to infer from (their whole list came from aliases) fall back to a
    // name-matched group/tag, then rdio's own Unknown/Untagged.
    let modalGroup = mostCommon(tgs.map((t) => t.groupId).filter((v): v is number => typeof v === 'number'));
    let modalTag = mostCommon(tgs.map((t) => t.tagId).filter((v): v is number => typeof v === 'number'));
    if (modalGroup === null && tgs.length > 0) {
      modalGroup = pickByName(cfg.rdioGroups, a.name) ?? pickByName(cfg.rdioGroups.filter((g) => String(g['label']) === 'Unknown'), 'Unknown');
      if (modalGroup !== null) report.hoisted.push(`${a.name}: defaultGroupId=${modalGroup} by NAME match (no rdio rows to infer from)`);
    }
    if (modalTag === null && tgs.length > 0) {
      modalTag = pickByName(cfg.rdioTags, a.name) ?? pickByName(cfg.rdioTags.filter((t) => String(t['label']) === 'Untagged'), 'Untagged');
      if (modalTag !== null) report.hoisted.push(`${a.name}: defaultTagId=${modalTag} by NAME match (no rdio rows to infer from)`);
    }
    if (modalGroup !== null) {
      a.defaultGroupId = modalGroup;
      let cleared = 0;
      for (const t of tgs) if (t.groupId === modalGroup) { delete (t as Record<string, unknown>)['groupId']; cleared++; }
      report.hoisted.push(`${a.name}: defaultGroupId=${modalGroup} (${cleared} rows inherit)`);
    }
    if (modalTag !== null) {
      a.defaultTagId = modalTag;
      let cleared = 0;
      for (const t of tgs) if (t.tagId === modalTag) { delete (t as Record<string, unknown>)['tagId']; cleared++; }
      report.hoisted.push(`${a.name}: defaultTagId=${modalTag} (${cleared} rows inherit)`);
    }
    // Agency priority: most common among this agency's rows when it differs
    // from the global default; matching rows go null = inherit.
    const agencyPriorities = tgs.map((t) => rowPriority.get(t)).filter((v): v is number => v !== undefined);
    const modalPriority = mostCommon(agencyPriorities);
    const inherited = modalPriority ?? globalPriority;
    if (modalPriority !== null && modalPriority !== globalPriority) {
      a.priority = modalPriority;
      report.hoisted.push(`${a.name}: priority=${modalPriority}`);
    } else {
      delete (a as Record<string, unknown>)['priority'];
    }
    for (const t of tgs) {
      const p = rowPriority.get(t);
      if (p !== undefined && p !== inherited) t.priority = p;
    }
    // sdrGroupName: the modal alias group, kept only when it differs from the name.
    const modalAliasGroup = mostCommon(aliasGroupPerAgency.get(a) ?? []);
    if (modalAliasGroup !== null && modalAliasGroup !== a.name.trim()) {
      a.sdrGroupName = modalAliasGroup;
      report.hoisted.push(`${a.name}: sdrGroupName="${modalAliasGroup}"`);
    }
  }

  // ── 3b. operator unit CSVs (rdio-only; CSV wins on label conflicts) ───────
  const byName = new Map<string, Agency>();
  for (const a of agencies) byName.set(a.name.trim(), a);
  for (const [file, agencyName] of Object.entries(UNIT_CSV_AGENCIES)) {
    const agency = byName.get(agencyName);
    if (!agency) {
      report.unroutableAliases.push(`unit csv ${file}: no agency named "${agencyName}"`);
      continue;
    }
    let csv: string;
    try {
      csv = readFileSync(path.join(__dirname, 'data', 'units', file), 'utf8');
    } catch {
      continue; // csv not shipped — nothing to merge
    }
    const units = agency.units as { id?: unknown; label?: unknown; order?: unknown }[];
    const byId = new Map<number, { id?: unknown; label?: unknown; order?: unknown }>();
    for (const u of units) if (typeof u.id === 'number') byId.set(u.id, u);
    const lines = csv.split(/\r?\n/).slice(1); // skip "Unit ID,Tag" header
    for (const line of lines) {
      if (!line.trim()) continue;
      const comma = line.indexOf(',');
      if (comma < 0) continue;
      const id = Number(line.slice(0, comma).trim());
      const tag = line.slice(comma + 1).trim().replace(/^"|"$/g, '');
      if (!Number.isInteger(id) || !tag) continue;
      const existing = byId.get(id);
      if (existing) {
        if (String(existing.label ?? '') !== tag) {
          existing.label = tag;
          report.unitsRelabelled++;
        }
      } else {
        const u = { id, label: tag, order: 0 };
        byId.set(id, u);
        units.push(u);
        report.unitsAdded++;
      }
    }
  }

  // ── 4. the encrypted agency ───────────────────────────────────────────────
  if (encRows.length > 0) {
    const usedIds = new Set(agencies.map((a) => a.systemId));
    let encId = 120; // mnemonic for the 120xx police block; bump past collisions
    while (usedIds.has(encId)) encId++;
    encRows.sort((x, y) => x.id - y.id);
    for (const row of encRows) {
      const p = rowPriority.get(row);
      if (p !== undefined && p !== globalPriority) row.priority = p;
    }
    const enc: Agency = {
      systemId: encId,
      name: ENC_AGENCY_NAME,
      encrypted: true,
      aliasIds: [],
      talkgroups: encRows,
      units: [],
    };
    agencies.push(enc);
    report.hoisted.push(`${ENC_AGENCY_NAME}: created (systemId ${encId}, ${encRows.length} talkgroups, encrypted)`);
  }

  // ── 5. clear the imported alias list — the data-level switchover ──────────
  const merged: GlobalConfigInput = {
    agencies,
    rdioGroups: cfg.rdioGroups,
    rdioTags: cfg.rdioTags,
    sdrtrunkConfig: {
      aliasLists: cfg.sdrtrunkConfig?.aliasLists ?? [],
      aliases: [],
      streams: cfg.sdrtrunkConfig?.streams ?? [],
    },
    defaults,
  };
  return { merged, report };
}

// ── CLI ─────────────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const getArg = (name: string): string | null => {
    const i = args.indexOf(name);
    return i >= 0 && args[i + 1] !== undefined ? (args[i + 1] as string) : null;
  };
  const apply = args.includes('--apply');
  const inputPath = getArg('--input');
  const outPath = getArg('--out');

  let cfg: GlobalConfigInput;
  if (inputPath) {
    const raw = JSON.parse(readFileSync(inputPath, 'utf8'));
    const parsed = GlobalConfigSchema.safeParse(raw);
    if (!parsed.success) {
      console.error('input failed GlobalConfigSchema:', parsed.error.issues.slice(0, 10));
      process.exit(1);
    }
    cfg = parsed.data;
  } else {
    const { getGlobalConfig } = await import('../src/services/nodes/globalConfig.js');
    const live = await getGlobalConfig();
    cfg = {
      agencies: live.agencies,
      rdioGroups: live.rdioGroups,
      rdioTags: live.rdioTags,
      sdrtrunkConfig: live.sdrtrunkConfig,
      defaults: live.defaults,
    };
  }

  if ((cfg.sdrtrunkConfig?.aliases.length ?? 0) === 0) {
    console.error('sdrtrunkConfig.aliases is already empty — nothing to merge (already unified?).');
    process.exit(1);
  }

  const { merged, report } = mergeTalkgroups(cfg);

  const tgCount = (a: GlobalConfigInput): number =>
    a.agencies.reduce((n, ag) => n + ag.talkgroups.length, 0);
  console.log('── merge report ──────────────────────────────');
  console.log(`talkgroups: ${tgCount(cfg)} -> ${tgCount(merged)}`);
  console.log(`matched both sides   : ${report.matched}`);
  console.log(`alias-only rows added: ${report.aliasOnlyAdded}`);
  console.log(`rdio-only now routed : ${report.rdioOnlyRouted}`);
  console.log(`encrypted rows       : ${report.encRows}`);
  console.log(`labels filled from RR: ${report.labelsFilled} (+${report.namesFilled} names)`);
  console.log(`units added          : ${report.unitsAdded} (+${report.unitsRelabelled} relabelled)`);
  console.log(`global priority      : ${report.chosen.priority}`);
  for (const h of report.hoisted) console.log(`hoisted: ${h}`);
  for (const d of report.duplicates) console.log(`DUPLICATE: ${d}`);
  for (const u of report.unroutableAliases) console.log(`UNROUTABLE: ${u}`);
  for (const u of report.unroutedOutsideEnc) console.log(`UNROUTED-NON-ENC: ${u}`);

  const check = GlobalConfigSchema.safeParse(merged);
  if (!check.success) {
    console.error('MERGED CONFIG FAILED SCHEMA:', check.error.issues.slice(0, 10));
    process.exit(1);
  }

  if (outPath) {
    writeFileSync(outPath, JSON.stringify(merged, null, 1));
    console.log(`merged config written to ${outPath}`);
  }

  if (apply) {
    const { saveGlobalConfig } = await import('../src/services/nodes/globalConfig.js');
    const saved = await saveGlobalConfig(check.data, 'system:talkgroup-merge');
    console.log(`APPLIED — new config version ${saved.version.slice(0, 12)}…`);
    console.log('Nodes re-sync on the next config push / reconnect.');
  } else {
    console.log('(dry run — nothing written to the DB; use --apply to save)');
  }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  main().then(
    () => process.exit(0),
    (err) => { console.error(err); process.exit(1); },
  );
}
