/**
 * Operator-declared talkgroup patches, read from the CENTRAL rdio-scanner.
 *
 * A patch is a set of talkgroups on one system carrying the SAME conversation —
 * a dispatcher ties two or more channels together and one transmission goes out
 * on all of them at once. rdio owns this concept (rdioScannerPatches, edited in
 * its admin) and we read it rather than duplicating the definition: the
 * operator declares a patch in one place.
 *
 * WHY WE NEED IT
 * The same transmission arrives once per member talkgroup, so without patch
 * knowledge our event feed records it as N separate calls on N talkgroups —
 * while rdio collapses them into one. That is the difference the Data tab shows
 * against rdio's own call list.
 *
 * THE RANKING MATTERS
 * `talkgroups` is in display order, and that order IS the ranking: rdio files
 * the surviving call under the highest-listed talkgroup that actually RECEIVED
 * a copy, and promotes it if a copy later arrives on a higher one. No talkgroup
 * is ever claimed without a real receipt (server/controller.go). We mirror that
 * by grouping on the highest-ranked member, so our logical call lands on the
 * same talkgroup rdio picked.
 *
 * A disabled patch is ignored — that is rdio's own off switch.
 */
import { Pool } from 'pg';
import { config } from '../config.js';
import { log } from '../lib/log.js';

export interface Patch {
  id: number;
  label: string;
  systemId: number;
  /** Member talkgroups in RANK order — highest-priority home first. */
  talkgroups: number[];
}

export interface PatchLookup {
  /** Talkgroup id → the patch it belongs to. A talkgroup is in at most one. */
  byTalkgroup: Map<number, Patch>;
  all: Patch[];
}

const EMPTY: PatchLookup = { byTalkgroup: new Map(), all: [] };

let _pool: Pool | null = null;
let _cache: { at: number; lookup: PatchLookup } | null = null;

/** ~60s, matching the other config-shaped lookups. Patches change by hand, so
 *  a minute of staleness is irrelevant and the read stays off the hot path. */
const TTL_MS = 60_000;

function pool(): Pool | null {
  if (!config.RDIO_DATABASE_URL) return null;
  if (!_pool) {
    _pool = new Pool({ connectionString: config.RDIO_DATABASE_URL, max: 2 });
    _pool.on('error', (err) => log.warn({ err }, 'rdioPatches: idle client error'));
  }
  return _pool;
}

/** Parse the `talkgroups` column, which rdio stores as a JSON array of ints. */
function parseMembers(raw: unknown): number[] {
  let arr: unknown = raw;
  if (typeof raw === 'string') {
    try {
      arr = JSON.parse(raw);
    } catch {
      return [];
    }
  }
  if (!Array.isArray(arr)) return [];
  const out: number[] = [];
  for (const v of arr) {
    const n = Number(v);
    if (Number.isInteger(n) && n > 0 && !out.includes(n)) out.push(n);
  }
  return out;
}

/**
 * Every enabled patch, indexed by member talkgroup.
 *
 * Never throws: a central rdio that is down or unconfigured yields an empty
 * lookup, and every caller then behaves exactly as it did before patches
 * existed. Patch awareness is an enhancement, not a dependency.
 */
export async function rdioPatches(): Promise<PatchLookup> {
  if (_cache && Date.now() - _cache.at < TTL_MS) return _cache.lookup;
  const p = pool();
  if (!p) {
    _cache = { at: Date.now(), lookup: EMPTY };
    return EMPTY;
  }
  const byTalkgroup = new Map<number, Patch>();
  const all: Patch[] = [];
  try {
    const res = await p.query<{
      _id: number;
      label: string | null;
      systemId: number | null;
      talkgroupId: number | null;
      talkgroups: unknown;
    }>(
      `SELECT _id, label, "systemId", "talkgroupId", talkgroups
         FROM "rdioScannerPatches"
        WHERE disabled IS NOT TRUE
        ORDER BY "order", _id`,
    );
    for (const row of res.rows) {
      const members = parseMembers(row.talkgroups);
      // A patch of one is not a patch — rdio's own displays read a single
      // received member as unpatched.
      if (members.length < 2) continue;
      const patch: Patch = {
        id: row._id,
        label: (row.label ?? '').trim() || `Patch ${row._id}`,
        systemId: row.systemId ?? 0,
        talkgroups: members,
      };
      all.push(patch);
      for (const tg of members) {
        // First patch wins if an operator lists a talkgroup twice — matching
        // rdio, which looks a talkgroup up in one patch.
        if (!byTalkgroup.has(tg)) byTalkgroup.set(tg, patch);
      }
    }
  } catch (err) {
    log.warn({ err }, 'rdioPatches: failed to read patches from central rdio');
    // Serve the previous good lookup rather than dropping patch awareness on a
    // transient error; only a cold failure yields empty.
    if (_cache) return _cache.lookup;
  }
  const lookup: PatchLookup = { byTalkgroup, all };
  _cache = { at: Date.now(), lookup };
  return lookup;
}

/**
 * The talkgroup a transmission on `talkgroup` should be GROUPED under.
 *
 * For a patch member this is the patch's highest-ranked talkgroup, so every
 * member's copy of one transmission lands in the same logical call instead of N
 * rival ones. For anything else it is the talkgroup itself.
 *
 * NOTE this is a grouping key, not a claim about where the call was heard: the
 * receptions keep their own talkgroup, so "which channels carried this" is
 * still answerable from the group's members — the same distinction rdio draws
 * between a call's talkgroup and its patch list.
 */
export function groupingTalkgroup(lookup: PatchLookup, talkgroup: number): number {
  const patch = lookup.byTalkgroup.get(talkgroup);
  return patch?.talkgroups[0] ?? talkgroup;
}

/**
 * How a transmission is FILED, and which patch (if any) it belongs to.
 *
 * `home` is the talkgroup the row is shown under. For a patched transmission
 * that is not simply "whichever reception we happened to pick": rdio files the
 * surviving call under the HIGHEST-RANKED member that actually received a copy,
 * and never claims a talkgroup without a real receipt (server/controller.go,
 * Patch.homeRank). The member list in rdioScannerPatches IS the ranking, so the
 * answer is the first entry of that list which appears in what we received.
 * Doing anything else puts a call on a talkgroup rdio would never file it
 * under, and the two records stop agreeing.
 *
 * The two kinds are genuinely different things and are labelled as such:
 *
 *   configured  an operator declared this patch in rdio's admin. Ranked, and
 *               the same for every call until someone edits it. The members
 *               here are the ones we really received, in rank order.
 *
 *   automatic   the trunking system announced the patch over the air, per
 *               call, and vce reported the transmission as CALL_PATCH_GROUP.
 *               There is no ranking to respect — the supergroup IS the home.
 *
 * A patch of one received member is not a patch: rdio's own displays read a
 * transmission that reached a single member as unpatched, and so do we.
 */
export interface PatchView {
  kind: 'configured' | 'automatic';
  /** rdio's label for a configured patch; null for an automatic one. */
  label: string | null;
  /** Every talkgroup this transmission was carried on, in rank order. */
  talkgroups: number[];
}

export function resolvePatch(
  patches: PatchLookup,
  representative: number | null,
  received: readonly number[],
  eventType: string | null,
): { home: number | null; patch: PatchView | null } {
  const members = received.filter((t) => Number.isInteger(t));

  for (const tg of members) {
    const patch = patches.byTalkgroup.get(tg);
    if (!patch) continue;
    // Rank order, restricted to members that really got a copy.
    const ranked = patch.talkgroups.filter((t) => members.includes(t));
    if (ranked.length < 2) break;
    return {
      home: ranked[0] ?? representative,
      patch: { kind: 'configured', label: patch.label, talkgroups: ranked },
    };
  }

  // An over-the-air patch: the supergroup is what was transmitted on, so it is
  // both the home and the first entry. Its member talkgroups are recorded by
  // vce per event (activity_event_talkgroup_member) but are not yet shipped by
  // the control server, so today the list is the supergroup alone.
  if ((eventType ?? '').toUpperCase().startsWith('CALL_PATCH_GROUP')) {
    const home = representative ?? members[0] ?? null;
    const talkgroups = home === null ? members : [home, ...members.filter((t) => t !== home)];
    return { home, patch: { kind: 'automatic', label: null, talkgroups } };
  }

  return { home: representative, patch: null };
}
