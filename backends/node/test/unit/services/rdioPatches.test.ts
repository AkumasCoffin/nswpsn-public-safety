/**
 * Operator-declared patches, read from the central rdio.
 *
 * A patch is several talkgroups carrying ONE conversation, so the same
 * transmission arrives once per member. These pin the ranking rule (rdio files
 * the call under the highest-listed member) and the degrade-to-nothing
 * behaviour — patch awareness is an enhancement, never a dependency.
 */
import { describe, it, expect, vi } from 'vitest';

const queryMock = vi.fn();
vi.mock('pg', () => ({
  Pool: class {
    query = queryMock;
    on = vi.fn();
  },
}));
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

// `url` uses null, not undefined, for "unset": passing undefined explicitly
// would trigger the default parameter and silently test the configured case.
async function load(rows: unknown[] | Error, url: string | null = 'postgres://x') {
  vi.resetModules();
  queryMock.mockReset();
  if (rows instanceof Error) queryMock.mockRejectedValue(rows);
  else queryMock.mockResolvedValue({ rows });
  vi.doMock('../../../src/config.js', () => ({
    config: { RDIO_DATABASE_URL: url ?? undefined },
  }));
  return import('../../../src/services/rdioPatches.js');
}

// The two live patches, in the shape rdio stores them.
const LIVE = [
  { _id: 1, label: 'Illawarra A Patch', systemId: 1, talkgroupId: 10079,
    talkgroups: '[10079,30003,20004,30013,10075]' },
  { _id: 2, label: 'North and South SES', systemId: 4, talkgroupId: 10250,
    talkgroups: '[10250,20202]' },
];

describe('rdioPatches', () => {
  it('indexes every member talkgroup onto its patch', async () => {
    const { rdioPatches } = await load(LIVE);
    const lookup = await rdioPatches();
    expect(lookup.all).toHaveLength(2);
    for (const tg of [10079, 30003, 20004, 30013, 10075]) {
      expect(lookup.byTalkgroup.get(tg)?.label).toBe('Illawarra A Patch');
    }
    expect(lookup.byTalkgroup.get(20202)?.label).toBe('North and South SES');
    expect(lookup.byTalkgroup.has(30013)).toBe(true);
    expect(lookup.byTalkgroup.has(99999)).toBe(false);
  });

  it('groups every member under the HIGHEST-RANKED talkgroup, which is list order', async () => {
    // rdio files the surviving call under the highest-listed member that
    // received a copy, so our logical call must land on the same talkgroup.
    const { rdioPatches, groupingTalkgroup } = await load(LIVE);
    const lookup = await rdioPatches();
    for (const tg of [10079, 30003, 20004, 30013, 10075]) {
      expect(groupingTalkgroup(lookup, tg)).toBe(10079);
    }
    expect(groupingTalkgroup(lookup, 20202)).toBe(10250);
  });

  it('leaves an unpatched talkgroup grouping as itself', async () => {
    const { rdioPatches, groupingTalkgroup } = await load(LIVE);
    const lookup = await rdioPatches();
    expect(groupingTalkgroup(lookup, 30013)).toBe(10079); // member
    expect(groupingTalkgroup(lookup, 30083)).toBe(30083); // not a member
  });

  it('ignores a patch of one — rdio reads a single received member as unpatched', async () => {
    const { rdioPatches } = await load([
      { _id: 3, label: 'Solo', systemId: 1, talkgroupId: 111, talkgroups: '[111]' },
    ]);
    const lookup = await rdioPatches();
    expect(lookup.all).toHaveLength(0);
    expect(lookup.byTalkgroup.has(111)).toBe(false);
  });

  it('degrades to no patches when central rdio is unconfigured', async () => {
    const { rdioPatches, groupingTalkgroup } = await load(LIVE, null);
    const lookup = await rdioPatches();
    expect(lookup.all).toEqual([]);
    // Everything then groups exactly as it did before patches existed.
    expect(groupingTalkgroup(lookup, 30013)).toBe(30013);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('degrades to no patches when the read fails, rather than throwing', async () => {
    const { rdioPatches } = await load(new Error('rdio down'));
    await expect(rdioPatches()).resolves.toEqual({ byTalkgroup: new Map(), all: [] });
  });

  it('accepts talkgroups already parsed as an array', async () => {
    const { rdioPatches } = await load([
      { _id: 4, label: 'Parsed', systemId: 1, talkgroupId: 1, talkgroups: [500, 600] },
    ]);
    const lookup = await rdioPatches();
    expect(lookup.byTalkgroup.get(600)?.talkgroups).toEqual([500, 600]);
  });
});

// ---------------------------------------------------------------------------
// resolvePatch — where a transmission is FILED, and what it says about itself.
//
// rdio never claims a talkgroup without a real receipt: the surviving call sits
// on the highest-RANKED member that actually received a copy, and climbs only
// when a copy arrives on a higher one (server/controller.go, Patch.homeRank).
// Picking any other member puts the call on a talkgroup rdio would never file
// it under, and the two records stop agreeing.
// ---------------------------------------------------------------------------
describe('resolvePatch', () => {
  // Illawarra A Patch, in rank order.
  const MEMBERS = [10079, 30003, 20004, 30013, 10075];
  const lookup = () => ({
    all: [],
    byTalkgroup: new Map(
      MEMBERS.map((t) => [
        t,
        { id: 1, label: 'Illawarra A Patch', systemId: 1, talkgroups: MEMBERS },
      ]),
    ),
  });

  async function resolve(...args: Parameters<typeof import('../../../src/services/rdioPatches.js')['resolvePatch']>) {
    const { resolvePatch } = await import('../../../src/services/rdioPatches.js');
    return resolvePatch(...args);
  }

  it('files under the highest-ranked member that really received a copy', async () => {
    // 10079 outranks everything but never got a copy, so it cannot be the home.
    const r = await resolve(lookup() as never, 30013, [30013, 30003, 10075], 'CALL_GROUP');
    expect(r.home).toBe(30003);
    expect(r.patch?.kind).toBe('configured');
    expect(r.patch?.label).toBe('Illawarra A Patch');
    // Members are the RECEIVED ones, in rank order — not the whole patch.
    expect(r.patch?.talkgroups).toEqual([30003, 30013, 10075]);
  });

  it('climbs when a higher-ranked member did receive one', async () => {
    const r = await resolve(lookup() as never, 30013, [30013, 10079], 'CALL_GROUP');
    expect(r.home).toBe(10079);
  });

  it('a transmission that reached ONE member is not a patch', async () => {
    // rdio's own displays read a single receipt as unpatched, and so do we.
    const r = await resolve(lookup() as never, 30003, [30003], 'CALL_GROUP');
    expect(r.home).toBe(30003);
    expect(r.patch).toBeNull();
  });

  it('flags an over-the-air patch from the event type alone', async () => {
    // CALL_PATCH_GROUP is vce saying the trunking system announced this patch
    // for this call. The supergroup is what was transmitted on, so it is the
    // home; its members are recorded by vce per event but not yet shipped.
    const r = await resolve(
      { byTalkgroup: new Map(), all: [] } as never,
      10128,
      [10128],
      'CALL_PATCH_GROUP',
    );
    expect(r.home).toBe(10128);
    expect(r.patch).toEqual({ kind: 'automatic', label: null, talkgroups: [10128] });
  });

  it('never lists the supergroup twice among its own members', async () => {
    const r = await resolve(
      { byTalkgroup: new Map(), all: [] } as never,
      10128,
      [10128, 10120],
      'CALL_PATCH_GROUP',
    );
    expect(r.patch?.talkgroups).toEqual([10128, 10120]);
  });

  it('leaves an ordinary transmission alone', async () => {
    const r = await resolve({ byTalkgroup: new Map(), all: [] } as never, 20458, [20458], 'CALL_GROUP');
    expect(r).toEqual({ home: 20458, patch: null });
  });

  it('degrades to the representative talkgroup when rdio is unreachable', async () => {
    // An empty lookup is what a down or unconfigured central rdio yields.
    const r = await resolve({ byTalkgroup: new Map(), all: [] } as never, 30003, [30003, 30013], 'CALL_GROUP');
    expect(r.home).toBe(30003);
    expect(r.patch).toBeNull();
  });
});
