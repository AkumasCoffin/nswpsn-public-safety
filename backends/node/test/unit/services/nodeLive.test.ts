/**
 * Fleet Live row shaping.
 *
 * These exist because the shape of vce's activeCalls payload is easy to get
 * wrong in a way nothing catches: a call refers to its channel as
 * `channelName`, and carries NO site, syncPercent or signalDbfs at all
 * (ControlServer.buildActiveCalls emits state/from/to/aliases/timeslot/
 * frequency and stops). Reading `name` off a call silently resolves undefined,
 * so the Sites and Decode columns rendered empty with no error anywhere.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => ({ query: queryMock })),
  getWriterPool: vi.fn(async () => ({ query: queryMock })),
  closePool: vi.fn(async () => undefined),
}));

vi.mock('../../../src/services/talkgroupCatalog.js', () => ({
  talkgroupCatalog: vi.fn(async () => ({
    labels: new Map([[30017, '141 STHHG A']]),
    agencies: new Map([[30017, 'Rural Fire Service']]),
    colors: new Map([[30017, '#ff8800']]),
  })),
}));

const NODE_ROW = { id: 'n1', name: 'radio-cl0ud', kind: 'radio' };

/** One control channel plus one call riding it, in vce's actual field shape. */
const STATUS = {
  channels: [
    {
      name: 'Knights Hill',
      site: 'Knights Hill',
      state: 'CONTROL',
      control: true,
      syncPercent: 97.5,
      signalDbfs: -42.5,
    },
  ],
  activeCalls: [
    {
      state: 'CALL',
      channelName: 'Knights Hill', // NOT `name` — this is the whole point
      to: '30017',
      from: '2019985',
      frequency: 851062500,
      // no site, no syncPercent, no signalDbfs — vce does not send them
    },
  ],
};

async function shape(status: unknown) {
  vi.resetModules();
  queryMock.mockResolvedValue({ rows: [NODE_ROW] });
  const { shapeNodeLive } = await import('../../../src/services/nodeLive.js');
  return shapeNodeLive('n1', status, Date.now());
}

beforeEach(() => {
  queryMock.mockReset();
});

describe('shapeNodeLive: a call inherits what vce never tells it', () => {
  it('never borrows the control channel\'s decode for a call', async () => {
    // The control channel's health is NOT the call's. A call is weak exactly
    // when its traffic channel is weak while the site's control channel is
    // fine, so borrowing would paint over the one case the column exists to
    // reveal. vce reports null for a traffic row (the quality monitor counts
    // TSBK/LCCH control signalling, which a voice channel does not carry), and
    // null must stay null so the UI shows an honest dash.
    const slice = await shape(STATUS);
    expect(slice?.calls).toHaveLength(1);
    expect(slice?.calls[0]!['syncPercent']).toBeNull();
    expect(slice?.calls[0]!['signalDbfs']).toBeNull();
  });

  it('reports the call\'s OWN decode when vce provides one', async () => {
    const withOwn = {
      channels: STATUS.channels,
      activeCalls: [{ ...STATUS.activeCalls[0], syncPercent: 61.5, signalDbfs: -70 }],
    };
    const slice = await shape(withOwn);
    expect(slice?.calls[0]!['syncPercent']).toBe(61.5);
    expect(slice?.calls[0]!['signalDbfs']).toBe(-70);
  });

  it('takes the site from the carrying channel', async () => {
    const slice = await shape(STATUS);
    expect(slice?.calls[0]!['site']).toBe('Knights Hill');
  });

  it('resolves the channel via channelName, not name', async () => {
    // Same payload with the field renamed the way an earlier version wrongly
    // assumed: nothing should resolve, proving the lookup keys on channelName.
    const renamed = {
      channels: STATUS.channels,
      activeCalls: [{ ...STATUS.activeCalls[0], channelName: 'Somewhere Else' }],
    };
    const slice = await shape(renamed);
    expect(slice?.calls[0]!['syncPercent']).toBeNull();
    // …and the site still degrades to the channel name rather than vanishing.
    expect(slice?.calls[0]!['site']).toBe('Somewhere Else');
  });

  it('treats a reported 0% as unmeasured rather than a real reading', async () => {
    // A call in progress cannot be decoding at 0%; a literal zero means the
    // field was defaulted, and must read as "not reported", not as a site
    // that is dead.
    const zeroed = {
      channels: STATUS.channels,
      activeCalls: [{ ...STATUS.activeCalls[0], syncPercent: 0 }],
    };
    const slice = await shape(zeroed);
    expect(slice?.calls[0]!['syncPercent']).toBeNull();
  });

  it('still labels and colours the talkgroup', async () => {
    const slice = await shape(STATUS);
    expect(slice?.calls[0]!['talkgroupLabel']).toBe('141 STHHG A');
    expect(slice?.calls[0]!['agency']).toBe('Rural Fire Service');
    expect(slice?.calls[0]!['color']).toBe('#ff8800');
  });

  it('matches a TRAFFIC channel to its site\'s control channel', async () => {
    // The real shape: calls ride "T-<site>" while channels[] holds the control
    // channel "<site>". An exact-name lookup never matched, so every call
    // showed no decode. The site label also drops the traffic marker.
    const traffic = {
      channels: STATUS.channels, // "Knights Hill"
      activeCalls: [{ ...STATUS.activeCalls[0], channelName: 'T-Knights Hill' }],
    };
    const slice = await shape(traffic);
    expect(slice?.calls[0]!['site']).toBe('Knights Hill');
    // …but decode is still the call's own, not the control channel's.
    expect(slice?.calls[0]!['syncPercent']).toBeNull();
  });

  it('leaves a site that genuinely starts with T alone', async () => {
    // The traffic marker needs its delimiter, or "Tumut" would become "umut".
    const tumut = {
      channels: [{ ...STATUS.channels[0], name: 'Tumut', site: undefined, syncPercent: 80 }],
      activeCalls: [{ ...STATUS.activeCalls[0], channelName: 'Tumut' }],
    };
    const slice = await shape(tumut);
    expect(slice?.calls[0]!['site']).toBe('Tumut');
  });

  it('resolves the talkgroup of a PATCH call', async () => {
    // A patched call reports the whole patch as its target — the supergroup
    // followed by its members. Number() gives NaN on that, so every patched
    // call was rendering as a bare dash with no label, agency or colour.
    const patched = {
      channels: STATUS.channels,
      activeCalls: [{ ...STATUS.activeCalls[0], to: 'P:30017 [10120, 10125]' }],
    };
    const slice = await shape(patched);
    expect(slice?.calls[0]!['talkgroup']).toBe(30017);
    expect(slice?.calls[0]!['talkgroupLabel']).toBe('141 STHHG A');
    expect(slice?.calls[0]!['agency']).toBe('Rural Fire Service');
  });

  it('leaves a genuinely unidentifiable target as no talkgroup', async () => {
    const odd = {
      channels: STATUS.channels,
      activeCalls: [{ ...STATUS.activeCalls[0], to: 'UNKNOWN', toAlias: 'x' }],
    };
    const slice = await shape(odd);
    expect(slice?.calls[0]!['talkgroup']).toBeNull();
  });

  it('drops a pager node entirely', async () => {
    vi.resetModules();
    queryMock.mockResolvedValue({ rows: [{ id: 'n1', name: 'pager-1', kind: 'pager' }] });
    const { shapeNodeLive } = await import('../../../src/services/nodeLive.js');
    expect(await shapeNodeLive('n1', STATUS, Date.now())).toBeNull();
  });
});

describe('shapeNodeLive: rows from the call window', () => {
  async function shapeWithWindow(windowCalls: unknown[]) {
    vi.resetModules();
    queryMock.mockResolvedValue({ rows: [NODE_ROW] });
    const { shapeNodeLive } = await import('../../../src/services/nodeLive.js');
    return shapeNodeLive('n1', STATUS, Date.now(), windowCalls as never);
  }

  const ENDED_AT = 1_700_000_005_000;
  const STARTED_AT = 1_700_000_000_000;

  it('carries the timing the raw frame has no room for', async () => {
    const slice = await shapeWithWindow([
      {
        key: 'k',
        raw: STATUS.activeCalls[0],
        firstSeenAt: STARTED_AT,
        lastSeenAt: ENDED_AT,
        endedAt: ENDED_AT,
      },
    ]);
    const row = slice!.calls[0]!;
    expect(row['ended']).toBe(true);
    expect(row['endedAt']).toBe(new Date(ENDED_AT).toISOString());
    expect(row['startedAt']).toBe(new Date(STARTED_AT).toISOString());
    expect(row['durationMs']).toBe(5_000);
  });

  it('still enriches an ended call with its site and talkgroup', async () => {
    // A held row is a real call that just finished — it must not lose the
    // labelling that made it readable while it was up.
    const slice = await shapeWithWindow([
      {
        key: 'k',
        raw: { ...STATUS.activeCalls[0], channelName: 'T-Knights Hill' },
        firstSeenAt: STARTED_AT,
        lastSeenAt: ENDED_AT,
        endedAt: ENDED_AT,
      },
    ]);
    const row = slice!.calls[0]!;
    expect(row['site']).toBe('Knights Hill');
    expect(row['talkgroupLabel']).toBe('141 STHHG A');
    expect(row['agency']).toBe('Rural Fire Service');
  });

  it('takes calls ONLY from the window, ignoring the frame', async () => {
    // The window is a superset of the frame; shaping both would double every
    // live call.
    const slice = await shapeWithWindow([]);
    expect(slice!.calls).toHaveLength(0);
    // …while channels still come from the frame.
    expect(slice!.channels).toHaveLength(1);
  });

  it('leaves the no-window path byte-identical', async () => {
    const withWindow = await shapeWithWindow([
      {
        key: 'k',
        raw: STATUS.activeCalls[0],
        firstSeenAt: STARTED_AT,
        lastSeenAt: ENDED_AT,
        endedAt: null,
      },
    ]);
    const without = await shape(STATUS);
    const strip = (r: Record<string, unknown>) => {
      const { ended, endedAt, startedAt, lastHeardAt, durationMs, ...rest } = r;
      return rest;
    };
    expect(strip(withWindow!.calls[0]!)).toEqual(strip(without!.calls[0]!));
  });
});
