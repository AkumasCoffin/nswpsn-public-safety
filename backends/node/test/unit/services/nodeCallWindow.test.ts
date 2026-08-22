import { describe, it, expect } from 'vitest';
import { LiveCallWindow, isLiveCallRow } from '../../../src/services/nodeCallWindow.js';

const HANG = 5_000;
const T0 = 1_700_000_000_000;

/** A window with a clock the test drives by hand. */
function mk(overrides = {}) {
  let now = T0;
  const w = new LiveCallWindow({ hangMs: HANG, now: () => now, ...overrides });
  return {
    w,
    at: () => now,
    tick: (ms: number) => {
      now += ms;
      return now;
    },
  };
}

const call = (over: Record<string, unknown> = {}) => ({
  state: 'CALL',
  channelName: 'T-Knights Hill',
  timeslot: 0,
  to: '1201',
  from: '5551',
  toAlias: 'Fireground 1',
  ...over,
});

const frame = (calls: unknown[], over: Record<string, unknown> = {}) => ({
  activeCalls: calls,
  events: [],
  ...over,
});

const keysOf = (w: LiveCallWindow, node = 'n1') =>
  w.callsFor(node).map((c) => `${c.raw['to']}/${c.raw['from']}${c.endedAt ? ':ended' : ''}`);

describe('isLiveCallRow', () => {
  it('rejects control channels and idle grants, keeps real calls', () => {
    expect(isLiveCallRow(call())).toBe(true);
    // A granted traffic channel sitting between calls: no target, no source.
    expect(isLiveCallRow({ state: 'CALL', to: null, from: null })).toBe(false);
    // Identity present but not a call state.
    expect(isLiveCallRow({ state: 'CONTROL', to: '1201' })).toBe(false);
    expect(isLiveCallRow({ state: 'DATA', to: '1201' })).toBe(false);
    // Alias-only rows still count — the ids just haven't resolved yet.
    expect(isLiveCallRow({ state: 'ENCRYPTED', to: null, from: null, toAlias: 'PF' })).toBe(true);
  });
});

describe('LiveCallWindow — live calls', () => {
  it('reports a call seen in a frame as live', () => {
    const { w } = mk();
    w.observe('n1', frame([call()]));
    const rows = w.callsFor('n1');
    expect(rows).toHaveLength(1);
    expect(rows[0]!.endedAt).toBeNull();
    expect(rows[0]!.firstSeenAt).toBe(T0);
    expect(rows[0]!.fromEvent).toBe(false);
  });

  it('holds a finished call for the hang window, then drops it', () => {
    const { w, tick } = mk();
    w.observe('n1', frame([call()]));
    const endedAt = tick(1_000);
    w.observe('n1', frame([])); // healthy frame, call gone

    const ended = w.callsFor('n1');
    expect(ended).toHaveLength(1);
    expect(ended[0]!.endedAt).toBe(endedAt);

    tick(HANG - 100); // still inside the hang
    expect(w.callsFor('n1')).toHaveLength(1);

    tick(200); // now past it
    expect(w.callsFor('n1')).toHaveLength(0);
  });

  it('revives a call that reappears inside the hang instead of duplicating it', () => {
    const { w, tick } = mk();
    w.observe('n1', frame([call()]));
    tick(1_000);
    w.observe('n1', frame([])); // briefly missed
    tick(1_000);
    w.observe('n1', frame([call()])); // back

    const rows = w.callsFor('n1');
    expect(rows).toHaveLength(1);
    expect(rows[0]!.endedAt).toBeNull();
    // The call did not restart — it is the same conversation.
    expect(rows[0]!.firstSeenAt).toBe(T0);
  });
});

describe('LiveCallWindow — caller identity', () => {
  it('fills in a caller that resolves mid-call without splitting the row', () => {
    const { w, tick } = mk();
    w.observe('n1', frame([call({ from: null, fromAlias: null })]));
    tick(1_000);
    w.observe('n1', frame([call({ from: '5551' })]));

    const rows = w.callsFor('n1');
    expect(rows).toHaveLength(1);
    expect(rows[0]!.raw['from']).toBe('5551');
    expect(rows[0]!.firstSeenAt).toBe(T0);
  });

  it('never downgrades a resolved caller back to unknown', () => {
    const { w, tick } = mk();
    w.observe('n1', frame([call({ from: '5551' })]));
    tick(1_000);
    w.observe('n1', frame([call({ from: null })]));
    expect(w.callsFor('n1')[0]!.raw['from']).toBe('5551');
  });

  it('treats a different caller on the same slot as a new call', () => {
    const { w, tick } = mk();
    w.observe('n1', frame([call({ from: '5551' })]));
    tick(1_000);
    w.observe('n1', frame([call({ from: '9999' })]));

    // The first over ends and gets its own hang; the second is live.
    expect(keysOf(w).sort()).toEqual(['1201/5551:ended', '1201/9999']);
  });

  it('keeps calls on different talkgroups and timeslots apart', () => {
    const { w } = mk();
    w.observe(
      'n1',
      frame([call({ to: '1201' }), call({ to: '1202' }), call({ to: '1201', timeslot: 1 })]),
    );
    expect(w.callsFor('n1')).toHaveLength(3);
  });
});

describe('LiveCallWindow — degraded frames', () => {
  const degraded = (calls: unknown[]) =>
    frame(calls, { components: { sdrtrunk: 'unreachable' } });

  it('does not end calls when the agent cannot reach sdrtrunk', () => {
    const { w, tick } = mk();
    w.observe('n1', frame([call()]));
    tick(1_000);
    // /tuners timed out: everything arrives empty. This must NOT read as
    // "every call ended" — that is what blanked the whole view.
    w.observe('n1', degraded([]));
    expect(w.callsFor('n1')[0]!.endedAt).toBeNull();
  });

  it('does end calls on a healthy empty frame', () => {
    const { w, tick } = mk();
    w.observe('n1', frame([call()]));
    tick(1_000);
    w.observe('n1', frame([]));
    expect(w.callsFor('n1')[0]!.endedAt).not.toBeNull();
  });

  it('force-ends a call once degradation outlasts staleActiveMs', () => {
    const { w, tick } = mk({ staleActiveMs: 30_000 });
    w.observe('n1', frame([call()]));
    tick(31_000);
    w.observe('n1', degraded([]));
    // Held while plausibly still up, but not forever.
    expect(w.callsFor('n1').every((c) => c.endedAt !== null)).toBe(true);
  });
});

describe('LiveCallWindow — recovering calls from decode events', () => {
  const ev = (over: Record<string, unknown> = {}) => ({
    type: 'CALL_GROUP',
    timeStart: T0 - 2_000,
    timeEnd: T0 - 1_000,
    durationMs: 1_000,
    from: '7001',
    to: '1300',
    toAlias: 'Ambo 3',
    timeslot: 0,
    channel: '460.5250',
    ...over,
  });

  it('recovers a call that ended between two polls', () => {
    const { w } = mk();
    w.observe('n1', frame([], { events: [ev()] }));
    const rows = w.callsFor('n1');
    expect(rows).toHaveLength(1);
    expect(rows[0]!.fromEvent).toBe(true);
    expect(rows[0]!.firstSeenAt).toBe(T0 - 2_000);
    expect(rows[0]!.endedAt).toBe(T0 - 1_000);
    expect(rows[0]!.raw['to']).toBe('1300');
    // The event's `channel` is a frequency descriptor, not a channel name —
    // claiming it as one would invent a site.
    expect(rows[0]!.raw['channelName']).toBeNull();
  });

  it('does not duplicate when the same events array is re-sent every frame', () => {
    const { w, tick } = mk();
    w.observe('n1', frame([], { events: [ev()] }));
    tick(1_000);
    w.observe('n1', frame([], { events: [ev()] }));
    tick(1_000);
    w.observe('n1', frame([], { events: [ev()] }));
    expect(w.callsFor('n1')).toHaveLength(1);
  });

  it('skips an event for a call already tracked live', () => {
    const { w, tick } = mk();
    // Same parties, observed live right now.
    w.observe('n1', frame([call({ to: '1300', from: '7001' })]));
    tick(1_000);
    w.observe('n1', frame([call({ to: '1300', from: '7001' })], { events: [ev({ timeStart: T0, timeEnd: T0 + 500 })] }));
    expect(w.callsFor('n1')).toHaveLength(1);
    expect(w.callsFor('n1')[0]!.fromEvent).toBe(false);
  });

  it('ignores in-progress, stale, and non-audio event types', () => {
    const { w } = mk();
    w.observe(
      'n1',
      frame([], {
        events: [
          ev({ timeEnd: 0 }), // still in progress — activeCalls owns it
          ev({ timeStart: T0 - 60_000, timeEnd: T0 - 30_000 }), // older than the hang
          ev({ type: 'CALL_ALERT' }), // a page, not audio
          ev({ type: 'CALL_DETECT' }),
          ev({ type: 'CALL_END' }),
          ev({ type: 'REGISTER' }),
          ev({ type: 'GPS' }),
        ],
      }),
    );
    expect(w.callsFor('n1')).toHaveLength(0);
  });

  it('accepts the encrypted and unit-to-unit call variants', () => {
    const { w } = mk();
    w.observe(
      'n1',
      frame([], {
        events: [
          ev({ type: 'CALL_GROUP_ENCRYPTED', to: '12001' }),
          ev({ type: 'CALL_UNIT_TO_UNIT', to: '12002' }),
          ev({ type: 'CALL_PATCH_GROUP', to: '12003' }),
        ],
      }),
    );
    const rows = w.callsFor('n1');
    expect(rows).toHaveLength(3);
    expect(rows.find((r) => r.raw['to'] === '12001')!.raw['state']).toBe('ENCRYPTED');
    expect(rows.find((r) => r.raw['to'] === '12002')!.raw['state']).toBe('CALL');
  });

  it('expires a recovered call on its own end time, not on when it arrived', () => {
    const { w, tick } = mk();
    w.observe('n1', frame([], { events: [ev({ timeStart: T0 - 2_000, timeEnd: T0 - 1_000 })] }));
    expect(w.callsFor('n1')).toHaveLength(1);
    tick(HANG - 1_000 + 100); // now past timeEnd + hang
    expect(w.callsFor('n1')).toHaveLength(0);
  });
});

describe('LiveCallWindow — bounds and lifecycle', () => {
  it('caps ended calls per node', () => {
    const { w, tick } = mk({ maxEndedPerNode: 5, hangMs: 600_000 });
    for (let i = 0; i < 20; i++) {
      w.observe('n1', frame([call({ to: String(2000 + i) })]));
      tick(10);
      w.observe('n1', frame([]));
      tick(10);
    }
    expect(w.callsFor('n1').length).toBeLessThanOrEqual(5);
  });

  it('keeps nodes independent and forgets one on drop', () => {
    const { w } = mk();
    w.observe('n1', frame([call()]));
    w.observe('n2', frame([call({ to: '9001' })]));
    expect(w.callsFor('n1')).toHaveLength(1);
    expect(w.callsFor('n2')).toHaveLength(1);

    w.dropNode('n1');
    expect(w.callsFor('n1')).toHaveLength(0);
    expect(w.callsFor('n2')).toHaveLength(1);
  });

  it('reads do not advance observation state', () => {
    const { w, tick } = mk();
    w.observe('n1', frame([call()]));
    tick(1_000);
    w.observe('n1', frame([]));
    // Polling repeatedly must not keep the ended row alive past its hang.
    tick(HANG + 100);
    w.callsFor('n1');
    w.callsFor('n1');
    expect(w.callsFor('n1')).toHaveLength(0);
  });

  it('sweepExpired names a node once per batch and force-ends stale calls', () => {
    const { w, tick } = mk({ staleActiveMs: 30_000 });
    w.observe('n1', frame([call()]));
    tick(1_000);
    w.observe('n1', frame([])); // ended, hanging

    expect(w.sweepExpired()).toEqual([]); // nothing expired yet
    tick(HANG + 100);
    expect(w.sweepExpired()).toEqual(['n1']); // the hang ran out
    expect(w.sweepExpired()).toEqual([]); // already cleaned; no repeat repaint

    // A node that goes silent mid-call gets it force-ended by the sweep.
    w.observe('n2', frame([call()]));
    tick(31_000);
    expect(w.sweepExpired()).toContain('n2');
    expect(w.callsFor('n2').every((c) => c.endedAt !== null)).toBe(true);
  });
});
