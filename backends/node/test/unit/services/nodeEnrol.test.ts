// Enrolment codes replace a permanent credential in the installer file with a
// short-lived, single-use one. The properties that matter are all about what
// CANNOT happen: a code used twice, a code that outlives its window, a code
// treated as bad when the database simply could not be reached.

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// A minimal in-memory stand-in for the one table these functions touch.
interface Row {
  id: string;
  user_id: string;
  kind: string;
  token_hash: string | null;
  token_prefix: string | null;
  install_id: string | null;
  enrol_code_hash: string | null;
  enrol_expires_at: Date | null;
}
let rows: Row[] = [];
let poolAvailable = true;
/** Statements the claim issued, so the transaction can be asserted. */
let issued: string[] = [];
/** Simulate another agent claiming the code between the read and the claim. */
let stealCodeAfterRead = false;

/** The real table carries UNIQUE(user_id, install_id); without it here the
 *  repurposed-machine test would pass for the wrong reason. */
function assertUnique(r: Row) {
  if (r.install_id === null) return;
  const clash = rows.some(
    (x) => x !== r && x.user_id === r.user_id && x.install_id === r.install_id,
  );
  if (clash) {
    const err = new Error(
      'duplicate key value violates unique constraint "nodes_user_id_install_id_key"',
    ) as Error & { code?: string };
    err.code = '23505';
    throw err;
  }
}

function makePool() {
  return {
    async query(sql: string, params: unknown[] = []) {
      const s = sql.replace(/\s+/g, ' ').trim();
      issued.push(s.slice(0, 40));

      if (s === 'BEGIN' || s === 'COMMIT' || s === 'ROLLBACK') {
        return { rowCount: 0, rows: [] };
      }
      if (s.startsWith('UPDATE nodes SET install_id = NULL')) {
        const [userId, installId, keepId] = params as [string, string, string];
        let n = 0;
        for (const r of rows) {
          if (r.user_id === userId && r.install_id === installId && r.id !== keepId) {
            r.install_id = null;
            n += 1;
          }
        }
        return { rowCount: n, rows: [] };
      }
      if (s.startsWith('UPDATE nodes SET enrol_code_hash = $2')) {
        const [id, hash, expires] = params as [string, string, Date];
        const r = rows.find((x) => x.id === id);
        if (!r) return { rowCount: 0, rows: [] };
        r.enrol_code_hash = hash;
        r.enrol_expires_at = expires;
        return { rowCount: 1, rows: [] };
      }
      if (s.startsWith('SELECT id, user_id, enrol_expires_at, enrol_code_hash')) {
        const [hash] = params as [string];
        const r = rows.find((x) => x.enrol_code_hash === hash);
        const out = { rowCount: r ? 1 : 0, rows: r ? [{ ...r }] : [] };
        if (r && stealCodeAfterRead) r.enrol_code_hash = null;
        return out;
      }
      if (s.startsWith('UPDATE nodes SET token_hash = $2')) {
        const [hash, tokenHash, tokenPrefix, installId] = params as [string, string, string, string];
        const r = rows.find(
          (x) =>
            x.enrol_code_hash === hash &&
            (x.enrol_expires_at === null || x.enrol_expires_at.getTime() > Date.now()),
        );
        if (!r) return { rowCount: 0, rows: [] };
        r.token_hash = tokenHash;
        r.token_prefix = tokenPrefix;
        const wasInstall = r.install_id;
        r.install_id = installId;
        try {
          assertUnique(r);
        } catch (e) {
          r.install_id = wasInstall;   // the statement did not take effect
          throw e;
        }
        r.enrol_code_hash = null;
        r.enrol_expires_at = null;
        return { rowCount: 1, rows: [{ id: r.id, kind: r.kind }] };
      }
      throw new Error('unexpected SQL in test: ' + s.slice(0, 80));
    },
  };
}

vi.mock('../../../src/db/pool.js', () => ({
  getPool: async () => {
    if (!poolAvailable) return null;
    const pool = makePool();
    // consumeEnrolCode takes a client for its transaction.
    return { ...pool, connect: async () => ({ ...pool, release() {} }) };
  },
}));

const { issueEnrolCode, consumeEnrolCode, ENROL_TTL_MS } = await import(
  '../../../src/services/auth/nodeEnrol.js'
);

describe('enrolment codes', () => {
  beforeEach(() => {
    poolAvailable = true;
    issued = [];
    stealCodeAfterRead = false;
    rows = [
      {
        id: 'node-1', user_id: 'owner-1', kind: 'adsb',
        token_hash: 'old-hash', token_prefix: 'npsn_old', install_id: null,
        enrol_code_hash: null, enrol_expires_at: null,
      },
    ];
  });
  afterEach(() => vi.useRealTimers());

  it('re-enrols a machine that used to run a different node', async () => {
    // The reported failure: a box that had been a radio node was reinstalled
    // as an ADS-B one, and enrolment died with a 500 forever. nodes carries
    // UNIQUE(user_id, install_id), so the old node row still holding the
    // machine id made the claim violate the constraint. That workflow worked
    // before enrolment codes, when the install binding was trust-on-first-use.
    rows.push({
      id: 'node-old', user_id: 'owner-1', kind: 'radio',
      token_hash: 'radio-hash', token_prefix: 'npsn_rad',
      install_id: 'machine-a',
      enrol_code_hash: null, enrol_expires_at: null,
    });
    const code = await issueEnrolCode('node-1');
    const r = await consumeEnrolCode(code!, 'machine-a');

    expect(r.ok).toBe(true);
    expect(rows.find((x) => x.id === 'node-1')!.install_id).toBe('machine-a');
    // The previous holder loses the binding, because one machine has one id
    // and it is not on that machine any more.
    expect(rows.find((x) => x.id === 'node-old')!.install_id).toBeNull();
  });

  it('only releases the id from the SAME owner', async () => {
    // The constraint is per-owner, so another account's node keeps its binding
    // — and the claim then fails loudly rather than silently stealing it.
    rows.push({
      id: 'node-other', user_id: 'owner-2', kind: 'radio',
      token_hash: 'h', token_prefix: 'p', install_id: 'machine-a',
      enrol_code_hash: null, enrol_expires_at: null,
    });
    const code = await issueEnrolCode('node-1');
    const r = await consumeEnrolCode(code!, 'machine-a');

    expect(r.ok).toBe(true);
    expect(rows.find((x) => x.id === 'node-other')!.install_id).toBe('machine-a');
  });

  it('claims inside a transaction', async () => {
    rows.push({
      id: 'node-old', user_id: 'owner-1', kind: 'radio',
      token_hash: 'h', token_prefix: 'p', install_id: 'machine-a',
      enrol_code_hash: null, enrol_expires_at: null,
    });
    const code = await issueEnrolCode('node-1');
    await consumeEnrolCode(code!, 'machine-a');
    expect(issued).toContain('BEGIN');
    expect(issued).toContain('COMMIT');
    expect(issued.indexOf('BEGIN')).toBeLessThan(
      issued.findIndex((x) => x.startsWith('UPDATE nodes SET install_id = NULL')));
  });

  it('does not release a binding when the code turns out to be spent', async () => {
    // The property that matters: a failed enrolment must not strand a node
    // that was working. A spent code is refused at the READ, before a
    // transaction is even opened, so nothing is released — and the claim's
    // own rollback covers the narrower race where the code is taken between
    // the read and the claim.
    rows.push({
      id: 'node-old', user_id: 'owner-1', kind: 'radio',
      token_hash: 'h', token_prefix: 'p', install_id: 'machine-a',
      enrol_code_hash: null, enrol_expires_at: null,
    });
    const code = await issueEnrolCode('node-1');
    await consumeEnrolCode(code!, 'machine-a');       // first agent wins
    rows.find((x) => x.id === 'node-old')!.install_id = 'machine-b';
    issued = [];

    const second = await consumeEnrolCode(code!, 'machine-b');
    expect(second.ok).toBe(false);
    expect(rows.find((x) => x.id === 'node-old')!.install_id).toBe('machine-b');
    expect(issued.some((x) => x.startsWith('UPDATE nodes SET install_id = NULL'))).toBe(false);
    expect(issued).not.toContain('BEGIN');
  });

  it('rolls back if the code is taken between the read and the claim', async () => {
    // The real race. The release has already run inside the transaction, so
    // without the rollback the losing agent would leave the previous node
    // unbound for an enrolment that never completed.
    rows.push({
      id: 'node-old', user_id: 'owner-1', kind: 'radio',
      token_hash: 'h', token_prefix: 'p', install_id: 'machine-a',
      enrol_code_hash: null, enrol_expires_at: null,
    });
    const code = await issueEnrolCode('node-1');
    stealCodeAfterRead = true;
    const r = await consumeEnrolCode(code!, 'machine-a');
    stealCodeAfterRead = false;

    expect(r.ok).toBe(false);
    expect(issued).toContain('ROLLBACK');
    expect(issued).not.toContain('COMMIT');
  });

  it('issues a code that is distinguishable from a node token', async () => {
    const code = await issueEnrolCode('node-1');
    expect(code).toBeTruthy();
    // A separate prefix so the two can never be confused in a config file, a
    // log line, or a support conversation.
    expect(code!.startsWith('nenr_')).toBe(true);
    expect(code!.startsWith('npsn_')).toBe(false);
  });

  it('stores only a hash, never the code', async () => {
    const code = await issueEnrolCode('node-1');
    expect(rows[0]!.enrol_code_hash).toBeTruthy();
    expect(rows[0]!.enrol_code_hash).not.toBe(code);
  });

  it('trades a code for a token and binds the machine', async () => {
    const code = await issueEnrolCode('node-1');
    const r = await consumeEnrolCode(code!, 'install-abc123');
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.token.startsWith('npsn_')).toBe(true);
    expect(r.kind).toBe('adsb');
    expect(rows[0]!.install_id).toBe('install-abc123');
  });

  it('spends the code, so a copied installer cannot enrol a second machine', async () => {
    const code = await issueEnrolCode('node-1');
    const first = await consumeEnrolCode(code!, 'install-one');
    expect(first.ok).toBe(true);

    const second = await consumeEnrolCode(code!, 'install-two');
    expect(second.ok).toBe(false);
    if (second.ok) return;
    expect(second.reason).toBe('bad_code');
    // ...and the second attempt must not have moved the binding.
    expect(rows[0]!.install_id).toBe('install-one');
  });

  it('replaces an outstanding code rather than accumulating them', async () => {
    // "Download it again" means the previous download is abandoned; leaving its
    // code live would keep a credential valid the operator thinks is replaced.
    const first = await issueEnrolCode('node-1');
    const second = await issueEnrolCode('node-1');
    expect(first).not.toBe(second);
    expect((await consumeEnrolCode(first!, 'm1')).ok).toBe(false);
    expect((await consumeEnrolCode(second!, 'm2')).ok).toBe(true);
  });

  it('refuses an expired code, and says so distinctly', async () => {
    const code = await issueEnrolCode('node-1');
    vi.useFakeTimers();
    vi.setSystemTime(Date.now() + ENROL_TTL_MS + 60_000);
    const r = await consumeEnrolCode(code!, 'install-late');
    expect(r.ok).toBe(false);
    if (r.ok) return;
    // 'expired' vs 'bad_code' is the whole of the operator's next step, so the
    // two must not be collapsed.
    expect(r.reason).toBe('expired');
  });

  it('rejects anything that is not an enrolment code', async () => {
    for (const bad of ['', 'npsn_deadbeef', 'hello', 'nenr']) {
      const r = await consumeEnrolCode(bad, 'install-x');
      expect(r.ok).toBe(false);
    }
  });

  it('reports a database outage as unavailable, never as a bad code', async () => {
    // The agent has exactly one credential. Telling it the code is bad when we
    // simply could not check would strand an install that is perfectly fine.
    const code = await issueEnrolCode('node-1');
    poolAvailable = false;
    const r = await consumeEnrolCode(code!, 'install-x');
    expect(r.ok).toBe(false);
    if (r.ok) return;
    expect(r.reason).toBe('unavailable');
  });

  it('rotates the node token, so a previous install stops being accepted', async () => {
    const before = rows[0]!.token_hash;
    const code = await issueEnrolCode('node-1');
    await consumeEnrolCode(code!, 'install-new');
    expect(rows[0]!.token_hash).not.toBe(before);
  });
});
