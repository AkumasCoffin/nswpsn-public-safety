/**
 * Safety tests for the incomplete-signup selection rule. The critical guarantee:
 * an account that has submitted a signup request is NEVER removed, even before
 * an owner assigns it roles (roles are assigned manually after review).
 */
import { describe, it, expect } from 'vitest';
import { selectOrphans } from '../../../src/services/orphanCleanup.js';

const NOW = 1_700_000_000_000;
const RACE_GUARD_MS = 15 * 60_000;
const oldTs = new Date(NOW - 60 * 60_000).toISOString(); // 1h ago (past the guard)
const freshTs = new Date(NOW - 60_000).toISOString(); // 1 min ago (inside the guard)

const base = {
  roleIds: new Set<string>(),
  reqIds: new Set<string>(),
  reqEmails: new Set<string>(),
  nowMs: NOW,
  raceGuardMs: RACE_GUARD_MS,
};
const user = (id: string, email: string | null, created_at: string) => ({ id, email, created_at });

describe('selectOrphans — never removes a signed-up user', () => {
  it('keeps a user who has a role', () => {
    const out = selectOrphans([user('u1', 'a@b.com', oldTs)], { ...base, roleIds: new Set(['u1']) });
    expect(out).toEqual([]);
  });

  it('keeps a PENDING signup (request linked by account id, no roles yet)', () => {
    const out = selectOrphans([user('u1', 'a@b.com', oldTs)], { ...base, reqIds: new Set(['u1']) });
    expect(out).toEqual([]);
  });

  it('keeps a signup whose request is linked only by email (case-insensitive)', () => {
    const out = selectOrphans([user('u1', 'A@B.com', oldTs)], { ...base, reqEmails: new Set(['a@b.com']) });
    expect(out).toEqual([]);
  });

  it('keeps a freshly-created account (in-flight signup, within the race guard)', () => {
    const out = selectOrphans([user('u1', 'a@b.com', freshTs)], base);
    expect(out).toEqual([]);
  });

  it('removes ONLY an old account with no role and no request', () => {
    const users = [
      user('keep-role', 'r@x.com', oldTs),
      user('keep-req-id', 'q@x.com', oldTs),
      user('keep-req-email', 'e@x.com', oldTs),
      user('keep-fresh', 'f@x.com', freshTs),
      user('orphan', 'o@x.com', oldTs),
    ];
    const out = selectOrphans(users, {
      ...base,
      roleIds: new Set(['keep-role']),
      reqIds: new Set(['keep-req-id']),
      reqEmails: new Set(['e@x.com']),
    });
    expect(out.map((o) => o.id)).toEqual(['orphan']);
  });
});

/**
 * Regression guard for the migration-059 base role. EVERY account now holds
 * 'authed' (granted on login by /api/profiles/sync), so any "does this user
 * have a role?" query MUST exclude it — otherwise no account ever looks
 * incomplete again and orphan cleanup silently becomes a no-op.
 */
describe('authed base role must not count as "has a role"', () => {
  it('accountIsIncomplete ignores an authed-only account', async () => {
    const seen: string[] = [];
    const pool = {
      query: async (sql: string) => {
        seen.push(sql);
        // 1st call = the role lookup, 2nd = the editor_requests lookup.
        return { rowCount: 0, rows: [] };
      },
    };
    const { accountIsIncomplete } = await import('../../../src/services/orphanCleanup.js');
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const out = await accountIsIncomplete(pool as any, 'u1', 'a@b.com');
    expect(out).toBe(true); // no real roles + no request → incomplete
    // The role query must filter the base role out.
    expect(seen[0]).toContain('user_roles');
    expect(seen[0]).toContain("role <> 'authed'");
  });
});
