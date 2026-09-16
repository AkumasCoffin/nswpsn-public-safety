/**
 * Enrolment codes: how an agent gets its token without the installer carrying
 * one.
 *
 * The installer used to have the node's long-lived token baked in. Because the
 * token is hashed at rest and never re-derivable, re-downloading an installer
 * had to mint a new token — silently breaking the agent already running on that
 * node. And the downloaded file was a permanent credential sitting in a
 * volunteer's home directory.
 *
 * So the installer now carries a short-lived, single-use code instead. The
 * agent trades it for a real token on first run; the code is spent in the same
 * statement that issues the token, so a copied installer cannot enrol a second
 * machine. Downloading an installer mints a code and touches nothing else, so a
 * running node keeps working.
 *
 * The exchange DOES rotate the node's token — enrolling a machine means that
 * machine is now the node, and any previous install must stop. That is the one
 * thing an operator genuinely means by running an installer, and unlike the old
 * behaviour it happens when the installer is RUN rather than when it is
 * downloaded.
 */
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { getPool } from '../../db/pool.js';
import { mintNodeToken, _clearNodeTokenCache } from './nodeToken.js';

/** Distinct from the token's 'npsn_' so the two can never be confused, in a
 *  config file, a log line or a support conversation. */
const ENROL_PREFIX = 'nenr_';
const ENROL_BYTES = 16; // 128 bits

/**
 * How long a code stays usable.
 *
 * Long enough to download an installer, copy it to a machine and get around to
 * running it; short enough that a file left in a downloads folder stops being a
 * way in. An operator who takes longer just downloads again, which is now a
 * harmless act.
 */
export const ENROL_TTL_MS = 24 * 60 * 60 * 1000;

function sha256hex(s: string): string {
  return createHash('sha256').update(s).digest('hex');
}

/** Constant-time compare of two hex digests of equal length. */
function safeEqualHex(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  return timingSafeEqual(Buffer.from(a, 'hex'), Buffer.from(b, 'hex'));
}

export function isEnrolCode(v: string): boolean {
  return typeof v === 'string' && v.startsWith(ENROL_PREFIX);
}

/**
 * Issue a fresh enrolment code for a node, replacing any outstanding one.
 *
 * Replacing rather than accumulating is deliberate: "download it again" means
 * the previous download is being abandoned, and leaving its code live would
 * keep a credential valid that the operator believes they have replaced.
 */
export async function issueEnrolCode(nodeId: string): Promise<string | null> {
  const pool = await getPool();
  if (!pool) return null;
  const code = ENROL_PREFIX + randomBytes(ENROL_BYTES).toString('hex');
  const expires = new Date(Date.now() + ENROL_TTL_MS);
  const res = await pool.query(
    `UPDATE nodes
        SET enrol_code_hash = $2, enrol_issued_at = now(), enrol_expires_at = $3
      WHERE id = $1`,
    [nodeId, sha256hex(code), expires],
  );
  return (res.rowCount ?? 0) > 0 ? code : null;
}

export type EnrolResult =
  | { ok: true; nodeId: string; kind: string; token: string }
  | { ok: false; reason: 'bad_code' | 'expired' | 'unavailable' };

/**
 * Trade a code for a node token.
 *
 * Single-statement claim: the UPDATE both checks the code and clears it, so two
 * agents racing the same code cannot both win — the second finds nothing to
 * clear. Expiry is evaluated in the same statement for the same reason.
 *
 * install_id is bound here too. The agent presents the machine id it will use,
 * and enrolment is exactly the moment that binding should be (re)made: the
 * operator has just declared "this machine is that node" by running the
 * installer on it.
 */
export async function consumeEnrolCode(
  code: string,
  installId: string,
): Promise<EnrolResult> {
  if (!isEnrolCode(code)) return { ok: false, reason: 'bad_code' };
  const pool = await getPool();
  // Cannot tell whether the code is good, so do not claim it is bad — the
  // agent must retry rather than give up on a credential that may be fine.
  if (!pool) return { ok: false, reason: 'unavailable' };

  const hash = sha256hex(code);

  // Read first, so an expired code can be reported as expired rather than as
  // an unknown one — the difference is the whole of the operator's next step.
  const found = await pool.query<{ id: string; enrol_expires_at: Date | null; enrol_code_hash: string }>(
    `SELECT id, enrol_expires_at, enrol_code_hash FROM nodes WHERE enrol_code_hash = $1`,
    [hash],
  );
  const row = found.rows[0];
  if (!row || !safeEqualHex(hash, row.enrol_code_hash)) {
    return { ok: false, reason: 'bad_code' };
  }
  if (row.enrol_expires_at && row.enrol_expires_at.getTime() <= Date.now()) {
    return { ok: false, reason: 'expired' };
  }

  const { token, tokenHash, tokenPrefix } = mintNodeToken();
  // The WHERE clause re-checks the code and the expiry, so this is still the
  // atomic claim even though the read above is separate: if anything changed in
  // between, zero rows update and nobody gets a token.
  const claimed = await pool.query<{ id: string; kind: string }>(
    `UPDATE nodes
        SET token_hash = $2,
            token_prefix = $3,
            token_rotated_at = now(),
            install_id = $4,
            enrol_code_hash = NULL,
            enrol_issued_at = NULL,
            enrol_expires_at = NULL
      WHERE enrol_code_hash = $1
        AND (enrol_expires_at IS NULL OR enrol_expires_at > now())
      RETURNING id, kind`,
    [hash, tokenHash, tokenPrefix, installId],
  );
  const claimedRow = claimed.rows[0];
  if (!claimedRow) return { ok: false, reason: 'bad_code' };

  // The node's old token is now invalid; drop it from the resolve cache so a
  // previous agent stops being accepted on a cached entry.
  _clearNodeTokenCache();
  return { ok: true, nodeId: claimedRow.id, kind: claimedRow.kind, token };
}
