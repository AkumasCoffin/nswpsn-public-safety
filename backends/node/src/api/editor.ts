/**
 * Editor-request approval queue + role-check endpoints.
 *
 * Mirrors python external_api_proxy.py:
 *   - 13503 POST /api/editor-requests       (public — anyone signed in)
 *   - 13606 GET  /api/editor-requests       (api-key)
 *   - 13669 POST /api/editor-requests/<id>/approve  (api-key)
 *   - 13808 POST /api/editor-requests/<id>/reject   (api-key)
 *   - 13860 GET  /api/check-editor/<user_id>        (public — exposes only role booleans)
 *   - 13907 GET  /api/check-admin/<user_id>         (api-key)
 *
 * Schema notes (init_postgres.py:62-82):
 *   editor_requests is SERIAL primary key, epoch-second integer
 *   created_at / reviewed_at, comma-separated `request_type` and
 *   `tech_experience` strings. We split on comma to expose them as
 *   arrays in JSON responses, matching python's behaviour at 13637 /
 *   13651.
 *
 * Critical: /api/check-admin/<user_id> MUST include the
 *   tabs: { requests, users, dev }
 * block — dashboard.html and staff.html key off it. See the
 * test suite for the exact assertions.
 *
 * Approval flow. Approval NEVER creates an account and never issues a
 * password. signup.html creates the Supabase account itself, with a password
 * the person chooses, before the request is submitted; approval only decides
 * which roles that account carries.
 *
 * That leaves two shapes. A request carrying a supabase_user_id has its roles
 * written to user_roles immediately. A request without one was filed while
 * email confirmation was still pending — real and common, not a broken signup
 * — so the approval is recorded in `approved_roles` and claimed by verified
 * email at that person's first signed-in page load (see profiles.ts).
 *
 * This replaced python's behaviour, which created a second Supabase account
 * with a generated `Changeme-XXXXXX` password. That call fails outright for an
 * email that already has an account, which since signup.html started creating
 * them is every email; the roles were silently not granted, and the password
 * went into the request notes and from there into a staff Discord channel.
 */
import { Hono } from 'hono';
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { config } from '../config.js';
import { notifyStaff } from '../services/staffNotify.js';
import {
  getUserRoles,
  canonicalRoles,
  invalidateUserRolesCache,
  requireRole,
  canManageUsers,
  canAssignPrivilegedRoles,
  isPrivilegedRole,
  isOwner,
  canonicalRole,
  isKnownRole,
} from '../services/auth/roles.js';
import { getUsername } from './users.js';

export const editorRouter = new Hono();

const DB_UNAVAILABLE = { error: 'database unavailable' } as const;

interface EditorRequestRow {
  id: number;
  email: string;
  discord_id: string;
  website: string | null;
  about: string | null;
  request_type: string | null;
  region: string | null;
  background: string | null;
  background_details: string | null;
  has_existing_setup: string | null;
  setup_details: string | null;
  tech_experience: string | null;
  experience_level: number | null;
  status: string;
  created_at: number | string;
  reviewed_at: number | string | null;
  notes: string | null;
  supabase_user_id: string | null;
  referred_by: string | null;
  referred_by_name: string | null;
}

function splitCsv(s: string | null | undefined): string[] {
  if (!s) return [];
  return s.split(',').filter((x) => x.length > 0);
}

function asArrayOfString(raw: unknown): string[] {
  if (Array.isArray(raw)) return raw.filter((x): x is string => typeof x === 'string');
  if (typeof raw === 'string' && raw) return [raw];
  return [];
}

function normaliseRequest(row: EditorRequestRow): Record<string, unknown> {
  return {
    id: row.id,
    email: row.email,
    discord_id: row.discord_id,
    website: row.website,
    about: row.about,
    request_type: splitCsv(row.request_type),
    region: row.region,
    background: row.background,
    background_details: row.background_details,
    has_existing_setup: row.has_existing_setup,
    setup_details: row.setup_details,
    tech_experience: splitCsv(row.tech_experience),
    experience_level: row.experience_level,
    status: row.status,
    created_at: row.created_at,
    reviewed_at: row.reviewed_at,
    notes: row.notes,
    supabase_user_id: row.supabase_user_id ?? null,
    referred_by: row.referred_by ?? null,
    referred_by_name: row.referred_by_name ?? null,
  };
}

// ---------------------------------------------------------------------------
// POST /api/editor-requests  — public submission
// ---------------------------------------------------------------------------

/**
 * Detail rows for a signup request.
 *
 * These land in a private staff channel, so they carry what someone actually
 * needs to make the call — who is asking, where from, what they already run —
 * rather than a type and a region that only told you a request existed.
 * The free-text answers go last and full-width; packFields truncates them.
 */
/** `radio_feeder,editor` is what the column holds; it is not what a heading
 *  should say. Unknown values pass through tidied rather than dropped, so a
 *  new request type never shows up blank. */
function prettyRequestType(raw: string | null | undefined): string {
  const parts = String(raw || '').split(',').map((x) => x.trim()).filter(Boolean);
  if (!parts.length) return 'Access request';
  return parts
    .map((t) => {
      const words = t.replace(/[_-]+/g, ' ').trim();
      return words.charAt(0).toUpperCase() + words.slice(1);
    })
    .join(' + ');
}

function signupFields(r: {
  email?: string | null;
  discordId?: string | null;
  website?: string | null;
  about?: string | null;
  region?: string | null;
  background?: string | null;
  backgroundDetails?: string | null;
  hasExistingSetup?: unknown;
  setupDetails?: string | null;
  techExperienceStr?: unknown;
  experienceLevel?: unknown;
  referredByName?: string | null;
}) {
  return [
    { name: 'Email', value: r.email },
    { name: 'Discord', value: r.discordId },
    { name: 'Region', value: r.region },
    { name: 'Experience', value: r.experienceLevel },
    { name: 'Background', value: r.background },
    { name: 'Existing setup', value: r.hasExistingSetup },
    { name: 'Tech', value: r.techExperienceStr },
    { name: 'Referred by', value: r.referredByName },
    { name: 'Website', value: r.website },
    { name: 'Setup details', value: r.setupDetails, inline: false },
    { name: 'Background details', value: r.backgroundDetails, inline: false },
    { name: 'About', value: r.about, inline: false },
  ];
}

editorRouter.post('/api/editor-requests', async (c) => {
  let data: Record<string, unknown>;
  try {
    data = (await c.req.json()) as Record<string, unknown>;
  } catch {
    return c.json({ error: 'Invalid JSON data' }, 400);
  }
  if (!data || typeof data !== 'object') {
    return c.json({ error: 'Invalid JSON data' }, 400);
  }

  const email = ((data['email'] as string | undefined) ?? '').trim().toLowerCase();
  // A real Discord id is a numeric snowflake. Anything else — most importantly a
  // Supabase user UUID, which an email signup used to send here — is dropped, so
  // an email account can never be recorded (or displayed) as Discord-linked.
  const rawDiscordId = ((data['discord_id'] as string | undefined) ?? '').trim();
  const discordId = /^\d{15,20}$/.test(rawDiscordId) ? rawDiscordId : '';
  const website = data['website'] ? String(data['website']).trim() : null;
  const about = data['about'] ? String(data['about']).trim() : null;
  const region = data['region'] ? String(data['region']).trim() : null;
  const background = data['background'] ? String(data['background']).trim() : null;
  const backgroundDetails = data['background_details'] ? String(data['background_details']).trim() : null;
  const hasExistingSetup = data['has_existing_setup'] ? String(data['has_existing_setup']).trim() : null;
  const setupDetails = data['setup_details'] ? String(data['setup_details']).trim() : null;
  const requestType = asArrayOfString(data['request_type']);
  const techExperience = asArrayOfString(data['tech_experience']);

  let experienceLevel: number | null = null;
  const rawExp = data['experience_level'];
  if (rawExp !== undefined && rawExp !== null) {
    const n = typeof rawExp === 'number' ? rawExp : Number.parseInt(String(rawExp), 10);
    if (Number.isFinite(n) && n >= 1 && n <= 5) experienceLevel = n;
  }

  // Link the request to an existing account (Discord OAuth signup, or an email
  // signup's just-created account). ONLY from the verified JWT — never from the
  // body, or a submitter could bind someone else's account and have roles
  // granted to it on approval.
  const linkedUserIdRaw = c.get('userId');
  const linkedUserId =
    typeof linkedUserIdRaw === 'string' && linkedUserIdRaw.length > 0
      ? linkedUserIdRaw
      : null;

  // Email identifies an ANONYMOUS request. A JWT-linked signup is identified by
  // its account instead, so email is optional there — Discord OAuth accounts
  // don't always share a (verified) email, and without this the draft was
  // silently dropped, leaving an account with no signup request in the queue.
  if (!linkedUserId && (!email || !email.includes('@'))) {
    return c.json({ error: 'Valid email is required' }, 400);
  }
  // discord_id is no longer required: Discord OAuth signups carry the numeric id
  // automatically (from the linked identity), and email signups don't collect
  // one. Stored as '' when absent (column is NOT NULL).

  // A request is created only when the user submits the full form, so the
  // "about" text and at least one request type are always required.
  if (!about) {
    return c.json({ error: 'Please tell us about yourself' }, 400);
  }
  if (requestType.length === 0) {
    return c.json({ error: 'Please select at least one request type' }, 400);
  }

  try {
    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    const requestTypeStr = requestType.length > 0 ? requestType.join(',') : null;
    const techExperienceStr = techExperience.length > 0 ? techExperience.join(',') : null;
    const createdAt = Math.floor(Date.now() / 1000);

    // Referral attribution (best-effort — an unknown/invalid code, a
    // self-referral, or a lookup failure must never fail the signup).
    // Codes are stored uppercase; uppercasing the input makes them
    // case-insensitive to use. The lookup only fires for a plausibly
    // shaped code, so codeless submissions issue no extra query.
    let referredBy: string | null = null;
    let referredByName: string | null = null;
    const rawRefCode = typeof data['referral_code'] === 'string'
      ? data['referral_code'].trim().toUpperCase()
      : '';
    if (/^[A-Z0-9]{4,32}$/.test(rawRefCode)) {
      try {
        const rr = await pool.query<{ user_id: string }>(
          'SELECT user_id FROM referral_codes WHERE code = $1',
          [rawRefCode],
        );
        const refOwner = rr.rows[0]?.user_id ?? null;
        if (refOwner && refOwner !== linkedUserId) { // self-referral ignored
          referredBy = refOwner;
          // Resolved once at submit time (048 precedent); null is fine —
          // the staff UI falls back to a slice of the id.
          referredByName = await getUsername(refOwner);
        }
      } catch {
        /* attribution dropped, signup proceeds */
      }
    }

    // Find this person's existing request — their linked account first, then
    // email — preferring a pending one, so a re-submit updates it in place
    // rather than erroring/duplicating.
    const existing = await pool.query<{ id: number; status: string }>(
      `SELECT id, status FROM editor_requests
        WHERE ($1::text IS NOT NULL AND supabase_user_id = $1) OR email = $2
        ORDER BY (status = 'pending') DESC, created_at DESC
        LIMIT 1`,
      [linkedUserId, email],
    );
    const existingRow = existing.rows[0];

    // Update an existing pending request (re-submit), otherwise insert a new one.
    if (existingRow && existingRow.status === 'pending') {
      await pool.query(
        `UPDATE editor_requests SET
           email = $1, discord_id = $2, website = $3, about = $4, request_type = $5,
           region = $6, background = $7, background_details = $8, has_existing_setup = $9,
           setup_details = $10, tech_experience = $11, experience_level = $12,
           supabase_user_id = COALESCE($13, supabase_user_id),
           referred_by = COALESCE(referred_by, $14),
           referred_by_name = COALESCE(referred_by_name, $15)
         WHERE id = $16`,
        [email, discordId, website, about, requestTypeStr, region, background, backgroundDetails,
          hasExistingSetup, setupDetails, techExperienceStr, experienceLevel, linkedUserId,
          referredBy, referredByName, existingRow.id],
      );
      log.info({ requestId: existingRow.id, email, requestType, linkedUserId }, 'Editor request updated');
      notifyStaff(pool, {
        kind: 'signup_request',
        event: 'new',
        ref: String(existingRow.id),
        title: prettyRequestType(requestTypeStr),
        subtitle: region ? `${region} · resubmitted` : 'resubmitted',
        fields: signupFields({
          email, discordId, website, about, region, background, backgroundDetails,
          hasExistingSetup, setupDetails, techExperienceStr, experienceLevel,
          referredByName,
        }),
      });
      return c.json({ success: true, message: 'Request submitted successfully', request_id: existingRow.id }, 200);
    }

    const inserted = await pool.query<{ id: number }>(
      `INSERT INTO editor_requests
        (email, discord_id, website, about, request_type, region, background, background_details,
         has_existing_setup, setup_details, tech_experience, experience_level, status, created_at,
         supabase_user_id, referred_by, referred_by_name)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'pending',$13,$14,$15,$16)
       RETURNING id`,
      [email, discordId, website, about, requestTypeStr, region, background, backgroundDetails,
        hasExistingSetup, setupDetails, techExperienceStr, experienceLevel, createdAt,
        linkedUserId, referredBy, referredByName],
    );
    const requestId = inserted.rows[0]?.id;
    log.info({ requestId, email, requestType, linkedUserId }, 'New editor request');
    notifyStaff(pool, {
      kind: 'signup_request',
      event: 'new',
      ref: String(requestId ?? ''),
      title: prettyRequestType(requestTypeStr),
      subtitle: region || null,
      fields: signupFields({
        email, discordId, website, about, region, background, backgroundDetails,
        hasExistingSetup, setupDetails, techExperienceStr, experienceLevel,
        referredByName,
      }),
    });
    return c.json({ success: true, message: 'Request submitted successfully', request_id: requestId }, 201);
  } catch (err) {
    log.error({ err }, 'Error submitting editor request');
    return c.json({ error: 'Failed to submit request' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/editor-requests  — admin list
// ---------------------------------------------------------------------------
editorRouter.get('/api/editor-requests', requireRole(canManageUsers), async (c) => {
  try {
    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    const url = new URL(c.req.url);
    const statusFilter = url.searchParams.get('status');
    const r = statusFilter
      ? await pool.query<EditorRequestRow>(
          'SELECT * FROM editor_requests WHERE status = $1 ORDER BY created_at DESC',
          [statusFilter],
        )
      : await pool.query<EditorRequestRow>(
          'SELECT * FROM editor_requests ORDER BY created_at DESC',
        );
    const requestsList = r.rows.map(normaliseRequest);
    return c.json({ requests: requestsList, count: requestsList.length });
  } catch (err) {
    log.error({ err }, 'Error listing editor requests');
    return c.json({ error: 'Failed to list requests' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/editor-requests/:id/approve
// ---------------------------------------------------------------------------
async function fetchRequest(pool: Pool, requestId: number): Promise<EditorRequestRow | null> {
  const r = await pool.query<EditorRequestRow>(
    'SELECT * FROM editor_requests WHERE id = $1',
    [requestId],
  );
  return r.rows[0] ?? null;
}
editorRouter.post('/api/editor-requests/:id/approve', requireRole(canManageUsers), async (c) => {
  const requestIdRaw = c.req.param('id');
  if (!/^\d+$/.test(requestIdRaw)) {
    return c.json({ error: 'Request not found' }, 404);
  }
  const requestId = Number.parseInt(requestIdRaw, 10);
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    // Canonicalise legacy names to their current equivalents, then reject
    // anything not in the known set — user_roles.role is free-form TEXT, so
    // without this a typo'd/hand-crafted role name would be stored verbatim.
    const rawRoles = asArrayOfString(data['roles']);
    const unknown = rawRoles.filter((r) => !isKnownRole(r));
    if (unknown.length > 0) {
      return c.json({ error: `Unknown role(s): ${unknown.join(', ')}` }, 400);
    }
    // The roles actually requested/approved (used for the audit note).
    const approvedRoles = [...new Set(rawRoles.map(canonicalRole))];
    // Approving is what GRANTS access, so it must actually grant something —
    // otherwise the request is marked approved and the applicant still can't do
    // anything. The UI blocks this too; enforced here so a hand-crafted request
    // can't slip an empty approval through. Use Reject to decline instead.
    if (approvedRoles.length === 0) {
      return c.json({ error: 'Select at least one role to approve this request.' }, 400);
    }
    // Every account also carries the base 'authed' role (see migration 059) —
    // granted alongside so an approved user is complete even before their first
    // page load triggers /api/profiles/sync.
    const roles = [...new Set([...approvedRoles, 'authed'])];
    // Staff can approve with the feature roles only — assigning staff / owner
    // during approval is owner-only. The UI hides those checkboxes for staff;
    // this enforces it server-side so a hand-crafted request can't escalate.
    if (
      roles.some(isPrivilegedRole) &&
      !(await canAssignPrivilegedRoles(c.get('userId') as string))
    ) {
      return c.json(
        { error: 'Only owners can assign the staff or owner roles.' },
        403,
      );
    }

    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    const req = await fetchRequest(pool, requestId);
    if (!req) return c.json({ error: 'Request not found' }, 404);
    if (req.status !== 'pending') {
      return c.json({ error: `Request is already ${req.status}` }, 400);
    }

    let supabaseUserId: string | null = null;
    let rolesAssignedToLinked = false;
    let pendingFirstSignIn = false;

    // Request came from an already-signed-in account (Discord OAuth, or an
    // email signup whose confirmation had already landed): assign the roles
    // straight to it. Approval unlocks an existing login; it never makes one.
    if (req.supabase_user_id) {
      supabaseUserId = req.supabase_user_id;
      if (roles.length > 0) {
        try {
          for (const role of roles) {
            await pool.query(
              `INSERT INTO user_roles (user_id, role, granted_by, request_id)
               VALUES ($1, $2, 'system', $3)
               ON CONFLICT (user_id, role) DO NOTHING`,
              [supabaseUserId, role, requestId],
            );
          }
          invalidateUserRolesCache(supabaseUserId);
          rolesAssignedToLinked = true;
          log.info(
            { userId: supabaseUserId, roles },
            'Assigned roles to linked account',
          );
        } catch (roleErr) {
          log.warn(
            { err: (roleErr as Error).message },
            'Error inserting roles for linked account (non-fatal)',
          );
        }
      }
    } else {
      // No linked account. This is NOT a missing signup — it is the ordinary
      // shape of a request filed while email confirmation was still pending,
      // when there was no session to link. The account already exists in
      // Supabase with a password its owner chose.
      //
      // What used to happen here was a second Supabase account, created by us
      // with a generated password that staff then had to pass on by hand. For
      // an email that already had an account that call simply failed, so the
      // roles were never granted and the note recorded the error. The grant is
      // recorded on the request instead and claimed by verified email at the
      // person's first signed-in page load.
      pendingFirstSignIn = true;
      log.info(
        { requestId, email: req.email, roles },
        'Approved an unlinked request — roles wait for first sign-in',
      );
    }

    const reviewedAt = Math.floor(Date.now() / 1000);
    // Note lists the roles that were actually approved — the implicit base
    // 'authed' grant is noise in an audit trail.
    const rolesStr = approvedRoles.join(',');
    const accountOutcome = rolesAssignedToLinked
      ? `Roles assigned to linked account ${supabaseUserId}`
      : pendingFirstSignIn
        ? 'Awaiting first sign-in'
        : null;
    let notes = `Roles: ${rolesStr}`;
    if (accountOutcome) notes += ` | ${accountOutcome}`;

    // approved_roles is what the first-sign-in claim reads. Written for every
    // approval, not just the unlinked ones, so the record of what an approval
    // granted does not depend on which shape it took.
    await pool.query(
      `UPDATE editor_requests
       SET status = 'approved', reviewed_at = $1, notes = $2, approved_roles = $3
       WHERE id = $4`,
      [reviewedAt, notes, roles.join(','), requestId],
    );

    log.info({ requestId, email: req.email, roles }, 'Approved editor request');
    notifyStaff(pool, {
      kind: 'signup_request',
      event: 'resolved',
      ref: String(requestId),
      title: prettyRequestType(req.request_type),
      status: 'approved',
      actor: (c.get('userName') as string | undefined) ?? null,
      fields: [
        { name: 'Applicant', value: req.email },
        { name: 'Discord', value: req.discord_id },
        { name: 'Region', value: req.region },
        { name: 'Roles granted', value: roles.join(', ') },
        { name: 'Account', value: accountOutcome, inline: false },
      ],
    });

    const result: Record<string, unknown> = {
      success: true,
      email: req.email,
      discord_id: req.discord_id,
      // Report the roles that were APPROVED — the implicit base 'authed' grant
      // is an internal detail, not part of the approval decision.
      roles: approvedRoles,
      roles_assigned_to_linked_account: rolesAssignedToLinked,
      // The roles are recorded and will apply the moment this person signs in;
      // there is nothing for staff to send them.
      pending_first_sign_in: pendingFirstSignIn,
    };
    return c.json(result);
  } catch (err) {
    log.error({ err }, 'Error approving editor request');
    return c.json({ error: 'Failed to approve request' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/editor-requests/:id/reject
// ---------------------------------------------------------------------------
editorRouter.post('/api/editor-requests/:id/reject', requireRole(canManageUsers), async (c) => {
  const requestIdRaw = c.req.param('id');
  if (!/^\d+$/.test(requestIdRaw)) {
    return c.json({ error: 'Request not found' }, 404);
  }
  const requestId = Number.parseInt(requestIdRaw, 10);
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const reason = (data['reason'] as string | undefined) ?? '';

    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    const req = await fetchRequest(pool, requestId);
    if (!req) return c.json({ error: 'Request not found' }, 404);
    if (req.status !== 'pending') {
      return c.json({ error: `Request is already ${req.status}` }, 400);
    }

    const reviewedAt = Math.floor(Date.now() / 1000);
    await pool.query(
      `UPDATE editor_requests
       SET status = 'rejected', reviewed_at = $1, notes = $2
       WHERE id = $3`,
      [reviewedAt, reason || 'Rejected', requestId],
    );

    log.info({ requestId, email: req.email }, 'Rejected editor request');
    notifyStaff(pool, {
      kind: 'signup_request',
      event: 'resolved',
      ref: String(requestId),
      title: prettyRequestType(req.request_type),
      status: 'rejected',
      actor: (c.get('userName') as string | undefined) ?? null,
      fields: [
        { name: 'Applicant', value: req.email },
        { name: 'Region', value: req.region },
        { name: 'Reason', value: reason, inline: false },
      ],
    });
    return c.json({ success: true, message: 'Request rejected' });
  } catch (err) {
    log.error({ err }, 'Error rejecting editor request');
    return c.json({ error: 'Failed to reject request' }, 500);
  }
});

// ---------------------------------------------------------------------------
// DELETE /api/editor-requests/:id  — OWNER-only. Deletes a request of ANY
// status. Note: deleting an approved request does NOT revoke the roles it
// granted — user_roles rows are independent (request_id is a plain column, no
// FK), so roles persist and are managed separately in the Users tab.
// ---------------------------------------------------------------------------
editorRouter.delete('/api/editor-requests/:id', requireRole(isOwner), async (c) => {
  const requestIdRaw = c.req.param('id');
  if (!/^\d+$/.test(requestIdRaw)) {
    return c.json({ error: 'Request not found' }, 404);
  }
  const requestId = Number.parseInt(requestIdRaw, 10);
  try {
    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    const req = await fetchRequest(pool, requestId);
    if (!req) return c.json({ error: 'Request not found' }, 404);

    await pool.query('DELETE FROM editor_requests WHERE id = $1', [requestId]);
    log.info({ requestId, email: req.email, status: req.status }, 'Deleted editor request');
    return c.json({ success: true, message: 'Request deleted' });
  } catch (err) {
    log.error({ err }, 'Error deleting editor request');
    return c.json({ error: 'Failed to delete request' }, 500);
  }
});

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET /api/check-editor/:userId  — public, returns role booleans only.
// ---------------------------------------------------------------------------
editorRouter.get('/api/check-editor/:userId', async (c) => {
  const userId = c.req.param('userId');
  try {
    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    // Permission checks run against the ALIAS-EXPANDED list (legacy + current
    // names both resolve during the migration-059 cutover); the `roles` array
    // returned for display uses the raw stored names.
    const userRoles = await getUserRoles(userId);
    const rawRoles = canonicalRoles(userRoles);
    const isOwner = userRoles.includes('owner');
    const isStaff = userRoles.includes('staff');
    const isMapEditor = userRoles.includes('map:editor') || userRoles.includes('map:manager');
    const hasAccess = isMapEditor || isOwner;
    // Whether this account has ever submitted an editor request (linked by id).
    // The login guard uses no-roles + no-request to spot an "incomplete signup".
    const reqRes = await pool.query(
      'SELECT 1 FROM editor_requests WHERE supabase_user_id = $1 LIMIT 1',
      [userId],
    );
    const hasRequest = (reqRes.rowCount ?? 0) > 0;
    const isDataFeeder = userRoles.includes('feeder:agency_data');
    const isWireContributor = userRoles.includes('wire:contributor');
    const isWireManager = userRoles.includes('wire:manager');
    // Flag NAMES are kept stable (is_team_member, is_media_feeder, …) so every
    // existing frontend consumer keeps working across the rename.
    return c.json({
      user_id: userId,
      has_access: hasAccess,
      is_owner: isOwner,
      is_team_member: isStaff,
      is_map_editor: isMapEditor,
      is_data_feeder: isDataFeeder,
      is_media_feeder: isWireContributor,
      // Owner OR feeder:agency_data may edit agency reference tables (owner
      // instant, contributor via approval). Surfaced here so the public agency
      // page can show the edit controls without a second round-trip.
      can_edit_agency_data: isOwner || isDataFeeder,
      // Owner OR wire:contributor may publish to The Wire; owner|staff|
      // wire:manager may moderate any post. Surfaced so wire.html /
      // wire-compose.html can show the compose + remove controls directly.
      can_feed_media: isOwner || isWireContributor || isWireManager,
      can_moderate_wire: isOwner || isStaff || isWireManager,
      has_request: hasRequest,
      roles: rawRoles,
    });
  } catch (err) {
    log.error({ err, userId }, 'Error checking editor status');
    return c.json({ error: 'Failed to check editor status' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/check-admin/:userId
// ---------------------------------------------------------------------------
editorRouter.get('/api/check-admin/:userId', async (c) => {
  const userId = c.req.param('userId');
  try {
    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    // Checks use the alias-expanded list; `roles` (display) uses raw names.
    const userRoles = await getUserRoles(userId);
    const rawRoles = canonicalRoles(userRoles);
    let isOwner = userRoles.includes('owner');
    const isStaff = userRoles.includes('staff');
    const isNodeMonitor = userRoles.includes('feeder:monitor');
    const isFeederManager = userRoles.includes('feeder:manager');
    const isWireManager = userRoles.includes('wire:manager');
    const isMapManager = userRoles.includes('map:manager');
    // feeder:monitor is view-only: it can load the staff page (is_admin) purely
    // to reach the read-only Data + Nodes tabs. Managers likewise get in to
    // reach their own area's screens.
    let isAdmin = isOwner || isStaff || isNodeMonitor || isFeederManager || isWireManager || isMapManager;

    // First-run lockout-prevention: if no owner exists anywhere, grant
    // owner to the requesting user. Mirrors python at 13941-13955.
    if (!isAdmin) {
      const r = await pool.query<{ user_id: string }>(
        "SELECT user_id FROM user_roles WHERE role = 'owner'",
      );
      if (r.rows.length === 0) {
        log.warn({ userId }, 'No owners exist in system - granting first-time owner');
        isAdmin = true;
        isOwner = true;
      }
    }

    const isDataFeeder = userRoles.includes('feeder:agency_data');
    const canViewRequests = isOwner || isStaff || isWireManager || isFeederManager;
    const canViewUsers = isOwner || isStaff;
    // 'dev' was removed in the migration-059 refactor — the Dev tab is now
    // owner-only. is_dev stays in the payload (always false) so any frontend
    // still reading it keeps working.
    const canViewDev = isOwner;
    const canManageNodes = isOwner || isFeederManager;
    // Read access to the Data + Nodes pages. feeder:monitor gets the views but
    // NOT write access (can_manage_nodes stays owner|feeder:manager).
    const canViewNodeData = canManageNodes || isNodeMonitor;
    // Owner|staff|feeder:manager review agency data-change requests (Requests
    // tab dropdown). Owner|feeder:agency_data EDIT the agency tables.
    const canReviewAgencyData = isOwner || isStaff || isFeederManager;
    const canEditAgencyData = isOwner || isDataFeeder;

    return c.json({
      user_id: userId,
      is_admin: isAdmin,
      is_owner: isOwner,
      is_team_member: isStaff,
      is_dev: false,
      is_node_monitor: isNodeMonitor,
      is_data_feeder: isDataFeeder,
      can_manage_users: canViewUsers,
      can_manage_nodes: canManageNodes,
      can_assign_privileged_roles: isOwner,
      can_review_agency_data: canReviewAgencyData,
      can_edit_agency_data: canEditAgencyData,
      // Wire moderation queue (Requests → Wire approvals/takedowns).
      can_moderate_wire: isOwner || isStaff || isWireManager,
      tabs: {
        requests: canViewRequests,
        users: canViewUsers,
        dev: canViewDev,
        nodes: canViewNodeData,
        data: canViewNodeData,
        data_changes: canReviewAgencyData,
      },
      roles: rawRoles,
    });
  } catch (err) {
    log.error({ err, userId }, 'Error checking admin status');
    return c.json({ error: 'Failed to check admin status' }, 500);
  }
});

// Re-exported only so the test for invalidation helpers can call it.
export { invalidateUserRolesCache };
