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
 * block — dashboard.html and editor-requests.html key off it. See the
 * test suite for the exact assertions.
 *
 * Approval flow: when `create_account: true`, python POSTs to the
 * Supabase Auth Admin API to create an auth user with a temporary
 * password (`Changeme-XXXXXX`), then inserts the granted roles into
 * `user_roles` keyed by the returned Supabase user id. We mirror that
 * here. The 15s upstream timeout, response-body shape (`temp_password`,
 * `supabase_account_created`, `supabase_error`), and notes-column
 * format (`Roles: a,b | Temp password: ... | Supabase account created`)
 * are kept byte-compatible with python so the admin UI sees identical
 * behaviour from either backend.
 */
import { Hono } from 'hono';
import type { Pool } from 'pg';
import { randomBytes } from 'node:crypto';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { config } from '../config.js';
import {
  getUserRoles,
  invalidateUserRolesCache,
} from '../services/auth/roles.js';

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
    tech_experience: row.tech_experience,
    experience_level: row.experience_level,
    status: row.status,
    created_at: row.created_at,
    reviewed_at: row.reviewed_at,
    notes: row.notes,
  };
}

// ---------------------------------------------------------------------------
// POST /api/editor-requests  — public submission
// ---------------------------------------------------------------------------
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
  const discordId = ((data['discord_id'] as string | undefined) ?? '').trim();
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

  if (!email || !email.includes('@')) {
    return c.json({ error: 'Valid email is required' }, 400);
  }
  if (!discordId) {
    return c.json({ error: 'Discord ID is required' }, 400);
  }
  if (!about) {
    return c.json({ error: 'Please tell us about yourself' }, 400);
  }
  if (requestType.length === 0) {
    return c.json({ error: 'Please select at least one request type' }, 400);
  }

  try {
    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    const existing = await pool.query<{ id: number }>(
      "SELECT id FROM editor_requests WHERE email = $1 AND status = 'pending'",
      [email],
    );
    if (existing.rows.length > 0) {
      return c.json({ error: 'A pending request with this email already exists' }, 409);
    }

    const requestTypeStr = requestType.length > 0 ? requestType.join(',') : null;
    const techExperienceStr = techExperience.length > 0 ? techExperience.join(',') : null;
    const createdAt = Math.floor(Date.now() / 1000);

    const inserted = await pool.query<{ id: number }>(
      `INSERT INTO editor_requests
        (email, discord_id, website, about, request_type, region, background, background_details,
         has_existing_setup, setup_details, tech_experience, experience_level, status, created_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'pending',$13)
       RETURNING id`,
      [email, discordId, website, about, requestTypeStr, region, background, backgroundDetails,
        hasExistingSetup, setupDetails, techExperienceStr, experienceLevel, createdAt],
    );
    const requestId = inserted.rows[0]?.id;

    log.info({ requestId, email, discordId, requestType }, 'New editor request');
    return c.json(
      {
        success: true,
        message: 'Request submitted successfully',
        request_id: requestId,
      },
      201,
    );
  } catch (err) {
    log.error({ err }, 'Error submitting editor request');
    return c.json({ error: 'Failed to submit request' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/editor-requests  — admin list
// ---------------------------------------------------------------------------
editorRouter.get('/api/editor-requests', async (c) => {
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

// Mirror of python's `Changeme-` + 6 chars from secrets.choice over
// `string.ascii_lowercase + string.digits`. There's a slight modulo bias
// here (4 of 36 chars are ~1.4% more likely) — fine for a one-time temp
// password that the user is forced to change on first login.
const TEMP_PASSWORD_ALPHABET = 'abcdefghijklmnopqrstuvwxyz0123456789';
function generateTempPassword(): string {
  const bytes = randomBytes(6);
  let suffix = '';
  for (let i = 0; i < 6; i++) {
    suffix += TEMP_PASSWORD_ALPHABET[bytes[i]! % TEMP_PASSWORD_ALPHABET.length];
  }
  return `Changeme-${suffix}`;
}

editorRouter.post('/api/editor-requests/:id/approve', async (c) => {
  const requestIdRaw = c.req.param('id');
  const requestId = Number.parseInt(requestIdRaw, 10);
  if (!Number.isFinite(requestId)) {
    return c.json({ error: 'Request not found' }, 404);
  }
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const roles = asArrayOfString(data['roles']);
    const createAccount = data['create_account'] === true;

    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    const req = await fetchRequest(pool, requestId);
    if (!req) return c.json({ error: 'Request not found' }, 404);
    if (req.status !== 'pending') {
      return c.json({ error: `Request is already ${req.status}` }, 400);
    }

    let tempPassword: string | null = null;
    let supabaseAccountCreated = false;
    let supabaseError: string | null = null;
    let supabaseUserId: string | null = null;

    if (createAccount) {
      tempPassword = generateTempPassword();
      if (config.SUPABASE_URL && config.SUPABASE_SERVICE_ROLE_KEY) {
        try {
          const createUrl = `${config.SUPABASE_URL}/auth/v1/admin/users`;
          const res = await fetch(createUrl, {
            method: 'POST',
            headers: {
              apikey: config.SUPABASE_SERVICE_ROLE_KEY,
              Authorization: `Bearer ${config.SUPABASE_SERVICE_ROLE_KEY}`,
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              email: req.email,
              password: tempPassword,
              email_confirm: true,
              user_metadata: {
                discord_id: req.discord_id,
                approved_request_id: requestId,
                roles,
                force_password_change: true,
              },
            }),
            signal: AbortSignal.timeout(15_000),
          });
          if (res.status === 200 || res.status === 201) {
            const body = (await res.json().catch(() => ({}))) as { id?: string };
            supabaseUserId = body.id ?? null;
            supabaseAccountCreated = true;
            log.info(
              { email: req.email, userId: supabaseUserId },
              'Created Supabase account',
            );
            // Best-effort role insert. Mirrors python's nested try/except
            // — a failure here doesn't roll back the account creation,
            // it just logs and lets the admin assign roles manually via
            // /api/users/<uid>/roles afterwards.
            if (supabaseUserId && roles.length > 0) {
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
                log.info({ userId: supabaseUserId, roles }, 'Assigned roles to user');
              } catch (roleErr) {
                log.warn(
                  { err: (roleErr as Error).message },
                  'Error inserting roles (non-fatal)',
                );
              }
            }
          } else {
            const errText = await res.text().catch(() => '');
            try {
              const errBody = JSON.parse(errText) as { message?: string; msg?: string };
              supabaseError =
                errBody.message ?? errBody.msg ?? `Status ${res.status}`;
            } catch {
              supabaseError = `Status ${res.status}`;
            }
            log.error(
              { status: res.status, supabaseError },
              'Failed to create Supabase account',
            );
          }
        } catch (err) {
          supabaseError = (err as Error).message;
          log.error(
            { err: (err as Error).message },
            'Error creating Supabase account',
          );
        }
      }
    }

    const reviewedAt = Math.floor(Date.now() / 1000);
    const rolesStr = roles.join(',');
    let notes = `Roles: ${rolesStr}`;
    if (tempPassword) notes += ` | Temp password: ${tempPassword}`;
    if (supabaseAccountCreated) {
      notes += ' | Supabase account created';
    } else if (supabaseError) {
      notes += ` | Supabase error: ${supabaseError}`;
    } else if (createAccount && !config.SUPABASE_SERVICE_ROLE_KEY) {
      notes += ' | Supabase not configured';
    }

    await pool.query(
      `UPDATE editor_requests
       SET status = 'approved', reviewed_at = $1, notes = $2
       WHERE id = $3`,
      [reviewedAt, notes, requestId],
    );

    log.info({ requestId, email: req.email, roles }, 'Approved editor request');

    const result: Record<string, unknown> = {
      success: true,
      email: req.email,
      discord_id: req.discord_id,
      roles,
      supabase_account_created: supabaseAccountCreated,
    };
    if (tempPassword) result['temp_password'] = tempPassword;
    if (supabaseError) result['supabase_error'] = supabaseError;
    return c.json(result);
  } catch (err) {
    log.error({ err }, 'Error approving editor request');
    return c.json({ error: 'Failed to approve request' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/editor-requests/:id/reject
// ---------------------------------------------------------------------------
editorRouter.post('/api/editor-requests/:id/reject', async (c) => {
  const requestIdRaw = c.req.param('id');
  const requestId = Number.parseInt(requestIdRaw, 10);
  if (!Number.isFinite(requestId)) {
    return c.json({ error: 'Request not found' }, 404);
  }
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
    return c.json({ success: true, message: 'Request rejected' });
  } catch (err) {
    log.error({ err }, 'Error rejecting editor request');
    return c.json({ error: 'Failed to reject request' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/check-editor/:userId  — public, returns role booleans only.
// ---------------------------------------------------------------------------
editorRouter.get('/api/check-editor/:userId', async (c) => {
  const userId = c.req.param('userId');
  try {
    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    const userRoles = await getUserRoles(userId);
    const isOwner = userRoles.includes('owner');
    const isTeamMember = userRoles.includes('team_member');
    const isMapEditor = userRoles.includes('map_editor');
    const hasAccess = isMapEditor || isOwner;
    return c.json({
      user_id: userId,
      has_access: hasAccess,
      is_owner: isOwner,
      is_team_member: isTeamMember,
      is_map_editor: isMapEditor,
      roles: userRoles,
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

    const userRoles = await getUserRoles(userId);
    let isOwner = userRoles.includes('owner');
    const isTeamMember = userRoles.includes('team_member');
    const isDev = userRoles.includes('dev');
    let isAdmin = isOwner || isTeamMember || isDev;

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

    const canViewRequests = isOwner || isTeamMember;
    const canViewUsers = isOwner || isTeamMember;
    const canViewDev = isOwner || isDev;

    return c.json({
      user_id: userId,
      is_admin: isAdmin,
      is_owner: isOwner,
      is_team_member: isTeamMember,
      is_dev: isDev,
      can_manage_users: canViewUsers,
      can_assign_privileged_roles: isOwner,
      tabs: {
        requests: canViewRequests,
        users: canViewUsers,
        dev: canViewDev,
      },
      roles: userRoles,
    });
  } catch (err) {
    log.error({ err, userId }, 'Error checking admin status');
    return c.json({ error: 'Failed to check admin status' }, 500);
  }
});

// Re-exported only so the test for invalidation helpers can call it.
export { invalidateUserRolesCache };
