/**
 * Agency reference data ("Extended" tables) + row-edit workflow.
 *
 * Reads (public — replaces the old static agency-extended.json):
 *   GET  /api/agency/extended        — all agencies
 *   GET  /api/agency/extended/:slug  — one agency
 *
 * Edits (auth + role gated):
 *   POST /api/agency/changes                 — propose add/update/delete a row.
 *       owner  → applied instantly (status approved)
 *       data_feeder → pending data-change request
 *   GET  /api/agency/changes?status=pending  — list requests (owner|team_member)
 *   POST /api/agency/changes/:id/approve      — apply + mark approved
 *   POST /api/agency/changes/:id/reject       — mark rejected (no apply)
 */
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import {
  requireRole,
  isOwner,
  canEditAgencyData,
  canReviewAgencyData,
} from '../services/auth/roles.js';
import { getUsername } from './users.js';
import {
  getAllAgencyExtended,
  getAgencyExtended,
  findEditableSection,
  applyRowChange,
  type RowChange,
} from '../services/agencyData.js';

export const agencyRouter = new Hono();

// ── reads (public) ──────────────────────────────────────────────────────────

agencyRouter.get('/api/agency/extended', async (c) => {
  c.header('Cache-Control', 'public, max-age=60');
  return c.json({ agencies: await getAllAgencyExtended() });
});

agencyRouter.get('/api/agency/extended/:slug', async (c) => {
  const slug = c.req.param('slug');
  const agency = await getAgencyExtended(slug);
  if (!agency) return c.json({ error: 'unknown agency' }, 404);
  c.header('Cache-Control', 'public, max-age=60');
  return c.json({ slug, ...agency });
});

// ── row edits ───────────────────────────────────────────────────────────────

const OPS = new Set(['add', 'update', 'delete']);

interface ChangeRow {
  id: number;
  slug: string;
  section_key: string;
  section_title: string | null;
  op: 'add' | 'update' | 'delete';
  row_key: string | null;
  before_cells: string[] | null;
  after_cells: string[] | null;
  status: string;
  created_by: string;
  created_by_name: string | null;
  reviewed_by: string | null;
  reviewed_by_name: string | null;
  created_at: Date;
  reviewed_at: Date | null;
}

function mapChange(r: ChangeRow) {
  return {
    id: r.id,
    slug: r.slug,
    sectionKey: r.section_key,
    sectionTitle: r.section_title,
    op: r.op,
    rowKey: r.row_key,
    beforeCells: r.before_cells,
    afterCells: r.after_cells,
    status: r.status,
    createdBy: r.created_by,
    createdByName: r.created_by_name,
    reviewedBy: r.reviewed_by,
    reviewedByName: r.reviewed_by_name,
    createdAt: r.created_at instanceof Date ? r.created_at.toISOString() : String(r.created_at),
    reviewedAt: r.reviewed_at instanceof Date ? r.reviewed_at.toISOString() : r.reviewed_at,
  };
}

const cellsOrNull = (v: unknown): string[] | null =>
  Array.isArray(v) ? v.map((x) => String(x ?? '')) : null;

// Propose a row change. Owner → applied now; data_feeder → pending request.
agencyRouter.post('/api/agency/changes', requireRole(canEditAgencyData), async (c) => {
  const userId = c.get('userId') as string;
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);

  let body: Record<string, unknown>;
  try {
    body = (await c.req.json()) as Record<string, unknown>;
  } catch {
    return c.json({ error: 'invalid JSON body' }, 400);
  }

  const slug = String(body.slug ?? '').trim();
  const sectionKey = String(body.sectionKey ?? '').trim();
  const op = String(body.op ?? '').trim();
  const rowKey = body.rowKey != null ? String(body.rowKey) : null;
  const afterCells = cellsOrNull(body.afterCells);
  const beforeCells = cellsOrNull(body.beforeCells);

  if (!slug || !sectionKey || !OPS.has(op)) {
    return c.json({ error: 'slug, sectionKey and a valid op (add|update|delete) are required' }, 400);
  }
  if ((op === 'add' || op === 'update') && !afterCells) {
    return c.json({ error: 'afterCells required for add/update' }, 400);
  }
  if ((op === 'update' || op === 'delete') && !rowKey) {
    return c.json({ error: 'rowKey required for update/delete' }, 400);
  }

  const agency = await getAgencyExtended(slug);
  if (!agency) return c.json({ error: 'unknown agency' }, 404);
  const section = findEditableSection(agency, sectionKey);
  if (!section) return c.json({ error: 'unknown or non-editable section' }, 400);
  const sectionTitle = typeof section.title === 'string' ? section.title : null;

  const owner = await isOwner(userId);
  const name = await getUsername(userId).catch(() => null);
  const change: RowChange = {
    sectionKey,
    op: op as RowChange['op'],
    rowKey: rowKey ?? undefined,
    afterCells: afterCells ?? undefined,
  };

  try {
    if (owner) {
      const err = await applyRowChange(pool, slug, change);
      if (err) return c.json({ error: err }, 400);
      const ins = await pool.query<{ id: number }>(
        `INSERT INTO agency_data_change
           (slug, section_key, section_title, op, row_key, before_cells, after_cells,
            status, created_by, created_by_name, reviewed_by, reviewed_by_name, reviewed_at)
         VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,'approved',$8,$9,$8,$9, now())
         RETURNING id`,
        [slug, sectionKey, sectionTitle, op, rowKey,
          beforeCells ? JSON.stringify(beforeCells) : null,
          afterCells ? JSON.stringify(afterCells) : null, userId, name],
      );
      return c.json({ applied: true, id: ins.rows[0]?.id });
    }

    // data_feeder → pending request (validated at approval time).
    const ins = await pool.query<{ id: number }>(
      `INSERT INTO agency_data_change
         (slug, section_key, section_title, op, row_key, before_cells, after_cells,
          status, created_by, created_by_name)
       VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,'pending',$8,$9)
       RETURNING id`,
      [slug, sectionKey, sectionTitle, op, rowKey,
        beforeCells ? JSON.stringify(beforeCells) : null,
        afterCells ? JSON.stringify(afterCells) : null, userId, name],
    );
    return c.json({ applied: false, pending: true, id: ins.rows[0]?.id });
  } catch (err) {
    log.error({ err, slug, op }, 'agency change insert failed');
    return c.json({ error: 'failed to record change' }, 500);
  }
});

// List data-change requests (reviewers).
agencyRouter.get('/api/agency/changes', requireRole(canReviewAgencyData), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  const raw = (new URL(c.req.url).searchParams.get('status') ?? 'pending').toLowerCase();
  const status = ['pending', 'approved', 'rejected', 'all'].includes(raw) ? raw : 'pending';
  try {
    const res =
      status === 'all'
        ? await pool.query<ChangeRow>('SELECT * FROM agency_data_change ORDER BY created_at DESC LIMIT 200')
        : await pool.query<ChangeRow>(
            'SELECT * FROM agency_data_change WHERE status = $1 ORDER BY created_at DESC LIMIT 200',
            [status],
          );
    const pendingCount = (
      await pool.query<{ n: string }>("SELECT COUNT(*)::int AS n FROM agency_data_change WHERE status='pending'")
    ).rows[0]?.n;
    return c.json({ changes: res.rows.map(mapChange), pendingCount: Number(pendingCount ?? 0) });
  } catch (err) {
    log.error({ err }, 'agency changes list failed');
    return c.json({ error: 'failed to load changes' }, 500);
  }
});

// Approve → apply + mark approved.
agencyRouter.post('/api/agency/changes/:id/approve', requireRole(canReviewAgencyData), async (c) => {
  const userId = c.get('userId') as string;
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id)) return c.json({ error: 'bad id' }, 400);
  try {
    const { rows } = await pool.query<ChangeRow>('SELECT * FROM agency_data_change WHERE id = $1', [id]);
    const ch = rows[0];
    if (!ch) return c.json({ error: 'not found' }, 404);
    if (ch.status !== 'pending') return c.json({ error: `already ${ch.status}` }, 409);
    const err = await applyRowChange(pool, ch.slug, {
      sectionKey: ch.section_key,
      op: ch.op,
      rowKey: ch.row_key ?? undefined,
      afterCells: ch.after_cells ?? undefined,
    });
    if (err) return c.json({ error: err }, 400); // stays pending so the reviewer can see why
    const name = await getUsername(userId).catch(() => null);
    await pool.query(
      "UPDATE agency_data_change SET status='approved', reviewed_by=$2, reviewed_by_name=$3, reviewed_at=now() WHERE id=$1",
      [id, userId, name],
    );
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err, id }, 'agency change approve failed');
    return c.json({ error: 'failed to approve' }, 500);
  }
});

// Reject → mark rejected (no data change).
agencyRouter.post('/api/agency/changes/:id/reject', requireRole(canReviewAgencyData), async (c) => {
  const userId = c.get('userId') as string;
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id)) return c.json({ error: 'bad id' }, 400);
  try {
    const name = await getUsername(userId).catch(() => null);
    const res = await pool.query(
      "UPDATE agency_data_change SET status='rejected', reviewed_by=$2, reviewed_by_name=$3, reviewed_at=now() WHERE id=$1 AND status='pending'",
      [id, userId, name],
    );
    if ((res.rowCount ?? 0) === 0) return c.json({ error: 'not found or already reviewed' }, 409);
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err, id }, 'agency change reject failed');
    return c.json({ error: 'failed to reject' }, 500);
  }
});
