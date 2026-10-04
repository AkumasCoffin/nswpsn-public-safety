// RF site-survey orchestration.
//
// The backend owns everything except the RF: it selects candidate GRN sites
// from the node's LGA neighbourhood (grnCandidates), commands the agent to
// measure them, records every result — pass, fail, no lock, unmeasured, and
// the sites skipped for unparseable control channels — applies the pass
// threshold, appends the winners to the node's channel config, and tells
// staff on Discord. The agent only measures and reports raw medians, so the
// threshold can change without an agent release.
//
// Lifecycle: startSurvey inserts a 'running' row + skipped rows and sends
// cmd{surveySites}; the agent acks instantly and works in the background;
// results arrive on POST /api/node-ingest/site-survey → ingestSurveyResults
// finishes the row. A survey 'running' past SURVEY_TIMEOUT_MS is lazily
// marked timeout whenever anything looks at it.
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { hub } from './nodes/hub.js';
import { getNode, type NodeRow } from './nodes/registry.js';
import { pushConfigToNode } from './nodes/configPush.js';
import { notifyStaff } from './staffNotify.js';
import { candidatesForNode, type SurveyCandidate } from './grnCandidates.js';

/** Median decode a candidate needs to be auto-added to the channel list. */
export const SURVEY_PASS_PCT = 65;
/** A survey still 'running' after this long is dead (agent crashed/offline). */
const SURVEY_TIMEOUT_MS = 45 * 60_000;
/** config_override.channels hard cap (ConfigOverrideSchema). */
const CHANNEL_CAP = 64;

export interface SurveyRow {
  id: number;
  node_id: string;
  started_at: Date;
  finished_at: Date | null;
  trigger: string;
  status: string;
  requested_by: string | null;
  rings: number;
  candidates: number;
  added: number | null;
  note: string | null;
}

// ---------------------------------------------------------------------------
// start
// ---------------------------------------------------------------------------

/**
 * Start a survey.
 *
 * `rings` is how far past the node's own council area to look: 0 is that area
 * alone, 1 adds its direct neighbours, and so on. A node's FIRST survey uses
 * 0 — the sites it exists to hear are the ones around it, and testing a whole
 * neighbourhood unasked costs a measurement window per site for candidates it
 * was never going to reach. Going wider is something a person asks for.
 */
export async function startSurvey(
  nodeId: string,
  trigger: 'install' | 'manual',
  requestedBy: string | null,
  rings = 1,
): Promise<{ ok: boolean; error?: string; surveyId?: number }> {
  const pool = await getPool();
  if (!pool) return { ok: false, error: 'database unavailable' };
  const node = await getNode(nodeId);
  if (!node) return { ok: false, error: 'node not found' };
  if (node.kind !== 'radio') return { ok: false, error: 'site surveys are radio-only' };
  if (!hub.isOnline(nodeId)) return { ok: false, error: 'node is offline' };
  if (!node.lga) return { ok: false, error: 'node has no LGA set — set its location first' };

  await expireStaleSurveys(pool, nodeId);
  const running = await pool.query(
    `SELECT id FROM node_site_surveys WHERE node_id = $1 AND status = 'running' LIMIT 1`,
    [nodeId],
  );
  if ((running.rowCount ?? 0) > 0) return { ok: false, error: 'a survey is already running' };

  const { candidates, skipped } = await candidatesForNode(node, rings);
  if (candidates.length === 0) {
    return {
      ok: false,
      error: rings === 0
        ? `no testable GRN sites in "${node.lga}" (${skipped.length} skipped) — widen the search to include neighbouring areas`
        : `no testable GRN sites found for LGA "${node.lga}" (${skipped.length} skipped)`,
    };
  }

  const ins = await pool.query<{ id: number }>(
    `INSERT INTO node_site_surveys (node_id, trigger, requested_by, rings, candidates)
     VALUES ($1, $2, $3, $4, $5) RETURNING id`,
    [nodeId, trigger, requestedBy, rings, candidates.length],
  );
  const surveyId = ins.rows[0]!.id;
  for (const s of skipped) {
    await pool.query(
      `INSERT INTO node_site_survey_results (survey_id, site_name, grn_key, verdict, note)
       VALUES ($1, $2, $3, 'skipped', $4)`,
      [surveyId, s.name, s.grnKey, s.note],
    );
  }

  const res = await hub.sendCmd(nodeId, 'surveySites', {
    surveyId,
    candidates: candidates.map((s) => ({ name: s.name, grnKey: s.grnKey, mhz: s.mhz, altMhz: s.altMhz })),
  });
  if (!res.ok) {
    await pool.query(
      `UPDATE node_site_surveys SET status = 'failed', finished_at = now(), note = $2 WHERE id = $1`,
      [surveyId, res.message ?? 'agent refused the survey'],
    );
    return { ok: false, error: res.message ?? 'agent refused the survey' };
  }
  _openSurveys.add(nodeId);
  log.info({ nodeId, surveyId, trigger, rings, candidates: candidates.length, skipped: skipped.length }, 'site survey started');
  return { ok: true, surveyId };
}

/**
 * Nodes this process believes have a survey open, so a status frame can be
 * judged without a query. Empty after a restart, which simply falls back to
 * the lazy timeout — it is a fast path, not a source of truth.
 */
const _openSurveys = new Set<string>();
/**
 * How long a node is allowed to report no survey before its record is treated
 * as lost. Generous: a survey's first act is a config import, and the node
 * reports progress from the moment it accepts the command, so this only has to
 * outlast a slow import plus a heartbeat.
 */
const SURVEY_SILENCE_MS = 3 * 60_000;

/**
 * Called on every status frame from a radio node: `surveying` is whether the
 * node says it is running one.
 *
 * A survey lives in the agent's memory only, so a node that restarts loses it
 * silently and the record would sit open until the timeout — showing staff a
 * survey running on a node that is plainly just decoding. The node not
 * claiming one, well past the point where it would have, is the answer.
 */
export function noteSurveyStatus(nodeId: string, surveying: boolean): void {
  if (surveying) {
    _openSurveys.add(nodeId);
    return;
  }
  if (!_openSurveys.has(nodeId)) return;
  void (async () => {
    try {
      const pool = await getPool();
      if (!pool) return;
      const r = await pool.query(
        `UPDATE node_site_surveys SET status = 'failed', finished_at = now(),
                note = 'the node stopped reporting it — most likely it restarted'
          WHERE node_id = $1 AND status = 'running'
            AND started_at < now() - ($2 || ' milliseconds')::interval`,
        [nodeId, String(SURVEY_SILENCE_MS)],
      );
      if ((r.rowCount ?? 0) > 0) {
        _openSurveys.delete(nodeId);
        log.info({ nodeId }, 'closed a survey record the node no longer reports');
      }
    } catch (err) {
      log.warn({ err, nodeId }, 'noteSurveyStatus failed');
    }
  })();
}

/** First-install hook, called from the hello handler. All guards inside;
 *  never throws. */
export async function maybeStartInstallSurvey(node: NodeRow): Promise<void> {
  try {
    if (node.kind !== 'radio') return;
    const channels = (node.config_override as { channels?: unknown[] } | null)?.channels ?? [];
    if (Array.isArray(channels) && channels.length > 0) return;
    if (!node.lga) {
      log.info({ nodeId: node.id }, 'install survey skipped: node has no LGA');
      return;
    }
    const pool = await getPool();
    if (!pool) return;
    const prior = await pool.query(
      `SELECT id FROM node_site_surveys WHERE node_id = $1 LIMIT 1`,
      [node.id],
    );
    if ((prior.rowCount ?? 0) > 0) return; // one automatic shot per node, ever
    const r = await startSurvey(node.id, 'install', null, 0);
    if (!r.ok) log.info({ nodeId: node.id, error: r.error }, 'install survey not started');
  } catch (err) {
    log.warn({ err, nodeId: node.id }, 'install survey trigger failed');
  }
}

// ---------------------------------------------------------------------------
// results
// ---------------------------------------------------------------------------

export interface AgentSurveyResult {
  grnKey: string | null;
  siteName: string;
  freqHz: number;
  altFreqHz: number | null;
  /** 'measured' | 'noLock' | 'unmeasured' from the agent's point of view. */
  outcome: string;
  medianPct: number | null;
  samples: number;
  signalDbfs: number | null;
  /** True when this row measured a site's ALT control channel. */
  isAlt: boolean;
}

export async function ingestSurveyResults(
  nodeId: string,
  surveyId: number,
  results: AgentSurveyResult[],
  agentNote: string | null,
  aborted = false,
): Promise<{ ok: boolean; error?: string; added?: number }> {
  const pool = await getPool();
  if (!pool) return { ok: false, error: 'database unavailable' };
  const sv = await pool.query<SurveyRow>(
    `SELECT * FROM node_site_surveys WHERE id = $1 AND node_id = $2`,
    [surveyId, nodeId],
  );
  const survey = sv.rows[0];
  if (!survey) return { ok: false, error: 'unknown survey' };
  if (survey.status !== 'running') return { ok: false, error: `survey is ${survey.status}` };

  // Fold alt-CC measurements into their primary site: the primary row decides
  // pass/fail; a PASSING alt rides along as an extra control frequency.
  const primaries = results.filter((r) => !r.isAlt);
  const altPassByName = new Map<string, number>();
  for (const r of results) {
    if (r.isAlt && r.outcome === 'measured' && (r.medianPct ?? 0) >= SURVEY_PASS_PCT) {
      altPassByName.set(r.siteName, r.freqHz);
    }
  }

  // An aborted survey (cancelled, or a config push wiped the test channels
  // mid-flight) still records what it measured — that is the audit — but adds
  // nothing: a partial pass list applied silently would look like a finished
  // survey that found little.
  const winners: Array<{ result: AgentSurveyResult; altHz: number | null }> = [];
  for (const r of primaries) {
    const measured = r.outcome === 'measured' && r.medianPct !== null;
    const pass = measured && (r.medianPct as number) >= SURVEY_PASS_PCT;
    const verdict = pass ? 'pass' : measured ? 'fail' : r.outcome === 'noLock' ? 'noLock' : 'unmeasured';
    await pool.query(
      `INSERT INTO node_site_survey_results
         (survey_id, site_name, grn_key, freq_hz, alt_freq_hz, verdict, median_pct, samples, signal_dbfs)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [surveyId, r.siteName, r.grnKey, r.freqHz, r.altFreqHz, verdict, r.medianPct, r.samples, r.signalDbfs],
    );
    if (pass && !aborted) winners.push({ result: r, altHz: altPassByName.get(r.siteName) ?? null });
  }

  const added = await appendWinnerChannels(pool, nodeId, surveyId, winners);

  await pool.query(
    `UPDATE node_site_surveys SET status = $4, finished_at = now(), added = $2, note = $3 WHERE id = $1`,
    [surveyId, added, agentNote, aborted ? 'failed' : 'done'],
  );

  const node = await getNode(nodeId);
  const top = primaries
    .filter((r) => r.medianPct !== null)
    .sort((a, b) => (b.medianPct ?? 0) - (a.medianPct ?? 0))
    .slice(0, 8);
  notifyStaff(pool, {
    kind: 'siteSurvey',
    event: 'new',
    ref: String(surveyId),
    title: `Site survey · ${node?.name ?? nodeId.slice(0, 8)}`,
    subtitle: `${survey.trigger === 'install' ? 'First install' : 'Manual'} · ${primaries.length} sites tested · ${winners.length} passed · ${added} added`,
    status: 'done',
    fields: top.map((r) => ({
      name: r.siteName,
      value: `${Math.round(r.medianPct ?? 0)}% @ ${(r.freqHz / 1e6).toFixed(4)} MHz${(r.medianPct ?? 0) >= SURVEY_PASS_PCT ? ' ✓' : ''}`,
      inline: true,
    })),
  });
  log.info({ nodeId, surveyId, tested: primaries.length, winners: winners.length, added }, 'site survey complete');
  return { ok: true, added };
}

/**
 * Handle a staff survey command from EITHER entry point — the REST
 * /api/nodes/:id/cmd route or the staff WebSocket — and return the result to
 * relay back, or null when the action is not a survey verb.
 *
 * It lives here rather than in a route because the candidate list is chosen
 * SERVER-SIDE: the browser sends only how far to look, never which sites to
 * tune. Both callers must behave identically, and the one that forwarded the
 * browser's arguments straight to the agent sent it a survey with no
 * candidates at all.
 */
export async function handleSurveyCommand(
  nodeId: string,
  action: string,
  args: unknown,
  requestedBy: string | null,
): Promise<{ ok: boolean; message: string } | null> {
  const a = (args ?? {}) as { rings?: unknown; surveyId?: unknown };
  if (action === 'surveySites') {
    const rings = Number(a.rings ?? 1);
    const r = await startSurvey(nodeId, 'manual', requestedBy, Number.isFinite(rings) ? rings : 1);
    return r.ok
      ? { ok: true, message: `survey started (#${r.surveyId})` }
      : { ok: false, message: r.error ?? 'could not start the survey' };
  }
  if (action === 'surveyCancel') {
    // Which survey is open is the BACKEND's own record, so look it up here
    // rather than trusting the caller. The browser only learns a survey id
    // from the node's progress reports — which is exactly what is missing
    // when a node has lost a survey the record still calls running, so a
    // caller-supplied id was unavailable in the one case that needs it most.
    const hinted = Number(a.surveyId ?? 0);
    const surveyId = (await openSurveyFor(nodeId)) ?? (hinted > 0 ? hinted : 0);
    if (surveyId === 0) {
      return { ok: true, message: 'no survey is running on this node' };
    }
    const r = await hub.sendCmd(nodeId, action, { surveyId });
    if (r.ok) {
      // The node is stopping: it restores itself and POSTs what it measured,
      // and THAT report closes the record. Closing it here would make the
      // ingest reject exactly the partial results staff cancelled to see.
      return { ok: true, message: r.message ?? 'survey cancelling' };
    }
    // The node is not running this survey — it restarted, or never picked the
    // command up, and its own state is in-process only. The record is stale,
    // so clear it; the node has nothing to restore and nothing to report.
    await failSurvey(nodeId, surveyId, r.message ?? 'the node was not running this survey');
    log.info({ nodeId, surveyId, agent: r.message }, 'cleared a survey record the node had lost');
    return { ok: true, message: 'the node was not running it — cleared the stale record' };
  }
  return null;
}

/**
 * The nodes with a survey running right now, as node id -> survey id.
 *
 * Read from the survey table rather than the agent's status frame: a node is
 * mid-survey from the moment it is commanded, the staff list does not get
 * status frames at all, and a page loaded between two heartbeats would
 * otherwise show nothing out of the ordinary on a node that has its channel
 * set replaced.
 */
export async function openSurveyFor(nodeId: string): Promise<number | null> {
  const pool = await getPool();
  if (!pool) return null;
  try {
    const r = await pool.query<{ id: number }>(
      `SELECT id FROM node_site_surveys
        WHERE node_id = $1 AND status = 'running'
        ORDER BY started_at DESC LIMIT 1`,
      [nodeId],
    );
    return r.rows[0]?.id ?? null;
  } catch (err) {
    log.warn({ err, nodeId }, 'openSurveyFor failed');
    return null;
  }
}

export async function runningSurveys(): Promise<Map<string, number>> {
  const out = new Map<string, number>();
  const pool = await getPool();
  if (!pool) return out;
  try {
    const r = await pool.query<{ id: number; node_id: string }>(
      `SELECT id, node_id FROM node_site_surveys
        WHERE status = 'running'
          AND started_at > now() - ($1 || ' milliseconds')::interval`,
      [String(SURVEY_TIMEOUT_MS)],
    );
    for (const row of r.rows) out.set(row.node_id, row.id);
  } catch (err) {
    log.warn({ err }, 'runningSurveys failed');
  }
  return out;
}

/** Mark a running survey failed (agent refused/cancelled mid-flight). */
export async function failSurvey(nodeId: string, surveyId: number, note: string): Promise<void> {
  const pool = await getPool();
  if (!pool) return;
  await pool.query(
    `UPDATE node_site_surveys SET status = 'failed', finished_at = now(), note = $3
      WHERE id = $1 AND node_id = $2 AND status = 'running'`,
    [surveyId, nodeId, note.slice(0, 300)],
  );
}

// ---------------------------------------------------------------------------
// channel append (atomic) — the registry config_override writer family,
// but FOR UPDATE because this is an array merge, not a scalar set.
// ---------------------------------------------------------------------------

async function appendWinnerChannels(
  pool: Pool,
  nodeId: string,
  surveyId: number,
  winners: Array<{ result: AgentSurveyResult; altHz: number | null }>,
): Promise<number> {
  if (winners.length === 0) return 0;
  const client = await pool.connect();
  let added = 0;
  try {
    await client.query('BEGIN');
    const cur = await client.query<{ config_override: Record<string, unknown> }>(
      'SELECT config_override FROM nodes WHERE id = $1 FOR UPDATE',
      [nodeId],
    );
    const override = cur.rows[0]?.config_override ?? {};
    const channels = Array.isArray(override['channels'])
      ? ([...(override['channels'] as Array<Record<string, unknown>>)])
      : [];
    const haveFreq = new Set(channels.map((ch) => Number(ch['frequency'])));
    const haveName = new Set(channels.map((ch) => String(ch['name'] ?? '').trim().toLowerCase()));

    for (const w of winners) {
      const r = w.result;
      const dup = haveFreq.has(r.freqHz) || haveName.has(r.siteName.trim().toLowerCase());
      const overCap = channels.length >= CHANNEL_CAP;
      if (dup || overCap) {
        await client.query(
          `UPDATE node_site_survey_results SET added = false, note = $3
            WHERE survey_id = $1 AND site_name = $2 AND verdict = 'pass'`,
          [surveyId, r.siteName, dup ? 'already configured' : 'channel cap (64) reached'],
        );
        continue;
      }
      const ch: Record<string, unknown> = {
        name: r.siteName,
        frequency: r.freqHz,
        decoder: 'p25p1',
        system: 'NSWPSN',
        site: r.siteName,
        autoStart: true,
      };
      if (w.altHz !== null && w.altHz !== r.freqHz) ch['frequencies'] = [w.altHz];
      channels.push(ch);
      haveFreq.add(r.freqHz);
      haveName.add(r.siteName.trim().toLowerCase());
      added++;
      await client.query(
        `UPDATE node_site_survey_results SET added = true
          WHERE survey_id = $1 AND site_name = $2 AND verdict = 'pass'`,
        [surveyId, r.siteName],
      );
    }

    if (added > 0) {
      await client.query(
        `UPDATE nodes SET config_override = jsonb_set($2::jsonb, '{channels}', $3::jsonb) WHERE id = $1`,
        [nodeId, JSON.stringify(override), JSON.stringify(channels)],
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    log.error({ err, nodeId, surveyId }, 'survey channel append failed');
    return 0;
  } finally {
    client.release();
  }
  if (added > 0) await pushConfigToNode(nodeId).catch(() => undefined);
  return added;
}

// ---------------------------------------------------------------------------
// reads + lazy timeout
// ---------------------------------------------------------------------------

async function expireStaleSurveys(pool: Pool, nodeId: string): Promise<void> {
  await pool.query(
    `UPDATE node_site_surveys SET status = 'timeout', finished_at = now()
      WHERE node_id = $1 AND status = 'running'
        AND started_at < now() - ($2 || ' milliseconds')::interval`,
    [nodeId, String(SURVEY_TIMEOUT_MS)],
  ).catch(() => undefined);
}

export async function listSurveys(nodeId: string, limit = 10): Promise<unknown[]> {
  const pool = await getPool();
  if (!pool) return [];
  await expireStaleSurveys(pool, nodeId);
  const surveys = await pool.query<SurveyRow>(
    `SELECT * FROM node_site_surveys WHERE node_id = $1 ORDER BY started_at DESC LIMIT $2`,
    [nodeId, limit],
  );
  if (surveys.rows.length === 0) return [];
  const ids = surveys.rows.map((s) => s.id);
  const rows = await pool.query<{
    survey_id: number; site_name: string; grn_key: string | null;
    freq_hz: string | null; alt_freq_hz: string | null; verdict: string;
    median_pct: number | null; samples: number | null; signal_dbfs: number | null;
    added: boolean; note: string | null;
  }>(
    `SELECT survey_id, site_name, grn_key, freq_hz, alt_freq_hz, verdict,
            median_pct, samples, signal_dbfs, added, note
       FROM node_site_survey_results WHERE survey_id = ANY($1::int[])
      ORDER BY median_pct DESC NULLS LAST, site_name`,
    [ids],
  );
  const bySurvey = new Map<number, unknown[]>();
  for (const r of rows.rows) {
    const list = bySurvey.get(r.survey_id) ?? [];
    list.push({
      siteName: r.site_name,
      grnKey: r.grn_key,
      freqHz: r.freq_hz !== null ? Number(r.freq_hz) : null,
      altFreqHz: r.alt_freq_hz !== null ? Number(r.alt_freq_hz) : null,
      verdict: r.verdict,
      medianPct: r.median_pct,
      samples: r.samples,
      signalDbfs: r.signal_dbfs,
      added: r.added,
      note: r.note,
    });
    bySurvey.set(r.survey_id, list);
  }
  return surveys.rows.map((s) => ({
    id: s.id,
    startedAt: s.started_at,
    finishedAt: s.finished_at,
    trigger: s.trigger,
    status: s.status,
    requestedBy: s.requested_by,
    rings: s.rings,
    candidates: s.candidates,
    added: s.added,
    note: s.note,
    results: bySurvey.get(s.id) ?? [],
  }));
}
