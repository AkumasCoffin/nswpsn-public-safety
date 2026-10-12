#!/usr/bin/env node
/**
 * Rebuild uq_nre_node_stream_event, the ingest dedup index, to reclaim its
 * bloat.
 *
 * That index is the biggest single object on node_radio_events: 1969 MB
 * against about 650 MB of actual keys. Its columns are (node_id, stream_id,
 * source_event_id) — node_id is TEXT averaging 35 bytes (a UUID kept as text)
 * and stream_id TEXT averaging 30, so ~90 bytes a row over 5.6 M rows is
 * ~500 MB, ~650 MB once fill-factor slack is counted. The rest is bloat, from
 * two sources: inserts land in node/stream order rather than append order, so
 * pages split; and until migration 124's companion change every row was
 * UPDATEd immediately after being INSERTed, which left a dead index entry per
 * row. For contrast, idx_nre_time (received_at alone, appended in order) is
 * 123 MB against ~134 MB expected — no bloat at all.
 *
 * This index is NOT droppable. Both ingest paths name it directly —
 * `ON CONFLICT (node_id, stream_id, source_event_id) DO NOTHING` in
 * services/nodeEvents.ts — which is what makes a re-sent agent batch
 * idempotent. Rebuilding is the only way to shrink it.
 *
 * Why this is a script and not a migration: REINDEX CONCURRENTLY cannot run
 * inside a transaction, and this takes minutes on a 2 vCPU container, which
 * does not belong in the middle of a deploy.
 *
 * Unlike scripts/vacuum-full-archive-tables.mjs this needs NO service stop.
 * CONCURRENTLY builds a replacement alongside the live index and swaps it in,
 * taking a brief lock only at the end; ingest keeps running throughout. It does
 * need room for both copies at once — about 2 GB transiently, against 14 GB
 * free on the container at the time of writing.
 *
 *   cd /var/www/nswpsn/backends/node
 *   node --env-file-if-exists=../.env scripts/reindex-nre-unique.mjs
 *
 * Safe to re-run: a second pass simply finds the index already compact.
 * If it is interrupted, Postgres may leave an invalid index named
 * uq_nre_node_stream_event_ccnew behind; this script reports one if it finds
 * it, and `DROP INDEX uq_nre_node_stream_event_ccnew` clears it.
 */
import pg from 'pg';

const { Pool } = pg;

const INDEX = 'uq_nre_node_stream_event';
const TABLE = 'node_radio_events';

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error('DATABASE_URL not set. Run with --env-file-if-exists=../.env');
    process.exit(1);
  }

  const pool = new Pool({
    connectionString: url,
    max: 1,
    // The writer pool's 60s cap would kill the rebuild part way through.
    statement_timeout: 0,
    idleTimeoutMillis: 0,
  });

  try {
    const before = await pool.query(
      `SELECT pg_relation_size(i.indexrelid) AS bytes,
              pg_size_pretty(pg_relation_size(i.indexrelid)) AS pretty,
              i.indisvalid AS valid
         FROM pg_stat_user_indexes s
         JOIN pg_index i ON i.indexrelid = s.indexrelid
        WHERE s.relname = $1 AND s.indexrelname = $2`,
      [TABLE, INDEX],
    );
    if (before.rows.length === 0) {
      console.error(`${INDEX} does not exist on ${TABLE} — nothing to do.`);
      console.error('That index is ingest\'s ON CONFLICT target; if it is really');
      console.error('missing, recreate it before worrying about its size.');
      process.exit(1);
    }
    if (before.rows[0].valid === false) {
      console.log(`${INDEX} is marked INVALID — a previous CONCURRENTLY run was`);
      console.log('interrupted. Rebuilding it is still the right move.');
    }

    // A leftover from an interrupted run holds disk and confuses nothing else;
    // say so rather than silently working around it.
    const stale = await pool.query(
      `SELECT indexrelname AS name, pg_size_pretty(pg_relation_size(indexrelid)) AS pretty
         FROM pg_stat_user_indexes
        WHERE relname = $1 AND indexrelname LIKE $2`,
      [TABLE, `${INDEX}_ccnew%`],
    );
    for (const r of stale.rows) {
      console.log(`note: leftover ${r.name} (${r.pretty}) from an interrupted run;`);
      console.log(`      DROP INDEX ${r.name}; when this finishes.`);
    }

    console.log(`=== REINDEX INDEX CONCURRENTLY ${INDEX} ===`);
    console.log(`before: ${before.rows[0].pretty}`);
    console.log('No service stop needed; ingest continues. This takes minutes.\n');

    const started = Date.now();
    await pool.query(`REINDEX INDEX CONCURRENTLY ${INDEX}`);
    const secs = Math.round((Date.now() - started) / 1000);

    const after = await pool.query(
      `SELECT pg_relation_size(indexrelid) AS bytes,
              pg_size_pretty(pg_relation_size(indexrelid)) AS pretty
         FROM pg_stat_user_indexes
        WHERE relname = $1 AND indexrelname = $2`,
      [TABLE, INDEX],
    );
    const saved = Number(before.rows[0].bytes) - Number(after.rows[0]?.bytes ?? 0);
    console.log(`after:  ${after.rows[0]?.pretty ?? '(gone?)'}  in ${secs}s`);
    console.log(
      saved > 0
        ? `reclaimed ${(saved / 1024 / 1024).toFixed(0)} MB`
        : 'no space reclaimed — it was already compact',
    );

    const total = await pool.query(
      `SELECT pg_size_pretty(pg_indexes_size($1::regclass)) AS idx,
              pg_size_pretty(pg_relation_size($1::regclass)) AS heap`,
      [TABLE],
    );
    console.log(`\n${TABLE} now: heap ${total.rows[0].heap}, indexes ${total.rows[0].idx}`);
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error('reindex failed:', err.message);
  console.error('Nothing is broken: CONCURRENTLY leaves the live index in place');
  console.error('on failure. Check for a *_ccnew leftover before retrying.');
  process.exit(1);
});
