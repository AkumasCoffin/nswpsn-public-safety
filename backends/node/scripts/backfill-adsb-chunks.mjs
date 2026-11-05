#!/usr/bin/env node
/**
 * Copy aircraft history from adsb_tracks (migration 103, one row per aircraft
 * per hour) into adsb_track_chunks (migration 139, one row per minute), and
 * seed adsb_identities, so the history map keeps its 31 days across the switch.
 *
 * Run once after the deploy that applies 139:
 *
 *   cd /var/www/nswpsn/backends/node
 *   node --env-file-if-exists=../.env scripts/backfill-adsb-chunks.mjs
 *
 * - Reads adsb_tracks only; never changes it. It stays as the rollback path
 *   until a later migration drops it.
 * - Writes seq -1 rows with ON CONFLICT DO NOTHING, so re-running is a no-op
 *   and it can never collide with the live writer (seq 0 and up).
 * - Skips any minute the live writer already has, so the hour the deploy
 *   landed in is not drawn twice from two slightly different point sets.
 * - One hour per statement; no service stop, ingest keeps running.
 *
 * Each stored hour's sources are applied to every minute of it: the old table
 * kept sources per hour, so nothing finer exists to copy.
 */
import pg from 'pg';

const { Pool } = pg;
const MINUTE_MS = 60_000;

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error('DATABASE_URL not set. Run with --env-file-if-exists=../.env');
    process.exit(1);
  }
  const pool = new Pool({ connectionString: url, max: 1 });
  try {
    const hours = await pool.query(
      `SELECT DISTINCT hour_bucket FROM adsb_tracks ORDER BY hour_bucket`,
    );
    console.log(`${hours.rows.length} hours to copy`);
    let minutesWritten = 0;
    let n = 0;
    for (const { hour_bucket: hour } of hours.rows) {
      const hourMs = hour.getTime();
      const rows = await pool.query(
        `SELECT hex, callsign, reg, type, es_tag, sources, points
           FROM adsb_tracks WHERE hour_bucket = $1`,
        [hour],
      );
      /** minute ms → hex → entry */
      const byMinute = new Map();
      for (const r of rows.rows) {
        if (!Array.isArray(r.points)) continue;
        for (const p of r.points) {
          const sec = Number(p[0]);
          if (!Number.isFinite(sec)) continue;
          const minuteMs = hourMs + Math.floor(sec / 60) * MINUTE_MS;
          let m = byMinute.get(minuteMs);
          if (!m) {
            m = {};
            byMinute.set(minuteMs, m);
          }
          let e = m[r.hex];
          if (!e) {
            e = {
              c: r.callsign, r: r.reg, t: r.type, e: r.es_tag,
              s: Array.from(new Set(r.sources ?? [])),
              p: [],
            };
            m[r.hex] = e;
          }
          e.p.push([(sec % 60) * 1000, p[1], p[2], p[3]]);
        }
      }
      if (byMinute.size === 0) continue;
      const mins = Array.from(byMinute.keys()).sort((a, b) => a - b);
      const res = await pool.query(
        `INSERT INTO adsb_track_chunks (minute, seq, aircraft)
         SELECT u.minute, -1, u.aircraft
           FROM unnest($1::timestamptz[], $2::json[]) AS u(minute, aircraft)
          WHERE NOT EXISTS (SELECT 1 FROM adsb_track_chunks c
                             WHERE c.minute = u.minute AND c.seq >= 0)
         ON CONFLICT (minute, seq) DO NOTHING`,
        [
          mins.map((m) => new Date(m).toISOString()),
          mins.map((m) => JSON.stringify(byMinute.get(m))),
        ],
      );
      minutesWritten += res.rowCount ?? 0;
      n += 1;
      if (n % 24 === 0) console.log(`  ${n} hours, ${minutesWritten} minutes written`);
    }
    console.log(`chunks: ${minutesWritten} minutes written from ${n} hours`);

    const ids = await pool.query(
      `INSERT INTO adsb_identities (hex, reg, type, updated_at)
       SELECT DISTINCT ON (hex) hex, reg, type, hour_bucket
         FROM adsb_tracks
        WHERE reg IS NOT NULL OR type IS NOT NULL
        ORDER BY hex, hour_bucket DESC
       ON CONFLICT (hex) DO NOTHING`,
    );
    console.log(`identities: ${ids.rowCount ?? 0} seeded`);
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
