/**
 * Which talkgroups the CENTRAL rdio-scanner is programmed to accept.
 *
 * rdio only stores a call on a talkgroup it has been configured for — an upload
 * for anything else is refused. So "is this talkgroup programmed" is the
 * difference between a transmission that could have been ingested and one that
 * never had a chance, and the Data tab cannot honestly report a miss without
 * knowing which of the two it is looking at.
 *
 * Read from rdio rather than mirrored here for the same reason patches are: the
 * operator programs a talkgroup in one place, and a second copy of the list
 * would be wrong the first time they edit it.
 *
 * READ-ONLY, like everything we do against the rdio database. One indexed
 * `SELECT DISTINCT id` per minute, on its own small pool, off the hot path.
 */
import { Pool } from 'pg';
import { config } from '../config.js';
import { log } from '../lib/log.js';

let _pool: Pool | null = null;
let _cache: { at: number; ids: Set<number> } | null = null;

/** ~60s, matching rdioPatches. The talkgroup list changes by hand. */
const TTL_MS = 60_000;

function pool(): Pool | null {
  if (!config.RDIO_DATABASE_URL) return null;
  if (!_pool) {
    _pool = new Pool({ connectionString: config.RDIO_DATABASE_URL, max: 2 });
    _pool.on('error', (err) => log.warn({ err }, 'rdioTalkgroups: idle client error'));
  }
  return _pool;
}

/**
 * Every talkgroup id programmed in central rdio.
 *
 * Never throws. The EMPTY case is load-bearing and deliberately means "we do
 * not know", not "nothing is programmed": callers must treat an empty set as
 * "cannot classify" and NOT as "every talkgroup is unprogrammed", or a rdio
 * that is merely unreachable would reclassify the entire network as denied.
 * A transient read error serves the last good set for the same reason
 * talkgroupCatalog does.
 */
export async function programmedTalkgroupIds(): Promise<Set<number>> {
  if (_cache && Date.now() - _cache.at < TTL_MS) return _cache.ids;
  const p = pool();
  if (!p) {
    const empty = new Set<number>();
    _cache = { at: Date.now(), ids: empty };
    return empty;
  }
  try {
    const res = await p.query<{ id: number | null }>(
      `SELECT DISTINCT id FROM "rdioScannerTalkgroups" WHERE id IS NOT NULL`,
    );
    const ids = new Set<number>();
    for (const row of res.rows) {
      const n = Number(row.id);
      if (Number.isInteger(n) && n > 0) ids.add(n);
    }
    _cache = { at: Date.now(), ids };
    return ids;
  } catch (err) {
    log.warn({ err }, 'rdioTalkgroups: failed to read talkgroups from central rdio');
    if (_cache) return _cache.ids;
    const empty = new Set<number>();
    _cache = { at: Date.now(), ids: empty };
    return empty;
  }
}

/** Test seam: drop the cache so the next call re-reads. */
export function _resetRdioTalkgroupsCache(): void {
  _cache = null;
}
