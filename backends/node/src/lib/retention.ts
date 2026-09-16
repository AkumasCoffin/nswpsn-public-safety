/**
 * How long this deployment keeps historical data — ONE definition.
 *
 * `DATA_RETENTION_DAYS` was parsed independently in three places and had
 * already drifted: services/cleanup.ts and api/status.ts both computed 31,
 * while api/heartbeat.ts hardcoded 7 and reported that number to the frontend.
 * status.ts even carries a comment saying the two "MUST mirror" each other
 * because they drifted once before.
 *
 * That is the same failure this codebase has hit twice already with node
 * logic: roleForKind lived in three files until a stale copy started revoking
 * healthy pager nodes, and the feed-target label lived in three places until
 * two of them told pager operators their pages went to rdio. The fix each time
 * is one export, not a fourth careful copy — so every reader imports from here
 * and a change to the environment moves all of them together.
 *
 * Read once at module load, like the rest of the env-derived config: the
 * cleanup pass, the status page and the ADS-B history window all have to agree
 * within a single process lifetime, and re-reading would let them disagree
 * after a mid-run `process.env` edit.
 */

/** Used when the variable is unset, empty, or not a positive integer. */
export const RETENTION_DAYS_FALLBACK = 31;

export const DATA_RETENTION_DAYS: number = (() => {
  const n = Number.parseInt(process.env['DATA_RETENTION_DAYS'] ?? '', 10);
  return Number.isFinite(n) && n > 0 ? n : RETENTION_DAYS_FALLBACK;
})();
