-- 077: give already-stored scanner rows the same P25 identity as node rows.
--
-- Every rollup on the Data page groups by (wacn, system) — systems,
-- talkgroups and radios alike (api/node-data.ts). recordScannerCall resolved
-- only the numeric `system` and left `wacn` and `system_label` NULL, so a
-- scanner reception of talkgroup X on system 721 was a DIFFERENT group to a
-- node reception of the same talkgroup on the same system. One P25 system
-- rendered as two ("NSWPSN 721" plus a nameless 721), and 53 of 402 talkgroups
-- appeared twice.
--
-- The code now writes all three (services/nodeEvents.ts resolveScannerSystem).
-- This repairs the rows written before it did — a scanner row inherits the
-- identity of the busiest node-observed (wacn, system) pair carrying the same
-- system number, which is by construction the pair the rollups already show.
--
-- Scoped to stream_id = 'scanner' and to rows that are actually missing the
-- identity, so it is a no-op on a fresh database and safe to re-run.

WITH node_identity AS (
  SELECT system,
         (array_agg(wacn ORDER BY received_at DESC)
            FILTER (WHERE wacn IS NOT NULL))[1] AS wacn,
         (array_agg(system_label ORDER BY received_at DESC)
            FILTER (WHERE system_label IS NOT NULL))[1] AS system_label
    FROM node_radio_events
   WHERE stream_id <> 'scanner'
     AND system IS NOT NULL
   GROUP BY system
)
UPDATE node_radio_events e
   SET wacn = COALESCE(e.wacn, i.wacn),
       system_label = COALESCE(e.system_label, i.system_label)
  FROM node_identity i
 WHERE e.stream_id = 'scanner'
   AND e.system = i.system
   AND (e.wacn IS NULL OR e.system_label IS NULL);
