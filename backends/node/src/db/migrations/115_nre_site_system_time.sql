-- Scope-led indexes for the Data tab's system and site drill-downs.
--
-- /api/node-data/site and /api/node-data/system both filter the radio detail
-- table by scope + window + the call predicate, but no index covers either
-- scope: the site columns (site_rfss, site_id) appear in no index at all, and
-- `system` only ever appears behind `talkgroup`, which these queries do not
-- constrain. The planner therefore takes idx_nre_rollup_time (received_at,
-- partial on the call predicate) and heap-filters every call row in the window
-- — ~200k rows per 24h against a ~6GB table. Cached, that answers in under a
-- second and nobody notices; cold, it is tens of seconds of random heap reads,
-- and when a staff browser fires the site + system + radios queries together
-- the slowest ones hit statement_timeout (57014) or the tunnel gives up and
-- the UI reports "Failed to fetch". Measured on production at 20:03 local:
-- site 15.5s/16.3s, system 29.7s, radios 500 at 30.0s; the identical queries
-- two minutes later (warm) answered in 0.6-3s.
--
-- Both indexes are partial on the exact call predicate every one of these
-- queries carries, so they stay small and the range scan lands directly on
-- the scope's own rows regardless of cache state.
--
-- Plain CREATE INDEX (not CONCURRENTLY), the migration-110 reasoning: the
-- migration runner wraps each file in a transaction, where CONCURRENTLY is
-- not allowed. The build takes well under a minute at current table size and
-- only blocks writers (ingest buffers and retries); reads keep flowing.
CREATE INDEX IF NOT EXISTS idx_nre_site_time_calls
  ON node_radio_events (system, site_rfss, site_id, received_at)
  WHERE event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%';

CREATE INDEX IF NOT EXISTS idx_nre_system_time_calls
  ON node_radio_events (system, received_at)
  WHERE event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%';
