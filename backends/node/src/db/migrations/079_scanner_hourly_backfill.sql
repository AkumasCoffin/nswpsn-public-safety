-- 079: roll the scanner feed's already-stored calls into the forever buckets.
--
-- node_radio_hourly / node_radio_hourly_sys are the permanent record — the
-- detail table is pruned at 30 days, and window=all reads nothing else.
-- recordActivityEvents has always rolled into them; recordScannerCall never
-- did, so every all-time rollup under-counted the scanner feed by its entire
-- volume: calls that exist in the detail table, appear in every 24h/7d/30d
-- view, and then vanish the moment the window is "all".
--
-- The code now bumps both buckets (services/nodeEvents.ts). This repairs what
-- was written before it did, reading the detail rows that are still inside the
-- 30-day window — which at the time of writing is the whole of the scanner
-- feed's history, so the repair is complete rather than partial.
--
-- Idempotency: the two INSERTs ADD to whatever is already there, so running
-- this twice would double-count. Migrations are applied once and recorded, and
-- this deliberately does not try to be re-runnable — a guard would have to
-- guess which of the existing counts came from here.

-- Per-node volume. Site plays no part in this table.
INSERT INTO node_radio_hourly (hour, node_id, system, talkgroup, calls, audio_bytes)
SELECT date_trunc('hour', e.received_at),
       e.node_id,
       COALESCE(e.system, 0),
       COALESCE(e.talkgroup, 0),
       COUNT(*)::int,
       COALESCE(SUM(e.audio_bytes), 0)::bigint
  FROM node_radio_events e
 WHERE e.stream_id = 'scanner'
 GROUP BY 1, 2, 3, 4
ON CONFLICT (hour, node_id, system, talkgroup) DO UPDATE
  SET calls = node_radio_hourly.calls + EXCLUDED.calls,
      audio_bytes = node_radio_hourly.audio_bytes + EXCLUDED.audio_bytes;

-- Network-wide volume. site_rfss/site_id = -1 is this table's "unknown", which
-- is what a scanner always is: it has no control-channel view and cannot say
-- which site carried a call.
--
-- logical_calls counts each over-the-air call ONCE, so only the reception that
-- OWNS its group contributes — logical_call_id = id. A scanner copy of a call a
-- node also heard joined that node's group and adds nothing, which is the same
-- rule the live path applies with `existingGroup === null`.
INSERT INTO node_radio_hourly_sys
  (hour, system, talkgroup, site_rfss, site_id, calls, logical_calls)
SELECT date_trunc('hour', e.received_at),
       COALESCE(e.system, 0),
       COALESCE(e.talkgroup, 0),
       -1,
       -1,
       COUNT(*)::int,
       (COUNT(*) FILTER (WHERE e.logical_call_id = e.id))::int
  FROM node_radio_events e
 WHERE e.stream_id = 'scanner'
 GROUP BY 1, 2, 3
ON CONFLICT (hour, system, talkgroup, site_rfss, site_id) DO UPDATE
  SET calls = node_radio_hourly_sys.calls + EXCLUDED.calls,
      logical_calls = node_radio_hourly_sys.logical_calls + EXCLUDED.logical_calls;
