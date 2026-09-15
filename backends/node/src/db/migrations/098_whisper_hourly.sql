-- 098: per-backend hourly whisper throughput.
--
-- The whisper router's counters (requests/failures/latency ring) are
-- in-memory only and reset on every restart, so the staff Transcripts
-- dashboard had nothing durable to graph. Every transcription attempt now
-- upserts into this table fire-and-forget (services/whisperStats.ts) and
-- GET /api/whisper/history reads it — the dashboard never touches the
-- rdio-scanner database. `backend` is the router's backend name, or 'none'
-- when no backend could take the call at all. `total_ms` sums the latency
-- of SUCCESSFUL attempts only, so avg = total_ms / (requests - failures)
-- isn't polluted by timeouts. History starts at deploy by design.
CREATE TABLE IF NOT EXISTS whisper_hourly (
  hour      TIMESTAMPTZ NOT NULL,
  backend   TEXT        NOT NULL,
  requests  INT         NOT NULL DEFAULT 0,
  failures  INT         NOT NULL DEFAULT 0,
  total_ms  BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (hour, backend)
);
