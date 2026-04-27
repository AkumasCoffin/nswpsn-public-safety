-- Mirrors backends/init_postgres.py:225-248. The python service has been
-- creating this table on startup; we add it as an explicit Node migration
-- so a fresh deploy lands the same schema even when the python boot path
-- is no longer running.

CREATE TABLE IF NOT EXISTS rdio_summaries (
  id SERIAL PRIMARY KEY,
  summary_type TEXT NOT NULL,
  period_start TIMESTAMPTZ NOT NULL,
  period_end TIMESTAMPTZ NOT NULL,
  day_date DATE NOT NULL,
  hour_slot INTEGER,
  summary TEXT NOT NULL,
  call_count INTEGER NOT NULL DEFAULT 0,
  transcript_chars INTEGER NOT NULL DEFAULT 0,
  model TEXT,
  details JSONB DEFAULT '{}'::jsonb,
  release_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(summary_type, period_start)
);

CREATE INDEX IF NOT EXISTS idx_rdio_summaries_type
  ON rdio_summaries(summary_type);
CREATE INDEX IF NOT EXISTS idx_rdio_summaries_day
  ON rdio_summaries(day_date);
CREATE INDEX IF NOT EXISTS idx_rdio_summaries_day_hour
  ON rdio_summaries(day_date, hour_slot);
CREATE INDEX IF NOT EXISTS idx_rdio_summaries_period
  ON rdio_summaries(period_start DESC);
CREATE INDEX IF NOT EXISTS idx_rdio_summaries_release
  ON rdio_summaries(release_at);
