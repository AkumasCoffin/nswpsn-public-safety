-- Per-receiver ADS-B statistics, by the hour.
--
-- node_adsb_daily holds one row per day, which is the right shape for "how has
-- this receiver been doing lately" and the wrong shape for a chart: on a 24h
-- window it yields a single point. The radio side has had node_radio_hourly for
-- exactly this reason.
--
-- Unlike the radio rollups this cannot be DERIVED. Those are rebuilt hourly
-- from node_radio_events, but ADS-B keeps no detail table — individual
-- positions are deliberately never persisted — so this is accumulated in
-- memory alongside the daily counters and flushed on the same minute timer.
--
-- Signal is here but will be null on every row until the receivers are running
-- an agent that sends it: dump1090 reports it, and the agent has always parsed
-- it for its own autogain loop, but it was never put on the wire.
CREATE TABLE IF NOT EXISTS node_adsb_hourly (
  node_id      TEXT        NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  hour         TIMESTAMPTZ NOT NULL,
  snapshots    integer     NOT NULL DEFAULT 0,
  positions    bigint      NOT NULL DEFAULT 0,
  max_aircraft integer     NOT NULL DEFAULT 0,
  max_range_km real,
  msg_rate_max real,
  -- Mean signal is a weighted average, not a maximum: averaging an average
  -- needs the weight that produced it, so the running sum and its divisor are
  -- both kept and the mean is computed on read.
  signal_sum   double precision,
  signal_n     integer,
  signal_peak  real,
  PRIMARY KEY (node_id, hour)
);

-- The only read: "this receiver's last N hours", newest first.
CREATE INDEX IF NOT EXISTS idx_node_adsb_hourly_node_hour
  ON node_adsb_hourly (node_id, hour DESC);

-- And the fleet chart, which sweeps every receiver across one window.
CREATE INDEX IF NOT EXISTS idx_node_adsb_hourly_hour
  ON node_adsb_hourly (hour DESC);
