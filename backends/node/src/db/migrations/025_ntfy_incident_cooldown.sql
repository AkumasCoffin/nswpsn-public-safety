-- 025_ntfy_incident_cooldown.sql
--
-- Dedup state for the rdio→ntfy "major incident" push notifier
-- (services/rdioIncidentAlerts.ts). A genuinely large incident generates
-- a sustained burst of radio traffic on one talkgroup spanning many calls
-- over many minutes; without a cooldown the detector would re-fire on
-- every poll tick for the whole life of the incident.
--
-- One row per (system, talkgroup). `last_alert` is bumped each time a
-- push is sent; the detector suppresses re-alerts while
-- last_alert > now() - RDIO_ALERT_COOLDOWN_MIN. Keyed on (system,
-- talkgroup) because talkgroup ids are only unique within a system.
--
-- Lives in the MAIN archive Postgres (DATABASE_URL), not the rdio-scanner
-- DB — the detector reads rdio (read-only) and writes its own state here.
CREATE TABLE IF NOT EXISTS ntfy_incident_cooldown (
  system     INTEGER     NOT NULL,
  talkgroup  INTEGER     NOT NULL,
  last_alert TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (system, talkgroup)
);
