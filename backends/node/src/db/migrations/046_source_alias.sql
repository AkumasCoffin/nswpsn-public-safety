-- 046: radio OTA/talker alias per activity event.
--
-- vce now ships two extra fields per activity event (control-server change):
--   - systemName  → stored in the pre-existing (until now unused) system_label
--     column: the channel's configured P25 system name (e.g. "NSWPSN") the
--     operator sees, so the Data tab can show a friendly name instead of the
--     bare numeric systemId.
--   - sourceAlias → stored in this NEW source_alias column: the over-the-air
--     talker alias last captured for the source radio (from vce's per-radio
--     identity table), so the Radios view / Events "Unit" cell can label a
--     radio id with its alias.
--
-- Both are best-effort and NULL when unresolvable, and null from any older
-- agent/control-server — the read path already falls back to the numbers.
-- Additive only (ADD COLUMN IF NOT EXISTS); no data is rewritten.

ALTER TABLE node_radio_events
  ADD COLUMN IF NOT EXISTS source_alias TEXT;
