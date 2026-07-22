-- Responding units on incidents + a persistent callsign dictionary.
--
-- units: editor-entered callsigns attached to a call (JSONB array of
-- strings, same storage pattern as type / responding_agencies).
--
-- callsigns: every callsign ever saved, so the editor's unit input can
-- tab-complete previously used callsigns even after the incidents that
-- carried them have expired. Upserted on every incident save.

ALTER TABLE incidents ADD COLUMN IF NOT EXISTS units JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS callsigns (
  callsign  TEXT PRIMARY KEY,
  last_used TIMESTAMPTZ NOT NULL DEFAULT now(),
  use_count INTEGER NOT NULL DEFAULT 1
);
