-- Over-the-air radio aliases, one row per radio.
--
-- Some radios transmit a text alias alongside their id (P25 vendor talker-alias
-- link control). It reaches us on two paths: the activity feed's `sourceAlias`
-- (vce's trunked_identity_summary.last_talker_alias) and the audio upload's
-- `talkerAlias` form field. At the time this migration was written only the
-- upload delivered — the identity join arrived null — which is no longer the
-- case: the feed now supplies every alias we hold and the two never disagree.
-- The upload path is kept as a fallback. See services/nodeEvents.ts.
--
-- This is a FOREVER table, deliberately separate from node_radio_events:
--
--   * node_radio_events is pruned at 30 days. An alias is durable identity, not
--     call traffic — once a radio has told us its name, forgetting it a month
--     later would be losing the most useful thing we know about it.
--   * aliases are sparse. Most radios never transmit one, so deriving the list
--     by scanning every event row for the rare non-null is expensive for a
--     handful of answers.
--   * dedupe is the point of the page this backs: one row per radio, not one
--     per call.
--
-- Keyed on (system, radio_id): radio ids are only unique within their P25
-- system, and `system` here is the DECODED system id, matching
-- node_radio_events.system.
CREATE TABLE IF NOT EXISTS node_radio_aliases (
  system      INTEGER NOT NULL,
  radio_id    INTEGER NOT NULL,
  alias       TEXT    NOT NULL,
  first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- How many uploads carried this alias. A radio that has announced itself
  -- hundreds of times is a safer identification than a single garbled decode.
  times_seen  INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (system, radio_id)
);

-- The page lists most-recently-heard first and offers a text search.
CREATE INDEX IF NOT EXISTS idx_nra_last_seen ON node_radio_aliases(last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_nra_alias ON node_radio_aliases(lower(alias));
