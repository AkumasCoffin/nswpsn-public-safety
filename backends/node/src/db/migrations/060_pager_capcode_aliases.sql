-- Learned capcode -> alias/agency map, synced from Pagermon.
--
-- The static data/pager/Capcode-Aliases.csv only covers a subset of the
-- capcodes the feeder nodes actually receive (36 heard, 2 known at the time
-- this was added), so the staff Data tab showed bare numbers. Every Pagermon
-- message already carries `alias` + `agency` (it's what the live map renders),
-- so we harvest that into this table and use it as the primary lookup, with
-- the CSV as fallback. Self-maintaining: no re-export, no extra credentials.
--
-- `capcode` is stored NORMALISED (leading zeros stripped) so it matches the
-- lookups in api/node-data.ts. `source` records where the row came from
-- ('pagermon-feed' | 'pagermon-api') for debugging.
CREATE TABLE IF NOT EXISTS pager_capcode_aliases (
  capcode     TEXT PRIMARY KEY,
  alias       TEXT NOT NULL,
  agency      TEXT,
  source      TEXT NOT NULL DEFAULT 'pagermon-feed',
  first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_pager_capcode_aliases_updated ON pager_capcode_aliases(updated_at DESC);
