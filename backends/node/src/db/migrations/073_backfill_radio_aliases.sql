-- Seed node_radio_aliases from the aliases already sitting in the event detail.
--
-- 072 created the table and the ingest fills it going forward, which left the
-- Aliases page empty on arrival even though ~21k aliased events across ~280
-- radios were already captured. Worse, aliases ride patch calls almost
-- exclusively here, and a patch is a temporary dispatcher-created link — when
-- no patch is up, no aliases arrive at all, so "wait for the next one" can mean
-- hours of an empty page.
--
-- Takes the most recently seen alias per radio, its first and last sighting,
-- and how many events carried it. DO NOTHING on conflict so this is safe to
-- re-run and can never overwrite something the live ingest has already learned.
INSERT INTO node_radio_aliases (system, radio_id, alias, first_seen, last_seen, times_seen)
SELECT DISTINCT ON (COALESCE(system, 0), source_unit)
       COALESCE(system, 0)                                              AS system,
       source_unit                                                      AS radio_id,
       first_value(source_alias) OVER w                                 AS alias,
       min(received_at) OVER (PARTITION BY COALESCE(system, 0), source_unit) AS first_seen,
       max(received_at) OVER (PARTITION BY COALESCE(system, 0), source_unit) AS last_seen,
       count(*) OVER (PARTITION BY COALESCE(system, 0), source_unit)::int    AS times_seen
  FROM node_radio_events
 WHERE source_alias IS NOT NULL
   AND btrim(source_alias) <> ''
   AND source_unit IS NOT NULL
WINDOW w AS (PARTITION BY COALESCE(system, 0), source_unit ORDER BY received_at DESC)
ON CONFLICT (system, radio_id) DO NOTHING;
