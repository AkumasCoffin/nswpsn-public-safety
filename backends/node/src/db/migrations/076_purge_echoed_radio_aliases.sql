-- Purge "aliases" that are only the radio's own id echoed back.
--
-- When a radio transmits no talker alias the decoder reports its id in that
-- field, so e.g. "2072676" was recorded as the over-the-air alias of radio
-- 2072676. That is not a name: it carries nothing the UID column doesn't
-- already say, and it made the Data tab print the same number as UID, OTA and
-- Alias on a single row.
--
-- Ingest now rejects these (services/nodeEvents.ts isRealTalkerAlias) and the
-- read path filters them, so this only clears the rows already stored — 7 at
-- the time of writing.
--
-- Numeric comparison, not a text one, so a zero-padded echo ("0200307" for
-- 200307) is caught alongside the exact match. A radio genuinely named
-- something numeric that ISN'T its own id is left alone.
DELETE FROM node_radio_aliases
 WHERE alias ~ '^[0-9]+$'
   AND alias::bigint = radio_id;
