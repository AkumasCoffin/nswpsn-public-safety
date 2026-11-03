-- 134: validate 133's bound.
--
-- VALIDATE CONSTRAINT reads every row once but takes only SHARE UPDATE
-- EXCLUSIVE, so inserts and reads carry on while it runs. Its own file so the
-- scan is not inside the same transaction as 133's brief exclusive lock.
--
-- If this fails, a row at or after the bound exists, which 133 checked for and
-- the CHECK itself has refused since: it would mean the rows predate 133 and
-- appeared between its check and its ALTER. Re-running the deploy repeats both.
SET LOCAL lock_timeout = '10s';

ALTER TABLE node_radio_events VALIDATE CONSTRAINT nre_legacy_bound;
