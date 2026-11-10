-- Drop adsb_tracks (migration 103), the hour-row aircraft archive.
--
-- 139 moved history to adsb_track_chunks (one row per minute, written once),
-- and scripts/backfill-adsb-chunks.mjs copied the old hours across. Since then
-- nothing has written or read adsb_tracks: it was kept only as the rollback
-- path until the owner had checked the map's history playback across the
-- cutover (2026-11-05 14:09 UTC), which they did on 2026-11-08. ~500 MB.
--
-- node_adsb_tracks (105, per-receiver coverage) is a different table and stays.
--
-- DROP TABLE takes ACCESS EXCLUSIVE; nothing queries the table, so the lock is
-- immediate, and the lock_timeout only guards against a stray psql session.
-- Re-running the deploy is the recovery.
SET LOCAL lock_timeout = '10s';

DROP TABLE IF EXISTS adsb_tracks;
