-- Call uploads that arrived BEFORE the activity event they belong to.
--
-- The two feeds race. Audio uploads as soon as the recorder closes the call —
-- a second or two for a short over — while activity events ship in batches on
-- a 3-5s tick. markRecorded was one-shot: when the upload won that race there
-- was no row to flag yet, so it gave up silently and the call showed no audio
-- forever, even though the audio existed. Busy talkgroups have more short
-- overs and so lost proportionally more (measured: 30017 at 67% coverage
-- against 30003 at 95%).
--
-- Parking the upload here lets recordActivityEvents claim it when the matching
-- event finally lands, making the flag independent of arrival order. Rows are
-- consumed on claim (DELETE ... RETURNING) so one upload can never flag two
-- calls, and swept by age so an upload that never finds its event cannot
-- accumulate — this is the one node table with no natural upper bound.
CREATE TABLE IF NOT EXISTS node_pending_recordings (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  node_id     TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  talkgroup   INTEGER,
  -- Calling radio + traffic frequency, straight off the rdio upload
  -- (FormField.SOURCE / FREQUENCY). They identify WHICH call this audio is,
  -- which the timestamps cannot: the upload carries the real call start while
  -- the event carries observed_at_ms, when the activity logger wrote its row.
  source_unit INTEGER,
  frequency   BIGINT,
  -- The upload's own timestamp (call start), NOT arrival time — arrival order
  -- is precisely what this table exists to stop mattering.
  started_at  TIMESTAMPTZ NOT NULL,
  audio_bytes BIGINT NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The claim lookup: one node's parked uploads for a talkgroup, near a time.
CREATE INDEX IF NOT EXISTS idx_pending_rec_lookup
  ON node_pending_recordings(node_id, talkgroup, started_at);

-- Age sweep (see nodeEventsPruner) needs its own predicate.
CREATE INDEX IF NOT EXISTS idx_pending_rec_created
  ON node_pending_recordings(created_at);
