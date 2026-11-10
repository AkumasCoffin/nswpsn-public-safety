package activityship

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
)

// readCursor reads and parses the persisted cursor file.
func TestSurveyChannelEventsAreNeverShipped(t *testing.T) {
	// A survey's test channels decode a control channel, and a control channel
	// announces every call on its site — calls this node is not monitoring and
	// never heard the audio of. None of that is its reception.
	name := func(v string) *string { return &v }
	in := []sdrctl.ActivityEvent{
		{ID: 1, ChannelName: name("Maclean")},
		{ID: 2, ChannelName: name("SURVEY: Northern Rivers Comms. Site CANGAI EAST")},
		{ID: 3, ChannelName: name("SURVEY: Elcom Hut Sheas Knob CLOUDS CREEK (alt)")},
		{ID: 4, ChannelName: nil},
		{ID: 5, ChannelName: name("  SURVEY: padded ")},
		{ID: 6, ChannelName: name("Woombah")},
	}
	kept, dropped := dropSurveyEvents(in)
	if dropped != 3 {
		t.Fatalf("want 3 survey events dropped, got %d", dropped)
	}
	var ids []int64
	for _, e := range kept {
		ids = append(ids, e.ID)
	}
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 4 || ids[2] != 6 {
		t.Fatalf("kept the wrong events: %v", ids)
	}
}

func TestOrdinaryBatchIsPassedThroughUntouched(t *testing.T) {
	// Nothing to drop must cost nothing: the same slice goes back.
	name := func(v string) *string { return &v }
	in := []sdrctl.ActivityEvent{{ID: 1, ChannelName: name("Maclean")}, {ID: 2, ChannelName: nil}}
	kept, dropped := dropSurveyEvents(in)
	if dropped != 0 || len(kept) != 2 {
		t.Fatalf("want an untouched batch, got %d dropped / %d kept", dropped, len(kept))
	}
}

func readCursor(t *testing.T, dataDir string) cursorFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dataDir, "activity-cursor.json"))
	if err != nil {
		t.Fatalf("read cursor file: %v", err)
	}
	var st cursorFile
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("parse cursor file: %v", err)
	}
	return st
}

func TestResetStreamInitializesAndPersists(t *testing.T) {
	dir := t.TempDir()
	s := New(Options{DataDir: dir})

	if err := s.loadState(); err != nil {
		t.Fatalf("loadState on empty dir: %v", err)
	}
	if s.st.StreamID != "" || s.st.LastID != 0 {
		t.Fatalf("expected zero state, got %+v", s.st)
	}

	if err := s.resetStream("keyA"); err != nil {
		t.Fatalf("resetStream: %v", err)
	}
	if len(s.st.StreamID) != 32 {
		t.Fatalf("streamId should be 32 hex chars, got %q", s.st.StreamID)
	}
	got := readCursor(t, dir)
	if got != s.st {
		t.Fatalf("persisted %+v != in-memory %+v", got, s.st)
	}
	if got.DBKey != "keyA" || got.LastID != 0 {
		t.Fatalf("unexpected persisted state %+v", got)
	}
}

func TestLoadStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s1 := New(Options{DataDir: dir})
	s1.st = cursorFile{StreamID: "abc123", LastID: 77, DBKey: "keyA"}
	if err := s1.persistState(); err != nil {
		t.Fatalf("persistState: %v", err)
	}

	s2 := New(Options{DataDir: dir})
	if err := s2.loadState(); err != nil {
		t.Fatalf("loadState: %v", err)
	}
	if s2.st != s1.st {
		t.Fatalf("loaded %+v != persisted %+v", s2.st, s1.st)
	}
}

func TestReconcileDBKey(t *testing.T) {
	dir := t.TempDir()
	s := New(Options{DataDir: dir})
	if err := s.resetStream("keyA"); err != nil {
		t.Fatalf("resetStream: %v", err)
	}
	s.st.LastID = 42
	if err := s.persistState(); err != nil {
		t.Fatalf("persistState: %v", err)
	}
	origStream := s.st.StreamID

	// Same key: no reset.
	if !s.reconcileDBKey("keyA", true) {
		t.Fatal("reconcile with same key should succeed")
	}
	if s.st.StreamID != origStream || s.st.LastID != 42 {
		t.Fatalf("same key must not reset, got %+v", s.st)
	}

	// Unknown observation (db file not statable): no reset.
	if !s.reconcileDBKey("", false) {
		t.Fatal("reconcile with unknown key should succeed")
	}
	if s.st.StreamID != origStream || s.st.LastID != 42 {
		t.Fatalf("unknown key must not reset, got %+v", s.st)
	}

	// Changed key: the new key is stored, and the CURSOR IS KEPT.
	//
	// A changed key means the database file was replaced, which is not the
	// same as the event ids restarting. vce's migrator installs an upgraded
	// database with an atomic move — new inode, same path, ids preserved — so
	// resetting here re-shipped the whole 7-day retention under a streamId the
	// backend had never seen, which its (streamId, id) dedupe could not
	// suppress. The real restart signal is lastId < cursor, in tick().
	if !s.reconcileDBKey("keyB", true) {
		t.Fatal("reconcile with changed key should succeed")
	}
	if s.st.StreamID != origStream {
		t.Fatal("a replaced database file must not start a new stream")
	}
	if s.st.LastID != 42 {
		t.Fatalf("a replaced database file must not rewind the cursor, got %d", s.st.LastID)
	}
	if s.st.DBKey != "keyB" {
		t.Fatalf("the new dbKey must be stored, got %q", s.st.DBKey)
	}
	if got := readCursor(t, dir); got != s.st {
		t.Fatalf("dbKey update not persisted: file %+v != state %+v", got, s.st)
	}
}

func TestTickResetsOnLastIDRegression(t *testing.T) {
	dir := t.TempDir()

	var sinceIDs []int64
	fetch := func(sinceID int64, limit int) ([]sdrctl.ActivityEvent, int64, error) {
		sinceIDs = append(sinceIDs, sinceID)
		if sinceID > 0 {
			// The DB was recreated: latest id (5) is behind the cursor.
			return nil, 5, nil
		}
		return nil, 0, nil // fresh stream, nothing yet
	}

	s := New(Options{
		DataDir: dir,
		DBPath:  filepath.Join(dir, "nonexistent.sqlite"), // ok=false → dbKey ignored
		Fetch:   fetch,
	})
	if err := s.resetStream("keyA"); err != nil {
		t.Fatalf("resetStream: %v", err)
	}
	s.st.LastID = 100
	if err := s.persistState(); err != nil {
		t.Fatalf("persistState: %v", err)
	}
	origStream := s.st.StreamID

	s.tick(context.Background())

	if s.st.StreamID == origStream {
		t.Fatal("lastId regression must generate a fresh streamId")
	}
	if s.st.LastID != 0 {
		t.Fatalf("lastId regression must reset the cursor to 0, got %d", s.st.LastID)
	}
	if got := readCursor(t, dir); got != s.st {
		t.Fatalf("reset not persisted: file %+v != state %+v", got, s.st)
	}
	// The tick re-fetches from the reset cursor in the same round set.
	if len(sinceIDs) != 2 || sinceIDs[0] != 100 || sinceIDs[1] != 0 {
		t.Fatalf("expected fetches at since=100 then since=0, got %v", sinceIDs)
	}
}

// vceFetch is what the control server actually answers: the rows above sinceId
// in id order, capped at limit, with lastId floored at sinceId. That flooring
// is the reason an empty poll cannot report a restarted id sequence.
func vceFetch(db *[]sdrctl.ActivityEvent, probes *[]int64) FetchFunc {
	return func(sinceID int64, limit int) ([]sdrctl.ActivityEvent, int64, error) {
		if limit == 1 && probes != nil {
			*probes = append(*probes, sinceID)
		}
		var out []sdrctl.ActivityEvent
		lastID := sinceID
		for _, ev := range *db {
			if ev.ID <= sinceID {
				continue
			}
			if len(out) >= limit {
				break
			}
			out = append(out, ev)
			if ev.ID > lastID {
				lastID = ev.ID
			}
		}
		return out, lastID, nil
	}
}

func TestRecreatedDatabaseIsDetectedFromAnEmptyFeed(t *testing.T) {
	// The reported failure: the vce database is recreated and its ids restart
	// at 1 while the cursor still points at 100. Every poll then returns an
	// empty batch with lastId == cursor, which the shipper read as "nothing
	// new" — so node_radio_events for this node simply stopped, with no line in
	// any log, until vce had produced 100 events again.
	dir := t.TempDir()

	db := []sdrctl.ActivityEvent{
		{ID: 1, AtMs: 1, Action: "CALL", EventType: "CALL_GROUP"},
		{ID: 2, AtMs: 2, Action: "CALL", EventType: "CALL_GROUP"},
	}
	var probes []int64

	var shipped []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []sdrctl.ActivityEvent `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		for _, ev := range body.Events {
			shipped = append(shipped, ev.ID)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	s := New(Options{
		DataDir:   dir,
		DBPath:    filepath.Join(dir, "nonexistent.sqlite"),
		Fetch:     vceFetch(&db, &probes),
		ServerURL: srv.URL,
		NodeToken: "tok",
		InstallID: "inst",
	})
	if err := s.resetStream("keyA"); err != nil {
		t.Fatalf("resetStream: %v", err)
	}
	s.st.LastID = 100
	origStream := s.st.StreamID

	s.tick(context.Background())

	if s.st.StreamID == origStream {
		t.Fatal("a restarted id sequence must generate a fresh streamId")
	}
	if len(shipped) != 2 || shipped[0] != 1 || shipped[1] != 2 {
		t.Fatalf("the restarted sequence was not shipped from the start: %v", shipped)
	}
	if s.st.LastID != 2 {
		t.Fatalf("cursor should follow the new sequence, got %d", s.st.LastID)
	}
	if got := readCursor(t, dir); got != s.st {
		t.Fatalf("reset not persisted: file %+v != state %+v", got, s.st)
	}
	// Proved with two one-row queries: nothing at or above the cursor, but the
	// database does still hold something below it.
	if len(probes) != 2 || probes[0] != 99 || probes[1] != 0 {
		t.Fatalf("want probes at since=99 then since=0, got %v", probes)
	}
}

func TestAnIntactCursorIsNeverTreatedAsARestart(t *testing.T) {
	dir := t.TempDir()

	// A vce migration replaces the database file with every id preserved, and a
	// quiet node polls an empty feed for hours. Both must be left alone: a
	// reset here re-ships the whole retention under a streamId the backend has
	// never seen, so its (streamId, id) dedupe cannot suppress any of it.
	db := []sdrctl.ActivityEvent{
		{ID: 99, AtMs: 1, Action: "CALL", EventType: "CALL_GROUP"},
		{ID: 100, AtMs: 2, Action: "CALL", EventType: "CALL_GROUP"},
	}
	var probes []int64
	s := New(Options{
		DataDir: dir,
		DBPath:  filepath.Join(dir, "nonexistent.sqlite"),
		Fetch:   vceFetch(&db, &probes),
	})
	if err := s.resetStream("keyA"); err != nil {
		t.Fatalf("resetStream: %v", err)
	}
	s.st.LastID = 100
	origStream := s.st.StreamID

	s.tick(context.Background())
	if s.st.StreamID != origStream || s.st.LastID != 100 {
		t.Fatalf("a quiet feed was mistaken for a restart: %+v", s.st)
	}
	// The cursor row is still there, so the second probe is never needed.
	if len(probes) != 1 || probes[0] != 99 {
		t.Fatalf("want one probe at since=99, got %v", probes)
	}

	// And it does not re-ask on every empty poll.
	s.tick(context.Background())
	if len(probes) != 1 {
		t.Fatalf("the id check ran again inside its interval: %v", probes)
	}

	// An unreadable database answers every query empty — vce reports a locked
	// or missing database as an empty batch — which proves nothing either way.
	db = nil
	probes = nil
	s.lastRestartCheck = time.Time{}
	s.tick(context.Background())
	if s.st.StreamID != origStream || s.st.LastID != 100 {
		t.Fatalf("an unreadable database must not reset the stream: %+v", s.st)
	}
	if len(probes) != 2 {
		t.Fatalf("want both probes attempted, got %v", probes)
	}
}

func TestTickShipsAndAdvancesCursorOnlyOn2xx(t *testing.T) {
	dir := t.TempDir()

	src := func(id int) *int { return &id }
	events := []sdrctl.ActivityEvent{
		{ID: 7, AtMs: 1000, Action: "CALL", EventType: "GROUP", Source: src(101)},
		{ID: 9, AtMs: 2000, Action: "CALL", EventType: "GROUP", Encrypted: true},
	}
	fetch := func(sinceID int64, limit int) ([]sdrctl.ActivityEvent, int64, error) {
		if sinceID < 9 {
			return events, 9, nil
		}
		return nil, 9, nil
	}

	backendOK := false
	var gotBody struct {
		StreamID string                 `json:"streamId"`
		Events   []sdrctl.ActivityEvent `json:"events"`
	}
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("X-Node-Token") != "tok" || r.Header.Get("X-Node-Install") != "inst" {
			t.Errorf("missing node auth headers: %v", r.Header)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if !backendOK {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"accepted":2}`))
	}))
	defer srv.Close()

	s := New(Options{
		DataDir:   dir,
		DBPath:    filepath.Join(dir, "nonexistent.sqlite"),
		Fetch:     fetch,
		ServerURL: srv.URL,
		NodeToken: "tok",
		InstallID: "inst",
	})
	if err := s.resetStream("keyA"); err != nil {
		t.Fatalf("resetStream: %v", err)
	}

	// Backend failing: cursor must NOT advance.
	s.tick(context.Background())
	if s.st.LastID != 0 {
		t.Fatalf("cursor advanced despite backend 500: %d", s.st.LastID)
	}
	if got := readCursor(t, dir); got.LastID != 0 {
		t.Fatalf("persisted cursor advanced despite backend 500: %+v", got)
	}

	// A failed ship now schedules a backoff, so the next tick is a deliberate
	// no-op until it expires — that is the point of it, and without clearing
	// it here the rest of this test would be measuring the backoff, not the
	// cursor.
	if s.shipFails != 1 {
		t.Fatalf("expected one recorded ship failure, got %d", s.shipFails)
	}
	if !s.retryAfter.After(time.Now()) {
		t.Fatalf("expected a backoff to be scheduled after a 500")
	}
	s.tick(context.Background())
	if s.st.LastID != 0 {
		t.Fatalf("tick during backoff must not ship: cursor moved to %d", s.st.LastID)
	}
	s.retryAfter = time.Time{}

	// Backend healthy: same rows re-fetched and shipped, cursor advances.
	backendOK = true
	s.tick(context.Background())
	if s.st.LastID != 9 {
		t.Fatalf("cursor should advance to 9 after 2xx, got %d", s.st.LastID)
	}
	if got := readCursor(t, dir); got.LastID != 9 {
		t.Fatalf("advanced cursor not persisted: %+v", got)
	}
	// A success clears the backoff, so an isolated blip cannot leave the
	// shipper throttled once the backend is well again.
	if s.shipFails != 0 || !s.retryAfter.IsZero() {
		t.Fatalf("successful ship must clear backoff, got fails=%d retryAfter=%v",
			s.shipFails, s.retryAfter)
	}
	if gotBody.StreamID != s.st.StreamID {
		t.Fatalf("POST streamId %q != cursor streamId %q", gotBody.StreamID, s.st.StreamID)
	}
	if len(gotBody.Events) != 2 || gotBody.Events[0].ID != 7 || gotBody.Events[1].ID != 9 {
		t.Fatalf("unexpected shipped events: %+v", gotBody.Events)
	}
	if calls != 2 {
		t.Fatalf("expected 2 backend calls (one failed, one ok), got %d", calls)
	}
}

// A single event the backend will never accept must not freeze the feed.
//
// ship() halves a too-large batch until one event remains, and then POSTs it
// anyway. Treating the resulting 413 as retryable meant that event was re-sent
// forever — and because the cursor only advances on success, every later event
// queued behind it indefinitely. One dropped event beats a dead feed; the
// audio send path already makes the same call.
func TestShipDropsPermanentlyRejectedSingleEvent(t *testing.T) {
	dir := t.TempDir()

	var posts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		w.WriteHeader(http.StatusRequestEntityTooLarge)
	}))
	defer srv.Close()

	s := New(Options{
		DataDir:   dir,
		DBPath:    filepath.Join(dir, "nonexistent.sqlite"),
		ServerURL: srv.URL,
		NodeToken: "tok",
		InstallID: "inst",
	})
	if err := s.resetStream("keyA"); err != nil {
		t.Fatalf("resetStream: %v", err)
	}

	one := []sdrctl.ActivityEvent{{ID: 7, AtMs: 1, Action: "CALL", EventType: "CALL_GROUP"}}
	if err := s.ship(context.Background(), one); err != nil {
		t.Fatalf("a permanently rejected single event must not be an error: %v", err)
	}
	if posts != 1 {
		t.Fatalf("expected exactly one POST, got %d", posts)
	}

	// A 413 on a batch that CAN still be split stays retryable — the split is
	// the fix there, not dropping half the batch.
	two := []sdrctl.ActivityEvent{
		{ID: 8, AtMs: 1, Action: "CALL", EventType: "CALL_GROUP"},
		{ID: 9, AtMs: 1, Action: "CALL", EventType: "CALL_GROUP"},
	}
	if err := s.ship(context.Background(), two); err == nil {
		t.Fatal("a 413 on a splittable batch must still surface as an error")
	}
}
