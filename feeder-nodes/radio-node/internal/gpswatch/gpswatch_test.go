package gpswatch

import (
	"errors"
	"testing"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
)

// fakeRuntime stands in for the control server's /gps.
type fakeRuntime struct {
	startedAt int64
	positions []sdrctl.GPSPosition
	invalid   int64
	err       error
	asked     []int64
}

func (f *fakeRuntime) fetch(after int64, limit int) (sdrctl.GPSResponse, error) {
	f.asked = append(f.asked, after)
	if f.err != nil {
		return sdrctl.GPSResponse{}, f.err
	}
	out := sdrctl.GPSResponse{Summary: sdrctl.GPSSummary{
		StartedAtMs: f.startedAt, LastSeq: int64(len(f.positions)), Invalid: f.invalid,
	}}
	for _, p := range f.positions {
		if p.Seq > after && len(out.Positions) < limit {
			out.Positions = append(out.Positions, p)
			out.Summary.LastAtMs = p.At
		}
	}
	return out, nil
}

func (f *fakeRuntime) add(n int) {
	for i := 0; i < n; i++ {
		seq := int64(len(f.positions) + 1)
		f.positions = append(f.positions, sdrctl.GPSPosition{Seq: seq, At: 1000 + seq, Lat: -33.8, Lon: 151.2, From: "1234"})
	}
}

func snap(w *Watcher) Snapshot { return w.Snapshot().(Snapshot) }

func TestReportsNothingUntilAPositionIsDecoded(t *testing.T) {
	rt := &fakeRuntime{startedAt: 500, invalid: 3}
	w := New(Options{Fetch: rt.fetch})
	if !w.tick() {
		t.Fatal("a runtime with /gps is supported")
	}
	s := snap(w)
	if !s.Supported || s.Positions != 0 || s.Invalid != 3 || len(s.Recent) != 0 || s.SinceMs != 500 {
		t.Fatalf("unexpected snapshot: %+v", s)
	}
}

func TestResumesFromTheLastPositionSeen(t *testing.T) {
	rt := &fakeRuntime{startedAt: 500}
	w := New(Options{Fetch: rt.fetch})
	rt.add(3)
	w.tick()
	rt.add(2)
	w.tick()
	if got := rt.asked; len(got) != 2 || got[0] != 0 || got[1] != 3 {
		t.Fatalf("asked after %v, want [0 3]", got)
	}
	s := snap(w)
	if s.Positions != 5 || len(s.Recent) != 5 || s.Recent[4].Seq != 5 {
		t.Fatalf("unexpected snapshot: %+v", s)
	}
}

func TestKeepsOnlyTheNewestPositions(t *testing.T) {
	rt := &fakeRuntime{startedAt: 500}
	w := New(Options{Fetch: rt.fetch})
	rt.add(recentKept + 10)
	w.tick()
	s := snap(w)
	if len(s.Recent) != recentKept || s.Recent[recentKept-1].Seq != int64(recentKept+10) {
		t.Fatalf("kept %d, newest seq %d", len(s.Recent), s.Recent[len(s.Recent)-1].Seq)
	}
	if s.Positions != int64(recentKept+10) {
		t.Fatalf("count should cover every position, got %d", s.Positions)
	}
}

func TestStartsAgainWhenTheRuntimeRestarts(t *testing.T) {
	rt := &fakeRuntime{startedAt: 500}
	w := New(Options{Fetch: rt.fetch})
	rt.add(4)
	w.tick()

	// sdrtrunk restarts: a new start time, and the sequence begins at 1.
	rt.startedAt = 900
	rt.positions = nil
	rt.add(2)
	w.tick() // notices the restart
	w.tick() // reads the new run from the start
	s := snap(w)
	if s.Positions != 2 || len(s.Recent) != 2 || s.Recent[0].Seq != 1 || s.SinceMs != 900 {
		t.Fatalf("unexpected snapshot after restart: %+v", s)
	}
}

func TestAnOlderRuntimeIsUnsupportedNotAnError(t *testing.T) {
	rt := &fakeRuntime{err: errors.New("control GET /gps?after=0&limit=500: status 404")}
	w := New(Options{Fetch: rt.fetch})
	if w.tick() {
		t.Fatal("a 404 means the runtime has no /gps")
	}
	if snap(w).Supported {
		t.Fatal("should report unsupported")
	}
}

func TestARuntimeThatIsDownKeepsWhatWasSeen(t *testing.T) {
	rt := &fakeRuntime{startedAt: 500}
	w := New(Options{Fetch: rt.fetch})
	rt.add(2)
	w.tick()
	rt.err = errors.New("connection refused")
	if !w.tick() {
		t.Fatal("a refused connection is not 'unsupported'")
	}
	if s := snap(w); !s.Supported || len(s.Recent) != 2 {
		t.Fatalf("unexpected snapshot: %+v", s)
	}
}
