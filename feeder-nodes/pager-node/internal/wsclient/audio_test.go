package wsclient

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeApplier records the audio calls the client makes. The config-apply half
// of ConfigApplier is never exercised here.
type fakeApplier struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeApplier) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *fakeApplier) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

func (f *fakeApplier) Apply(PagerConfig) error   { return nil }
func (f *fakeApplier) Restart(string) error      { return nil }
func (f *fakeApplier) Rescan() error             { return nil }
func (f *fakeApplier) AudioStart(l string) error { f.record("start:" + l); return nil }
func (f *fakeApplier) AudioStop() error          { f.record("stop"); return nil }
func (f *fakeApplier) SupportedGains() []float64 { return nil }

// settle waits for the applier's last call to be want.
func settle(t *testing.T, f *fakeApplier, want string) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		calls := f.snapshot()
		if len(calls) > 0 && calls[len(calls)-1] == want {
			return calls
		}
		if time.Now().After(deadline) {
			t.Fatalf("audio never settled on %q; calls %v", want, calls)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A stop must never overtake the start it was sent before.
//
// audioStart and audioStop used to each run in their own goroutine, so the
// order they executed in was the scheduler's, not the order the frames arrived
// — and Go puts the newest goroutine in runnext, so the later frame often ran
// first. Switching the monitor from one reader to another sends stop then
// start: run backwards, the start set the label, the stop then cleared it, and
// the staff UI showed a reader as live while the node streamed nothing.
func TestAudioStopCannotOvertakeItsStart(t *testing.T) {
	f := &fakeApplier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &Client{applier: f, audioWake: make(chan struct{}, 1)}
	go c.audioLoop(ctx)

	c.wantAudio("NSWRFS")
	settle(t, f, "start:NSWRFS")

	// The switch, delivered as one burst the way the read loop delivers it.
	c.wantAudio("")
	c.wantAudio("FRNSW")
	calls := settle(t, f, "start:FRNSW")

	// Whatever the loop did in between, it must not end tapping nothing, and it
	// must not have stopped the new reader after starting it.
	for i, call := range calls {
		if call == "stop" && i == len(calls)-1 {
			t.Fatalf("the node ended up with no tap after a switch: %v", calls)
		}
	}
}

// The reverse case: a quick Listen then Stop must end with the tap OFF. Running
// those backwards left the tap on after staff had stopped listening, streaming
// 22050 Hz PCM to nobody.
func TestAudioStartCannotOutliveItsStop(t *testing.T) {
	f := &fakeApplier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &Client{applier: f, audioWake: make(chan struct{}, 1)}
	go c.audioLoop(ctx)

	c.wantAudio("NSWRFS")
	c.wantAudio("")

	// Either the burst collapsed and nothing was ever tapped, or it was tapped
	// and then stopped. Both leave the tap off; a lone start does not.
	time.Sleep(200 * time.Millisecond)
	calls := f.snapshot()
	if len(calls) > 0 && calls[len(calls)-1] != "stop" {
		t.Fatalf("the tap was left running after a stop; calls %v", calls)
	}
}

// A failed change must not be recorded as applied, or the next frame asking for
// the same thing would be skipped and the node left where it was.
func TestFailedAudioChangeIsRetried(t *testing.T) {
	f := &fakeApplier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failing := &failOnceApplier{fakeApplier: f}
	c := &Client{applier: failing, audioWake: make(chan struct{}, 1)}
	go c.audioLoop(ctx)

	c.wantAudio("NSWRFS")
	settle(t, f, "start:NSWRFS")
	c.wantAudio("NSWRFS") // same label again: the first attempt failed
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(f.snapshot()) >= 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a failed audio change was recorded as applied: %v", f.snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// failOnceApplier fails its first AudioStart and succeeds afterwards.
type failOnceApplier struct {
	*fakeApplier
	mu     sync.Mutex
	failed bool
}

func (f *failOnceApplier) AudioStart(l string) error {
	f.fakeApplier.record("start:" + l)
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.failed {
		f.failed = true
		return errAudioTest
	}
	return nil
}

var errAudioTest = errTest("audio change failed")

type errTest string

func (e errTest) Error() string { return string(e) }
