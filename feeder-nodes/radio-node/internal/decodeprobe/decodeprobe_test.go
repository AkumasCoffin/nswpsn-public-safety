package decodeprobe

import (
	"context"
	"testing"
	"time"
)

// clock is an instant-sleep fake: the measurement's minute of wall time runs
// in microseconds.
type clock struct {
	now   time.Time
	live  map[string]Snapshot
	fetch int
	// onFetch mutates the world as the measurement polls it.
	onFetch func(c *clock)
	err     error
}

func newClock() *clock {
	return &clock{now: time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC), live: map[string]Snapshot{}}
}

func (c *clock) deps() Deps {
	return Deps{
		Now:   func() time.Time { return c.now },
		Sleep: func(_ context.Context, d time.Duration) { c.now = c.now.Add(d) },
		Fetch: func() (map[string]Snapshot, error) {
			c.fetch++
			if c.onFetch != nil {
				c.onFetch(c)
			}
			if c.err != nil {
				return nil, c.err
			}
			out := make(map[string]Snapshot, len(c.live))
			for k, v := range c.live {
				out[k] = v
			}
			return out, nil
		},
	}
}

func fptr(v float64) *float64 { return &v }

func TestAcquisitionSamplesAreDiscarded(t *testing.T) {
	// syncPercent is a rolling window that is not cleared on start, so the
	// first half-minute reads low on a perfectly good channel. Only samples
	// older than TrustAge may count — otherwise every good site fails.
	c := newClock()
	started := c.now
	c.live["A"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(5)}
	c.onFetch = func(c *clock) {
		pct := 5.0
		if c.now.Sub(started) >= TrustAge {
			pct = 90
		}
		c.live["A"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(pct)}
	}
	res := Measure(context.Background(), []string{"A"}, c.deps())["A"]
	if res.Outcome != Measured || res.MedianPct != 90 {
		t.Fatalf("acquisition losses leaked into the verdict: %+v", res)
	}
}

func TestNoLockGivesUpAtLockWait(t *testing.T) {
	c := newClock()
	c.live["A"] = Snapshot{State: "IDLE"}
	start := c.now
	res := Measure(context.Background(), []string{"A"}, c.deps())["A"]
	if res.Outcome != NoLock || res.MedianPct != -1 {
		t.Fatalf("want noLock, got %+v", res)
	}
	if waited := c.now.Sub(start); waited > LockWait+2*time.Second {
		t.Fatalf("a dead frequency should not cost the full window: waited %s", waited)
	}
}

func TestLockedButUnmeasurable(t *testing.T) {
	// An old runtime (or a channel with no monitor) reports null forever:
	// that is "never measured", never a reading of zero.
	c := newClock()
	c.live["A"] = Snapshot{Control: true, State: "CONTROL"}
	res := Measure(context.Background(), []string{"A"}, c.deps())["A"]
	if res.Outcome != Unmeasured || res.Samples != 0 {
		t.Fatalf("want unmeasured, got %+v", res)
	}
	// Zero is unmeasured too, not a decode rate of 0%.
	c = newClock()
	c.live["A"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(0)}
	if res := Measure(context.Background(), []string{"A"}, c.deps())["A"]; res.Outcome != Unmeasured {
		t.Fatalf("zero must not count as a sample: %+v", res)
	}
}

func TestBatchIsMeasuredInOneWindow(t *testing.T) {
	c := newClock()
	c.live["A"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(80)}
	c.live["B"] = Snapshot{Control: true, State: "CALL", SyncPercent: fptr(40)}
	c.live["C"] = Snapshot{State: "IDLE"}
	start := c.now
	out := Measure(context.Background(), []string{"A", "B", "C"}, c.deps())
	if out["A"].MedianPct != 80 || out["B"].MedianPct != 40 {
		t.Fatalf("batch verdicts wrong: %+v", out)
	}
	if out["C"].Outcome != NoLock {
		t.Fatalf("C never locked: %+v", out["C"])
	}
	// One shared window, not one per channel.
	if elapsed := c.now.Sub(start); elapsed > Window+2*time.Second {
		t.Fatalf("batch took %s — the window is shared", elapsed)
	}
}

func TestSpikeLosesToTheMedian(t *testing.T) {
	c := newClock()
	n := 0
	c.onFetch = func(c *clock) {
		n++
		pct := 12.0
		if n%7 == 0 {
			pct = 95
		}
		c.live["A"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(pct)}
	}
	res := Measure(context.Background(), []string{"A"}, c.deps())["A"]
	if res.Outcome != Measured || res.MedianPct != 12 {
		t.Fatalf("a mostly-dead channel must not pass on its best moment: %+v", res)
	}
}

func TestVanishedChannelIsFlagged(t *testing.T) {
	c := newClock()
	c.live["A"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(90)}
	c.onFetch = func(c *clock) {
		if c.fetch >= 3 {
			delete(c.live, "A")
		}
	}
	res := Measure(context.Background(), []string{"A"}, c.deps())["A"]
	if !res.Vanished {
		t.Fatalf("a channel that disappeared must say so: %+v", res)
	}
}

func TestFetchErrorsAreNotVerdicts(t *testing.T) {
	// A control-server hiccup is not evidence about RF: it must not look like
	// a vanished channel or shorten the window.
	c := newClock()
	c.live["A"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(70)}
	c.onFetch = func(c *clock) {
		c.err = nil
		if c.fetch%3 == 0 {
			c.err = context.DeadlineExceeded
		}
	}
	res := Measure(context.Background(), []string{"A"}, c.deps())["A"]
	if res.Outcome != Measured || res.MedianPct != 70 || res.Vanished {
		t.Fatalf("transient fetch errors changed the verdict: %+v", res)
	}
}

func TestEnoughSamplesEndsTheWindowEarly(t *testing.T) {
	// The whole cost of a survey is one window per wave, so once every
	// channel has answered enough times there is nothing left to buy.
	c := newClock()
	c.live["A"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(82)}
	start := c.now
	res := Measure(context.Background(), []string{"A"}, c.deps())["A"]
	if res.Outcome != Measured || res.MedianPct != 82 {
		t.Fatalf("verdict changed: %+v", res)
	}
	if res.Samples < MinSamples {
		t.Fatalf("too few samples to rule on: %d", res.Samples)
	}
	took := c.now.Sub(start)
	if took >= Window {
		t.Fatalf("ran the full window despite having the answer: %s", took)
	}
	if took < TrustAge {
		t.Fatalf("cut the acquisition period short: %s", took)
	}
}

func TestSlowChannelStillGetsTheFullWindow(t *testing.T) {
	// A channel reporting only now and then must not be cut off early just
	// because a sibling has answered.
	c := newClock()
	c.live["Fast"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(90)}
	c.live["Slow"] = Snapshot{Control: true, State: "CONTROL"}
	n := 0
	c.onFetch = func(c *clock) {
		n++
		// The slow one reports a reading only every fourth look.
		if n%4 == 0 {
			c.live["Slow"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(55)}
		} else {
			c.live["Slow"] = Snapshot{Control: true, State: "CONTROL"}
		}
	}
	start := c.now
	out := Measure(context.Background(), []string{"Fast", "Slow"}, c.deps())
	if out["Fast"].MedianPct != 90 {
		t.Fatalf("fast channel: %+v", out["Fast"])
	}
	if c.now.Sub(start) < Window-samplePoll {
		t.Fatalf("gave up on the slow channel after %s", c.now.Sub(start))
	}
}

func i64(v int64) *int64 { return &v }

func TestRuntimeThatExcludesAcquisitionIsTrustedImmediately(t *testing.T) {
	// The wait exists because an older runtime counts a channel's acquisition
	// as failed decoding, so the figure only means something once those
	// seconds have aged out of its 30s window. A runtime that reports how long
	// the channel has actually been decoding has already excluded them.
	c := newClock()
	started := c.now
	c.onFetch = func(c *clock) {
		ms := c.now.Sub(started).Milliseconds()
		c.live["A"] = Snapshot{
			Control: true, State: "CONTROL", SyncPercent: fptr(84),
			DecodingForMs: i64(ms), SyncFrames: i64(ms / 20),
		}
	}
	res := Measure(context.Background(), []string{"A"}, c.deps())["A"]
	if res.Outcome != Measured || res.MedianPct != 84 {
		t.Fatalf("verdict wrong: %+v", res)
	}
	took := c.now.Sub(started)
	if took >= TrustAge {
		t.Fatalf("waited out the old window despite the runtime answering: %s", took)
	}
	if took < PostLockTrust {
		t.Fatalf("took the figure before the channel had decoded for long enough: %s", took)
	}
}

func TestFreshlyDecodingChannelIsNotSampledYet(t *testing.T) {
	// Decoding, but only just: the figure has almost nothing behind it.
	c := newClock()
	started := c.now
	c.onFetch = func(c *clock) {
		// Pinned just under the threshold for the whole run.
		c.live["A"] = Snapshot{
			Control: true, State: "CONTROL", SyncPercent: fptr(99),
			DecodingForMs: i64(PostLockTrust.Milliseconds() - 1), SyncFrames: i64(1000),
		}
	}
	res := Measure(context.Background(), []string{"A"}, c.deps())["A"]
	if res.Outcome != Unmeasured {
		t.Fatalf("a figure with nothing behind it is not a measurement: %+v", res)
	}
	if c.now.Sub(started) < Window {
		t.Fatalf("should have kept waiting for it to settle")
	}
}

func TestTooFewFramesIsNotYetAMeasurement(t *testing.T) {
	// Decoding long enough, but on traffic so slow that barely any frames
	// back the number.
	c := newClock()
	c.onFetch = func(c *clock) {
		c.live["A"] = Snapshot{
			Control: true, State: "CONTROL", SyncPercent: fptr(70),
			DecodingForMs: i64(30_000), SyncFrames: i64(MinFramesTrusted - 1),
		}
	}
	if res := Measure(context.Background(), []string{"A"}, c.deps())["A"]; res.Outcome != Unmeasured {
		t.Fatalf("want unmeasured, got %+v", res)
	}
}

func TestOlderRuntimeStillWaitsOutItsWindow(t *testing.T) {
	// No decodingForMs = the old contract, where early samples are acquisition
	// losses and the only safe answer is to wait.
	c := newClock()
	started := c.now
	c.onFetch = func(c *clock) {
		pct := 5.0
		if c.now.Sub(started) >= TrustAge {
			pct = 88
		}
		c.live["A"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(pct)}
	}
	res := Measure(context.Background(), []string{"A"}, c.deps())["A"]
	if res.MedianPct != 88 {
		t.Fatalf("acquisition leaked into the verdict: %+v", res)
	}
	if c.now.Sub(started) < TrustAge {
		t.Fatalf("trusted an old runtime too early: %s", c.now.Sub(started))
	}
}

func TestMedian(t *testing.T) {
	if got := Median([]float64{10, 95, 12}); got != 12 {
		t.Fatalf("median of a spike should be the middle value, got %v", got)
	}
	if got := Median([]float64{80, 90}); got != -1 {
		t.Fatalf("too few samples must not produce a verdict, got %v", got)
	}
	if got := Median([]float64{70, 80, 90, 100}); got != 85 {
		t.Fatalf("even-count median wrong: %v", got)
	}
	// The caller's slice is left alone.
	in := []float64{30, 10, 20}
	_ = Median(in)
	if in[0] != 30 {
		t.Fatalf("Median sorted the caller's slice: %v", in)
	}
}

func TestCancellationStopsTheMeasurement(t *testing.T) {
	c := newClock()
	c.live["A"] = Snapshot{Control: true, State: "CONTROL", SyncPercent: fptr(90)}
	ctx, cancel := context.WithCancel(context.Background())
	c.onFetch = func(c *clock) {
		if c.fetch >= 2 {
			cancel()
		}
	}
	start := c.now
	res := Measure(ctx, []string{"A"}, c.deps())["A"]
	cancel()
	if c.now.Sub(start) >= Window {
		t.Fatalf("cancellation did not cut the window short")
	}
	if res.Outcome == Measured {
		t.Fatalf("a cancelled measurement has no verdict: %+v", res)
	}
}
