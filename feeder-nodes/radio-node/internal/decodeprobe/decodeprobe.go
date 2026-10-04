// Package decodeprobe measures how well a started channel is decoding.
//
// It is the one place that knows how to turn vce's live channel state into a
// verdict, shared by the automatic channel manager (retesting a channel it
// stopped) and the RF site survey (ranking candidate GRN sites). Both ask the
// same question — "is this frequency actually decoding?" — and getting two
// different answers from two implementations would be worse than either.
//
// The measurement, learned from the channel manager's history:
//
//   - A channel that has not reached a locked state within LockWait is not a
//     weak signal, it is no signal. No decode number it reports afterwards
//     would mean anything.
//   - vce's syncPercent is a 30s ROLLING WINDOW. On a runtime that counts the
//     acquisition period as failed decoding, the first half-minute of samples
//     is dominated by it and only samples older than TrustAge mean anything.
//     A runtime that reports decodingForMs does not have that problem — it
//     excludes acquisition — so a sample is trusted as soon as the channel has
//     been decoding for PostLockTrust, which is seconds rather than most of a
//     minute. That difference is the bulk of what an RF site survey costs.
//   - The verdict is the MEDIAN of the trusted samples, never the best one: a
//     mostly-dead channel still throws occasional good readings, and judging
//     it on its best moment is how a bad site gets kept.
//   - Null and 0 both mean "unmeasured" (no monitor attached, or no fresh
//     snapshot) — never a reading of zero.
//
// Measure handles a BATCH of channels in one shared window. The survey starts
// several channels inside one tuner's bandwidth at a time, and measuring them
// concurrently is what makes a whole-neighbourhood survey finish in minutes
// rather than an hour; the channel manager simply passes a batch of one.
package decodeprobe

import (
	"context"
	"sort"
	"time"
)

const (
	// LockWait: a control channel reaches CONTROL within a few seconds on a
	// usable signal. Nothing locked by now = nothing to measure.
	LockWait = 15 * time.Second
	// TrustAge: how old a sample must be before the rolling window has
	// outgrown the channel's acquisition period.
	TrustAge = 35 * time.Second
	// Window: total measurement time per batch, from start to verdict.
	Window = 60 * time.Second
	// PostLockTrust: how long a channel must have been DECODING before its
	// figure is taken, on a runtime that reports that. Short on purpose: the
	// number already excludes acquisition, so this is only asking for enough
	// frames behind it to be steady.
	PostLockTrust = 4 * time.Second
	// MinFramesTrusted: and enough frames, for a channel whose traffic is slow
	// enough that four seconds is only a handful of them.
	MinFramesTrusted = 24

	// MinSamples: fewer trusted readings than this and the answer is "never
	// measured", not a ruling on one or two numbers.
	MinSamples = 3
	// EnoughSamples: once every channel in the batch has this many trusted
	// readings the verdict will not change, so the rest of the window is time
	// spent for nothing. At the sampling cadence this is reached around 15s
	// after the trust age — and with a survey paying this per wave, those
	// seconds are most of what a caller can actually get back.
	EnoughSamples = 8

	// lockPoll/samplePoll: fast polling while waiting for lock (the sooner a
	// dead batch is known, the sooner it can be abandoned), slower once the
	// only job is collecting samples.
	lockPoll   = 1 * time.Second
	samplePoll = 2 * time.Second
)

// Outcome is a channel's verdict.
type Outcome string

const (
	// Measured: locked, and enough trusted samples to rule on.
	Measured Outcome = "measured"
	// NoLock: never reached a locked state within LockWait.
	NoLock Outcome = "noLock"
	// Unmeasured: locked, but decode was never measurable (an old runtime, no
	// monitor attached, or the window ended too early).
	Unmeasured Outcome = "unmeasured"
)

// lockedStates mirrors vce's isLockedState: any of these means the channel has
// acquired its control channel (or is actively working).
var lockedStates = map[string]bool{
	"CONTROL": true, "CALL": true, "ENCRYPTED": true, "DATA": true, "ACTIVE": true,
}

// Locked reports whether a channel state means the channel has acquired.
func Locked(control bool, state string) bool { return control || lockedStates[state] }

// Snapshot is the slice of a live channel decodeprobe reads.
type Snapshot struct {
	Control     bool
	State       string
	SyncPercent *float64
	SignalDbfs  *float64
	// DecodingForMs/SyncFrames: how long the channel has been decoding and how
	// many frames are behind the figure. Nil on a runtime that does not report
	// them, which is what selects the slow, wait-out-the-window path.
	DecodingForMs *int64
	SyncFrames    *int64
}

// trusted reports whether this reading can be taken at face value yet, and
// whether the runtime was able to say so itself.
func trusted(s Snapshot, age time.Duration) (ok bool, authoritative bool) {
	if s.DecodingForMs == nil {
		return age >= TrustAge, false
	}
	if *s.DecodingForMs < PostLockTrust.Milliseconds() {
		return false, true
	}
	if s.SyncFrames != nil && *s.SyncFrames < MinFramesTrusted {
		return false, true
	}
	return true, true
}

// Deps injects the clock, the sleep, and the live-channel read, so the whole
// measurement runs in tests without a JVM.
type Deps struct {
	// Fetch returns the current live channels keyed by TRIMMED name. vce
	// channel ids are reassigned on every reload; names are what survive.
	Fetch func() (map[string]Snapshot, error)
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration)
}

// Result is one channel's measurement.
type Result struct {
	Outcome Outcome
	// MedianPct is the median trusted sample, or -1 when there was no verdict.
	MedianPct float64
	Samples   int
	// SignalDbfs is the last signal level seen while sampling (nil if never).
	SignalDbfs *float64
	// Vanished means the channel disappeared from the live list mid-measure —
	// a config import replaced the channel set under us. The caller decides
	// whether that invalidates the run (the survey aborts; the manager just
	// lets the next tick reconcile).
	Vanished bool
}

type state struct {
	locked   bool
	vanished bool
	samples  []float64
	signal   *float64
	// fast: the runtime answered for itself how long this channel has been
	// decoding, so the window does not have to be waited out.
	fast bool
}

// Measure starts nothing and stops nothing: the caller has already started the
// named channels. It watches them for Window and returns a verdict each.
//
// It gives up early when every channel has vanished, or when LockWait has
// passed and not one of them ever locked — a batch of dead frequencies should
// not cost the full window.
func Measure(ctx context.Context, names []string, d Deps) map[string]Result {
	out := make(map[string]Result, len(names))
	if len(names) == 0 {
		return out
	}
	now := d.Now
	st := make(map[string]*state, len(names))
	for _, n := range names {
		st[n] = &state{}
	}

	started := now()
	for ctx.Err() == nil {
		age := now().Sub(started)
		if age >= Window {
			break
		}
		// Poll quickly until there is something to sample, then ease off.
		poll := lockPoll
		if age >= TrustAge {
			poll = samplePoll
		}
		d.Sleep(ctx, poll)
		if ctx.Err() != nil {
			break
		}
		age = now().Sub(started)

		live, err := d.Fetch()
		if err != nil {
			continue // control server hiccup — not a verdict about RF
		}
		anyPresent, anyLocked := false, false
		for _, n := range names {
			s := st[n]
			snap, ok := live[n]
			if !ok {
				s.vanished = true
				continue
			}
			s.vanished = false
			anyPresent = true
			if Locked(snap.Control, snap.State) {
				s.locked = true
			}
			if s.locked {
				anyLocked = true
			}
			take, authoritative := trusted(snap, age)
			if authoritative {
				s.fast = true
			}
			if take && snap.SyncPercent != nil && *snap.SyncPercent > 0 {
				s.samples = append(s.samples, *snap.SyncPercent)
				if snap.SignalDbfs != nil {
					v := *snap.SignalDbfs
					s.signal = &v
				}
			}
		}
		if !anyPresent {
			break // the whole batch is gone — the world changed under us
		}
		if age >= LockWait && !anyLocked {
			break // nothing here locks; no point waiting out the window
		}
		// Every channel that could answer has answered enough times.
		if settled(names, st) {
			break
		}
	}

	for _, n := range names {
		s := st[n]
		r := Result{MedianPct: -1, Samples: len(s.samples), SignalDbfs: s.signal, Vanished: s.vanished}
		switch {
		case !s.locked:
			r.Outcome = NoLock
		default:
			if m := Median(s.samples); m >= 0 {
				r.Outcome, r.MedianPct = Measured, m
			} else {
				r.Outcome = Unmeasured
			}
		}
		out[n] = r
	}
	return out
}

// settled reports whether every channel still present has collected enough
// trusted samples to rule on. A channel that never locked is not waited for —
// it has already failed — and one that has vanished cannot answer at all.
//
// A runtime that reports its own decoding time needs far fewer samples to be
// conclusive: each one already excludes acquisition, where on an older runtime
// the spread across the window IS the measurement.
func settled(names []string, st map[string]*state) bool {
	answered := 0
	for _, n := range names {
		s := st[n]
		if s.vanished || !s.locked {
			continue
		}
		need := EnoughSamples
		if s.fast {
			need = MinSamples
		}
		if len(s.samples) < need {
			return false
		}
		answered++
	}
	return answered > 0
}

// Median is the middle of the trusted samples, or -1 when there were too few
// to rule on. It sorts a copy: the caller's slice order is not meaningful but
// the samples are, and a surprise in-place sort is a bad habit.
func Median(in []float64) float64 {
	if len(in) < MinSamples {
		return -1
	}
	v := append([]float64(nil), in...)
	sort.Float64s(v)
	mid := len(v) / 2
	if len(v)%2 == 1 {
		return v[mid]
	}
	return (v[mid-1] + v[mid]) / 2
}
