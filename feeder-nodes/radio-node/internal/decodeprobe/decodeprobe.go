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
//   - vce's syncPercent is a 30s ROLLING WINDOW that is NOT cleared when a
//     channel starts, so the first half-minute of samples is dominated by
//     acquisition losses. Only samples older than TrustAge count.
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
	// MinSamples: fewer trusted readings than this and the answer is "never
	// measured", not a ruling on one or two numbers.
	MinSamples = 3

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
			if age >= TrustAge && snap.SyncPercent != nil && *snap.SyncPercent > 0 {
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
