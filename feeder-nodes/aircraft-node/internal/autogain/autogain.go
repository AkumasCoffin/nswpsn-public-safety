// Package autogain adapts the receiver's gain to its site.
//
// There is no single correct gain for ADS-B. The right value depends on the
// antenna, the feedline loss, and what else is radiating nearby — a rooftop
// antenna with 20m of coax and a city full of transmitters wants a very
// different setting from a window-sill whip in a country town. A national
// fleet of volunteer receivers therefore cannot ship one number, and asking
// volunteers to tune by hand asks them to interpret decoder statistics.
//
// The approach follows wiedehopf's autogain1090: watch the share of messages
// arriving "strong" (loud enough that the front end is being over-driven) and
// walk the gain down when there are too many, up when there are too few.
// Overload is the more damaging failure — a saturated front end loses distant
// aircraft entirely, while slightly low gain merely trims the fringe — so the
// loop is deliberately asymmetric: it reacts sooner to overload than to quiet.
//
// This is NOT the dongle's hardware AGC (`--gain -10`). That responds to total
// band energy on millisecond timescales, which for the short, bursty 1090
// transmissions means it rides down on interference and misses the packets
// that matter. Every serious ADS-B setup uses a fixed gain; the question is
// only which one, and this answers it per site.
//
// Each change restarts the decoder (~2s of lost reception, plus a USB settle),
// so the loop runs on a multi-minute cadence and logs every step. Cheap to get
// right slowly; expensive to thrash.
package autogain

import (
	"log"
	"time"
)

// Tuning constants. The thresholds come from autogain1090's rationale rather
// than from a specific measurement of any one site.
const (
	// StrongHigh: above this share of strong messages the front end is being
	// over-driven and gain comes down.
	StrongHigh = 0.10
	// StrongLow: below this, there is headroom to hear further, so gain goes
	// up. The gap between the two is the hysteresis band — inside it, nothing
	// happens, which is what stops the loop oscillating around a threshold.
	StrongLow = 0.02

	// Step is how far gain moves per decision, dB. Roughly two steps of the
	// R820T's gain table: large enough to converge from either extreme within
	// a few decisions, small enough not to overshoot past the band.
	Step = 2.5

	// Interval between decisions. Each one costs a decoder restart, and the
	// statistics need time to reflect the previous change.
	Interval = 5 * time.Minute

	// MinMessages is the sample floor for a decision. A receiver that heard
	// almost nothing in the window tells us nothing about its gain — at 3am
	// with no aircraft overhead, a naive loop would read the silence as "too
	// quiet" and walk gain to maximum.
	MinMessages = 200

	// StartGain is where a fresh receiver begins: max. Starting high and
	// walking down converges faster than the reverse, because an over-driven
	// receiver still produces plenty of messages to measure, whereas one set
	// far too low produces almost none and trips MinMessages.
	StartGain = 49.6
)

// Sample is one window of decoder statistics.
type Sample struct {
	// Messages accepted in the window.
	Messages int64
	// StrongFraction is the share of them that arrived loud, 0..1.
	StrongFraction float64
}

// Controller holds the loop's state.
type Controller struct {
	gain     float64
	lastMove time.Time
	// settled records that the loop has stopped moving, so the reason is
	// logged once rather than every interval.
	settled bool
}

// New starts a controller at `start` dB, or at StartGain when start is nil
// (a receiver with no remembered gain).
func New(start *float64) *Controller {
	g := StartGain
	if start != nil {
		g = *start
	}
	return &Controller{gain: g}
}

// Gain is the controller's current setting, dB.
func (c *Controller) Gain() float64 { return c.gain }

// Consider evaluates a sample and reports the new gain if it should change.
//
// Returns ok=false when nothing should happen: too soon since the last move,
// not enough messages to judge, or the receiver is already inside the band.
func (c *Controller) Consider(s Sample, now time.Time) (newGain float64, ok bool) {
	if !c.lastMove.IsZero() && now.Sub(c.lastMove) < Interval {
		return 0, false
	}
	if s.Messages < MinMessages {
		// Not enough traffic to judge. Explicitly NOT a reason to raise gain:
		// see MinMessages.
		return 0, false
	}

	var want float64
	switch {
	case s.StrongFraction > StrongHigh:
		want = c.gain - Step
	case s.StrongFraction < StrongLow:
		want = c.gain + Step
	default:
		if !c.settled {
			c.settled = true
			log.Printf("autogain: settled at %.1f dB (strong %.1f%% within %.0f-%.0f%% band)",
				c.gain, s.StrongFraction*100, StrongLow*100, StrongHigh*100)
		}
		return 0, false
	}

	if want < MinGain {
		want = MinGain
	}
	if want > MaxGain {
		want = MaxGain
	}
	if want == c.gain {
		// Already at a rail — moving further is impossible, and logging it
		// every interval would be noise.
		return 0, false
	}

	dir := "up"
	if want < c.gain {
		dir = "down"
	}
	log.Printf("autogain: %s %.1f -> %.1f dB (strong %.1f%%, %d msgs)",
		dir, c.gain, want, s.StrongFraction*100, s.Messages)
	c.gain = want
	c.lastMove = now
	c.settled = false
	return want, true
}

// Gain rails, duplicated from the decoder package's tuner limits so this
// package stays dependency-free and unit-testable on its own.
const (
	MinGain = 0.0
	MaxGain = 49.6
)
