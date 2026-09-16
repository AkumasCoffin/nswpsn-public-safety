package autogain

import (
	"testing"
	"time"
)

// ready returns a controller whose interval has already elapsed, so a single
// Consider call is evaluated rather than rate-limited.
func ready(gain float64) (*Controller, time.Time) {
	c := New(&gain)
	now := time.Now()
	c.lastMove = now.Add(-2 * Interval)
	return c, now
}

func TestWalksDownWhenOverdriven(t *testing.T) {
	c, now := ready(40)
	g, ok := c.Consider(Sample{Messages: 5000, StrongFraction: 0.30}, now)
	if !ok {
		t.Fatal("expected a gain change when strongly over-driven")
	}
	if g != 40-Step {
		t.Fatalf("gain = %v, want %v", g, 40-Step)
	}
}

func TestWalksUpWhenQuiet(t *testing.T) {
	c, now := ready(20)
	g, ok := c.Consider(Sample{Messages: 5000, StrongFraction: 0.001}, now)
	if !ok {
		t.Fatal("expected a gain change with headroom available")
	}
	if g != 20+Step {
		t.Fatalf("gain = %v, want %v", g, 20+Step)
	}
}

func TestHoldsInsideTheBand(t *testing.T) {
	// The hysteresis band is what stops the loop oscillating around a single
	// threshold, restarting the decoder every interval forever.
	c, now := ready(30)
	if _, ok := c.Consider(Sample{Messages: 5000, StrongFraction: 0.05}, now); ok {
		t.Fatal("expected no change inside the hysteresis band")
	}
	if c.Gain() != 30 {
		t.Fatalf("gain moved to %v", c.Gain())
	}
}

func TestSilenceIsNotQuietness(t *testing.T) {
	// The failure this guards: at 3am with no aircraft overhead, a naive loop
	// reads zero strong messages as "lots of headroom" and walks gain to max.
	c, now := ready(20)
	if _, ok := c.Consider(Sample{Messages: 3, StrongFraction: 0}, now); ok {
		t.Fatal("expected no change from a sample with too few messages")
	}
}

func TestRespectsTheInterval(t *testing.T) {
	// Each change costs a decoder restart, so decisions must be paced.
	c := New(nil)
	now := time.Now()
	if _, ok := c.Consider(Sample{Messages: 5000, StrongFraction: 0.9}, now); !ok {
		t.Fatal("first decision should be allowed")
	}
	if _, ok := c.Consider(Sample{Messages: 5000, StrongFraction: 0.9}, now.Add(time.Second)); ok {
		t.Fatal("a second decision one second later must be suppressed")
	}
	if _, ok := c.Consider(Sample{Messages: 5000, StrongFraction: 0.9}, now.Add(2*Interval)); !ok {
		t.Fatal("a decision after the interval should be allowed")
	}
}

func TestStopsAtTheRails(t *testing.T) {
	c, now := ready(MinGain)
	if _, ok := c.Consider(Sample{Messages: 5000, StrongFraction: 0.9}, now); ok {
		t.Fatal("must not move below the minimum gain")
	}
	c2, now2 := ready(MaxGain)
	if _, ok := c2.Consider(Sample{Messages: 5000, StrongFraction: 0}, now2); ok {
		t.Fatal("must not move above the maximum gain")
	}
}

func TestConvergesFromMaximum(t *testing.T) {
	// A new receiver starts at max and should walk into the band within a few
	// decisions rather than wandering.
	c := New(nil)
	if c.Gain() != StartGain {
		t.Fatalf("start gain = %v, want %v", c.Gain(), StartGain)
	}
	now := time.Now()
	for i := 0; i < 10; i++ {
		// Model a site where everything above 30 dB over-drives the front end.
		strong := 0.005
		if c.Gain() > 30 {
			strong = 0.4
		}
		now = now.Add(Interval)
		c.Consider(Sample{Messages: 5000, StrongFraction: strong}, now)
	}
	if c.Gain() > 30 || c.Gain() < 20 {
		t.Fatalf("did not converge into the usable range: %v", c.Gain())
	}
}
