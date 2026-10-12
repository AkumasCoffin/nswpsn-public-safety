package pagersdr

import (
	"strings"
	"testing"
)

// A settled reading is one several consecutive readings agree on. Taking the
// last line of a fixed run instead is what produced -1, 28, -94 and 10 ppm on
// four consecutive boots of the same dongle — and a bogus 94 ppm at
// 148.5875 MHz is ~14 kHz of detune, which walks the POCSAG carrier out of
// rtl_fm's passband and leaves the reader decoding nothing at all.
func TestScanTakesTheSettledReadingNotTheLastOne(t *testing.T) {
	out := strings.Join([]string{
		"Found 1 device(s):",
		"cumulative PPM: -1",
		"cumulative PPM: 28",
		"cumulative PPM: -94",
		"cumulative PPM: 11",
		"cumulative PPM: 12",
		"cumulative PPM: 12",
		"cumulative PPM: -40", // a late outlier the old code would have taken
	}, "\n")

	stopped := 0
	ppm, ok, seen, _ := scanForStablePPM(strings.NewReader(out), func() { stopped++ })
	if !ok {
		t.Fatalf("three agreeing readings should settle; saw %v", seen)
	}
	if ppm != 12 {
		t.Fatalf("want the settled 12 ppm, got %d (readings %v)", ppm, seen)
	}
	if stopped != 1 {
		t.Fatalf("rtl_test should be interrupted exactly once, on settling; got %d", stopped)
	}
	// It keeps reading to EOF after interrupting, so rtl_test exits cleanly and
	// releases the dongle — a SIGPIPE'd rtl_test leaves it briefly claimed and
	// the reader that starts next dies on usb_claim_interface.
	if len(seen) != 7 {
		t.Fatalf("scan should drain to EOF after stopping, saw %d readings", len(seen))
	}
}

func TestScanRefusesAReadingThatNeverSettles(t *testing.T) {
	out := "cumulative PPM: -1\ncumulative PPM: 28\ncumulative PPM: -94\ncumulative PPM: 10\n"
	ppm, ok, seen, _ := scanForStablePPM(strings.NewReader(out), nil)
	if ok {
		t.Fatalf("a drifting dongle must not yield a figure, got %d", ppm)
	}
	if len(seen) != 4 {
		t.Fatalf("want all four readings recorded for the error message, got %v", seen)
	}
	if lo, hi := ppmSpread(seen); lo != -94 || hi != 28 {
		t.Fatalf("spread should bound the readings, got %d..%d", lo, hi)
	}
}

func TestScanNeedsAFullWindow(t *testing.T) {
	// Two readings that happen to agree are not evidence; rtl_test prints one
	// about every ten seconds and the early ones are USB jitter.
	out := "cumulative PPM: 7\ncumulative PPM: 7\n"
	if _, ok, _, _ := scanForStablePPM(strings.NewReader(out), nil); ok {
		t.Fatalf("%d agreeing readings must not settle the figure", ppmStableWindow-1)
	}
}

func TestScanToleratesASmallSpread(t *testing.T) {
	// A dongle sitting a ppm either side of its figure has settled; demanding
	// three identical readings would mean waiting out the whole budget on
	// perfectly usable hardware.
	out := "cumulative PPM: 30\ncumulative PPM: 31\ncumulative PPM: 32\n"
	ppm, ok, _, _ := scanForStablePPM(strings.NewReader(out), nil)
	if !ok || ppm != 32 {
		t.Fatalf("want the newest of a settled window (32), got %d ok=%t", ppm, ok)
	}
}
