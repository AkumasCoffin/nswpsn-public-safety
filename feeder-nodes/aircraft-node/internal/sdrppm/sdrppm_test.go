package sdrppm

import (
	"strings"
	"testing"
	"time"
)

// The banner rtl_test prints before any reading. Reproduced because the parser
// has to skip it, and because "Press ^C after a few minutes" is the upstream
// statement that the old twenty-second run contradicted.
const banner = `Found 3 device(s):
  0:  Generic, RTL2832U OEM, SN: 00000002
  1:  Realtek, RTL2838UHIDIR, SN: 00000001
  2:  Nooelec, NESDR SMArt v5, SN: 00000004

Using device 0: Generic RTL2832U OEM
Found Rafael Micro R820T tuner
Sampling at 2048000 S/s.
Reporting PPM error measurement every 10 seconds...
Press ^C after a few minutes.
Reading samples in async mode...
lost at least 44 bytes
`

func reading(cum int) string {
	return "real sample rate: 2048013 current PPM: 3 cumulative PPM: " +
		strings.TrimSpace(itoa(cum)) + "\n"
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var d []byte
	for v > 0 {
		d = append([]byte{byte('0' + v%10)}, d...)
		v /= 10
	}
	if neg {
		return "-" + string(d)
	}
	return string(d)
}

func TestSettledReadingIsAccepted(t *testing.T) {
	out := banner + reading(6) + reading(1) + reading(1) + reading(1)
	ppm, ok, seen, _ := scanForStable(strings.NewReader(out), nil)
	if !ok {
		t.Fatalf("a settled dongle should produce a reading; saw %v", seen)
	}
	if ppm != 1 {
		t.Fatalf("ppm = %d, want 1 (the newest of the settled window)", ppm)
	}
}

func TestNoisyDongleYieldsNothingRatherThanNoise(t *testing.T) {
	// The reported failure. One node measured -1, 28, -94 and 10 ppm on four
	// consecutive boots of the SAME dongle, each the final line of its own
	// twenty-second run. -94 ppm is ~102 kHz at 1090 MHz; feeding that to the
	// decoder is far worse than leaving the receiver uncorrected. Readings this
	// far apart must produce no figure at all.
	out := banner + reading(-1) + reading(28) + reading(-94) + reading(10)
	ppm, ok, seen, _ := scanForStable(strings.NewReader(out), nil)
	if ok {
		t.Fatalf("readings spanning %v must not be trusted; got ppm=%d", seen, ppm)
	}
	if len(seen) != 4 {
		t.Fatalf("every reading should still be reported for diagnosis, got %v", seen)
	}
}

func TestNearMissesStillCount(t *testing.T) {
	// Settling is not the same as being identical; a dongle wobbling by one ppm
	// is settled, and demanding exactness would mean never correcting anything.
	out := banner + reading(12) + reading(11) + reading(12)
	ppm, ok, _, _ := scanForStable(strings.NewReader(out), nil)
	if !ok || ppm != 12 {
		t.Fatalf("ppm=%d ok=%v, want 12/true", ppm, ok)
	}
}

func TestSpreadAtTheToleranceEdge(t *testing.T) {
	stable := banner + reading(10) + reading(12) + reading(10) // spans exactly 2
	if _, ok, _, _ := scanForStable(strings.NewReader(stable), nil); !ok {
		t.Fatal("a spread equal to the tolerance is settled")
	}
	unstable := banner + reading(10) + reading(13) + reading(10) // spans 3
	if _, ok, _, _ := scanForStable(strings.NewReader(unstable), nil); ok {
		t.Fatal("a spread beyond the tolerance is not settled")
	}
}

func TestSettlesOnlyAfterAFullWindow(t *testing.T) {
	// Two readings that agree prove nothing — the old code effectively trusted
	// one. The window must actually be filled.
	out := banner + reading(7) + reading(7)
	if _, ok, seen, _ := scanForStable(strings.NewReader(out), nil); ok {
		t.Fatalf("%d readings must not settle a %d-wide window", len(seen), StableWindow)
	}
}

func TestDeviceThatNeverOpened(t *testing.T) {
	// What the node actually logged when the decoder still held the dongle.
	out := banner + "usb_claim_interface error -6\nFailed to open rtlsdr device #0.\n"
	_, ok, seen, raw := scanForStable(strings.NewReader(out), nil)
	if ok || len(seen) != 0 {
		t.Fatalf("a device that never opened has no readings, got %v", seen)
	}
	if !strings.Contains(raw, "usb_claim_interface error -6") {
		t.Fatal("the raw output must survive for the error message to be diagnosable")
	}
}

func TestKeepsDrainingAfterInterrupting(t *testing.T) {
	// Returning at the moment of convergence would close the pipe under a child
	// still writing to it, killing rtl_test with SIGPIPE. Only a clean exit
	// releases the USB device; otherwise the decoder we start next fails with
	// "usb_claim_interface error" and crash-loops. So the scan interrupts and
	// then reads on to EOF.
	out := banner + reading(5) + reading(5) + reading(5) +
		reading(5) + reading(5) + "Signal caught, exiting!\n"

	stops := 0
	ppm, ok, seen, _ := scanForStable(strings.NewReader(out), func() { stops++ })

	if !ok || ppm != 5 {
		t.Fatalf("ppm=%d ok=%v, want 5/true", ppm, ok)
	}
	if stops != 1 {
		t.Fatalf("rtl_test should be interrupted exactly once, got %d", stops)
	}
	if len(seen) != 5 {
		t.Fatalf("the scan must run to EOF, not stop at convergence; saw %d readings", len(seen))
	}
}

func TestMeasureDurFitsAWindowWithoutStallingTheDecoder(t *testing.T) {
	// Two bounds, pulling opposite ways. Below StableWindow readings the
	// measurement can never settle, which is what the old twenty-second budget
	// got wrong. Above a minute it becomes the startup cost itself: the decoder
	// cannot open the dongle until this lets go, and a receiver that hears
	// nothing for two minutes every boot is worse off than one running a few
	// ppm out.
	minimum := StableWindow * 10
	if MeasureDur.Seconds() < float64(minimum) {
		t.Fatalf("MeasureDur %s cannot fit %d readings ten seconds apart",
			MeasureDur, StableWindow)
	}
	if MeasureDur > time.Minute {
		t.Fatalf("MeasureDur %s holds the dongle too long; the decoder waits on it", MeasureDur)
	}
}
