// Package sdrppm measures an RTL-SDR's crystal error.
//
// A cheap RTL dongle's oscillator is typically tens of ppm off its nominal
// 28.8 MHz, and drifts further as the board warms up. At 1090 MHz a 30 ppm
// error is ~33 kHz — enough to cost messages at the edge of coverage, which is
// exactly the traffic a receiver exists to hear.
//
// So the agent measures it the same way the pager agent does: run `rtl_test -p`
// briefly at startup and take its last cumulative reading. The measurement is
// only used when the backend has not pushed an explicit ppm; a staff-set value
// always wins, since someone who typed a number meant it.
//
// This costs ~20s of startup during which the dongle is busy, so it runs BEFORE
// the decoder is launched and never again while the agent is up.
package sdrppm

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const rtlTestBin = "rtl_test"

// MeasureDur is how long rtl_test -p runs. It reports a cumulative figure that
// settles within a few seconds; 20s is a compromise between a stable reading
// and the reception lost while the dongle is occupied.
const MeasureDur = 20 * time.Second

// MaxPlausible bounds what is accepted. A dongle needing more correction than
// this is faulty or the reading is noise, and writing an absurd ppm into the
// decoder's command line would be worse than not correcting at all.
const MaxPlausible = 100

// cumPpmRe captures the integer from rtl_test's "cumulative PPM: <n>" lines.
var cumPpmRe = regexp.MustCompile(`cumulative PPM:\s*(-?\d+)`)

// Measure runs rtl_test -p against the given device index and returns its last
// cumulative ppm reading.
func Measure(index int, dur time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	cmd := exec.CommandContext(ctx, rtlTestBin, "-d", strconv.Itoa(index), "-p")
	// SIGINT, not the default SIGKILL: rtl_test runs until interrupted, and only
	// a clean exit releases the USB device properly. A kill can leave the dongle
	// briefly claimed, and the decoder we start next then fails with
	// "usb_claim_interface error" and crash-loops. WaitDelay force-kills if
	// rtl_test ignores the interrupt.
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 5 * time.Second

	// The interrupt makes this return an error; the readings printed before the
	// stop are what we want, so the error is deliberately ignored.
	out, _ := cmd.CombinedOutput()
	text := string(out)

	ms := cumPpmRe.FindAllStringSubmatch(text, -1)
	if len(ms) == 0 {
		return 0, fmt.Errorf("%s -p produced no cumulative PPM reading for device %d: %s",
			rtlTestBin, index, strings.TrimSpace(tail(text, 200)))
	}
	ppm, err := strconv.Atoi(ms[len(ms)-1][1])
	if err != nil {
		return 0, fmt.Errorf("parse ppm %q: %w", ms[len(ms)-1][1], err)
	}
	if ppm > MaxPlausible || ppm < -MaxPlausible {
		return 0, fmt.Errorf("measured ppm %d is outside the plausible range (+/-%d)", ppm, MaxPlausible)
	}
	return ppm, nil
}

// tail returns up to the last n bytes of s, for compact error context.
func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
