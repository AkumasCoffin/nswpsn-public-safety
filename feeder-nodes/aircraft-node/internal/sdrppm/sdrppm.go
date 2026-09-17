// Package sdrppm measures an RTL-SDR's crystal error.
//
// A cheap RTL dongle's oscillator is typically tens of ppm off its nominal
// 28.8 MHz, and drifts further as the board warms up. At 1090 MHz a 30 ppm
// error is ~33 kHz — enough to cost messages at the edge of coverage, which is
// exactly the traffic a receiver exists to hear.
//
// So the agent measures it the same way the pager agent does: run `rtl_test -p`
// and take its cumulative reading. The measurement is only used when the
// backend has not pushed an explicit ppm; a staff-set value always wins, since
// someone who typed a number meant it.
//
// WAITING FOR THE READING TO SETTLE IS THE WHOLE JOB. rtl_test prints a
// cumulative figure every ten seconds and tells you, in its own startup banner,
// to "press ^C after a few minutes" — the early readings are dominated by USB
// transfer jitter and startup transients. This package used to run for twenty
// seconds and take whatever the last line said, which is one or two readings of
// noise. One node measured -1, 28, -94 and 10 ppm on four consecutive boots of
// the same dongle; -94 ppm is ~102 kHz at 1090 MHz, and applying it would have
// been far worse than not correcting at all.
//
// So readings are now accepted only once consecutive ones agree, and the run
// stops as soon as they do — which costs about thirty seconds on a healthy
// dongle rather than the full budget. A dongle that never settles yields an
// error, and the caller keeps its previous correction or runs without one.
package sdrppm

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// rtlTestBin is a var, not a const, so tests can point it at a stub.
var rtlTestBin = "rtl_test"

// MeasureDur is the LONGEST rtl_test -p is allowed to run. It is a ceiling, not
// a duration: the measurement returns as soon as the reading settles, which
// needs three agreeing readings and so takes about thirty seconds.
//
// A minute, not the "few minutes" rtl_test itself suggests, because this holds
// the dongle and the decoder cannot start until it lets go. A longer budget
// buys a correction on marginal hardware at the price of real decoding time on
// every boot, and a receiver that is deaf for two minutes is worse off than one
// running a few ppm out. Hosts whose reading never settles at all are common
// enough to plan for — a VM's clock jitters, and rtl_test measures the dongle
// against it.
const MeasureDur = time.Minute

const (
	// StableWindow is how many consecutive cumulative readings must agree
	// before the figure is believed. rtl_test prints one every ten seconds, so
	// three is the shortest window that can distinguish a settled reading from
	// two noisy ones that happen to land near each other.
	StableWindow = 3

	// StableTolerance is the spread those readings may span, in ppm. Two ppm is
	// ~2 kHz at 1090 MHz, which is far below what costs a message.
	StableTolerance = 2
)

// MaxPlausible bounds what is accepted. A dongle needing more correction than
// this is faulty or the reading is noise, and writing an absurd ppm into the
// decoder's command line would be worse than not correcting at all.
const MaxPlausible = 100

// cumPpmRe captures the integer from rtl_test's "cumulative PPM: <n>" lines.
var cumPpmRe = regexp.MustCompile(`cumulative PPM:\s*(-?\d+)`)

// Measure runs rtl_test -p against the given device index and returns a settled
// cumulative ppm reading, or an error if it never settles.
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

	// One os.Pipe for both streams rather than two StdoutPipe/StderrPipe
	// readers: these are *os.File, so exec hands the same descriptor to the
	// child twice and there is no interleaving goroutine to race. rtl_test
	// reports on stderr, but that is its choice to change, not ours to assume.
	pr, pw, err := os.Pipe()
	if err != nil {
		return 0, fmt.Errorf("pipe for %s: %w", rtlTestBin, err)
	}
	cmd.Stdout = pw
	cmd.Stderr = pw

	if err := cmd.Start(); err != nil {
		_ = pw.Close()
		_ = pr.Close()
		return 0, fmt.Errorf("start %s: %w", rtlTestBin, err)
	}
	// Drop the parent's writer so the read side sees EOF when the child exits.
	_ = pw.Close()

	ppm, ok, seen, raw := scanForStable(pr, func() {
		_ = cmd.Process.Signal(os.Interrupt)
	})
	_ = pr.Close()
	_ = cmd.Wait()

	if !ok {
		if len(seen) == 0 {
			return 0, fmt.Errorf("%s -p produced no cumulative PPM reading for device %d: %s",
				rtlTestBin, index, strings.TrimSpace(tail(raw, 200)))
		}
		lo, hi := spread(seen)
		// The spread is the useful part of this message: it separates "the
		// dongle is drifting" from "the dongle was never opened".
		return 0, fmt.Errorf("%s -p never settled for device %d: %d readings spanning %d..%d ppm over %s",
			rtlTestBin, index, len(seen), lo, hi, dur)
	}
	if ppm > MaxPlausible || ppm < -MaxPlausible {
		return 0, fmt.Errorf("measured ppm %d is outside the plausible range (+/-%d)", ppm, MaxPlausible)
	}
	return ppm, nil
}

// scanForStable reads rtl_test's output and returns the first cumulative
// reading backed by StableWindow consecutive readings agreeing to within
// StableTolerance.
//
// On success it calls stop (which interrupts rtl_test) and then KEEPS READING
// until EOF. Returning straight away would close the pipe under a child that is
// still writing, killing it with SIGPIPE — and only a clean exit releases the
// USB device, which is the difference between the decoder starting and the
// decoder crash-looping on "usb_claim_interface error".
func scanForStable(r io.Reader, stop func()) (ppm int, ok bool, seen []int, raw string) {
	var b strings.Builder
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

	for sc.Scan() {
		line := sc.Text()
		// Enough context for an error message, not the whole run.
		if b.Len() < 8192 {
			b.WriteString(line)
			b.WriteByte('\n')
		}
		m := cumPpmRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		v, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		seen = append(seen, v)
		if ok || len(seen) < StableWindow {
			continue
		}
		lo, hi := spread(seen[len(seen)-StableWindow:])
		if hi-lo <= StableTolerance {
			// The newest reading integrates the longest run, so it is the best
			// of the window rather than an average of it.
			ppm, ok = v, true
			if stop != nil {
				stop()
			}
		}
	}
	return ppm, ok, seen, b.String()
}

// spread returns the smallest and largest of vs. Callers only reach it with a
// non-empty slice.
func spread(vs []int) (lo, hi int) {
	lo, hi = vs[0], vs[0]
	for _, v := range vs {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}
	return lo, hi
}

// tail returns up to the last n bytes of s, for compact error context.
func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
