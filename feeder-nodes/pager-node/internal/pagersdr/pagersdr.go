// Package pagersdr enumerates and prepares RTL-SDR dongles for the pager
// readers. It shells out to the librtlsdr command-line tools (rtl_test,
// rtl_eeprom) installed on the host by the node installer.
//
// The key job is guaranteeing each dongle has a UNIQUE, stable USB serial so a
// reader can be pinned to a specific antenna/frequency by serial (rtl_fm -d
// <serial>). Cheap RTL-SDR clones ship with identical serials ("00000001"), which
// makes per-device addressing ambiguous. We only rewrite EEPROM serials when they
// actually collide — an EEPROM write is a permanent hardware change, so a dongle
// that already has a distinct serial is left untouched.
package pagersdr

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Device is one enumerated RTL-SDR dongle.
type Device struct {
	Index  int       // librtlsdr device index (as reported by rtl_test)
	Serial string    // USB serial ("SN:" from rtl_test); may be blank on unprogrammed clones
	PPM    int       // measured sample-clock error (rtl_test -p); 0 if unmeasured
	Gains  []float64 // supported tuner-gain steps (dB) parsed from rtl_test; nil if unknown
	// PPMMeasured is set when a measurement was attempted at the last detect.
	// A spare dongle the frequency plan does not use is left unmeasured.
	PPMMeasured bool
}

const (
	rtlTestBin   = "rtl_test"
	rtlEepromBin = "rtl_eeprom"

	detectTimeout = 30 * time.Second
	eepromTimeout = 30 * time.Second

	// PPMMeasureDur is the LONGEST rtl_test -p is allowed to run per dongle. It
	// is a ceiling, not a duration: the measurement returns as soon as the
	// reading settles, which needs ppmStableWindow agreeing readings and so
	// takes about thirty seconds on a healthy dongle.
	//
	// A minute, not the "few minutes" rtl_test itself suggests, because this
	// holds the dongle and no reader can start until it lets go — and a node
	// that is deaf for two minutes on every boot is worse off than one running
	// a few ppm out.
	PPMMeasureDur = time.Minute
)

// How a cumulative reading is believed. The aircraft agent's internal/sdrppm
// reached the same two numbers for the same reason; this is a port, not a
// coincidence, and the two agents are separate Go modules so the code cannot
// simply be shared.
const (
	// ppmStableWindow is how many consecutive cumulative readings must agree
	// before the figure is believed. rtl_test prints one about every ten
	// seconds, so three is the shortest window that can tell a settled reading
	// from two noisy ones that happen to land near each other.
	ppmStableWindow = 3
	// ppmStableTolerance is the spread those readings may span, in ppm. Two ppm
	// is ~300 Hz at 148 MHz, far below anything that costs a page.
	ppmStableTolerance = 2
)

// cumPpmRe captures the integer from rtl_test's "cumulative PPM: <n>" lines.
var cumPpmRe = regexp.MustCompile(`cumulative PPM:\s*(-?\d+)`)

// deviceLineRe matches a device-list entry such as
// "  0:  Realtek, RTL2838UHIDIR, SN: 00000001", capturing the index and the
// trailing descriptor (from which the serial is extracted).
var deviceLineRe = regexp.MustCompile(`^\s*(\d+):\s+(.*)$`)

// foundRe matches the "Found N device(s):" header that precedes the device list.
var foundRe = regexp.MustCompile(`Found\s+\d+\s+device`)

// gainsRe captures the space-separated gain list from rtl_test's line
// "Supported gain values (29): 0.0 0.9 1.4 ... 49.6".
var gainsRe = regexp.MustCompile(`Supported gain values\s*\(\d+\):\s*(.+)`)

// Detect enumerates attached RTL-SDR dongles by running `rtl_test -t` (which
// prints the device list to stderr, runs a brief tuner test, and exits) and
// parsing its output. It returns a clear error if rtl_test is missing/fails and
// no devices could be parsed.
func Detect() ([]Device, error) {
	ctx, cancel := context.WithTimeout(context.Background(), detectTimeout)
	defer cancel()

	// rtl_test prints to stderr; CombinedOutput captures both streams. A non-zero
	// exit is tolerated as long as the device list parsed (some builds exit non-zero
	// after the tuner test), so we key the error on "did we parse any device?".
	out, err := exec.CommandContext(ctx, rtlTestBin, "-t").CombinedOutput()
	text := string(out)
	devs := parseDevices(text)
	if len(devs) == 0 {
		if err != nil {
			return nil, fmt.Errorf("run %s: %w: %s", rtlTestBin, err, strings.TrimSpace(text))
		}
		return nil, fmt.Errorf("%s reported no RTL-SDR devices", rtlTestBin)
	}
	return devs, nil
}

// parseGains extracts the supported tuner-gain steps (dB) from rtl_test output,
// or nil if the line isn't present. rtl_test -t opens one device and prints its
// tuner's gains; a node's dongles are the same model, so this list applies to
// all of them.
func parseGains(text string) []float64 {
	m := gainsRe.FindStringSubmatch(text)
	if m == nil {
		return nil
	}
	var gains []float64
	for _, tok := range strings.Fields(m[1]) {
		if v, err := strconv.ParseFloat(tok, 64); err == nil {
			gains = append(gains, v)
		}
	}
	return gains
}

// parseDevices extracts the device list from rtl_test output. It starts
// collecting at the "Found N device(s):" header and stops at the first line that
// isn't a device entry.
func parseDevices(text string) []Device {
	var devs []Device
	inList := false
	for _, line := range strings.Split(text, "\n") {
		if !inList {
			if foundRe.MatchString(line) {
				inList = true
			}
			continue
		}
		m := deviceLineRe.FindStringSubmatch(strings.TrimRight(line, "\r"))
		if m == nil {
			// End of the list (blank line or "Using device ...").
			if len(devs) > 0 {
				break
			}
			continue
		}
		idx, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		devs = append(devs, Device{Index: idx, Serial: extractSerial(m[2])})
	}
	// Attach the tuner's supported gain steps (same model across the node) to
	// every device so a reader/UI can offer valid gains.
	if gains := parseGains(text); len(gains) > 0 {
		for i := range devs {
			devs[i].Gains = gains
		}
	}
	return devs
}

// extractSerial pulls the value after "SN:" from a device descriptor, or "" if
// none is present (an unprogrammed dongle).
func extractSerial(desc string) string {
	i := strings.LastIndex(desc, "SN:")
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(desc[i+len("SN:"):])
}

// MeasurePPM estimates a dongle's sample-clock error in ppm by running
// `rtl_test -p` on it for at most `dur`. rtl_test measures the RTL's actual vs
// nominal sample rate against the host clock (no RF signal or antenna needed)
// and prints a cumulative estimate roughly every 10s. The device MUST be free
// (no reader running).
//
// WAITING FOR THE READING TO SETTLE IS THE WHOLE JOB. This used to run for a
// fixed 30s and take whatever the last line said, which is two or three
// readings still dominated by USB transfer jitter and start-up transients — the
// approach the aircraft agent abandoned after one node measured -1, 28, -94 and
// 10 ppm on four consecutive boots of the same dongle. A bogus 94 ppm at
// 148.5875 MHz is ~14 kHz of detune, which walks the POCSAG carrier out of
// rtl_fm's passband at -s 22050: the reader then decodes nothing at all, while
// the heartbeat still reports it running.
//
// So a reading is accepted only once consecutive ones agree, and the run stops
// as soon as they do — about thirty seconds on a healthy dongle rather than the
// full budget. Returns 0 + an error when nothing was produced (tool missing,
// device busy) or when the reading never settled; the caller falls back to
// ppm=0, which is close enough for wide NBFM.
func MeasurePPM(index int, dur time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	// rtl_test -p runs until interrupted; the context timeout stops it after dur.
	cmd := exec.CommandContext(ctx, rtlTestBin, "-d", strconv.Itoa(index), "-p")
	// Stop it with SIGINT (Ctrl-C) so it closes the USB device CLEANLY — a SIGKILL
	// (the default) can leave the dongle briefly claimed and make the reader that
	// starts next fail with "usb_claim_interface error". WaitDelay force-kills if
	// rtl_test ignores the interrupt.
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 5 * time.Second

	// One os.Pipe for both streams rather than two pipe readers: these are
	// *os.File, so exec hands the child the same descriptor twice and there is
	// no interleaving goroutine to race. rtl_test reports on stderr, but that
	// is its choice to change, not ours to assume.
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

	ppm, ok, seen, raw := scanForStablePPM(pr, func() {
		_ = cmd.Process.Signal(os.Interrupt)
	})
	_ = pr.Close()
	_ = cmd.Wait()

	if ok {
		return ppm, nil
	}
	if len(seen) == 0 {
		return 0, fmt.Errorf("%s -p produced no cumulative PPM reading for device %d: %s",
			rtlTestBin, index, strings.TrimSpace(tail(raw, 200)))
	}
	// The spread is the useful part of this message: it separates "the dongle is
	// drifting" from "the dongle was never opened".
	lo, hi := ppmSpread(seen)
	return 0, fmt.Errorf("%s -p never settled for device %d: %d readings spanning %d..%d ppm over %s",
		rtlTestBin, index, len(seen), lo, hi, dur)
}

// scanForStablePPM reads rtl_test's output and returns the first cumulative
// reading backed by ppmStableWindow consecutive readings agreeing to within
// ppmStableTolerance.
//
// On success it calls stop (which interrupts rtl_test) and then KEEPS READING
// until EOF. Returning straight away would close the pipe under a child that is
// still writing, killing it with SIGPIPE — and only a clean exit releases the
// USB device, which is the difference between the reader starting and the
// reader crash-looping on "usb_claim_interface error".
func scanForStablePPM(r io.Reader, stop func()) (ppm int, ok bool, seen []int, raw string) {
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
		if ok || len(seen) < ppmStableWindow {
			continue
		}
		lo, hi := ppmSpread(seen[len(seen)-ppmStableWindow:])
		if hi-lo <= ppmStableTolerance {
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

// ppmSpread returns the smallest and largest of vs. Callers only reach it with
// a non-empty slice.
func ppmSpread(vs []int) (lo, hi int) {
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

// EnsureDistinctSerials guarantees every dongle has a unique, non-blank serial.
//
// It ONLY reprograms EEPROMs when serials collide (two or more dongles share a
// serial, or a serial is blank). When all serials are already distinct and
// non-empty it does nothing and returns the devices unchanged — an EEPROM write
// is a permanent hardware change and must not happen gratuitously.
//
// On collision it assigns each device (in index order) a fresh serial
// "0000000N" via `rtl_eeprom -d <index> -s ...`, logs every write, notes that a
// USB replug/reset may be needed for the new serials to take effect, then
// re-detects and returns the updated list.
func EnsureDistinctSerials(devs []Device) ([]Device, error) {
	// A single dongle is addressed by index and can never collide — never rewrite
	// its EEPROM (a blank serial on a lone device is fine).
	if len(devs) < 2 {
		return devs, nil
	}
	if !hasCollision(devs) {
		return devs, nil
	}

	log.Printf("pagersdr: SDR serials collide or are blank; reprogramming EEPROM to assign unique serials")
	for i, d := range devs {
		serial := fmt.Sprintf("%08d", i+1) // 0000000N
		if err := writeSerial(d.Index, serial); err != nil {
			return devs, fmt.Errorf("assign serial %q to device %d: %w", serial, d.Index, err)
		}
		log.Printf("pagersdr: wrote EEPROM serial %q to device index %d (PERMANENT hardware change)", serial, d.Index)
	}
	log.Printf("pagersdr: EEPROM serials rewritten; a USB replug/reset may be required for the new serials to take effect")

	updated, err := Detect()
	if err != nil {
		return devs, fmt.Errorf("re-detect after EEPROM rewrite: %w", err)
	}
	return updated, nil
}

// hasCollision reports whether any serial is blank or shared by two or more
// devices.
func hasCollision(devs []Device) bool {
	seen := make(map[string]int, len(devs))
	for _, d := range devs {
		if strings.TrimSpace(d.Serial) == "" {
			return true
		}
		seen[d.Serial]++
	}
	for _, n := range seen {
		if n > 1 {
			return true
		}
	}
	return false
}

// writeSerial runs rtl_eeprom to set a device's USB serial. rtl_eeprom prompts
// for confirmation before writing, so we feed "y" on stdin to auto-confirm.
func writeSerial(index int, serial string) error {
	ctx, cancel := context.WithTimeout(context.Background(), eepromTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, rtlEepromBin, "-d", strconv.Itoa(index), "-s", serial)
	cmd.Stdin = strings.NewReader("y\n") // auto-confirm the interactive write prompt
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w: %s", rtlEepromBin, err, strings.TrimSpace(string(out)))
	}
	return nil
}
